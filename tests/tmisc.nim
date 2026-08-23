import ../[nimlangserver, ls, utils]
import ../protocol/[enums, types]
import
  std/[options, json, os, jsonutils, sequtils, strutils, sugar, strformat]
import json_rpc/[rpcclient]
import chronicles
import lspsocketclient
import chronos/asyncproc
import unittest2

suite "Nimlangserver misc":
  let cmdParams = CommandLineParams(mode: some lsp, transport: some socket, port: getNextFreePort())
  let ls = main(cmdParams) #we could accesss to the ls here to test against its state
  let client = newLspSocketClient()
  waitFor client.connect("localhost", cmdParams.port)
  client.registerNotification(
    "window/showMessage", "window/workDoneProgress/create", "workspace/configuration",
    "extension/statusUpdate", "textDocument/publishDiagnostics", "$/progress",
  )

  test "after a period of inactivity, nimsuggest should be stopped":
    let initParams =
      LspInitializeParams %* {
        "processId": %getCurrentProcessId(),
        "rootUri": fixtureUri("projects/hw/"),
        "capabilities":
          {"window": {"workDoneProgress": true}, "workspace": {"configuration": true}},
      }
    let initializeResult = waitFor client.initialize(initParams)
    let nsTimeout = 1000
    let conf = NlsConfig(nimsuggestIdleTimeout: some nsTimeout)
    ls.workspaceConfiguration.complete(% @[conf])
    
    let gConf = waitFor ls.workspaceConfiguration

    asyncSpawn ls.tickLs() #We need to tick the ls so it get rid of the inactive nimsuggests

    let helloWorldUri = fixtureUri("projects/hw/hw.nim")
    let helloWorldFile = "projects/hw/hw.nim"
    let hwAbsFile = uriToPath(helloWorldFile.fixtureUri())
    client.notify("textDocument/didOpen", %createDidOpenParams(helloWorldFile))

    check waitFor client.waitForNotificationMessage(
      fmt"Nimsuggest initialized for {hwAbsFile}",
    )
    
    check waitFor client.waitForNotificationMessage(
      fmt"Nimsuggest for {hwAbsFile} was stopped because it was idle for too long",
    )

suite "Nimlangserver fail count":
  let cmdParams = CommandLineParams(mode: some lsp, transport: some socket, port: getNextFreePort())
  let ls = main(cmdParams)
  let client = newLspSocketClient()
  waitFor client.connect("localhost", cmdParams.port)
  client.registerNotification(
    "window/showMessage", "window/workDoneProgress/create", "workspace/configuration",
    "extension/statusUpdate", "textDocument/publishDiagnostics", "$/progress",
  )

  test "fail count is reset when a nimsuggest starts successfully":
    # ls.failTable only ever increments, so a project that crashes and
    # recovers keeps ratcheting toward MaxFails in getNimsuggest, after which
    # its requests are silently rerouted or dropped for the rest of the
    # session. A successful start must clear the count.
    let initParams =
      LspInitializeParams %* {
        "processId": %getCurrentProcessId(),
        "rootUri": fixtureUri("projects/hw/"),
        "capabilities":
          {"window": {"workDoneProgress": true}, "workspace": {"configuration": true}},
      }
    discard waitFor client.initialize(initParams)
    ls.workspaceConfiguration.complete(% @[NlsConfig()])
    discard waitFor ls.workspaceConfiguration

    let helloWorldFile = "projects/hw/hw.nim"
    let hwAbsFile = uriToPath(helloWorldFile.fixtureUri())
    ls.failTable[hwAbsFile] = 5

    client.notify("textDocument/didOpen", %createDidOpenParams(helloWorldFile))
    check waitFor client.waitForNotificationMessage(
      fmt"Nimsuggest initialized for {hwAbsFile}"
    )

    check hwAbsFile notin ls.failTable

suite "Nimlangserver pending requests":
  test "cancelled projectFile future does not escape addProjectFileToPendingRequest":
    # Regression test for #419: addProjectFileToPendingRequest is asyncSpawn'd,
    # so an escaping CancelledError (nimsuggest restart or $/cancelRequest
    # cancelling the awaited projectFile future) is re-raised into the event
    # loop, escapes runForever and hits main's `except Exception: quit(1)`.
    # The spawned task must swallow cancellation instead of failing.
    let ls = LanguageServer(serverMode: lsp)
    let uri = "file:///tmp/tpending419.nim"
    let projectFileFut = newFuture[string]("projectFile")
    ls.openFiles[uri] = NlsFileInfo(projectFile: projectFileFut)
    ls.pendingRequests[1'u] = PendingRequest(id: 1, name: "textDocument/definition")

    let fut = ls.addProjectFileToPendingRequest(1'u, uri)
    projectFileFut.cancelSoon()
    waitFor sleepAsync(10)

    check fut.finished
    check fut.completed

suite "Nimlangserver request cancellation":
  let cmdParams =
    CommandLineParams(mode: some lsp, transport: some socket, port: getNextFreePort())
  let ls = main(cmdParams)
  let client = newLspSocketClient()
  waitFor client.connect("localhost", cmdParams.port)
  client.registerNotification(
    "window/showMessage", "extension/statusUpdate", "textDocument/publishDiagnostics",
    "$/progress",
  )

  test "$/cancelRequest cancels a request that is still in flight":
    # This also pins down that the transport keeps reading while a request is
    # running: the cancellation can only be acted on if the in-flight request
    # is not holding up the connection.
    let initParams =
      LspInitializeParams %* {
        "processId": %getCurrentProcessId(),
        "rootUri": fixtureUri("projects/hw/"),
        "capabilities": {"window": {"workDoneProgress": true}},
      }
    discard waitFor client.initialize(initParams)

    # A file whose project never resolves, so the handler stays parked on it
    let uri = "file:///tmp/tcancel.nim"
    ls.openFiles[uri] = NlsFileInfo(projectFile: newFuture[string]("never"))

    let request = client.call("textDocument/definition", %positionParams(uri, 0, 0))
    waitFor sleepAsync(200)

    var id = 0'u
    for pendingId, pending in ls.pendingRequests:
      if pending.name == "textDocument/definition":
        id = pendingId
    check id != 0'u
    check ls.pendingRequests[id].state == prsOnGoing

    client.notify("$/cancelRequest", %*{"id": id.int})
    waitFor sleepAsync(200)

    check ls.pendingRequests[id].state == prsCancelled

    #The client is answered, so that it stops waiting on the request
    check request.failed
    check "-32800" in request.error.msg

  test "notifications are not tracked as pending requests":
    #They carry no id, so there is nothing to cancel or to report
    let before = ls.pendingRequests.len
    client.notify("$/setTrace", %*{"value": "verbose"})
    waitFor sleepAsync(200)
    check ls.pendingRequests.len == before

suite "Nimlangserver didOpen visibility":
  let cmdParams =
    CommandLineParams(mode: some lsp, transport: some socket, port: getNextFreePort())
  let ls = main(cmdParams)
  let client = newLspSocketClient()
  waitFor client.connect("localhost", cmdParams.port)
  client.registerNotification(
    "window/showMessage", "extension/statusUpdate", "textDocument/publishDiagnostics",
    "$/progress",
  )

  test "didOpen makes the file visible before it yields":
    # Requests are handled concurrently, and didOpen parks on ls.nimsuggestInit
    # before doing any real work. Unless the file is registered synchronously, a
    # request dispatched while didOpen is parked hits `uri notin ls.openFiles`
    # and answers nothing for a file the editor just opened.
    let initParams =
      LspInitializeParams %* {
        "processId": %getCurrentProcessId(),
        "rootUri": fixtureUri("projects/hw/"),
        "capabilities": {"window": {"workDoneProgress": true}},
      }
    discard waitFor client.initialize(initParams)

    ls.nimsuggestInit = newFuture[void]("parked") #didOpen cannot get past this
    let file = "projects/hw/hw.nim"
    client.notify("textDocument/didOpen", %createDidOpenParams(file))
    waitFor sleepAsync(200)

    let uri = fixtureUri(file)
    check uri in ls.openFiles #The entry the readers look for
    check ls.openFiles[uri].fingerTable.len > 0 #The contents were stashed too

suite "Nimlangserver didOpen ordering":
  let cmdParams =
    CommandLineParams(mode: some lsp, transport: some socket, port: getNextFreePort())
  let ls = main(cmdParams)
  let client = newLspSocketClient()
  waitFor client.connect("localhost", cmdParams.port)
  client.registerNotification(
    "window/showMessage", "extension/statusUpdate", "textDocument/publishDiagnostics",
    "$/progress",
  )

  test "a request sent right behind didOpen is answered against it":
    # The two messages go out back to back with nothing in between, so the only
    # thing that can make the request see the file is didOpen having finished
    # its synchronous part before the transport read the next message. Without
    # that, `tryGetNimsuggest` does not know the uri and the request is answered
    # with an empty result.
    let initParams =
      LspInitializeParams %* {
        "processId": %getCurrentProcessId(),
        "rootUri": fixtureUri("projects/hw/"),
        "capabilities": {"window": {"workDoneProgress": true}},
      }
    discard waitFor client.initialize(initParams)

    ls.nimsuggestInit = newFuture[void]("parked") #So didOpen gets no further
    let file = "projects/hw/hw.nim"
    let uri = fixtureUri(file)
    client.notify("textDocument/didOpen", %createDidOpenParams(file))
    let locations = to(
      waitFor client.call("textDocument/definition", %positionParams(uri, 1, 6)),
      seq[Location],
    )

    check locations.len == 1
    check locations[0].uri == uri

suite "Nimlangserver idle nimsuggest cleanup":
  let cmdParams = CommandLineParams(mode: some lsp, transport: some socket, port: getNextFreePort())
  let ls = main(cmdParams)
  let client = newLspSocketClient()
  waitFor client.connect("localhost", cmdParams.port)
  client.registerNotification(
    "window/showMessage", "window/workDoneProgress/create", "workspace/configuration",
    "extension/statusUpdate", "textDocument/publishDiagnostics", "$/progress",
  )

  test "idle nimsuggest is removed even when an open file was already evicted":
    # Regression test for #420: a URI evicted from ls.openFiles while the
    # nimsuggest still tracks it made removeIdleNimsuggests raise KeyError,
    # skipping project.stop()/projectFiles.del so the project was re-selected
    # for removal on every tick.
    let initParams =
      LspInitializeParams %* {
        "processId": %getCurrentProcessId(),
        "rootUri": fixtureUri("projects/hw/"),
        "capabilities":
          {"window": {"workDoneProgress": true}, "workspace": {"configuration": true}},
      }
    discard waitFor client.initialize(initParams)
    let conf = NlsConfig(nimsuggestIdleTimeout: some 1000)
    ls.workspaceConfiguration.complete(% @[conf])
    discard waitFor ls.workspaceConfiguration

    let helloWorldFile = "projects/hw/hw.nim"
    let hwAbsFile = uriToPath(helloWorldFile.fixtureUri())
    client.notify("textDocument/didOpen", %createDidOpenParams(helloWorldFile))
    check waitFor client.waitForNotificationMessage(
      fmt"Nimsuggest initialized for {hwAbsFile}"
    )
    ls.openFiles.del(helloWorldFile.fixtureUri())

    var removed = false
    for attempt in 0 ..< 5:
      waitFor sleepAsync(1100)
      waitFor ls.removeIdleNimsuggests()
      if hwAbsFile notin ls.projectFiles:
        removed = true
        break
    check removed
