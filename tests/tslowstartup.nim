import ../[nimlangserver, ls, utils]
import ../protocol/types
import std/[options, json, os, sequtils, strutils, tables]
import chronos
import lspsocketclient
import unittest2

# Reproduces the Constantine issue (#436, #453) without Constantine: a nimble
# project whose nimsuggest roots are slow to compile (see
# tests/projects/slowroot). While the server starts up (`nimsuggestInit`:
# `nimble dump`, then nimsuggest for the nimble entry point) the editor is
# already opening and querying files.

const CallTimeout = 60.seconds

template eventually(cond: untyped, timeout = 10.seconds): bool =
  block:
    let deadline = Moment.now() + timeout
    var satisfied = false
    while true:
      if cond:
        satisfied = true
        break
      if Moment.now() > deadline:
        break
      waitFor sleepAsync(50.milliseconds)
    satisfied

let
  entryPath = uriToPath(fixtureUri("projects/slowroot/slowroot.nim"))
  rootFile = "projects/slowroot/mappedroot.nim"
  otherFile = "projects/slowroot/other.nim"
  rootPath = uriToPath(fixtureUri(rootFile))
  otherUri = fixtureUri(otherFile)

proc mappedConfiguration(params: JsonNode): Future[JsonNode] {.async.} =
  # `other.nim` and `mappedroot.nim` belong to `mappedroot.nim`, a root that is
  # not the nimble entry point, like Constantine's mapped roots.
  return %*[
    {
      "projectMapping":
        [{"projectFile": "mappedroot.nim", "fileRegex": "(other|mappedroot)\\.nim$"}],
      "maxNimsuggestProcesses": 0,
      "autoCheckFile": false,
      "autoCheckProject": false,
    }
  ]

proc defaultConfiguration(params: JsonNode): Future[JsonNode] {.async.} =
  return %*[{"autoCheckFile": false, "autoCheckProject": false}]

proc answerNull(params: JsonNode): Future[JsonNode] {.async.} =
  newJNull()

proc startSlowRootServer(configuration: Rpc): (LanguageServer, LspSocketClient) =
  let cmdParams =
    CommandLineParams(mode: some lsp, transport: some socket, port: getNextFreePort())
  let ls = main(cmdParams)
  let client = newLspSocketClient()
  client.registerNotification(
    "window/showMessage", "extension/statusUpdate", "textDocument/publishDiagnostics",
    "$/progress",
  )
  client.register("workspace/configuration", configuration)
  client.register("client/registerCapability", answerNull)
  client.register("window/workDoneProgress/create", answerNull)

  waitFor client.connect("localhost", cmdParams.port)
  discard waitFor client.initialize(
    LspInitializeParams %* {
      "processId": %getCurrentProcessId(),
      "rootUri": fixtureUri("projects/slowroot/"),
      "capabilities":
        {"window": {"workDoneProgress": false}, "workspace": {"configuration": true}},
    }
  )
  client.notify("initialized", newJObject())
  doAssert eventually(ls.workspaceConfiguration.finished)
  (ls, client)

proc documentSymbols(client: LspSocketClient, file: string): Future[JsonNode] =
  client.call(
    "textDocument/documentSymbol", %*{"textDocument": {"uri": fixtureUri(file)}}
  )

suite "Nimsuggest startup for a slow mapped root":
  let (ls, client) = startSlowRootServer(mappedConfiguration)

  proc initializedMessages(): int =
    client.calls["window/showMessage"].countIt(
      it["message"].getStr == "Nimsuggest initialized for " & rootPath
    )

  suiteTeardown:
    waitFor ls.stopNimsuggestProcesses()

  test "files opened during startup share a single nimsuggest for their root":
    # Both opens get to start nimsuggest for the mapped root at about the same
    # time. A root is only registered in `projectFiles` after nimsuggest's
    # initial compilation, so each of them used to start its own, and the last
    # one stopped the others.
    check not ls.nimsuggestInit.finished
    client.notify("textDocument/didOpen", %createDidOpenParams(otherFile))
    client.notify("textDocument/didOpen", %createDidOpenParams(rootFile))

    check eventually(initializedMessages() >= 1, CallTimeout)
    # A duplicate would either be reported as initialized too, or be stopped by
    # the next one and show up as a failure for the root.
    check not eventually(initializedMessages() > 1, 3.seconds)
    check ls.failTable.getOrDefault(rootPath, 0) == 0
    check rootPath in ls.projectFiles

    # The nimsuggest that is left is the one serving the files.
    let symbols = waitFor client.documentSymbols(otherFile).wait(CallTimeout)
    check symbols.getElems.anyIt(it["name"].getStr == "other")

suite "Requests during startup":
  let (ls, client) = startSlowRootServer(mappedConfiguration)

  suiteTeardown:
    waitFor ls.stopNimsuggestProcesses()

  test "a request right behind didOpen waits for the file instead of answering empty":
    # The editor has done nothing wrong: the file was opened before it was
    # queried. The server must not answer as if the file were unknown just
    # because it is still starting up.
    check not ls.nimsuggestInit.finished
    client.notify("textDocument/didOpen", %createDidOpenParams(otherFile))
    let symbols = waitFor client.documentSymbols(otherFile).wait(CallTimeout)
    check symbols.getElems.anyIt(it["name"].getStr == "other")

suite "Project resolution during startup":
  let (ls, client) = startSlowRootServer(defaultConfiguration)

  suiteTeardown:
    waitFor ls.stopNimsuggestProcesses()

  test "a file opened during startup uses the nimsuggest the startup brings up":
    # With the default `maxNimsuggestProcesses: 1` a file without a mapping is
    # served by the nimsuggest already running. Resolving its project before
    # the startup has registered that nimsuggest sees none running, falls back
    # to the file itself, and starts a second one.
    check not ls.nimsuggestInit.finished
    client.notify("textDocument/didOpen", %createDidOpenParams(otherFile))

    check eventually(
      otherUri in ls.openFiles and ls.openFiles[otherUri].projectFile.finished and
        ls.nimsuggestInit.finished,
      CallTimeout,
    )
    # Give a wrongly resolved project the time to start its own nimsuggest.
    check not eventually(ls.projectFiles.len > 1, 8.seconds)
    check (waitFor ls.openFiles[otherUri].projectFile) == entryPath
    check toSeq(ls.projectFiles.keys) == @[entryPath]
