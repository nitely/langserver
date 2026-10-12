import
  std/[options, os, tables, json],
  chronos,
  chronos/asyncproc,
  unittest2,
  ../[nimlangserver, ls, utils],
  ../suggestapi,
  ../protocol/types,
  ./[lspsocketclient, testhelpers]

import ../routes/lsp as lspRoutes

suite "Restart nimsuggest on save":
  let cmdParams =
    CommandLineParams(mode: some lsp, transport: some socket, port: getNextFreePort())
  let ls = main(cmdParams)
  let client = newLspSocketClient()
  waitFor client.connect("localhost", cmdParams.port)
  client.registerNotification(
    "window/showMessage", "window/workDoneProgress/create", "workspace/configuration",
    "extension/statusUpdate", "textDocument/publishDiagnostics", "$/progress",
  )
  discard waitFor client.initialize(
    LspInitializeParams %* {
      "processId": %getCurrentProcessId(),
      "rootUri": fixtureUri("projects/hw/"),
      "capabilities":
        {"window": {"workDoneProgress": false}, "workspace": {"configuration": true}},
    }
  )

  let
    helloWorldFile = "projects/hw/hw.nim"
    helloWorldUri = fixtureUri(helloWorldFile)
    helloWorldPath = uriToPath(helloWorldUri)
    saveParams = DidSaveTextDocumentParams(
      textDocument: TextDocumentIdentifier(uri: helloWorldUri)
    )

  suiteTeardown:
    waitFor ls.shutdownNimsuggest()

  proc isRunning(project: Project): bool =
    project.process.running().get(false)

  let textDocument = TextDocumentItem(
    uri: helloWorldUri,
    languageId: "nim",
    version: 0,
    text: readFile("tests" / helloWorldFile),
  )
  ls.setWorkspaceConfiguration(% @[NlsConfig()])
  check waitFor ls.didOpenFile(textDocument).withTimeout(30.seconds)

  test "saving keeps the same nimsuggest by default":
    ls.setWorkspaceConfiguration(% @[NlsConfig()])
    let before = ls.projectFiles[helloWorldPath]
    check waitFor lspRoutes.didSave(ls, saveParams).withTimeout(30.seconds)
    check ls.projectFiles[helloWorldPath] == before
    check before.isRunning

  test "saving replaces nimsuggest and stops the old process":
    ls.setWorkspaceConfiguration(% @[NlsConfig(nimsuggestRestartOnSave: some true)])
    let before = ls.projectFiles[helloWorldPath]
    check waitFor lspRoutes.didSave(ls, saveParams).withTimeout(30.seconds)
    check waitUntil(ls.projectFiles[helloWorldPath] != before, timeout = 60.seconds)
    let after = ls.projectFiles[helloWorldPath]
    check after.isRunning
    check waitUntil(not before.isRunning, timeout = 30.seconds)

  test "a save during a restart restarts again once that restart is done":
    # The first restart may have read the file before the second save wrote
    # it, so joining that restart would leave a nimsuggest built from stale
    # sources.
    let
      before = ls.projectFiles[helloWorldPath]
      first = ls.restartNimsuggestOnSave(helloWorldPath, helloWorldUri)
      second = ls.restartNimsuggestOnSave(helloWorldPath, helloWorldUri)
    check waitFor first.withTimeout(60.seconds)
    let afterFirst = ls.projectFiles[helloWorldPath]
    check afterFirst != before
    check waitFor second.withTimeout(60.seconds)
    let afterSecond = ls.projectFiles[helloWorldPath]
    check afterSecond != afterFirst
    check afterSecond.isRunning
    check waitUntil(not afterFirst.isRunning, timeout = 30.seconds)
