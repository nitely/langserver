import
  std/[options, json, os, sequtils, tables],
  unittest2,
  chronos,
  ../[nimlangserver, ls, suggestapi, utils],
  ../protocol/[types],
  ./[lspsocketclient, testhelpers]

const
  RootFile = "projects/hw/hw.nim"
  ConfigurationDelay = 750.milliseconds

proc answerNull(params: JsonNode): Future[JsonNode] {.async.} =
  newJNull()

proc slowConfiguration(exceptionHints: bool): Rpc =
  return proc(params: JsonNode): Future[JsonNode] {.async.} =
    await sleepAsync(ConfigurationDelay)
    %*[
      {
        "autoCheckFile": false,
        "autoCheckProject": false,
        "inlayHints": {"exceptionHints": {"enable": exceptionHints}},
      }
    ]

proc initParams(): LspInitializeParams =
  LspInitializeParams %* {
    "processId": %getCurrentProcessId(),
    "rootUri": fixtureUri("projects/hw/"),
    "capabilities":
      {"window": {"workDoneProgress": false}, "workspace": {"configuration": true}},
  }

proc startServer(exceptionHints: bool): (LanguageServer, LspSocketClient) =
  let cmdParams =
    CommandLineParams(mode: some lsp, transport: some socket, port: getNextFreePort())
  let ls = main(cmdParams)
  let client = newLspSocketClient()
  client.registerNotification(
    "window/showMessage", "extension/statusUpdate", "textDocument/publishDiagnostics",
    "$/progress",
  )
  client.register("workspace/configuration", slowConfiguration(exceptionHints))
  client.register("client/registerCapability", answerNull)
  client.register("window/workDoneProgress/create", answerNull)
  waitFor client.connect("localhost", cmdParams.port)
  (ls, client)

proc openRootFile(ls: LanguageServer, client: LspSocketClient): Nimsuggest =
  discard waitFor client.initialize(initParams())
  client.notify("initialized", newJObject())
  client.notify("textDocument/didOpen", %createDidOpenParams(RootFile))
  doAssert waitUntil(ls.projectFiles.len == 1, 60.seconds)
  let projectFile = ls.projectFiles.keys.toSeq[0]
  doAssert waitUntil(ls.projectFiles[projectFile].ns != nil, 60.seconds)
  ls.projectFiles[projectFile].ns

suite "Nimsuggest is started with the client configuration":
  test "a file opened before the configuration arrives still honors it":
    let (ls, client) = startServer(exceptionHints = false)
    defer:
      waitFor ls.stopNimsuggestProcesses()

    let ns = ls.openRootFile(client)
    check nsExceptionInlayHints in ns.capabilities
    check "--exceptionInlayHints:off" in ns.startArgs
    check "--exceptionInlayHints:on" notin ns.startArgs
    check not ls.getWorkspaceConfiguration.exceptionHintsEnabled

  test "exception inlay hints are enabled when the configuration asks for them":
    let (ls, client) = startServer(exceptionHints = true)
    defer:
      waitFor ls.stopNimsuggestProcesses()

    let ns = ls.openRootFile(client)
    check "--exceptionInlayHints:on" in ns.startArgs
    check "--exceptionInlayHints:off" notin ns.startArgs
    check ls.getWorkspaceConfiguration.exceptionHintsEnabled
