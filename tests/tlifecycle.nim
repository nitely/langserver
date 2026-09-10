## Process level tests: they spawn the real `nimlangserver` binary and assert
## how it terminates. None of this can be checked in process, since the server
## ends by quitting, which would take the test runner with it.

import std/[json, os, osproc, strformat, strutils, streams, net]
import unittest2
import ../utils
import lspsocketclient

const
  CRLF = "\r\n"
  ServerSource = "nimlangserver.nim"
  ServerBinary = "tests" / "nimlangserver_lifecycle".addFileExt(ExeExt)
  SigSegv = 128 + 11

proc frame(msg: JsonNode): string =
  let body = $msg
  &"Content-Length: {body.len}{CRLF}{CRLF}{body}"

proc initializeMsg(rootUri: JsonNode, pullConfiguration: bool): JsonNode =
  let capabilities =
    if pullConfiguration:
      %*{"workspace": {"configuration": true}}
    else:
      newJObject()
  %*{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
      "processId": newJNull(),
      "rootUri": rootUri,
      "workspaceFolders": newJNull(),
      "capabilities": capabilities,
    },
  }

proc exitCodeWithin(p: Process, timeoutMs: int): int =
  ## The exit code, or -1 if the process outlived the timeout. A process killed
  ## by a signal reports 128 + the signal, so a crash is visible here.
  var waited = 0
  while p.running() and waited < timeoutMs:
    sleep(50)
    waited += 50
  if p.running():
    p.kill()
    discard p.waitForExit()
    return -1
  p.peekExitCode()

proc startServer(args: seq[string]): Process =
  startProcess(ServerBinary.absolutePath, args = args, options = {poStdErrToStdOut})

suite "Nimlangserver process lifecycle":
  # Required for the rest of the tests
  test "the server binary has been built":
    let res = execCmdEx(&"nim c --hints:off -o:{ServerBinary} {ServerSource}")
    if res.exitCode != 0:
      checkpoint "nimlangserver build output: " & res.output
      fail()

  test "stdio: closing stdin exits cleanly":
    let p = startServer(@["--stdio"])
    p.inputStream.write(frame(initializeMsg(newJNull(), false)))
    p.inputStream.flush()
    sleep(500)
    p.inputStream.close()
    check p.exitCodeWithin(30_000) == 0

  test "stdio: the exit notification exits cleanly":
    let p = startServer(@["--stdio"])
    p.inputStream.write(frame(initializeMsg(newJNull(), false)))
    p.inputStream.flush()
    sleep(500)
    p.inputStream.write(frame(%*{"jsonrpc": "2.0", "id": 2, "method": "shutdown"}))
    p.inputStream.write(frame(%*{"jsonrpc": "2.0", "method": "exit"}))
    p.inputStream.flush()
    check p.exitCodeWithin(30_000) == 0

  test "stdio: a malformed frame is reported as a failure":
    let p = startServer(@["--stdio"])
    p.inputStream.write(&"Content-Length: -5{CRLF}{CRLF}xxxxx")
    p.inputStream.flush()
    p.inputStream.close()
    check p.exitCodeWithin(30_000) == 1

  test "stdio: a body shorter than the declared length ends the session":
    let p = startServer(@["--stdio"])
    let body = """{"jsonrpc":"2.0","id":1,"method":"shutdown"}"""
    p.inputStream.write(&"Content-Length: 500{CRLF}{CRLF}{body}")
    p.inputStream.flush()
    p.inputStream.close()
    check p.exitCodeWithin(30_000) == 0

  test "stdio: leaving with a request pending does not crash the server":
    ## The client asks for `workspace/configuration` to be pulled, then leaves
    ## without answering, while nimsuggest is being created. json-rpc fails the
    ## in flight call on disconnect; that failure used to unwind out of a
    ## nested `waitFor` and take the process down with SIGSEGV.
    let p = startServer(@["--stdio"])
    p.inputStream.write(frame(initializeMsg(%fixtureUri("projects/hw/"), true)))
    p.inputStream.write(frame(%*{"jsonrpc": "2.0", "method": "initialized"}))
    p.inputStream.flush()
    sleep(1500)
    p.inputStream.close()

    let code = p.exitCodeWithin(60_000)
    check code != SigSegv
    check code == 0

  test "socket: leaving with a request pending does not crash the server":
    let port = getNextFreePort()
    let p = startServer(@["--socket", &"--port={port.int}"])
    sleep(1000)
    var socket = newSocket()
    socket.connect("localhost", port)
    socket.send(frame(initializeMsg(%fixtureUri("projects/hw/"), true)))
    socket.send(frame(%*{"jsonrpc": "2.0", "method": "initialized"}))
    sleep(1500)
    socket.close()

    let code = p.exitCodeWithin(60_000)
    check code != SigSegv
