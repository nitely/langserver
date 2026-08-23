## Socket (TCP) JSON-RPC transport for the language server.
##
## Framing, routing, request/response correlation and error responses all come
## from `json_rpc`: an accepted connection becomes a bidirectional
## `RpcSocketClient` speaking the LSP `Content-Length` framing, and incoming
## requests are dispatched through `ls.srv.router`. What is left here is the
## glue the language server needs on top of that:
##
## * `wrapRpc` adapts the handlers in `routes/` to `RpcProc`, since they take
##   the whole params object rather than one argument per member.
## * `route` dispatches each request off the read loop and records it, which
##   is what makes `$/cancelRequest` work.
## * `initActions` implements `ls.notify` / `ls.call` / `ls.onExit`.

import json_rpc/[servers/socketserver, clients/socketclient]
import chronicles, chronos
import std/times
import ls, utils
import protocol/types

logScope:
  topics = "lstransport"

type Rpc* = proc(params: RequestParamsRx): Future[JsonString] {.gcsafe, raises: [].}

func toJson(params: RequestParamsRx): string =
  ## LSP passes a single structured object as `params`, which json-rpc splits
  ## into one named param per member. Put it back together.
  if params.kind == rpPositional:
    # Neither LSP nor MCP use positional params, but Copilot CLI sends no
    # params at all for tools/list, which decodes as an empty positional list.
    doAssert params.positional.len == 0
    "{}"
  else:
    JrpcSys.encode(params.toTx)

func toParams(params: JsonString): Result[RequestParamsTx, string] =
  ## The inverse of `toJson`. An empty object means no params at all: json-rpc
  ## leaves the member out of the message.
  try:
    ok JrpcSys.decode(params.string, RequestParamsRx).toTx
  except CatchableError as ex:
    err ex.msg

proc wrapRpc*[T](fn: proc(params: T): Future[auto] {.gcsafe, raises: [].}): Rpc =
  return proc(params: RequestParamsRx): Future[JsonString] {.gcsafe, async.} =
    let val = LspConv.decode(params.toJson, T)
    when typeof(fn(val)) is Future[void]: #Notification
      await fn(val)
      #The router only answers messages carrying an id, so this is dropped
      return JsonString("null")
    else:
      let res = await fn(val)
      return JsonString(LspConv.encode(res))

#
# Server
#

proc trackRequest(
    ls: LanguageServer, request: RequestBatchRx, fut: FutureBase
) {.raises: [].} =
  ## Records an in-flight request so that `$/cancelRequest` can cancel it and
  ## the `extension/status` view can show what the server is busy with.
  if request.kind != rbkSingle:
    return
  let req = request.single
  let id = req.id.valueOr:
    return #A notification, there is nothing to cancel or report
  if id.kind != riNumber:
    return

  let reqId = id.num.uint
  ls.pendingRequests[reqId] = PendingRequest(
    id: reqId, name: req.meth, startTime: now(), state: prsOnGoing, request: fut
  )
  ls.sendStatusChanged

  #Which project the request is waiting on, for the status view
  if req.params.kind == rpNamed:
    for np in req.params.named:
      if np.name == "textDocument":
        try:
          let uri = LspConv.decode(np.value.string, TextDocumentIdentifier).uri
          asyncSpawn ls.addProjectFileToPendingRequest(reqId, uri)
        except CatchableError as ex:
          error "Cannot read the request textDocument", err = ex.msg
        break

  fut.addCallback proc(_: pointer) =
    try:
      ls.pendingRequests[reqId].state = prsComplete
      ls.pendingRequests[reqId].endTime = now()
      ls.sendStatusChanged
    except KeyError:
      error "Cannot complete the pending request, id not found", id = reqId

proc respond(
    ls: LanguageServer, conn: RpcSocketClient, handled: Future[seq[byte]].Raising([])
) {.async: (raises: []).} =
  let res =
    try:
      await handled
    except CancelledError:
      #Cancelled through `$/cancelRequest`, the client is no longer waiting
      return
  if res.len == 0: #A notification, the client expects no answer
    return
  try:
    await conn.send(res)
  except CancelledError:
    discard
  except JsonRpcError as ex:
    error "Cannot send response", err = ex.msg

proc route(
    ls: LanguageServer, conn: RpcSocketClient, request: RequestBatchRx
): Future[seq[byte]] {.async: (raises: [], raw: true).} =
  ## json-rpc awaits whatever this returns before it reads the next message off
  ## the connection, so hand back an empty response straight away and let the
  ## request run on its own. Handling it here instead would stall the whole
  ## connection for the duration, and `$/cancelRequest` could never be read
  ## while the request it cancels is still running.
  ##
  ## Messages are therefore handled concurrently, with one ordering guarantee:
  ## chronos runs an async body up to its first `await`, and the way down to the
  ## handler does not suspend, so whatever a handler does before it yields is
  ## done before the next message is read. Handlers have to hold up their end of
  ## that: anything a following message could look at has to be applied in that
  ## prefix. `ls.registerOpenFile` is the part of opening a file that exists for
  ## this reason.
  let handled = ls.srv.router.route(request)
  ls.trackRequest(request, handled)
  asyncSpawn ls.respond(conn, handled)

  result = Future[seq[byte]].Raising([]).init(
    "lstransport.route", {FutureFlag.OwnCancelSchedule}
  )
  result.complete(default(seq[byte]))

proc processClient(
    ls: LanguageServer, server: StreamServer, transport: StreamTransport
) {.async: (raises: []), gcsafe.} =
  let remote = transport.remoteAddress2().valueOr(default(TransportAddress))
  var conn: RpcSocketClient #Captured by the router, assigned right below
  conn = RpcSocketClient.new(
    framing = Framing.httpHeader(),
    router = proc(
        request: RequestBatchRx
    ): Future[seq[byte]] {.async: (raises: [], raw: true).} =
      ls.route(conn, request),
  )

  debug "Client connected", address = remote
  ls.srv.connections.incl(conn)
  ls.connection = conn

  await conn.attach(transport, $remote)

  debug "Client disconnected", address = remote
  ls.srv.connections.excl(conn)
  if ls.connection == conn:
    ls.connection = nil

proc initActions*(ls: LanguageServer) =
  let onExit: OnExitCallback = proc() {.async.} =
    ls.srv.stop()
    ls.srv.close()

  let notifyAction: NotifyAction = proc(name: string, params: JsonString) =
    #Not `ls.srv.notify`, that one goes out to every connected client
    let conn = ls.connection
    if conn.isNil:
      return
    let reqParams = params.toParams.valueOr:
      error "Cannot encode the notification params", name = name, err = error
      return

    proc send() {.async: (raises: []).} =
      try:
        await conn.notify(name, reqParams)
      except CancelledError:
        discard
      except JsonRpcError as ex:
        error "Cannot send notification", name = name, err = ex.msg

    asyncSpawn send()

  let callAction: CallAction = proc(name: string, params: JsonString): Future[JsonNode] =
    let fut = newFuture[JsonNode]("ls.call")
    let conn = ls.connection
    if conn.isNil:
      fut.fail newException(JsonRpcError, "No client connected")
      return fut
    let reqParams = params.toParams.valueOr:
      fut.fail newException(JsonRpcError, "Cannot encode the request params: " & error)
      return fut

    proc call() {.async: (raises: []).} =
      try:
        let res = await conn.call(name, reqParams)
        fut.complete(LspConv.decode(res.string, JsonNode))
      except CatchableError as ex:
        error "Call to the client failed", name = name, err = ex.msg
        fut.fail ex

    asyncSpawn call()
    fut

  ls.call = callAction
  ls.notify = notifyAction
  ls.onExit = onExit

proc initSocketServer*(ls: LanguageServer) =
  ## Creates the rpc server so that the routes can be registered on it, and
  ## hooks up `ls.notify` / `ls.call` / `ls.onExit`. Nothing is listening yet.
  ls.srv = newRpcSocketServer(partial(processClient, ls))
  ls.initActions()

proc startSocketServer*(ls: LanguageServer, port: Port) =
  ls.srv.addStreamServer("localhost", port)
  ls.srv.start()

  proc waitUntilConnected(ls: LanguageServer) {.async.} =
    while ls.connection.isNil:
      await sleepAsync(0)

  when not defined(test):
    #`ls.notify` and `ls.call` need a client to talk to
    debug "Waiting for socket server to be ready"
    waitFor waitUntilConnected(ls)
    debug "Socket server started"
