## JSON-RPC transports for the language server: stdio and socket (TCP).
##
## Framing, routing, request/response correlation and error responses all come
## from `json_rpc`: the served connection becomes a bidirectional
## `RpcConnection` speaking the LSP `Content-Length` framing (see
## `stdioFraming` for the one exception), and incoming requests are dispatched
## through `ls.srv.router`. What is left here is the glue the language server
## needs on top of that:
##
## * `wrapRpc` adapts the handlers in `routes/` to `RpcProc`, since they take
##   the whole params object rather than one argument per member.
## * `route` dispatches each request off the read loop and records it, which
##   is what makes `$/cancelRequest` work.
## * `initActions` implements `ls.notify` / `ls.call` / `ls.onExit`.
##
## The two transports differ only in where the connection comes from: stdio
## serves the pipes the spawning client left on our own descriptors, the socket
## server serves every client that connects. `processStdioClient` and
## `processSocketClient` are the whole of that difference; the rest is shared.

import json_rpc/[servers/socketserver, clients/socketclient]
import json_rpc/[servers/stdioserver, clients/stdioclient]
import chronicles, chronos
import stew/byteutils
import std/times
import ls, utils
import protocol/[enums, types]

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
    ok JrpcSys.decode(params, RequestParamsRx).toTx
  except CatchableError as ex:
    err ex.msg

proc wrapRpc*[T](fn: proc(params: T): Future[auto] {.gcsafe, raises: [].}): Rpc =
  return proc(params: RequestParamsRx): Future[JsonString] {.gcsafe, async.} =
    let val = LspConv.decode(params.toJson, T)
    try:
      when typeof(fn(val)) is Future[void]: #Notification
        await fn(val)
        #The router only answers messages carrying an id, so this is dropped
        return JsonString("null")
      else:
        let res = await fn(val)
        return JsonString(LspConv.encode(res))
    except CancelledError:
      #`$/cancelRequest`. Answered with the code LSP reserves for it, otherwise
      #json-rpc reports the cancellation as an internal server error. The code
      #is inside the range json-rpc asks applications to stay out of, but it is
      #the one the LSP spec assigns and clients match on it.
      raise (ref ApplicationError)(
        code: ord(RequestCancelled), msg: "Request cancelled"
      )

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
          let uri = LspConv.decode(np.value, TextDocumentIdentifier).uri
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
    ls: LanguageServer, conn: RpcConnection, handled: Future[seq[byte]].Raising([])
) {.async: (raises: []).} =
  let res =
    try:
      await handled
    except CancelledError:
      #`wrapRpc` turns a cancelled handler into a response, so this only
      #happens if the routing itself is cancelled and nobody is owed an answer
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
    ls: LanguageServer, conn: RpcConnection, request: RequestBatchRx
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

proc register(ls: LanguageServer, conn: RpcConnection) =
  ## Makes the connection *the* client: `ls.notify` and `ls.call` talk to
  ## whatever is registered here.
  ls.srv.connections.incl(conn)
  ls.connection = conn

proc unregister(ls: LanguageServer, conn: RpcConnection) =
  ls.srv.connections.excl(conn)
  if ls.connection == conn:
    ls.connection = nil

proc processSocketClient(
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
  ls.register(conn)

  await conn.attach(transport, $remote)

  debug "Client disconnected", address = remote
  ls.unregister(conn)

proc recvJsonLine(
    transport: StreamTransport, limit: int
): Future[seq[byte]] {.async: (raises: [CancelledError, TransportError]).} =
  toBytes(await transport.readLine(limit, sep = "\n"))

proc sendJsonLine(
    transport: StreamTransport, msg: seq[byte]
) {.async: (raises: [CancelledError, TransportError]).} =
  discard await transport.write(msg & toBytes("\n"))

proc stdioFraming(ls: LanguageServer): Framing =
  ## LSP frames every message with a `Content-Length` header. MCP over stdio
  ## does not: it is one JSON object per line, which is what an agent spawning
  ## `--mcp --stdio` speaks. (Over a socket both modes use the LSP framing;
  ## MCP does not specify one there.)
  case ls.serverMode
  of lsp:
    Framing.httpHeader()
  of mcp:
    Framing.init(recvJsonLine, sendJsonLine)

proc processStdioClient(
    ls: LanguageServer, server: RpcStdioServer, input, output: StreamTransport
) {.async: (raises: []), gcsafe.} =
  ## The stdio counterpart of `processSocketClient`. There is nothing to accept:
  ## the client is the process that spawned us, and the connection is the pair
  ## of pipes it left on our standard descriptors.
  var conn: RpcStdioClient #Captured by the router, assigned right below
  conn = RpcStdioClient.new(
    framing = ls.stdioFraming(),
    router = proc(
        request: RequestBatchRx
    ): Future[seq[byte]] {.async: (raises: [], raw: true).} =
      ls.route(conn, request),
  )

  debug "Serving the client on stdio"
  ls.register(conn)

  await conn.attach(input, output, "stdio")

  debug "Client disconnected"
  ls.unregister(conn)

proc initActions*(ls: LanguageServer) =
  let onExit: OnExitCallback = proc() {.async.} =
    case ls.transportMode
    of stdio:
      await RpcStdioServer(ls.srv).stop()
    of socket:
      RpcSocketServer(ls.srv).stop()
      RpcSocketServer(ls.srv).close()

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
        fut.complete(LspConv.decode(res, JsonNode))
      except CatchableError as ex:
        error "Call to the client failed", name = name, err = ex.msg
        fut.fail ex

    asyncSpawn call()
    fut

  ls.call = callAction
  ls.notify = notifyAction
  ls.onExit = onExit

proc initServer*(ls: LanguageServer) =
  ## Creates the rpc server so that the routes can be registered on it, and
  ## hooks up `ls.notify` / `ls.call` / `ls.onExit`. Nothing is served yet.
  ls.srv =
    case ls.transportMode
    of stdio:
      newRpcStdioServer(partial(processStdioClient, ls))
    of socket:
      newRpcSocketServer(partial(processSocketClient, ls))
  ls.initActions()

proc startStdioServer*(ls: LanguageServer, input, output: StreamTransport) =
  ## Serves the connection on the given pair of pipes. Tests use it to serve a
  ## connection of their own making rather than the process' descriptors.
  RpcStdioServer(ls.srv).start(input, output)
  debug "Stdio server started"

proc startStdioServer*(ls: LanguageServer) =
  ## Serves the client that spawned us over its own pipes. Unlike a socket
  ## there is nothing to wait for: the connection exists from the start, so
  ## `ls.notify` and `ls.call` can be used as soon as this returns.
  RpcStdioServer(ls.srv).start()
  debug "Stdio server started"

proc startSocketServer*(ls: LanguageServer, port: Port) =
  let srv = RpcSocketServer(ls.srv)
  srv.addStreamServer("localhost", port)
  srv.start()

  proc waitUntilConnected(ls: LanguageServer) {.async.} =
    while ls.connection.isNil:
      await sleepAsync(0)

  when not defined(test):
    #`ls.notify` and `ls.call` need a client to talk to
    debug "Waiting for socket server to be ready"
    waitFor waitUntilConnected(ls)
    debug "Socket server started"

proc startServer*(ls: LanguageServer, port: Port) =
  case ls.transportMode
  of stdio:
    ls.startStdioServer()
  of socket:
    ls.startSocketServer(port)
