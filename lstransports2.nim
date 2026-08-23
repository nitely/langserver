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
## * `addRpcToCancellable` records in-flight requests so `$/cancelRequest` can
##   cancel them.
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

proc wrapRpc*[T](
    fn: proc(params: T, id: int): Future[auto] {.gcsafe, raises: [].}
): Rpc =
  return proc(params: RequestParamsRx): Future[JsonString] {.gcsafe, async.} =
    let val = LspConv.decode(params.toJson, T)
    var idRequest = 0
    try:
      idRequest = get[int](params, "idRequest")
    except KeyError:
      error "IdRequest not found in the request params", params = params
    let res = await fn(val, idRequest)
    return JsonString(LspConv.encode(res))

proc addRpcToCancellable*(ls: LanguageServer, rpc: Rpc): Rpc =
  return proc(params: RequestParamsRx): Future[JsonString] {.gcsafe, raises: [].} =
    try:
      let idRequest = get[uint](params, "idRequest")
      let name = get[string](params, "method")
      ls.pendingRequests[idRequest] =
        PendingRequest(id: idRequest, name: name, startTime: now(), state: prsOnGoing)
      ls.sendStatusChanged
      var fut = rpc(params)
      ls.pendingRequests[idRequest].request = fut
        #we need to add it before because the rpc may access to the pendingRequest to set the projectFile
      fut.addCallback proc(d: pointer) =
        try:
          ls.pendingRequests[idRequest].state = prsComplete
          ls.pendingRequests[idRequest].endTime = now()
          ls.sendStatusChanged
        except KeyError:
          error "Error completing pending requests. Id not found in pending requests"
      return fut
    except KeyError as ex:
      error "IdRequest not found in the request params"
      writeStackTrace(ex)
    except Exception as ex:
      error "Error adding request to cancellable requests"
      writeStackTrace(ex)

#
# Server
#

proc addLspParams(req: var RequestRx2) =
  ## An `Rpc` only receives the params, but `wrapRpc` and
  ## `addRpcToCancellable` also need the request id and the method name, so
  ## pass them along as extra params. They are ignored when decoding the
  ## handler's own params.
  if req.params.kind != rpNamed:
    return
  let id = req.id.valueOr:
    return
  if id.kind != riNumber:
    return
  req.params.named.add ParamDescNamed(name: "idRequest", value: JsonString($id.num))
  req.params.named.add ParamDescNamed(
    name: "method", value: JsonString(escapeJson(req.meth))
  )

func withLspParams(request: sink RequestBatchRx): RequestBatchRx =
  result = request
  case result.kind
  of rbkSingle:
    result.single.addLspParams()
  of rbkMany:
    for req in result.many.mitems:
      req.addLspParams()

proc processClient(
    ls: LanguageServer, server: StreamServer, transport: StreamTransport
) {.async: (raises: []), gcsafe.} =
  let
    remote = transport.remoteAddress2().valueOr(default(TransportAddress))
    conn = RpcSocketClient.new(
      framing = Framing.httpHeader(),
      router = proc(
          request: RequestBatchRx
      ): Future[seq[byte]] {.async: (raises: [], raw: true).} =
        ls.srv.router.route(request.withLspParams),
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
