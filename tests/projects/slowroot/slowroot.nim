# The nimble entry point. Slow to compile, so the server's startup
# (`initNimsuggestInstances`) is still running when the editor opens files.
const busy = block:
  var x = 0
  for i in 0 ..< 20_000_000:
    x = x xor i
  x

proc entry*(): int =
  busy
