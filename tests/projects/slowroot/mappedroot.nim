import other

# The root `projectMapping` points files at, separate from the nimble entry
# point, like Constantine's mapped roots. Its initial nimsuggest compilation is
# slow too. It has to be VM work: nimsuggest does not run `staticExec`, but it
# evaluates this before it reports its port.
const busy = block:
  var x = 0
  for i in 0 ..< 20_000_000:
    x = x xor i
  x

proc root*(): int =
  other() + busy
