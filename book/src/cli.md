# Command-Line Reference

You don't normally launch `nimlangserver` by hand — editors and AI tools start it automatically based on their configuration. This page is a reference for when you do need direct control: debugging, socket mode, scripting, or integrating with a tool not covered by the existing configs.

## Synopsis

```
nimlangserver [options]
```

## Options

| Option | Description |
|---|---|
| `--lsp` | Run in LSP server mode. This is the default. |
| `--mcp` | Run in MCP server mode. |
| `--socket` | Use socket transport. This is currently the only transport. |
| `--port=<port>` | Port to listen on when using socket transport. If omitted, a free port is chosen automatically and printed to the console. |
| `--clientProcessId=<pid>` | Exit automatically when the process with the given PID terminates. Editors pass this to tie the server lifetime to their own. |
| `--version`, `-v` | Print version information and exit. |
| `--help`, `-h` | Print a help message and exit. |

## Mode and transport combinations

```bash
nimlangserver                          # LSP over socket, auto port
nimlangserver --lsp --socket --port=6000

nimlangserver --mcp                    # MCP over socket, auto port
nimlangserver --mcp --socket --port=6001
```

The chosen port is printed on stdout as `port=<port>` before the server starts
listening.

> **The stdio transport has been removed for the time being.** `--stdio` now
> exits with an error. Only the socket transport is supported, so a client that
> launches `nimlangserver` as a subprocess has to read the port off stdout and
> connect to it.
