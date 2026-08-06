# mcp-desk — MCP tools, offline, end to end

```sh
go run ./examples/mcp-desk

go run ./examples/mcp-desk -state /tmp/loom-mcp    # first run: 6 tool calls
go run ./examples/mcp-desk -state /tmp/loom-mcp    # second run: 0 tool calls

go run ./examples/mcp-desk -view localhost:8077    # watch it
```

An inventory desk answering three questions. Every tool call goes over the
Model Context Protocol to a **real child process** — this same binary
re-executed with `-serve`, which is what a deployment does with an `npx` or
`uvx` command — over real pipes, with a real `initialize` handshake.

No API key, no network, no model: the "model" is `model.Mock`, the server is
`mcp/mcptest`.

## What it shows

**One connection.** Three records, two tool-calling stages, six calls, and the
report ends with `1 session(s), 1 dial(s)`. The connection is made once at
provisioning and every task leases a call slot from it — not a connection. Put
the same server on a `loom.Fleet` and every agent shares that one session and
one bound on how hard they may push it. See [docs/MCP.md](../../docs/MCP.md) for
why that is the right unit and why "a connection per partition" is the right
instinct with the wrong unit.

**A tool call is work, and Loom does not pay for it twice.** Run it twice with
`-state` and the second run makes zero tool calls and zero model calls: the tool
results are inside the cached stage artifacts. The `name` and `call` stages
carry `pipeline.WithVersion`, which is the author asserting that replaying a
lookup is as good as repeating it. A stage that *wrote* through a tool would
take `pipeline.WithNoCache` instead.

**Least privilege, unchanged.** The `name` stage declares
`pipeline.WithMCP("inventory", "lookup_sku")` and can call exactly that tool;
asking for `stock_level` from it is a permanent failure, audited. The grant is
an ordinary `security.ToolCap` — MCP needed no second permission mechanism.

**The model chooses a tool, and the choice is data.** `choose` is an ordinary
`Infer` stage that emits `{"tool": ..., "args": {...}}`; `call` is
`mcp.Dispatch`, which runs it. Two stages instead of an agent loop inside a
task, so the choice appears in the record, the lineage, and the constellation
view — and the call is scheduled, retried, and budgeted like everything else.
The tool list the model chooses from sits in the stage's `Prefix`, so it renders
once per task and the provider's prompt cache serves it for the rest of the
stage.

**A document behind the server becomes a broadcast.**
`loom.WithMCPResource("voice", ...)` reads `mem://voice` once at provisioning
and registers it by content hash. The `reply` stage reads it with
`{{broadcast "voice"}}` — one copy for the run, and part of that stage's
fingerprint.

## Expected output

```
stage                   tasks     ok   fail  retry  cache   tokens  prefix    cost($)        p95
questions                   0      0      0      0      0        0      0%     0.0000         0s
name                        3      3      0      0      0        0      0%     0.0000         0s
choose                      3      3      0      0      0      215     38%     0.0000         0s
call                        3      3      0      0      0        0      0%     0.0000         0s
reply                       3      3      0      0      0       71     51%     0.0000         0s
TOTAL                                                          286     41%     0.0000
mcp: 6 tool call(s), 2ms spent in them (no tokens, no cost)

q1     Widget: 17.
q2     Sprocket: 2026-08-19.
q3     Grommet: 4.

mcp inventory: 1 session(s), 1 dial(s), 6 call(s), 2ms busy
```

The `mcp:` line is separate from the cost table on purpose — tool calls are the
only work in Loom that costs no tokens, so a stage can be slow for a reason the
dollar column will never show.
