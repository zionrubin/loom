# Giving a stage more than the record

Everything here follows one rule: a stage gets what it *declares*, and
declaring is what produces the grant, the egress entry, and the fingerprint
component. A name nobody registered fails compilation rather than the first
record that reaches it, and an undeclared read is a permanent failure rather
than a silent success.

- [Broadcasts](#broadcasts)
- [Context fragments](#context-fragments)
- [Tools](#tools)
- [MCP servers](#mcp-servers)
- [Secrets and egress](#secrets-and-egress)
- [Sandboxing](#sandboxing)

## Broadcasts

A value registered once for the whole run — a lookup table, a taxonomy, a
rubric, a policy document. Stored once by content hash; task envelopes carry
the *hash*, not the bytes, so a large shared table costs one copy per run
instead of one per task and the tasks stay small enough to ship to a remote
executor.

```go
// Run side: register it.
loom.WithBroadcast("policy", policyText)
loom.WithBroadcast("catalog", map[string]map[string]string{...})

// Stage side: declare it, then read it.
```

From a prompt template (the stage must declare it):

```go
src.Infer("classify", pipeline.InferSpec{
    Prefix: `{{broadcast "policy"}}` + "\n",
    Prompt: `Ticket: {{.text}}`,
}, pipeline.WithBroadcast("policy"))
```

From Go, via `MapTools`:

```go
src.MapTools("enrich", func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
    cat, err := core.BroadcastAs[map[string]map[string]string](ctx, s, "catalog")
    if err != nil {
        return core.Record{}, err
    }
    r.Data["product"] = cat[r.String("sku")]["name"]
    return r, nil
}, pipeline.WithBroadcast("catalog"), pipeline.WithVersion("v1"))
```

Template functions: `{{broadcast "name"}}` (the value — index it for
structured data) and `{{broadcastJSON "name"}}` (indented JSON).

Three things worth internalizing:

- **Values are read-only.** Every reader in the run sees the same value;
  mutating it is a data race. `core.BroadcastAs[T]` gives you an
  independently-owned typed copy, which is what you want in Go code. It exists
  because broadcasts make a JSON round trip into the store, so a
  `map[string]string` comes back as `map[string]any`.
- **Editing a broadcast invalidates exactly the stages that read it.** The
  content hash is part of each reading stage's fingerprint, so the rest of the
  cache stays warm. `examples/support-desk` demonstrates this by rerunning with
  an edited policy.
- **Reading an undeclared broadcast is a permanent failure**, and a name that
  was never registered fails at compile time with the stage named.

## Context fragments

`InferSpec.Context []task.Fragment` delivers named documents into the task
envelope, prefixed to the prompt — the exact context the task needs, no more:

```go
Context: []task.Fragment{{Name: "schema", Content: schemaText}},
```

Use a fragment for something specific to *this stage*, and a broadcast for
something shared across stages or large enough that per-task copies matter.
Fragments join the stage fingerprint too.

## Tools

Register an implementation on the run, grant it to the stage, invoke it from
`MapTools`:

```go
type Tool interface {
    Name() string
    Invoke(ctx context.Context, args map[string]any) (any, error)
}
// Implement NetworkTool (adds Endpoint() string) and the host is checked
// against the task's egress allowlist before every call.

loom.WithTools(myTool)

d.MapTools("lookup", func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
    out, err := s.Invoke(ctx, "my-tool", map[string]any{"q": r.String("q")})
    ...
}, pipeline.WithGrants(security.ToolCap("my-tool")), pipeline.WithVersion("v1"))
```

Every invocation is capability-checked against the task's grants and lands in
`res.Audit`.

## MCP servers

Connections are made once, at provisioning, before anything runs — shared by
every stage and every agent on a fleet. A misconfigured server therefore fails
the run at startup rather than at the first record, and no task pays for a
handshake. What a task gets is a *lease on a call slot*, bounded per server.

```go
// Run side.
loom.WithMCPServer(
    mcp.Stdio("inventory", "./bin/inventory-server").
        WithTools("lookup_sku", "stock_level"),
    mcp.HTTP("search", "https://mcp.example.com").WithAuth(secretRef),
)

// Stage side: declare the server and (ideally) the exact tools.
pipeline.WithMCP("inventory", "lookup_sku")   // narrow — prefer this
pipeline.WithMCP("inventory")                  // every tool the server offers
```

Call a tool by qualified name:

```go
out, err := s.Invoke(ctx, mcp.ToolName("inventory", "lookup_sku"),
    map[string]any{"sku": r.String("sku")})
r.Data["product"] = mcp.Text(out)
```

**Letting the model choose the tool** is two stages, not a hidden loop — the
choice is visible in the record and in the lineage, and the call is an ordinary
scheduled task under the same envelope, retries, and budget:

```go
chosen := named.Infer("choose", pipeline.InferSpec{
    Prefix: "Tools:\n" + mcp.Describe([]mcp.ToolDesc{
        {Name: "stock_level", Description: "units on hand right now"},
        {Name: "restock_eta", Description: "when the next shipment lands"},
    }),
    Prompt:    `Product {{.product}}. Question: {{.ask}}` + "\n" + `Reply with JSON {"tool": "...", "args": {...}}.`,
    ParseJSON: true,
})

called := chosen.MapTools("call", mcp.Dispatch(mcp.DispatchSpec{
    Server: "inventory",
    Allow:  []string{"stock_level", "restock_eta"},
    // ToolField "tool", ArgsField "args", OutputField "tool_result" by default
}), pipeline.WithMCP("inventory", "stock_level", "restock_eta"), pipeline.WithVersion("v1"))
```

An MCP **resource** becomes an ordinary broadcast, read once per run rather
than once per task:

```go
loom.WithMCPResource("voice", "inventory", "mem://voice")
// then: pipeline.WithBroadcast("voice") on the stage that reads it
```

Two rules that matter:

- **The tools' descriptor digest joins the stage fingerprint**, so upgrading a
  server recomputes exactly the stages that could have called the tools that
  changed and leaves the rest of the cache warm.
- **A cached stage does not repeat its tool calls.** For a lookup that is
  correct — replaying the recorded answer is as good as asking again. For a
  *write* it never is: give that stage `pipeline.WithNoCache()`.

`res.MCP` reports per server: `Dials`, `Calls`, and time spent waiting on tools
that cost no tokens. `mcp/mcptest` provides a scriptable in-process server, and
`examples/mcp-desk` runs a real child process over real pipes, offline.

## Secrets and egress

Secrets live in the run's broker and are resolved per call under the task's
granted capabilities. No task, op, or executor ever holds one; every resolution
is audited.

```go
loom.WithSecrets(map[security.SecretRef]string{
    anthropic.DefaultSecretRef: os.Getenv("ANTHROPIC_API_KEY"),
})
```

Egress is deny-by-default. A stage's envelope automatically allows the
endpoints of the models it is bound to — and nothing else. `loom.WithEgress(hosts...)`
adds hosts for tools that need them. This is what makes "these records cannot
reach a vendor" an enforced property rather than a comment.

Capability helpers: `security.ModelCap(id)`, `security.SecretCap(ref)`,
`security.ToolCap(name)`, `security.DataCap(name)`.

Check `res.Audit` in a test if the point of the pipeline is that something
*couldn't* be reached; `AuditLog.Denials()` isolates refusals.

## Sandboxing

`pipeline.WithSandbox(profile)` requests an isolation level for a stage's ops:
`task.SandboxInline` (default, in-process, for trusted first-party ops),
`SandboxSubprocess`, `SandboxContainer`, `SandboxWASM`. The local executor
implements Inline; the rest are the seam for stronger backends.
