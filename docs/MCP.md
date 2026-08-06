# MCP: tools under the envelope

The Model Context Protocol gives a model access to tools it did not ship with:
a filesystem, a ticket system, a code search, a database. Loom's problem with
adopting it is not the protocol — JSON-RPC with four methods that matter — but
the fact that a pipeline is a *fleet of tasks*, and the naive integration
("open a connection where you need one") is wrong at every scale a pipeline
operates at.

This document is the design: where connections come from, what a task carries,
what the cache does with a tool call, and what is deliberately not built.

## 1. The connection question

A pipeline that calls one MCP tool per record over ten thousand records has to
answer a question a chat client never faces: how many connections is that?

The candidate answers, and what each one costs:

| Connection per… | What happens |
|---|---|
| **record** | A process spawn or TLS handshake and an `initialize` round trip per record. The handshake dominates the work. Ten thousand child processes. |
| **task** | Spark's `mapPartitions` answer. Better — the cost amortizes over the batch — but Loom's default batch is one record, so this *is* per record. |
| **worker** | Warm connections, bounded by concurrency. But a worker is an implementation detail of one run, so two runs in a process open two sets, and neither respects the other's load. |
| **run** | One set per pipeline. Still one set *per pipeline*: a fleet of ten agents opens ten, and together they exceed a limit none of them individually violates. |
| **host** | One set per process, shared by every agent, bounded once. ← what Loom does |

The last row is the same argument [`docs/ASYNC.md`](ASYNC.md) makes about rate
limiters and budget governors, and it is the same argument because it is the
same kind of thing. A rate limit belongs to an account. A dollar ceiling
belongs to a wallet. **A connection belongs to a server process and an
account**, not to the pipeline that happens to want one. Anything a `Run`
provisions and tears down is a thing each concurrent pipeline gets its own copy
of, and every duplicate is a bug waiting for load.

So the catalog of MCP connections lives on the `host` — the structure a fleet
holds once and lends to every agent — right next to `limiter` and `gov`.

### Why "per partition" is the right instinct and the wrong unit

Spark opens a connection per partition because a JDBC connection carries one
statement at a time: it cannot be shared, so it must be replicated, so the
replication is amortized over the largest safe unit.

MCP is JSON-RPC. Every request carries an `id`, responses may return in any
order, and one session serves any number of concurrent calls. The constraint
Spark is working around does not exist here.

That splits the question in two, and Loom answers them separately:

- **Sessions** are long-lived and multiplexed. `Server.Sessions` defaults to
  **1**, because a second transport buys parallelism only for a server that
  serializes requests internally. One connection genuinely serves ten thousand
  records.
- **Concurrency** is what has to be rationed, so it is bounded explicitly:
  `Server.MaxConcurrent` (default: the engine's slot count) is a semaphore in
  front of the session — the tool-side analogue of the scheduler's token-bucket
  admission control on models.

What a task leases is therefore a **call slot**, not a connection. It holds it
for exactly as long as it is calling, and the session underneath stays warm.
That is the sentence the whole design reduces to:

> Connections are made once per host. Concurrency is bounded per server. A task
> leases a call slot, not a connection.

`TestOneSessionServesConcurrentCalls` pins the first half (eight concurrent
calls, one dial) and `TestServerNeverSeesMoreThanTheBound` pins the second (the
server itself reports the peak overlap it saw).

### Connect at provisioning, not on demand

`loom.WithMCPServer` connects during host construction, before a single task
exists — `initialize`, `tools/list`, and then the session stays open. Three
things fall out of that:

1. **A misconfigured server fails the run, not the first record.** An
   unreachable command, a bad credential, or an allowlisted tool the server
   does not offer is a startup error next to "model not registered", instead of
   a scatter of dead-lettered records ten minutes in.
2. **The tool descriptors exist at compile time**, which is what lets them
   join stage fingerprints (§3).
3. **No task pays a handshake.** A latency budget that includes a process spawn
   is a startup cost charged to a record.

## 2. What a task carries: a name, never a connection

A task envelope is serializable — that is what makes it shippable to a remote
or sandboxed executor. A live socket is not. So an envelope names its servers
and carries no connection:

```go
type Envelope struct {
    ...
    Broadcasts map[string]string // name  → content hash
    MCP        map[string]string // server → tool-descriptor digest
}
```

This is the broadcast mechanism applied to a different kind of resource.
A broadcast is *referenced, never copied*; a connection is *named, never
carried*. In both cases the envelope stays small and the executor resolves the
reference locally.

Three checks stand between a task and a tool call, and the planner assembles
all three from one declaration:

```go
src.MapTools("enrich", enrich, pipeline.WithMCP("catalog", "lookup_sku"))
```

- **Capability.** The grant is `security.ToolCap("mcp/catalog/lookup_sku")` —
  an ordinary tool capability, because an MCP tool is an ordinary tool. There
  is no second grant mechanism to keep in sync, and `security.GrantSet` did not
  change. Declaring one tool does not grant its neighbours on the same server.
- **Egress.** A networked server's host joins the stage's egress allowlist,
  and `executor.NetworkTool` makes the executor check it before the call —
  exactly what already happened for a model provider's endpoint. A stdio server
  opens no socket and adds no host.
- **Contract.** `env.MCP[server]` is the digest of the tool descriptors the
  stage was compiled against. Before every call the executor recomputes that
  digest from the live connection over the tools *this envelope grants*, and
  refuses if it differs.

That last check earns its place on reconnect. A session that dies is redialed
transparently; if the server that comes back is a different version, the digest
changes and every task planned against the old one fails loudly rather than
calling a tool nobody planned, priced, or granted. It is narrow on purpose —
derived from the envelope's own grants — so a stage that declared one tool is
not failed by a different tool changing beside it.

### Credentials

The descriptor names credentials; it never holds them:

```go
mcp.HTTP("github", "https://mcp.example.com/rpc").WithAuth("github_token")
mcp.Stdio("db", "npx", "-y", "@example/db-mcp")   // + EnvSecrets{"DB_URL": "db_url"}
```

At provisioning the host resolves exactly the references the configured servers
name, through the run's `security.SecretBroker`, against a grant set built from
those descriptors — so the check is real rather than ceremonial and the audit
log records precisely which credentials a connection consumed. The resolved
value reaches the transport (an HTTP header, a child process's environment) and
stops there. **No task needs a secret grant for a server it calls**; what a task
gets is a lease on an already-authenticated session. That is strictly stronger
than handing credentials to tasks, and it is what a connection pool has to do
anyway.

## 3. What the cache does with a tool call

Loom's central bargain is that completed AI work is never paid for twice, and
its mechanism is a fingerprint over (op spec, input content). A tool call is
work; the question is what belongs in the fingerprint.

**The tool descriptors do.** The digest of the descriptors a stage declared —
names, documentation, and JSON schemas — joins that stage's fingerprint. Upgrade
an MCP server and exactly the stages that could have called the tools that
changed recompute; every other stage keeps its cached results. This is the
broadcast argument again: hashing the *value* rather than the *name* is what
makes the cache honest. `TestMCPDigestNarrowsCacheInvalidation` pins both
halves, including that a pipeline declaring no MCP fingerprints exactly as it
did before this feature existed.

**Whether to cache at all is the author's call**, and the honest framing is
this: a cacheable stage asserts that replaying the recorded result is as good as
calling again. For a lookup that is usually true. For a write it is never true —
a cached `create_issue` creates no issue. Stages that call tools are
`MapTools` stages, which are only cacheable when given
`pipeline.WithVersion`, so the default is already "don't", and
`pipeline.WithNoCache` makes it explicit for a stage that must call every time.

What Loom deliberately does *not* do is try to be clever here — sniffing tool
names for verbs, or asking the server whether a tool is idempotent (the protocol
has no such notion). The declaration is the author's, because only the author
knows what the tool does.

## 4. Model-directed tool use, without a hidden loop

The other thing people mean by "MCP support" is a model that chooses tools.
Loom expresses it as two stages rather than an agent loop inside a task:

```go
src.
    Infer("choose", pipeline.InferSpec{
        Binding:   model.Binding{Tier: model.TierFast},
        Prefix:    "Tools:\n" + mcp.Describe(manifest.Tools),   // once per task, cached
        Prompt:    `{{.question}}` + "\n" + `Reply with JSON {"tool": "...", "args": {...}}.`,
        ParseJSON: true,
    }).
    MapTools("call", mcp.Dispatch(mcp.DispatchSpec{Server: "catalog"}),
        pipeline.WithMCP("catalog"))
```

The alternative — a task that calls tools until the model is satisfied — breaks
four things Loom is built on at once. Its cost is unbounded while its budget is
per task. Its cache key describes only its input, so replaying it replays a
trajectory that never happened. Its failures are unclassifiable, because a
transient tool error, a semantic model error, and an exhausted loop all surface
as one task failing. And its lineage is a black box: the record shows the
conclusion, never the six hops.

Splitting it means the model's choice is **data** — visible in the record, in
the lineage, in the constellation view, and in `Explain` — and running it is an
ordinary scheduled task under the ordinary envelope, retry policy, and governor.
`Dispatch` enforces that a tool the model *asked* for is not thereby a tool it
is *authorized* to run: the name is checked against the stage's allowlist, and
the envelope's grant is checked again underneath.

When the loop is genuinely wanted, it already exists one altitude up:
`pipeline.Iterate` with a `Refine`-style algorithm runs choose→call→judge for as
many rounds as convergence, a round cap, and a stage budget allow — with every
round an ordinary stage batch, and every bound reported (see
[ALGORITHMS.md](ALGORITHMS.md)).

## 5. Resources as broadcasts

An MCP server also serves documents. `loom.WithMCPResource` reads one at
provisioning and registers it as a broadcast:

```go
loom.WithMCPResource("voice", "inventory", "mem://voice")
```

From there it is an ordinary shared value: stored once by content hash,
referenced (never copied) by every task that declares it, readable from a prompt
with `{{broadcast "voice"}}`, and part of the reading stages' fingerprints — so
editing the document upstream recomputes exactly the stages that read it.

Reading it per task instead would be a network call per record whose result
silently joins a cached artifact. Read once, it is a value with a hash, which is
what the rest of the framework already knows how to reason about.

## 6. Observability

- **`mcp.called`** events carry server, tool, latency, and error, attributed to
  the run, stage, and task that made the call. The collector folds them into
  `StageStats.ToolCalls` and `ToolTime`, and the run report ends with a line
  saying how many tool calls a run made and how long it spent in them. That line
  exists because tool calls are the only work in Loom that costs **no tokens**:
  a report that only totalled money would explain a stage bounded by a slow
  server as a fast stage.
- **Connections are not run events.** They are made before any run starts and
  outlive every run on the host, so they land in the audit log
  (`mcp.connect`, subject: the server name) rather than in a run's event stream,
  alongside the `secret.resolve` lines for the credentials they consumed.
- **`RunResult.MCP`** and **`FleetReport.MCP`** report sessions, dials, calls,
  errors, and slot-busy time per server. `dials > sessions` is the number that
  says a server has been dropping connections.
- **`Explain`** compiles MCP declarations without connecting — a projection
  that dials a server has broken its own promise. Names and allowlists still
  validate; the descriptor digests do not exist, so a stage that declares MCP
  fingerprints differently under `Explain` than under `Run`, which nothing in a
  projection reads. The projection warns that MCP calls are unpriced and that
  such a stage's wall-clock is bounded by the server rather than by the rate
  limits in the table.

## 7. What is not built

- **Tool use inside a provider call.** Loom does not translate MCP tools into
  Anthropic/OpenAI tool definitions and run the provider's tool-use loop inside
  one task. That is the hidden loop of §4, and the two-stage form is both
  expressible today and better behaved. If it is added later it belongs behind
  `Iterate`, not inside `Infer`.
- **Sampling and elicitation** (server→client requests). Loom advertises no
  capabilities that invite them, and ignores them if they arrive. A server that
  could ask the client to run a model call would be issuing spend outside the
  governor, which is exactly the thing this framework exists to prevent.
- **Notifications** (`tools/list_changed`, progress, logging) are ignored. A
  live tool set that changed mid-run would invalidate a plan that is already
  running; the digest check turns that into a clean, loud failure on the next
  reconnect instead.
- **A registry or auto-discovery of servers.** Servers are configuration, named
  explicitly, like models.

## 8. Map of the code

| File | What it holds |
|---|---|
| `mcp/mcp.go` | The protocol: `Server` descriptors, JSON-RPC framing, the stdio and streamable-HTTP transports, `Session` (initialize / tools / resources), `Digest`. No dependencies. |
| `mcp/pool.go` | The connection architecture: `Catalog`, the per-server session pool and call-slot semaphore, redial and drift detection, and the `Tool` adapter. |
| `mcp/dispatch.go` | `Dispatch` (model-chosen tool calls) and `Describe` (a tool list for a prompt prefix). |
| `mcp/mcptest` | A scriptable in-process MCP server — stdio, HTTP, or SSE — for tests and offline examples. What `model.Mock` is to a provider. |
| `plan/plan.go` | `resolveMCP`: declarations → grants, egress, digest, fingerprint. |
| `executor/executor.go` | `NetworkTool` (egress check) and `ScopedTool` (envelope-aware invocation). |
| `fleet.go` | `host.connectMCP` / `readMCPResources`: provisioning, shared by every agent. |

`examples/mcp-desk` is the whole thing running offline: a real child-process
server, a per-record tool call, a model-chosen call, a resource-as-broadcast,
and a second run that replays every tool call for free.
