package mcp

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/task"
)

// Options configure a Catalog. All three come from the host: the broker that
// resolves credentials, the log that records every access decision, and the
// bus every other component publishes on.
type Options struct {
	// Resolve returns a credential for a secret reference. The host wires this
	// to the run's broker, scoped to exactly the references the configured
	// servers declare — so provisioning can authenticate a connection while no
	// task, op, or executor ever holds the credential.
	Resolve func(ref security.SecretRef) (string, error)
	// Audit records connection and call decisions.
	Audit *security.AuditLog
	// Bus carries mcp.called events.
	Bus *observe.Bus
	// Slots is the engine's concurrency, used as the default per-server
	// ceiling when a Server does not set MaxConcurrent. A bound above the
	// number of tasks that can run at once is not a bound.
	Slots int
}

// Catalog is the set of MCP servers a host holds connections to, and the
// thing that makes those connections a property of the account rather than of
// a pipeline. It is created once per host — one per fleet, one per Run — and
// every agent on that host shares its sessions, its concurrency ceilings, and
// its discovered tool contracts.
//
// A Catalog is safe for concurrent use.
type Catalog struct {
	opts  Options
	pools map[string]*pool
	order []string

	mu        sync.Mutex
	connected bool
	closed    bool
}

// NewCatalog builds a catalog over the given server descriptors. It validates
// and resolves credentials but opens nothing: Connect does that.
func NewCatalog(opts Options, servers ...Server) (*Catalog, error) {
	c := &Catalog{opts: opts, pools: map[string]*pool{}}
	for _, s := range servers {
		if err := s.Validate(); err != nil {
			return nil, err
		}
		if _, dup := c.pools[s.Name]; dup {
			return nil, fmt.Errorf("mcp: server %q registered twice", s.Name)
		}
		if s.MaxConcurrent == 0 && opts.Slots > 0 {
			s.MaxConcurrent = opts.Slots
		}
		secrets, err := c.resolve(s)
		if err != nil {
			return nil, err
		}
		c.pools[s.Name] = newPool(s, secrets, opts)
		c.order = append(c.order, s.Name)
	}
	sort.Strings(c.order)
	return c, nil
}

// resolve pulls a server's credentials through the broker once, at
// construction. Doing it here rather than per dial is what keeps the
// credential out of the reconnect path — and out of every task, which never
// needs a secret grant for a server it calls, only a tool grant.
func (c *Catalog) resolve(s Server) (map[security.SecretRef]string, error) {
	refs := s.secretRefs()
	if len(refs) == 0 {
		return nil, nil
	}
	if c.opts.Resolve == nil {
		return nil, fmt.Errorf("mcp: server %q needs secret %q but no broker is configured", s.Name, refs[0])
	}
	out := make(map[security.SecretRef]string, len(refs))
	for _, ref := range refs {
		val, err := c.opts.Resolve(ref)
		if err != nil {
			return nil, fmt.Errorf("mcp: server %q: %w", s.Name, err)
		}
		out[ref] = val
	}
	return out, nil
}

// Len is the number of configured servers.
func (c *Catalog) Len() int { return len(c.pools) }

// Servers lists the configured server names in order.
func (c *Catalog) Servers() []string { return append([]string(nil), c.order...) }

// Connect dials every server and discovers its tools, in parallel. It is
// called once at provisioning, before any task runs, for three reasons: a
// misconfigured server should fail the run rather than the first record that
// reaches it, the tool descriptors are what the plan is compiled against, and
// a task that pays a handshake is a task whose latency is a startup cost in
// disguise.
func (c *Catalog) Connect(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("mcp: catalog closed")
	}
	c.connected = true
	c.mu.Unlock()

	errs := make([]error, len(c.order))
	var wg sync.WaitGroup
	for i, name := range c.order {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = c.pools[name].connect(ctx)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// Manifest is the discovered contract: what each server offers and the digest
// a plan is compiled against. It is plain data — no connections — so it can be
// serialized to a remote planner, and so Explain can validate a pipeline's MCP
// declarations against it without opening a socket.
func (c *Catalog) Manifest() Manifest {
	m := make(Manifest, len(c.pools))
	for name, p := range c.pools {
		m[name] = p.manifest()
	}
	return m
}

// Tools returns one adapter per allowlisted tool, ready to register in the
// executor's tool set. Names are qualified ("mcp/<server>/<tool>"), so a grant
// on an MCP tool is an ordinary security.ToolCap and needs no new concept.
func (c *Catalog) Tools() []*Tool {
	var out []*Tool
	for _, name := range c.order {
		out = append(out, c.pools[name].tools()...)
	}
	return out
}

// Call invokes a tool by server and name, without an envelope. Ops go through
// the capability-checked session instead; this is for the host itself and for
// tests.
func (c *Catalog) Call(ctx context.Context, server, tool string, args map[string]any) (CallResult, error) {
	p, ok := c.pools[server]
	if !ok {
		return CallResult{}, core.Permanent(fmt.Errorf("mcp: server %q is not configured", server))
	}
	return p.call(ctx, tool, args)
}

// ReadResource reads an MCP resource and returns its content as a
// JSON-serializable value: the text of a single text resource, or a list of
// {uri, mime_type, text} entries when the URI resolves to several.
//
// It exists so a resource can be registered as a broadcast — read once at
// provisioning, stored by content hash, referenced by every task that declares
// it, and folded into their fingerprints so editing the document upstream
// recomputes exactly the stages that read it.
func (c *Catalog) ReadResource(ctx context.Context, server, uri string) (any, error) {
	p, ok := c.pools[server]
	if !ok {
		return nil, fmt.Errorf("mcp: server %q is not configured", server)
	}
	contents, err := p.readResource(ctx, uri)
	if err != nil {
		return nil, err
	}
	if len(contents) == 1 && contents[0].Blob == "" {
		return contents[0].Text, nil
	}
	out := make([]map[string]any, 0, len(contents))
	for _, ct := range contents {
		b, err := ct.Bytes()
		if err != nil {
			return nil, fmt.Errorf("mcp %s: resource %s: %w", server, uri, err)
		}
		out = append(out, map[string]any{
			"uri": ct.URI, "mime_type": ct.MimeType, "text": string(b),
		})
	}
	return out, nil
}

// Stats reports what each server's connections have done, for the run report.
func (c *Catalog) Stats() []Stats {
	out := make([]Stats, 0, len(c.order))
	for _, name := range c.order {
		out = append(out, c.pools[name].stats())
	}
	return out
}

// Close shuts every session down, ending the child processes of stdio servers.
func (c *Catalog) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	var errs []error
	for _, name := range c.order {
		errs = append(errs, c.pools[name].close())
	}
	return errors.Join(errs...)
}

// Stats is one server's connection accounting.
type Stats struct {
	Server   string
	Endpoint string
	Tools    int
	Digest   string
	Sessions int           // transports currently open
	Dials    int           // connections made, including reconnects
	Calls    int           // tool calls issued
	Errors   int           // tool calls that failed
	Busy     time.Duration // total time held on a call slot
}

// ServerManifest is one server's discovered contract.
type ServerManifest struct {
	Name     string     `json:"name"`
	Endpoint string     `json:"endpoint,omitempty"`
	Tools    []ToolDesc `json:"tools,omitempty"`
	Digest   string     `json:"digest,omitempty"`
	// Allow is the configured allowlist ("" for all), carried so a manifest
	// built without connecting can still reject a stage declaring a tool the
	// deployment never permitted.
	Allow []string `json:"allow,omitempty"`
	// Discovered is false for a manifest built from configuration alone —
	// Explain's case, which must not open a socket. Declarations still
	// validate; digests are simply not available, so fingerprints computed
	// against it differ from the ones a run computes.
	Discovered bool `json:"discovered"`
}

// Manifest maps server name to its contract.
type Manifest map[string]ServerManifest

// Declared builds a manifest from configuration alone, without connecting.
func Declared(servers ...Server) Manifest {
	m := make(Manifest, len(servers))
	for _, s := range servers {
		m[s.Name] = ServerManifest{
			Name: s.Name, Endpoint: s.Endpoint(), Allow: s.Tools,
		}
	}
	return m
}

// Select resolves the tools a stage declared against this server's contract,
// returning their names in sorted order. An empty selection means every tool
// the stage is permitted, which is every discovered tool narrowed by the
// deployment's allowlist.
func (m ServerManifest) Select(tools []string) ([]string, error) {
	allowed := map[string]bool{}
	for _, t := range m.Allow {
		allowed[t] = true
	}
	if !m.Discovered {
		// Without discovery the only check available is the deployment's own
		// allowlist; a named tool outside it is wrong whatever the server
		// turns out to advertise.
		if len(tools) == 0 {
			return append([]string(nil), m.Allow...), nil
		}
		for _, t := range tools {
			if len(allowed) > 0 && !allowed[t] {
				return nil, fmt.Errorf("tool %q is not on server %q's allowlist", t, m.Name)
			}
		}
		out := append([]string(nil), tools...)
		sort.Strings(out)
		return out, nil
	}

	have := map[string]bool{}
	var all []string
	for _, d := range m.Tools {
		have[d.Name] = true
		all = append(all, d.Name)
	}
	if len(tools) == 0 {
		sort.Strings(all)
		return all, nil
	}
	out := make([]string, 0, len(tools))
	for _, t := range tools {
		if !have[t] {
			return nil, fmt.Errorf("server %q does not offer tool %q (it offers %s)",
				m.Name, t, strings.Join(all, ", "))
		}
		out = append(out, t)
	}
	sort.Strings(out)
	return out, nil
}

// DigestOf hashes the descriptors of the named tools — the fingerprint
// component a declaring stage carries, so that upgrading a server invalidates
// exactly the cached results of the stages that could have called the tools
// that changed.
func (m ServerManifest) DigestOf(tools []string) string {
	if !m.Discovered {
		return ""
	}
	want := map[string]bool{}
	for _, t := range tools {
		want[t] = true
	}
	sel := make([]ToolDesc, 0, len(tools))
	for _, d := range m.Tools {
		if want[d.Name] {
			sel = append(sel, d)
		}
	}
	return Digest(sel)
}

// --- per-server pool -----------------------------------------------------

// pool holds one server's sessions and the semaphore that bounds how hard the
// whole host may push it.
//
// The two are deliberately separate. Sessions are multiplexed, so their count
// is a property of the server's internals — one is right unless it serializes
// requests — while the concurrency bound is a property of its quota. Conflating
// them, as a classic connection pool does, means you cannot raise throughput
// without spawning processes or lower load without losing warm connections.
type pool struct {
	server  Server
	secrets map[security.SecretRef]string
	opts    Options
	sem     chan struct{}
	// dialMu serializes reconnects. Without it, a server that drops its
	// connection under load is met by every waiting task dialing at once —
	// which for a stdio server means a burst of child processes, and for an
	// HTTP one a thundering herd against a server that is already unwell.
	dialMu sync.Mutex

	mu       sync.Mutex
	sessions []*Session
	descs    []ToolDesc // allowlisted subset, sorted
	digest   string     // over descs: the whole server's contract, for reporting
	subsets  map[string]string
	closed   bool

	next  atomic.Uint64
	dials atomic.Int64
	calls atomic.Int64
	errs  atomic.Int64
	busy  atomic.Int64
}

func newPool(s Server, secrets map[security.SecretRef]string, opts Options) *pool {
	return &pool{
		server: s, secrets: secrets, opts: opts,
		sem: make(chan struct{}, s.maxConcurrent()),
	}
}

func (p *pool) connect(ctx context.Context) error {
	for i := 0; i < p.server.sessions(); i++ {
		s, err := p.dial(ctx)
		if err != nil {
			p.audit("mcp.connect", false, err.Error())
			return err
		}
		p.mu.Lock()
		p.sessions = append(p.sessions, s)
		p.mu.Unlock()
	}
	p.audit("mcp.connect", true, fmt.Sprintf("%d session(s), %d tool(s)",
		p.server.sessions(), len(p.descs)))
	return nil
}

// dial opens one session and reconciles what it advertises with what this
// deployment allows. It also refreshes the pool's digest, which is what makes
// a reconnect a checkpoint rather than a silent substitution: a server that
// comes back offering different tools gets a different digest, and every task
// whose envelope names the old one fails loudly.
func (p *pool) dial(ctx context.Context) (*Session, error) {
	s, err := Dial(ctx, p.server, p.secrets)
	if err != nil {
		return nil, err
	}
	descs, err := p.allowlist(s.Tools)
	if err != nil {
		_ = s.Close()
		return nil, err
	}
	p.mu.Lock()
	p.descs, p.digest = descs, Digest(descs)
	p.subsets = map[string]string{} // the memo describes the old connection
	p.mu.Unlock()
	p.dials.Add(1)
	return s, nil
}

// digestOf hashes the descriptors of a subset of this server's tools — the
// same value the planner computed for a stage that declared exactly those
// tools, which is what makes the two comparable at call time. Memoized because
// a stage asks the same question on every record, and dropped on redial
// because that is precisely when the answer can change.
func (p *pool) digestOf(tools []string) string {
	key := strings.Join(tools, "\x00")
	p.mu.Lock()
	defer p.mu.Unlock()
	if d, ok := p.subsets[key]; ok {
		return d
	}
	want := make(map[string]bool, len(tools))
	for _, t := range tools {
		want[t] = true
	}
	sel := make([]ToolDesc, 0, len(tools))
	for _, d := range p.descs {
		if want[d.Name] {
			sel = append(sel, d)
		}
	}
	d := Digest(sel)
	if p.subsets == nil {
		p.subsets = map[string]string{}
	}
	p.subsets[key] = d
	return d
}

// allowlist narrows a server's advertised tools to the ones this deployment
// permits, failing when a named tool is missing — a typo in a config should
// fail at provisioning, not at the first record that calls it.
func (p *pool) allowlist(all []ToolDesc) ([]ToolDesc, error) {
	if len(p.server.Tools) == 0 {
		return all, nil
	}
	have := map[string]ToolDesc{}
	var names []string
	for _, d := range all {
		have[d.Name] = d
		names = append(names, d.Name)
	}
	out := make([]ToolDesc, 0, len(p.server.Tools))
	for _, want := range p.server.Tools {
		d, ok := have[want]
		if !ok {
			return nil, fmt.Errorf("mcp: server %q does not offer allowlisted tool %q (it offers %s)",
				p.server.Name, want, strings.Join(names, ", "))
		}
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (p *pool) manifest() ServerManifest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return ServerManifest{
		Name: p.server.Name, Endpoint: p.server.Endpoint(),
		Tools: append([]ToolDesc(nil), p.descs...), Digest: p.digest,
		Allow: p.server.Tools, Discovered: len(p.sessions) > 0,
	}
}

func (p *pool) currentDigest() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.digest
}

func (p *pool) tools() []*Tool {
	p.mu.Lock()
	descs := append([]ToolDesc(nil), p.descs...)
	p.mu.Unlock()
	out := make([]*Tool, 0, len(descs))
	for _, d := range descs {
		out = append(out, &Tool{pool: p, desc: d, name: ToolName(p.server.Name, d.Name)})
	}
	return out
}

// acquire takes a call slot. This is the lease: what a task holds for the
// duration of a call, and what a fleet's agents queue for when a server is
// saturated. It is deliberately not a connection — the session underneath is
// shared and stays warm.
func (p *pool) acquire(ctx context.Context) error {
	select {
	case p.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return core.Transient(fmt.Errorf("mcp %s: waiting for a call slot: %w", p.server.Name, ctx.Err()))
	}
}

func (p *pool) release() { <-p.sem }

// session returns a live session, dialing one if the pool is empty or a
// previous call found the connection dead. Round-robin over the sessions
// spreads load when a server needed more than one.
func (p *pool) session(ctx context.Context) (*Session, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, core.Permanent(fmt.Errorf("mcp %s: catalog closed", p.server.Name))
	}
	if n := len(p.sessions); n > 0 {
		s := p.sessions[int(p.next.Add(1)-1)%n]
		p.mu.Unlock()
		return s, nil
	}
	p.mu.Unlock()

	p.dialMu.Lock()
	defer p.dialMu.Unlock()
	// Someone may have reconnected while this call queued behind them, in
	// which case there is nothing left to do but use it.
	p.mu.Lock()
	if n := len(p.sessions); n > 0 {
		s := p.sessions[int(p.next.Add(1)-1)%n]
		p.mu.Unlock()
		return s, nil
	}
	p.mu.Unlock()

	s, err := p.dial(ctx)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = s.Close()
		return nil, core.Permanent(fmt.Errorf("mcp %s: catalog closed", p.server.Name))
	}
	p.sessions = append(p.sessions, s)
	p.mu.Unlock()
	return s, nil
}

// drop retires a session whose transport died, so the next call redials
// instead of writing into a closed pipe.
func (p *pool) drop(dead *Session) {
	p.mu.Lock()
	kept := p.sessions[:0]
	for _, s := range p.sessions {
		if s != dead {
			kept = append(kept, s)
		}
	}
	p.sessions = kept
	p.mu.Unlock()
	_ = dead.Close()
}

func (p *pool) call(ctx context.Context, tool string, args map[string]any) (CallResult, error) {
	if err := p.acquire(ctx); err != nil {
		return CallResult{}, err
	}
	start := time.Now()
	defer func() {
		p.busy.Add(int64(time.Since(start)))
		p.release()
	}()

	callCtx, cancel := context.WithTimeout(ctx, p.server.timeout())
	defer cancel()

	sess, err := p.session(callCtx)
	if err != nil {
		p.errs.Add(1)
		return CallResult{}, err
	}
	p.calls.Add(1)
	res, err := sess.Call(callCtx, tool, args)
	if err != nil {
		p.errs.Add(1)
		// A transient failure is the transport's, not the tool's: retire the
		// session so the scheduler's retry lands on a fresh one.
		if core.ClassOf(err) == core.FailTransient {
			p.drop(sess)
		}
		return res, err
	}
	return res, nil
}

func (p *pool) readResource(ctx context.Context, uri string) ([]ResourceContent, error) {
	if err := p.acquire(ctx); err != nil {
		return nil, err
	}
	defer p.release()
	callCtx, cancel := context.WithTimeout(ctx, p.server.timeout())
	defer cancel()

	sess, err := p.session(callCtx)
	if err != nil {
		return nil, err
	}
	contents, err := sess.ReadResource(callCtx, uri)
	if err != nil && core.ClassOf(err) == core.FailTransient {
		p.drop(sess)
	}
	return contents, err
}

func (p *pool) stats() Stats {
	p.mu.Lock()
	sessions, tools, digest := len(p.sessions), len(p.descs), p.digest
	p.mu.Unlock()
	return Stats{
		Server: p.server.Name, Endpoint: p.server.Endpoint(),
		Tools: tools, Digest: digest, Sessions: sessions,
		Dials: int(p.dials.Load()), Calls: int(p.calls.Load()),
		Errors: int(p.errs.Load()), Busy: time.Duration(p.busy.Load()),
	}
}

func (p *pool) close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	sessions := p.sessions
	p.sessions = nil
	p.mu.Unlock()

	var errs []error
	for _, s := range sessions {
		errs = append(errs, s.Close())
	}
	return errors.Join(errs...)
}

func (p *pool) audit(action string, allowed bool, reason string) {
	if p.opts.Audit == nil {
		return
	}
	p.opts.Audit.Record(security.AuditEntry{
		TaskID: "provisioning", Action: action, Subject: p.server.Name,
		Allowed: allowed, Reason: reason,
	})
}

// --- tool adapter --------------------------------------------------------

// Tool is one MCP tool presented as a Loom tool: a name an envelope grants, an
// endpoint the egress policy checks, and an invocation that leases a call slot
// from its server's pool.
//
// It satisfies executor.Tool and, through InvokeIn, the executor's scoped tool
// interface — which is how it gets the envelope it needs to verify the server
// still offers the contract the plan was built against.
type Tool struct {
	pool *pool
	desc ToolDesc
	name string
}

// Name is the qualified tool name, e.g. "mcp/github/search_code".
func (t *Tool) Name() string { return t.name }

// Server is the MCP server this tool belongs to.
func (t *Tool) Server() string { return t.pool.server.Name }

// Endpoint is the host the call reaches, or "" for a stdio server. The
// executor checks it against the task's egress allowlist before every
// invocation, exactly as it does for a model provider.
func (t *Tool) Endpoint() string { return t.pool.server.Endpoint() }

// Describe returns the server's own description of the tool — the name,
// documentation, and JSON schema, which is what a prompt should be shown when
// a model is asked to choose a tool.
func (t *Tool) Describe() ToolDesc { return t.desc }

// Invoke calls the tool without a task envelope. Ops reach tools through the
// capability-checked session instead; this path exists for the host and for
// direct use, and skips the contract check that has no envelope to read.
func (t *Tool) Invoke(ctx context.Context, args map[string]any) (any, error) {
	return t.InvokeIn(ctx, task.Envelope{}, "", args)
}

// InvokeIn calls the tool under a task envelope.
func (t *Tool) InvokeIn(ctx context.Context, env task.Envelope, taskID string, args map[string]any) (any, error) {
	if err := t.checkContract(env); err != nil {
		return nil, err
	}
	start := time.Now()
	res, err := t.pool.call(ctx, t.desc.Name, args)
	t.publish(env, taskID, time.Since(start), res, err)
	if err != nil {
		return nil, err
	}
	return res.Record(), nil
}

// checkContract compares the tool digest the plan was compiled against with
// the one the live connection produces for the same tools. They differ only
// when a server was replaced under a running plan — a reconnect that landed on
// a new version — and continuing would mean calling a tool nobody planned,
// priced, or granted.
//
// The subset to hash comes from the envelope's own grants, not from the whole
// server, so the check is as narrow as the fingerprint it mirrors: a stage that
// declared one tool is not failed by a different tool changing beside it.
func (t *Tool) checkContract(env task.Envelope) error {
	planned, ok := env.MCP[t.pool.server.Name]
	if !ok || planned == "" {
		return nil
	}
	if live := t.pool.digestOf(grantedTools(env, t.pool.server.Name)); live != planned {
		return core.Permanent(fmt.Errorf(
			"mcp %s: the server's tools changed since this stage was planned "+
				"(planned %s, live %s); rerun so the plan is compiled against the "+
				"tools that now exist", t.pool.server.Name, short(planned), short(live)))
	}
	return nil
}

// grantedTools lists the tools of one server an envelope grants, in sorted
// order — the same set, derived the same way, that the planner hashed into the
// stage's fingerprint.
func grantedTools(env task.Envelope, server string) []string {
	prefix := "tool:" + ToolName(server, "")
	var out []string
	for _, c := range env.Grants.List() { // List is sorted
		if tool, ok := strings.CutPrefix(string(c), prefix); ok && tool != "" {
			out = append(out, tool)
		}
	}
	return out
}

func (t *Tool) publish(env task.Envelope, taskID string, latency time.Duration, res CallResult, err error) {
	// A call with no run behind it — Tool.Invoke used directly — has nowhere to
	// be attributed, and an event without a run ID would land in whichever run
	// an observer happened to be showing.
	if t.pool.opts.Bus == nil || env.RunID == "" {
		return
	}
	e := observe.Event{
		Type: observe.MCPCalled, RunID: env.RunID, Stage: env.Stage, TaskID: taskID,
		Server: t.pool.server.Name, Tool: t.desc.Name, Latency: latency,
	}
	if err != nil {
		e.Err = err.Error()
	} else {
		e.Bytes = len(res.Text())
		e.Response = observe.Clip(res.Text())
	}
	t.pool.opts.Bus.Publish(e)
}

func short(hash string) string {
	if len(hash) <= 12 {
		return hash
	}
	return hash[:12]
}
