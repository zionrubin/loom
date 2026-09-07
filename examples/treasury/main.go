// Command treasury runs the same fleet twice against the same ceiling, once
// with each process holding it alone and once with all of them sharing it —
// and prints the difference, which is money.
//
//	go run ./examples/treasury                        # 4 processes, a shared directory
//	go run ./examples/treasury -processes 8           # more of them
//	go run ./examples/treasury -service               # the quota behind an HTTP service
//	go run ./examples/treasury -wallet 0.25           # a different ceiling
//	go run ./examples/treasury -rpm 60                # a bucket tight enough to bind (slow)
//
// Every process here is a full Loom run: its own scheduler, its own cache, its
// own report. That is exactly what a batch of workers under a systemd unit, a
// job per pod, or a service running a pipeline per request already is — and
// until a shared quota existed, each of them believed it owned the whole
// provider account:
//
//   - a $0.10 ceiling held by four processes is a $0.40 ceiling, silently;
//   - a 4,000 requests/min limit admitted against by four processes is 16,000,
//     and the provider answers the difference with the 429s the scheduler was
//     built to avoid.
//
// What to look for:
//
//   - **spend against the ceiling**: the "alone" column spends N× the wallet,
//     the "shared" column spends the wallet. The overrun in the shared column
//     is bounded by the calls that were in flight when it was crossed, and the
//     report says how many that was.
//   - **who stopped**: every process is stopped by a wallet most of them did
//     not empty. That is the point — a ceiling is a property of an account.
//   - **the rate bucket**: the model is metered, so admission goes through the
//     shared buckets too, and the per-model line is the fleet's rather than any
//     one process's. At the default limits it reports zero deferrals, which is
//     the useful answer — the rate limit is not what is slowing this fleet down,
//     the wallet is. Pass -rpm 60 to make the bucket bind instead, and watch a
//     minute's allowance shared four ways rather than handed to each of them.
//   - **the projection**: before either round, Explain reads the wallet and
//     says whether what is left covers the run. Knowing that costs one read
//     instead of half a job.
//
// It runs entirely offline: mock models, a shared directory (or an HTTP
// service with -service) in a temporary path, and no keys.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/quota"
)

// tickets is the work. Each process gets the same shape of job with its own
// records, which is what a fleet pulling from a queue looks like: no two
// processes doing the same task, all of them spending the same money.
const ticketsPerProcess = 200

func main() {
	var (
		child     = flag.Bool("child", false, "run as one process of the fleet (set by the parent)")
		name      = flag.String("name", "", "this process's name")
		dir       = flag.String("quota", "", "the shared quota directory")
		service   = flag.String("quota-url", "", "the shared quota service")
		out       = flag.String("out", "", "where to write this process's report")
		ready     = flag.String("ready", "", "file to touch once provisioned")
		start     = flag.String("go", "", "file to wait for before spending")
		shared    = flag.Bool("shared", true, "hold the ceiling with the rest of the fleet")
		wallet    = flag.Float64("wallet", 0.05, "the ceiling, in dollars")
		processes = flag.Int("processes", 4, "how many processes to run")
		asService = flag.Bool("service", false, "put the quota behind an HTTP service")
		latency   = flag.Duration("latency", 4*time.Millisecond, "how slow one model call is")
		rpm       = flag.Int("rpm", 600, "the model's requests/min limit, shared by the fleet")
	)
	flag.Parse()

	if *child {
		if err := runChild(context.Background(), config{
			Name: *name, Dir: *dir, URL: *service, Out: *out, Ready: *ready,
			Go: *start, Shared: *shared, Wallet: *wallet, Latency: *latency, RPM: *rpm,
		}); err != nil {
			log.Fatalf("%s: %v", *name, err)
		}
		return
	}
	if err := runFleet(*processes, *wallet, *asService, *latency, *rpm); err != nil {
		log.Fatal(err)
	}
}

// --- one process of the fleet -------------------------------------------

type config struct {
	Name    string
	Dir     string // the shared quota directory, when not using the service
	URL     string // the shared quota service, when using it
	Out     string
	Ready   string
	Go      string
	Shared  bool
	Wallet  float64
	Latency time.Duration
	RPM     int
}

// report is what one process tells the parent it did.
type report struct {
	Name     string  `json:"name"`
	SpentUSD float64 `json:"spent_usd"`
	Calls    int     `json:"calls"`
	Stopped  string  `json:"stopped"`
	// Fleet is what the shared ledger said when this process finished. Only
	// filled in the shared round; in the "alone" round there is no such number
	// to have, which is the whole problem.
	FleetUSD float64 `json:"fleet_usd"`
	// Admitted and Waited are this process's own traffic through the shared
	// buckets. What the buckets did fleet-wide is a counter every process reads
	// the same value of, so the parent reads it once rather than summing four
	// copies of it.
	Admitted int    `json:"admitted"`
	Waited   string `json:"waited"`
}

func runChild(ctx context.Context, cfg config) error {
	reg, err := registry(cfg.Latency, cfg.RPM)
	if err != nil {
		return err
	}
	budget := core.Budget{MaxCostUSD: cfg.Wallet}

	opts := []loom.Option{loom.WithRegistry(reg), loom.WithWorkers(2)}
	var shared quota.Store
	switch {
	case !cfg.Shared:
		// What every process was before this feature: the ceiling, held alone.
		opts = append(opts, loom.WithRunBudget(budget))
	case cfg.URL != "":
		shared = quota.Dial(cfg.URL, quota.ClientOptions{})
	default:
		d, err := quota.Open(cfg.Dir, quota.Options{})
		if err != nil {
			return err
		}
		shared = d
	}
	if shared != nil {
		defer shared.Close()
		opts = append(opts,
			// Refreshed aggressively because a mock provider spends a wallet in
			// milliseconds. Against a real provider the default second of
			// staleness is a call or two, not a round.
			loom.WithQuotaConfig(quota.Config{Refresh: 2 * time.Millisecond}),
			loom.WithSharedQuota(shared, budget, 0))
	}

	if cfg.Ready != "" {
		if err := os.WriteFile(cfg.Ready, []byte(cfg.Name), 0o644); err != nil {
			return err
		}
	}
	// The fleet starts spending together, or what the numbers below measure is
	// which process the operating system scheduled first.
	if cfg.Go != "" && !waitFor(cfg.Go, time.Minute) {
		return fmt.Errorf("%s: the start signal never arrived", cfg.Name)
	}

	res, runErr := loom.Run(ctx, triage(cfg.Name, ticketsPerProcess), opts...)
	rep := report{Name: cfg.Name}
	if res != nil {
		rep.SpentUSD, rep.Calls = res.Spent.CostUSD, res.Spent.Requests
		rep.FleetUSD = res.Quota.Ledger.Spent.CostUSD
		rep.Admitted = res.Quota.Admitted
		rep.Waited = res.Quota.Waited.Round(time.Millisecond).String()
	}
	if runErr != nil {
		rep.Stopped = runErr.Error()
	}
	blob, err := json.Marshal(rep)
	if err != nil {
		return err
	}
	return os.WriteFile(cfg.Out, blob, 0o644)
}

// registry prices one mock model and meters it, so both halves of a shared
// quota — the wallet and the buckets — are exercised by the same run.
func registry(latency time.Duration, rpm int) (*model.Registry, error) {
	reg := model.NewRegistry()
	mock := model.NewMock("mock",
		model.WithLatency(latency),
		model.WithHandler(func(req model.Request) (string, error) {
			if strings.Contains(req.Prompt, "urgent") {
				return "urgent: escalate to the on-call engineer", nil
			}
			return "routine: queue for the next business day", nil
		}))
	err := reg.Register(model.Info{
		ID: "triage-fast", Provider: mock, Tier: model.TierFast,
		// A small requests/min limit, so the shared bucket is contended by four
		// processes rather than merely shared by them.
		Limits:  model.Limits{RequestsPerMinute: rpm, TokensPerMinute: 200000},
		Pricing: model.Pricing{InputPerMTok: 10, OutputPerMTok: 30},
	})
	return reg, err
}

func triage(owner string, n int) *pipeline.Pipeline {
	recs := make([]core.Record, n)
	for i := range recs {
		subject := "password reset request"
		if i%3 == 0 {
			subject = "urgent: checkout is down for all customers"
		}
		recs[i] = core.NewRecord(fmt.Sprintf("%s-%03d", owner, i),
			map[string]any{"subject": subject})
	}
	p := pipeline.New("triage-" + owner)
	p.FromRecords("tickets", recs).Infer("classify", pipeline.InferSpec{
		Binding: model.Binding{Tier: model.TierFast},
		System:  "You triage support tickets.",
		Prompt:  "Classify this ticket: {{.subject}}",
		// A classification is a sentence, and saying so is what makes the
		// projected ceiling a number worth comparing a wallet against: the
		// default reserves a kilobyte of output for every record.
		MaxTokens: 64,
	})
	return p
}

// --- the parent ---------------------------------------------------------

func runFleet(processes int, wallet float64, asService bool, latency time.Duration, rpm int) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	root, err := os.MkdirTemp("", "loom-treasury-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)

	fmt.Printf("%d processes · %d tickets each · $%.2f ceiling · one provider account\n",
		processes, ticketsPerProcess, wallet)
	if asService {
		fmt.Printf("quota: an HTTP service in this process\n")
	} else {
		fmt.Printf("quota: a shared directory (%s)\n", root)
	}

	// The projection, before a cent is spent: what one of these processes would
	// cost, and — once the wallet has a balance — whether the fleet can afford
	// it. It issues no model call and writes nothing.
	reg, err := registry(latency, rpm)
	if err != nil {
		return err
	}
	proj, err := loom.Explain(triage("proc-1", ticketsPerProcess), loom.WithRegistry(reg))
	if err != nil {
		return err
	}
	fmt.Printf("explain: one process's ceiling is $%.4f, so %d of them are $%.4f "+
		"against a $%.2f wallet\n\n",
		proj.Ceiling().CostUSD, processes, float64(processes)*proj.Ceiling().CostUSD, wallet)

	alone, err := round(self, root, "alone", processes, wallet, latency, rpm, false, "")
	if err != nil {
		return err
	}

	qdir := filepath.Join(root, "quota")
	url := ""
	if asService {
		stop, addr, err := serve(qdir)
		if err != nil {
			return err
		}
		defer stop()
		url = addr
	}
	together, err := round(self, root, "shared", processes, wallet, latency, rpm, true, url)
	if err != nil {
		return err
	}

	q, err := quota.Open(qdir, quota.Options{})
	if err != nil {
		return err
	}
	defer q.Close()
	buckets, err := q.Models(context.Background())
	if err != nil {
		return err
	}
	fmt.Print(compare(alone, together, buckets, wallet))

	// The projection again, now that the wallet has been spent: the same
	// pipeline, the same ceiling, and an answer that has changed.
	after, err := loom.Explain(triage("proc-1", ticketsPerProcess),
		loom.WithRegistry(reg),
		loom.WithSharedQuota(q, core.Budget{MaxCostUSD: wallet}, 0))
	if err != nil {
		return err
	}
	fmt.Printf("\nand asked again, against the wallet the fleet just spent:\n")
	for _, line := range strings.Split(strings.TrimSpace(after.String()), "\n") {
		if strings.HasPrefix(line, "shared wallet") || strings.HasPrefix(line, "  which") {
			fmt.Printf("  %s\n", strings.TrimSpace(line))
		}
	}
	return nil
}

// serve puts a quota directory behind an HTTP service on a loopback port, so
// the example exercises the backend a fleet spanning hosts would use.
func serve(dir string) (func(), string, error) {
	d, err := quota.Open(dir, quota.Options{})
	if err != nil {
		return nil, "", err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, "", err
	}
	srv := &http.Server{Handler: quota.Handler(d)}
	go srv.Serve(ln)
	stop := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		_ = d.Close()
	}
	return stop, "http://" + ln.Addr().String(), nil
}

// round starts the processes, releases them together, and collects what each
// one spent.
func round(self, root, label string, processes int, wallet float64,
	latency time.Duration, rpm int, shared bool, url string) ([]report, error) {

	dir := filepath.Join(root, label)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	start := filepath.Join(dir, "go")

	cmds := make([]*exec.Cmd, processes)
	names := make([]string, processes)
	for i := range cmds {
		names[i] = fmt.Sprintf("proc-%d", i+1)
		args := []string{"-child",
			"-name", names[i],
			"-quota", filepath.Join(root, "quota"),
			"-quota-url", url,
			"-out", filepath.Join(dir, names[i]+".json"),
			"-ready", filepath.Join(dir, "ready-"+names[i]),
			"-go", start,
			"-wallet", fmt.Sprintf("%g", wallet),
			"-latency", latency.String(),
			"-rpm", fmt.Sprint(rpm),
			fmt.Sprintf("-shared=%t", shared),
		}
		cmd := exec.Command(self, args...)
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		cmds[i] = cmd
	}
	for _, name := range names {
		if !waitFor(filepath.Join(dir, "ready-"+name), time.Minute) {
			return nil, fmt.Errorf("%s never became ready", name)
		}
	}
	if err := os.WriteFile(start, []byte("go"), 0o644); err != nil {
		return nil, err
	}
	for i, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			return nil, fmt.Errorf("%s: %w", names[i], err)
		}
	}

	out := make([]report, processes)
	for i, name := range names {
		blob, err := os.ReadFile(filepath.Join(dir, name+".json"))
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(blob, &out[i]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func compare(alone, shared []report, buckets []quota.ModelState, wallet float64) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%-10s %12s %8s %12s %8s\n", "", "alone($)", "calls", "shared($)", "calls")
	var aloneTotal, sharedTotal float64
	var aloneCalls, sharedCalls int
	for i := range alone {
		fmt.Fprintf(&b, "%-10s %12.4f %8d %12.4f %8d\n",
			alone[i].Name, alone[i].SpentUSD, alone[i].Calls,
			shared[i].SpentUSD, shared[i].Calls)
		aloneTotal += alone[i].SpentUSD
		sharedTotal += shared[i].SpentUSD
		aloneCalls += alone[i].Calls
		sharedCalls += shared[i].Calls
	}
	fmt.Fprintf(&b, "%-10s %12.4f %8d %12.4f %8d\n", "TOTAL",
		aloneTotal, aloneCalls, sharedTotal, sharedCalls)

	fmt.Fprintf(&b, "\nthe ceiling was $%.4f in both rounds.\n", wallet)
	fmt.Fprintf(&b, "  alone:  %d processes × $%.4f = $%.4f spent — %.1f× the ceiling\n",
		len(alone), wallet, aloneTotal, aloneTotal/wallet)
	fmt.Fprintf(&b, "  shared: $%.4f spent, %.0f%% of the ceiling, overrun bounded by the "+
		"calls in flight when it was crossed\n", sharedTotal, 100*sharedTotal/wallet)

	// The bucket, and what it cost to share it. A fleet that never contends
	// reports zero deferrals here, which is the answer that says the rate limit
	// is not the thing slowing it down.
	for _, m := range buckets {
		fmt.Fprintf(&b, "  %s: %d request(s) admitted through one shared bucket, "+
			"%d deferred for want of room, %d returned unissued\n",
			m.Model, m.Admitted, m.Deferred, m.Returned)
	}
	for _, r := range shared {
		if r.Stopped != "" {
			fmt.Fprintf(&b, "  %s stopped: %s\n", r.Name, r.Stopped)
		}
	}
	return b.String()
}

func waitFor(path string, limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}
