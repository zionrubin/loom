package typesafe_test

import (
	"context"
	"errors"
	"testing"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/providers/typesafe"
	"github.com/zionrubin/loom/providers/typesafe/typesafetest"
	"github.com/zionrubin/loom/security"
)

func call(key string) model.CallContext {
	return model.CallContext{TaskID: "t", ResolveSecret: func(ref security.SecretRef) (string, error) {
		if ref != typesafe.DefaultSecretRef {
			return "", errors.New("secret not granted")
		}
		return key, nil
	}}
}

func qs() model.Questions {
	return model.Questions{
		"topic": model.Choice("What is the issue about?", map[string]string{
			"billing": "money problems", "bug": "broken product",
		}),
		"urgent":      model.Noul("Escalate to a human now?"),
		"frustration": model.Score("How frustrated is the customer?", "Calm", "Frustrated", "Very angry"),
	}
}

func TestDecisionOverTheWire(t *testing.T) {
	srv := &typesafetest.Server{
		APIKey: "sk-test",
		Judge: func(state string, q model.Questions) (map[string]model.Answer, error) {
			return map[string]model.Answer{
				"topic":       typesafetest.Choice(map[string]float64{"billing": 9, "bug": 1}),
				"urgent":      typesafetest.Noul(0.87),
				"frustration": typesafetest.Score(q["frustration"].Levels, []float64{0, 19, 1}),
			}, nil
		},
	}
	url, stop := srv.Start()
	defer stop()

	p := typesafe.New(typesafe.WithBaseURL(url))
	if p.Endpoint() != "127.0.0.1" {
		t.Errorf("endpoint = %q, want the bare loopback host", p.Endpoint())
	}
	if c := p.Capabilities(); !c.Decisions || c.Text {
		t.Errorf("capabilities = %+v, want decisions only", c)
	}
	resp, err := p.Complete(context.Background(), call("sk-test"), model.Request{
		Model: typesafe.Jev, Prefix: "Policy: refunds within 30 days.\n",
		Prompt: "Customer: I was charged twice and I am furious.", Questions: qs(),
	})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	a := resp.Answers
	if a["topic"].Choice != "billing" || a["topic"].Confidence <= 0.3 {
		t.Errorf("topic = %+v", a["topic"])
	}
	if a["urgent"].Noul != 0.87 {
		t.Errorf("urgent = %+v", a["urgent"])
	}
	if a["frustration"].Level() != 1 || a["frustration"].Legend["2"] != "Very angry" {
		t.Errorf("frustration = %+v", a["frustration"])
	}
	if resp.Usage.InputTokens == 0 || resp.Text == "" {
		t.Errorf("usage or text missing: %+v", resp)
	}

	got := srv.Requests()
	if len(got) != 1 || got[0].State != "Policy: refunds within 30 days.\nCustomer: I was charged twice and I am furious." {
		t.Fatalf("server saw %+v; the state must be prefix + prompt", got)
	}
	if lv := got[0].Questions["frustration"].Levels; len(lv) != 3 || lv[2] != "Very angry" {
		t.Errorf("score levels did not survive the wire: %v", lv)
	}
	if opts := got[0].Questions["topic"].Options; opts["bug"] != "broken product" {
		t.Errorf("choice options did not survive the wire: %v", opts)
	}
}

func TestErrorsAreClassified(t *testing.T) {
	srv := &typesafetest.Server{APIKey: "sk-test", Failures: []int{429, 503}}
	url, stop := srv.Start()
	defer stop()
	p := typesafe.New(typesafe.WithBaseURL(url))
	ctx := context.Background()
	req := model.Request{Model: typesafe.Jev, Prompt: "x", Questions: qs()}

	for _, want := range []core.FailureClass{core.FailTransient, core.FailTransient} {
		_, err := p.Complete(ctx, call("sk-test"), req)
		if core.ClassOf(err) != want {
			t.Errorf("scripted failure classified %q, want %q (%v)", core.ClassOf(err), want, err)
		}
	}
	if _, err := p.Complete(ctx, call("wrong"), req); core.ClassOf(err) != core.FailPermanent {
		t.Errorf("bad key: %v, want permanent", err)
	}
	bad := model.Request{Model: typesafe.Jev, Prompt: "x",
		Questions: model.Questions{"q": model.Score("x", "only one level")}}
	if _, err := p.Complete(ctx, call("sk-test"), bad); core.ClassOf(err) != core.FailPermanent {
		t.Errorf("malformed schema: %v, want permanent (HTTP 400)", err)
	}
	if _, err := p.Complete(ctx, call("sk-test"), model.Request{Model: typesafe.Jev, Prompt: "write a poem"}); core.ClassOf(err) != core.FailPermanent {
		t.Errorf("free-form prompt: %v, want permanent", err)
	}
	if _, err := p.Complete(ctx, call("sk-test"), req); err != nil {
		t.Errorf("after the scripted failures a call should succeed: %v", err)
	}
}

func TestRegisterDefaults(t *testing.T) {
	reg := model.NewRegistry()
	if err := typesafe.RegisterDefaults(reg, model.Limits{RequestsPerMinute: 600}); err != nil {
		t.Fatal(err)
	}
	info, err := reg.Get(typesafe.Jev)
	if err != nil {
		t.Fatal(err)
	}
	if info.Tier != "" {
		t.Errorf("Jev must not claim a tier a text stage could resolve to, got %q", info.Tier)
	}
	if info.Pricing.OutputPerMTok != 0 || info.Pricing.InputPerMTok != 0.042 {
		t.Errorf("pricing = %+v", info.Pricing)
	}
	if info.InputOverheadTokens != typesafe.RequestOverheadTokens {
		t.Errorf("Jev bills a preamble on every call; registered overhead = %d", info.InputOverheadTokens)
	}
	if info.SecretRef != typesafe.DefaultSecretRef || info.Provider.Endpoint() != typesafe.Endpoint {
		t.Errorf("secret %q endpoint %q", info.SecretRef, info.Provider.Endpoint())
	}
}
