/* loom-survey-scene.jsx — examples/research (pipeline "lit-survey") as one
   continuous composition: 50 papers, ~207 tasks, a branching DAG and two
   ReduceAI trees. Corpus, prompts, stage names, scripted failures and the
   model tiers are taken from examples/research/main.go; the page palette and
   type come from public/index.html. */

const { CompositionStage, useComposition, Captions, Easing, animate, interpolate, clamp } = window;
const { useTweaks, TweaksPanel, TweakSection, TweakToggle, TweakRadio } = window;

const MOTION = {
  enter: Easing.easeOutCubic,
  draw: Easing.easeInOutQuart,
  pop: Easing.easeOutBack,
};

const SANS = 'system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif';
const MONO = 'ui-monospace, "SF Mono", SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace';

const THEMES = {
  dark: {
    bg: '#0c1012', surface: '#131a1d', ink: '#edf3f5', ink2: '#9dabb1', ink3: '#6b787e',
    line: 'rgba(237,243,245,0.13)', line2: 'rgba(237,243,245,0.24)',
    accent: '#6fd3f2', wash: 'rgba(111,211,242,0.10)',
    scout: '#6fd3f2', analyst: '#86d6b0', oracle: '#c4a5f0', warn: '#e8a87c',
    shadow: '0 1px 2px rgba(0,0,0,0.3), 0 10px 34px rgba(0,0,0,0.28)',
  },
  light: {
    bg: '#f1f3f4', surface: '#fafbfb', ink: '#10161a', ink2: '#4d585f', ink3: '#7f8c93',
    line: 'rgba(16,22,26,0.11)', line2: 'rgba(16,22,26,0.20)',
    accent: '#06758f', wash: 'rgba(6,117,143,0.07)',
    scout: '#06627a', analyst: '#1a6f52', oracle: '#7a4bb5', warn: '#9a4a1f',
    shadow: '0 1px 2px rgba(16,22,26,0.04), 0 8px 28px rgba(16,22,26,0.05)',
  },
};

/* ── the corpus (examples/research/main.go) ────────────────────────────── */
const AREAS = ['agent planning and search', 'long-horizon memory', 'tool use and skill acquisition',
  'evaluation and benchmarks', 'safety and oversight', 'multi-agent coordination'];
const CORPUS = [
  ['Monte-Carlo Plan Repair for Long-Horizon Agent Tasks', 'NeurIPS', 0],
  ['Hierarchical Task Decomposition with Learned Subgoal Critics', 'ICML', 0],
  ['Toward Provably Bounded Search in Open-Ended Planning', 'arXiv', 0],
  ['Test-Time Deliberation Budgets for Agentic Search', 'ICLR', 0],
  ['Backtracking Transformers: Plan Revision as a First-Class Operation', 'NeurIPS', 0],
  ['World-Model Rollouts versus Tree Search in Embodied Agents', 'CoRL', 0],
  ['Anytime Replanning under Partial Observability in Web Agents', 'ICML', 0],
  ['A Trillion-Token Ablation of Context Compression Strategies', 'arXiv', 1, 'straggler'],
  ['Retracted: Infinite Context Windows via Recursive Summarization', 'arXiv', 1, 'dead'],
  ['Episodic Memory Consolidation in Continually Deployed Agents', 'NeurIPS', 1],
  ['Sleep-Time Compute: Offline Memory Reorganization for Agents', 'ICLR', 1],
  ['Toward Lossless Semantic Compression of Agent Trajectories', 'arXiv', 1],
  ['Vector Stores Considered Insufficient: A Case for Structured Recall', 'ICML', 1],
  ['Forgetting as a Feature: Selective Memory Decay in Agent Fleets', 'ICLR', 1],
  ['Distilling Tool-Use Trajectories into Reusable Skills', 'NeurIPS', 2, 'garbleExtract'],
  ['Zero-Shot API Composition from Natural-Language Contracts', 'ICLR', 2],
  ['Sandboxed Execution Feedback Improves Code-Agent Reliability', 'ICML', 2],
  ['Toward Self-Verifying Tool Calls in Production Agents', 'arXiv', 2],
  ['Learning When Not to Use a Tool', 'NeurIPS', 2],
  ['Typed Capability Grants for Model-Driven Automation', 'IEEE S&P', 2],
  ['Curriculum Discovery of Composite Skills in Software Agents', 'ICLR', 2],
  ['Self-Rewarding Agents Trained on Synthetic Preferences', 'ICML', 3, 'badGrade'],
  ['Benchmarks That Fight Back: Adversarially Refreshed Evaluation', 'NeurIPS', 3],
  ['Measuring Silent Regressions in Multi-Step Agent Workflows', 'ICLR', 3],
  ['Toward Calibrated Confidence in Agent Self-Reports', 'arXiv', 3],
  ['Pass@k Is Not Reliability: Metrics for Deployed Agents', 'ICML', 3],
  ['Holdout Contamination in Web-Scale Agent Training', 'NeurIPS', 3],
  ['Cost-Normalized Scoring for Model Escalation Ladders', 'ICLR', 3],
  ['Emergent Deception in Self-Improving Agent Populations', 'NeurIPS', 4, 'garbleScreen'],
  ['Least-Privilege Envelopes for Autonomous Task Execution', 'IEEE S&P', 4],
  ['Auditable Lineage for Model-Generated Artifacts', 'USENIX Security', 4],
  ['Toward Interruptibility Guarantees in Recursive Self-Improvement', 'arXiv', 4],
  ['Reward Hacking under Distribution Shift: A Field Study', 'ICML', 4],
  ['Sandbagging Detection via Cross-Model Interrogation', 'NeurIPS', 4],
  ['Constitutional Constraints Survive Fine-Tuning, Mostly', 'ICLR', 4],
  ['Emergent Division of Labor in Heterogeneous Agent Teams', 'AAMAS', 5],
  ['Consensus Protocols for Redundant Model Verification', 'NeurIPS', 5],
  ['Toward Market Mechanisms for Compute Allocation among Agents', 'arXiv', 5],
  ['Adversarial Verification Panels Reduce Confabulated Findings', 'ICML', 5],
  ['Swarm Curricula: Population-Level Exploration for Agent Fleets', 'ICLR', 5],
  ['Cheap Talk and Costly Signals in Agent Negotiation', 'AAMAS', 5],
  ['Failure Cascades in Pipelined Agent Systems', 'SOSP', 5],
  ['Sediment Transport in Tidal Estuaries: A Decade of Lidar', 'AGU', -1],
  ['Perovskite Solar Cell Degradation under Humid Cycling', 'Joule', -1],
  ['Acoustic Niches of Coral Reef Fish Communities', 'Ecology Letters', -1],
  ['Bronze Age Trade Networks of the Aegean: New Isotope Evidence', 'Antiquity', -1],
  ['Gut Microbiome Succession in Preterm Infants', 'Cell Host & Microbe', -1],
  ['Urban Heat Island Mitigation via Reflective Roofing', 'Nature Cities', -1],
  ['Recursive Self-Improvement: A Position Paper', 'preprint', 4, 'stub'],
  ['Notes on Agent Benchmarking', 'preprint', 3, 'stub'],
];
const RETRY_AT = { 2: 1, 10: 1, 18: 1, 26: 1, 33: 1, 39: 1 }; // 6 scripted 429/503s

function fnv(s) {
  let h = 2166136261 >>> 0;
  for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619) >>> 0; }
  return (h >>> 0) % 1000;
}
const firstWords = (s, n) => s.trim().split(/\s+/).slice(0, n).join(' ');
const rnd = (i, salt) => (((Math.sin(i * 12.9898 + salt * 78.233) * 43758.5453) % 1) + 1) % 1;

const PAPERS = CORPUS.map((c, i) => {
  const title = c[0], flag = c[3] || '';
  const gain = 4 + fnv(title) % 19;
  const headline = firstWords(title, 6) + ': +' + gain + '% task success, stable across scales';
  return {
    i, id: 'p' + String(i + 1).padStart(2, '0'), title, venue: c[1],
    area: c[2] >= 0 ? AREAS[c[2]] : '', stub: flag === 'stub', flag,
    retry: !!RETRY_AT[i], gain, headline,
    grade: 2 + fnv(headline) % 4,
    nFind: 2 + (fnv(title) % 2 === 0 ? 1 : 0) + (title.indexOf('Toward') === 0 ? 1 : 0),
  };
});
const SCREENED = PAPERS.filter((p) => !p.stub);                       // 48
const RELEVANT = SCREENED.filter((p) => p.area && p.flag !== 'dead'); // 41
const STRONG = RELEVANT.filter((p) => p.grade >= 3);
const CLAIMS = RELEVANT.reduce((n, p) => n + p.nFind - (p.title.indexOf('Toward') === 0 ? 1 : 0), 0);
const treeOf = (n, f) => { const L = [n]; while (n > 1) { n = Math.ceil(n / f); L.push(n); } return L; };
const SYN = treeOf(STRONG.length, 4);      // 28 → 7 → 2 → 1
const OQ = treeOf(CLAIMS, 6);              // 98 → 17 → 3 → 1
const TASKS = PAPERS.length + SCREENED.length + RELEVANT.length * 3
  + (SYN[1] + SYN[2] + SYN[3]) + 1 + (OQ[1] + OQ[2] + OQ[3]);   // 50+48+123+10+1+21

const ABSTRACT = 'This survey of 41 papers finds that self-improvement gains are real but uneven: '
  + 'planning, tool use, and multi-agent verification show replicated double-digit improvements, '
  + 'while long-horizon memory remains bottlenecked on consolidation. Safety findings — emergent '
  + 'deception, reward hacking, and sandbagging — replicate across labs and argue for auditable '
  + 'lineage and least-privilege execution as defaults.';
const QUESTIONS = 'From ' + CLAIMS + ' claims, three open questions: (1) do reported gains survive '
  + 'distribution shift off-benchmark? (2) do memory consolidation and plan search compose, or compete '
  + 'for the same context budget? (3) which invariants keep self-improvement auditable as capability grows?';

const CMD = 'LOOM_STATE=/tmp/loom-research go run ./examples/research';

/* ── world geometry ───────────────────────────────────────────────────── */
const W = { w: 6300, h: 1080 };
const G = {
  paper: { x: 120, y: 140, cw: 208, ch: 88, w: 192, h: 74, cols: 5 },
  norm: { x: 1240, y: 180, w: 240, h: 760 },
  screenable: { x: 1600 },
  exec: { x: 1760, y: 66, gap: 100 },
  screen: { x: 1760, y: 250, cw: 100, ch: 108, s: 76, cols: 8 },
  gate1: { x: 2760 },
  find: { x: 3000, y: 250, cw: 110, ch: 98, w: 92, h: 76, cols: 6 },
  split: { x: 3710 },
  grade: { x: 3800, y: 120, cw: 62, ch: 58, w: 46, h: 42, cols: 8 },
  gate2: { x: 4340 },
  syn: { x: 4460, y: 130, cw: 46, ch: 48, cols: 4, l1: 4780, l2: 4940, l3: 5080 },
  abs: { x: 5200, y: 130, w: 600, h: 380 },
  claim: { x: 3800, y: 628, cw: 38, ch: 34, cols: 14 },
  oq: { l1: 4560, l2: 4760, l3: 4920 },
  out: { x: 5200, y: 628, w: 600, h: 340 },
  bcast: { y: -430, h: 300 },
};

/* ── the findings gate (loom.WithFindings) ────────────────────────────────
   Tier names and totals from docs/FINDINGS.md §9 / examples/commons:
   24 asked · 18 reused (75%) · 6 researched — exact 0, class 12, near 0,
   coalesced 6. Here the six subjects are the six areas of the corpus, so the
   calls that do go out are the ones the 50 paper records come back from. */
const SHORT = ['planning', 'memory', 'tool use', 'evaluation', 'safety', 'multi-agent'];
const PHRASINGS = [
  (a) => 'new results in ' + a + ' since 2024?',
  (a) => a + ': strongest evidence this year',
  (a) => 'key papers on ' + a,
  (a) => 'who publishes on ' + a + '?',
];
const GT = {
  qx: -2080, qy: 44, qw: 420, rh: 36, gg: 24,
  sx: -1470, sw: 300, sy: 60, sh: 980,
  cx: -1040, cy: 690, cw: 380, ch: 210,
  rail: -600, mx: -2080, my: 1130, mw: 1580,
};
const gGroupY = (s) => GT.qy + s * (4 * GT.rh + GT.gg);
const ASKS = [];
AREAS.forEach((area, s) => PHRASINGS.forEach((f, d) => {
  const verdict = d === 0 ? 'research' : d === 1 ? 'coalesced' : 'class';
  const lead = 0.30 + s * 0.13;
  const t0 = d <= 1 ? lead : 1.95 + s * 0.15 + (d - 2) * 0.30;
  const lane = gGroupY(s) + d * GT.rh + GT.rh / 2;
  const a = { k: ASKS.length, s, d, verdict, lane, t0, lead, text: f(SHORT[s]) };
  a.ts = verdict === 'research' ? [t0, t0 + 0.5, t0 + 0.95, t0 + 1.5, t0 + 2.0, t0 + 2.45]
    : verdict === 'coalesced' ? [t0, t0 + 0.5, lead + 2.0, lead + 2.35, lead + 2.8]
      : [t0, t0 + 0.45, t0 + 0.85, t0 + 1.35];
  a.xs = verdict === 'research' ? [-1650, -1470, -1170, -1020, -660, GT.rail]
    : verdict === 'coalesced' ? [-1650, -1470, -1470, -1170, GT.rail]
      : [-1650, -1470, -1170, GT.rail];
  a.ys = verdict === 'research' ? [a.lane, a.lane, a.lane, 795, 795, a.lane] : a.xs.map(() => a.lane);
  a.arrive = a.ts[a.ts.length - 1];
  ASKS.push(a);
}));
const GHUE = (v, P) => (v === 'research' ? P.oracle : v === 'coalesced' ? P.analyst : P.accent);

/* run-level broadcasts (examples/research/main.go: loom.WithBroadcast) */
const BCASTS = [
  { name: 'venue-tiers', hash: 'b1f04c7e', body: '18 venues → tier + weight',
    reader: 'normalize', tasks: PAPERS.length, x: 620, tx: 1370, ty: 170, at: 0.9 },
  { name: 'inclusion-criteria', hash: '7a2e9d31', body: 'year ≥ 2024 · in-scope area',
    reader: 'screen', tasks: SCREENED.length, x: 1760, tx: 2000, ty: 236, at: 1.9 },
  { name: 'evidence-rubric', hash: '4c8bb0a6', body: '1–5 scale · replication, sample, ablation, effect size',
    reader: 'grade-evidence', tasks: RELEVANT.length, x: 2900, tx: 3980, ty: 120, at: 3.0 },
];
const pcell = (i) => ({ x: G.paper.x + (i % G.paper.cols) * G.paper.cw, y: G.paper.y + Math.floor(i / G.paper.cols) * G.paper.ch });
const scell = (k) => ({ x: G.screen.x + (k % G.screen.cols) * G.screen.cw, y: G.screen.y + Math.floor(k / G.screen.cols) * G.screen.ch });
const fcell = (k) => ({ x: G.find.x + (k % G.find.cols) * G.find.cw, y: G.find.y + Math.floor(k / G.find.cols) * G.find.ch });
const gcell = (k) => ({ x: G.grade.x + (k % G.grade.cols) * G.grade.cw, y: G.grade.y + Math.floor(k / G.grade.cols) * G.grade.ch });
const ccell = (k) => ({ x: G.claim.x + (k % G.claim.cols) * G.claim.cw, y: G.claim.y + Math.floor(k / G.claim.cols) * G.claim.ch });

/* screen-stage schedule: waves of 8 (loom.WithWorkers(8)) */
const SCHED = SCREENED.map((p, k) => {
  const start = 0.30 + Math.floor(k / 8) * 0.62 + (k % 8) * 0.05;
  let dur = 0.55 + rnd(k, 3) * 0.35;
  if (p.flag === 'straggler') dur = 3.6;
  if (p.flag === 'garbleScreen') dur = 1.9;
  if (p.retry) dur += 0.7;
  return { p, k, start, end: start + dur, dur };
});

function StageLabel({ x, y, name, sub, P, o, w }) {
  return (
    <div style={{ position: 'absolute', left: x, top: y, width: w || 300, opacity: o, borderTop: '1px solid ' + P.line2, paddingTop: 12 }}>
      <div style={{ fontFamily: SANS, fontSize: 30, fontWeight: 600, letterSpacing: '-0.022em', color: P.ink }}>{name}</div>
      <div style={{ marginTop: 3, fontFamily: MONO, fontSize: 22, color: P.ink3 }}>{sub}</div>
    </div>
  );
}

/* ── region 1: the corpus ─────────────────────────────────────────────── */
function GateRegion({ T, C, P, out }) {
  const A = C.Ask, R = C.Reuse, t = T - R;
  const lab = animate({ from: 0, to: 1, start: A + 0.1, end: A + 0.8, ease: MOTION.enter })(T);
  const railO = animate({ from: 0, to: 1, start: R + 1.0, end: R + 1.7, ease: MOTION.enter })(T);
  const cardO = animate({ from: 0, to: 1, start: R + 0.3, end: R + 0.9, ease: MOTION.pop })(T);
  const link = animate({ from: 0, to: 1, start: C.Corpus - 0.8, end: C.Corpus + 0.5, ease: MOTION.draw })(T);
  const dim = 1 - 0.5 * animate({ from: 0, to: 1, start: C.Ledger, end: C.Ledger + 1.2, ease: MOTION.draw })(T);

  const nClass = ASKS.filter((a) => a.verdict === 'class' && t >= a.arrive).length;
  const nCoal = ASKS.filter((a) => a.verdict === 'coalesced' && t >= a.arrive).length;
  const nSrc = ASKS.filter((a) => a.verdict === 'research' && t >= a.t0 + 1.5).length;
  const nAsk = ASKS.filter((a) => t >= a.t0).length;
  const LEADS = ASKS.filter((a) => a.verdict === 'research');

  const ROWS = [
    ['exact', 'canonical question key', 0, P.accent, false],
    ['class', 'topic + facets', nClass, P.accent, false],
    ['near', 'embedding · not configured', 0, P.accent, false],
    ['lease', 'single-flight, concurrent askers', nCoal, P.analyst, true],
    ['miss', 'reaches the public source', nSrc, P.oracle, false],
  ];
  const stat = (n, label, col) => (
    <div>
      <div style={{ fontFamily: MONO, fontSize: 84, lineHeight: 1, color: col, fontVariantNumeric: 'tabular-nums' }}>{n}</div>
      <div style={{ marginTop: 10, fontFamily: SANS, fontSize: 32, letterSpacing: '-0.014em', color: P.ink2 }}>{label}</div>
    </div>
  );
  const sep = (ch) => <div style={{ fontFamily: MONO, fontSize: 52, color: P.ink3, alignSelf: 'center', paddingTop: 18 }}>{ch}</div>;

  return (
    <div style={{ position: 'absolute', inset: 0, opacity: out * dim }}>
      <StageLabel x={GT.qx} y={-58} name="research questions" sub="4 desks · 6 subjects · own phrasing" P={P} o={lab} w={520} />
      <StageLabel x={GT.sx} y={-58} name="findings gate" sub="loom.WithFindings · question-keyed" P={P} o={lab} w={430} />

      {AREAS.map((area, s) => {
        const gy = gGroupY(s);
        const o = animate({ from: 0, to: 1, start: A + 0.30 + s * 0.42, end: A + 0.80 + s * 0.42, ease: MOTION.enter })(T);
        return (
          <div key={'g' + s} style={{ opacity: o }}>
            <div style={{ position: 'absolute', left: GT.qx, top: gy - 21, fontFamily: MONO, fontSize: 16, color: P.ink3 }}>
              subject {s + 1} · {SHORT[s]} · 4 wordings
            </div>
            <div style={{ position: 'absolute', left: GT.qx - 12, top: gy, width: 2, height: 4 * GT.rh - 6, background: P.line2 }} />
          </div>
        );
      })}

      {ASKS.map((a) => {
        const y = gGroupY(a.s) + a.d * GT.rh;
        const inn = animate({ from: 0, to: 1, start: A + 0.35 + a.s * 0.42 + a.d * 0.09, end: A + 0.85 + a.s * 0.42 + a.d * 0.09, ease: MOTION.enter })(T);
        const sent = clamp((t - a.t0) * 3, 0, 1);
        const res = clamp((t - a.arrive) * 3, 0, 1);
        const hue = GHUE(a.verdict, P);
        const tag = a.verdict === 'research' ? 'researched' : a.verdict === 'coalesced' ? 'coalesced' : 'class hit';
        return (
          <div key={'c' + a.k} style={{
            position: 'absolute', left: GT.qx, top: y, width: GT.qw, height: GT.rh - 6,
            boxSizing: 'border-box', background: P.surface, borderRadius: 6,
            border: '1px solid ' + (res > 0.5 ? hue : P.line), padding: '0 10px',
            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
            opacity: inn * (1 - 0.45 * (sent - res)), boxShadow: P.shadow,
          }}>
            <span style={{ fontFamily: MONO, fontSize: 16, color: P.ink2, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{a.text}</span>
            <span style={{ fontFamily: MONO, fontSize: 14, color: hue, opacity: res, paddingLeft: 8, whiteSpace: 'nowrap' }}>{tag}</span>
          </div>
        );
      })}

      <div style={{
        position: 'absolute', left: GT.sx, top: GT.sy, width: GT.sw, height: GT.sh, boxSizing: 'border-box',
        background: P.surface, border: '1px solid ' + P.line, borderRadius: 12, boxShadow: P.shadow,
        opacity: lab, padding: '26px 20px',
      }}>
        {ROWS.map(([name, sub, n, col, rule], i) => (
          <div key={name} style={{
            marginBottom: 92, paddingTop: rule ? 20 : 0, borderTop: rule ? '1px solid ' + P.line : 'none',
            opacity: animate({ from: 0, to: 1, start: A + 0.5 + i * 0.14, end: A + 1.0 + i * 0.14, ease: MOTION.enter })(T),
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
              <span style={{ fontFamily: MONO, fontSize: 24, color: n > 0 ? col : P.ink3 }}>{name}</span>
              <span style={{ fontFamily: MONO, fontSize: 30, color: n > 0 ? col : P.ink3, fontVariantNumeric: 'tabular-nums' }}>{n}</span>
            </div>
            <div style={{ marginTop: 4, fontFamily: MONO, fontSize: 17, color: P.ink3 }}>{sub}</div>
          </div>
        ))}
        <div style={{ position: 'absolute', left: 20, right: 20, bottom: 22, fontFamily: MONO, fontSize: 17, color: P.ink3 }}>
          gate overhead 47µs · question
        </div>
      </div>

      <div style={{
        position: 'absolute', left: GT.cx, top: GT.cy, width: GT.cw, height: GT.ch, boxSizing: 'border-box',
        background: P.surface, border: '1px solid ' + (nSrc > 0 ? P.oracle : P.line), borderRadius: 12,
        boxShadow: P.shadow, opacity: lab, padding: '18px 18px',
      }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
          <span style={{ fontFamily: MONO, fontSize: 22, color: P.oracle }}>mcp/web/search</span>
          <span style={{ fontFamily: MONO, fontSize: 17, color: P.ink3 }}>public source</span>
        </div>
        <div style={{ marginTop: 8, fontFamily: MONO, fontSize: 17, color: P.ink3 }}>120ms · billed per query</div>
        <div style={{ position: 'absolute', left: 18, bottom: 62, fontFamily: MONO, fontSize: 22, color: P.oracle, fontVariantNumeric: 'tabular-nums' }}>
          {nSrc} of {ASKS.length} questions
        </div>
        <div style={{ position: 'absolute', left: 18, right: 18, bottom: 20, display: 'flex', gap: 8 }}>
          {LEADS.map((a, i) => (
            <span key={i} style={{
              flex: 1, height: 26, borderRadius: 5, boxSizing: 'border-box',
              border: '1.5px solid ' + (t >= a.t0 + 1.5 ? P.oracle : P.line2),
              background: t >= a.t0 + 1.5 ? P.wash : 'transparent',
            }} />
          ))}
        </div>
      </div>

      <Gate x={GT.rail} y0={180} y1={1020} name="findings" sub={'24 covered · ' + nSrc + ' researched'} P={P} o={railO} T={T} />

      {ASKS.map((a) => {
        const x = interpolate(a.ts, a.xs, MOTION.enter)(t);
        const y = interpolate(a.ts, a.ys, MOTION.enter)(t);
        const fade = clamp((t - a.t0) * 8, 0, 1) * (1 - clamp((t - a.arrive) * 4, 0, 1));
        const wait = a.verdict === 'coalesced'
          ? clamp((t - (a.t0 + 0.52)) * 5, 0, 1) * (1 - clamp((t - (a.lead + 2.0)) * 5, 0, 1)) : 0;
        return (
          <div key={'d' + a.k} style={{
            position: 'absolute', left: x - 8, top: y - 8, width: 16, height: 16, borderRadius: 8,
            background: GHUE(a.verdict, P), opacity: t >= a.t0 - 0.02 ? fade : 0,
          }}>
            <span style={{ position: 'absolute', inset: -8, borderRadius: 16, border: '1.5px dashed ' + P.analyst, opacity: wait * 0.9 }} />
          </div>
        );
      })}

      <div style={{
        position: 'absolute', left: GT.mx, top: GT.my, width: GT.mw, boxSizing: 'border-box',
        background: P.surface, border: '1px solid ' + P.line, borderRadius: 16, boxShadow: P.shadow,
        padding: '30px 36px 34px', opacity: cardO,
        transform: 'scale(' + (0.97 + 0.03 * cardO).toFixed(3) + ')', transformOrigin: 'left top',
      }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
          <span style={{ fontFamily: MONO, fontSize: 34, color: P.accent }}>findings</span>
          <span style={{ fontFamily: MONO, fontSize: 28, color: P.ink3 }}>loom.WithFindings · Gate: mcp/web/search</span>
        </div>
        <div style={{ marginTop: 24, display: 'flex', gap: 40 }}>
          {stat(nAsk, 'research questions', P.ink)}
          {sep('→')}
          {stat(nSrc, 'external source calls', P.oracle)}
          {sep('·')}
          {stat(nClass + nCoal, 'reused / coalesced', P.accent)}
        </div>
        <div style={{
          marginTop: 26, paddingTop: 20, borderTop: '1px solid ' + P.line,
          display: 'flex', justifyContent: 'space-between', alignItems: 'baseline',
        }}>
          <span style={{ fontFamily: MONO, fontSize: 28, color: P.ink3 }}>
            exact 0 · class {nClass} · near 0 · coalesced {nCoal}
          </span>
          <span style={{ fontFamily: MONO, fontSize: 28, color: P.ink3 }}>$0.0960 → $0.0240 · 415ms → 171ms</span>
        </div>
      </div>

      <svg width="2900" height="1200" style={{ position: 'absolute', left: -2200, top: 0, overflow: 'visible' }}>
        {[[250, 250], [470, 430], [700, 700], [910, 930]].map(([y0, y1], i) => {
          const p = clamp(link * 1.12 - i * 0.06, 0, 1);
          const x1 = GT.rail + 2200, x2 = G.paper.x + 2200;
          const len = Math.hypot(x2 - x1, y1 - y0) * 1.15;
          return <path key={i}
            d={'M ' + x1 + ' ' + y0 + ' C ' + (x1 + 230) + ' ' + y0 + ', ' + (x2 - 230) + ' ' + y1 + ', ' + x2 + ' ' + y1}
            fill="none" stroke={P.line2} strokeWidth="1.4" strokeDasharray={len} strokeDashoffset={len * (1 - p)} opacity="0.9" />;
        })}
      </svg>
      <div style={{
        position: 'absolute', left: GT.rail + 120, top: 1055, width: 700, opacity: link,
        fontFamily: MONO, fontSize: 21, color: P.ink3,
      }}>{nSrc} source calls → {PAPERS.length} paper records</div>
    </div>
  );
}

/* ── region 1: the corpus ─────────────────────────────────────────────── */
function PaperGrid({ T, C, P, out }) {
  return (
    <div style={{ position: 'absolute', inset: 0, opacity: out }}>
      <StageLabel x={G.paper.x} y={54} name="papers" sub={'FromRecords · ' + PAPERS.length + ' records'} P={P} o={animate({ from: 0, to: 1, start: C.Corpus + 0.1, end: C.Corpus + 0.8, ease: MOTION.enter })(T)} w={420} />
      {PAPERS.map((p) => {
        const c = pcell(p.i);
        const inn = animate({ from: 0, to: 1, start: C.Corpus + 0.25 + p.i * 0.042, end: C.Corpus + 0.85 + p.i * 0.042, ease: MOTION.enter })(T);
        const dropped = p.stub ? animate({ from: 1, to: 0, start: C.Normalize + 2.6, end: C.Normalize + 3.3, ease: MOTION.draw })(T) : 1;
        const sent = animate({ from: 0, to: 1, start: C.Normalize + 0.3 + (p.i % 8) * 0.04, end: C.Normalize + 1.3, ease: MOTION.enter })(T);
        return (
          <div key={p.id} style={{
            position: 'absolute', left: c.x, top: c.y, width: G.paper.w, height: G.paper.h,
            boxSizing: 'border-box', background: P.surface, border: '1px solid ' + (p.stub ? P.warn : P.line),
            borderRadius: 8, padding: '9px 11px', opacity: inn * dropped * (1 - 0.55 * sent),
            transform: 'translateY(' + ((1 - inn) * 12 + (1 - dropped) * 26).toFixed(1) + 'px)', boxShadow: P.shadow,
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
              <span style={{ fontFamily: MONO, fontSize: 17, color: p.stub ? P.warn : P.accent }}>{p.id}</span>
              <span style={{ fontFamily: MONO, fontSize: 15, color: P.ink3 }}>{p.stub ? 'no abstract' : p.venue}</span>
            </div>
            <div style={{
              marginTop: 3, fontFamily: SANS, fontSize: 18, lineHeight: 1.2, color: P.ink,
              letterSpacing: '-0.012em', display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical', overflow: 'hidden',
            }}>{p.title}</div>
          </div>
        );
      })}
    </div>
  );
}

function BroadcastBand({ T, C, P, out }) {
  const lab = animate({ from: 0, to: 1, start: C.Broadcast + 0.1, end: C.Broadcast + 0.7, ease: MOTION.enter })(T);
  const settle = animate({ from: 1, to: 0.42, start: C.Screen - 0.6, end: C.Screen + 0.6, ease: MOTION.draw })(T);
  return (
    <div style={{ position: 'absolute', inset: 0, opacity: out }}>
      <div style={{
        position: 'absolute', left: 620, top: G.bcast.y - 92, opacity: lab,
        fontFamily: MONO, fontSize: 46, color: P.ink3, whiteSpace: 'nowrap',
      }}>
        loom.WithBroadcast — registered once per run, referenced by content hash
      </div>
      <svg width={W.w} height="2600" style={{ position: 'absolute', left: 0, top: -400, overflow: 'visible' }}>
        <defs>
          {BCASTS.map((b, i) => {
            const p = animate({ from: 0, to: 1, start: C.Broadcast + b.at + 0.35, end: C.Broadcast + b.at + 1.25, ease: MOTION.draw })(T);
            const y1 = G.bcast.y + G.bcast.h + 400;
            const y2 = b.ty + 400;
            return (
              <clipPath key={i} id={'bcw' + i}>
                <rect x={Math.min(b.x + 500, b.tx) - 40} y={y1 - 2} width="1200" height={Math.max(0.001, (y2 - y1 + 6) * p)} />
              </clipPath>
            );
          })}
        </defs>
        {BCASTS.map((b, i) => {
          const x1 = b.x + 500, y1 = G.bcast.y + G.bcast.h + 400;
          const x2 = b.tx, y2 = b.ty + 400;
          return (
            <path key={i} clipPath={'url(#bcw' + i + ')'}
              d={'M ' + x1 + ' ' + y1 + ' C ' + x1 + ' ' + (y1 + 70) + ', ' + x2 + ' ' + (y2 - 80) + ', ' + x2 + ' ' + y2}
              fill="none" stroke={P.accent} strokeWidth="1.4" strokeDasharray="7 7"
              opacity={0.26 + 0.46 * settle} />
          );
        })}
      </svg>
      {BCASTS.map((b, i) => {
        const a = animate({ from: 0, to: 1, start: C.Broadcast + b.at + 1.05, end: C.Broadcast + b.at + 1.5, ease: MOTION.pop })(T);
        return (
          <div key={'a' + i} style={{
            position: 'absolute', left: b.tx - 14, top: b.ty - 14, width: 28, height: 28, borderRadius: 14,
            boxSizing: 'border-box', border: '2px solid ' + P.accent, background: P.bg,
            opacity: a * (0.45 + 0.55 * settle), transform: 'scale(' + (0.6 + 0.4 * a).toFixed(3) + ')',
          }} />
        );
      })}
      {BCASTS.map((b, i) => {
        const inn = animate({ from: 0, to: 1, start: C.Broadcast + b.at, end: C.Broadcast + b.at + 0.6, ease: MOTION.pop })(T);
        const hot = b.name === 'evidence-rubric'
          ? animate({ from: 0, to: 1, start: C.Grade + 0.2, end: C.Grade + 0.8, ease: MOTION.enter })(T)
            * (1 - animate({ from: 0, to: 1, start: C.Grade + 2.9, end: C.Grade + 3.6, ease: MOTION.draw })(T))
          : b.name === 'inclusion-criteria'
            ? animate({ from: 0, to: 1, start: C.Screen + 0.1, end: C.Screen + 0.7, ease: MOTION.enter })(T)
              * (1 - animate({ from: 0, to: 1, start: C.Relevant, end: C.Relevant + 0.8, ease: MOTION.draw })(T))
            : 0;
        return (
          <div key={b.name} style={{
            position: 'absolute', left: b.x, top: G.bcast.y, width: 1000, height: G.bcast.h,
            boxSizing: 'border-box', background: P.surface, borderRadius: 16, padding: '28px 32px',
            border: '1px solid ' + (hot > 0.4 ? P.accent : P.line), boxShadow: P.shadow,
            opacity: inn * (0.5 + 0.5 * settle + 0.5 * hot),
            transform: 'scale(' + (0.94 + 0.06 * inn).toFixed(3) + ')', transformOrigin: 'center bottom',
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
              <span style={{ fontFamily: MONO, fontSize: 48, color: hot > 0.4 ? P.accent : P.ink }}>{b.name}</span>
              <span style={{ fontFamily: MONO, fontSize: 36, color: P.ink3 }}>sha256:{b.hash}</span>
            </div>
            <div style={{ marginTop: 18, fontFamily: SANS, fontSize: 42, lineHeight: 1.25, color: P.ink2, letterSpacing: '-0.014em' }}>{b.body}</div>
            <div style={{ position: 'absolute', left: 32, right: 32, bottom: 24, display: 'flex', justifyContent: 'space-between' }}>
              <span style={{ fontFamily: MONO, fontSize: 38, color: P.ink3 }}>{b.reader}</span>
              <span style={{ fontFamily: MONO, fontSize: 38, color: P.accent }}>1 copy · {b.tasks} tasks</span>
            </div>
          </div>
        );
      })}
    </div>
  );
}

/* ── region 1b: normalize + screenable (plain code, 50 → 48) ─────────── */
function NormalizeStage({ T, C, P, out }) {
  const n = G.norm;
  const lab = animate({ from: 0, to: 1, start: C.Normalize - 0.5, end: C.Normalize + 0.2, ease: MOTION.enter })(T);
  const gate = animate({ from: 0, to: 1, start: C.Normalize + 1.7, end: C.Normalize + 2.3, ease: MOTION.enter })(T);
  const kept = animate({ from: 0, to: 1, start: C.Normalize + 3.1, end: C.Normalize + 3.7, ease: MOTION.pop })(T)
    * (1 - animate({ from: 0, to: 1, start: C.Screen + 0.4, end: C.Screen + 1.2, ease: MOTION.draw })(T));
  const ROWS = ['venue → tier + weight', 'year, authors, area', 'abstract present?', 'field: screenable'];
  return (
    <div style={{ position: 'absolute', inset: 0, opacity: out }}>
      <StageLabel x={n.x} y={-20} name="normalize" sub={'Map · ' + PAPERS.length + ' tasks · no model call'} P={P} o={lab} w={440} />
      <div style={{
        position: 'absolute', left: n.x, top: n.y, width: n.w, height: n.h, boxSizing: 'border-box',
        background: P.surface, border: '1px solid ' + P.line, borderRadius: 12, boxShadow: P.shadow,
        opacity: lab, padding: '22px 18px',
      }}>
        {ROWS.map((r, i) => (
          <div key={r} style={{
            marginBottom: 15, fontFamily: MONO, fontSize: 19, lineHeight: 1.25,
            color: i === 3 ? P.accent : P.ink3,
            opacity: animate({ from: 0, to: 1, start: C.Normalize + 0.25 + i * 0.16, end: C.Normalize + 0.75 + i * 0.16, ease: MOTION.enter })(T),
          }}>{r}</div>
        ))}
      </div>
      <Gate x={G.screenable.x} y0={200} y1={960} name="screenable" sub={'Filter · ' + PAPERS.length + ' → ' + SCREENED.length} P={P} o={gate} T={T} />
      <div style={{
        position: 'absolute', left: G.screenable.x - 170, top: 986, width: 340, textAlign: 'center',
        fontFamily: MONO, fontSize: 21, color: P.warn, opacity: kept,
      }}>2 papers have no abstract</div>
      {PAPERS.map((p) => {
        const src = pcell(p.i);
        const lane = n.y + 26 + (p.i / (PAPERS.length - 1)) * (n.h - 52);
        const inX = n.x, outX = n.x + n.w;
        const k = SCREENED.indexOf(p);
        const dst = k >= 0
          ? { x: scell(k).x + G.screen.s / 2, y: scell(k).y + G.screen.s / 2 }
          : { x: G.screenable.x + 24, y: lane };
        const q = animate({ from: 0, to: 1, start: C.Normalize + 0.25 + p.i * 0.03, end: C.Normalize + 2.5 + p.i * 0.03, ease: MOTION.draw })(T);
        let x, y;
        if (q < 0.4) {
          const u = q / 0.4;
          x = src.x + G.paper.w / 2 + (inX - src.x - G.paper.w / 2) * u;
          y = src.y + G.paper.h / 2 + (lane - src.y - G.paper.h / 2) * u;
        } else if (q < 0.62) {
          const u = (q - 0.4) / 0.22;
          x = inX + (outX - inX) * u; y = lane;
        } else {
          const u = (q - 0.62) / 0.38;
          x = outX + (dst.x - outX) * u; y = lane + (dst.y - lane) * u;
        }
        const fade = k >= 0 ? 1 - clamp((q - 0.96) * 26, 0, 1) : 1 - clamp((q - 0.74) / 0.16, 0, 1);
        return (
          <div key={p.id} style={{
            position: 'absolute', left: x - 8, top: y - 8, width: 16, height: 16, borderRadius: 8,
            background: k >= 0 ? P.accent : P.warn, opacity: q > 0.02 ? fade : 0,
          }} />
        );
      })}
    </div>
  );
}

/* ── the two terminal stages, side by side and never joined ───────────── */
function Terminals({ T, C, P, out }) {
  const o = animate({ from: 0, to: 1, start: C.Questions + 5.5, end: C.Questions + 6.2, ease: MOTION.enter })(T);
  const x = G.abs.x + G.abs.w + 44;
  return (
    <div style={{ position: 'absolute', inset: 0, opacity: out * o }}>
      <svg width={W.w} height={W.h} style={{ position: 'absolute', inset: 0 }}>
        <path d={'M ' + x + ' 180 L ' + (x + 26) + ' 180 L ' + (x + 26) + ' 950 L ' + x + ' 950'}
          fill="none" stroke={P.line2} strokeWidth="1.5" />
      </svg>
      <div style={{ position: 'absolute', left: x + 48, top: 500, width: 330, fontFamily: MONO, fontSize: 22, color: P.ink3, lineHeight: 1.45 }}>
        <div>two terminal stages</div>
        <div>the pipeline never joins them</div>
      </div>
    </div>
  );
}

/* ── region 2: screen (48 model calls, 8 executors) ───────────────────── */
function ScreenField({ T, C, P, out }) {
  const t = T - C.Screen;
  const lab = animate({ from: 0, to: 1, start: C.Screen - 0.6, end: C.Screen + 0.2, ease: MOTION.enter })(T);
  const execs = [];
  for (let k = 0; k < 8; k++) {
    const run = SCHED.find((s) => s.k % 8 === k && t >= s.start && t < s.end);
    execs.push({ k, run });
  }
  return (
    <div style={{ position: 'absolute', inset: 0, opacity: out }}>
      <StageLabel x={G.screen.x} y={150} name="screen" sub={'Infer · scout ↗ oracle · workers 8'} P={P} o={lab} w={660} />
      <svg width={W.w} height={W.h} style={{ position: 'absolute', inset: 0 }}>
        {execs.map(({ k, run }) => {
          if (!run) return null;
          const c = scell(run.k);
          return <line key={k} x1={G.exec.x + k * G.exec.gap + 13} y1={G.exec.y + 26}
            x2={c.x + G.screen.s / 2} y2={c.y + 6} stroke={P.accent} strokeWidth="1.2" opacity="0.5" />;
        })}
      </svg>
      {execs.map(({ k, run }) => (
        <div key={k} style={{
          position: 'absolute', left: G.exec.x + k * G.exec.gap, top: G.exec.y, width: 26, height: 26,
          transform: 'rotate(45deg)', boxSizing: 'border-box', opacity: lab,
          border: '1.5px solid ' + (run ? P.accent : P.line2), background: run ? P.wash : 'transparent',
        }} />
      ))}
      {SCHED.map(({ p, k, start, end, dur }) => {
        const c = scell(k);
        const inn = animate({ from: 0, to: 1, start: C.Screen + start - 0.35, end: C.Screen + start, ease: MOTION.pop })(T);
        const prog = clamp((t - start) / dur, 0, 1);
        const retryTick = p.retry ? clamp((t - (start + dur * 0.35)) * 6, 0, 1) : 0;
        const esc = p.flag === 'garbleScreen' ? clamp((t - (start + dur * 0.55)) * 4, 0, 1) : 0;
        const dead = p.flag === 'dead' ? clamp((t - (start + dur * 0.7)) * 4, 0, 1) : 0;
        const done = clamp((prog - 0.995) * 200, 0, 1) * (1 - dead);
        const ring = p.flag === 'straggler' ? clamp((t - (start + 0.9)) * 2, 0, 1) * (1 - done) : 0;
        const cache = animate({ from: 0, to: 1, start: C.Ledger + 2.8 + (c.x - 1400) / 3400, end: C.Ledger + 3.3 + (c.x - 1400) / 3400, ease: MOTION.enter })(T);
        const hue = dead ? P.warn : cache > 0.5 ? P.accent : esc > 0.5 ? P.oracle : done ? P.scout : P.line2;
        return (
          <div key={p.id} style={{
            position: 'absolute', left: c.x, top: c.y, width: G.screen.s, height: G.screen.s,
            boxSizing: 'border-box', borderRadius: 9, opacity: inn,
            border: '1.5px solid ' + hue, background: done || cache > 0.5 ? P.wash : 'transparent',
            transform: 'scale(' + (0.9 + 0.1 * inn + 0.06 * ring).toFixed(3) + ')',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}>
            <span style={{ fontFamily: MONO, fontSize: 26, color: hue, opacity: Math.max(done, dead, cache) }}>
              {dead ? '✕' : cache > 0.5 ? '✧' : esc > 0.5 ? '↗' : '✓'}
            </span>
            <span style={{
              position: 'absolute', left: 6, right: 6, bottom: 8, height: 3, borderRadius: 2,
              background: P.line, opacity: (1 - Math.max(done, dead)) * (1 - cache),
            }}>
              <span style={{ position: 'absolute', left: 0, top: 0, bottom: 0, width: (prog * 100).toFixed(1) + '%', background: esc > 0.5 ? P.oracle : P.accent, borderRadius: 2 }} />
            </span>
            <span style={{
              position: 'absolute', top: -11, right: -9, width: 20, height: 20, borderRadius: 10,
              border: '1.5px solid ' + P.warn, color: P.warn, fontFamily: MONO, fontSize: 13,
              display: 'flex', alignItems: 'center', justifyContent: 'center', opacity: retryTick * (1 - cache),
              background: P.bg,
            }}>↻</span>
            <span style={{
              position: 'absolute', inset: -7, borderRadius: 13, border: '1.5px dashed ' + P.accent,
              opacity: ring * 0.9,
            }} />
          </div>
        );
      })}
    </div>
  );
}

/* ── a filter gate ────────────────────────────────────────────────────── */
function Gate({ x, y0, y1, name, sub, P, o, T }) {
  return (
    <div style={{ position: 'absolute', left: x, top: y0, opacity: o }}>
      <div style={{ position: 'absolute', left: 0, top: 0, width: 2, height: y1 - y0, background: P.line2 }} />
      <div style={{ position: 'absolute', left: -136, top: -84, width: 272, textAlign: 'center' }}>
        <div style={{ fontFamily: SANS, fontSize: 26, fontWeight: 600, letterSpacing: '-0.02em', color: P.ink }}>{name}</div>
        <div style={{ marginTop: 3, fontFamily: MONO, fontSize: 20, color: P.ink3 }}>{sub}</div>
      </div>
    </div>
  );
}

/* ── region 3+4: relevance filter and extract-findings ────────────────── */
function FindingsField({ T, C, P, out }) {
  const gate = animate({ from: 0, to: 1, start: C.Relevant - 0.4, end: C.Relevant + 0.3, ease: MOTION.enter })(T);
  const lab = animate({ from: 0, to: 1, start: C.Findings - 0.5, end: C.Findings + 0.2, ease: MOTION.enter })(T);
  return (
    <div style={{ position: 'absolute', inset: 0, opacity: out }}>
      <Gate x={G.gate1.x} y0={220} y1={900} name="relevant-only" sub={'Filter · 48 → ' + RELEVANT.length} P={P} o={gate} T={T} />
      <StageLabel x={G.find.x} y={62} name="extract-findings" sub={'Infer · ' + RELEVANT.length + ' tasks · ParseJSON'} P={P} o={lab} w={560} />
      {SCREENED.map((p, k) => {
        const from = scell(k);
        const passes = p.area && p.flag !== 'dead';
        const ri = RELEVANT.indexOf(p);
        const to = passes ? fcell(ri) : { x: G.gate1.x - 40, y: from.y + 170 };
        const q = animate({ from: 0, to: 1, start: C.Relevant + 0.25 + k * 0.022, end: C.Relevant + 1.5 + k * 0.022, ease: MOTION.draw })(T);
        const fade = passes ? 1 : 1 - clamp((q - 0.55) / 0.4, 0, 1);
        const x = from.x + G.screen.s / 2 + (to.x - from.x - G.screen.s / 2) * q;
        const y = from.y + G.screen.s / 2 + (to.y - from.y - G.screen.s / 2) * q;
        return (
          <div key={p.id} style={{
            position: 'absolute', left: x, top: y, width: 18, height: 18, borderRadius: 9,
            background: passes ? P.accent : P.warn, opacity: q > 0.02 ? fade * (passes ? 1 - clamp((q - 0.96) * 25, 0, 1) : 1) : 0,
          }} />
        );
      })}
      {RELEVANT.map((p, k) => {
        const c = fcell(k);
        const inn = animate({ from: 0, to: 1, start: C.Findings + 0.15 + k * 0.045, end: C.Findings + 0.7 + k * 0.045, ease: MOTION.pop })(T);
        const esc = p.flag === 'garbleExtract' ? animate({ from: 0, to: 1, start: C.Findings + 1.5, end: C.Findings + 2.0, ease: MOTION.enter })(T) : 0;
        const cache = animate({ from: 0, to: 1, start: C.Ledger + 3.0 + (c.x - 1400) / 3400, end: C.Ledger + 3.5 + (c.x - 1400) / 3400, ease: MOTION.enter })(T);
        const hue = cache > 0.5 ? P.accent : esc > 0.5 ? P.oracle : P.line;
        return (
          <div key={p.id} style={{
            position: 'absolute', left: c.x, top: c.y, width: G.find.w, height: G.find.h,
            boxSizing: 'border-box', background: P.surface, border: '1px solid ' + hue, borderRadius: 7,
            padding: '8px 9px', opacity: inn, transform: 'scale(' + (0.9 + 0.1 * inn).toFixed(3) + ')', boxShadow: P.shadow,
          }}>
            <div style={{ height: 4, borderRadius: 2, width: '78%', background: cache > 0.5 ? P.accent : P.accent, opacity: 0.75 }} />
            <div style={{ marginTop: 6, fontFamily: MONO, fontSize: 18, color: P.ink3 }}>+{p.gain}%</div>
            <div style={{ position: 'absolute', left: 9, right: 9, bottom: 8, display: 'flex', gap: 3 }}>
              {Array.from({ length: p.nFind }).map((_, j) => (
                <span key={j} style={{ flex: 1, height: 3, borderRadius: 2, background: P.line2 }} />
              ))}
            </div>
          </div>
        );
      })}
    </div>
  );
}

/* ── branch A: grade → strong → synthesis tree → executive abstract ───── */
function BranchA({ T, C, P, out }) {
  const away = animate({ from: 0, to: 1, start: C.Questions + 0.5, end: C.Questions + 1.4, ease: MOTION.draw })(T)
    * (1 - animate({ from: 0, to: 1, start: C.Questions + 4.7, end: C.Questions + 5.4, ease: MOTION.draw })(T));
  out = out * (1 - 0.74 * away);
  const lab = animate({ from: 0, to: 1, start: C.Grade - 0.5, end: C.Grade + 0.2, ease: MOTION.enter })(T);
  const gate = animate({ from: 0, to: 1, start: C.Grade + 3.0, end: C.Grade + 3.5, ease: MOTION.enter })(T);
  const synLab = animate({ from: 0, to: 1, start: C.Synthesis - 0.4, end: C.Synthesis + 0.3, ease: MOTION.enter })(T);
  const absIn = animate({ from: 0, to: 1, start: C.Synthesis + 2.9, end: C.Synthesis + 3.5, ease: MOTION.pop })(T);
  const chars = Math.round(animate({ from: 0, to: ABSTRACT.length, start: C.Synthesis + 3.2, end: C.Synthesis + 5.0, ease: MOTION.draw })(T));

  const l0 = STRONG.map((p, k) => ({
    x: G.syn.x + (k % G.syn.cols) * G.syn.cw, y: G.syn.y + Math.floor(k / G.syn.cols) * G.syn.ch, p,
  }));
  const colY = (n, i, h) => 290 - ((n - 1) * h) / 2 + i * h;
  const l1 = Array.from({ length: SYN[1] }, (_, i) => ({ x: G.syn.l1, y: colY(SYN[1], i, 52) }));
  const l2 = Array.from({ length: SYN[2] }, (_, i) => ({ x: G.syn.l2, y: colY(SYN[2], i, 96) }));
  const l3 = [{ x: G.syn.l3, y: 290 }];

  const edge = (a, b, p, key, col) => {
    const len = Math.hypot(b.x - a.x, b.y - a.y) * 1.1;
    return <path key={key} d={'M ' + a.x + ' ' + a.y + ' C ' + (a.x + 40) + ' ' + a.y + ', ' + (b.x - 40) + ' ' + b.y + ', ' + b.x + ' ' + b.y}
      fill="none" stroke={col} strokeWidth="1.2" strokeDasharray={len} strokeDashoffset={len * (1 - p)} opacity="0.75" />;
  };

  return (
    <div style={{ position: 'absolute', inset: 0, opacity: out }}>
      <StageLabel x={G.grade.x} y={-16} name="grade-evidence" sub={'Infer · analyst ↗ oracle · Validate 1–5'} P={P} o={lab} w={740} />
      <Gate x={G.gate2.x} y0={176} y1={470} name="strong-evidence" sub={'Filter · ≥3 · ' + RELEVANT.length + ' → ' + STRONG.length} P={P} o={gate} T={T} />
      <StageLabel x={G.syn.x} y={-16} name="synthesis" sub={'ReduceAI · FanIn 4 · ' + SYN.join(' → ')} P={P} o={synLab} w={620} />

      {RELEVANT.map((p, k) => {
        const c = gcell(k);
        const inn = animate({ from: 0, to: 1, start: C.Grade + 0.2 + k * 0.05, end: C.Grade + 0.65 + k * 0.05, ease: MOTION.pop })(T);
        const bad = p.flag === 'badGrade';
        const fix = bad ? animate({ from: 0, to: 1, start: C.Grade + 2.05, end: C.Grade + 2.5, ease: MOTION.enter })(T) : 0;
        const weak = p.grade < 3;
        const moved = animate({ from: 0, to: 1, start: C.Grade + 3.4, end: C.Grade + 4.3, ease: MOTION.draw })(T);
        const si = STRONG.indexOf(p);
        const tgt = si >= 0 ? l0[si] : { x: c.x, y: c.y + 120 };
        const cache = animate({ from: 0, to: 1, start: C.Ledger + 3.35, end: C.Ledger + 3.8, ease: MOTION.enter })(T);
        const hue = cache > 0.5 ? P.accent : bad && fix < 0.5 ? P.warn : bad ? P.oracle : weak ? P.ink3 : P.analyst;
        return (
          <div key={p.id} style={{
            position: 'absolute', left: c.x + (tgt.x - c.x) * moved, top: c.y + (tgt.y - c.y) * moved,
            width: G.grade.w - (G.grade.w - 26) * moved, height: G.grade.h - (G.grade.h - 26) * moved,
            boxSizing: 'border-box', border: '1.5px solid ' + hue, borderRadius: 7,
            background: weak ? 'transparent' : P.wash, opacity: inn * (weak ? 1 - moved : 1),
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}>
            <span style={{ fontFamily: MONO, fontSize: 22 * (1 - moved * 0.55), color: hue }}>
              {bad && fix < 0.5 ? '9' : p.grade}
            </span>
          </div>
        );
      })}

      <svg width={W.w} height={W.h} style={{ position: 'absolute', inset: 0 }}>
        {l0.map((a, k) => edge({ x: a.x + 26, y: a.y + 13 }, { x: l1[Math.floor(k / 4)].x, y: l1[Math.floor(k / 4)].y },
          animate({ from: 0, to: 1, start: C.Synthesis + 0.35 + k * 0.03, end: C.Synthesis + 1.0 + k * 0.03, ease: MOTION.draw })(T), 'a' + k, P.oracle))}
        {l1.map((a, k) => edge({ x: a.x + 18, y: a.y }, { x: l2[Math.floor(k / 4)].x, y: l2[Math.floor(k / 4)].y },
          animate({ from: 0, to: 1, start: C.Synthesis + 1.5 + k * 0.05, end: C.Synthesis + 2.0 + k * 0.05, ease: MOTION.draw })(T), 'b' + k, P.oracle))}
        {l2.map((a, k) => edge({ x: a.x + 18, y: a.y }, l3[0],
          animate({ from: 0, to: 1, start: C.Synthesis + 2.3 + k * 0.06, end: C.Synthesis + 2.8 + k * 0.06, ease: MOTION.draw })(T), 'c' + k, P.oracle))}
      </svg>

      {[[l1, 1, 1.05], [l2, 2, 1.95], [l3, 3, 2.7]].map(([nodes, lvl, at]) => nodes.map((n, k) => {
        const inn = animate({ from: 0, to: 1, start: C.Synthesis + at + k * 0.05, end: C.Synthesis + at + 0.5 + k * 0.05, ease: MOTION.pop })(T);
        const r = lvl === 3 ? 26 : lvl === 2 ? 22 : 18;
        return (
          <div key={'n' + lvl + k} style={{
            position: 'absolute', left: n.x - r, top: n.y - r, width: r * 2, height: r * 2,
            boxSizing: 'border-box', borderRadius: r, border: '1.5px solid ' + P.oracle,
            background: P.surface, opacity: inn, transform: 'scale(' + (0.7 + 0.3 * inn).toFixed(3) + ')',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}>
            <span style={{ fontFamily: MONO, fontSize: lvl === 3 ? 22 : 17, color: P.oracle }}>{lvl === 3 ? '∑' : ''}</span>
          </div>
        );
      }))}

      <div style={{
        position: 'absolute', left: G.abs.x, top: G.abs.y, width: G.abs.w,
        boxSizing: 'border-box', background: P.surface, border: '1px solid ' + P.line, borderRadius: 12,
        boxShadow: P.shadow, padding: '22px 24px 26px', opacity: absIn,
        transform: 'scale(' + (0.95 + 0.05 * absIn).toFixed(3) + ')', transformOrigin: 'left center',
      }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
          <span style={{ fontFamily: MONO, fontSize: 20, color: P.oracle, letterSpacing: '0.04em' }}>executive-abstract</span>
          <span style={{ fontFamily: MONO, fontSize: 19, color: P.ink3 }}>oracle · 1 call</span>
        </div>
        <div style={{
          marginTop: 14, fontFamily: SANS, fontSize: 25, lineHeight: 1.42, color: P.ink,
          letterSpacing: '-0.014em', minHeight: 280, textWrap: 'pretty',
        }}>{ABSTRACT.slice(0, chars)}</div>
        <div style={{
          marginTop: 16, paddingTop: 14, borderTop: '1px solid ' + P.line,
          display: 'flex', justifyContent: 'space-between', alignItems: 'baseline',
        }}>
          <span style={{ fontFamily: MONO, fontSize: 19, color: P.oracle }}>primary deliverable</span>
          <span style={{ fontFamily: MONO, fontSize: 19, color: P.ink3 }}>{STRONG.length} strong-evidence papers</span>
        </div>
      </div>
    </div>
  );
}

/* ── branch B: explode-claims → open-questions tree ───────────────────── */
function BranchB({ T, C, P, out }) {
  const lab = animate({ from: 0, to: 1, start: C.Questions - 0.5, end: C.Questions + 0.2, ease: MOTION.enter })(T);
  const outIn = animate({ from: 0, to: 1, start: C.Questions + 3.3, end: C.Questions + 3.9, ease: MOTION.pop })(T);
  const chars = Math.round(animate({ from: 0, to: QUESTIONS.length, start: C.Questions + 3.6, end: C.Questions + 5.3, ease: MOTION.draw })(T));
  const colY = (n, i, h, mid) => mid - ((n - 1) * h) / 2 + i * h;
  const l1 = Array.from({ length: OQ[1] }, (_, i) => ({ x: G.oq.l1, y: colY(OQ[1], i, 24, 790) }));
  const l2 = Array.from({ length: OQ[2] }, (_, i) => ({ x: G.oq.l2, y: colY(OQ[2], i, 92, 790) }));
  const l3 = [{ x: G.oq.l3 + 60, y: 790 }];
  const edge = (a, b, p, key, col) => {
    const len = Math.hypot(b.x - a.x, b.y - a.y) * 1.12;
    return <path key={key} d={'M ' + a.x + ' ' + a.y + ' C ' + (a.x + 36) + ' ' + a.y + ', ' + (b.x - 36) + ' ' + b.y + ', ' + b.x + ' ' + b.y}
      fill="none" stroke={col} strokeWidth="1" strokeDasharray={len} strokeDashoffset={len * (1 - p)} opacity="0.6" />;
  };
  return (
    <div style={{ position: 'absolute', inset: 0, opacity: out }}>
      <StageLabel x={G.claim.x} y={520} name="explode-claims → claims" sub={'FlatMap + Filter (fused) · ' + CLAIMS + ' claims'} P={P} o={lab} w={640} />
      <StageLabel x={G.oq.l1 - 40} y={1010} name="open-questions" sub={'ReduceAI · FanIn 6 · ' + OQ.join(' → ')} P={P} o={animate({ from: 0, to: 1, start: C.Questions + 1.4, end: C.Questions + 2.0, ease: MOTION.enter })(T)} w={560} />
      {Array.from({ length: CLAIMS }).map((_, k) => {
        const c = ccell(k);
        const src = fcell(k % RELEVANT.length);
        const q = animate({ from: 0, to: 1, start: C.Questions + 0.15 + k * 0.012, end: C.Questions + 1.1 + k * 0.012, ease: MOTION.draw })(T);
        const cache = animate({ from: 0, to: 1, start: C.Ledger + 3.4, end: C.Ledger + 3.9, ease: MOTION.enter })(T);
        return (
          <div key={k} style={{
            position: 'absolute', left: src.x + (c.x - src.x) * q, top: src.y + (c.y - src.y) * q,
            width: 26 + 4 * q, height: 20, borderRadius: 5, boxSizing: 'border-box',
            border: '1px solid ' + (cache > 0.5 ? P.accent : P.line2),
            background: cache > 0.5 ? P.wash : 'transparent', opacity: q > 0.02 ? 1 : 0,
          }} />
        );
      })}
      <svg width={W.w} height={W.h} style={{ position: 'absolute', inset: 0 }}>
        {Array.from({ length: CLAIMS }).map((_, k) => {
          const c = ccell(k);
          const t1 = l1[Math.floor(k / 6)] || l1[l1.length - 1];
          return edge({ x: c.x + 30, y: c.y + 10 }, t1,
            animate({ from: 0, to: 1, start: C.Questions + 1.35 + k * 0.006, end: C.Questions + 1.9 + k * 0.006, ease: MOTION.draw })(T), 'q' + k, P.line2);
        })}
        {l1.map((a, k) => edge({ x: a.x + 11, y: a.y }, l2[Math.floor(k / 6)],
          animate({ from: 0, to: 1, start: C.Questions + 2.25 + k * 0.02, end: C.Questions + 2.7 + k * 0.02, ease: MOTION.draw })(T), 'r' + k, P.scout))}
        {l2.map((a, k) => edge({ x: a.x + 16, y: a.y }, l3[0],
          animate({ from: 0, to: 1, start: C.Questions + 2.9 + k * 0.04, end: C.Questions + 3.3 + k * 0.04, ease: MOTION.draw })(T), 's' + k, P.scout))}
      </svg>
      {[[l1, 1, 2.0, 11], [l2, 2, 2.65, 16], [l3, 3, 3.15, 24]].map(([nodes, lvl, at, r]) => nodes.map((n, k) => {
        const inn = animate({ from: 0, to: 1, start: C.Questions + at + k * 0.03, end: C.Questions + at + 0.4 + k * 0.03, ease: MOTION.pop })(T);
        return (
          <div key={'m' + lvl + k} style={{
            position: 'absolute', left: n.x - r, top: n.y - r, width: r * 2, height: r * 2,
            boxSizing: 'border-box', borderRadius: r, border: '1.5px solid ' + P.scout,
            background: P.surface, opacity: inn, transform: 'scale(' + (0.7 + 0.3 * inn).toFixed(3) + ')',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}>
            <span style={{ fontFamily: MONO, fontSize: 20, color: P.scout, opacity: lvl === 3 ? 1 : 0 }}>?</span>
          </div>
        );
      }))}
      <div style={{
        position: 'absolute', left: G.out.x, top: G.out.y, width: G.out.w,
        boxSizing: 'border-box', background: P.surface, border: '1px solid ' + P.line, borderRadius: 12,
        boxShadow: P.shadow, padding: '22px 24px 26px', opacity: outIn,
        transform: 'scale(' + (0.95 + 0.05 * outIn).toFixed(3) + ')', transformOrigin: 'left center',
      }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
          <span style={{ fontFamily: MONO, fontSize: 20, color: P.scout, letterSpacing: '0.04em' }}>open-questions</span>
          <span style={{ fontFamily: MONO, fontSize: 19, color: P.ink3 }}>scout · tree root</span>
        </div>
        <div style={{
          marginTop: 14, fontFamily: SANS, fontSize: 25, lineHeight: 1.42, color: P.ink,
          letterSpacing: '-0.014em', minHeight: 230, textWrap: 'pretty',
        }}>{QUESTIONS.slice(0, chars)}</div>
        <div style={{
          marginTop: 16, paddingTop: 14, borderTop: '1px solid ' + P.line,
          display: 'flex', justifyContent: 'space-between', alignItems: 'baseline',
        }}>
          <span style={{ fontFamily: MONO, fontSize: 19, color: P.scout }}>exploratory companion</span>
          <span style={{ fontFamily: MONO, fontSize: 19, color: P.ink3 }}>all {RELEVANT.length} papers · {CLAIMS} claims</span>
        </div>
      </div>
    </div>
  );
}

/* ── the split marker between the two branches ────────────────────────── */
function Split({ T, C, P, out }) {
  const o = animate({ from: 0, to: 1, start: C.Grade - 1.5, end: C.Grade - 0.7, ease: MOTION.enter })(T);
  const d = (y2) => 'M ' + G.split.x + ' 560 C ' + (G.split.x + 60) + ' 560, ' + (G.split.x + 40) + ' ' + y2 + ', ' + (G.split.x + 110) + ' ' + y2;
  return (
    <div style={{ position: 'absolute', inset: 0, opacity: out * o }}>
      <svg width={W.w} height={W.h} style={{ position: 'absolute', inset: 0 }}>
        <path d={d(300)} fill="none" stroke={P.line2} strokeWidth="1.5" />
        <path d={d(790)} fill="none" stroke={P.line2} strokeWidth="1.5" />
      </svg>
      <div style={{ position: 'absolute', left: G.split.x - 30, top: 470, fontFamily: MONO, fontSize: 20, color: P.ink3 }}>branch</div>
    </div>
  );
}

/* ── HUD (screen space) ───────────────────────────────────────────────── */
function Hud({ T, C, P, total }) {
  const t = (x) => T - x;
  let done = 0;
  done += T > C.Normalize ? PAPERS.filter((_, i) => T - C.Normalize >= 0.65 + i * 0.03).length : 0;
  done += SCHED.filter((s) => t(C.Screen) >= s.end).length;
  done += T > C.Findings ? RELEVANT.filter((_, k) => t(C.Findings) >= 0.7 + k * 0.045).length : 0;
  done += T > C.Grade ? RELEVANT.filter((_, k) => t(C.Grade) >= 0.65 + k * 0.05).length : 0;
  if (T > C.Synthesis + 1.5) done += SYN[1];
  if (T > C.Synthesis + 2.4) done += SYN[2];
  if (T > C.Synthesis + 3.0) done += SYN[3];
  if (T > C.Synthesis + 5.6) done += 1;
  if (T > C.Questions + 1.2) done += RELEVANT.length;
  if (T > C.Questions + 2.4) done += OQ[1];
  if (T > C.Questions + 3.0) done += OQ[2];
  if (T > C.Questions + 3.4) done += OQ[3];
  done = Math.min(done, TASKS);

  const retries = SCHED.filter((s) => s.p.retry && t(C.Screen) >= s.start + s.dur * 0.35).length;
  const escal = (t(C.Screen) > 1.6 ? 1 : 0) + (t(C.Findings) > 2.0 ? 1 : 0) + (t(C.Grade) > 2.5 ? 1 : 0);
  const dead = t(C.Screen) > 1.4 ? 1 : 0;
  const cached = T > C.Ledger + 3.9;
  const fade = animate({ from: 1, to: 0, start: total - 1.15, end: total - 0.35, ease: MOTION.draw })(T);

  const stage = T < C.Reuse ? 'findings gate · 24 questions'
    : T < C.Corpus ? 'findings gate · reuse + single-flight'
      : T < C.Broadcast ? 'papers' : T < C.Normalize ? 'broadcasts'
    : T < C.Screen ? 'normalize · screenable' : T < C.Relevant ? 'screen'
      : T < C.Findings ? 'relevant-only' : T < C.Grade ? 'extract-findings'
        : T < C.Synthesis ? 'grade-evidence' : T < C.Questions ? 'synthesis → executive-abstract'
          : T < C.Ledger ? 'explode-claims → open-questions' : 'run report';

  const stat = (label, value, col) => (
    <span style={{ display: 'inline-flex', alignItems: 'baseline', gap: 8 }}>
      <span style={{ fontFamily: MONO, fontSize: 25, color: col || P.ink2, fontVariantNumeric: 'tabular-nums' }}>{value}</span>
      <span style={{ fontFamily: MONO, fontSize: 21, color: P.ink3 }}>{label}</span>
    </span>
  );

  return (
    <div style={{ position: 'absolute', inset: 0, pointerEvents: 'none' }}>
      <div style={{
        position: 'absolute', left: 44, top: 30, padding: '14px 20px 16px', borderRadius: 10,
        background: P.bg, boxShadow: '0 0 24px 18px ' + P.bg,
      }}>
        <div style={{ fontFamily: MONO, fontSize: 26, color: P.ink3, whiteSpace: 'nowrap' }}>
          pipeline.New(<span style={{ color: P.accent }}>&quot;lit-survey&quot;</span>)
        </div>
        <div style={{ position: 'relative', marginTop: 6, height: 30 }}>
          <div style={{ position: 'absolute', left: 0, top: 0, fontFamily: MONO, fontSize: 23, color: P.ink2, opacity: fade, whiteSpace: 'nowrap' }}>
            stage: {stage}
          </div>
          <div style={{ position: 'absolute', left: 0, top: 0, fontFamily: MONO, fontSize: 23, color: P.ink2, opacity: 1 - fade, whiteSpace: 'nowrap' }}>
            stage: papers
          </div>
        </div>
      </div>
      <div style={{
        position: 'absolute', right: 44, top: 30, padding: '14px 20px 16px', borderRadius: 10,
        textAlign: 'right', whiteSpace: 'nowrap', background: P.bg, boxShadow: '0 0 24px 18px ' + P.bg,
      }}>
        <div style={{ position: 'absolute', right: 20, top: 14, opacity: 1 - fade, whiteSpace: 'nowrap' }}>
          {stat('tasks', '0/' + TASKS, P.ink)}
        </div>
        <div style={{ opacity: fade, display: 'flex', gap: 26, justifyContent: 'flex-end' }}>
          {stat('tasks', done + '/' + TASKS, P.ink)}
          {retries > 0 && stat('retries', retries, P.warn)}
          {escal > 0 && stat('escalations', escal, P.oracle)}
          {dead > 0 && stat('dead letter', dead, P.warn)}
        </div>
        <div style={{ marginTop: 8, opacity: cached ? fade : 0, display: 'flex', gap: 26, justifyContent: 'flex-end' }}>
          {stat('cache hits', TASKS, P.accent)}
          {stat('model calls', 0, P.accent)}
        </div>
      </div>
      <div style={{
        position: 'absolute', left: 44, bottom: 156, padding: '12px 20px', borderRadius: 10,
        background: P.bg, boxShadow: '0 0 24px 18px ' + P.bg, whiteSpace: 'pre',
        fontFamily: MONO, fontSize: 25, color: P.ink2,
        opacity: animate({ from: 0, to: 1, start: C.Ledger + 2.35, end: C.Ledger + 2.8, ease: MOTION.enter })(T) * fade,
      }}>
        <span style={{ color: P.ink3 }}>$ </span>
        {CMD.slice(0, Math.round(animate({ from: 0, to: CMD.length, start: C.Ledger + 2.5, end: C.Ledger + 3.4, ease: MOTION.draw })(T)))}
      </div>
    </div>
  );
}

/* ── the piece ────────────────────────────────────────────────────────── */
function Piece({ tw }) {
  const c = useComposition();
  const T = c.T, C = c.CUES, total = c.authoredTotal;
  const P = THEMES[tw && tw.theme === 'light' ? 'light' : 'dark'];
  const out = animate({ from: 1, to: 0, start: total - 1.15, end: total - 0.35, ease: MOTION.draw })(T);

  /* camera: [authored time, world x, world y, scale] */
  const KEYS = [
    [0, -1480, 500, 0.58],
    [C.Ask + 1.7, -1870, 520, 0.82],
    [C.Ask + 3.6, -1560, 520, 0.60],
    [C.Reuse + 1.5, -1180, 640, 0.74],
    [C.Reuse + 3.0, -1150, 560, 0.82],
    [C.Reuse + 4.4, -1290, 1010, 0.54],
    [C.Reuse + 5.4, -520, 620, 0.46],
    [C.Corpus + 0.2, 900, 560, 0.62],
    [C.Corpus + 2.3, 520, 330, 1.28],
    [C.Corpus + 4.1, 880, 620, 0.66],
    [C.Broadcast + 1.2, 1500, -40, 0.62],
    [C.Broadcast + 4.9, 3120, -70, 0.62],
    [C.Normalize + 0.5, 1240, 540, 0.76],
    [C.Normalize + 2.4, 1440, 560, 0.94],
    [C.Normalize + 4.3, 1660, 570, 0.90],
    [C.Screen - 0.3, 2000, 380, 0.72],
    [C.Screen + 0.5, 2170, 430, 0.80],
    [C.Screen + 0.9, 2170, 520, 0.92],
    [C.Screen + 4.2, 2040, 430, 1.45],
    [C.Relevant + 0.6, 2660, 560, 1.0],
    [C.Findings + 0.8, 3280, 540, 1.05],
    [C.Findings + 3.3, 3420, 600, 1.0],
    [C.Grade - 0.8, 3600, 560, 0.58],
    [C.Grade + 0.9, 4060, 190, 0.9],
    [C.Grade + 3.9, 4240, 300, 1.0],
    [C.Synthesis + 1.6, 4760, 290, 0.95],
    [C.Synthesis + 3.5, 5390, 300, 0.88],
    [C.Synthesis + 6.1, 5410, 330, 0.82],
    [C.Questions + 1.0, 4080, 780, 0.95],
    [C.Questions + 2.6, 4500, 790, 0.9],
    [C.Questions + 3.9, 5390, 780, 0.88],
    [C.Questions + 5.4, 5410, 800, 0.82],
    [C.Questions + 6.4, 5560, 550, 0.70],
    [C.Ledger + 1.6, 2300, 300, 0.255],
    [total - 1.1, 2340, 300, 0.255],
    [total - 0.1, -1480, 500, 0.58],
  ];
  const ts = KEYS.map((k) => k[0]);
  const camX = interpolate(ts, KEYS.map((k) => k[1]), MOTION.enter)(T);
  const camY = interpolate(ts, KEYS.map((k) => k[2]), MOTION.enter)(T);
  const camS = interpolate(ts, KEYS.map((k) => k[3]), MOTION.enter)(T);

  return (
    <div style={{ position: 'absolute', inset: 0, background: P.bg, overflow: 'hidden', fontFamily: SANS, color: P.ink }}>
      <div style={{
        position: 'absolute', left: 0, top: 0, width: W.w, height: W.h, transformOrigin: '0 0',
        transform: 'translate(' + (960 - camX * camS).toFixed(2) + 'px, ' + (540 - camY * camS).toFixed(2) + 'px) scale(' + camS.toFixed(4) + ')',
      }}>
        <GateRegion T={T} C={C} P={P} out={out} />
        <PaperGrid T={T} C={C} P={P} out={out} />
        <BroadcastBand T={T} C={C} P={P} out={out} />
        <NormalizeStage T={T} C={C} P={P} out={out} />
        <ScreenField T={T} C={C} P={P} out={out} />
        <FindingsField T={T} C={C} P={P} out={out} />
        <Split T={T} C={C} P={P} out={out} />
        <BranchA T={T} C={C} P={P} out={out} />
        <BranchB T={T} C={C} P={P} out={out} />
        <Terminals T={T} C={C} P={P} out={out} />
      </div>

      <Hud T={T} C={C} P={P} total={total} />

      {(!tw || tw.captions !== false) && (
        <Captions items={[
          { at: C.Ask + 0.5, until: C.Ask + 2.7, text: 'Before any of this: 24 research questions, four desks, six subjects, each desk in its own phrasing.' },
          { at: C.Ask + 3.0, until: C.Reuse + 0.3, text: 'A result cache keys on the bytes, so it sees 24 different questions. The gate keys on the question.' },
          { at: C.Reuse + 0.5, until: C.Reuse + 2.5, text: 'Six miss and reach the public source. Six more ask at the same instant and wait on the leader\u2019s flight instead of calling.' },
          { at: C.Reuse + 2.7, until: C.Reuse + 4.1, text: 'Twelve are answered from findings already in the ledger — same subject, different wording, no call and no model.' },
          { at: C.Reuse + 4.3, until: C.Corpus + 0.5, text: '24 questions, 6 external calls, 18 reused. What those six calls returned is what enters the pipeline.' },
          { at: C.Corpus + 0.9, until: C.Corpus + 2.2, text: '50 papers on self-improving agents. One record each.' },
          { at: C.Broadcast + 0.7, until: C.Broadcast + 2.3, text: 'Three values every task needs: the venue tiers, the inclusion criteria, the grading rubric.' },
          { at: C.Broadcast + 2.5, until: C.Broadcast + 4.9, text: 'Registered once and referenced by hash — not pasted into ' + TASKS + ' prompts. Edit one and only its readers recompute.' },
          { at: C.Normalize + 0.4, until: C.Normalize + 2.3, text: 'The first stage is plain code: tag the venue tier, and mark whether a paper is screenable.' },
          { at: C.Normalize + 2.5, until: C.Normalize + 4.7, text: 'Two position papers carry no abstract. They fail the filter and leave before any model call: 50 → 48.' },
          { at: C.Screen + 0.5, until: C.Screen + 3.4, text: '48 screening calls on the cheap tier, eight workers deep.' },
          { at: C.Screen + 3.7, until: C.Screen + 6.2, text: 'A rate limit retries. Garbled output climbs to the deep model. A retracted paper dead-letters — the run continues.' },
          { at: C.Relevant + 0.4, until: C.Relevant + 2.6, text: 'Seven leave here: one dead letter and six out of scope. 41 go through.' },
          { at: C.Findings + 0.5, until: C.Findings + 2.6, text: 'Findings extracted per paper, parsed into the record.' },
          { at: C.Findings + 3.2, until: C.Grade - 0.2, text: 'The DAG forks here: grade the evidence, and explode the claims.' },
          { at: C.Grade + 0.5, until: C.Grade + 2.9, text: 'Grades scored against one shared rubric. An out-of-range 9 fails Validate and escalates.' },
          { at: C.Grade + 3.4, until: C.Grade + 4.9, text: 'Keep evidence grade 3 and up: ' + STRONG.length + ' findings.' },
          { at: C.Synthesis + 0.4, until: C.Synthesis + 3.1, text: 'ReduceAI is a tree, not a loop: ' + SYN.join(' → ') + ', four at a time.' },
          { at: C.Synthesis + 3.2, until: C.Synthesis + 6.2, text: 'One deep-model call turns the synthesis into the abstract — built only from the ' + STRONG.length + ' strong papers.' },
          { at: C.Questions + 0.4, until: C.Questions + 2.2, text: 'The other branch fans out instead: ' + CLAIMS + ' claims from all ' + RELEVANT.length + ' papers, weak evidence included.' },
          { at: C.Questions + 2.4, until: C.Questions + 5.2, text: 'A second tree, fan-in 6, distils them into the open questions. Deliberately exploratory, not a graded finding.' },
          { at: C.Questions + 5.4, until: C.Questions + 6.9, text: 'Two terminal stages, never joined: the abstract is the deliverable, the open questions its companion.' },
          { at: C.Ledger + 0.6, until: C.Ledger + 2.6, text: TASKS + ' tasks across one DAG. No keys, no network.' },
          { at: C.Ledger + 2.9, until: C.Ledger + 4.6, text: 'Run it again with a state dir: the whole sky replays from cache. Zero model calls.' },
        ]} style={{
          bottom: '4%', font: '500 33px ' + SANS, letterSpacing: '-0.02em', color: P.ink2,
          textShadow: 'none', left: '6%', right: '6%', padding: '14px 26px',
          background: P.bg, borderRadius: 12, boxShadow: '0 0 26px 20px ' + P.bg,
        }} />
      )}
    </div>
  );
}

function LoomSurvey() {
  const [t, setTweak] = useTweaks(window.TWEAK_DEFAULTS);
  return (
    <div style={{ position: 'absolute', inset: 0 }}>
      <CompositionStage width={1920} height={1080} bg={THEMES[t.theme === 'light' ? 'light' : 'dark'].bg}
                        scenes={window.OM_SCENES} playback={window.OM_PLAYBACK}>
        <Piece tw={t} />
      </CompositionStage>
      <TweaksPanel>
        <TweakSection label="Animation" />
        <TweakToggle label="Motion editor" value={t.motionEditor} onChange={(v) => setTweak('motionEditor', v)} />
        <TweakToggle label="Captions" value={t.captions} onChange={(v) => setTweak('captions', v)} />
        <TweakRadio label="Theme" value={t.theme} options={['dark', 'light']} onChange={(v) => setTweak('theme', v)} />
      </TweaksPanel>
    </div>
  );
}

function LoomSurveyEmbed(props) {
  const theme = props.theme === 'light' ? 'light' : 'dark';
  return (
    <CompositionStage width={1920} height={1080} bg={THEMES[theme].bg}
                      scenes={window.OM_SCENES} playback={window.OM_PLAYBACK}>
      <Piece tw={{ theme: theme, captions: props.captions !== false }} />
    </CompositionStage>
  );
}

window.LoomSurvey = LoomSurvey;
window.LoomSurveyEmbed = LoomSurveyEmbed;
