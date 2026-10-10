/* survey.js — examples/research (pipeline "lit-survey") as one continuous
   run, drawn on a single <canvas>.

   24 research questions through the findings gate, 50 papers down to one
   executive abstract and a set of open questions, with the retries, the
   escalations and the dead letter the run actually produces. Corpus, stage
   names, scripted failures and model tiers come from examples/research; the
   palette and type are public/index.html's own.

   It replaces a React composition that needed React, ReactDOM and Babel
   (about 1.5 MB, the JSX transpiled in the reader's browser) and re-laid-out
   some 1,700 absolutely positioned DOM nodes on every frame. The twelve
   scenes, their timing and the choreography are the same; the medium is
   not. Every frame is a pure function of the time T — drawFrame(T) paints
   what the run looks like at T and nothing else — so playing, scrubbing,
   looping and the still shown to a reader who asked for less motion are all
   one call.

   The module has no dependencies and no side effects at import, so Node can
   import it too: the Pages workflow does, to check the scene table. */

/* ── the timeline ─────────────────────────────────────────────────────── */
export const SCENES = [
  { name: 'Ask', dur: 4.5, desc: 'Twenty-four research questions from four desks about six subjects line up in front of the findings gate — the same six subjects in four house phrasings, which a byte-keyed result cache would read as twenty-four different questions' },
  { name: 'Reuse', dur: 5.5, desc: 'The gate resolves all twenty-four: six miss and reach the public source, six concurrent duplicates coalesce onto those flights behind the single-flight lease, twelve are served from the ledger by subject class, and the efficiency card totals 24 questions to 6 external calls with 18 reused' },
  { name: 'Corpus', dur: 4.5, desc: 'Fifty synthetic paper records materialise; the camera pushes in on a few titles' },
  { name: 'Broadcast', dur: 5.5, desc: 'The three run-level broadcasts appear above the DAG — venue tiers into normalize, inclusion criteria into screen, evidence rubric into grade-evidence — each registered once by content hash and wired down into the stage that actually reads it' },
  { name: 'Normalize', dur: 5, desc: 'All fifty records flow through the plain-code normalize stage, which tags the venue tier and marks each paper screenable; the two abstract-less position papers fail the filter and leave before any model call, 50 → 48' },
  { name: 'Screen', dur: 7, desc: 'Forty-eight screening calls run eight workers deep: a rate limit retries, garbled output escalates to the deep model, one retracted paper dead-letters, one straggler grows an activity ring' },
  { name: 'Relevant', dur: 3.5, desc: 'Six out-of-scope papers and the dead letter are filtered out and forty-one pass the gate' },
  { name: 'Findings', dur: 5, desc: 'Findings are extracted per paper and the camera pulls back to reveal the DAG forking in two' },
  { name: 'Grade', dur: 5.5, desc: 'Evidence is graded against the shared rubric, an out-of-range grade escalates, and the strong-evidence filter keeps grade 3 and up' },
  { name: 'Synthesis', dur: 6.5, desc: 'The ReduceAI tree folds the twenty-eight strong findings four at a time until one node remains, then the deep model writes the executive abstract' },
  { name: 'Questions', dur: 7, desc: 'The other branch fans out into ninety-eight claims from all forty-one papers and a second tree, fan-in 6, distils them into the open questions; the camera then widens onto both terminal artifacts, which the pipeline never joins' },
  { name: 'Ledger', dur: 6, desc: 'The camera pulls back over the whole DAG for the run report, then the rerun replays every task from cache with zero model calls' },
];

/* CUES.Name is that scene's start, the running sum of the durations before
   it. Every time below is written against a cue, so retiming one scene
   moves everything keyed to it and nothing else. */
export const CUES = {};
export const DURATION = SCENES.reduce((at, s) => { CUES[s.name] = at; return at + s.dur; }, 0);
const C = CUES;

/* The frame a reader who asked for less motion sees: the whole DAG from the
   far camera, every task replayed from cache, the rerun command typed out. */
export const STILL = C.Ledger + 4.6;

/* ── motion ───────────────────────────────────────────────────────────── */
const clamp = (v, a, b) => (v < a ? a : v > b ? b : v);
const lerp = (a, b, u) => a + (b - a) * u;
const ease = {
  out: (t) => 1 - (1 - t) ** 3,
  inOut: (t) => (t < 0.5 ? 4 * t * t * t : 1 - (-2 * t + 2) ** 3 / 2),
  draw: (t) => (t < 0.5 ? 8 * t ** 4 : 1 - 8 * (t - 1) ** 4),
  sine: (t) => (1 - Math.cos(Math.PI * t)) / 2,
  pop: (t) => 1 + 2.70158 * (t - 1) ** 3 + 1.70158 * (t - 1) ** 2,
};
/* 0 before a, 1 after b, eased between. */
const tw = (T, a, b, f = ease.out) => (T <= a ? 0 : T >= b ? 1 : f((T - a) / (b - a)));

/* Piecewise between keyframes, one easing per segment. */
function interp(ts, vs, f, t) {
  const n = ts.length - 1;
  if (t <= ts[0]) return vs[0];
  if (t >= ts[n]) return vs[n];
  let i = 0;
  while (t > ts[i + 1]) i++;
  const span = ts[i + 1] - ts[i];
  return lerp(vs[i], vs[i + 1], span > 0 ? f((t - ts[i]) / span) : 1);
}

/* Monotone cubic (Fritsch–Carlson) through keyframes. Unlike an ease per
   segment it carries velocity through a key the camera is passing rather
   than stopping at, never overshoots one it is settling on, and comes to
   rest at the first and last. */
function monotone(xs, ys) {
  const n = xs.length, d = [], m = [];
  for (let i = 0; i < n - 1; i++) {
    const h = xs[i + 1] - xs[i];
    d.push(h > 0 ? (ys[i + 1] - ys[i]) / h : 0);
  }
  m[0] = 0; m[n - 1] = 0;
  for (let i = 1; i < n - 1; i++) m[i] = d[i - 1] * d[i] <= 0 ? 0 : (d[i - 1] + d[i]) / 2;
  for (let i = 0; i < n - 1; i++) {
    if (d[i] === 0) { m[i] = 0; m[i + 1] = 0; continue; }
    const a = m[i] / d[i], b = m[i + 1] / d[i], s = a * a + b * b;
    if (s > 9) { const k = 3 / Math.sqrt(s); m[i] = k * a * d[i]; m[i + 1] = k * b * d[i]; }
  }
  return (x) => {
    if (x <= xs[0]) return ys[0];
    if (x >= xs[n - 1]) return ys[n - 1];
    let i = 0;
    while (x > xs[i + 1]) i++;
    const h = xs[i + 1] - xs[i], u = (x - xs[i]) / h, u2 = u * u, u3 = u2 * u;
    return (2 * u3 - 3 * u2 + 1) * ys[i] + (u3 - 2 * u2 + u) * h * m[i]
      + (-2 * u3 + 3 * u2) * ys[i + 1] + (u3 - u2) * h * m[i + 1];
  };
}

/* ── palette and type (public/index.html's tokens) ────────────────────── */
const SANS = 'system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif';
const MONO = 'ui-monospace, "SF Mono", SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace';
const sans = (px, w = 400) => w + ' ' + px + 'px ' + SANS;
const mono = (px, w = 400) => w + ' ' + px + 'px ' + MONO;

export const THEMES = {
  dark: {
    bg: '#0c1012', bgRGB: '12,16,18', surface: '#131a1d', ink: '#edf3f5', ink2: '#9dabb1', ink3: '#6b787e',
    line: 'rgba(237,243,245,0.13)', line2: 'rgba(237,243,245,0.24)',
    accent: '#6fd3f2', wash: 'rgba(111,211,242,0.10)', lift: 'rgba(0,0,0,0.35)',
    scout: '#6fd3f2', analyst: '#86d6b0', oracle: '#c4a5f0', warn: '#e8a87c',
  },
  light: {
    bg: '#f1f3f4', bgRGB: '241,243,244', surface: '#fafbfb', ink: '#10161a', ink2: '#4d585f', ink3: '#7f8c93',
    line: 'rgba(16,22,26,0.11)', line2: 'rgba(16,22,26,0.20)',
    accent: '#06758f', wash: 'rgba(6,117,143,0.07)', lift: 'rgba(16,22,26,0.05)',
    scout: '#06627a', analyst: '#1a6f52', oracle: '#7a4bb5', warn: '#9a4a1f',
  },
};

/* ── the corpus (examples/research/main.go) ───────────────────────────── */
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
const RETRY_AT = { 2: 1, 10: 1, 18: 1, 26: 1, 33: 1, 39: 1 }; // the six scripted 429/503s

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
    retry: !!RETRY_AT[i], gain,
    grade: 2 + fnv(headline) % 4,
    nFind: 2 + (fnv(title) % 2 === 0 ? 1 : 0) + (title.indexOf('Toward') === 0 ? 1 : 0),
  };
});
const SCREENED = PAPERS.filter((p) => !p.stub);                       // 48
const RELEVANT = SCREENED.filter((p) => p.area && p.flag !== 'dead'); // 41
const STRONG = RELEVANT.filter((p) => p.grade >= 3);                  // 28
const CLAIMS = RELEVANT.reduce((n, p) => n + p.nFind - (p.title.indexOf('Toward') === 0 ? 1 : 0), 0);
const treeOf = (n, f) => { const L = [n]; while (n > 1) { n = Math.ceil(n / f); L.push(n); } return L; };
const SYN = treeOf(STRONG.length, 4);      // 28 → 7 → 2 → 1
const OQ = treeOf(CLAIMS, 6);              // 98 → 17 → 3 → 1
export const TASKS = PAPERS.length + SCREENED.length + RELEVANT.length * 3
  + (SYN[1] + SYN[2] + SYN[3]) + 1 + (OQ[1] + OQ[2] + OQ[3]);
export const COUNTS = {
  papers: PAPERS.length, screened: SCREENED.length, relevant: RELEVANT.length,
  strong: STRONG.length, claims: CLAIMS, tasks: TASKS,
};

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
const G = {
  paper: { x: 120, y: 140, cw: 208, ch: 88, w: 192, h: 74, cols: 5 },
  norm: { x: 1240, y: 180, w: 240, h: 760 },
  screenable: { x: 1600 },
  exec: { x: 1760, y: 66, gap: 100 },
  screen: { x: 1760, y: 250, cw: 100, ch: 108, s: 76, cols: 8 },
  gate1: { x: 2760 },
  find: { x: 3000, y: 250, cw: 110, ch: 98, w: 92, h: 76, cols: 6 },
  split: { x: 3710 },
  grade: { x: 3800, y: 120, cw: 62, ch: 58, w: 46, h: 42, cols: 7 },
  gate2: { x: 4350 },
  syn: { x: 4490, y: 130, cw: 46, ch: 48, cols: 4, l1: 4800, l2: 4960, l3: 5100 },
  abs: { x: 5200, y: 110, w: 640 },
  claim: { x: 3800, y: 628, cw: 38, ch: 34, cols: 14 },
  oq: { l1: 4560, l2: 4760, l3: 4920 },
  out: { x: 5200, y: 640, w: 640 },
  bcast: { y: -500, h: 300 },
};
const pcell = (i) => ({ x: G.paper.x + (i % G.paper.cols) * G.paper.cw, y: G.paper.y + Math.floor(i / G.paper.cols) * G.paper.ch });
const scell = (k) => ({ x: G.screen.x + (k % G.screen.cols) * G.screen.cw, y: G.screen.y + Math.floor(k / G.screen.cols) * G.screen.ch });
const fcell = (k) => ({ x: G.find.x + (k % G.find.cols) * G.find.cw, y: G.find.y + Math.floor(k / G.find.cols) * G.find.ch });
const gcell = (k) => ({ x: G.grade.x + (k % G.grade.cols) * G.grade.cw, y: G.grade.y + Math.floor(k / G.grade.cols) * G.grade.ch });
const ccell = (k) => ({ x: G.claim.x + (k % G.claim.cols) * G.claim.cw, y: G.claim.y + Math.floor(k / G.claim.cols) * G.claim.ch });

/* ── the findings gate (loom.WithFindings) ────────────────────────────────
   Tier names and totals from docs/FINDINGS.md §9 / examples/commons:
   24 asked · 18 reused (75%) · 6 researched — exact 0, class 12, near 0,
   coalesced 6. The six subjects are the six areas of the corpus, so the
   calls that do go out are the ones the 50 paper records come back from.

   Each question is a token. It flies from its chip into the row of the gate
   that decides it, comes out the far side of that row, and goes where the
   verdict sends it: a miss to the public source and back, a concurrent
   duplicate to wait on that flight's lease, a class hit straight through. */
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
  rail: -600, mx: -2080, my: 1130, mw: 1580, mh: 300,
};
const ROWS = [ // name, what it keys on, gate row centre
  ['exact', 'canonical question key', 150],
  ['class', 'topic + facets', 330],
  ['near', 'embedding · not configured', 510],
  ['lease', 'single-flight, concurrent askers', 700],
  ['miss', 'reaches the public source', 880],
];
const gGroupY = (s) => GT.qy + s * (4 * GT.rh + GT.gg);
const srcSlot = (s) => {
  const w = (GT.cw - 36 - 5 * 8) / 6;
  return { x: GT.cx + 18 + s * (w + 8), y: GT.cy + GT.ch - 46, w, h: 26 };
};
const ASKS = [];
AREAS.forEach((area, s) => PHRASINGS.forEach((f, d) => {
  const verdict = d === 0 ? 'research' : d === 1 ? 'coalesced' : 'class';
  const lead = 0.30 + s * 0.13;
  const t0 = d <= 1 ? lead : 1.95 + s * 0.15 + (d - 2) * 0.30;
  const y = gGroupY(s) + d * GT.rh, lane = y + (GT.rh - 6) / 2;
  const row = verdict === 'research' ? 4 : verdict === 'coalesced' ? 3 : 1;
  const ry = ROWS[row][2];
  const chip = [GT.qx + GT.qw, lane], inP = [GT.sx, ry], outP = [GT.sx + GT.sw, ry], rail = [GT.rail, lane];
  let ts, pts;
  if (verdict === 'research') {
    const sl = srcSlot(s), sp = [sl.x + sl.w / 2, sl.y + sl.h / 2];
    ts = [t0, t0 + 0.5, t0 + 0.8, t0 + 1.4, t0 + 2.0, t0 + 2.45];
    pts = [chip, inP, outP, sp, sp, rail];
  } else if (verdict === 'coalesced') {
    const wait = [GT.sx + GT.sw + 46, ry - 60 + s * 24];
    ts = [t0, t0 + 0.5, t0 + 0.8, t0 + 1.05, lead + 2.0, lead + 2.8];
    pts = [chip, inP, outP, wait, wait, rail];
  } else {
    ts = [t0, t0 + 0.5, t0 + 0.8, t0 + 1.35];
    pts = [chip, inP, outP, rail];
  }
  ASKS.push({
    k: ASKS.length, s, d, verdict, y, lane, row, t0, lead, ts,
    xs: pts.map((p) => p[0]), ys: pts.map((p) => p[1]),
    hit: ts[1], arrive: ts[ts.length - 1], text: f(SHORT[s]),
  });
}));
const ghue = (v, P) => (v === 'research' ? P.oracle : v === 'coalesced' ? P.analyst : P.accent);

/* run-level broadcasts (examples/research/main.go: loom.WithBroadcast) */
const BCASTS = [
  { name: 'venue-tiers', hash: 'b1f04c7e', body: '18 venues → tier + weight',
    reader: 'normalize', tasks: PAPERS.length, x: 620, tx: 1370, ty: 170, at: 0.9 },
  { name: 'inclusion-criteria', hash: '7a2e9d31', body: 'year ≥ 2024 · in-scope area',
    reader: 'screen', tasks: SCREENED.length, x: 1760, tx: 2000, ty: 236, at: 1.9 },
  { name: 'evidence-rubric', hash: '4c8bb0a6', body: '1–5 scale · replication, sample, ablation, effect size',
    reader: 'grade-evidence', tasks: RELEVANT.length, x: 2900, tx: 3980, ty: 92, at: 3.0 },
];

/* screen-stage schedule: waves of 8 (loom.WithWorkers(8)) */
const SCHED = SCREENED.map((p, k) => {
  const start = 0.30 + Math.floor(k / 8) * 0.62 + (k % 8) * 0.05;
  let dur = 0.55 + rnd(k, 3) * 0.35;
  if (p.flag === 'straggler') dur = 3.6;
  if (p.flag === 'garbleScreen') dur = 1.9;
  if (p.retry) dur += 0.7;
  return { p, k, start, end: start + dur, dur };
});

/* The two ReduceAI trees, laid out once. */
const colY = (n, i, h, mid) => mid - ((n - 1) * h) / 2 + i * h;
const TREE_A = {
  l0: STRONG.map((p, k) => ({ x: G.syn.x + (k % G.syn.cols) * G.syn.cw, y: G.syn.y + Math.floor(k / G.syn.cols) * G.syn.ch, p })),
  l1: Array.from({ length: SYN[1] }, (_, i) => ({ x: G.syn.l1, y: colY(SYN[1], i, 52, 290) })),
  l2: Array.from({ length: SYN[2] }, (_, i) => ({ x: G.syn.l2, y: colY(SYN[2], i, 96, 290) })),
  l3: [{ x: G.syn.l3, y: 290 }],
};
const TREE_B = {
  l1: Array.from({ length: OQ[1] }, (_, i) => ({ x: G.oq.l1, y: colY(OQ[1], i, 24, 790) })),
  l2: Array.from({ length: OQ[2] }, (_, i) => ({ x: G.oq.l2, y: colY(OQ[2], i, 92, 790) })),
  l3: [{ x: G.oq.l3 + 60, y: 790 }],
};

/* ── camera ───────────────────────────────────────────────────────────────
   Each key names the world rectangle the shot frames; fit() turns it into a
   centre and a scale that put that rectangle inside the safe area — clear
   of the HUD across the top, with a margin everywhere else. `zoom` pushes
   in past the fit, for a close-up that is meant to crop. */
const SAFE = { x0: 70, y0: 150, x1: 1850, y1: 1040 };
function fit(x0, y0, x1, y1, zoom = 1) {
  const s = Math.min((SAFE.x1 - SAFE.x0) / (x1 - x0), (SAFE.y1 - SAFE.y0) / (y1 - y0)) * zoom;
  return [(x0 + x1) / 2 - ((SAFE.x0 + SAFE.x1) / 2 - 960) / s, (y0 + y1) / 2 - ((SAFE.y0 + SAFE.y1) / 2 - 540) / s, s];
}
const GATE_ALL = fit(-2160, -90, -540, 1080);
const WORLD = fit(-2160, -620, 6380, 1460);
const CAM_KEYS = [
  [0, ...fit(-2160, -90, -540, 1080, 0.9)],
  [C.Ask + 1.7, ...fit(-2130, -90, -1130, 560)],                 // the questions, close
  [C.Ask + 3.6, ...GATE_ALL],                                    // the gate takes them
  [C.Reuse + 1.6, ...fit(-1940, -40, -560, 1060)],
  [C.Reuse + 3.2, ...GATE_ALL],
  [C.Reuse + 4.4, ...fit(-2120, 660, -460, 1450)],               // the efficiency card
  [C.Reuse + 5.4, ...fit(-760, -40, 1320, 1120)],                // calls → corpus
  [C.Corpus + 0.3, ...fit(40, 20, 1200, 1060)],
  [C.Corpus + 2.3, ...fit(70, 30, 900, 560, 0.84)],              // a few titles
  [C.Corpus + 4.1, ...fit(40, 20, 1200, 1060)],
  [C.Broadcast + 1.2, ...fit(560, -610, 2840, 300)],             // broadcasts, left to right
  [C.Broadcast + 4.9, ...fit(1700, -610, 3980, 300)],
  [C.Normalize + 0.5, ...fit(60, -60, 2120, 1060)],              // papers → normalize
  [C.Normalize + 2.4, ...fit(880, -40, 2420, 1040)],
  [C.Normalize + 4.3, ...fit(1150, -40, 2650, 1040)],
  [C.Screen - 0.3, ...fit(1200, -10, 2700, 1000)],
  [C.Screen + 0.9, ...fit(1700, 30, 2560, 900)],                 // eight workers deep
  [C.Screen + 4.2, ...fit(1720, 30, 2580, 700)],                 // the straggler, the dead letter
  [C.Relevant + 0.6, ...fit(1700, 120, 3700, 960)],
  [C.Findings + 0.8, ...fit(2950, 40, 3700, 940)],
  [C.Findings + 3.3, ...fit(2700, 20, 4300, 1060)],              // the fork
  [C.Grade - 0.8, ...fit(2900, -120, 6000, 1100)],
  [C.Grade + 0.9, ...fit(3740, -40, 4520, 520, 0.78)],           // grading
  [C.Grade + 3.9, ...fit(3760, -40, 4720, 520, 0.8)],            // the strong-evidence gate
  [C.Synthesis + 1.6, ...fit(4420, -40, 5180, 520, 0.8)],        // the tree
  [C.Synthesis + 3.5, ...fit(4980, 60, 5900, 600, 0.84)],        // the abstract
  [C.Synthesis + 6.1, ...fit(4440, 40, 5900, 600, 0.9)],
  [C.Questions + 1.0, ...fit(3740, 480, 4660, 1090, 0.82)],      // claims
  [C.Questions + 2.6, ...fit(3740, 480, 5120, 1090, 0.86)],
  [C.Questions + 3.9, ...fit(4600, 560, 5900, 1080, 0.86)],      // the open questions
  [C.Questions + 5.4, ...fit(4460, 560, 5900, 1080, 0.92)],
  [C.Questions + 6.4, ...fit(4400, 40, 6300, 1080)],             // both terminals, never joined
  [C.Ledger + 1.6, ...WORLD],                                    // the run report
  [DURATION - 1.1, ...WORLD],
  [DURATION - 0.1, ...fit(-2160, -90, -540, 1080, 0.9)],
];
const camX = monotone(CAM_KEYS.map((k) => k[0]), CAM_KEYS.map((k) => k[1]));
const camY = monotone(CAM_KEYS.map((k) => k[0]), CAM_KEYS.map((k) => k[2]));
const camZ = monotone(CAM_KEYS.map((k) => k[0]), CAM_KEYS.map((k) => Math.log(k[3])));
export const camera = (T) => ({ x: camX(T), y: camY(T), s: Math.exp(camZ(T)) });

/* ── drawing ──────────────────────────────────────────────────────────── */
/* The state of the frame being drawn. drawFrame sets it, everything below
   reads it; drawing is synchronous, so one module-level slot is enough. */
let g = null;

function setFont(f) { if (g.font !== f) { g.ctx.font = f; g.font = f; } }
function alpha(a) { g.ctx.globalAlpha = clamp(a * g.o, 0, 1); }
const hair = (w) => Math.max(w, g.px * 0.85);
const vis = (x, y, w, h) => x <= g.x1 && x + w >= g.x0 && y <= g.y1 && y + h >= g.y0;

function rrect(x, y, w, h, r) {
  const c = g.ctx;
  w = Math.max(w, 0.01); h = Math.max(h, 0.01);
  r = Math.max(0, Math.min(r, w / 2, h / 2));
  c.beginPath();
  if (c.roundRect) { c.roundRect(x, y, w, h, r); return; }
  c.moveTo(x + r, y);
  c.arcTo(x + w, y, x + w, y + h, r); c.arcTo(x + w, y + h, x, y + h, r);
  c.arcTo(x, y + h, x, y, r); c.arcTo(x, y, x + w, y, r); c.closePath();
}

/* A card: fill, border and — for the ones that are objects rather than
   tiles — a soft lift underneath instead of a blurred shadow, which the
   canvas would have to rasterise every frame. */
function box(x, y, w, h, r, fill, stroke, lw = 1, lift = false) {
  const c = g.ctx;
  if (lift) { rrect(x, y + 3, w, h, r); c.fillStyle = g.P.lift; c.fill(); }
  rrect(x, y, w, h, r);
  if (fill) { c.fillStyle = fill; c.fill(); }
  if (stroke) { c.strokeStyle = stroke; c.lineWidth = hair(lw); c.stroke(); }
}

/* Text, or — when it would come out smaller than anyone can read — a
   greeked bar of the same length, so a pulled-back camera sees the shape of
   a document instead of a few thousand pixels of noise. */
function text(s, x, y, f, px, col, align = 'left') {
  const c = g.ctx;
  setFont(f);
  if (px * g.lod < 5) {
    const w = measure(s, f);
    const x0 = align === 'right' ? x - w : align === 'center' ? x - w / 2 : x;
    const a = c.globalAlpha;
    c.globalAlpha = a * 0.32;
    c.fillStyle = col;
    c.fillRect(x0, y - px * 0.2, w, px * 0.4);
    c.globalAlpha = a;
    return w;
  }
  c.fillStyle = col;
  c.textAlign = align;
  c.fillText(s, x, y);
  return 0;
}

const MEASURE = new Map();
function measure(s, f) {
  const key = f + '\u0000' + s;
  let w = MEASURE.get(key);
  if (w === undefined) {
    setFont(f);
    w = g.ctx.measureText(s).width;
    if (MEASURE.size > 4000) MEASURE.clear();
    MEASURE.set(key, w);
  }
  return w;
}

/* Greedy word wrap, cached. With `max` lines, the last one is ellipsised. */
const WRAP = new Map();
function wrap(s, f, width, max = 99) {
  const key = f + '\u0000' + width + '\u0000' + max + '\u0000' + s;
  let lines = WRAP.get(key);
  if (lines) return lines;
  lines = [];
  let line = '';
  for (const word of s.split(' ')) {
    const next = line ? line + ' ' + word : word;
    if (line && measure(next, f) > width) { lines.push(line); line = word; } else line = next;
  }
  if (line) lines.push(line);
  if (lines.length > max) {
    lines = lines.slice(0, max);
    let last = lines[max - 1];
    while (last.length > 1 && measure(last + '…', f) > width) last = last.slice(0, -1).trimEnd();
    lines[max - 1] = last + '…';
  }
  WRAP.set(key, lines);
  return lines;
}

function ellipsis(s, f, width) {
  if (measure(s, f) <= width) return s;
  let t = s;
  while (t.length > 1 && measure(t + '…', f) > width) t = t.slice(0, -1);
  return t + '…';
}

function dot(x, y, r, col) {
  const c = g.ctx;
  c.beginPath(); c.arc(x, y, r, 0, Math.PI * 2);
  c.fillStyle = col; c.fill();
}

function ring(x, y, r, col, lw = 1.5, dash = null, offset = 0) {
  const c = g.ctx;
  c.beginPath(); c.arc(x, y, r, 0, Math.PI * 2);
  c.strokeStyle = col; c.lineWidth = hair(lw);
  if (dash) { c.setLineDash(dash); c.lineDashOffset = offset; }
  c.stroke();
  if (dash) c.setLineDash([]);
}

/* A moving record: a dot with a short streak behind it, drawn from where it
   was a few hundredths of a second ago. */
function token(x, y, px, py, r, col) {
  const c = g.ctx;
  const dx = x - px, dy = y - py, d = Math.hypot(dx, dy);
  if (d > r * 0.75) {
    // a comet: a wedge from the head's width down to a point at the tail
    const nx = -dy / d * r * 0.8, ny = dx / d * r * 0.8;
    const grad = c.createLinearGradient(px, py, x, y);
    grad.addColorStop(0, tint(col, 0));
    grad.addColorStop(1, tint(col, 0.42));
    c.beginPath();
    c.moveTo(px, py); c.lineTo(x + nx, y + ny); c.lineTo(x - nx, y - ny); c.closePath();
    c.fillStyle = grad; c.fill();
  }
  dot(x, y, r, col);
}

const TINT = new Map();
function tint(hex, a) {
  const key = hex + a;
  let v = TINT.get(key);
  if (!v) {
    const n = parseInt(hex.slice(1), 16);
    v = 'rgba(' + (n >> 16) + ',' + ((n >> 8) & 255) + ',' + (n & 255) + ',' + a + ')';
    TINT.set(key, v);
  }
  return v;
}

/* Glyphs drawn as paths, so they are the same weight on every platform and
   at every zoom instead of whatever the fallback font has for ✓ or ✧. */
function glyph(kind, cx, cy, r, col, lw = 2) {
  const c = g.ctx;
  c.beginPath();
  if (kind === 'check') {
    c.moveTo(cx - 0.5 * r, cy + 0.02 * r); c.lineTo(cx - 0.14 * r, cy + 0.36 * r); c.lineTo(cx + 0.52 * r, cy - 0.36 * r);
  } else if (kind === 'cross') {
    c.moveTo(cx - 0.4 * r, cy - 0.4 * r); c.lineTo(cx + 0.4 * r, cy + 0.4 * r);
    c.moveTo(cx + 0.4 * r, cy - 0.4 * r); c.lineTo(cx - 0.4 * r, cy + 0.4 * r);
  } else if (kind === 'up') {
    c.moveTo(cx - 0.4 * r, cy + 0.4 * r); c.lineTo(cx + 0.4 * r, cy - 0.4 * r);
    c.moveTo(cx - 0.1 * r, cy - 0.4 * r); c.lineTo(cx + 0.4 * r, cy - 0.4 * r); c.lineTo(cx + 0.4 * r, cy + 0.1 * r);
  } else if (kind === 'cache') {
    for (let i = 0; i < 8; i++) {
      const a = i * Math.PI / 4 - Math.PI / 2, rr = i % 2 ? 0.2 * r : 0.6 * r;
      if (i === 0) c.moveTo(cx + Math.cos(a) * rr, cy + Math.sin(a) * rr);
      else c.lineTo(cx + Math.cos(a) * rr, cy + Math.sin(a) * rr);
    }
    c.closePath();
  } else if (kind === 'retry') {
    c.arc(cx, cy, 0.42 * r, -Math.PI * 0.35, Math.PI * 1.25);
    const a = Math.PI * 1.25, ex = cx + Math.cos(a) * 0.42 * r, ey = cy + Math.sin(a) * 0.42 * r;
    c.moveTo(ex - 0.3 * r, ey - 0.05 * r); c.lineTo(ex, ey); c.lineTo(ex + 0.06 * r, ey - 0.3 * r);
  }
  c.strokeStyle = col; c.lineWidth = hair(lw); c.lineCap = 'round'; c.lineJoin = 'round';
  c.stroke();
}

/* An edge that draws itself from a to b, with a packet riding its head. */
function edge(a, b, p, col, lw, bend, head) {
  if (p <= 0) return;
  const c = g.ctx;
  const len = Math.hypot(b.x - a.x, b.y - a.y) * 1.12;
  c.beginPath();
  c.moveTo(a.x, a.y);
  c.bezierCurveTo(a.x + bend, a.y, b.x - bend, b.y, b.x, b.y);
  c.strokeStyle = col; c.lineWidth = hair(lw);
  if (p < 1) { c.setLineDash([len, len]); c.lineDashOffset = len * (1 - p); }
  c.stroke();
  if (p < 1) {
    c.setLineDash([]);
    if (head) {
      const u = p, v = 1 - u;
      const x = v * v * v * a.x + 3 * v * v * u * (a.x + bend) + 3 * v * u * u * (b.x - bend) + u * u * u * b.x;
      const y = v * v * v * a.y + 3 * v * v * u * a.y + 3 * v * u * u * b.y + u * u * u * b.y;
      dot(x, y, head, col);
    }
  }
}

function stageLabel(x, y, name, sub, w, o) {
  if (o <= 0 || !vis(x, y, w, 90)) return;
  const c = g.ctx;
  alpha(o);
  c.fillStyle = g.P.line2;
  c.fillRect(x, y, w * (0.4 + 0.6 * o), hair(1));
  text(name, x, y + 31, sans(30, 600), 30, g.P.ink);
  text(sub, x, y + 64, mono(22), 22, g.P.ink3);
}

/* a filter gate: a rule the records cross, labelled from above */
function gate(x, y0, y1, name, sub, o, flash = 0) {
  if (o <= 0 || !vis(x - 160, y0 - 90, 320, y1 - y0 + 90)) return;
  const c = g.ctx, P = g.P;
  alpha(o);
  const h = (y1 - y0) * ease.out(clamp(o * 1.2, 0, 1));
  c.fillStyle = P.line2;
  c.fillRect(x - hair(1), y0, hair(2), h);
  if (flash > 0) {
    alpha(o * flash);
    c.fillStyle = P.accent;
    c.fillRect(x - hair(1.5), y0, hair(3), h);
    alpha(o);
  }
  text(name, x, y0 - 66, sans(26, 600), 26, P.ink, 'center');
  text(sub, x, y0 - 34, mono(20), 20, P.ink3, 'center');
}

/* ── region 0: the findings gate ──────────────────────────────────────── */
function gateRegion(T) {
  const A = C.Ask, R = C.Reuse, t = T - R, P = g.P, c = g.ctx;
  const dim = 1 - 0.5 * tw(T, C.Ledger, C.Ledger + 1.2, ease.draw);
  g.o = g.out * dim;
  if (g.o <= 0.002 || !vis(-2240, -160, 2400, 1640)) return;

  const lab = tw(T, A + 0.1, A + 0.8);
  const railO = tw(T, R + 0.9, R + 1.6);
  const cardO = tw(T, R + 0.3, R + 0.9, ease.pop);
  const link = tw(T, C.Corpus - 0.8, C.Corpus + 0.5, ease.draw);

  let nClass = 0, nCoal = 0, nSrc = 0, nAsk = 0;
  for (const a of ASKS) {
    if (t >= a.t0) nAsk++;
    if (t >= a.hit) { if (a.verdict === 'class') nClass++; else if (a.verdict === 'coalesced') nCoal++; }
    if (a.verdict === 'research' && t >= a.ts[3]) nSrc++;
  }

  stageLabel(GT.qx, -58, 'research questions', '4 desks · 6 subjects · own phrasing', 520, lab);
  stageLabel(GT.sx, -58, 'findings gate', 'loom.WithFindings', GT.sw, lab);

  // the questions, grouped by subject
  for (let s = 0; s < 6; s++) {
    const gy = gGroupY(s);
    const o = tw(T, A + 0.30 + s * 0.42, A + 0.80 + s * 0.42);
    if (o <= 0) continue;
    alpha(o);
    text('subject ' + (s + 1) + ' · ' + SHORT[s] + ' · 4 wordings', GT.qx, gy - 13, mono(16), 16, P.ink3);
    c.fillStyle = P.line2;
    c.fillRect(GT.qx - 12, gy, hair(2), 4 * GT.rh - 6);
  }
  const chipFont = mono(16);
  for (const a of ASKS) {
    const inn = tw(T, A + 0.35 + a.s * 0.42 + a.d * 0.09, A + 0.85 + a.s * 0.42 + a.d * 0.09);
    if (inn <= 0) continue;
    const sent = clamp((t - a.t0) * 3, 0, 1);
    const res = clamp((t - a.arrive) * 3, 0, 1);
    const hue = ghue(a.verdict, P);
    alpha(inn * (1 - 0.5 * (sent - res)));
    const dx = (1 - inn) * -18;
    box(GT.qx + dx, a.y, GT.qw, GT.rh - 6, 6, P.surface, res > 0.5 ? hue : P.line);
    const tag = a.verdict === 'research' ? 'researched' : a.verdict === 'coalesced' ? 'coalesced' : 'class hit';
    const tagW = res > 0 ? measure(tag, mono(14)) + 12 : 0;
    text(ellipsis(a.text, chipFont, GT.qw - 22 - tagW), GT.qx + dx + 10, a.lane, chipFont, 16, P.ink2);
    if (res > 0) {
      alpha(inn * res);
      text(tag, GT.qx + dx + GT.qw - 10, a.lane, mono(14), 14, hue, 'right');
    }
  }

  // the gate: five tiers, each with a port on either side
  if (lab > 0) {
    alpha(lab);
    box(GT.sx, GT.sy, GT.sw, GT.sh, 12, P.surface, P.line, 1, true);
    ROWS.forEach(([name, sub, ry], i) => {
      const o = tw(T, A + 0.5 + i * 0.14, A + 1.0 + i * 0.14);
      const n = i === 1 ? nClass : i === 3 ? nCoal : i === 4 ? nSrc : 0;
      const col = i === 3 ? P.analyst : i === 4 ? P.oracle : P.accent;
      alpha(lab * o);
      if (i === 3) { c.fillStyle = P.line; c.fillRect(GT.sx + 20, ry - 62, GT.sw - 40, hair(1)); }
      text(name, GT.sx + 22, ry - 12, mono(24), 24, n > 0 ? col : P.ink3);
      text(String(n), GT.sx + GT.sw - 22, ry - 12, mono(30), 30, n > 0 ? col : P.ink3, 'right');
      text(sub, GT.sx + 22, ry + 20, mono(17), 17, P.ink3);
      // ports, lit for a moment by every question the row takes
      let flash = 0;
      for (const a of ASKS) if (a.row === i) flash = Math.max(flash, 1 - clamp(Math.abs(t - a.hit) / 0.35, 0, 1));
      for (const px of [GT.sx, GT.sx + GT.sw]) {
        dot(px, ry, 6, P.bg);
        ring(px, ry, 6, flash > 0 ? col : P.line2, 1.5);
        if (flash > 0) { alpha(lab * o * flash); dot(px, ry, 3.5, col); alpha(lab * o); }
      }
    });
    alpha(lab);
    text('gate overhead 47µs · question', GT.sx + 22, GT.sy + GT.sh - 30, mono(17), 17, P.ink3);

    // the public source
    alpha(lab);
    box(GT.cx, GT.cy, GT.cw, GT.ch, 12, P.surface, nSrc > 0 ? P.oracle : P.line, 1, true);
    text('mcp/web/search', GT.cx + 18, GT.cy + 32, mono(22), 22, P.oracle);
    text('public source', GT.cx + GT.cw - 18, GT.cy + 32, mono(17), 17, P.ink3, 'right');
    text('120ms · billed per query', GT.cx + 18, GT.cy + 66, mono(17), 17, P.ink3);
    text(nSrc + ' of ' + ASKS.length + ' questions', GT.cx + 18, GT.cy + GT.ch - 76, mono(22), 22, P.oracle);
    for (let s = 0; s < 6; s++) {
      const sl = srcSlot(s);
      const a = ASKS[s * 4];
      const lit = clamp((t - a.ts[3]) * 4, 0, 1);
      box(sl.x, sl.y, sl.w, sl.h, 5, lit > 0 ? P.wash : null, lit > 0.5 ? P.oracle : P.line2, 1.5);
    }
  }

  // the findings rail: every question ends here, answered
  if (railO > 0) {
    gate(GT.rail, 30, 1050, 'findings', '24 covered · ' + nSrc + ' researched', railO);
    for (const a of ASKS) {
      const lit = clamp((t - a.arrive) * 5, 0, 1);
      if (lit <= 0) continue;
      alpha(railO * lit);
      c.fillStyle = ghue(a.verdict, P);
      c.fillRect(GT.rail - 9, a.lane - hair(1.5), 18 * lit, hair(3));
    }
  }

  // the questions in flight
  for (const a of ASKS) {
    if (t < a.t0 || t > a.arrive + 0.2) continue;
    const at = (u) => [interp(a.ts, a.xs, ease.inOut, u), interp(a.ts, a.ys, ease.sine, u)];
    // inside the gate, between its two ports, a question is out of sight
    const inside = t > a.ts[1] + 0.02 && t < a.ts[2] - 0.02;
    if (inside) continue;
    const fade = clamp((t - a.t0) * 8, 0, 1) * (1 - clamp((t - a.arrive) * 5, 0, 1));
    const [x, y] = at(t);
    const [px, py] = at(Math.max(a.t0, t - 0.06));
    alpha(fade);
    token(x, y, px, py, 8, ghue(a.verdict, P));
    if (a.verdict === 'coalesced') {
      const wait = clamp((t - a.ts[3]) * 5, 0, 1) * (1 - clamp((t - a.ts[4]) * 5, 0, 1));
      if (wait > 0) { alpha(wait * 0.9); ring(x, y, 15, P.analyst, 1.5, [4, 4], -T * 18); }
    }
  }

  // the efficiency card
  if (cardO > 0 && vis(GT.mx, GT.my, GT.mw, GT.mh)) {
    const sc = 0.97 + 0.03 * cardO;
    c.save();
    c.translate(GT.mx, GT.my); c.scale(sc, sc); c.translate(-GT.mx, -GT.my);
    alpha(Math.min(cardO, 1));
    box(GT.mx, GT.my, GT.mw, GT.mh, 16, P.surface, P.line, 1, true);
    text('findings', GT.mx + 36, GT.my + 50, mono(34), 34, P.accent);
    text('loom.WithFindings · Gate: mcp/web/search', GT.mx + GT.mw - 36, GT.my + 50, mono(28), 28, P.ink3, 'right');
    const stat = (x, n, label, col) => {
      text(String(n), x, GT.my + 132, mono(84), 84, col);
      text(label, x, GT.my + 198, sans(32), 32, P.ink2);
    };
    stat(GT.mx + 36, nAsk, 'research questions', P.ink);
    text('→', GT.mx + 420, GT.my + 140, mono(52), 52, P.ink3);
    stat(GT.mx + 520, nSrc, 'external source calls', P.oracle);
    text('·', GT.mx + 920, GT.my + 140, mono(52), 52, P.ink3);
    stat(GT.mx + 1000, nClass + nCoal, 'reused / coalesced', P.accent);
    c.fillStyle = P.line;
    c.fillRect(GT.mx + 36, GT.my + 232, GT.mw - 72, hair(1));
    text('exact 0 · class ' + nClass + ' · near 0 · coalesced ' + nCoal, GT.mx + 36, GT.my + 268, mono(28), 28, P.ink3);
    text('$0.0960 → $0.0240 · 415ms → 171ms', GT.mx + GT.mw - 36, GT.my + 268, mono(28), 28, P.ink3, 'right');
    c.restore();
    g.font = '';
  }

  // what the six calls returned becomes the corpus
  if (link > 0) {
    alpha(0.9);
    [[250, 250], [470, 430], [700, 700], [910, 930]].forEach(([y0, y1], i) => {
      const p = clamp(link * 1.12 - i * 0.06, 0, 1);
      edge({ x: GT.rail, y: y0 }, { x: G.paper.x - 8, y: y1 }, p, P.line2, 1.4, 230, 4);
    });
    alpha(link);
    text(nSrc + ' source calls → ' + PAPERS.length + ' paper records', GT.rail + 120, 1080, mono(21), 21, P.ink3);
  }
}

/* ── region 1: the corpus ─────────────────────────────────────────────── */
function paperGrid(T) {
  const P = g.P;
  g.o = g.out;
  stageLabel(G.paper.x, 54, 'papers', 'FromRecords · ' + PAPERS.length + ' records', 420, tw(T, C.Corpus + 0.1, C.Corpus + 0.8));
  if (T < C.Corpus || !vis(G.paper.x, G.paper.y, 5 * G.paper.cw, 10 * G.paper.ch + 40)) return;
  const titleFont = sans(18, 450);
  for (const p of PAPERS) {
    const cc = pcell(p.i);
    const inn = tw(T, C.Corpus + 0.25 + p.i * 0.042, C.Corpus + 0.85 + p.i * 0.042);
    if (inn <= 0) continue;
    const dropped = p.stub ? 1 - tw(T, C.Normalize + 2.6, C.Normalize + 3.3, ease.draw) : 1;
    if (dropped <= 0) continue;
    const sent = tw(T, C.Normalize + 0.3 + (p.i % 8) * 0.04, C.Normalize + 1.3);
    const y = cc.y + (1 - inn) * 14 + (1 - dropped) * 26;
    if (!vis(cc.x, y, G.paper.w, G.paper.h)) continue;
    alpha(inn * dropped * (1 - 0.55 * sent));
    box(cc.x, y, G.paper.w, G.paper.h, 8, P.surface, p.stub ? P.warn : P.line, 1);
    text(p.id, cc.x + 11, y + 18, mono(17), 17, p.stub ? P.warn : P.accent);
    text(ellipsis(p.stub ? 'no abstract' : p.venue, mono(15), 110), cc.x + G.paper.w - 11, y + 18, mono(15), 15, P.ink3, 'right');
    const lines = wrap(p.title, titleFont, G.paper.w - 22, 2);
    lines.forEach((l, j) => text(l, cc.x + 11, y + 41 + j * 21, titleFont, 18, P.ink));
  }
}

/* ── run-level broadcasts ─────────────────────────────────────────────── */
/* How much a broadcast is in use at T: from when its reader starts to when
   that stage has finished with it. */
function hotness(b, T) {
  return b.reader === 'normalize'
    ? tw(T, C.Normalize + 0.2, C.Normalize + 0.8) * (1 - tw(T, C.Normalize + 2.6, C.Normalize + 3.3, ease.draw))
    : b.reader === 'screen'
      ? tw(T, C.Screen + 0.1, C.Screen + 0.7) * (1 - tw(T, C.Relevant, C.Relevant + 0.8, ease.draw))
      : tw(T, C.Grade + 0.2, C.Grade + 0.8) * (1 - tw(T, C.Grade + 2.9, C.Grade + 3.6, ease.draw));
}

function broadcastBand(T) {
  const P = g.P, c = g.ctx;
  g.o = g.out;
  if (T < C.Broadcast || !vis(600, G.bcast.y - 140, 3400, 820)) return;
  const lab = tw(T, C.Broadcast + 0.1, C.Broadcast + 0.7);
  const settle = 1 - 0.58 * tw(T, C.Screen - 0.6, C.Screen + 0.6, ease.draw);
  alpha(lab);
  text('loom.WithBroadcast — registered once per run, referenced by content hash', 620, G.bcast.y - 70, mono(46), 46, P.ink3);

  BCASTS.forEach((b) => {
    const p = tw(T, C.Broadcast + b.at + 0.35, C.Broadcast + b.at + 1.25, ease.draw);
    const x1 = b.x + 500, y1 = G.bcast.y + G.bcast.h, x2 = b.tx, y2 = b.ty;
    const bez = (u) => {
      const v = 1 - u;
      return [v * v * v * x1 + 3 * v * v * u * x1 + 3 * v * u * u * x2 + u * u * u * x2,
        v * v * v * y1 + 3 * v * v * u * (y1 + 70) + 3 * v * u * u * (y2 - 80) + u * u * u * y2];
    };
    // while its reader runs, the value is being read: packets run down the wire
    const reading = hotness(b, T);
    if (reading > 0) {
      for (let i = 0; i < 3; i++) {
        const u = (T * 0.7 + i / 3) % 1;
        const [x, y] = bez(ease.sine(u));
        alpha(reading * Math.sin(Math.PI * u));
        dot(x, y, 6, P.accent);
      }
    }
    if (p > 0) {
      // the wire draws itself down, a hash riding its head
      alpha(0.26 + 0.46 * settle);
      c.save();
      c.beginPath();
      c.rect(Math.min(x1, x2) - 40, y1 - 2, Math.abs(x2 - x1) + 80, Math.max(0.001, (y2 - y1 + 6) * p));
      c.clip();
      c.beginPath();
      c.moveTo(x1, y1);
      c.bezierCurveTo(x1, y1 + 70, x2, y2 - 80, x2, y2);
      c.strokeStyle = P.accent; c.lineWidth = hair(1.4);
      c.setLineDash([7, 7]); c.lineDashOffset = -T * 14;
      c.stroke(); c.setLineDash([]);
      c.restore();
      g.font = '';
      if (p < 1) {
        const yy = y1 + (y2 - y1) * p;
        // the bezier is monotone in y, so solve for the head's x by bisection
        let lo = 0, hi = 1;
        for (let i = 0; i < 14; i++) {
          const u = (lo + hi) / 2;
          if (bez(u)[1] < yy) lo = u; else hi = u;
        }
        alpha(1);
        dot(bez(lo)[0], yy, 6, P.accent);
      }
    }
    const a = tw(T, C.Broadcast + b.at + 1.05, C.Broadcast + b.at + 1.5, ease.pop);
    if (a > 0) {
      alpha(Math.min(a, 1) * (0.45 + 0.55 * settle));
      dot(b.tx, b.ty, 13 * (0.6 + 0.4 * a), P.bg);
      ring(b.tx, b.ty, 13 * (0.6 + 0.4 * a), P.accent, 2);
      const burst = clamp((T - (C.Broadcast + b.at + 1.05)) / 0.6, 0, 1);
      if (burst < 1) { alpha((1 - burst) * 0.6); ring(b.tx, b.ty, 14 + 26 * ease.out(burst), P.accent, 1.5); }
      const arrives = b.reader === 'normalize' ? C.Normalize : b.reader === 'screen' ? C.Screen : C.Grade;
      const named = Math.min(a, 1) * (1 - tw(T, arrives - 0.9, arrives - 0.3));
      if (named > 0) { alpha(named); text('→ ' + b.reader, b.tx + 26, b.ty, mono(26), 26, P.accent); }
    }
  });

  BCASTS.forEach((b) => {
    const inn = tw(T, C.Broadcast + b.at, C.Broadcast + b.at + 0.6, ease.pop);
    if (inn <= 0) return;
    const hot = hotness(b, T);
    const sc = 0.94 + 0.06 * inn, w = 1000, h = G.bcast.h;
    c.save();
    c.translate(b.x + w / 2, G.bcast.y + h); c.scale(sc, sc); c.translate(-(b.x + w / 2), -(G.bcast.y + h));
    alpha(Math.min(inn, 1) * (0.5 + 0.5 * settle));
    box(b.x, G.bcast.y, w, h, 16, P.surface, hot > 0.4 ? P.accent : P.line, hot > 0.4 ? 2 : 1, true);
    text(b.name, b.x + 32, G.bcast.y + 56, mono(48), 48, hot > 0.4 ? P.accent : P.ink);
    text('sha256:' + b.hash, b.x + w - 32, G.bcast.y + 56, mono(36), 36, P.ink3, 'right');
    text(b.body, b.x + 32, G.bcast.y + 130, sans(42), 42, P.ink2);
    text(b.reader, b.x + 32, G.bcast.y + h - 44, mono(38), 38, P.ink3);
    text('1 copy · ' + b.tasks + ' tasks', b.x + w - 32, G.bcast.y + h - 44, mono(38), 38, P.accent, 'right');
    c.restore();
    g.font = '';
  });
}

/* ── normalize + screenable: plain code, 50 → 48 ──────────────────────── */
function normPos(p, q) {
  const n = G.norm, src = pcell(p.i);
  const lane = n.y + 26 + (p.i / (PAPERS.length - 1)) * (n.h - 52);
  const inX = n.x, outX = n.x + n.w;
  const k = SCREENED.indexOf(p);
  const dst = k >= 0
    ? { x: scell(k).x + G.screen.s / 2, y: scell(k).y + G.screen.s / 2 }
    : { x: G.screenable.x + 24, y: lane };
  if (q < 0.4) {
    const u = ease.sine(q / 0.4);
    return [lerp(src.x + G.paper.w / 2, inX, u), lerp(src.y + G.paper.h / 2, lane, u)];
  }
  if (q < 0.62) return [lerp(inX, outX, (q - 0.4) / 0.22), lane];
  const u = (q - 0.62) / 0.38;
  return [lerp(outX, dst.x, u), lerp(lane, dst.y, ease.sine(u))];
}

function normalizeStage(T) {
  const P = g.P, n = G.norm;
  g.o = g.out;
  if (T < C.Broadcast + 4 || !vis(n.x - 1200, n.y - 120, 2400, n.h + 200)) return;
  const lab = tw(T, C.Normalize - 0.5, C.Normalize + 0.2);
  const gateO = tw(T, C.Normalize + 1.7, C.Normalize + 2.3);
  const kept = tw(T, C.Normalize + 3.1, C.Normalize + 3.7, ease.pop) * (1 - tw(T, C.Screen + 0.4, C.Screen + 1.2, ease.draw));

  stageLabel(n.x, -20, 'normalize', 'Map · ' + PAPERS.length + ' tasks · no model call', 440, lab);
  if (lab > 0) {
    alpha(lab);
    box(n.x, n.y, n.w, n.h, 12, P.surface, P.line, 1, true);
    ['venue → tier + weight', 'year, authors, area', 'abstract present?', 'field: screenable'].forEach((r, i) => {
      alpha(lab * tw(T, C.Normalize + 0.25 + i * 0.16, C.Normalize + 0.75 + i * 0.16));
      text(r, n.x + 18, n.y + 36 + i * 39, mono(19), 19, i === 3 ? P.accent : P.ink3);
    });
    // records crossing the stage light it from inside
    let busy = 0;
    for (const p of PAPERS) {
      const q = tw(T, C.Normalize + 0.25 + p.i * 0.03, C.Normalize + 2.5 + p.i * 0.03, ease.draw);
      if (q > 0.4 && q < 0.62) busy++;
    }
    if (busy > 0) {
      alpha(lab * Math.min(1, busy / 6) * 0.9);
      box(n.x, n.y, n.w, n.h, 12, P.wash, P.accent, 1.5);
    }
  }
  let crossing = 0;
  for (const p of PAPERS) {
    const q = tw(T, C.Normalize + 0.25 + p.i * 0.03, C.Normalize + 2.5 + p.i * 0.03, ease.draw);
    if (q > 0.62 && q < 0.7) crossing = Math.max(crossing, 1 - Math.abs(q - 0.66) / 0.04);
  }
  gate(G.screenable.x, 200, 960, 'screenable', 'Filter · ' + PAPERS.length + ' → ' + SCREENED.length, gateO, crossing * 0.5);
  if (kept > 0) {
    alpha(Math.min(kept, 1));
    text('2 papers have no abstract', G.screenable.x, 1000, mono(21), 21, P.warn, 'center');
  }
  for (const p of PAPERS) {
    const t0 = C.Normalize + 0.25 + p.i * 0.03, t1 = C.Normalize + 2.5 + p.i * 0.03;
    if (T < t0 || T > t1 + 0.1) continue;
    const q = tw(T, t0, t1, ease.draw), qp = tw(T - 0.05, t0, t1, ease.draw);
    const k = SCREENED.indexOf(p);
    const fade = k >= 0 ? 1 - clamp((q - 0.96) * 26, 0, 1) : 1 - clamp((q - 0.74) / 0.16, 0, 1);
    if (fade <= 0 || q <= 0.01) continue;
    const [x, y] = normPos(p, q), [px, py] = normPos(p, qp);
    alpha(fade);
    token(x, y + (k < 0 ? 40 * ease.inOut(clamp((q - 0.66) / 0.24, 0, 1)) : 0), px, py, 8, k >= 0 ? P.accent : P.warn);
  }
}

/* ── screen: 48 model calls, eight executors ──────────────────────────── */
function screenField(T) {
  const P = g.P, c = g.ctx, t = T - C.Screen;
  g.o = g.out;
  if (T < C.Screen - 0.7 || !vis(G.screen.x - 40, 0, 8 * G.screen.cw + 80, 6 * G.screen.ch + 300)) return;
  const lab = tw(T, C.Screen - 0.6, C.Screen + 0.2);
  stageLabel(G.screen.x, 150, 'screen', 'Infer · scout ↗ oracle · workers 8', 660, lab);

  for (let k = 0; k < 8; k++) {
    const run = SCHED.find((s) => s.k % 8 === k && t >= s.start && t < s.end);
    const ex = G.exec.x + k * G.exec.gap + 13, ey = G.exec.y + 13;
    if (run) {
      const cc = scell(run.k);
      alpha(lab * 0.4);
      c.beginPath(); c.moveTo(ex, ey + 18); c.lineTo(cc.x + G.screen.s / 2, cc.y + 4);
      c.strokeStyle = P.accent; c.lineWidth = hair(1.2); c.stroke();
    }
    alpha(lab);
    c.beginPath();
    c.moveTo(ex, ey - 18); c.lineTo(ex + 18, ey); c.lineTo(ex, ey + 18); c.lineTo(ex - 18, ey); c.closePath();
    if (run) { c.fillStyle = P.wash; c.fill(); }
    c.strokeStyle = run ? P.accent : P.line2; c.lineWidth = hair(1.5); c.stroke();
  }

  for (const { p, k, start, end, dur } of SCHED) {
    const cc = scell(k), S = G.screen.s;
    const inn = tw(T, C.Screen + start - 0.35, C.Screen + start, ease.pop);
    if (inn <= 0 || !vis(cc.x - 12, cc.y - 12, S + 24, S + 24)) continue;
    const prog = clamp((t - start) / dur, 0, 1);
    const retryTick = p.retry ? clamp((t - (start + dur * 0.35)) * 6, 0, 1) : 0;
    const esc = p.flag === 'garbleScreen' ? clamp((t - (start + dur * 0.55)) * 4, 0, 1) : 0;
    const dead = p.flag === 'dead' ? clamp((t - (start + dur * 0.7)) * 4, 0, 1) : 0;
    const done = (t >= end ? 1 : 0) * (1 - dead);
    const ring_ = p.flag === 'straggler' ? clamp((t - (start + 0.9)) * 2, 0, 1) * (1 - done) : 0;
    const cache = tw(T, C.Ledger + 2.8 + (cc.x - 1400) / 3400, C.Ledger + 3.3 + (cc.x - 1400) / 3400);
    const hue = dead ? P.warn : cache > 0.5 ? P.accent : esc > 0.5 ? P.oracle : done ? P.scout : P.line2;
    const sc = Math.min(inn, 1.06) * 0.1 + 0.9 + 0.06 * ring_;
    const w = S * sc, x = cc.x + (S - w) / 2, y = cc.y + (S - w) / 2;
    alpha(Math.min(inn, 1));
    box(x, y, w, w, 9, P.bg, null);
    box(x, y, w, w, 9, done || cache > 0.5 ? P.wash : null, hue, 1.5);
    const mark = Math.max(done, dead, cache);
    if (mark > 0) {
      alpha(mark);
      glyph(dead ? 'cross' : cache > 0.5 ? 'cache' : esc > 0.5 ? 'up' : 'check', cc.x + S / 2, cc.y + S / 2 - 3, 26, hue, 2.4);
      alpha(1);
    }
    const bar = (1 - Math.max(done, dead)) * (1 - cache);
    if (bar > 0 && t >= start - 0.1) {
      alpha(bar);
      c.fillStyle = P.line;
      c.fillRect(x + 7, y + w - 11, w - 14, 3);
      c.fillStyle = esc > 0.5 ? P.oracle : P.accent;
      c.fillRect(x + 7, y + w - 11, (w - 14) * prog, 3);
    }
    if (retryTick > 0) {
      alpha(retryTick * (1 - cache));
      dot(x + w + 2, y - 2, 11, P.bg);
      ring(x + w + 2, y - 2, 11, P.warn, 1.5);
      glyph('retry', x + w + 2, y - 2, 15, P.warn, 1.6);
    }
    if (ring_ > 0) {
      alpha(ring_ * 0.9);
      c.beginPath();
      rrect(x - 7, y - 7, w + 14, w + 14, 13);
      c.strokeStyle = P.accent; c.lineWidth = hair(1.5);
      c.setLineDash([6, 6]); c.lineDashOffset = -T * 30; c.stroke(); c.setLineDash([]);
    }
  }
}

/* ── relevant-only and extract-findings ───────────────────────────────── */
function findingsField(T) {
  const P = g.P;
  g.o = g.out;
  if (T < C.Relevant - 0.5 || !vis(G.gate1.x - 1000, 0, 1900, 1000)) return;
  const gateO = tw(T, C.Relevant - 0.4, C.Relevant + 0.3);
  const lab = tw(T, C.Findings - 0.5, C.Findings + 0.2);

  let crossing = 0;
  const tokens = [];
  SCREENED.forEach((p, k) => {
    const t0 = C.Relevant + 0.25 + k * 0.022, t1 = C.Relevant + 1.5 + k * 0.022;
    if (T < t0 || T > t1 + 0.05) return;
    const from = scell(k);
    const passes = p.area && p.flag !== 'dead';
    const ri = RELEVANT.indexOf(p);
    const to = passes ? { x: fcell(ri).x + G.find.w / 2, y: fcell(ri).y + G.find.h / 2 } : { x: G.gate1.x - 40, y: from.y + 170 };
    const at = (u) => {
      const q = tw(u, t0, t1, ease.draw);
      return [lerp(from.x + G.screen.s / 2, to.x, q), lerp(from.y + G.screen.s / 2, to.y, ease.sine(q)), q];
    };
    const [x, y, q] = at(T), [px, py] = at(T - 0.05);
    if (Math.abs(x - G.gate1.x) < 30) crossing = 1;
    const fade = passes ? 1 - clamp((q - 0.96) * 25, 0, 1) : 1 - clamp((q - 0.55) / 0.4, 0, 1);
    tokens.push([x, y, px, py, fade, passes ? P.accent : P.warn]);
  });
  gate(G.gate1.x, 220, 900, 'relevant-only', 'Filter · 48 → ' + RELEVANT.length, gateO, crossing * 0.5);
  stageLabel(G.find.x, 62, 'extract-findings', 'Infer · ' + RELEVANT.length + ' tasks · ParseJSON', 560, lab);

  RELEVANT.forEach((p, k) => {
    const cc = fcell(k);
    const inn = tw(T, C.Findings + 0.15 + k * 0.045, C.Findings + 0.7 + k * 0.045, ease.pop);
    if (inn <= 0 || !vis(cc.x, cc.y, G.find.w, G.find.h)) return;
    const esc = p.flag === 'garbleExtract' ? tw(T, C.Findings + 1.5, C.Findings + 2.0) : 0;
    const cache = tw(T, C.Ledger + 3.0 + (cc.x - 1400) / 3400, C.Ledger + 3.5 + (cc.x - 1400) / 3400);
    const hue = cache > 0.5 ? P.accent : esc > 0.5 ? P.oracle : P.line;
    const sc = 0.9 + 0.1 * inn, w = G.find.w * sc, h = G.find.h * sc;
    const x = cc.x + (G.find.w - w) / 2, y = cc.y + (G.find.h - h) / 2;
    alpha(Math.min(inn, 1));
    box(x, y, w, h, 7, cache > 0.5 ? P.wash : P.surface, hue, esc > 0.5 || cache > 0.5 ? 1.5 : 1);
    g.ctx.fillStyle = esc > 0.5 ? P.oracle : P.accent;
    alpha(Math.min(inn, 1) * 0.75);
    rrect(x + 9, y + 9, (w - 18) * 0.78, 4, 2); g.ctx.fill();
    alpha(Math.min(inn, 1));
    text('+' + p.gain + '%', x + 9, y + 30, mono(18), 18, P.ink3);
    const bw = (w - 18 - 3 * (p.nFind - 1)) / p.nFind;
    g.ctx.fillStyle = P.line2;
    for (let j = 0; j < p.nFind; j++) g.ctx.fillRect(x + 9 + j * (bw + 3), y + h - 11, bw, 3);
  });

  for (const [x, y, px, py, fade, col] of tokens) {
    if (fade <= 0) continue;
    alpha(fade);
    token(x, y, px, py, 9, col);
  }
}

/* ── the fork ─────────────────────────────────────────────────────────── */
function split(T) {
  const P = g.P;
  const o = tw(T, C.Grade - 1.5, C.Grade - 0.7);
  g.o = g.out * o;
  if (o <= 0 || !vis(G.split.x - 40, 280, 200, 540)) return;
  const d = tw(T, C.Grade - 1.5, C.Grade - 0.4, ease.draw);
  edge({ x: G.split.x, y: 560 }, { x: G.split.x + 110, y: 300 }, d, P.line2, 1.5, 50, 4);
  edge({ x: G.split.x, y: 560 }, { x: G.split.x + 110, y: 790 }, d, P.line2, 1.5, 50, 4);
  alpha(1);
  text('branch', G.split.x - 30, 480, mono(20), 20, P.ink3);
}

/* ── branch A: grade → strong → synthesis tree → executive abstract ───── */
function branchA(T) {
  const P = g.P, c = g.ctx, L = TREE_A;
  const away = tw(T, C.Questions + 0.5, C.Questions + 1.4, ease.draw) * (1 - tw(T, C.Questions + 4.7, C.Questions + 5.4, ease.draw));
  g.o = g.out * (1 - 0.74 * away);
  if (T < C.Grade - 0.6 || !vis(G.grade.x - 40, -40, 2100, 640)) return;
  const lab = tw(T, C.Grade - 0.5, C.Grade + 0.2);
  const gateO = tw(T, C.Grade + 3.0, C.Grade + 3.5);
  const synLab = tw(T, C.Synthesis - 0.4, C.Synthesis + 0.3);
  const absIn = tw(T, C.Synthesis + 2.9, C.Synthesis + 3.5, ease.pop);
  const chars = Math.round(lerp(0, ABSTRACT.length, tw(T, C.Synthesis + 3.2, C.Synthesis + 5.0, ease.inOut)));

  stageLabel(G.grade.x, -16, 'grade-evidence', 'Infer · analyst ↗ oracle · Validate 1–5', 740, lab);
  const moved = tw(T, C.Grade + 3.4, C.Grade + 4.3, ease.draw);
  gate(G.gate2.x, 176, 470, 'strong-evidence', 'Filter · ≥3 · ' + RELEVANT.length + ' → ' + STRONG.length, gateO,
    moved > 0.3 && moved < 0.8 ? 0.5 : 0);
  stageLabel(G.syn.x, -16, 'synthesis', 'ReduceAI · FanIn 4 · ' + SYN.join(' → '), 620, synLab);

  const cache = tw(T, C.Ledger + 3.35, C.Ledger + 3.8);
  RELEVANT.forEach((p, k) => {
    const cc = gcell(k);
    const inn = tw(T, C.Grade + 0.2 + k * 0.05, C.Grade + 0.65 + k * 0.05, ease.pop);
    if (inn <= 0) return;
    const bad = p.flag === 'badGrade';
    const fix = bad ? tw(T, C.Grade + 2.05, C.Grade + 2.5) : 0;
    const weak = p.grade < 3;
    const si = STRONG.indexOf(p);
    const tgt = si >= 0 ? L.l0[si] : { x: cc.x, y: cc.y + 120 };
    const hue = cache > 0.5 ? P.accent : bad && fix < 0.5 ? P.warn : bad ? P.oracle : weak ? P.ink3 : P.analyst;
    const o = Math.min(inn, 1) * (weak ? 1 - moved : 1);
    if (o <= 0) return;
    const x = lerp(cc.x, tgt.x, moved), y = lerp(cc.y, tgt.y, ease.sine(moved));
    const w = lerp(G.grade.w, 26, moved), h = lerp(G.grade.h, 26, moved);
    if (!vis(x, y, w, h)) return;
    alpha(o);
    box(x, y, w, h, 7, P.bg, null);
    box(x, y, w, h, 7, weak ? null : P.wash, hue, 1.5);
    text(bad && fix < 0.5 ? '9' : String(p.grade), x + w / 2, y + h / 2 + 1, mono(22 * (1 - moved * 0.55)), 22 * (1 - moved * 0.55), hue, 'center');
    if (bad && T > C.Grade + 1.8 && T < C.Grade + 2.9) {
      // the out-of-range grade fails Validate and climbs the ladder
      const u = clamp((T - (C.Grade + 1.8)) / 1.1, 0, 1);
      alpha((1 - u) * 0.8);
      ring(x + w / 2, y + h / 2, 22 + 18 * ease.out(u), fix > 0.5 ? P.oracle : P.warn, 1.5);
    }
  });

  // the tree
  alpha(0.75);
  L.l0.forEach((a, k) => edge({ x: a.x + 26, y: a.y + 13 }, L.l1[Math.floor(k / 4)],
    tw(T, C.Synthesis + 0.35 + k * 0.03, C.Synthesis + 1.0 + k * 0.03, ease.draw), P.oracle, 1.2, 40, 3));
  L.l1.forEach((a, k) => edge({ x: a.x + 18, y: a.y }, L.l2[Math.floor(k / 4)],
    tw(T, C.Synthesis + 1.5 + k * 0.05, C.Synthesis + 2.0 + k * 0.05, ease.draw), P.oracle, 1.2, 40, 4));
  L.l2.forEach((a, k) => edge({ x: a.x + 22, y: a.y }, L.l3[0],
    tw(T, C.Synthesis + 2.3 + k * 0.06, C.Synthesis + 2.8 + k * 0.06, ease.draw), P.oracle, 1.2, 40, 5));
  [[L.l1, 1, 1.05, 18], [L.l2, 2, 1.95, 22], [L.l3, 3, 2.7, 26]].forEach(([nodes, lvl, at, r]) => nodes.forEach((n, k) => {
    const inn = tw(T, C.Synthesis + at + k * 0.05, C.Synthesis + at + 0.5 + k * 0.05, ease.pop);
    if (inn <= 0) return;
    const rr = r * (0.7 + 0.3 * inn);
    alpha(Math.min(inn, 1));
    dot(n.x, n.y, rr, cache > 0.5 ? P.wash : P.surface);
    ring(n.x, n.y, rr, cache > 0.5 ? P.accent : P.oracle, 1.5);
    if (lvl === 3) text('∑', n.x, n.y + 1, mono(22), 22, P.oracle, 'center');
  }));
  if (absIn > 0) {
    const e = tw(T, C.Synthesis + 2.9, C.Synthesis + 3.6, ease.draw);
    alpha(0.75);
    edge({ x: L.l3[0].x + 26, y: L.l3[0].y }, { x: G.abs.x, y: L.l3[0].y }, e, P.oracle, 1.2, 30, 4);
  }

  // the executive abstract
  const R = artifactRect(G.abs, ABSTRACT);
  if (absIn > 0 && vis(R.x, R.y, R.w, R.h)) {
    const sc = 0.95 + 0.05 * absIn;
    c.save();
    c.translate(R.x, R.y + R.h / 2); c.scale(sc, sc); c.translate(-R.x, -(R.y + R.h / 2));
    alpha(Math.min(absIn, 1));
    artifactCard(R, 'executive-abstract', 'oracle · 1 call', ABSTRACT, chars, P.oracle,
      'primary deliverable', STRONG.length + ' strong-evidence papers', T, C.Synthesis + 5.0);
    c.restore();
    g.font = '';
  }
}

/* A terminal artifact: header, the text as the model writes it, footer.
   It is as tall as the whole text, from the first character, so the card
   never grows while the model is writing into it. */
const BODY = sans(25), LH = 35.5;
const artifactRect = (r, body) => ({ ...r, h: 174 + (wrap(body, BODY, r.w - 48).length - 1) * LH });

function artifactCard(r, name, who, body, chars, col, foot, footR, T, doneAt) {
  const P = g.P, c = g.ctx;
  box(r.x, r.y, r.w, r.h, 12, P.surface, P.line, 1, true);
  text(name, r.x + 24, r.y + 34, mono(20), 20, col);
  text(who, r.x + r.w - 24, r.y + 34, mono(19), 19, P.ink3, 'right');
  const f = BODY, lh = LH;
  const lines = wrap(body, f, r.w - 48);
  let left = chars, lx = r.x + 24, ly = r.y + 82;
  for (let i = 0; i < lines.length && left > 0; i++) {
    const s = lines[i].slice(0, left);
    text(s, r.x + 24, r.y + 82 + i * lh, f, 25, P.ink);
    left -= lines[i].length + 1;
    lx = r.x + 24 + measure(s, f); ly = r.y + 82 + i * lh;
  }
  // a caret while the model is still writing, and a moment after
  if (chars > 0 && T < doneAt + 0.9 && (T < doneAt || Math.floor(T * 2.4) % 2 === 0)) {
    c.fillStyle = col;
    c.fillRect(lx + 3, ly - 13, 2.5, 26);
  }
  c.fillStyle = P.line;
  c.fillRect(r.x + 24, r.y + r.h - 56, r.w - 48, hair(1));
  text(foot, r.x + 24, r.y + r.h - 30, mono(19), 19, col);
  text(footR, r.x + r.w - 24, r.y + r.h - 30, mono(19), 19, P.ink3, 'right');
}

/* ── branch B: explode-claims → open-questions tree ───────────────────── */
function branchB(T) {
  const P = g.P, c = g.ctx, L = TREE_B;
  g.o = g.out;
  if (T < C.Questions - 0.6 || !vis(G.claim.x - 40, 500, 2100, 560)) return;
  const lab = tw(T, C.Questions - 0.5, C.Questions + 0.2);
  const outIn = tw(T, C.Questions + 3.3, C.Questions + 3.9, ease.pop);
  const chars = Math.round(lerp(0, QUESTIONS.length, tw(T, C.Questions + 3.6, C.Questions + 5.3, ease.inOut)));
  const cache = tw(T, C.Ledger + 3.4, C.Ledger + 3.9);

  stageLabel(G.claim.x, 520, 'explode-claims → claims', 'FlatMap + Filter (fused) · ' + CLAIMS + ' claims', 640, lab);
  stageLabel(G.oq.l1 - 40, 1010, 'open-questions', 'ReduceAI · FanIn 6 · ' + OQ.join(' → '), 560, tw(T, C.Questions + 1.4, C.Questions + 2.0));

  for (let k = 0; k < CLAIMS; k++) {
    const cc = ccell(k), src = fcell(k % RELEVANT.length);
    const q = tw(T, C.Questions + 0.15 + k * 0.012, C.Questions + 1.1 + k * 0.012, ease.draw);
    if (q <= 0.02) continue;
    const x = lerp(src.x + 30, cc.x, q), y = lerp(src.y + 30, cc.y, ease.sine(q));
    if (!vis(x, y, 30, 20)) continue;
    alpha(1);
    box(x, y, 26 + 4 * q, 20, 5, cache > 0.5 ? P.wash : null, cache > 0.5 ? P.accent : P.line2, 1);
  }
  // claim → first level: drawn as one path once they have all landed
  const p0 = (k) => tw(T, C.Questions + 1.35 + k * 0.006, C.Questions + 1.9 + k * 0.006, ease.draw);
  alpha(0.6);
  if (p0(CLAIMS - 1) >= 1) {
    c.beginPath();
    for (let k = 0; k < CLAIMS; k++) {
      const a = ccell(k), b = L.l1[Math.floor(k / 6)] || L.l1[L.l1.length - 1];
      c.moveTo(a.x + 30, a.y + 10);
      c.bezierCurveTo(a.x + 66, a.y + 10, b.x - 36, b.y, b.x, b.y);
    }
    c.strokeStyle = P.line2; c.lineWidth = hair(1); c.stroke();
  } else {
    for (let k = 0; k < CLAIMS; k++) {
      const a = ccell(k);
      edge({ x: a.x + 30, y: a.y + 10 }, L.l1[Math.floor(k / 6)] || L.l1[L.l1.length - 1], p0(k), P.line2, 1, 36, 0);
    }
  }
  alpha(0.75);
  L.l1.forEach((a, k) => edge({ x: a.x + 11, y: a.y }, L.l2[Math.floor(k / 6)],
    tw(T, C.Questions + 2.25 + k * 0.02, C.Questions + 2.7 + k * 0.02, ease.draw), P.scout, 1, 36, 3));
  L.l2.forEach((a, k) => edge({ x: a.x + 16, y: a.y }, L.l3[0],
    tw(T, C.Questions + 2.9 + k * 0.04, C.Questions + 3.3 + k * 0.04, ease.draw), P.scout, 1, 36, 4));
  [[L.l1, 1, 2.0, 11], [L.l2, 2, 2.65, 16], [L.l3, 3, 3.15, 24]].forEach(([nodes, lvl, at, r]) => nodes.forEach((n, k) => {
    const inn = tw(T, C.Questions + at + k * 0.03, C.Questions + at + 0.4 + k * 0.03, ease.pop);
    if (inn <= 0) return;
    const rr = r * (0.7 + 0.3 * inn);
    alpha(Math.min(inn, 1));
    dot(n.x, n.y, rr, cache > 0.5 ? P.wash : P.surface);
    ring(n.x, n.y, rr, cache > 0.5 ? P.accent : P.scout, 1.5);
    if (lvl === 3) text('?', n.x, n.y + 1, mono(20), 20, P.scout, 'center');
  }));
  if (outIn > 0) {
    alpha(0.75);
    edge({ x: L.l3[0].x + 24, y: L.l3[0].y }, { x: G.out.x, y: L.l3[0].y }, tw(T, C.Questions + 3.3, C.Questions + 3.9, ease.draw), P.scout, 1.2, 30, 4);
  }
  const R = artifactRect(G.out, QUESTIONS);
  if (outIn > 0 && vis(R.x, R.y, R.w, R.h)) {
    const sc = 0.95 + 0.05 * outIn;
    c.save();
    c.translate(R.x, R.y + R.h / 2); c.scale(sc, sc); c.translate(-R.x, -(R.y + R.h / 2));
    alpha(Math.min(outIn, 1));
    artifactCard(R, 'open-questions', 'scout · tree root', QUESTIONS, chars, P.scout,
      'exploratory companion', 'all ' + RELEVANT.length + ' papers · ' + CLAIMS + ' claims', T, C.Questions + 5.3);
    c.restore();
    g.font = '';
  }
}

/* ── the two terminal stages, side by side and never joined ───────────── */
function terminals(T) {
  const P = g.P, c = g.ctx;
  const o = tw(T, C.Questions + 5.5, C.Questions + 6.2);
  g.o = g.out * o;
  const x = G.abs.x + G.abs.w + 44;
  const top = G.abs.y + 40, bot = G.out.y + artifactRect(G.out, QUESTIONS).h - 40, mid = (top + bot) / 2;
  if (o <= 0 || !vis(x, top, 420, bot - top)) return;
  alpha(1);
  const d = tw(T, C.Questions + 5.5, C.Questions + 6.3, ease.draw);
  c.beginPath();
  c.moveTo(x, top); c.lineTo(x + 26, top); c.lineTo(x + 26, top + (bot - top) * d);
  if (d >= 1) c.lineTo(x, bot);
  c.strokeStyle = P.line2; c.lineWidth = hair(1.5); c.stroke();
  text('two terminal stages', x + 48, mid - 16, mono(22), 22, P.ink3);
  text('the pipeline never joins them', x + 48, mid + 16, mono(22), 22, P.ink3);
}

/* The rerun: a sweep crosses the DAG left to right and every task it passes
   settles in the cache-replay hue. */
function replaySweep(T) {
  const u = (T - (C.Ledger + 2.8)) / 1.4;
  if (u <= 0 || u >= 1) return;
  const P = g.P, c = g.ctx;
  g.o = g.out;
  const x = 1400 + u * 3400;
  const grad = c.createLinearGradient(x - 420, 0, x + 40, 0);
  grad.addColorStop(0, 'rgba(' + (P === THEMES.dark ? '111,211,242' : '6,117,143') + ',0)');
  grad.addColorStop(1, 'rgba(' + (P === THEMES.dark ? '111,211,242' : '6,117,143') + ',0.10)');
  alpha(Math.sin(Math.PI * u));
  c.fillStyle = grad;
  c.fillRect(x - 420, -460, 460, 1520);
  c.fillStyle = P.accent;
  c.fillRect(x, -460, hair(2.5), 1520);
}

/* ── HUD (screen space) ───────────────────────────────────────────────── */
const STAGES = [
  'findings gate · 24 questions', 'findings gate · reuse + single-flight', 'papers', 'broadcasts',
  'normalize · screenable', 'screen', 'relevant-only', 'extract-findings', 'grade-evidence',
  'synthesis → executive-abstract', 'explode-claims → open-questions', 'run report',
];
export const sceneAt = (T) => {
  let i = 0;
  while (i < SCENES.length - 1 && T >= CUES[SCENES[i + 1].name]) i++;
  return i;
};

function tasksDone(T) {
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
  return Math.min(done, TASKS);
}

function hud(T, captions) {
  const P = g.P, c = g.ctx;
  g.o = 1;
  const t = (x) => T - x;
  // loop seam: the HUD crossfades into what the first frame shows
  const fade = 1 - tw(T, DURATION - 1.15, DURATION - 0.35, ease.draw);

  // On a narrow frame the HUD would come out below reading size, so it is
  // scaled up around its corner until the stage line is ~10 CSS px — and,
  // being bigger, keeps only what it has room for: the stage and the count.
  const hs = clamp(10 / (23 * g.lod), 1, 2.3), compact = hs > 1.35;
  const at = (ax, ay, sc, fn) => {
    const lod = g.lod;
    c.save();
    c.translate(ax, ay); c.scale(sc, sc); c.translate(-ax, -ay);
    g.lod = lod * sc;
    fn();
    g.lod = lod;
    c.restore();
    g.font = '';
  };

  // a scrim under the HUD, so the world passes beneath it rather than through it
  const grad = c.createLinearGradient(0, 0, 0, 160 * hs);
  grad.addColorStop(0, 'rgba(' + P.bgRGB + ',0.97)');
  grad.addColorStop(0.62, 'rgba(' + P.bgRGB + ',0.9)');
  grad.addColorStop(1, 'rgba(' + P.bgRGB + ',0)');
  c.globalAlpha = 1;
  c.fillStyle = grad;
  c.fillRect(0, 0, 1920, 160 * hs);

  // top left: the pipeline and the stage it is in
  at(0, 0, hs, () => {
    let x = 64;
    const f26 = mono(26);
    alpha(1);
    text('pipeline.New(', x, 62, f26, 26, P.ink3);
    x += measure('pipeline.New(', f26);
    text('"lit-survey"', x, 62, f26, 26, P.accent);
    x += measure('"lit-survey"', f26);
    text(')', x, 62, f26, 26, P.ink3);

    const i = sceneAt(T), since = T - CUES[SCENES[i].name];
    const sw = clamp(since / 0.4, 0, 1);
    const label = (s, o, dy) => { alpha(o); text('stage: ' + s, 64, 100 + dy, mono(23), 23, P.ink2); };
    if (fade < 1) {
      label(STAGES[i], fade, 0);
      label(STAGES[0], 1 - fade, 0);
    } else if (sw < 1 && i > 0) {
      label(STAGES[i - 1], 1 - ease.out(sw), -8 * ease.out(sw));
      label(STAGES[i], ease.out(sw), 8 * (1 - ease.out(sw)));
    } else label(STAGES[i], 1, 0);
  });

  // top right: the run, counted
  at(1920, 0, hs, () => {
    const done = tasksDone(T);
    const retries = SCHED.filter((s) => s.p.retry && t(C.Screen) >= s.start + s.dur * 0.35).length;
    const escal = (t(C.Screen) > 1.6 ? 1 : 0) + (t(C.Findings) > 2.0 ? 1 : 0) + (t(C.Grade) > 2.5 ? 1 : 0);
    const dead = t(C.Screen) > 1.4 ? 1 : 0;
    const stats = [
      [done + '/' + TASKS, 'tasks', P.ink, 1],
      [String(retries), 'retries', P.warn, clamp((t(C.Screen) - 0.9) * 4, 0, 1) * (retries > 0 ? 1 : 0)],
      [String(escal), escal === 1 ? 'escalation' : 'escalations', P.oracle, clamp((t(C.Screen) - 1.6) * 4, 0, 1)],
      [String(dead), 'dead letter', P.warn, clamp((t(C.Screen) - 1.4) * 4, 0, 1)],
    ];
    const fv = mono(25), fl = mono(21);
    // Items slide in from the right as they first appear, so the row grows
    // instead of jumping.
    let right = 1856;
    for (let j = compact ? 0 : stats.length - 1; j >= 0; j--) {
      const [v, l, col, o] = stats[j];
      if (o <= 0) continue;
      const w = measure(v, fv) + 9 + measure(l, fl);
      alpha(o * fade);
      text(l, right, 62, fl, 21, P.ink3, 'right');
      text(v, right - measure(l, fl) - 9, 62, fv, 25, col, 'right');
      right -= (w + 28) * ease.out(o);
    }
    if (fade < 1) {
      alpha(1 - fade);
      text('tasks', 1856, 62, fl, 21, P.ink3, 'right');
      text('0/' + TASKS, 1856 - measure('tasks', fl) - 9, 62, fv, 25, P.ink, 'right');
    }
    // the run's progress, as a hairline under the count
    const bw = compact ? 200 : 300;
    alpha(0.9 * fade);
    c.fillStyle = P.line;
    c.fillRect(1856 - bw, 84, bw, 2);
    c.fillStyle = P.accent;
    c.fillRect(1856 - bw, 84, bw * (done / TASKS), 2);

    const cached = clamp((T - (C.Ledger + 3.9)) * 3, 0, 1);
    if (cached > 0) {
      alpha(cached * fade);
      const l2 = 'model calls', v2 = '0', l1 = 'cache hits', v1 = String(TASKS);
      let r = 1856;
      text(l2, r, 112, fl, 21, P.ink3, 'right'); r -= measure(l2, fl) + 9;
      text(v2, r, 112, fv, 25, P.accent, 'right'); r -= measure(v2, fv) + 28;
      if (!compact) {
        text(l1, r, 112, fl, 21, P.ink3, 'right'); r -= measure(l1, fl) + 9;
        text(v1, r, 112, fv, 25, P.accent, 'right');
      }
    }
  });

  // the rerun, typed
  const cmdO = tw(T, C.Ledger + 2.35, C.Ledger + 2.8) * fade;
  if (cmdO > 0) at(0, 1080, Math.min(hs, 1.9), () => {
    const n = Math.round(lerp(0, CMD.length, tw(T, C.Ledger + 2.5, C.Ledger + 3.4, ease.inOut)));
    const f = mono(25);
    const w = measure('$ ' + CMD, f);
    alpha(cmdO * 0.92);
    box(44, 900, w + 44, 56, 10, P.bg, null);
    alpha(cmdO);
    text('$ ', 66, 929, f, 25, P.ink3);
    const s = CMD.slice(0, n);
    text(s, 66 + measure('$ ', f), 929, f, 25, P.ink2);
    if (n < CMD.length || Math.floor(T * 2.4) % 2 === 0) {
      c.fillStyle = P.accent;
      c.fillRect(66 + measure('$ ' + s, f) + 3, 916, 12, 26);
    }
  });

  if (captions) at(960, 1080, clamp(12 / (33 * g.lod), 1, 2), () => caption(T, clamp(12 / (33 * g.lod), 1, 2)));
}

/* ── captions (the standalone player only) ────────────────────────────── */
const CAPTIONS = [
  [C.Ask + 0.5, C.Ask + 2.7, 'Before any of this: 24 research questions, four desks, six subjects, each desk in its own phrasing.'],
  [C.Ask + 3.0, C.Reuse + 0.3, 'A result cache keys on the bytes, so it sees 24 different questions. The gate keys on the question.'],
  [C.Reuse + 0.5, C.Reuse + 2.5, 'Six miss and reach the public source. Six more ask at the same instant and wait on the leader’s flight instead of calling.'],
  [C.Reuse + 2.7, C.Reuse + 4.1, 'Twelve are answered from findings already in the ledger — same subject, different wording, no call and no model.'],
  [C.Reuse + 4.3, C.Corpus + 0.5, '24 questions, 6 external calls, 18 reused. What those six calls returned is what enters the pipeline.'],
  [C.Corpus + 0.9, C.Corpus + 2.2, '50 papers on self-improving agents. One record each.'],
  [C.Broadcast + 0.7, C.Broadcast + 2.3, 'Three values every task needs: the venue tiers, the inclusion criteria, the grading rubric.'],
  [C.Broadcast + 2.5, C.Broadcast + 4.9, 'Registered once and referenced by hash — not pasted into ' + TASKS + ' prompts. Edit one and only its readers recompute.'],
  [C.Normalize + 0.4, C.Normalize + 2.3, 'The first stage is plain code: tag the venue tier, and mark whether a paper is screenable.'],
  [C.Normalize + 2.5, C.Normalize + 4.7, 'Two position papers carry no abstract. They fail the filter and leave before any model call: 50 → 48.'],
  [C.Screen + 0.5, C.Screen + 3.4, '48 screening calls on the cheap tier, eight workers deep.'],
  [C.Screen + 3.7, C.Screen + 6.2, 'A rate limit retries. Garbled output climbs to the deep model. A retracted paper dead-letters — the run continues.'],
  [C.Relevant + 0.4, C.Relevant + 2.6, 'Seven leave here: one dead letter and six out of scope. 41 go through.'],
  [C.Findings + 0.5, C.Findings + 2.6, 'Findings extracted per paper, parsed into the record.'],
  [C.Findings + 3.2, C.Grade - 0.2, 'The DAG forks here: grade the evidence, and explode the claims.'],
  [C.Grade + 0.5, C.Grade + 2.9, 'Grades scored against one shared rubric. An out-of-range 9 fails Validate and escalates.'],
  [C.Grade + 3.4, C.Grade + 4.9, 'Keep evidence grade 3 and up: ' + STRONG.length + ' findings.'],
  [C.Synthesis + 0.4, C.Synthesis + 3.1, 'ReduceAI is a tree, not a loop: ' + SYN.join(' → ') + ', four at a time.'],
  [C.Synthesis + 3.2, C.Synthesis + 6.2, 'One deep-model call turns the synthesis into the abstract — built only from the ' + STRONG.length + ' strong papers.'],
  [C.Questions + 0.4, C.Questions + 2.2, 'The other branch fans out instead: ' + CLAIMS + ' claims from all ' + RELEVANT.length + ' papers, weak evidence included.'],
  [C.Questions + 2.4, C.Questions + 5.2, 'A second tree, fan-in 6, distils them into the open questions. Deliberately exploratory, not a graded finding.'],
  [C.Questions + 5.4, C.Questions + 6.9, 'Two terminal stages, never joined: the abstract is the deliverable, the open questions its companion.'],
  [C.Ledger + 0.6, C.Ledger + 2.6, TASKS + ' tasks across one DAG. No keys, no network.'],
  [C.Ledger + 2.9, C.Ledger + 4.6, 'Run it again with a state dir: the whole sky replays from cache. Zero model calls.'],
];

function caption(T, sc = 1) {
  const P = g.P;
  for (const [a, b, s] of CAPTIONS) {
    if (T < a - 0.3 || T > b + 0.3) continue;
    const o = Math.min(clamp((T - a + 0.3) / 0.3, 0, 1), clamp((b + 0.3 - T) / 0.3, 0, 1));
    const f = sans(33, 500);
    const lines = wrap(s, f, Math.round(1560 / sc));
    const w = Math.max(...lines.map((l) => measure(l, f)));
    const h = lines.length * 44 + 28;
    const y = 1080 - 52 - h;
    alpha(o * 0.94);
    box(960 - w / 2 - 28, y, w + 56, h, 12, P.bg, P.line, 1);
    alpha(o);
    lines.forEach((l, j) => text(l, 960, y + 14 + 22 + j * 44, f, 33, P.ink2, 'center'));
  }
}

/* ── one frame ────────────────────────────────────────────────────────── */
/* Paints the run at time T into ctx. The canvas is any size: the 1920×1080
   composition is fitted into it and centred, and the margins are the page's
   own ground. `dpr` is the canvas's device pixels per CSS pixel; it keeps
   hairlines a real pixel wide and decides when text is too small to set. */
export function drawFrame(ctx, T, { width, height, dpr = 1, dark = false, captions = false } = {}) {
  const P = dark ? THEMES.dark : THEMES.light;
  T = clamp(T, 0, DURATION);
  const fw = Math.min(width, (height * 16) / 9), fh = (fw * 9) / 16;
  const ox = (width - fw) / 2, oy = (height - fh) / 2, k = fw / 1920;

  ctx.setTransform(1, 0, 0, 1, 0, 0);
  ctx.globalAlpha = 1;
  ctx.fillStyle = P.bg;
  ctx.fillRect(0, 0, width, height);
  ctx.save();
  ctx.beginPath(); ctx.rect(ox, oy, fw, fh); ctx.clip();

  const cam = camera(T), s = cam.s;
  ctx.setTransform(k * s, 0, 0, k * s, ox + k * (960 - cam.x * s), oy + k * (540 - cam.y * s));
  ctx.textBaseline = 'middle';
  ctx.lineCap = 'round';
  ctx.lineJoin = 'round';
  g = {
    ctx, P, font: '', o: 1,
    out: 1 - tw(T, DURATION - 1.15, DURATION - 0.35, ease.draw),
    px: dpr / (k * s), lod: (k * s) / dpr,
    x0: cam.x - 960 / s, x1: cam.x + 960 / s, y0: cam.y - 540 / s, y1: cam.y + 540 / s,
  };
  if (g.out > 0) {
    gateRegion(T);
    paperGrid(T);
    broadcastBand(T);
    normalizeStage(T);
    screenField(T);
    findingsField(T);
    split(T);
    branchA(T);
    branchB(T);
    terminals(T);
    replaySweep(T);
  }

  ctx.setTransform(k, 0, 0, k, ox, oy);
  g.px = dpr / k; g.lod = k / dpr;
  g.x0 = 0; g.x1 = 1920; g.y0 = 0; g.y1 = 1080;
  hud(T, captions);
  ctx.restore();
  ctx.globalAlpha = 1;
  g = null;
}

/* ── a player ─────────────────────────────────────────────────────────── */
/* Mounts the run on a canvas and keeps it playing — but only while someone
   can see it. Frames stop when the canvas scrolls out of view or the tab is
   hidden and pick up where they left off, so the loop costs nothing while
   the reader is elsewhere on the page.

   options: autoplay (default true), captions (default false), time (the
   frame to open on). Returns a small controller; on(fn) is called with it
   after every frame and every change of state. */
export function mountSurvey(canvas, opts = {}) {
  const ctx = canvas.getContext('2d', { alpha: false }) || canvas.getContext('2d');
  if (!ctx) throw new Error('survey: no 2D canvas');
  const mq = window.matchMedia ? window.matchMedia('(prefers-color-scheme: dark)') : null;
  const st = {
    T: clamp(opts.time ?? 0, 0, DURATION),
    want: opts.autoplay !== false,
    inView: true,
    shown: !document.hidden,
    captions: !!opts.captions,
    dark: !!(mq && mq.matches),
  };
  const subs = new Set();
  let raf = 0, last = 0, dpr = 1;

  const api = {
    duration: DURATION,
    scenes: SCENES,
    cues: CUES,
    get time() { return st.T; },
    get playing() { return st.want; },
    get captions() { return st.captions; },
    get scene() { return sceneAt(st.T); },
    play() { st.want = true; sync(); emit(); },
    pause() { st.want = false; sync(); emit(); },
    toggle() { st.want ? api.pause() : api.play(); },
    seek(t) { st.T = ((t % DURATION) + DURATION) % DURATION; render(); emit(); },
    setCaptions(on) { st.captions = !!on; render(); emit(); },
    on(fn) { subs.add(fn); return () => subs.delete(fn); },
    destroy() {
      st.want = false; sync();
      ro && ro.disconnect(); io && io.disconnect();
      document.removeEventListener('visibilitychange', onVis);
      if (mq) mq.removeEventListener ? mq.removeEventListener('change', onScheme) : mq.removeListener(onScheme);
      subs.clear();
    },
  };
  const emit = () => subs.forEach((fn) => fn(api));

  function fit() {
    const r = canvas.getBoundingClientRect();
    // two device pixels per CSS pixel is as sharp as this needs to be, and
    // past ~4K of backing store the fill rate is better spent elsewhere
    dpr = Math.min(window.devicePixelRatio || 1, 2, 3840 / Math.max(1, r.width));
    const w = Math.max(1, Math.round(r.width * dpr)), h = Math.max(1, Math.round(r.height * dpr));
    if (canvas.width !== w || canvas.height !== h) { canvas.width = w; canvas.height = h; }
  }
  function render() {
    drawFrame(ctx, st.T, { width: canvas.width, height: canvas.height, dpr, dark: st.dark, captions: st.captions });
  }
  function frame(now) {
    // a long gap (a dropped tab, a debugger) is not time the run should skip
    const dt = Math.min((now - last) / 1000, 0.1);
    last = now;
    st.T = (st.T + dt) % DURATION;
    render();
    emit();
    raf = requestAnimationFrame(frame);
  }
  function sync() {
    const run = st.want && st.inView && st.shown;
    if (run && !raf) { last = performance.now(); raf = requestAnimationFrame(frame); }
    else if (!run && raf) { cancelAnimationFrame(raf); raf = 0; }
  }
  const onVis = () => { st.shown = !document.hidden; sync(); };
  const onScheme = (e) => { st.dark = e.matches; render(); };

  document.addEventListener('visibilitychange', onVis);
  if (mq) mq.addEventListener ? mq.addEventListener('change', onScheme) : mq.addListener(onScheme);
  const ro = typeof ResizeObserver === 'function' ? new ResizeObserver(() => { fit(); render(); }) : null;
  ro && ro.observe(canvas);
  const io = typeof IntersectionObserver === 'function'
    ? new IntersectionObserver((es) => { st.inView = es[es.length - 1].isIntersecting; sync(); })
    : null;
  io && io.observe(canvas);

  fit();
  render();
  sync();
  return api;
}
