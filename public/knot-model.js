/**
 * woven_knot — four glass strands braided around a (2,3) torus knot, a
 * glowing thread down the core, and light pulses travelling the strands.
 *
 * Imported from the Claude Design project "3D object modeling request".
 * The geometry, materials and motion are the design's; the only addition
 * is the reduced-motion path at the bottom, so the page can hold the
 * object still for visitors who ask for that.
 */
const stage = document.querySelector('three-d-stage');
const { THREE } = await stage.ready;

/* ── materials ── */
const M = {
  glass: new THREE.MeshPhysicalMaterial({
    name: 'frosted_glass',
    color: 0xdce9ef, roughness: 0.38, metalness: 0.0,
    transmission: 0.95, thickness: 0.42, ior: 1.44,
    attenuationColor: new THREE.Color(0xa8d8e8), attenuationDistance: 1.6,
    clearcoat: 0.6, clearcoatRoughness: 0.35,
    transparent: true, side: THREE.DoubleSide,
  }),
  core: new THREE.MeshStandardMaterial({
    name: 'inner_glow', color: 0xdff6ff,
    emissive: 0x6fd3f2, emissiveIntensity: 1.35, roughness: 0.5, metalness: 0.0,
  }),
  pulse: new THREE.MeshStandardMaterial({
    name: 'light_pulse', color: 0xffffff,
    emissive: 0xbdeeff, emissiveIntensity: 2.6, roughness: 0.35, metalness: 0.0,
  }),
};

const obj = new THREE.Group(); obj.name = 'woven_knot';
const spin = new THREE.Group(); spin.name = 'knot_spin'; obj.add(spin);

/* ── base path: (2,3) torus knot, standing upright in the xy plane ── */
const S = 0.30, P = 2, Q = 3;
class KnotCurve extends THREE.Curve {
  getPoint(u, target = new THREE.Vector3()) {
    const t = u * Math.PI * 2 * P;
    const a = (Q / P) * t;
    const r = 2 + Math.cos(a);
    return target.set(r * Math.cos(t) * S, r * Math.sin(t) * S, Math.sin(a) * S * 1.15);
  }
}
const base = new KnotCurve();

/* ── parallel-transported frames, closed so the braid meets itself ── */
const SEG = 900;
const frames = base.computeFrenetFrames(SEG, true);
const _n = new THREE.Vector3(), _b = new THREE.Vector3();
function frameAt(u) {
  const x = ((u % 1) + 1) % 1 * SEG;
  const i = Math.floor(x), f = x - i, j = Math.min(i + 1, SEG);
  _n.copy(frames.normals[i]).lerp(frames.normals[j], f).normalize();
  _b.copy(frames.binormals[i]).lerp(frames.binormals[j], f).normalize();
}

const OFFSET = 0.105, TWISTS = 5, STRANDS = 4;
class StrandCurve extends THREE.Curve {
  constructor(phase) { super(); this.phase = phase; }
  getPoint(u, target = new THREE.Vector3()) {
    base.getPointAt(((u % 1) + 1) % 1, target);
    frameAt(u);
    const ang = Math.PI * 2 * (TWISTS * u + this.phase);
    return target
      .addScaledVector(_n, Math.cos(ang) * OFFSET)
      .addScaledVector(_b, Math.sin(ang) * OFFSET);
  }
}

const strands = [];
for (let k = 0; k < STRANDS; k++) {
  const curve = new StrandCurve(k / STRANDS);
  const mesh = new THREE.Mesh(new THREE.TubeGeometry(curve, 760, 0.041, 14, true), M.glass);
  mesh.name = `strand_${k + 1}`;
  spin.add(mesh);
  strands.push({ curve, mesh });
}

/* ── inner light thread running down the middle of the braid ── */
const core = new THREE.Mesh(new THREE.TubeGeometry(base, 700, 0.034, 12, true), M.core);
core.name = 'core_thread';
spin.add(core);

/* ── travelling pulses inside the strands ── */
const pulseGeo = new THREE.SphereGeometry(0.052, 18, 14);
const pulses = [];
for (let i = 0; i < 10; i++) {
  const m = new THREE.Mesh(pulseGeo, M.pulse);
  m.name = `pulse_${i + 1}`;
  spin.add(m);
  pulses.push({ mesh: m, strand: strands[i % STRANDS], u: i / 10, speed: 0.055 + (i % 3) * 0.018 });
}
const lampA = new THREE.PointLight(0x9fe4ff, 2.2, 2.4, 2); lampA.name = 'glow_light_a';
const lampB = new THREE.PointLight(0xbfe9ff, 1.6, 2.0, 2); lampB.name = 'glow_light_b';
spin.add(lampA, lampB);

obj.rotation.x = -0.18;
stage.setObject(obj);

/* ── animation ── */
function frame(dt, t) {
  spin.rotation.z += dt * 0.22;
  spin.rotation.y = Math.sin(t * 0.28) * 0.16;

  M.core.emissiveIntensity = 1.1 + Math.sin(t * 1.15) * 0.35;
  M.pulse.emissiveIntensity = 2.3 + Math.sin(t * 3.1) * 0.5;

  pulses.forEach((p, i) => {
    p.u = (p.u + dt * p.speed) % 1;
    p.strand.curve.getPoint(p.u, p.mesh.position);
    const s = 0.75 + Math.sin(t * 2.4 + i) * 0.22;
    p.mesh.scale.setScalar(s);
  });
  lampA.position.copy(pulses[0].mesh.position);
  lampB.position.copy(pulses[5].mesh.position);
}

/* Seat the pulses on their strands before the first render, so the object
   is complete even when it never advances a frame. */
frame(0, 0);

/* Honour prefers-reduced-motion, live: the knot holds a still pose and
   stays fully orbitable. The clock keeps running while held, so releasing
   it resumes the phase smoothly instead of jumping. */
const calm = window.matchMedia?.('(prefers-reduced-motion: reduce)');
let held = calm?.matches ?? false;
calm?.addEventListener?.('change', (e) => { held = e.matches; });

const clock = new THREE.Clock();
(function tick() {
  requestAnimationFrame(tick);
  const dt = Math.min(clock.getDelta(), 0.05), t = clock.elapsedTime;
  if (held) return;
  frame(dt, t);
})();
