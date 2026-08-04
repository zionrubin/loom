package main

// This file is the offline studio's answer key: the module implementations the
// scripted "coding model" returns when asked to implement each module of the
// game. Every other example in this repo scripts its mock responses the same
// way (see examples/research); here the scripted response happens to be
// working JavaScript, which is what makes the offline demo end in a game you
// can actually play — no key, no network, zero cost.
//
// Nothing downstream of the model knows the difference. The build run
// validates, lints, reviews, orders, links, and pays for these strings exactly
// as it would for tokens off a real provider, and `-provider anthropic|openai`
// sends the same prompts to a real model instead.
//
// Every module obeys the contract in shared.go: one IIFE, one namespace
// assignment, no imports, no network, no DOM outside the canvas it is handed.

// moduleSource maps a module ID to the implementation the studio returns.
var moduleSource = map[string]string{

	// --- vec: math, deterministic RNG, wrapping -----------------------------
	"vec": `(function (G) {
  var seed = 20260801 >>> 0;
  function rnd() {
    seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
    return seed / 4294967296;
  }
  G.vec = {
    TAU: Math.PI * 2,
    seed: function (n) { seed = n >>> 0; },
    rnd: rnd,
    rand: function (a, b) { return a + rnd() * (b - a); },
    randInt: function (a, b) { return Math.floor(a + rnd() * (b - a + 1)); },
    pick: function (xs) { return xs[Math.floor(rnd() * xs.length)]; },
    clamp: function (v, a, b) { return v < a ? a : (v > b ? b : v); },
    lerp: function (a, b, t) { return a + (b - a) * t; },
    dist2: function (ax, ay, bx, by) { var dx = ax - bx, dy = ay - by; return dx * dx + dy * dy; },
    hit: function (a, b) {
      var r = a.r + b.r;
      return G.vec.dist2(a.x, a.y, b.x, b.y) <= r * r;
    },
    wrap: function (o, w, h, pad) {
      pad = pad === undefined ? 24 : pad;
      if (o.x < -pad) o.x += w + pad * 2;
      else if (o.x > w + pad) o.x -= w + pad * 2;
      if (o.y < -pad) o.y += h + pad * 2;
      else if (o.y > h + pad) o.y -= h + pad * 2;
      return o;
    }
  };
})(window.LOOM);`,

	// --- input: keyboard state with per-frame edge detection ----------------
	"input": `(function (G) {
  var held = {}, edge = {}, swallow = ['Space', 'ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight'];
  G.input = {
    attach: function () {
      window.addEventListener('keydown', function (e) {
        var k = e.code || e.key;
        if (!held[k]) edge[k] = true;
        held[k] = true;
        if (swallow.indexOf(k) >= 0) e.preventDefault();
      });
      window.addEventListener('keyup', function (e) { held[e.code || e.key] = false; });
      window.addEventListener('blur', function () { held = {}; });
    },
    down: function () {
      for (var i = 0; i < arguments.length; i++) if (held[arguments[i]]) return true;
      return false;
    },
    hit: function () {
      for (var i = 0; i < arguments.length; i++) if (edge[arguments[i]]) return true;
      return false;
    },
    anyHit: function () { for (var k in edge) if (edge[k]) return true; return false; },
    endFrame: function () { edge = {}; }
  };
})(window.LOOM);`,

	// --- audio: synthesized blips, no asset files ---------------------------
	"audio": `(function (G) {
  var ac = null, master = null, muted = false;
  function ensure() {
    if (ac) return ac;
    var AC = window.AudioContext || window.webkitAudioContext;
    if (!AC) return null;
    ac = new AC();
    master = ac.createGain();
    master.gain.value = 0.15;
    master.connect(ac.destination);
    return ac;
  }
  function voice(o) {
    var c = ensure();
    if (!c || muted) return;
    var t0 = c.currentTime, osc = c.createOscillator(), g = c.createGain();
    osc.type = o.type || 'square';
    osc.frequency.setValueAtTime(o.from, t0);
    if (o.to) osc.frequency.exponentialRampToValueAtTime(Math.max(30, o.to), t0 + o.dur);
    g.gain.setValueAtTime(0.0001, t0);
    g.gain.exponentialRampToValueAtTime(o.vol || 0.3, t0 + 0.012);
    g.gain.exponentialRampToValueAtTime(0.0001, t0 + o.dur);
    osc.connect(g); g.connect(master);
    osc.start(t0); osc.stop(t0 + o.dur + 0.03);
  }
  function noise(dur, vol, cut) {
    var c = ensure();
    if (!c || muted) return;
    var n = Math.floor(c.sampleRate * dur), buf = c.createBuffer(1, n, c.sampleRate), d = buf.getChannelData(0);
    for (var i = 0; i < n; i++) d[i] = (Math.random() * 2 - 1) * (1 - i / n);
    var src = c.createBufferSource(); src.buffer = buf;
    var f = c.createBiquadFilter(); f.type = 'lowpass'; f.frequency.value = cut || 900;
    var g = c.createGain(); g.gain.value = vol || 0.4;
    src.connect(f); f.connect(g); g.connect(master);
    src.start();
  }
  G.audio = {
    unlock: function () { var c = ensure(); if (c && c.state === 'suspended') c.resume(); },
    muted: function () { return muted; },
    toggle: function () { muted = !muted; return !muted; },
    blip: function (kind) {
      if (kind === 'fire') voice({ from: 900, to: 240, dur: 0.07, vol: 0.18, type: 'square' });
      else if (kind === 'boom') noise(0.34, 0.5, 780);
      else if (kind === 'mote') voice({ from: 720, to: 1440, dur: 0.08, vol: 0.14, type: 'triangle' });
      else if (kind === 'pulse') voice({ from: 130, to: 980, dur: 0.55, vol: 0.3, type: 'sawtooth' });
      else if (kind === 'hurt') voice({ from: 320, to: 55, dur: 0.45, vol: 0.32, type: 'sawtooth' });
      else if (kind === 'wave') voice({ from: 420, to: 1680, dur: 0.3, vol: 0.2, type: 'triangle' });
      else if (kind === 'start') voice({ from: 220, to: 880, dur: 0.28, vol: 0.24, type: 'triangle' });
    }
  };
})(window.LOOM);`,

	// --- starfield: three parallax layers -----------------------------------
	"starfield": `(function (G) {
  var layers = [];
  G.starfield = {
    init: function (w) {
      var V = G.vec, specs = [
        { n: 170, sp: 5, r: 1.0, a: 0.42 },
        { n: 90, sp: 13, r: 1.5, a: 0.62 },
        { n: 38, sp: 26, r: 2.1, a: 0.95 }
      ];
      layers = [];
      for (var i = 0; i < specs.length; i++) {
        var s = specs[i], stars = [];
        for (var j = 0; j < s.n; j++) {
          stars.push({ x: V.rand(0, w.w || 800), y: V.rand(0, w.h || 600), r: s.r * V.rand(0.6, 1.5), tw: V.rand(0, V.TAU) });
        }
        layers.push({ speed: s.sp, alpha: s.a, stars: stars });
      }
    },
    update: function (w) {
      for (var i = 0; i < layers.length; i++) {
        var L = layers[i];
        for (var j = 0; j < L.stars.length; j++) {
          var s = L.stars[j];
          s.y += L.speed * w.dt;
          s.tw += w.dt * 2.2;
          if (s.y > w.h + 4) { s.y = -4; s.x = G.vec.rand(0, w.w); }
        }
      }
    },
    draw: function (ctx, w) {
      for (var i = 0; i < layers.length; i++) {
        var L = layers[i];
        ctx.fillStyle = i === 2 ? '#e8f2ff' : '#a9c4ee';
        for (var j = 0; j < L.stars.length; j++) {
          var s = L.stars[j];
          ctx.globalAlpha = L.alpha * (0.6 + 0.4 * Math.sin(s.tw));
          ctx.fillRect(s.x, s.y, s.r, s.r);
        }
      }
      ctx.globalAlpha = 1;
    }
  };
})(window.LOOM);`,

	// --- particles: sparks for everything that explodes ---------------------
	"particles": `(function (G) {
  var ps = [];
  G.particles = {
    reset: function () { ps = []; },
    count: function () { return ps.length; },
    burst: function (w, x, y, opt) {
      opt = opt || {};
      var V = G.vec, n = opt.n || 14;
      if (ps.length > 900) return;
      for (var i = 0; i < n; i++) {
        var a = V.rand(0, V.TAU), sp = V.rand(opt.min || 40, opt.max || 260);
        ps.push({
          x: x, y: y,
          vx: Math.cos(a) * sp + (opt.vx || 0),
          vy: Math.sin(a) * sp + (opt.vy || 0),
          life: 0, max: V.rand(0.2, opt.life || 0.85),
          c: opt.color || '#ffd479', s: opt.size || 2.2
        });
      }
    },
    update: function (w) {
      for (var i = ps.length - 1; i >= 0; i--) {
        var p = ps[i];
        p.life += w.dt;
        if (p.life >= p.max) { ps.splice(i, 1); continue; }
        p.x += p.vx * w.dt;
        p.y += p.vy * w.dt;
        var d = Math.pow(0.22, w.dt);
        p.vx *= d; p.vy *= d;
      }
    },
    draw: function (ctx, w) {
      ctx.globalCompositeOperation = 'lighter';
      for (var i = 0; i < ps.length; i++) {
        var p = ps[i], k = 1 - p.life / p.max, s = p.s * k + 0.5;
        ctx.globalAlpha = k;
        ctx.fillStyle = p.c;
        ctx.fillRect(p.x - s / 2, p.y - s / 2, s, s);
      }
      ctx.globalAlpha = 1;
      ctx.globalCompositeOperation = 'source-over';
    }
  };
})(window.LOOM);`,

	// --- bullets: the threads the shuttle fires -----------------------------
	"bullets": `(function (G) {
  var bs = [];
  G.bullets = {
    reset: function () { bs = []; },
    list: function () { return bs; },
    fire: function (w, x, y, a, speed) {
      bs.push({ x: x, y: y, vx: Math.cos(a) * speed, vy: Math.sin(a) * speed, r: 3, life: 0, max: 1.05 });
    },
    update: function (w) {
      for (var i = bs.length - 1; i >= 0; i--) {
        var b = bs[i];
        b.life += w.dt;
        if (b.dead || b.life >= b.max) { bs.splice(i, 1); continue; }
        b.x += b.vx * w.dt;
        b.y += b.vy * w.dt;
        G.vec.wrap(b, w.w, w.h, 6);
      }
    },
    draw: function (ctx, w) {
      ctx.globalCompositeOperation = 'lighter';
      ctx.strokeStyle = '#ffd479';
      ctx.lineWidth = 2.2;
      ctx.lineCap = 'round';
      for (var i = 0; i < bs.length; i++) {
        var b = bs[i];
        ctx.globalAlpha = 1 - (b.life / b.max) * 0.65;
        ctx.beginPath();
        ctx.moveTo(b.x, b.y);
        ctx.lineTo(b.x - b.vx * 0.022, b.y - b.vy * 0.022);
        ctx.stroke();
      }
      ctx.globalAlpha = 1;
      ctx.globalCompositeOperation = 'source-over';
    }
  };
})(window.LOOM);`,

	// --- shards: the drifting rocks, split on hit ---------------------------
	"shards": `(function (G) {
  var list = [], RADIUS = [0, 13, 24, 40];
  function make(x, y, size) {
    var V = G.vec, verts = [], n = 7 + size * 2;
    for (var i = 0; i < n; i++) verts.push(V.rand(0.72, 1.25));
    var a = V.rand(0, V.TAU), sp = V.rand(16, 42) + (3 - size) * 26;
    return {
      x: x, y: y, vx: Math.cos(a) * sp, vy: Math.sin(a) * sp,
      r: RADIUS[size], size: size, rot: V.rand(0, V.TAU),
      spin: V.rand(-1.2, 1.2), verts: verts, flash: 0
    };
  }
  G.shards = {
    reset: function () { list = []; },
    list: function () { return list; },
    count: function () { return list.length; },
    spawnWave: function (w, n) {
      var V = G.vec;
      for (var i = 0; i < n; i++) {
        var edge = V.randInt(0, 3), x, y;
        if (edge === 0) { x = V.rand(0, w.w); y = -34; }
        else if (edge === 1) { x = w.w + 34; y = V.rand(0, w.h); }
        else if (edge === 2) { x = V.rand(0, w.w); y = w.h + 34; }
        else { x = -34; y = V.rand(0, w.h); }
        list.push(make(x, y, 3));
      }
    },
    split: function (w, s) {
      var i = list.indexOf(s);
      if (i >= 0) list.splice(i, 1);
      if (s.size <= 1) return 0;
      for (var k = 0; k < 2; k++) {
        list.push(make(s.x + G.vec.rand(-7, 7), s.y + G.vec.rand(-7, 7), s.size - 1));
      }
      return 2;
    },
    clear: function () { var n = list.length; list = []; return n; },
    update: function (w) {
      for (var i = 0; i < list.length; i++) {
        var s = list[i];
        s.x += s.vx * w.dt;
        s.y += s.vy * w.dt;
        s.rot += s.spin * w.dt;
        if (s.flash > 0) s.flash -= w.dt * 5;
        G.vec.wrap(s, w.w, w.h, 46);
      }
    },
    draw: function (ctx, w) {
      var V = G.vec;
      for (var i = 0; i < list.length; i++) {
        var s = list[i];
        ctx.save();
        ctx.translate(s.x, s.y);
        ctx.rotate(s.rot);
        ctx.beginPath();
        for (var j = 0; j < s.verts.length; j++) {
          var a = (j / s.verts.length) * V.TAU, rr = s.r * s.verts[j];
          var px = Math.cos(a) * rr, py = Math.sin(a) * rr;
          if (j === 0) ctx.moveTo(px, py); else ctx.lineTo(px, py);
        }
        ctx.closePath();
        ctx.fillStyle = 'rgba(18,22,50,0.78)';
        ctx.fill();
        ctx.lineWidth = 1.7;
        ctx.strokeStyle = s.flash > 0 ? '#ffffff' : (s.size === 3 ? '#8b9bff' : (s.size === 2 ? '#a98bff' : '#c58bff'));
        ctx.shadowColor = ctx.strokeStyle;
        ctx.shadowBlur = 14;
        ctx.stroke();
        ctx.restore();
      }
      ctx.shadowBlur = 0;
    }
  };
})(window.LOOM);`,

	// --- motes: the sparks that charge the weave pulse ----------------------
	"motes": `(function (G) {
  var list = [];
  G.motes = {
    reset: function () { list = []; },
    list: function () { return list; },
    drop: function (w, x, y, n) {
      var V = G.vec;
      for (var i = 0; i < n; i++) {
        var a = V.rand(0, V.TAU), sp = V.rand(25, 95);
        list.push({ x: x, y: y, vx: Math.cos(a) * sp, vy: Math.sin(a) * sp, r: 5, life: 0, max: 9, ph: V.rand(0, V.TAU) });
      }
    },
    update: function (w) {
      var sh = w.ship;
      for (var i = list.length - 1; i >= 0; i--) {
        var m = list[i];
        m.life += w.dt;
        if (m.life >= m.max) { list.splice(i, 1); continue; }
        m.ph += w.dt * 6;
        if (sh && sh.alive) {
          var dx = sh.x - m.x, dy = sh.y - m.y, d2 = dx * dx + dy * dy;
          if (d2 < 30000) {
            var d = Math.sqrt(d2) || 1;
            m.vx += (dx / d) * 460 * w.dt;
            m.vy += (dy / d) * 460 * w.dt;
          }
        }
        m.x += m.vx * w.dt;
        m.y += m.vy * w.dt;
        var damp = Math.pow(0.35, w.dt);
        m.vx *= damp; m.vy *= damp;
        G.vec.wrap(m, w.w, w.h, 10);
      }
    },
    draw: function (ctx, w) {
      ctx.globalCompositeOperation = 'lighter';
      ctx.fillStyle = '#5cffb1';
      for (var i = 0; i < list.length; i++) {
        var m = list[i];
        var fade = m.life > m.max - 2 ? (m.max - m.life) / 2 : 1;
        var r = m.r * (0.75 + 0.25 * Math.sin(m.ph));
        ctx.globalAlpha = 0.95 * fade;
        ctx.beginPath(); ctx.arc(m.x, m.y, r, 0, G.vec.TAU); ctx.fill();
        ctx.globalAlpha = 0.22 * fade;
        ctx.beginPath(); ctx.arc(m.x, m.y, r * 2.8, 0, G.vec.TAU); ctx.fill();
      }
      ctx.globalAlpha = 1;
      ctx.globalCompositeOperation = 'source-over';
    }
  };
})(window.LOOM);`,

	// --- ship: the player shuttle ------------------------------------------
	"ship": `(function (G) {
  G.ship = {
    spawn: function (w) {
      return {
        x: w.w / 2, y: w.h / 2, vx: 0, vy: 0, a: -Math.PI / 2, r: 11,
        cool: 0, thrust: 0, invuln: 2.4, alive: true
      };
    },
    update: function (w) {
      var s = w.ship, V = G.vec, I = G.input;
      if (!s || !s.alive) return;
      if (s.invuln > 0) s.invuln -= w.dt;

      if (I.down('ArrowLeft', 'KeyA')) s.a -= 3.6 * w.dt;
      if (I.down('ArrowRight', 'KeyD')) s.a += 3.6 * w.dt;

      s.thrust = I.down('ArrowUp', 'KeyW') ? 1 : 0;
      if (s.thrust) {
        s.vx += Math.cos(s.a) * 540 * w.dt;
        s.vy += Math.sin(s.a) * 540 * w.dt;
        G.particles.burst(w, s.x - Math.cos(s.a) * 13, s.y - Math.sin(s.a) * 13, {
          n: 2, min: 20, max: 110, life: 0.32, size: 2, color: '#7ce7ff',
          vx: -Math.cos(s.a) * 110, vy: -Math.sin(s.a) * 110
        });
      }
      var drag = Math.pow(0.62, w.dt);
      s.vx *= drag; s.vy *= drag;
      var sp2 = s.vx * s.vx + s.vy * s.vy, max = 440;
      if (sp2 > max * max) { var f = max / Math.sqrt(sp2); s.vx *= f; s.vy *= f; }
      s.x += s.vx * w.dt;
      s.y += s.vy * w.dt;
      V.wrap(s, w.w, w.h, 16);

      s.cool -= w.dt;
      if (I.down('Space') && s.cool <= 0) {
        s.cool = 0.16;
        G.bullets.fire(w, s.x + Math.cos(s.a) * 15, s.y + Math.sin(s.a) * 15, s.a, 580);
        s.vx -= Math.cos(s.a) * 24;
        s.vy -= Math.sin(s.a) * 24;
        G.audio.blip('fire');
      }

      if (I.hit('ShiftLeft', 'ShiftRight', 'KeyZ') && w.charge >= 1) {
        var rocks = G.shards.list();
        for (var i = 0; i < rocks.length; i++) {
          G.particles.burst(w, rocks[i].x, rocks[i].y, { n: 16, color: '#c58bff', max: 300 });
          w.score += 20 * rocks[i].size;
        }
        w.pulses++;
        G.shards.clear();
        G.particles.burst(w, s.x, s.y, { n: 80, color: '#7ce7ff', min: 140, max: 640, life: 1.0, size: 3 });
        G.audio.blip('pulse');
        w.charge = 0;
        w.flash = 1;
        w.shake = Math.max(w.shake, 15);
      }
    },
    draw: function (ctx, w) {
      var s = w.ship;
      if (!s || !s.alive) return;
      var blink = s.invuln > 0 && Math.floor(s.invuln * 14) % 2 === 0;
      ctx.save();
      ctx.translate(s.x, s.y);
      ctx.rotate(s.a);
      if (s.thrust) {
        ctx.globalCompositeOperation = 'lighter';
        ctx.beginPath();
        ctx.moveTo(-10, -5.5);
        ctx.lineTo(-10 - (11 + Math.random() * 12), 0);
        ctx.lineTo(-10, 5.5);
        ctx.closePath();
        ctx.fillStyle = 'rgba(124,231,255,0.7)';
        ctx.fill();
        ctx.globalCompositeOperation = 'source-over';
      }
      ctx.globalAlpha = blink ? 0.35 : 1;
      ctx.beginPath();
      ctx.moveTo(17, 0);
      ctx.lineTo(-11, 9.5);
      ctx.lineTo(-6, 0);
      ctx.lineTo(-11, -9.5);
      ctx.closePath();
      ctx.fillStyle = 'rgba(8,16,38,0.9)';
      ctx.fill();
      ctx.lineWidth = 2;
      ctx.strokeStyle = '#7ce7ff';
      ctx.shadowColor = '#7ce7ff';
      ctx.shadowBlur = 16;
      ctx.stroke();
      ctx.restore();
      ctx.shadowBlur = 0;
      ctx.globalAlpha = 1;
    }
  };
})(window.LOOM);`,

	// --- collide: every interaction between two things ----------------------
	"collide": `(function (G) {
  G.collide = {
    resolve: function (w) {
      var V = G.vec, bs = G.bullets.list(), ss = G.shards.list(), sh = w.ship;

      for (var i = bs.length - 1; i >= 0; i--) {
        var b = bs[i];
        for (var j = ss.length - 1; j >= 0; j--) {
          var s = ss[j];
          if (!V.hit(b, s)) continue;
          b.dead = true;
          s.flash = 1;
          w.score += (4 - s.size) * 25;
          w.broken++;
          G.particles.burst(w, b.x, b.y, { n: 10 + s.size * 6, color: '#ffd479', max: 260 });
          G.motes.drop(w, s.x, s.y, s.size);
          G.shards.split(w, s);
          G.audio.blip('boom');
          w.shake = Math.min(15, w.shake + 3 + s.size);
          break;
        }
      }

      if (!sh || !sh.alive) return;

      var ms = G.motes.list();
      for (var k = ms.length - 1; k >= 0; k--) {
        var m = ms[k], reach = sh.r + m.r + 6;
        if (V.dist2(m.x, m.y, sh.x, sh.y) > reach * reach) continue;
        ms.splice(k, 1);
        w.score += 15;
        w.collected++;
        w.charge = Math.min(1, w.charge + 0.07);
        G.particles.burst(w, m.x, m.y, { n: 7, color: '#5cffb1', max: 140, life: 0.4 });
        G.audio.blip('mote');
      }

      if (sh.invuln > 0) return;
      for (var q = 0; q < ss.length; q++) {
        if (!V.hit(sh, ss[q])) continue;
        w.onShipHit(ss[q]);
        return;
      }
    }
  };
})(window.LOOM);`,

	// --- hud: score, charge, overlays, provenance ---------------------------
	"hud": `(function (G) {
  var MONO = 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace';
  function text(ctx, s, x, y, size, color, align, weight) {
    ctx.font = (weight || 600) + ' ' + size + 'px ' + MONO;
    ctx.fillStyle = color;
    ctx.textAlign = align || 'left';
    ctx.textBaseline = 'alphabetic';
    ctx.fillText(s, x, y);
  }
  function panel(ctx, x, y, w, h) {
    ctx.fillStyle = 'rgba(6,9,22,0.82)';
    ctx.strokeStyle = 'rgba(124,231,255,0.25)';
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.rect(x, y, w, h);
    ctx.fill();
    ctx.stroke();
  }
  function money(n) { return '$' + (n || 0).toFixed(4); }

  G.hud = {
    draw: function (ctx, w) {
      text(ctx, 'SCORE ' + String(w.score).padStart(6, '0'), 22, 34, 15, '#dfe7ff');
      text(ctx, 'WAVE ' + w.wave, 22, 56, 12, '#8b9bff');
      text(ctx, 'BEST ' + w.best, 22, 74, 11, 'rgba(223,231,255,0.45)');

      for (var i = 0; i < w.lives; i++) {
        var x = w.w - 26 - i * 22, y = 32;
        ctx.save();
        ctx.translate(x, y);
        ctx.rotate(-Math.PI / 2);
        ctx.beginPath();
        ctx.moveTo(9, 0); ctx.lineTo(-6, 5.5); ctx.lineTo(-3, 0); ctx.lineTo(-6, -5.5);
        ctx.closePath();
        ctx.strokeStyle = '#7ce7ff';
        ctx.lineWidth = 1.6;
        ctx.stroke();
        ctx.restore();
      }

      var bw = 150, bx = w.w - bw - 22, by = 52;
      ctx.fillStyle = 'rgba(255,255,255,0.08)';
      ctx.fillRect(bx, by, bw, 7);
      ctx.fillStyle = w.charge >= 1 ? '#5cffb1' : '#7ce7ff';
      ctx.globalAlpha = w.charge >= 1 ? 0.7 + 0.3 * Math.sin(w.t * 9) : 1;
      ctx.fillRect(bx, by, bw * w.charge, 7);
      ctx.globalAlpha = 1;
      text(ctx, w.charge >= 1 ? 'WEAVE PULSE READY [Z]' : 'PULSE CHARGE', bx + bw, by + 22, 10,
        w.charge >= 1 ? '#5cffb1' : 'rgba(223,231,255,0.5)', 'right');

      var m = w.manifest || {};
      text(ctx, 'forged by loom · ' + (m.modules ? m.modules.length : 0) + ' modules · ' +
        (m.tasks || 0) + ' tasks · ' + (m.calls || 0) + ' model calls · ' + money(m.cost),
        22, w.h - 18, 10, 'rgba(139,155,255,0.55)');
      text(ctx, '[P] provenance   [M] sound', w.w - 22, w.h - 18, 10, 'rgba(139,155,255,0.45)', 'right');
    },

    overlay: function (ctx, w) {
      var card = w.card || {};
      if (w.showProvenance) { G.hud.provenance(ctx, w); return; }
      if (w.state === 'playing') return;

      ctx.fillStyle = 'rgba(4,6,16,0.72)';
      ctx.fillRect(0, 0, w.w, w.h);
      var cx = w.w / 2, cy = w.h / 2;

      if (w.state === 'title') {
        text(ctx, card.title || 'CONSTELLATION DRIFT', cx, cy - 78, 42, '#7ce7ff', 'center', 700);
        text(ctx, card.tagline || 'weave the dark', cx, cy - 44, 14, '#c58bff', 'center');
        var how = card.howto || [];
        for (var i = 0; i < how.length; i++) {
          text(ctx, how[i], cx, cy + 6 + i * 22, 13, 'rgba(223,231,255,0.8)', 'center', 400);
        }
        text(ctx, 'PRESS ENTER TO LAUNCH', cx, cy + 40 + how.length * 22, 15,
          Math.sin(w.t * 4) > 0 ? '#ffd479' : 'rgba(255,212,121,0.45)', 'center');
      } else {
        text(ctx, 'RUN COMPLETE', cx, cy - 62, 34, '#ff5c7a', 'center', 700);
        text(ctx, 'SCORE ' + w.score, cx, cy - 20, 20, '#dfe7ff', 'center');
        text(ctx, 'WAVE ' + w.wave + ' · ' + w.broken + ' shards cut · ' + w.collected + ' motes woven',
          cx, cy + 8, 12, 'rgba(223,231,255,0.6)', 'center', 400);
        if (w.score >= w.best) text(ctx, 'NEW BEST', cx, cy + 32, 13, '#5cffb1', 'center');
        text(ctx, 'PRESS ENTER TO RE-RUN', cx, cy + 66, 15,
          Math.sin(w.t * 4) > 0 ? '#ffd479' : 'rgba(255,212,121,0.45)', 'center');
      }
    },

    provenance: function (ctx, w) {
      var m = w.manifest || {}, mods = m.modules || [];
      ctx.fillStyle = 'rgba(4,6,16,0.92)';
      ctx.fillRect(0, 0, w.w, w.h);
      var pw = Math.min(760, w.w - 48), px = (w.w - pw) / 2, py = 46;
      var ph = Math.min(w.h - 92, 150 + mods.length * 20);
      panel(ctx, px, py, pw, ph);

      text(ctx, 'HOW THIS GAME WAS BUILT', px + 20, py + 32, 16, '#7ce7ff', 'left', 700);
      text(ctx, (m.runs || []).length + ' loom runs · ' + (m.tasks || 0) + ' tasks · ' +
        (m.calls || 0) + ' model calls · ' + (m.tokens || 0) + ' tokens · ' + money(m.cost),
        px + 20, py + 54, 11, 'rgba(223,231,255,0.6)', 'left', 400);

      var y = py + 84;
      text(ctx, 'MODULE', px + 20, y, 10, 'rgba(139,155,255,0.7)');
      text(ctx, 'WRITTEN BY', px + 150, y, 10, 'rgba(139,155,255,0.7)');
      text(ctx, 'BYTES', px + pw - 200, y, 10, 'rgba(139,155,255,0.7)', 'right');
      text(ctx, 'TOKENS', px + pw - 120, y, 10, 'rgba(139,155,255,0.7)', 'right');
      text(ctx, 'COST', px + pw - 20, y, 10, 'rgba(139,155,255,0.7)', 'right');
      y += 8;
      ctx.strokeStyle = 'rgba(124,231,255,0.15)';
      ctx.beginPath(); ctx.moveTo(px + 20, y); ctx.lineTo(px + pw - 20, y); ctx.stroke();
      y += 16;

      for (var i = 0; i < mods.length && y < py + ph - 34; i++) {
        var d = mods[i];
        text(ctx, d.id, px + 20, y, 11, d.escalated ? '#ffd479' : '#dfe7ff', 'left', 400);
        text(ctx, d.model + (d.escalated ? ' ↑' : ''), px + 150, y, 11, 'rgba(197,139,255,0.9)', 'left', 400);
        text(ctx, String(d.bytes), px + pw - 200, y, 11, 'rgba(223,231,255,0.55)', 'right', 400);
        text(ctx, String(d.tokens), px + pw - 120, y, 11, 'rgba(223,231,255,0.55)', 'right', 400);
        text(ctx, money(d.cost), px + pw - 20, y, 11, 'rgba(92,255,177,0.8)', 'right', 400);
        y += 20;
      }
      text(ctx, '[P] back to the game', px + 20, py + ph - 14, 10, 'rgba(139,155,255,0.55)');
    }
  };
})(window.LOOM);`,

	// --- game: world state, the loop, the wiring ----------------------------
	"game": `(function (G) {
  G.game = {
    boot: function (canvas, manifest, card) {
      var ctx = canvas.getContext('2d');
      var w = {
        w: 0, h: 0, dt: 0, t: 0, state: 'title',
        score: 0, best: 0, wave: 0, lives: 3, charge: 0,
        broken: 0, collected: 0, pulses: 0,
        shake: 0, flash: 0, showProvenance: false,
        ship: null, manifest: manifest || {}, card: card || {}
      };

      function resize() {
        var dpr = Math.min(window.devicePixelRatio || 1, 2);
        w.w = canvas.clientWidth || window.innerWidth;
        w.h = canvas.clientHeight || window.innerHeight;
        canvas.width = Math.floor(w.w * dpr);
        canvas.height = Math.floor(w.h * dpr);
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      }
      window.addEventListener('resize', resize);
      resize();
      G.input.attach();
      G.starfield.init(w);

      function nextWave() {
        w.wave++;
        // Scale the field with the window so a wide screen is not empty.
        var room = Math.round((w.w * w.h) / 380000);
        G.shards.spawnWave(w, 3 + w.wave + Math.max(0, Math.min(4, room)));
        G.audio.blip('wave');
      }

      function start() {
        w.score = 0; w.wave = 0; w.lives = 3; w.charge = 0;
        w.broken = 0; w.collected = 0; w.pulses = 0;
        w.shake = 0; w.flash = 0;
        G.shards.reset(); G.bullets.reset(); G.motes.reset(); G.particles.reset();
        w.ship = G.ship.spawn(w);
        w.state = 'playing';
        G.audio.unlock();
        G.audio.blip('start');
        nextWave();
      }

      w.onShipHit = function (shard) {
        var s = w.ship;
        G.particles.burst(w, s.x, s.y, { n: 46, color: '#ff5c7a', min: 60, max: 420, life: 1.0, size: 3 });
        G.particles.burst(w, s.x, s.y, { n: 22, color: '#7ce7ff', min: 40, max: 300, life: 0.8 });
        G.shards.split(w, shard);
        G.audio.blip('hurt');
        w.shake = 24;
        w.lives--;
        if (w.lives <= 0) {
          s.alive = false;
          w.state = 'over';
          if (w.score > w.best) w.best = w.score;
        } else {
          w.ship = G.ship.spawn(w);
        }
      };

      function update() {
        if (G.input.hit('KeyP')) w.showProvenance = !w.showProvenance;
        if (G.input.hit('KeyM')) { G.audio.unlock(); G.audio.toggle(); }
        if (w.state !== 'playing' && G.input.hit('Enter', 'NumpadEnter', 'Space')) start();

        G.starfield.update(w);
        G.particles.update(w);
        if (w.state !== 'playing') return;

        G.ship.update(w);
        G.bullets.update(w);
        G.shards.update(w);
        G.motes.update(w);
        G.collide.resolve(w);

        if (G.shards.count() === 0) nextWave();
        if (w.shake > 0) w.shake = Math.max(0, w.shake - w.dt * 34);
        if (w.flash > 0) w.flash = Math.max(0, w.flash - w.dt * 2.2);
      }

      function render() {
        ctx.save();
        ctx.fillStyle = '#05060c';
        ctx.fillRect(0, 0, w.w, w.h);
        if (w.shake > 0.2) {
          ctx.translate((Math.random() - 0.5) * w.shake, (Math.random() - 0.5) * w.shake);
        }
        G.starfield.draw(ctx, w);
        G.motes.draw(ctx, w);
        G.shards.draw(ctx, w);
        G.bullets.draw(ctx, w);
        G.ship.draw(ctx, w);
        G.particles.draw(ctx, w);
        if (w.flash > 0) {
          ctx.fillStyle = 'rgba(124,231,255,' + (w.flash * 0.25).toFixed(3) + ')';
          ctx.fillRect(-40, -40, w.w + 80, w.h + 80);
        }
        ctx.restore();
        G.hud.draw(ctx, w);
        G.hud.overlay(ctx, w);
      }

      var last = 0;
      function frame(now) {
        if (!last) last = now;
        w.dt = Math.min(0.05, (now - last) / 1000);
        last = now;
        w.t += w.dt;
        update();
        render();
        G.input.endFrame();
        window.requestAnimationFrame(frame);
      }
      window.requestAnimationFrame(frame);

      G.game.world = function () { return w; };
      G.game.start = start;
      return w;
    }
  };
})(window.LOOM);`,
}
