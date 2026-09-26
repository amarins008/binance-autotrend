/* =============================================================================
 * NeuralBrain3D — WebGL brain for "Hermes Trading Lab · Neural Core"
 *
 * STABILITY / PERFORMANCE MODE (2026-09-26):
 *   - Persistent scene: canvas + WebGL context + scene graph live in a
 *     module-owned wrapper that is MOVED between mount points, so dashboard
 *     innerHTML re-renders never rebuild the scene (0 GPU churn per poll).
 *   - On-demand rendering: frames capped at 30fps, skipped while the brain is
 *     out of view (IntersectionObserver) or the tab is hidden.
 *   - Cheap quality: antialias off, pixelRatio capped at 1.75 (glow hides it).
 *   - Context-loss safe: webglcontextlost -> preventDefault + sleep(), then
 *     the registered onContextLost callback lets the dashboard fall back to
 *     the legacy 2D SVG brain.
 *   - sleep() fully frees GPU memory when the operator picks Lite/2D mode.
 *
 * Public API:
 *   NeuralBrain3D.available()          -> bool (THREE + WebGL context present)
 *   NeuralBrain3D.mountInto(container) -> idempotent mount (returns bool)
 *   NeuralBrain3D.update({neurons, synapses}) -> refresh states/labels
 *   NeuralBrain3D.sleep()              -> stop loop + free GPU resources
 *   NeuralBrain3D.isAwake()            -> bool
 *   NeuralBrain3D.onContextLost(cb)    -> register fallback hook
 *   NeuralBrain3D._stats()             -> {rebuilds, frames, inView}
 * ========================================================================== */
(function () {
  "use strict";

  var _wrap = null;          /* persistent host element (moved between mounts) */
  var _labelLayer = null;
  var _canvas = null;
  var _renderer = null;
  var _scene = null;
  var _camera = null;
  var _brainGroup = null;
  var _particles = null;
  var _raf = 0;
  var _neurons = [];
  var _synapses = [];
  var _nodeMap = {};
  var _curveMap = {};
  var _ro = null;
  var _inView = true;
  var _lastVisCheck = 0;
  var _bound = false;
  var _lostCb = null;
  var _pointer = { down: false, x: 0, y: 0, vx: 0, vy: 0 };
  var _rotY = -0.5;
  var _rotX = 0.22;
  var _autoSpin = true;
  var _zoom = 1.0;
  var _lastFrame = 0;
  var FPS_CAP = 30;
  var _stat = { rebuilds: 0, frames: 0 };

  var STATE_COLORS = {
    active: { main: 0x22d3ee, halo: 0x0891b2, glow: 1.9, pulse: 2.2 },
    blocked: { main: 0xfb7185, halo: 0x9f1239, glow: 2.1, pulse: 3.4 },
    done: { main: 0x2dd4bf, halo: 0x0f766e, glow: 1.0, pulse: 0.9 },
    idle: { main: 0x64748b, halo: 0x334155, glow: 0.45, pulse: 0.35 },
    todo: { main: 0x475f7f, halo: 0x1e293b, glow: 0.35, pulse: 0.25 },
  };

  var _glowTex = null;
  function glowTexture() {
    if (_glowTex) return _glowTex;
    var c = document.createElement("canvas"); c.width = c.height = 128;
    var g = c.getContext("2d");
    var grd = g.createRadialGradient(64, 64, 0, 64, 64, 64);
    grd.addColorStop(0.0, "rgba(255,255,255,1)");
    grd.addColorStop(0.32, "rgba(255,255,255,0.5)");
    grd.addColorStop(0.65, "rgba(255,255,255,0.16)");
    grd.addColorStop(1.0, "rgba(255,255,255,0)");
    g.fillStyle = grd; g.fillRect(0, 0, 128, 128);
    _glowTex = new THREE.CanvasTexture(c);
    return _glowTex;
  }

  function webglOk() {
    try {
      var c = document.createElement("canvas");
      return !!(window.WebGLRenderingContext &&
        (c.getContext("webgl") || c.getContext("experimental-webgl")));
    } catch (e) { return false; }
  }

  function available() {
    return typeof window.THREE !== "undefined" && webglOk();
  }

  function layout3d(xPct, yPct, i) {
    var x = (xPct - 50) / 50 * 26;
    var y = (50 - yPct) / 50 * 17 + 1.5;
    var bulge = Math.cos((xPct - 50) / 50 * Math.PI / 2);
    var z = (bulge - 0.45) * 2 * 12 + Math.sin(i * 2.399) * 5.2;
    return { x: x, y: y, z: z };
  }

  function ensureScene() {
    if (_scene) return;
    _stat.rebuilds += 1;

    _canvas = _canvas || document.createElement("canvas");
    _renderer = new THREE.WebGLRenderer({ canvas: _canvas, antialias: false, alpha: true });
    _renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 1.75));
    _scene = new THREE.Scene();
    _brainGroup = new THREE.Group();
    _scene.add(_brainGroup);

    _camera = new THREE.PerspectiveCamera(46, 1, 0.1, 400);
    _camera.position.set(0, 8, 86);

    _scene.add(new THREE.AmbientLight(0x88aaff, 0.55));
    var key = new THREE.PointLight(0x7dd3fc, 1.15, 300); key.position.set(40, 42, 60);
    var rim = new THREE.PointLight(0xf43f5e, 0.5, 260); rim.position.set(-55, -20, -40);
    _scene.add(key, rim);

    var shellMat = new THREE.MeshBasicMaterial({
      color: 0x1e3a5f, wireframe: true, transparent: true, opacity: 0.10,
    });
    var hemiGeo = new THREE.SphereGeometry(34, 26, 20, 0, Math.PI * 2, 0, Math.PI);
    var l = new THREE.Mesh(hemiGeo, shellMat); l.scale.set(1.0, 0.72, 0.62); l.position.x = -9;
    var r = new THREE.Mesh(hemiGeo, shellMat); r.scale.set(1.0, 0.72, 0.62); r.position.x = 9;
    _brainGroup.add(l, r);

    var COUNT = 420;
    var pos = new Float32Array(COUNT * 3);
    for (var i = 0; i < COUNT; i++) {
      var u = Math.random() * 2 - 1, th = Math.random() * Math.PI * 2, rr = Math.pow(Math.random(), 0.55);
      pos[i * 3] = Math.sqrt(1 - u * u) * Math.cos(th) * 30 * rr;
      pos[i * 3 + 1] = u * 20 * rr + 1.5;
      pos[i * 3 + 2] = Math.sqrt(1 - u * u) * Math.sin(th) * 17 * rr;
    }
    var pGeo = new THREE.BufferGeometry();
    pGeo.setAttribute("position", new THREE.BufferAttribute(pos, 3));
    _particles = new THREE.Points(pGeo, new THREE.PointsMaterial({
      color: 0x67e8f9, size: 0.55, transparent: true, opacity: 0.5,
      blending: THREE.AdditiveBlending, depthWrite: false,
    }));
    _brainGroup.add(_particles);

    _labelLayer = document.createElement("div");
    _labelLayer.className = "nb3d-labels";
    _wrap.appendChild(_canvas);
    _wrap.appendChild(_labelLayer);

    if (!_bound) {
      _wrap.addEventListener("pointerdown", onDown);
      _wrap.addEventListener("pointermove", onMove);
      window.addEventListener("pointerup", onUp);
      _wrap.addEventListener("wheel", onWheel, { passive: false });
      _wrap.addEventListener("pointerleave", onUp);
      _canvas.addEventListener("webglcontextlost", onContextLost, false);
      _bound = true;
    }

    if (_ro) _ro.disconnect();
    _ro = new ResizeObserver(function () { resize(); });
    _ro.observe(_wrap);
    resize();

    _lastFrame = 0;
    if (!_raf) _raf = requestAnimationFrame(tick);
  }

  function onContextLost(e) {
    e.preventDefault();
    sleep(); /* frees scene + stops loop; canvas element kept */
    if (typeof _lostCb === "function") { try { _lostCb(); } catch (err) {} }
  }

  function onDown(e) { _pointer.down = true; _pointer.x = e.clientX; _pointer.y = e.clientY; _autoSpin = false; }
  function onMove(e) {
    if (!_pointer.down) return;
    var dx = e.clientX - _pointer.x, dy = e.clientY - _pointer.y;
    _pointer.x = e.clientX; _pointer.y = e.clientY;
    _rotY += dx * 0.006; _rotX = Math.max(-0.9, Math.min(0.9, _rotX + dy * 0.004));
    _pointer.vx = dx * 0.006; _pointer.vy = dy * 0.004;
  }
  function onUp() { _pointer.down = false; setTimeout(function () { _autoSpin = true; }, 2600); }
  function onWheel(e) {
    e.preventDefault();
    _zoom = Math.max(0.55, Math.min(1.8, _zoom - e.deltaY * 0.0011));
  }

  function resize() {
    if (!_wrap || !_renderer) return;
    var w = _wrap.clientWidth || 600, h = _wrap.clientHeight || 420;
    _renderer.setSize(w, h, false);
    if (_camera) {
      _camera.aspect = w / Math.max(h, 1);
      _camera.updateProjectionMatrix();
    }
  }

  function rebuild() {
    if (!_scene) return;
    _neurons.forEach(function (n, i) {
      var rec = _nodeMap[n.id];
      if (!rec) {
        var geo = new THREE.SphereGeometry(2.1, 22, 18);
        var mat = new THREE.MeshStandardMaterial({
          color: 0x94a3b8, emissive: 0x0ea5e9, emissiveIntensity: 0.5,
          roughness: 0.32, metalness: 0.15,
        });
        var mesh = new THREE.Mesh(geo, mat);
        var halo = new THREE.Sprite(new THREE.SpriteMaterial({
          map: glowTexture(), color: 0x22d3ee, transparent: true, opacity: 0.0,
          blending: THREE.AdditiveBlending, depthWrite: false,
        }));
        halo.scale.set(11, 11, 1);
        var p3 = layout3d(n.xPct, n.yPct, i);
        mesh.position.set(p3.x, p3.y, p3.z);
        halo.position.copy(mesh.position);
        _brainGroup.add(mesh, halo);
        rec = { mesh: mesh, halo: halo, pos3: p3, glow: 0.5 };
        _nodeMap[n.id] = rec;
      }
      var st = STATE_COLORS[n.cls] || STATE_COLORS.todo;
      rec.mesh.material.color.setHex(st.main);
      rec.mesh.material.emissive.setHex(st.main);
      rec.mesh.material.emissiveIntensity = st.glow;
      rec.halo.material.color.setHex(st.halo);
      rec.stateCfg = st;
      rec.baseScale = n.id === "hermes_supervisor" ? 1.35 : 1.0;
      rec.label = n;
    });
    _synapses.forEach(function (s) {
      var key = s.a + "|" + s.b;
      if (_curveMap[key]) { _curveMap[key].cls = s.cls; return; }
      var ra = _nodeMap[s.a], rb = _nodeMap[s.b];
      if (!ra || !rb) return;
      var a = ra.pos3, b = rb.pos3;
      var mid = new THREE.Vector3((a.x + b.x) / 2, (a.y + b.y) / 2 + 2.2, (a.z + b.z) / 2);
      var curve = new THREE.QuadraticBezierCurve3(
        new THREE.Vector3(a.x, a.y, a.z), mid, new THREE.Vector3(b.x, b.y, b.z));
      var geo = new THREE.TubeGeometry(curve, 40, 0.24, 6, false);
      var mat = new THREE.MeshBasicMaterial({ color: 0x3b82f6, transparent: true, opacity: 0.28 });
      var line = new THREE.Mesh(geo, mat);
      _brainGroup.add(line);
      var spark = new THREE.Sprite(new THREE.SpriteMaterial({
        map: glowTexture(), color: 0x7dd3fc, transparent: true, opacity: 0,
        blending: THREE.AdditiveBlending, depthWrite: false,
      }));
      spark.scale.set(3.4, 3.4, 1);
      _brainGroup.add(spark);
      _curveMap[key] = { line: line, spark: spark, curve: curve, cls: s.cls, t: Math.random() };
    });
  }

  function tick(t) {
    _raf = requestAnimationFrame(tick);
    if (!_scene || !_wrap) return;
    /* visibility gate: poll the wrapper rect at 4Hz — deterministic across
       browsers, unlike IntersectionObserver (which never fired in the field
       for this embedded page). Pausing off-screen renders is the main win. */
    if (t - _lastVisCheck > 250) {
      _lastVisCheck = t;
      var r = _wrap.getBoundingClientRect();
      var was = _inView;
      _inView = r.width > 0 && r.height > 0 &&
        r.bottom > -40 && r.right > -40 &&
        r.top < (window.innerHeight || 800) + 40 &&
        r.left < (window.innerWidth || 1200) + 40;
      if (_inView && !was) _lastFrame = 0;
    }
    if (!_inView || document.hidden) { _lastFrame = t; return; }
    var interval = 1000 / FPS_CAP - 1;
    if (t - _lastFrame < interval) return;
    var dt = Math.min(0.05, (t - _lastFrame) / 1000 || 0.016);
    _lastFrame = t;

    if (_autoSpin) _rotY += dt * 0.14;
    else { _rotY += _pointer.vx * 0.9; _rotX += _pointer.vy * 0.5; }
    _brainGroup.rotation.y = _rotY;
    _brainGroup.rotation.x = _rotX;
    _camera.position.z = 86 / _zoom;
    _particles.rotation.y = -_rotY * 0.35;

    var now = t / 1000;
    Object.keys(_nodeMap).forEach(function (id, idx) {
      var rec = _nodeMap[id];
      var cfg = rec.stateCfg || STATE_COLORS.todo;
      var pulse = 1 + Math.sin(now * cfg.pulse + idx) * 0.09;
      rec.mesh.scale.setScalar(rec.baseScale * pulse);
      rec.glow += (cfg.glow - rec.glow) * Math.min(1, dt * 4);
      rec.mesh.material.emissiveIntensity = rec.glow;
      var want = (rec.label && rec.label.cls === "active") ? 0.5 :
        (rec.label && rec.label.cls === "blocked") ? 0.55 :
        (rec.label && rec.label.cls === "done") ? 0.26 : 0.12;
      rec.halo.material.opacity += (want - rec.halo.material.opacity) * Math.min(1, dt * 5);
      var haloS = 11 * rec.baseScale * (1 + Math.sin(now * cfg.pulse + idx * 1.7) * 0.12);
      rec.halo.scale.set(haloS, haloS, 1);
    });
    Object.keys(_curveMap).forEach(function (key) {
      var c = _curveMap[key];
      var fireCls = c.cls;
      var speed = fireCls === "downstream" ? 0.55 : 0.85;
      c.t = (c.t + dt * speed) % 1;
      var mat = c.line.material;
      if (fireCls === "fire") {
        mat.color.setHex(0x2dd4bf); mat.opacity = 0.5 + Math.sin(now * 6) * 0.18;
        c.spark.material.color.setHex(0x5eead4);
      } else if (fireCls === "downstream") {
        mat.color.setHex(0xa78bfa); mat.opacity = 0.42;
        c.spark.material.color.setHex(0xc4b5fd);
      } else {
        mat.color.setHex(0x3b82f6); mat.opacity = 0.16;
        c.spark.material.opacity = 0;
        return;
      }
      var p = c.curve.getPoint(c.t);
      c.spark.position.copy(p);
      c.spark.material.opacity = 0.85 * Math.sin(c.t * Math.PI);
    });

    _renderer.render(_scene, _camera);
    _stat.frames += 1;
    updateLabels();
  }

  function updateLabels() {
    if (!_labelLayer || !_wrap || !_camera) return;
    var w = _wrap.clientWidth, h = _wrap.clientHeight;
    var v = new THREE.Vector3();
    _neurons.forEach(function (n) {
      var rec = _nodeMap[n.id];
      if (!rec || !rec.el) return;
      v.copy(rec.pos3).applyMatrix4(_brainGroup.matrixWorld).project(_camera);
      var sx = (v.x * 0.5 + 0.5) * w;
      var sy = (-v.y * 0.5 + 0.5) * h;
      var behind = v.z > 1;
      rec.el.style.transform = "translate(-50%,-50%) translate(" + sx.toFixed(1) + "px," + (sy - 30).toFixed(1) + "px)";
      rec.el.style.opacity = behind ? 0 : (v.z > 0.75 ? 0.35 : 1);
      rec.el.style.pointerEvents = behind ? "none" : "auto";
    });
  }

  function buildLabelEls() {
    if (!_labelLayer) return;
    _labelLayer.innerHTML = "";
    _neurons.forEach(function (n) {
      var el = document.createElement("div");
      el.className = "nb3d-label " + n.cls;
      el.style.setProperty("--nc", n.color);
      el.innerHTML =
        '<span class="nb3d-tool">' + esc(n.tool || "\u2022") + "</span>" +
        '<span class="nb3d-name">' + esc(n.label || n.id) + "</span>" +
        '<span class="nb3d-pill">' + esc(n.badge || n.cls) + (n.runs > 0 ? " \u00d7" + n.runs : "") + "</span>" +
        (n.action ? '<span class="nb3d-act">' + esc(n.action) + "</span>" : "");
      el.title = (n.label || n.id) + " \u2014 " + (n.role || "");
      _labelLayer.appendChild(el);
      var rec = _nodeMap[n.id];
      if (rec) rec.el = el;
    });
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }

  function mountInto(container) {
    if (!container || !available()) return false;
    if (!_wrap) {
      _wrap = document.createElement("div");
      _wrap.className = "nb3d-host";
      _wrap.id = "nb3dHost";
    }
    if (_wrap.parentNode !== container) container.appendChild(_wrap);
    ensureScene();
    resize();
    rebuild();
    buildLabelEls();
    return true;
  }

  function update(payload) {
    payload = payload || {};
    _neurons = payload.neurons || _neurons;
    _synapses = payload.synapses || _synapses;
    if (!_scene) return false;
    rebuild();
    buildLabelEls();
    return true;
  }

  function sleep() {
    if (_raf) { cancelAnimationFrame(_raf); _raf = 0; }
    if (_ro) { _ro.disconnect(); _ro = null; }
    if (_wrap && _wrap.parentNode) _wrap.parentNode.removeChild(_wrap);
    _scene = null; _brainGroup = null; _particles = null;
    _nodeMap = {}; _curveMap = {};
    _renderer = null; /* old GL context released; canvas element kept */
    _inView = true;
  }

  function isAwake() { return !!_scene; }

  function onContextLost(cb) { _lostCb = cb; }

  window.NeuralBrain3D = {
    available: available,
    mountInto: mountInto,
    update: update,
    sleep: sleep,
    isAwake: isAwake,
    onContextLost: onContextLost,
    _stats: function () { return { rebuilds: _stat.rebuilds, frames: _stat.frames, inView: _inView }; },
  };
})();
