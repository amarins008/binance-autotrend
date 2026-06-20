(() => {
  const BASE = "http://127.0.0.1:8020";
  const LAUNCHER = "http://127.0.0.1:8021";
  const NATIVE_HOST = "com.cmux_hermes.host";
  let backendOnline = false;
  let backendHealth = null;
  const id = "cmux-hermes-root";
  const toggleId = "cmux-hermes-toggle";
  const sidebarWidth = 400;
  const FLIP_PREF_KEY = "cmux_hermes_auto_flip_pref_v1";
  const MODE_PREF_KEY = "cmux_hermes_auto_mode_pref_v1";
  const AUTOTRADE_STATE_KEY = "cmux_hermes_autotrade_state_v1";
  const TAB_KEY = "cmux_hermes_tab_v1";
  const PERF_MODE_KEY = "cmux_hermes_perf_mode_v1";
  let autoSessionId = null;
  let symbolSyncInProgress = false;
  let lastSymbolSyncTs = 0;
  let refreshHealthInFlight = false;
  let refreshSignalInFlight = false;
  let refreshAutoInFlight = false;
  let autoRunningState = false;
  let lastFullAutoStatusAt = 0;
  const AUTO_STATUS_FULL_EVERY_MS = 60000;
  const AUTO_STATUS_POLL_VISIBLE_MS = 9000;
  const AUTO_STATUS_POLL_HIDDEN_MS = 18000;
  const AUTO_STATUS_POLL_ECO_VISIBLE_MS = 15000;
  const AUTO_STATUS_POLL_ECO_HIDDEN_MS = 30000;
  const HEALTH_POLL_MS = 10000;
  const HEALTH_POLL_ECO_MS = 20000;
  const SYMBOL_POLL_MS = 3000;
  const SYMBOL_POLL_ECO_MS = 6000;
  let perfEcoMode = true;
  let healthPollTimer = null;
  let symbolPollTimer = null;
  let lastAutoLiteSig = "";
  let lastAutoFullSig = "";

  /* ── Storage helpers ── */
  const storageGet = (keys) => new Promise((r) => { try { if (typeof chrome !== "undefined" && chrome.storage?.local) chrome.storage.local.get(keys, r); else r({}); } catch { r({}); } });
  const storageSet = (obj) => new Promise((r) => { try { if (typeof chrome !== "undefined" && chrome.storage?.local) chrome.storage.local.set(obj, r); else r(); } catch { r(); } });
  const storageRemove = (keys) => new Promise((r) => { try { if (typeof chrome !== "undefined" && chrome.storage?.local) chrome.storage.local.remove(keys, r); else r(); } catch { r(); } });

  const persistAutotradeState = async (p) => { try { localStorage.setItem(AUTOTRADE_STATE_KEY, JSON.stringify(p)); } catch {} await storageSet({ [AUTOTRADE_STATE_KEY]: p }); };
  const loadPersistedAutotradeState = async () => { try { const b = await storageGet([AUTOTRADE_STATE_KEY]); const v = b?.[AUTOTRADE_STATE_KEY]; if (v && typeof v === "object" && v.sessionId) return v; } catch {} try { const r = localStorage.getItem(AUTOTRADE_STATE_KEY); if (r) return JSON.parse(r); } catch {} return null; };
  const clearPersistedAutotradeState = async () => { try { localStorage.removeItem(AUTOTRADE_STATE_KEY); } catch {} await storageRemove([AUTOTRADE_STATE_KEY]); };

  if (document.getElementById(id) || document.getElementById(toggleId)) return;
  const isFuturesPage = () => /\/futures\//i.test(location.pathname) || window.__MOCK_FUTURES__;
  if (!isFuturesPage()) return;

  /* ── Fonts ── */
  const fl = document.createElement("link"); fl.rel = "stylesheet";
  fl.href = "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&family=JetBrains+Mono:wght@400;500;700&display=swap";
  document.head.appendChild(fl);

  const applyPageOffset = (on) => { document.documentElement.style.marginRight = on ? `${sidebarWidth}px` : ""; document.body.style.marginRight = on ? `${sidebarWidth}px` : ""; };

  const root = document.createElement("div"); root.id = id;
  root.style.cssText = `position:fixed;top:0;right:0;width:${sidebarWidth}px;height:100vh;overflow:auto;z-index:2147483647;box-sizing:border-box;`;
  document.body.appendChild(root);

  /* ── CSS Design System ── */
  const style = document.createElement("style");
  style.textContent = `
#${id}{
  --bg0:#060a11;--bg1:#0b1120;--surface:rgba(15,23,42,.65);--glass:rgba(30,41,59,.42);
  --border:rgba(148,163,184,.12);--border-s:rgba(148,163,184,.25);
  --text:#f1f5f9;--text2:#94a3b8;--text3:#64748b;
  --accent:#3b82f6;--accent-s:rgba(59,130,246,.15);
  --long:#10b981;--long-bg:rgba(16,185,129,.12);--long-glow:rgba(16,185,129,.35);
  --short:#ef4444;--short-bg:rgba(239,68,68,.12);--short-glow:rgba(239,68,68,.35);
  --warn:#f59e0b;--warn-bg:rgba(245,158,11,.12);
  --cyan:#06b6d4;--radius:12px;--radius-s:8px;
  font-family:'Inter',system-ui,sans-serif;color:var(--text);
  background:linear-gradient(180deg,var(--bg1),var(--bg0));
  border-left:1px solid var(--border-s);
  box-shadow:-8px 0 32px rgba(0,0,0,.4);
  -webkit-font-smoothing:antialiased;scrollbar-width:thin;scrollbar-color:rgba(148,163,184,.25) transparent;
}
#${id} *{box-sizing:border-box}
#${id}::-webkit-scrollbar{width:6px}
#${id}::-webkit-scrollbar-thumb{background:rgba(148,163,184,.2);border-radius:99px}
#${id} .mn{font-family:'JetBrains Mono','Consolas',monospace}
#${id} .wrap{padding:0 14px 20px;display:flex;flex-direction:column;gap:10px}

/* Header */
#${id} .hdr{position:sticky;top:0;z-index:10;padding:12px 14px 0;
  background:linear-gradient(180deg,rgba(6,10,17,.97) 60%,transparent);backdrop-filter:blur(12px)}
#${id} .hdr-row{display:flex;align-items:center;justify-content:space-between}
#${id} .hdr-brand{display:flex;align-items:center;gap:8px}
#${id} .hdr-dot{width:8px;height:8px;border-radius:50%;background:var(--long);box-shadow:0 0 8px var(--long-glow);flex-shrink:0}
#${id} .hdr-dot.off{background:var(--short);box-shadow:0 0 8px var(--short-glow)}
#${id} .hdr-title{font-size:13px;font-weight:800;letter-spacing:.03em;text-transform:uppercase;color:var(--text2)}
#${id} .hdr-sym{font-size:1.3rem;font-weight:900;letter-spacing:.02em;margin:2px 0 0;
  background:linear-gradient(90deg,#f8fafc,#7dd3fc);-webkit-background-clip:text;background-clip:text;color:transparent}
#${id} .hdr-ts{font-size:10px;color:var(--text3);margin-top:2px}
#${id} .backend-hdr{font-size:10px;margin-top:4px;line-height:1.35}
#${id} .backend-hdr.on{color:var(--long)}
#${id} .backend-hdr.off{color:var(--short)}
#${id} .backend-hdr-sub{font-size:9px;color:var(--text3);margin-top:1px}
#${id} .backend-box{border-radius:var(--radius-s);padding:10px;background:var(--glass);border:1px solid var(--border);margin-bottom:8px}
#${id} .backend-box.on{border-color:rgba(16,185,129,.35)}
#${id} .backend-box.off{border-color:rgba(239,68,68,.35)}
#${id} .backend-row{display:flex;align-items:center;justify-content:space-between;gap:8px}
#${id} .backend-stat{font-size:13px;font-weight:800}
#${id} .backend-stat.on{color:var(--long)}
#${id} .backend-stat.off{color:var(--short)}
#${id} .backend-meta{font-size:10px;color:var(--text3);margin-top:6px;line-height:1.45}
#${id} .backend-cmd{display:block;margin-top:6px;padding:6px 8px;border-radius:var(--radius-s);background:rgba(6,10,17,.8);border:1px solid var(--border);font-size:9px;color:var(--text2);word-break:break-all}

/* Tabs */
#${id} .tabs{display:flex;gap:0;margin-top:10px;border-bottom:1px solid var(--border)}
#${id} .tab{flex:1;padding:9px 4px;font-size:12px;font-weight:700;color:var(--text3);background:0;border:0;
  cursor:pointer;text-align:center;position:relative;transition:color .2s}
#${id} .tab:hover{color:var(--text2)}
#${id} .tab.active{color:var(--accent)}
#${id} .tab.active::after{content:'';position:absolute;bottom:-1px;left:15%;right:15%;height:2px;
  background:var(--accent);border-radius:2px;animation:bc-slide .2s ease}

/* Cards */
#${id} .card{background:var(--surface);backdrop-filter:blur(10px);border:1px solid var(--border);
  border-radius:var(--radius);padding:12px;box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}
#${id} .card-t{font-size:12px;font-weight:800;color:var(--text2);margin:0 0 10px;text-transform:uppercase;letter-spacing:.06em}
#${id} .tab-pane{display:none}
#${id} .tab-pane.active{display:flex;flex-direction:column;gap:10px}
#${id} #pane-auto.active .card{border-color:rgba(59,130,246,.28);box-shadow:inset 0 1px 0 rgba(255,255,255,.03),0 0 0 1px rgba(59,130,246,.08)}

/* Signal Badge */
#${id} .sig-badge{text-align:center;padding:16px 12px}
#${id} .sig-dir{display:inline-flex;align-items:center;justify-content:center;padding:10px 28px;
  border-radius:99px;font-size:18px;font-weight:900;letter-spacing:.08em;
  border:2px solid transparent;transition:all .4s ease}
#${id} .sig-dir.long{background:var(--long-bg);border-color:var(--long);color:var(--long);box-shadow:0 0 20px var(--long-glow)}
#${id} .sig-dir.short{background:var(--short-bg);border-color:var(--short);color:var(--short);box-shadow:0 0 20px var(--short-glow)}
#${id} .sig-dir.wait{background:var(--warn-bg);border-color:var(--warn);color:var(--warn);box-shadow:0 0 20px rgba(245,158,11,.25)}
#${id} .sig-conf{font-size:22px;font-weight:900;margin-top:8px}
#${id} .sig-conf-label{font-size:11px;color:var(--text3);margin-top:2px}

/* Signal Strength Bar */
#${id} .str-bar{height:8px;border-radius:99px;background:linear-gradient(90deg,var(--short),var(--warn) 50%,var(--long));position:relative;overflow:visible;margin:6px 0}
#${id} .str-dot{position:absolute;top:50%;width:16px;height:16px;border-radius:50%;background:#fff;
  border:3px solid var(--accent);transform:translate(-50%,-50%);transition:left .6s ease;
  box-shadow:0 0 10px rgba(59,130,246,.5)}
#${id} .str-labels{display:flex;justify-content:space-between;font-size:10px;color:var(--text3)}

/* Confluence Bars */
#${id} .conf-row{display:flex;align-items:center;gap:8px;margin:3px 0}
#${id} .conf-label{width:42px;font-size:11px;font-weight:700;color:var(--text2);flex-shrink:0}
#${id} .conf-track{flex:1;height:10px;background:rgba(30,41,59,.6);border-radius:99px;overflow:hidden}
#${id} .conf-fill{height:100%;border-radius:99px;transition:width .6s ease}
#${id} .conf-fill.l{background:linear-gradient(90deg,rgba(16,185,129,.5),var(--long))}
#${id} .conf-fill.s{background:linear-gradient(90deg,rgba(239,68,68,.5),var(--short))}
#${id} .conf-val{width:20px;font-size:12px;font-weight:800;text-align:right}

/* Metric Grid */
#${id} .mg{display:grid;grid-template-columns:1fr 1fr;gap:6px}
#${id} .mg3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px}
#${id} .mc{background:var(--glass);border:1px solid var(--border);border-radius:var(--radius-s);padding:8px}
#${id} .mc-l{font-size:10px;color:var(--text3);margin-bottom:2px;font-weight:600}
#${id} .mc-v{font-size:15px;font-weight:800;line-height:1.2}
#${id} .mc-s{font-size:10px;color:var(--text3);margin-top:1px}

/* RSI Bar */
#${id} .rsi-bar{height:6px;border-radius:99px;background:linear-gradient(90deg,var(--long) 0%,var(--long) 28%,var(--warn) 45%,var(--warn) 55%,var(--short) 72%,var(--short) 100%);position:relative;margin:4px 0}
#${id} .rsi-mark{position:absolute;top:-3px;width:4px;height:12px;background:#fff;border-radius:2px;transform:translateX(-50%);transition:left .5s ease;box-shadow:0 0 6px rgba(255,255,255,.5)}

/* Badges */
#${id} .bdg{display:inline-block;padding:3px 8px;border-radius:99px;font-size:10px;font-weight:700;border:1px solid}
#${id} .bdg-l{background:var(--long-bg);border-color:rgba(16,185,129,.4);color:var(--long)}
#${id} .bdg-s{background:var(--short-bg);border-color:rgba(239,68,68,.4);color:var(--short)}
#${id} .bdg-w{background:var(--warn-bg);border-color:rgba(245,158,11,.4);color:var(--warn)}
#${id} .bdg-i{background:rgba(6,182,212,.1);border-color:rgba(6,182,212,.4);color:var(--cyan)}

/* Inputs */
#${id} .inp,#${id} .sel{width:100%;padding:8px 10px;border-radius:var(--radius-s);
  border:1px solid var(--border-s);background:rgba(2,6,23,.6);color:var(--text);
  font:inherit;font-size:13px;outline:0;transition:border-color .15s,box-shadow .15s}
#${id} .inp:focus,#${id} .sel:focus{border-color:rgba(59,130,246,.5);box-shadow:0 0 0 3px var(--accent-s)}
#${id} .lbl{font-size:11px;font-weight:700;color:var(--text2);margin:0 0 4px}
#${id} .row{display:flex;gap:8px;align-items:flex-end}
#${id} .row>*{flex:1}
#${id} .stack{display:flex;flex-direction:column;gap:8px}
#${id} .hint{font-size:11px;color:var(--cyan);margin-top:3px}

/* Buttons */
#${id} .btn{font:inherit;font-weight:700;font-size:13px;border-radius:var(--radius-s);cursor:pointer;
  border:1px solid transparent;padding:10px 12px;transition:transform .08s,filter .12s,box-shadow .15s,opacity .12s;position:relative;overflow:hidden}
#${id} .btn:disabled{opacity:.5;cursor:not-allowed;transform:none}
#${id} .btn:not(:disabled):hover{filter:brightness(1.08)}
#${id} .btn:not(:disabled):active{transform:scale(.97);filter:brightness(.94)}
#${id} .btn-p{background:linear-gradient(180deg,#2563eb,#1d4ed8);color:#eff6ff;border-color:rgba(37,99,235,.5);
  box-shadow:0 8px 20px rgba(37,99,235,.2),inset 0 1px 0 rgba(255,255,255,.15)}
#${id} .btn-block{width:100%}
#${id} .btn-long{background:linear-gradient(180deg,rgba(16,185,129,.3),rgba(6,78,59,.9));border-color:rgba(16,185,129,.5);color:#ecfdf5}
#${id} .btn-short{background:linear-gradient(180deg,rgba(239,68,68,.25),rgba(127,29,29,.9));border-color:rgba(239,68,68,.45);color:#fef2f2}
#${id} .btn-wait{background:linear-gradient(180deg,rgba(245,158,11,.22),rgba(120,53,15,.9));border-color:rgba(245,158,11,.4);color:#fffbeb}
#${id} .btn-warn{background:linear-gradient(180deg,rgba(245,158,11,.28),rgba(120,53,15,.95));border-color:rgba(245,158,11,.55);color:#fffbeb}
#${id} .btn-close{background:linear-gradient(180deg,rgba(59,130,246,.22),rgba(30,58,138,.9));border-color:rgba(59,130,246,.45);color:#eff6ff}
#${id} .btn-ghost{background:rgba(15,23,42,.55);color:var(--text);border-color:var(--border-s);padding:7px 6px;font-size:12px}
#${id} .btn-ghost.active{background:linear-gradient(180deg,rgba(59,130,246,.2),rgba(30,58,138,.8));border-color:rgba(59,130,246,.5);color:#bfdbfe}
#${id} .btn-sm{padding:7px 8px;font-size:11px}
#${id} .g2{display:grid;grid-template-columns:1fr 1fr;gap:6px}
#${id} .g3{display:grid;grid-template-columns:repeat(3,1fr);gap:6px}
#${id} .g4{display:grid;grid-template-columns:repeat(4,1fr);gap:6px}

/* Loading / Feedback */
#${id} .is-loading{opacity:.7;cursor:wait!important}
#${id} .is-success{box-shadow:0 0 0 2px var(--long-glow) inset}
#${id} .is-error{box-shadow:0 0 0 2px var(--short-glow) inset}
#${id} .shimmer{background:linear-gradient(90deg,transparent 0%,rgba(148,163,184,.06) 50%,transparent 100%);
  background-size:200px 100%;animation:bc-shimmer 1.5s infinite}

/* Auto KPIs */
#${id} .kpis{display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-top:8px}
#${id} .kpi{background:var(--glass);border:1px solid var(--border);border-radius:var(--radius-s);padding:8px}
#${id} .kpi-l{font-size:10px;color:var(--text3);margin-bottom:1px}
#${id} .kpi-v{font-size:17px;font-weight:800;line-height:1.15}
#${id} .kpi-s{font-size:10px;color:var(--text3);margin-top:1px}
#${id} .pnl-p{color:var(--long)} #${id} .pnl-m{color:var(--short)}

/* Auto status */
#${id} .a-badges{display:flex;flex-wrap:wrap;gap:6px;align-items:center}
#${id} .a-badge{padding:4px 10px;border-radius:99px;font-size:10px;font-weight:700;border:1px solid var(--border-s);background:var(--bg0);color:var(--text2)}
#${id} .a-badge.run{background:rgba(16,185,129,.15);border-color:rgba(16,185,129,.4);color:#86efac}
#${id} .a-badge.run::before{content:'';display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--long);margin-right:5px;animation:bc-pulse 1.5s infinite}
#${id} .a-badge.stop{background:rgba(148,163,184,.1);border-color:rgba(148,163,184,.3);color:var(--text2)}
#${id} .a-badge.regime-trend{background:rgba(16,185,129,.14);border-color:rgba(16,185,129,.45);color:#86efac}
#${id} .a-badge.regime-range{background:rgba(245,158,11,.12);border-color:rgba(245,158,11,.45);color:#fcd34d}
#${id} .a-badge.regime-volatile{background:rgba(239,68,68,.12);border-color:rgba(239,68,68,.45);color:#fca5a5}
#${id} .a-badge.regime-normal{background:rgba(6,182,212,.12);border-color:rgba(6,182,212,.45);color:#67e8f9}
#${id} .regime-card{margin-top:8px;border-radius:var(--radius-s);padding:10px;border:1px solid var(--border);background:linear-gradient(135deg,rgba(2,6,23,.8),rgba(15,23,42,.86))}
#${id} .regime-card.trend{border-color:rgba(16,185,129,.45);box-shadow:0 0 0 1px rgba(16,185,129,.12) inset}
#${id} .regime-card.range{border-color:rgba(245,158,11,.45);box-shadow:0 0 0 1px rgba(245,158,11,.12) inset}
#${id} .regime-card.volatile{border-color:rgba(239,68,68,.45);box-shadow:0 0 0 1px rgba(239,68,68,.12) inset}
#${id} .regime-card.normal{border-color:rgba(6,182,212,.45);box-shadow:0 0 0 1px rgba(6,182,212,.12) inset}
#${id} .regime-title{font-size:11px;font-weight:800;color:var(--text2);text-transform:uppercase;letter-spacing:.05em}
#${id} .regime-value{font-size:18px;font-weight:900;line-height:1.1;margin-top:3px}
#${id} .regime-desc{font-size:11px;color:var(--text2);margin-top:4px;line-height:1.35}

/* Guardian */
#${id} .guard{margin-top:8px;background:linear-gradient(135deg,rgba(6,182,212,.12),rgba(15,23,42,.9));
  border:1px solid rgba(6,182,212,.35);border-radius:var(--radius-s);padding:10px}
#${id} .guard-t{font-size:11px;font-weight:800;color:var(--cyan);margin-bottom:4px}
#${id} .guard-r{font-size:11px;color:var(--text);margin:2px 0}
#${id} .guard-btn{margin-top:6px;padding:7px 12px;border:1px solid var(--cyan);border-radius:var(--radius-s);
  background:linear-gradient(180deg,#0c4a6e,#082f49);color:#e0f2fe;cursor:pointer;font-size:11px;font-weight:700}

/* Log */
#${id} .log{font-size:11px;color:var(--text2);margin-top:6px;padding:8px;background:var(--glass);
  border-radius:var(--radius-s);border:1px solid var(--border);max-height:60px;overflow-y:auto;line-height:1.4}

/* Check */
#${id} .chk{display:flex;align-items:center;gap:7px;font-size:12px;color:var(--text2)}
#${id} .chk input{width:15px;height:15px;accent-color:var(--accent);cursor:pointer}

/* Alerts */
#${id} .alerts{font-size:11px;color:var(--text2);line-height:1.5}

/* Notes */
#${id} .notes{font-size:11px;color:var(--text2);line-height:1.4;margin-top:4px}
#${id} .notes span{display:block;padding:2px 0;border-bottom:1px solid var(--border)}
#${id} .notes span:last-child{border:0}

/* Risk card */
#${id} .risk-row{display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid var(--border);font-size:12px}
#${id} .risk-row:last-child{border:0}
#${id} .risk-k{color:var(--text2);font-weight:600}
#${id} .risk-v{font-weight:800}

/* Animations */
@keyframes bc-slide{from{transform:scaleX(0)}to{transform:scaleX(1)}}
@keyframes bc-shimmer{0%{background-position:-200px 0}100%{background-position:200px 0}}
@keyframes bc-pulse{0%,100%{opacity:1}50%{opacity:.4}}
@keyframes bc-fadein{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}
#${id} .fadein{animation:bc-fadein .3s ease}
`;
  document.head.appendChild(style);


  /* ── Toggle Button ── */
  const toggle = document.createElement("button"); toggle.id = toggleId;
  toggle.textContent = "ซ่อน Cmux";
  toggle.style.cssText = `position:fixed;top:12px;right:${sidebarWidth+12}px;z-index:2147483647;padding:8px 14px;border-radius:99px;border:1px solid rgba(148,163,184,.25);background:rgba(11,17,32,.88);color:#e8edf7;cursor:pointer;font-family:'Inter',system-ui,sans-serif;font-size:11px;font-weight:700;box-shadow:0 8px 24px rgba(0,0,0,.35);backdrop-filter:blur(8px);transition:all .2s`;
  document.body.appendChild(toggle);
  let isHidden = false; applyPageOffset(true);
  const setHidden = (h) => { isHidden = h; root.style.display = h ? "none" : "block"; toggle.textContent = h ? "แสดง Cmux" : "ซ่อน Cmux"; toggle.style.right = h ? "12px" : `${sidebarWidth+12}px`; applyPageOffset(!h); };
  toggle.onclick = () => setHidden(!isHidden);

  /* ── Symbol Detection ── */
  const symbol = () => {
    const u = new URL(location.href), pp = u.pathname.split("/").filter(Boolean);
    const fi = pp.findIndex(p => p.toLowerCase() === "futures");
    const ps = fi >= 0 ? pp[fi+1] : "";
    const qs = u.searchParams.get("symbol") || "";
    const hm = u.hash.match(/([A-Z0-9]{4,20}(?:USDT|BUSD))/i);
    const tm = document.title.match(/([A-Z0-9]{4,20}(?:USDT|BUSD))/i);
    return (ps || qs || (hm?hm[1]:"") || (tm?tm[1]:"") || "BTCUSDT").toUpperCase().replace("/","");
  };

  /* ── HTML Template ── */
  const html = () => `
<div class="hdr">
  <div class="hdr-row">
    <div class="hdr-brand"><div id="connDot" class="hdr-dot"></div><span class="hdr-title">AI Cmux</span></div>
    <span style="font-size:10px;color:var(--text3)">v2.0</span>
  </div>
  <div id="sym" class="hdr-sym">${symbol()}</div>
  <div id="lastUpdate" class="hdr-ts">กำลังโหลด...</div>
  <div id="backendHdr" class="backend-hdr off">Backend: กำลังตรวจ…</div>
  <div id="backendHdrSub" class="backend-hdr-sub">${BASE.replace("http://", "")}</div>
  <div class="tabs">
    <button class="tab active" data-tab="signal">📊 Signal</button>
    <button class="tab" data-tab="trade">⚡ Trade</button>
    <button class="tab" data-tab="auto">🤖 Auto</button>
    <button class="tab" data-tab="config">⚙️</button>
  </div>
</div>

<div class="wrap">
  <!-- ═══ SIGNAL TAB ═══ -->
  <div id="pane-signal" class="tab-pane active">
    <button id="refresh" class="btn btn-p btn-block" style="font-size:12px;padding:8px">⟳ รีเฟรชการวิเคราะห์</button>
    <div class="card sig-badge">
      <div id="sigDir" class="sig-dir wait">WAIT</div>
      <div id="sigConf" class="sig-conf mn">—</div>
      <div class="sig-conf-label">CONFIDENCE</div>
    </div>
    <div class="card">
      <div class="card-t">Signal Strength</div>
      <div class="str-bar"><div id="strDot" class="str-dot" style="left:50%"></div></div>
      <div class="str-labels"><span>◄ Bearish</span><span>Bullish ►</span></div>
    </div>
    <div class="card">
      <div class="card-t">Confluence Score</div>
      <div class="conf-row"><span class="conf-label" style="color:var(--long)">Long</span><div class="conf-track"><div id="confL" class="conf-fill l" style="width:0%"></div></div><span id="confLV" class="conf-val mn" style="color:var(--long)">0</span></div>
      <div class="conf-row"><span class="conf-label" style="color:var(--short)">Short</span><div class="conf-track"><div id="confS" class="conf-fill s" style="width:0%"></div></div><span id="confSV" class="conf-val mn" style="color:var(--short)">0</span></div>
    </div>
    <div class="card">
      <div class="card-t">Market Metrics</div>
      <div class="mg">
        <div class="mc"><div class="mc-l">RSI (14)</div><div id="mRsi" class="mc-v mn">—</div><div class="rsi-bar"><div id="rsiMark" class="rsi-mark" style="left:50%"></div></div></div>
        <div class="mc"><div class="mc-l">Momentum</div><div id="mMom" class="mc-v mn">—</div><div id="mMomS" class="mc-s">—</div></div>
        <div class="mc"><div class="mc-l">Vol Ratio</div><div id="mVol" class="mc-v mn">—</div></div>
        <div class="mc"><div class="mc-l">ATR %</div><div id="mAtr" class="mc-v mn">—</div></div>
        <div class="mc"><div class="mc-l">Spread</div><div id="mSpread" class="mc-v mn">—</div><div class="mc-s">bps</div></div>
        <div class="mc"><div class="mc-l">Funding</div><div id="mFunding" class="mc-v mn">—</div></div>
      </div>
    </div>
    <div class="card">
      <div class="card-t">Signals</div>
      <div id="sigBadges" style="display:flex;flex-wrap:wrap;gap:4px;margin-bottom:6px"></div>
      <div class="card-t" style="margin-top:8px">Setup</div>
      <div id="sigSetup" style="font-size:12px;color:var(--text)">—</div>
      <div id="sigNotes" class="notes"></div>
    </div>
    <div class="card">
      <div class="card-t">การแจ้งเตือน</div>
      <div id="alerts" class="alerts">—</div>
    </div>
  </div>

  <!-- ═══ TRADE TAB ═══ -->
  <div id="pane-trade" class="tab-pane">
    <div class="card">
      <div class="card-t">ตั้งค่าคำสั่ง</div>
      <div class="stack">
        <div class="row">
          <div><div class="lbl">เลเวอเรจ</div><input id="lev" class="inp mn" type="number" min="1" max="125" value="5"></div>
          <div><div class="lbl">มาร์จิ้น</div><select id="marginType" class="sel"><option value="CROSSED">CROSSED</option><option value="ISOLATED">ISOLATED</option></select></div>
        </div>
        <div class="row">
          <div><div class="lbl">TP %</div><input id="tp" class="inp mn" type="number" step="0.1" min="0.1" value="1.8"></div>
          <div><div class="lbl">SL %</div><input id="sl" class="inp mn" type="number" step="0.1" min="0.1" value="0.8"></div>
          <div style="flex:.6"><div class="lbl">R:R</div><div id="rrRatio" class="mn" style="font-size:14px;font-weight:800;padding:8px 0;color:var(--cyan)">2.25</div></div>
        </div>
      </div>
    </div>
    <div class="card">
      <div class="card-t">เทรดคลิกเดียว</div>
      <div class="lbl">มูลค่า (USDT)</div>
      <input id="usdtAmount" class="inp mn" type="number" step="0.1" min="1" value="5">
      <div class="g4" style="margin-top:6px">
        <button data-quick-usdt="5" class="btn btn-ghost btn-sm">5</button>
        <button data-quick-usdt="10" class="btn btn-ghost btn-sm">10</button>
        <button data-quick-usdt="20" class="btn btn-ghost btn-sm">20</button>
        <button data-quick-usdt="50" class="btn btn-ghost btn-sm">50</button>
      </div>
      <div id="minUsdtHint" class="hint">ขั้นต่ำ: — USDT</div>
      <div class="g2" style="margin-top:8px">
        <button data-side="LONG" class="btn btn-long" style="padding:12px">▲ Long</button>
        <button data-side="SHORT" class="btn btn-short" style="padding:12px">▼ Short</button>
        <button data-side="WAIT" class="btn btn-wait">⏸ รอ</button>
        <button data-side="CLOSE" class="btn btn-close">✕ ปิดโพซิชัน</button>
      </div>
    </div>
  </div>

  <!-- ═══ AUTO TAB ═══ -->
  <div id="pane-auto" class="tab-pane">
    <div class="card">
      <div class="card-t">AutoTrade (ตามกราฟที่เปิด)</div>
      <div class="stack">
        <div class="row">
          <div><div class="lbl">โหมด</div><select id="autoMode" class="sel"><option value="PAPER">PAPER (จำลอง)</option><option value="LIVE">LIVE (จริง)</option></select></div>
        </div>
        <div><div class="lbl">พรีเซ็ต</div>
          <div class="g4">
            <button data-preset="SCALP" class="btn btn-ghost btn-sm">⚡ Scalp</button>
            <button data-preset="MAJOR" class="btn btn-ghost btn-sm">🏛 Major</button>
            <button data-preset="ALT" class="btn btn-ghost btn-sm">💎 Alt</button>
            <button data-preset="MEME" class="btn btn-ghost btn-sm">🐸 Meme</button>
          </div>
        </div>
        <div class="row">
          <div><div class="lbl">Min Confidence</div><input id="autoMinConf" class="inp mn" type="number" step="0.05" min="0.4" max="0.95" value="0.65"></div>
          <div><div class="lbl">Max Spread (bps)</div><input id="autoMaxSpread" class="inp mn" type="number" step="0.5" min="1" max="200" value="22"></div>
        </div>
        <div class="row">
          <div><div class="lbl">Max Slippage (bps)</div><input id="autoMaxSlip" class="inp mn" type="number" step="0.5" min="1" max="300" value="28"></div>
          <div><div class="lbl">Trailing Stop %</div><input id="autoTrail" class="inp mn" type="number" step="0.1" min="0" max="10" value="0.6"></div>
        </div>
        <label class="chk"><input id="autoHybridScan" type="checkbox"> Hybrid: สแกนหาเหรียญที่ดีกว่าแล้วสลับเฉพาะจังหวะชัด</label>
        <div class="row">
          <div><div class="lbl">Hybrid Min Score</div><input id="autoHybridMinScore" class="inp mn" type="number" step="0.01" min="0.2" max="2.0" value="0.72"></div>
          <div><div class="lbl">Hybrid Min Edge</div><input id="autoHybridMinEdge" class="inp mn" type="number" step="0.01" min="0" max="1.0" value="0.06"></div>
        </div>
        <div><div class="lbl">เพดานจำนวน Position สูงสุด</div><input id="autoMaxOpenPositions" class="inp mn" type="number" step="1" min="1" max="20" value="6"></div>
        <label class="chk"><input id="autoVolTargetEnabled" type="checkbox" checked> เปิด Vol-Target Position Sizing</label>
        <div class="row">
          <div><div class="lbl">Vol Target %</div><input id="autoVolTargetPct" class="inp mn" type="number" step="0.01" min="0.05" max="3.0" value="0.22"></div>
          <div><div class="lbl">Vol Lookback (แท่ง 1m)</div><input id="autoVolLookback" class="inp mn" type="number" step="1" min="10" max="240" value="30"></div>
        </div>
        <div class="row">
          <div><div class="lbl">Vol Size Min Mult</div><input id="autoVolSizeMinMult" class="inp mn" type="number" step="0.05" min="0.2" max="1.2" value="0.6"></div>
          <div><div class="lbl">Vol Size Max Mult</div><input id="autoVolSizeMaxMult" class="inp mn" type="number" step="0.05" min="0.8" max="3.0" value="1.4"></div>
        </div>
        <div><div class="lbl">ข้ามเมื่อ funding ต่อต้าน (0=ปิด)</div><input id="autoSkipFunding" class="inp mn" type="number" step="0.0001" min="0" max="0.02" value="0"></div>
        <label class="chk"><input id="autoFlip" type="checkbox"> อนุญาตกลับฝั่ง (Flip)</label>
      </div>
      <div class="g3" style="margin-top:10px">
        <button id="autoStart" class="btn btn-long btn-sm">▶ เริ่ม</button>
        <button id="autoStop" class="btn btn-short btn-sm">■ หยุด</button>
        <button id="autoReset" class="btn btn-wait btn-sm">↺ รีเซ็ต</button>
      </div>
    </div>
    <div class="card">
      <div class="card-t">สถานะ</div>
      <div id="autoStatus" class="a-badges"><span class="a-badge stop">หยุดทำงาน</span></div>
      <div id="orphanBox" class="log" style="margin-top:8px">
        Orphan LIVE: —
        <div class="g3" style="margin-top:6px">
          <button id="orphanCheckBtn" class="btn btn-ghost btn-sm">เช็ก Orphan</button>
          <button id="orphanCloseBtn" class="btn btn-warn btn-sm">Close All LIVE</button>
          <button id="orphanAdoptBtn" class="btn btn-close btn-sm">Adopt LIVE</button>
        </div>
      </div>
      <div id="overallStats" class="kpis" style="margin-top:8px">
        <div class="kpi"><div class="kpi-l">Primary</div><div class="kpi-v mn">—</div></div>
        <div class="kpi"><div class="kpi-l">Active</div><div class="kpi-v mn">—</div></div>
        <div class="kpi"><div class="kpi-l">Hybrid Switch</div><div class="kpi-v mn">—</div></div>
        <div class="kpi"><div class="kpi-l">Top Candidate</div><div class="kpi-v mn">—</div></div>
      </div>
      <div id="regimeCard" class="regime-card normal">
        <div class="regime-title">Market Regime</div>
        <div id="regimeValue" class="regime-value mn">UNKNOWN</div>
        <div id="regimeDesc" class="regime-desc">รอสัญญาณจากระบบ…</div>
        <div id="regimeImpact" class="regime-desc mn">MinConf +0.00 | Size x1.00 | strictness normal</div>
      </div>
      <div id="paperStats" class="kpis">
        <div class="kpi"><div class="kpi-l">Win / Loss</div><div class="kpi-v">—</div></div>
        <div class="kpi"><div class="kpi-l">Win Rate</div><div class="kpi-v">—</div></div>
        <div class="kpi"><div class="kpi-l">PnL</div><div class="kpi-v">—</div></div>
        <div class="kpi"><div class="kpi-l">Position</div><div class="kpi-v">—</div></div>
      </div>
      <div id="guardianCard" class="guard" style="display:none">
        <div class="guard-t">⛨ Guardian TP/SL</div>
        <div id="guardianBody" class="guard-r">—</div>
      </div>
      <div id="autoLog" class="log">Log: —</div>
    </div>
  </div>

  <!-- ═══ CONFIG TAB ═══ -->
  <div id="pane-config" class="tab-pane">
    <div class="card">
      <div class="card-t">Backend API</div>
      <div id="backendBox" class="backend-box off">
        <div class="backend-row">
          <div>
            <div id="backendStatus" class="backend-stat off">ออฟไลน์</div>
            <div id="backendStatusDetail" class="backend-meta">กำลังตรวจสอบ…</div>
          </div>
          <button id="backendRestart" class="btn btn-warn btn-sm" type="button">↻ Restart</button>
        </div>
        <div id="backendHint" class="backend-meta" style="display:none;margin-top:8px"></div>
        <code id="backendCmd" class="backend-cmd" style="display:none"></code>
      </div>
      <button id="backendCheckNow" class="btn btn-ghost btn-block btn-sm" type="button">↻ ตรวจสอบสถานะ</button>
      <label class="chk" style="margin-top:8px"><input id="perfEcoMode" type="checkbox" checked> โหมดประหยัดทรัพยากร (ลดภาระ Chrome)</label>
    </div>
    <div class="card">
      <div class="card-t">⛨ Risk Guardian</div>
      <div id="riskBody">
        <div class="risk-row"><span class="risk-k">Kill Switch</span><span id="riskKill" class="risk-v">—</span></div>
        <div class="risk-row"><span class="risk-k">Max Notional</span><span id="riskNotional" class="risk-v mn">—</span></div>
        <div class="risk-row"><span class="risk-k">Max Leverage</span><span id="riskLev" class="risk-v mn">—</span></div>
        <div class="risk-row"><span class="risk-k">Max Daily Loss</span><span id="riskLoss" class="risk-v mn">—</span></div>
        <div class="risk-row"><span class="risk-k">Daily PnL</span><span id="riskPnl" class="risk-v mn">—</span></div>
      </div>
      <button id="riskRefresh" class="btn btn-ghost btn-block btn-sm" style="margin-top:8px">↻ โหลดใหม่</button>
    </div>
    <div class="card">
      <div class="card-t">การแจ้งเตือน</div>
      <div id="alertsConfig" class="alerts">—</div>
    </div>
  </div>
</div>`;

  root.innerHTML = html();


  /* ── Tab System ── */
  const switchTab = (t) => {
    if (!["signal","trade","auto","config"].includes(String(t))) t = "signal";
    root.querySelectorAll(".tab").forEach(b => b.classList.toggle("active", b.dataset.tab === t));
    root.querySelectorAll(".tab-pane").forEach(p => p.classList.toggle("active", p.id === `pane-${t}`));
    try { localStorage.setItem(TAB_KEY, t); } catch {}
  };
  root.querySelectorAll(".tab").forEach(b => { b.onclick = () => switchTab(b.dataset.tab); });
  try { const st = localStorage.getItem(TAB_KEY); if (st) switchTab(st); } catch {}

  /* ── Alerts ── */
  const setAlerts = (arr) => {
    const t = (arr||[]).map(x => `• ${x}`).join("<br>") || "—";
    const el = root.querySelector("#alerts"); if (el) el.innerHTML = t;
    const el2 = root.querySelector("#alertsConfig"); if (el2) el2.innerHTML = t;
  };

  /* ── Preferences ── */
  const loadFlipPref = () => { try { return localStorage.getItem(FLIP_PREF_KEY) === "1"; } catch {} return false; };
  const saveFlipPref = (c) => { try { localStorage.setItem(FLIP_PREF_KEY, c?"1":"0"); } catch {} };
  const loadModePref = () => { try { const r = localStorage.getItem(MODE_PREF_KEY); if (r==="LIVE"||r==="PAPER") return r; } catch {} return "PAPER"; };
  const saveModePref = (m) => { try { if (m==="LIVE"||m==="PAPER") localStorage.setItem(MODE_PREF_KEY, m); } catch {} };
  const loadPerfMode = () => { try { return localStorage.getItem(PERF_MODE_KEY) !== "normal"; } catch {} return true; };
  const savePerfMode = (eco) => { try { localStorage.setItem(PERF_MODE_KEY, eco ? "eco" : "normal"); } catch {} };
  perfEcoMode = loadPerfMode();

  const flipEl = root.querySelector("#autoFlip");
  if (flipEl) { flipEl.checked = loadFlipPref(); flipEl.addEventListener("change", () => saveFlipPref(!!flipEl.checked)); }
  const modeEl = root.querySelector("#autoMode");
  if (modeEl) { modeEl.value = loadModePref(); modeEl.addEventListener("change", () => saveModePref(modeEl.value)); }

  /* ── R:R ratio ── */
  const updateRR = () => {
    const tp = Number(root.querySelector("#tp")?.value||1.8), sl = Number(root.querySelector("#sl")?.value||0.8);
    const rr = sl > 0 ? (tp/sl).toFixed(2) : "—";
    const el = root.querySelector("#rrRatio"); if (el) el.textContent = rr;
  };
  root.querySelector("#tp")?.addEventListener("input", updateRR);
  root.querySelector("#sl")?.addEventListener("input", updateRR);
  updateRR();

  /* ── Button sync ── */
  const syncAutoButtons = (running) => {
    const sa = root.querySelector("#autoStart"), so = root.querySelector("#autoStop");
    if (sa && so) {
      if (running) { sa.disabled=true;so.disabled=false;sa.style.opacity=".5";so.style.opacity="1";sa.textContent="🔄 กำลังรัน"; if(!so.classList.contains("is-loading")) so.textContent="■ หยุด"; }
      else { sa.disabled=false;so.disabled=true;sa.style.opacity="1";so.style.opacity=".5"; if(!sa.classList.contains("is-loading")) sa.textContent="▶ เริ่ม"; so.textContent="ยังไม่เริ่ม"; }
    }
  };

  const refreshOrphanBox = async () => {
    const box = root.querySelector("#orphanBox");
    if (!box) return;
    try {
      const res = await fetch(`${BASE}/autotrade/orphan-check`);
      const data = await res.json();
      const n = Number(data?.count||0);
      if (n <= 0) {
        box.childNodes[0].textContent = "Orphan LIVE: ไม่มีโพซิชันค้าง";
        return;
      }
      const p = (Array.isArray(data.positions) && data.positions.length) ? data.positions[0] : null;
      const txt = p ? `Orphan LIVE: ${n} | ${p.symbol} ${p.side} qty ${Number(p.qty||0).toFixed(6)} | uPnL ${Number(p.unRealizedProfit||0).toFixed(6)}` : `Orphan LIVE: ${n}`;
      box.childNodes[0].textContent = txt;
    } catch {
      box.childNodes[0].textContent = "Orphan LIVE: ตรวจสอบไม่สำเร็จ";
    }
  };
  const forceUnlockAutoControls = () => {
    const sa = root.querySelector("#autoStart"), so = root.querySelector("#autoStop"), sr = root.querySelector("#autoReset");
    if (sa) { sa.disabled = false; sa.style.opacity = "1"; if(!sa.classList.contains("is-loading")) sa.textContent = "▶ เริ่ม"; }
    if (so) { so.disabled = true; so.style.opacity = ".5"; if(!so.classList.contains("is-loading")) so.textContent = "ยังไม่เริ่ม"; }
    if (sr) { sr.disabled = false; sr.style.opacity = "1"; if(!sr.classList.contains("is-loading")) sr.textContent = "↺ รีเซ็ต"; }
  };
  const getCurrentTab = () => {
    const active = root.querySelector(".tab.active");
    return active?.getAttribute("data-tab") || "signal";
  };
  const autoStatusPollIntervalMs = () => {
    const visible = document.visibilityState === "visible";
    if (perfEcoMode) return visible ? AUTO_STATUS_POLL_ECO_VISIBLE_MS : AUTO_STATUS_POLL_ECO_HIDDEN_MS;
    return visible ? AUTO_STATUS_POLL_VISIBLE_MS : AUTO_STATUS_POLL_HIDDEN_MS;
  };
  const symbolPollIntervalMs = () => perfEcoMode ? SYMBOL_POLL_ECO_MS : SYMBOL_POLL_MS;
  const healthPollIntervalMs = () => perfEcoMode ? HEALTH_POLL_ECO_MS : HEALTH_POLL_MS;
  const updatePerfModeUI = () => {
    const el = root.querySelector("#perfEcoMode");
    if (el) el.checked = !!perfEcoMode;
  };
  const syncFocusLayout = (running) => {
    ["pane-signal","pane-trade"].forEach(pid => { const el = root.querySelector(`#${pid}`); if(el) el.style.pointerEvents = running?"none":""; });
  };

  /* ── Button feedback ── */
  const withBtnFeedback = async (btn, task) => {
    const orig = btn.textContent; btn.disabled=true;
    btn.classList.remove("is-success","is-error"); btn.classList.add("is-loading"); btn.textContent="กำลังทำงาน...";
    try {
      const out = await task();
      btn.classList.remove("is-loading"); btn.classList.add("is-success"); btn.textContent="✓ สำเร็จ";
      setTimeout(() => { btn.classList.remove("is-success"); btn.textContent=orig; btn.disabled=false; }, 600);
      return out;
    } catch(err) {
      btn.classList.remove("is-loading"); btn.classList.add("is-error"); btn.textContent="✗ ผิดพลาด";
      setTimeout(() => { btn.classList.remove("is-error"); btn.textContent=orig; btn.disabled=false; }, 800);
      throw err;
    }
  };

  /* ── Trade payload ── */
  const tradePayload = (side) => ({
    symbol: symbol(), side,
    usdtAmount: Number(root.querySelector("#usdtAmount").value||"5"),
    leverage: Number(root.querySelector("#lev").value||"5"),
    marginType: root.querySelector("#marginType").value,
    takeProfitPct: Number(root.querySelector("#tp").value||"1.8"),
    stopLossPct: Number(root.querySelector("#sl").value||"0.8")
  });

  /* ── Backend health ── */
  const formatUptime = (sec) => {
    if (sec == null || sec < 0) return "—";
    if (sec < 60) return `${sec}s`;
    const m = Math.floor(sec / 60);
    const h = Math.floor(m / 60);
    if (h > 0) return `${h}h ${m % 60}m`;
    return `${m}m`;
  };

  const renderBackendUi = (online, detail) => {
    const dot = root.querySelector("#connDot");
    const hdr = root.querySelector("#backendHdr");
    const hdrSub = root.querySelector("#backendHdrSub");
    const box = root.querySelector("#backendBox");
    const stat = root.querySelector("#backendStatus");
    const statD = root.querySelector("#backendStatusDetail");
    const hint = root.querySelector("#backendHint");
    const cmd = root.querySelector("#backendCmd");
    if (online) {
      dot?.classList.remove("off");
      hdr?.classList.remove("off");
      hdr?.classList.add("on");
      if (hdr) hdr.textContent = "Backend ออนไลน์";
      box?.classList.remove("off");
      box?.classList.add("on");
      stat?.classList.remove("off");
      stat?.classList.add("on");
      if (stat) stat.textContent = "ออนไลน์";
    } else {
      dot?.classList.add("off");
      hdr?.classList.add("off");
      hdr?.classList.remove("on");
      if (hdr) hdr.textContent = "Backend ออฟไลน์";
      box?.classList.add("off");
      box?.classList.remove("on");
      stat?.classList.add("off");
      stat?.classList.remove("on");
      if (stat) stat.textContent = "ออฟไลน์";
    }
    if (statD) statD.textContent = detail || "";
    if (hdrSub) hdrSub.textContent = detail || BASE.replace("http://", "");
    if (hint) {
      if (!online) {
        hint.style.display = "block";
        hint.innerHTML =
          "กด <b>Restart</b> เพื่อเปิด/รีสตาร์ท backend — แนะนำรัน <code>backend\\start_launcher.bat</code> ค้างไว้ก่อน (พอร์ต 8021)";
      } else {
        hint.style.display = "none";
      }
    }
    if (cmd) {
      if (!online) {
        cmd.style.display = "block";
        cmd.textContent =
          "cd backend\n.venv\\Scripts\\python.exe -m uvicorn main:app --host 127.0.0.1 --port 8020";
      } else {
        cmd.style.display = "none";
      }
    }
  };

  const refreshBackendHealth = async () => {
    if (refreshHealthInFlight) return backendOnline;
    refreshHealthInFlight = true;
    const started = Date.now();
    try {
      const r = await fetch(`${BASE}/health`, { signal: AbortSignal.timeout(4000) });
      const d = await r.json();
      if (!r.ok || !d.ok) throw new Error("unhealthy");
      backendOnline = true;
      backendHealth = d;
      const ms = Date.now() - started;
      const parts = [
        BASE.replace("http://", ""),
        `${ms}ms`,
        d.version ? `v${d.version}` : null,
        d.uptimeSec != null ? `uptime ${formatUptime(d.uptimeSec)}` : null,
        d.autotradeRunning ? "AutoTrade รันอยู่" : null,
      ].filter(Boolean);
      renderBackendUi(true, parts.join(" · "));
      return true;
    } catch {
      backendOnline = false;
      backendHealth = null;
      renderBackendUi(false, `ไม่ตอบที่ ${BASE.replace("http://", "")}`);
      return false;
    } finally {
      refreshHealthInFlight = false;
    }
  };

  const errText = (d, fallback) => {
    const x = d?.detail;
    if (typeof x === "string" && x.trim()) return x.trim();
    if (x && typeof x === "object") return x.message || x.msg || JSON.stringify(x);
    if (typeof d?.message === "string" && d.message.trim()) return d.message.trim();
    return fallback;
  };

  const waitForBackend = async (timeoutMs = 32000) => {
    const t0 = Date.now();
    while (Date.now() - t0 < timeoutMs) {
      if (await refreshBackendHealth()) return true;
      await new Promise((r) => setTimeout(r, 1500));
    }
    return false;
  };

  const nativeStartBackend = async () =>
    new Promise((resolve) => {
      try {
        if (typeof chrome === "undefined" || !chrome.runtime?.sendNativeMessage) {
          resolve({ ok: false, msg: "Native messaging unavailable" });
          return;
        }
        chrome.runtime.sendNativeMessage(NATIVE_HOST, { action: "start" }, (resp) => {
          if (chrome.runtime.lastError) {
            resolve({ ok: false, msg: chrome.runtime.lastError.message });
            return;
          }
          resolve(resp || { ok: false, msg: "No native host response" });
        });
      } catch (e) {
        resolve({ ok: false, msg: e?.message || "Native host error" });
      }
    });

  const restartBackend = async () => {
    renderBackendUi(false, "กำลังรีสตาร์ท…");
    if (backendOnline) {
      try {
        const r = await fetch(`${BASE}/system/restart`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: "{}",
          signal: AbortSignal.timeout(8000),
        });
        const d = await r.json().catch(() => ({}));
        if (r.ok) {
          setAlerts([d.message || "กำลังรีสตาร์ท backend…"]);
          if (await waitForBackend()) {
            setAlerts(["Backend กลับมาออนไลน์แล้ว"]);
            refreshSignal();
            await refreshAutoStatus(true);
            refreshRisk();
          } else {
            setAlerts(["รีสตาร์ทแล้วแต่ยังไม่ตอบ — ลอง launcher หรือรัน uvicorn ด้วยมือ"]);
          }
          return;
        }
      } catch {
        // During self-restart the socket can drop and fetch throws "Failed to fetch".
        // In that case, treat as "restart in progress" and wait for health to come back first.
        setAlerts(["กำลังรีสตาร์ท backend… กำลังรอให้กลับมาออนไลน์"]);
        if (await waitForBackend()) {
          setAlerts(["Backend กลับมาออนไลน์แล้ว"]);
          refreshSignal();
          await refreshAutoStatus(true);
          refreshRisk();
          return;
        }
      }
    }
    try {
      // Launcher path (recommended when backend is offline)
      const r = await fetch(`${LAUNCHER}/restart`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
        signal: AbortSignal.timeout(15000),
      });
      const d = await r.json().catch(() => ({}));
      if (r.ok && d.ok !== false) {
        setAlerts([d.message || "สั่งเปิด backend แล้ว"]);
        if (await waitForBackend()) {
          setAlerts(["Backend กลับมาออนไลน์แล้ว"]);
          refreshSignal();
          await refreshAutoStatus(true);
          refreshRisk();
        } else {
          setAlerts([
            "Launcher ตอบแล้วแต่ backend ยังไม่ขึ้น",
            "รัน backend\\start_launcher.bat แล้วกด Restart อีกครั้ง",
          ]);
        }
        return;
      }
      throw new Error(d.message || d.detail || `launcher HTTP ${r.status}`);
    } catch (e) {
      // Final fallback: native host can spawn backend even when launcher is down.
      const n = await nativeStartBackend();
      if (n?.ok) {
        setAlerts([n.msg || "Native host สั่งเปิด backend แล้ว"]);
        if (await waitForBackend()) {
          setAlerts(["Backend กลับมาออนไลน์แล้ว"]);
          refreshSignal();
          await refreshAutoStatus(true);
          refreshRisk();
          return;
        }
      }
      setAlerts([
        `Restart ไม่สำเร็จ: ${e.message || e}`,
        `Native host: ${n?.msg || "ไม่พร้อมใช้งาน"}`,
        "รัน: backend\\start_launcher.bat (พอร์ต 8021) แล้วกด Restart",
        "หรือ: cd backend && .venv\\Scripts\\python.exe -m uvicorn main:app --port 8020",
      ]);
    }
  };

  root.querySelector("#backendRestart")?.addEventListener("click", async () => {
    const btn = root.querySelector("#backendRestart");
    try {
      await withBtnFeedback(btn, restartBackend);
    } catch {}
  });
  root.querySelector("#backendCheckNow")?.addEventListener("click", async () => {
    const btn = root.querySelector("#backendCheckNow");
    await withBtnFeedback(btn, async () => {
      await refreshBackendHealth();
      setAlerts([backendOnline ? "Backend ออนไลน์" : "Backend ออฟไลน์"]);
    });
  });

  /* ── Signal Refresh (uses /intel/analyze for max accuracy) ── */
  const refreshSignal = async () => {
    if (refreshSignalInFlight) return;
    refreshSignalInFlight = true;
    if (!backendOnline) {
      setAlerts(["Backend ออฟไลน์ — ไปแท็บ ⚙️ แล้วกด Restart"]);
      refreshSignalInFlight = false;
      return;
    }
    try {
      const sym = symbol();
      root.querySelector("#sym").textContent = sym;
      const res = await fetch(`${BASE}/intel/analyze`, { method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify({symbol:sym}) });
      const d = await res.json();
      const sig = d.signal || "WAIT";
      const conf = d.confidence || 0;
      const prec = d.precision || {};
      const mom = d.momentum || {};
      const exec = d.execution || {};
      const confPct = (conf*100).toFixed(1);

      // Signal badge
      const dirEl = root.querySelector("#sigDir");
      if(dirEl){ dirEl.textContent=sig; dirEl.className=`sig-dir ${sig==="LONG"?"long":sig==="SHORT"?"short":"wait"}`; }
      const confEl = root.querySelector("#sigConf");
      if(confEl) confEl.textContent = `${confPct}%`;

      // Strength bar
      const strength = sig==="LONG" ? 50+conf*50 : sig==="SHORT" ? 50-conf*50 : 50;
      const dotEl = root.querySelector("#strDot");
      if(dotEl) dotEl.style.left = `${strength}%`;

      // Confluence
      const ls = prec.longScore||0, ss = prec.shortScore||0;
      const maxS = Math.max(ls,ss,8);
      root.querySelector("#confL").style.width = `${(ls/maxS)*100}%`;
      root.querySelector("#confS").style.width = `${(ss/maxS)*100}%`;
      root.querySelector("#confLV").textContent = ls;
      root.querySelector("#confSV").textContent = ss;

      // Metrics
      const rsi = prec.rsi14 ?? 50;
      root.querySelector("#mRsi").textContent = rsi.toFixed(1);
      root.querySelector("#rsiMark").style.left = `${Math.min(Math.max(rsi,0),100)}%`;
      root.querySelector("#mMom").textContent = `${(mom.momentumPct||0).toFixed(3)}%`;
      root.querySelector("#mMomS").textContent = mom.divergence||"NONE";
      root.querySelector("#mVol").textContent = (mom.volumeRatio||0).toFixed(2);
      root.querySelector("#mAtr").textContent = `${((prec.atrPct||0)*100).toFixed(2)}%`;
      root.querySelector("#mSpread").textContent = exec.spreadBps!=null ? exec.spreadBps.toFixed(1) : "—";
      root.querySelector("#mFunding").textContent = exec.lastFundingRate!=null ? (exec.lastFundingRate*100).toFixed(4)+"%" : "—";

      // Signal badges
      const badges = [];
      if(prec.trendUp) badges.push('<span class="bdg bdg-l">Trend ↑</span>');
      if(prec.trendDown) badges.push('<span class="bdg bdg-s">Trend ↓</span>');
      if(prec.breakoutUp) badges.push('<span class="bdg bdg-l">Breakout ↑</span>');
      if(prec.breakoutDown) badges.push('<span class="bdg bdg-s">Breakout ↓</span>');
      if(mom.divergence && mom.divergence!=="NONE") badges.push(`<span class="bdg bdg-w">${mom.divergence}</span>`);
      root.querySelector("#sigBadges").innerHTML = badges.join("") || '<span class="bdg bdg-i">No signals</span>';

      // Setup & notes
      root.querySelector("#sigSetup").textContent = d.setup || "—";
      const notes = d.notes || [];
      root.querySelector("#sigNotes").innerHTML = notes.map(n => `<span>${n}</span>`).join("");

      // Alerts
      try {
        const ar = await fetch(`${BASE}/risk-alerts?symbol=${encodeURIComponent(sym)}`);
        const ad = await ar.json();
        setAlerts(ad.alerts || []);
      } catch {}

      // Symbol meta
      try {
        const mr = await fetch(`${BASE}/symbol-meta?symbol=${encodeURIComponent(sym)}`);
        const md = await mr.json();
        if(md?.minUsdtApprox) root.querySelector("#minUsdtHint").textContent = `ขั้นต่ำ: ${md.minUsdtApprox} USDT`;
      } catch {}

      root.querySelector("#lastUpdate").textContent = `อัพเดท: ${new Date().toLocaleTimeString()}`;
    } catch(e) {
      setAlerts([`ข้อผิดพลาด: ${e.message||e}`]);
    } finally {
      refreshSignalInFlight = false;
    }
  };

  root.querySelector("#refresh").onclick = () => {
    const btn = root.querySelector("#refresh");
    withBtnFeedback(btn, refreshSignal).catch(()=>{});
  };

  /* ── Trade handlers ── */
  root.querySelectorAll("button[data-side]").forEach(b => {
    b.onclick = async () => {
      try {
        await withBtnFeedback(b, async () => {
          const side = b.getAttribute("data-side"), payload = tradePayload(side);
          const res = await fetch(`${BASE}/trade`, { method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(payload) });
          const data = await res.json();
          if(!res.ok) {
            if(data?.detail?.requiredMinUsdtApprox) { root.querySelector("#usdtAmount").value = String(Number(data.detail.requiredMinUsdtApprox)); throw new Error(`${data.detail.message} (ปรับขั้นต่ำแล้ว)`); }
            throw new Error(data?.detail?.message || (typeof data?.detail==="string"?data.detail:JSON.stringify(data)));
          }
          setAlerts([`ส่งคำสั่ง: ${side} ${payload.symbol} ${payload.usdtAmount} USDT (qty ${data?.quantity||"-"})`]);
        });
      } catch(e) { setAlerts([`ส่งคำสั่งไม่สำเร็จ: ${e.message||e}`]); }
    };
  });

  /* ── Guardian close ── */
  root.addEventListener("click", async (ev) => {
    const t = ev.target; if(!(t instanceof HTMLElement) || t.id !== "guardianCloseNow") return;
    try {
      await withBtnFeedback(t, async () => {
        const payload = tradePayload("CLOSE");
        const res = await fetch(`${BASE}/trade`, { method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(payload) });
        const data = await res.json();
        if(!res.ok) throw new Error(data?.detail?.message || (typeof data?.detail==="string"?data.detail:JSON.stringify(data)));
        setAlerts([`ปิดโพซิชันฉุกเฉินแล้ว (${payload.symbol})`]);
        await refreshAutoStatus(true);
      });
    } catch(e) { setAlerts([`ปิด Guardian ไม่สำเร็จ: ${e.message||e}`]); }
  });

  /* ── Quick USDT ── */
  root.querySelectorAll("button[data-quick-usdt]").forEach(b => {
    b.onclick = () => {
      root.querySelector("#usdtAmount").value = b.getAttribute("data-quick-usdt");
      root.querySelectorAll("button[data-quick-usdt]").forEach(x => x.classList.toggle("active", x===b));
    };
  });

  /* ── Presets ── */
  const applyPreset = (p) => {
    root.querySelectorAll("button[data-preset]").forEach(el => el.classList.toggle("active", el.getAttribute("data-preset")===p));
    const mc=root.querySelector("#autoMinConf"),sp=root.querySelector("#autoMaxSpread"),sl=root.querySelector("#autoMaxSlip"),tr=root.querySelector("#autoTrail"),fl2=root.querySelector("#autoFlip");
    if(p==="SCALP"){ mc.value="0.62";sp.value="12";sl.value="22";tr.value="0.2";root.querySelector("#tp").value="0.35";root.querySelector("#sl").value="0.25";fl2.checked=true;setAlerts(["พรีเซ็ต Scalp: เล่นสั้น เก็บกำไรทีละนิด"]); }
    else if(p==="MAJOR"){ mc.value="0.6";sp.value="10";sl.value="15";tr.value="0.6";fl2.checked=false;setAlerts(["พรีเซ็ต Major: BTC/ETH/SOL"]); }
    else if(p==="ALT"){ mc.value="0.52";sp.value="20";sl.value="30";tr.value="0.8";fl2.checked=true;setAlerts(["พรีเซ็ต Alt: เหรียญกลาง"]); }
    else{ mc.value="0.48";sp.value="35";sl.value="55";tr.value="1.2";fl2.checked=true;setAlerts(["พรีเซ็ต Meme: ผันผวนสูง"]); }
    updateRR();
  };
  root.querySelectorAll("button[data-preset]").forEach(b => { b.onclick = () => applyPreset(b.getAttribute("data-preset")); });

  const syncAutoConfigFromBackend = (cfg, sessionRunning) => {
    if(sessionRunning && cfg.executionMode && modeEl){
      modeEl.value = cfg.executionMode;
      saveModePref(cfg.executionMode);
    }
    const hyEl = root.querySelector("#autoHybridScan");
    if (hyEl && typeof cfg.hybridScan === "boolean") hyEl.checked = !!cfg.hybridScan;
    const hsEl = root.querySelector("#autoHybridMinScore");
    if (hsEl && Number.isFinite(Number(cfg.hybridMinScore))) hsEl.value = String(Number(cfg.hybridMinScore));
    const heEl = root.querySelector("#autoHybridMinEdge");
    if (heEl && Number.isFinite(Number(cfg.hybridMinEdge))) heEl.value = String(Number(cfg.hybridMinEdge));
    const mopEl = root.querySelector("#autoMaxOpenPositions");
    if (mopEl) {
      if (sessionRunning && Number.isFinite(Number(cfg.maxOpenPositions))) mopEl.value = String(Number(cfg.maxOpenPositions));
      else mopEl.value = "6";
    }
    const vteEl = root.querySelector("#autoVolTargetEnabled");
    if (vteEl && typeof cfg.volTargetEnabled === "boolean") vteEl.checked = !!cfg.volTargetEnabled;
    const vtpEl = root.querySelector("#autoVolTargetPct");
    if (vtpEl && Number.isFinite(Number(cfg.volTargetPct))) vtpEl.value = String(Number(cfg.volTargetPct));
    const vlbEl = root.querySelector("#autoVolLookback");
    if (vlbEl && Number.isFinite(Number(cfg.volLookback))) vlbEl.value = String(Number(cfg.volLookback));
    const vminEl = root.querySelector("#autoVolSizeMinMult");
    if (vminEl && Number.isFinite(Number(cfg.volSizeMinMult))) vminEl.value = String(Number(cfg.volSizeMinMult));
    const vmaxEl = root.querySelector("#autoVolSizeMaxMult");
    if (vmaxEl && Number.isFinite(Number(cfg.volSizeMaxMult))) vmaxEl.value = String(Number(cfg.volSizeMaxMult));
  };

  const buildRegimeView = (d) => {
    const regimeName = String(d?.currentRegime?.name || d?.lastDecision?.regime?.name || "UNKNOWN").toUpperCase();
    const regimeObj = d?.currentRegime || d?.lastDecision?.regime || {};
    const confidenceBoost = Number(regimeObj?.confidenceBoost ?? 0);
    const sizeMultiplier = Number(regimeObj?.sizeMultiplier ?? 1);
    const strictness = String(regimeObj?.strictness || "normal");
    const regimeClass = regimeName==="TREND" ? "regime-trend" : regimeName==="RANGE" ? "regime-range" : regimeName==="VOLATILE" ? "regime-volatile" : "regime-normal";
    const regimeTone = regimeName==="TREND" ? "trend" : regimeName==="RANGE" ? "range" : regimeName==="VOLATILE" ? "volatile" : "normal";
    const regimeDesc = regimeName==="TREND"
      ? "แนวโน้มชัดเจน ระบบผ่อนเกณฑ์เล็กน้อยเพื่อเกาะเทรนด์"
      : regimeName==="RANGE"
        ? "ตลาดแกว่งในกรอบ ระบบเพิ่มความระวังและลดขนาดไม้"
        : regimeName==="VOLATILE"
          ? "ผันผวนสูง ระบบเข้มงวดขึ้นและลดความเสี่ยงอัตโนมัติ"
          : "สมดุลปกติ ใช้เกณฑ์มาตรฐานของระบบ";
    return { regimeName, confidenceBoost, sizeMultiplier, strictness, regimeClass, regimeTone, regimeDesc };
  };

  const renderAutoTop = (cfg, cfgSym, curSym, sessionRunning, d, regime) => {
    const otherHint = sessionRunning && cfgSym && cfgSym!==curSym
      ? `<span class="a-badge" style="border-color:rgba(245,158,11,.4);color:#fde68a">รันที่ ${cfgSym}</span>`
      : "";
    root.querySelector("#autoStatus").innerHTML = `
      <span class="a-badge ${sessionRunning?"run":"stop"}">${sessionRunning?"กำลังทำงาน":"หยุดทำงาน"}</span>
      <span class="a-badge">โหมด ${cfg.executionMode||"-"}</span>
      <span class="a-badge ${regime.regimeClass}">Regime ${regime.regimeName}</span>
      <span class="a-badge">เทรด/ชม. ${d.tradesLastHour??0}</span>
      ${otherHint}`;
  };

  const renderOverallAndRegime = (cfg, cfgSym, curSym, d, regime) => {
    const primarySym = (cfg.primarySymbol || cfgSym || curSym || "-").toUpperCase();
    const activeSym = (cfgSym || curSym || "-").toUpperCase();
    const logsAll = Array.isArray(d.log) ? d.log : [];
    const switchCount = logsAll.filter(x => /HYBRID switch/i.test(String(x?.msg||""))).length;
    const topC = Array.isArray(d.scanBoard) && d.scanBoard.length ? d.scanBoard[0] : null;
    const scanSkip = d.scanStatus || d.lastSkip || null;
    const scanCode = String(scanSkip?.code || "").toUpperCase();
    const scanMsg = String(scanSkip?.msg || "");
    const scanRelevant = /SCAN|TIMEOUT|ANALYZE|INTEL/i.test(`${scanCode} ${scanMsg}`);
    const scanFallback = scanRelevant && (scanCode || scanMsg)
      ? `${scanCode || "SCAN"}${scanMsg ? ` · ${scanMsg.replace(/^Skip:\s*/i, "")}` : ""}`
      : "—";
    const overallEl = root.querySelector("#overallStats");
    if(overallEl){
      overallEl.innerHTML = `
        <div class="kpi"><div class="kpi-l">Primary</div><div class="kpi-v mn">${primarySym}</div></div>
        <div class="kpi"><div class="kpi-l">Active</div><div class="kpi-v mn">${activeSym}</div></div>
        <div class="kpi"><div class="kpi-l">Regime</div><div class="kpi-v mn">${regime.regimeName}</div><div class="kpi-s">ตลาดปัจจุบัน</div></div>
        <div class="kpi"><div class="kpi-l">Hybrid Switch</div><div class="kpi-v mn">${switchCount}</div></div>
        <div class="kpi"><div class="kpi-l">Top Candidate</div><div class="kpi-v mn">${topC?`${topC.symbol} ${topC.signal}`:scanFallback}</div></div>`;
    }
    const regimeCard = root.querySelector("#regimeCard");
    const regimeValueEl = root.querySelector("#regimeValue");
    const regimeDescEl = root.querySelector("#regimeDesc");
    const regimeImpactEl = root.querySelector("#regimeImpact");
    if (regimeCard) regimeCard.className = `regime-card ${regime.regimeTone}`;
    if (regimeValueEl) regimeValueEl.textContent = regime.regimeName;
    if (regimeDescEl) regimeDescEl.textContent = regime.regimeDesc;
    if (regimeImpactEl) {
      const boostSign = regime.confidenceBoost >= 0 ? "+" : "";
      regimeImpactEl.textContent = `MinConf ${boostSign}${regime.confidenceBoost.toFixed(2)} | Size x${regime.sizeMultiplier.toFixed(2)} | strictness ${regime.strictness}`;
    }
  };

  const renderPaperAndLog = (cfg, d, sessionRunning) => {
    const p = d.paper||{};
    const liveToday = d.kpiTodayAllSymbols?.live || d.kpiToday?.live || {};
    const lv = d.liveStatsAll || d.liveStats || {};
    const ap = d.activePosition||{};
    const mode = ap.mode || cfg.executionMode || "PAPER";
    const pos = mode==="LIVE" ? (ap.live||{}) : (ap.paper||{});
    const st = mode==="LIVE" ? (liveToday.wins!=null || liveToday.losses!=null ? liveToday : lv) : p;
    const posSide = pos.side||"FLAT";
    const posQty = Number(pos.qty||0);
    const posUsdt = Number(pos.notionalUsdtApprox||0);
    const posLev = Number(pos.leverage || cfg.leverage || 0);
    const posLevText = posLev > 0 ? ` · x${Math.floor(posLev)}` : "";
    const pnl = Number(st.realizedPnl??0);
    const statLabel = mode==="LIVE" ? "Today LIVE · all symbols" : "Paper Trades";
    root.querySelector("#paperStats").innerHTML = `
      <div class="kpi"><div class="kpi-l">Win / Loss</div><div class="kpi-v mn">${st.wins??0} / ${st.losses??0}</div><div class="kpi-s">${statLabel}</div></div>
      <div class="kpi"><div class="kpi-l">Win Rate</div><div class="kpi-v mn">${st.winRatePct??0}%</div></div>
      <div class="kpi"><div class="kpi-l">Realized PnL</div><div class="kpi-v mn ${pnl>=0?"pnl-p":"pnl-m"}">${pnl} USDT</div></div>
      <div class="kpi"><div class="kpi-l">Position</div><div class="kpi-v">${posSide}${posLevText}</div><div class="kpi-s">${mode} · ${posQty.toFixed(6)} (~${posUsdt.toFixed(2)} USDT)</div></div>`;

    const logs = Array.isArray(d.log) ? d.log : [];
    const startedAt = Number(d.startedAt||0);
    const isHistoryMsg = (m) => /Snapshot restored|auto-resume/i.test(String(m||""));
    const pickLog = (() => {
      if (!logs.length) return null;
      if (sessionRunning) {
        const liveNow = logs.find(x => Number(x?.ts||0) >= startedAt && !isHistoryMsg(x?.msg));
        if (liveNow) return liveNow;
      }
      const nonHistory = logs.find(x => !isHistoryMsg(x?.msg));
      return nonHistory || logs[0];
    })();
    const logTag = pickLog && isHistoryMsg(pickLog.msg) ? "[HISTORY]" : "[LIVE]";
    const lg = pickLog ? `${logTag} ${new Date((pickLog.ts||0)*1000).toLocaleTimeString()} ${pickLog.msg}` : "—";
    const skip = d.lastSkip;
    const skipPart = skip && skip.msg && (!pickLog || String(pickLog.msg)!==String(skip.msg)) ? ` | ข้าม [${skip.code||"?"}]: ${skip.msg}` : "";
    const errN = Number(d.consecutiveErrors)||0;
    const errPart = errN>0 ? ` | ผิดพลาดต่อเนื่อง: ${errN}` : "";
    const cont = d.continuity||{};
    const hStr = Array.isArray(cont.hints)&&cont.hints.length ? cont.hints.join(" · ") : "";
    const rStr = cont.recoveredLog ? String(cont.recoveredLog) : "";
    const contP = [hStr,rStr].filter(Boolean).join(" | ");
    root.querySelector("#autoLog").textContent = `Log: ${lg}${skipPart}${errPart}${contP?` | ${contP}`:""}`;
  };

  const renderGuardian = (d) => {
    const guardian = d.liveGuardian||null;
    const gCard = root.querySelector("#guardianCard");
    const gBody = root.querySelector("#guardianBody");
    if(!(gCard && gBody)) return;
    if(!guardian){ gCard.style.display="none"; gBody.textContent="—"; return; }
    const tp=Number(guardian.tp||0),sl2=Number(guardian.sl||0),entry=Number(guardian.entryMark||0);
    const armTs = guardian.armedAt ? new Date(Number(guardian.armedAt)*1000).toLocaleTimeString() : "—";
    const active = guardian.active;
    gBody.innerHTML = `
      <div class="guard-r">สถานะ: <b style="color:${active?"var(--cyan)":"var(--text3)"}">${active?"กำลังเฝ้าราคา":`หยุดแล้ว (${guardian.closedBy||"ปิด"})`}</b></div>
      <div class="guard-r">คู่: ${guardian.symbol||"—"} | ฝั่ง: ${guardian.side||"—"}</div>
      <div class="guard-r">เข้า: <span class="mn">${entry?entry.toFixed(6):"—"}</span></div>
      <div class="guard-r">TP: <span class="mn" style="color:var(--long)">${tp?tp.toFixed(6):"—"}</span> | SL: <span class="mn" style="color:var(--short)">${sl2?sl2.toFixed(6):"—"}</span></div>
      <div class="guard-r">เริ่มเฝ้า: ${armTs}</div>
      ${active?`<button id="guardianCloseNow" class="guard-btn">✕ ปิด Guardian ตอนนี้</button>`:""}`;
    gCard.style.display = "block";
  };

  /* ── Auto Status Refresh ── */
  const refreshAutoStatus = async (forceFull = false) => {
    if (refreshAutoInFlight) return;
    refreshAutoInFlight = true;
    const curSym = symbol();
    try {
      const now = Date.now();
      const needFull = !!forceFull || (now - lastFullAutoStatusAt >= AUTO_STATUS_FULL_EVERY_MS);
      const statusPath = needFull ? "autotrade/status" : "autotrade/status-lite";
      const r = await fetch(`${BASE}/${statusPath}?symbol=${encodeURIComponent(curSym)}`);
      const d = await r.json();
      if (needFull && r.ok) lastFullAutoStatusAt = now;
      const liteSig = JSON.stringify({
        running: !!d.running,
        sid: d.sessionId || null,
        trades: d.tradesLastHour || 0,
        skip: d.lastSkip?.msg || "",
        err: d.consecutiveErrors || 0,
        mode: d.activePosition?.mode || "",
        liveSide: d.activePosition?.live?.side || "",
        paperSide: d.activePosition?.paper?.side || "",
        signal: d.lastDecision?.signal || "",
        conf: d.lastDecision?.confidence || 0,
        regime: d.currentRegime?.name || d.lastDecision?.regime?.name || "",
      });
      if (!needFull && liteSig === lastAutoLiteSig) return;
      lastAutoLiteSig = liteSig;
      if (needFull) {
        const fullSig = JSON.stringify({
          cfg: d.config || {},
          board: d.scanBoard || [],
          liveStats: d.liveStats || {},
          paper: d.paper || {},
          guardian: d.liveGuardian || null,
          continuity: d.continuity || {},
        });
        if (fullSig === lastAutoFullSig) {
          syncAutoButtons(!!d.running);
          return;
        }
        lastAutoFullSig = fullSig;
      }
      const cfg = d.config || {};
      const cfgSym = (cfg.symbol||"").toUpperCase();
      const cfgScanMode = !!cfg.marketScan || cfgSym === "AUTO" || cfgSym === "SCAN";
      const sessionRunning = !!d.running;
      autoRunningState = sessionRunning;

      if(sessionRunning && d.sessionId){ autoSessionId = d.sessionId; await persistAutotradeState({ sessionId:d.sessionId, symbol:cfgSym||curSym, running:true, ts:Date.now() }); }
      else { autoSessionId = null; await clearPersistedAutotradeState(); }

      const samePair = sessionRunning && (!cfgSym || cfgSym===curSym);
      syncAutoButtons(sessionRunning);
      syncFocusLayout(samePair);
      syncAutoConfigFromBackend(cfg, sessionRunning);
      const regime = buildRegimeView(d);
      renderAutoTop(cfg, cfgSym, curSym, sessionRunning, d, regime);
      renderOverallAndRegime(cfg, cfgSym, curSym, d, regime);
      // Keep running symbol aligned with the currently opened chart symbol.
      if (
        sessionRunning &&
        !cfgScanMode &&
        cfgSym &&
        cfgSym !== curSym &&
        !symbolSyncInProgress &&
        Date.now() - lastSymbolSyncTs > 12000
      ) {
        symbolSyncInProgress = true;
        lastSymbolSyncTs = Date.now();
        setAlerts([`ซิงก์เหรียญอัตโนมัติ: ${cfgSym} → ${curSym}`]);
        try {
          const stopReq = async (body) => {
            const rr = await fetch(`${BASE}/autotrade/stop`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(body)
            });
            return { r: rr, d: await rr.json().catch(() => ({})) };
          };
          let { r: rs, d: ds } = await stopReq({ sessionId: autoSessionId || d.sessionId || null });
          if (ds?.ignored && ds?.reason === "SESSION_MISMATCH") {
            ({ r: rs, d: ds } = await stopReq({ force: true }));
          }
          if (!rs.ok) throw new Error(ds?.detail || "หยุด AutoTrade เพื่อซิงก์เหรียญไม่สำเร็จ");

          const nextCfg = { ...(cfg || {}), symbol: curSym };
          const rr = await fetch(`${BASE}/autotrade/start`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(nextCfg)
          });
          const dd = await rr.json().catch(() => ({}));
          if (!rr.ok) throw new Error(dd?.detail?.message || dd?.detail || "เริ่ม AutoTrade หลังซิงก์เหรียญไม่สำเร็จ");
          autoSessionId = dd?.sessionId || null;
          await persistAutotradeState({ sessionId: autoSessionId, symbol: curSym, running: true, ts: Date.now() });
          setAlerts([`ซิงก์เหรียญสำเร็จ: ตอนนี้รันที่ ${curSym}`]);
        } catch (e) {
          setAlerts([`ซิงก์เหรียญอัตโนมัติไม่สำเร็จ: ${e.message || e}`]);
        } finally {
          symbolSyncInProgress = false;
          setTimeout(() => refreshAutoStatus(true), 1200);
        }
      }

      renderPaperAndLog(cfg, d, sessionRunning);
      renderGuardian(d);
      if (needFull) await refreshOrphanBox();
    } catch {
      // If status API is unreachable/stale, do not hard-lock "Start" button by old cache.
      autoSessionId = null;
      autoRunningState = false;
      await clearPersistedAutotradeState();
      syncAutoButtons(false);
      syncFocusLayout(false);
    } finally {
      refreshAutoInFlight = false;
    }
  };

  const startAutoFromChartTab = async () => {
    try {
      const btn = root.querySelector("#autoStart");
      if (!btn) throw new Error("ไม่พบปุ่มเริ่ม Auto");
      setAlerts(["กำลังเริ่ม AutoTrade..."]);
      await withBtnFeedback(btn, async () => {
        const payload = {
          symbol:"AUTO", primarySymbol:symbol(), usdtAmount:Number(root.querySelector("#usdtAmount").value||"5"),
          leverage:Number(root.querySelector("#lev").value||"5"), marginType:root.querySelector("#marginType").value,
          takeProfitPct:Number(root.querySelector("#tp").value||"1.8"), stopLossPct:Number(root.querySelector("#sl").value||"0.8"),
          minConfidence:Number(root.querySelector("#autoMinConf").value||"0.65"), allowFlip:!!root.querySelector("#autoFlip").checked,
          executionMode:root.querySelector("#autoMode").value, maxSpreadBps:Number(root.querySelector("#autoMaxSpread").value||"22"),
          maxSlippageBps:Number(root.querySelector("#autoMaxSlip").value||"28"), trailingStopPct:Number(root.querySelector("#autoTrail").value||"0.6"),
          skipFundingAgainst:Number(root.querySelector("#autoSkipFunding").value||"0"),
          marketScan:true,
          hybridScan:!!root.querySelector("#autoHybridScan")?.checked,
          hybridMinScore:Number(root.querySelector("#autoHybridMinScore")?.value||"0.72"),
          hybridMinEdge:Number(root.querySelector("#autoHybridMinEdge")?.value||"0.06"),
          maxOpenPositions:Number(root.querySelector("#autoMaxOpenPositions")?.value||"6"),
          volTargetEnabled:!!root.querySelector("#autoVolTargetEnabled")?.checked,
          volTargetPct:Number(root.querySelector("#autoVolTargetPct")?.value||"0.22"),
          volLookback:Number(root.querySelector("#autoVolLookback")?.value||"30"),
          volSizeMinMult:Number(root.querySelector("#autoVolSizeMinMult")?.value||"0.6"),
          volSizeMaxMult:Number(root.querySelector("#autoVolSizeMaxMult")?.value||"1.4"),
          noTradeWindows:[],
          intervalSec:20, cooldownSec:120, maxTradesPerHour:6
        };
        saveFlipPref(!!payload.allowFlip); saveModePref(payload.executionMode);
        const r = await fetch(`${BASE}/autotrade/start`, { method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(payload) });
        const d = await r.json().catch(() => ({}));
        if(!r.ok) throw new Error(errText(d, "เริ่ม AutoTrade ไม่สำเร็จ"));
        autoSessionId = d?.sessionId||null;
        await persistAutotradeState({ sessionId:autoSessionId, symbol:(payload.symbol||"").toUpperCase(), running:true, ts:Date.now() });
        setAlerts([`เริ่ม AutoTrade: ${payload.symbol} ${payload.usdtAmount} USDT`]);
        syncAutoButtons(true); syncFocusLayout(true); await refreshAutoStatus(true);
      });
    } catch(e) { syncAutoButtons(false); setAlerts([`AutoTrade error: ${e.message||e}`]); }
  };
  /* ── Auto Start ── */
  let autoStartClickLock = false;
  const forceStartIfStaleDisabled = () => {
    const btn = root.querySelector("#autoStart");
    if (btn instanceof HTMLButtonElement && btn.disabled && !autoRunningState) {
      btn.disabled = false;
      btn.style.opacity = "1";
      if(!btn.classList.contains("is-loading")) btn.textContent = "▶ เริ่ม";
    }
  };
  root.querySelector("#autoStart").onclick = async () => {
    forceStartIfStaleDisabled();
    if (autoStartClickLock) return;
    autoStartClickLock = true;
    try { await startAutoFromChartTab(); }
    finally { setTimeout(() => { autoStartClickLock = false; }, 250); }
  };
  root.querySelector("#autoStart")?.addEventListener("click", async (ev) => {
    // Hard fallback: if page scripts interfere with onclick, still force start action.
    if (ev.defaultPrevented) return;
    const btn = ev.currentTarget;
    if (!(btn instanceof HTMLButtonElement)) return;
    if (btn.disabled && !autoRunningState) {
      btn.disabled = false;
      btn.style.opacity = "1";
    }
    if (btn.disabled) return;
    if (autoStartClickLock) return;
    autoStartClickLock = true;
    try { await startAutoFromChartTab(); }
    finally { setTimeout(() => { autoStartClickLock = false; }, 250); }
  });

  /* ── Auto Stop ── */
  root.querySelector("#autoStop").onclick = async () => {
    try {
      const btn = root.querySelector("#autoStop");
      await withBtnFeedback(btn, async () => {
        if(!autoSessionId) await refreshAutoStatus(true);
        const postStop = async (body) => { const r = await fetch(`${BASE}/autotrade/stop`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)}); return {r, d:await r.json()}; };
        let {r,d} = await postStop({sessionId:autoSessionId});
        if(d?.ignored && d?.reason==="SESSION_MISMATCH"){ await refreshAutoStatus(true); ({r,d}=await postStop({sessionId:autoSessionId})); }
        if(d?.ignored && d?.reason==="SESSION_MISMATCH"){ ({r,d}=await postStop({force:true})); }
        if(!r.ok) throw new Error(d?.detail||"หยุด AutoTrade ไม่สำเร็จ");
        autoSessionId=null; await clearPersistedAutotradeState();
        setAlerts(["หยุด AutoTrade แล้ว"]); syncAutoButtons(false); syncFocusLayout(false); forceUnlockAutoControls(); refreshAutoStatus(true);
      });
    } catch(e) { forceUnlockAutoControls(); setAlerts([`หยุด AutoTrade ไม่สำเร็จ: ${e.message||e}`]); }
  };

  /* ── Auto Reset ── */
  root.querySelector("#autoReset").onclick = async () => {
    try {
      const btn = root.querySelector("#autoReset");
      await withBtnFeedback(btn, async () => {
        if(!autoSessionId) await refreshAutoStatus(true);
        const postReset = async (body) => { const r = await fetch(`${BASE}/autotrade/reset`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)}); return {r, d:await r.json()}; };
        let {r,d} = await postReset({sessionId:autoSessionId});
        if(d?.ignored && d?.reason==="SESSION_MISMATCH"){ await refreshAutoStatus(true); ({r,d}=await postReset({sessionId:autoSessionId})); }
        if(d?.ignored && d?.reason==="SESSION_MISMATCH"){ ({r,d}=await postReset({force:true})); }
        if(!r.ok) throw new Error(d?.detail||"รีเซ็ตไม่สำเร็จ");
        autoSessionId=null; await clearPersistedAutotradeState();
        setAlerts(["รีเซ็ตเซสชัน AutoTrade แล้ว"]); syncAutoButtons(false); syncFocusLayout(false); forceUnlockAutoControls(); refreshAutoStatus(true);
      });
    } catch(e) { forceUnlockAutoControls(); setAlerts([`รีเซ็ตไม่สำเร็จ: ${e.message||e}`]); }
  };

  root.querySelector("#orphanCheckBtn")?.addEventListener("click", async () => {
    await refreshOrphanBox();
    setAlerts(["อัปเดตสถานะ Orphan LIVE แล้ว"]);
  });
  root.querySelector("#orphanCloseBtn")?.addEventListener("click", async () => {
    try {
      const r = await fetch(`${BASE}/autotrade/close-orphan`, { method:"POST" });
      const d = await r.json().catch(() => ({}));
      if(!r.ok) throw new Error(d?.detail || "ปิด Orphan ไม่สำเร็จ");
      await refreshOrphanBox();
      await refreshAutoStatus(true);
      setAlerts([`Close All LIVE เสร็จแล้ว (${d?.count||0} คู่)`]);
    } catch(e) {
      setAlerts([`Close All LIVE ไม่สำเร็จ: ${e.message||e}`]);
    }
  });
  root.querySelector("#orphanAdoptBtn")?.addEventListener("click", async () => {
    try {
      const r = await fetch(`${BASE}/autotrade/adopt-live`, { method:"POST" });
      const d = await r.json().catch(() => ({}));
      if(!r.ok || !d?.ok) throw new Error(d?.detail?.message || d?.detail || d?.reason || "Adopt ไม่สำเร็จ");
      autoSessionId = d?.sessionId || null;
      await persistAutotradeState({ sessionId:autoSessionId, symbol:(d?.config?.symbol||"").toUpperCase(), running:true, ts:Date.now() });
      await refreshOrphanBox();
      await refreshAutoStatus(true);
      setAlerts([`Adopt LIVE สำเร็จ: ${d?.config?.symbol||"-"}`]);
    } catch(e) {
      setAlerts([`Adopt LIVE ไม่สำเร็จ: ${e.message||e}`]);
    }
  });

  /* ── Risk Config ── */
  const refreshRisk = async () => {
    try {
      const r = await fetch(`${BASE}/risk-config`); const d = await r.json();
      root.querySelector("#riskKill").innerHTML = d.killSwitch ? '<span style="color:var(--short)">เปิด ⚠</span>' : '<span style="color:var(--long)">ปิด ✓</span>';
      root.querySelector("#riskNotional").textContent = `${d.maxNotionalUSDT} USDT`;
      root.querySelector("#riskLev").textContent = `${d.maxLeverage}x`;
      root.querySelector("#riskLoss").textContent = `${d.maxDailyLossUSDT} USDT`;
      root.querySelector("#riskPnl").textContent = `${d.dailyRealizedPnlUSDT??0} USDT`;
    } catch {}
  };
  root.querySelector("#riskRefresh").onclick = () => refreshRisk();

  /* ── Symbol change detection ── */
  let lastSymbol = symbol();

  let autoStatusPollTimer = null;
  const scheduleAutoStatusPoll = () => {
    if (autoStatusPollTimer) clearTimeout(autoStatusPollTimer);
    const interval = autoStatusPollIntervalMs();
    autoStatusPollTimer = setTimeout(async () => {
      const force = getCurrentTab() === "auto";
      await refreshAutoStatus(force);
      scheduleAutoStatusPoll();
    }, interval);
  };
  const scheduleHealthPoll = () => {
    if (healthPollTimer) clearTimeout(healthPollTimer);
    healthPollTimer = setTimeout(async () => {
      await refreshBackendHealth();
      scheduleHealthPoll();
    }, healthPollIntervalMs());
  };
  const scheduleSymbolPoll = () => {
    if (symbolPollTimer) clearTimeout(symbolPollTimer);
    symbolPollTimer = setTimeout(async () => {
      if (document.visibilityState === "visible") {
        const cur = symbol();
        if(cur !== lastSymbol){ lastSymbol=cur; root.querySelector("#sym").textContent=cur; refreshSignal(); refreshAutoStatus(true); }
      }
      scheduleSymbolPoll();
    }, symbolPollIntervalMs());
  };

  /* ── Boot ── */
  const boot = async () => {
    syncAutoButtons(false);
    const snap = await loadPersistedAutotradeState();
    if(snap?.running && snap.sessionId) autoSessionId = snap.sessionId;
    await refreshBackendHealth();
    if (backendOnline) refreshSignal();
    await refreshAutoStatus(true);
    await refreshOrphanBox();
    refreshRisk();
    scheduleAutoStatusPoll();
    scheduleHealthPoll();
    scheduleSymbolPoll();
    updatePerfModeUI();
  };
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      refreshBackendHealth();
      refreshAutoStatus(true);
      refreshOrphanBox();
    }
    scheduleAutoStatusPoll();
  });
  window.addEventListener("pageshow", (ev) => { if(ev.persisted) refreshAutoStatus(true); });
  root.querySelector("#perfEcoMode")?.addEventListener("change", (ev) => {
    perfEcoMode = !!ev.target.checked;
    savePerfMode(perfEcoMode);
    scheduleAutoStatusPoll();
    scheduleHealthPoll();
    scheduleSymbolPoll();
    setAlerts([perfEcoMode ? "เปิดโหมดประหยัดทรัพยากรแล้ว" : "ปิดโหมดประหยัดทรัพยากรแล้ว"]);
  });
  boot();
})();
