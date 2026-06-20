
const $ = (id) => document.getElementById(id);
const defaultCmuxUrl = "http://127.0.0.1:8030";
const MODE_KEY = "cmux_dashboard_cfg_mode";
const LEV_MIN_KEY = "cmux_dashboard_lev_min";
const LEV_MAX_KEY = "cmux_dashboard_lev_max";
let SERVER_MAX_LEVERAGE = 25;
const PAIR_LOCK_ENABLED_KEY = "cmux_dashboard_pair_lock_enabled";
const PAIR_LOCK_STREAK_KEY = "cmux_dashboard_pair_lock_streak";
const PAIR_LOCK_MINUTES_KEY = "cmux_dashboard_pair_lock_minutes";
const ui = {
  baseUrl: $("baseUrl"), svcStatus: $("svcStatus"), botStatus: $("botStatus"),
  publicIp: $("publicIp"), publishIpHeader: $("publishIpHeader"), winRate: $("winRate"), realizedPnl: $("realizedPnl"),
  winRateSub: $("winRateSub"), pnlSub: $("pnlSub"),
  levCapHint: $("levCapHint"), engineVer: $("engineVer"), engineBadge: $("engineBadge"),
  backendBanner: $("backendBanner"),
  log: $("log"), scanBoard: $("scanBoard"), entryPipeline: $("entryPipeline"),
  pipelineSkip: $("pipelineSkip"), openPositions: $("openPositions"),
  hermesKanban: $("hermesKanban"), hermesKanbanMeta: $("hermesKanbanMeta"), hermesMission: $("hermesMission"),
  hermesSupervisorReview: $("hermesSupervisorReview"),
  botToggle: $("botToggle"), serviceToggle: $("serviceToggle"),
  learningLastTrain: $("learningLastTrain"), learningSymbols: $("learningSymbols"),
  learningPromoted: $("learningPromoted"), learningThreshold: $("learningThreshold"),
  learningRows: $("learningRows"), learningApplied: $("learningApplied"),
  learningPromotedList: $("learningPromotedList"), learningAppliedBadge: $("learningAppliedBadge"),
};
if (ui.baseUrl && !ui.baseUrl.value) ui.baseUrl.value = defaultCmuxUrl;
if (ui.scanBoard && !ui.scanBoard.innerHTML.trim()) {
  ui.scanBoard.innerHTML = `<div class="log-line" style="color:var(--muted)">กำลังโหลดสถานะ scan…</div>`;
}
if (ui.entryPipeline && !ui.entryPipeline.innerHTML.trim()) {
  ui.entryPipeline.innerHTML = `<span class="gate-chip" style="color:var(--muted)">idle</span>`;
}

document.querySelectorAll(".vo-tab").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".vo-tab").forEach((t) => t.classList.remove("active"));
    document.querySelectorAll(".panel-view").forEach((v) => v.classList.remove("active"));
    btn.classList.add("active");
    const id = btn.dataset.tab;
    const view = $("view-" + id);
    if (view) view.classList.add("active");
  });
});

if ($("cfgMode")) {
  const savedMode = localStorage.getItem(MODE_KEY);
  if (savedMode === "LIVE" || savedMode === "PAPER") $("cfgMode").value = savedMode;
  $("cfgMode").addEventListener("change", () => localStorage.setItem(MODE_KEY, $("cfgMode").value || "PAPER"));
}
if ($("cfgLevMin") && $("cfgLevMax")) {
  const sMin = Number(localStorage.getItem(LEV_MIN_KEY) || "");
  const sMax = Number(localStorage.getItem(LEV_MAX_KEY) || "");
  if (Number.isFinite(sMin) && sMin >= 1) $("cfgLevMin").value = String(Math.floor(sMin));
  if (Number.isFinite(sMax) && sMax >= 1) $("cfgLevMax").value = String(Math.floor(sMax));
}
if ($("cfgPairLockEnabled")) {
  const saved = localStorage.getItem(PAIR_LOCK_ENABLED_KEY);
  if (saved === "true" || saved === "false") $("cfgPairLockEnabled").checked = saved === "true";
  $("cfgPairLockEnabled").addEventListener("change", async () => {
    localStorage.setItem(PAIR_LOCK_ENABLED_KEY, $("cfgPairLockEnabled").checked ? "true" : "false");
    try {
      const rs = await req("/bot/config", "POST", { pairLockEnabled: !!$("cfgPairLockEnabled").checked });
      if (rs?.bot?.ok === false && rs.bot.reason !== "NO_ACTIVE_CONFIG") throw new Error(rs.bot.reason || "failed");
      safeAppendLog(`PairLock: ${$("cfgPairLockEnabled").checked}`, "ok");
      await refreshStatus();
    } catch (e) { showToast("err", String(e.message || e).slice(0, 120)); }
  });
}
if ($("cfgPairLockStreak")) {
  const saved = localStorage.getItem(PAIR_LOCK_STREAK_KEY);
  if (saved && !Number.isNaN(Number(saved))) $("cfgPairLockStreak").value = saved;
  $("cfgPairLockStreak").addEventListener("change", async () => {
    localStorage.setItem(PAIR_LOCK_STREAK_KEY, $("cfgPairLockStreak").value || "2");
    try {
      const rs = await req("/bot/config", "POST", { pairLockLossStreak: Number($("cfgPairLockStreak").value || 2) });
      if (rs?.bot?.ok === false && rs.bot.reason !== "NO_ACTIVE_CONFIG") throw new Error(rs.bot.reason || "failed");
      await refreshStatus();
    } catch (e) { showToast("err", String(e.message || e).slice(0, 120)); }
  });
}
if ($("cfgPairLockMinutes")) {
  const saved = localStorage.getItem(PAIR_LOCK_MINUTES_KEY);
  if (saved && !Number.isNaN(Number(saved))) $("cfgPairLockMinutes").value = saved;
  $("cfgPairLockMinutes").addEventListener("change", async () => {
    localStorage.setItem(PAIR_LOCK_MINUTES_KEY, $("cfgPairLockMinutes").value || "45");
    try {
      const rs = await req("/bot/config", "POST", { pairLockMinutes: Number($("cfgPairLockMinutes").value || 45) });
      if (rs?.bot?.ok === false && rs.bot.reason !== "NO_ACTIVE_CONFIG") throw new Error(rs.bot.reason || "failed");
      await refreshStatus();
    } catch (e) { showToast("err", String(e.message || e).slice(0, 120)); }
  });
}

let activeActionCount = 0, actionLockUntil = 0, levUpdateInFlight = false, toastId = 0, lastSeenPublicIp = null;
let kpiSticky = { liveWinAll: 0, livePnlAll: 0, liveWinsToday: 0, liveLossToday: 0, liveWinToday: 0, livePnlToday: 0, lastTradeAt: 0 };
let learningAutoTrainInFlight = false;
let learningLastAutoTrainAt = 0;
let openPositionsCache = { key: "", html: "", updatedAt: 0 };
let scanTickerCache = { key: "", html: "", updatedAt: 0, durationMs: 56000 };
let lastGoodStatus = null;
let lastRichBotStatus = null;
let statusRefreshInFlight = false;
let lastStatusToastAt = 0;
const num0 = (v, d = 0) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
};
const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));

const CONTROL_BUTTON_IDS = ["btnRefresh", "serviceToggle", "botToggle", "learningTrainNow", "learningRefresh", "loadProPreset"];

function setGlobalControlLock(locked) {
  $("busyOverlay")?.classList.toggle("show", !!locked);
  for (const id of CONTROL_BUTTON_IDS) {
    const btn = $(id);
    if (!btn) continue;
    if (locked && btn.dataset.busy !== "1") btn.disabled = true;
    else if (btn.dataset.busy !== "1") btn.disabled = false;
  }
}
function isControlLocked() { return activeActionCount > 0 || Date.now() < actionLockUntil; }

function pillSignal(sig) {
  const s = String(sig || "WAIT").toUpperCase();
  const cls = s === "LONG" ? "pill-long" : s === "SHORT" ? "pill-short" : "pill-wait";
  return `<span class="pill ${cls}">${s}</span>`;
}

function scanTickSignal(sig) {
  const s = String(sig || "WAIT").toUpperCase();
  const cls = s === "LONG" ? "long" : s === "SHORT" ? "short" : "wait";
  return `<span class="scan-tick-side ${cls}">${esc(s)}</span>`;
}

function compactAge(ts) {
  const n = Number(ts || 0);
  if (!Number.isFinite(n) || n <= 0) return "—";
  const sec = Math.max(0, Math.floor(Date.now() / 1000 - n));
  if (sec < 60) return `${sec}s`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m`;
  return `${Math.floor(min / 60)}h`;
}

function scanTickerPayload(bot) {
  const rows = Array.isArray(bot?.scanBoard) ? bot.scanBoard.slice(0, 8) : [];
  if (!rows.length) {
    const cfg = bot?.config || {};
    const cfgSymbol = String(cfg?.symbol || "").toUpperCase();
    const scanMode = !!cfg?.marketScan || cfgSymbol === "AUTO" || cfgSymbol === "SCAN";
    const decision = bot?.lastDecision || {};
    const decisionSymbol = String(decision?.symbol || cfgSymbol || "—").toUpperCase();
    const decisionSignal = String(decision?.signal || "").toUpperCase();
    const decisionConf = Number(decision?.confidence || 0);
    const skipCode = String(bot?.lastSkip?.code || "").toLowerCase();
    let text = "รอ market scan · Hermes กำลังเตรียมข้อมูล";
    if (!bot?.running) {
      text = "บอทหยุดทำงาน";
    } else if (!scanMode && decisionSignal) {
      const confText = Number.isFinite(decisionConf) && decisionConf > 0 ? ` c=${decisionConf.toFixed(2)}` : "";
      const waitText = skipCode === "signal_wait" || decisionSignal === "WAIT" ? " · รอสัญญาณ LONG/SHORT ชัดเจน" : "";
      text = `${decisionSymbol} ${decisionSignal}${confText}${waitText}`;
    } else if (!scanMode) {
      text = `${decisionSymbol} · วิเคราะห์เหรียญหลัก`;
    }
    return {
      key: `empty:${text}`,
      html: `<span class="scan-tick-item">${esc(text)}</span>`,
      durationMs: 42000,
    };
  }
  const parts = rows.map((row, idx) => {
    const symbol = String(row?.symbol || "—").toUpperCase();
    const side = String(row?.signal || row?.side || row?.decision || row?.action || "WAIT").toUpperCase();
    const conf = Number(row?.confidence || row?.conf || 0);
    const score = Number(row?.score || 0);
    const reason = String(row?.rejectReason || row?.reason || row?.status || row?.note || "").slice(0, 24);
    return {
      key: [idx + 1, symbol, side, conf.toFixed(2), score.toFixed(1), reason].join(":"),
      html: `<span class="scan-tick-item">#${idx + 1} <b>${esc(symbol)}</b> ${scanTickSignal(side)} c=${conf.toFixed(2)} s=${score.toFixed(1)} ${esc(reason)}</span>`,
    };
  });
  const textLen = parts.map((x) => x.key).join(" ").length;
  const durationMs = Math.max(48000, Math.min(140000, Math.round(textLen * 190)));
  return {
    key: parts.map((x) => x.key).join("|"),
    html: parts.map((x) => x.html).join(""),
    durationMs,
  };
}

function scanTickerLoopHtml(html) {
  const content = String(html || `<span class="scan-tick-item">รอ market scan</span>`);
  return `<span class="scan-tick-loop">${content}</span><span class="scan-tick-loop" aria-hidden="true">${content}</span>`;
}

function updateSymbolOptions(bot) {
  const sel = $("cfgSymbol");
  if (!sel) return;
  const current = String(sel.value || "AUTO").toUpperCase();
  const set = new Set(["AUTO"]);
  const add = (v) => {
    const s = String(v || "").toUpperCase().trim();
    if (s && s !== "AUTO" && s !== "SCAN" && /^[A-Z0-9]{3,20}USDT$/.test(s)) set.add(s);
  };
  add(bot?.config?.symbol);
  add(bot?.config?.primarySymbol);
  for (const r of (bot?.scanBoard || [])) add(r?.symbol);
  for (const r of (bot?.openLivePositions || [])) add(r?.symbol);
  for (const s of ["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT"]) set.add(s);
  sel.innerHTML = Array.from(set).map((s) => `<option value="${s}">${s}</option>`).join("");
  sel.value = set.has(current) ? current : "AUTO";
}

function hasHermesOfficeState(bot) {
  const agents = bot?.hermesAgents?.agents;
  return !!(agents && typeof agents === "object" && Object.keys(agents).length);
}

function withLastRichBotStatus(data) {
  if (!data || typeof data !== "object") return data;
  const bot = data.bot && typeof data.bot === "object" ? data.bot : {};
  if (hasHermesOfficeState(bot)) {
    lastRichBotStatus = { ...bot };
    return data;
  }
  if (!lastRichBotStatus || !hasHermesOfficeState(lastRichBotStatus)) return data;
  const mergedBot = {
    ...lastRichBotStatus,
    ...bot,
    hermesAgents: lastRichBotStatus.hermesAgents,
    hermesSupervisorReview: bot.hermesSupervisorReview || lastRichBotStatus.hermesSupervisorReview,
    scanBoard: Array.isArray(bot.scanBoard) && bot.scanBoard.length ? bot.scanBoard : lastRichBotStatus.scanBoard,
    openLivePositions: Array.isArray(bot.openLivePositions) && bot.openLivePositions.length
      ? bot.openLivePositions
      : lastRichBotStatus.openLivePositions,
    log: Array.isArray(bot.log) && bot.log.length ? bot.log : lastRichBotStatus.log,
    lastDecision: bot.lastDecision && typeof bot.lastDecision === "object" && Object.keys(bot.lastDecision).length
      ? bot.lastDecision
      : lastRichBotStatus.lastDecision,
  };
  return { ...data, bot: mergedBot, dashboardOfficeCached: true };
}

function renderHermesKanban(bot) {
  if (!ui.hermesKanban) return;
  const state = bot?.hermesAgents || {};
  const agents = state.agents && typeof state.agents === "object" ? state.agents : {};
  const agentSkin = {
    hermes_supervisor: { hair: "#111827", color: "#f43f5e", skin: "#f0c9a6", tool: "S", short: "Supervisor", role: "คุมจังหวะและ policy ของทีม" },
    market_analyst: { hair: "#1e3a5f", color: "#3b82f6", skin: "#f5d0b5", tool: "🔎", short: "Scan Lead", role: "สแกนตลาดหาโอกาส" },
    data_quality_guard: { hair: "#0e7490", color: "#22d3ee", skin: "#e8c4a8", tool: "✓", short: "Liquidity QC", role: "ตรวจคุณภาพข้อมูล" },
    news_sentiment_guard: { hair: "#713f12", color: "#60a5fa", skin: "#d4a574", tool: "📰", short: "News Pulse", role: "วิเคราะห์ sentiment" },
    risk_manager: { hair: "#14532d", color: "#22c55e", skin: "#f0c9a6", tool: "🛡", short: "Risk Gate", role: "กรองความเสี่ยง" },
    portfolio_manager: { hair: "#166534", color: "#4ade80", skin: "#e8b896", tool: "📊", short: "Size Desk", role: "จัดสรรขนาดพอร์ต" },
    position_guardian: { hair: "#064e3b", color: "#86efac", skin: "#f7c59f", tool: "👁", short: "Pos Watch", role: "เฝ้า position ค้าง" },
    strategy_builder: { hair: "#581c87", color: "#a78bfa", skin: "#f5d0b5", tool: "🧩", short: "Plan Maker", role: "สร้างแผนเข้าเทรด" },
    backtest_agent: { hair: "#4c1d95", color: "#c4b5fd", skin: "#d4a574", tool: "🧪", short: "WF Tester", role: "ทดสอบ walk-forward" },
    execution_agent: { hair: "#92400e", color: "#f59e0b", skin: "#f0c9a6", tool: "⚡", short: "Order Exec", role: "ส่งคำสั่งซื้อขาย" },
    reflection_agent: { hair: "#78350f", color: "#fbbf24", skin: "#e8c4a8", tool: "💭", short: "Trade Review", role: "ทบทวนผลเทรด" },
    memory_agent: { hair: "#374151", color: "#34d399", skin: "#f7c59f", tool: "📚", short: "Lesson Bank", role: "เก็บบทเรียน" },
  };
  const roomProps = {
    market: ["shelf", "map", "plant"],
    risk: ["clock", "plant", "shelf"],
    strategy: ["shelf", "clock", "plant"],
    ops: ["map", "clock", "plant"],
  };
  const orderedIds = [
    "hermes_supervisor",
    "market_analyst",
    "risk_manager",
    "portfolio_manager",
    "position_guardian",
    "strategy_builder",
    "backtest_agent",
    "execution_agent",
    "reflection_agent",
    "memory_agent",
  ];
  const rawAgentState = (agent) => {
    const s = String(agent?.state || "todo").toLowerCase();
    return ["todo", "doing", "done", "blocked"].includes(s) ? s : "todo";
  };
  const agentDisplayState = (agent) => {
    const raw = rawAgentState(agent);
    const runs = Number(agent?.runs || 0);
    const updatedAt = Number(agent?.updatedAt || 0);
    const ageSec = updatedAt > 0 ? (Date.now() / 1000) - updatedAt : Infinity;
    if (raw === "blocked") return { cls: "blocked", badge: "BLOCKED", label: "ติดเงื่อนไข" };
    if (raw === "doing" || ageSec <= 15) return { cls: "active", badge: "ACTIVE", label: "กำลังทำ" };
    if (raw === "done") return { cls: "done", badge: "DONE", label: "งานล่าสุดผ่าน" };
    if (runs > 0) return { cls: "idle", badge: "IDLE", label: "รอรอบถัดไป" };
    return { cls: "todo", badge: "TODO", label: "รอเริ่ม" };
  };
  const actionText = (agentId, agent) => {
    const display = agentDisplayState(agent);
    const runs = Number(agent?.runs || 0);
    const isFreshTodo = display.cls === "todo";
    const skin = agentSkin[agentId] || {};
    const fallbackRole = skin.role || agent.role || "รอรอบทำงาน";
    const action = isFreshTodo ? fallbackRole : (agent.lastAction || fallbackRole);
    const idleProgress = display.cls === "idle" && runs > 0 ? ` · เคยทำ ${runs} รอบ` : "";
    const reason = !isFreshTodo && agent.lastReason ? ` · ${agent.lastReason}` : "";
    return String(action + idleProgress + reason).slice(0, 62);
  };
  const renderAgentCard = (agentId, idx) => {
    const agent = agents[agentId] || {};
    const skin = agentSkin[agentId] || { hair: "#334155", color: "#38bdf8", skin: "#f7c59f", tool: "•", short: agentId };
    const display = agentDisplayState(agent);
    const name = skin.short || agent.name || agentId;
    const runs = Number(agent?.runs || 0);
    const runBadge = runs > 0 ? `<span class="trading-runs">×${runs}</span>` : "";
    const delay = ((idx % 5) * 0.42).toFixed(2);
    return `
      <div class="trading-agent ${display.cls}" style="--agent-hair:${skin.hair};--agent-color:${skin.color};--skin-tone:${skin.skin || "#f7c59f"};--agent-delay:${delay}s">
        <span class="trading-status-badge" title="${esc(name)} ${esc(display.badge)}">${esc(display.badge)}</span>
        <span class="trading-state"></span>
        <div class="trading-desk" aria-hidden="true">
          <div class="trading-monitor"><div class="trading-screen"></div></div>
          <div class="trading-chair"></div>
        </div>
        <div class="trading-avatar" aria-hidden="true">
          <div class="trading-hair"></div>
          <div class="trading-head">
            <div class="trading-eye left"></div>
            <div class="trading-eye right"></div>
            <div class="trading-blush left"></div>
            <div class="trading-blush right"></div>
            <div class="trading-mouth"></div>
          </div>
          <div class="trading-body"></div>
          <div class="trading-leg left"></div>
          <div class="trading-leg right"></div>
          <div class="trading-arm left"></div>
          <div class="trading-arm right"></div>
          <div class="trading-tool">${skin.tool}</div>
        </div>
        <div class="trading-name">${esc(name)}${runBadge}</div>
        <div class="trading-action"><b>${esc(display.label)}</b>${esc(actionText(agentId, agent))}</div>
      </div>`;
  };
  const rooms = [
    { key: "control", title: "Control", color: "#f43f5e", ids: ["hermes_supervisor"] },
    { key: "market", title: "Intel Core", color: "#2563eb", ids: ["market_analyst", "data_quality_guard", "news_sentiment_guard"] },
    { key: "risk", title: "Risk Core", color: "#16a34a", ids: ["risk_manager", "portfolio_manager", "position_guardian"] },
    { key: "strategy", title: "Strategy Lab", color: "#7c3aed", ids: ["strategy_builder", "backtest_agent"] },
    { key: "ops", title: "Trade Ops", color: "#d97706", ids: ["execution_agent", "reflection_agent", "memory_agent"] },
  ];
  const flatIds = rooms.flatMap((r) => r.ids);
  const counts = flatIds.reduce((acc, id) => {
    const display = agentDisplayState(agents[id] || {});
    acc.total += 1;
    acc[display.cls] = (acc[display.cls] || 0) + 1;
    return acc;
  }, { total: 0 });
  const openPositions = Array.isArray(bot?.openLivePositions) ? bot.openLivePositions.length : 0;
  const activeTasks = (counts.active || 0) + openPositions;
  const statusText = bot?.running ? "All Systems Operational" : "Standby";
  const winsToday = num0(kpiSticky.liveWinsToday);
  const lossesToday = num0(kpiSticky.liveLossToday);
  const tradesToday = winsToday + lossesToday;
  const winToday = tradesToday > 0 ? num0(kpiSticky.liveWinToday).toFixed(0) : "—";
  const pnlToday = num0(kpiSticky.livePnlToday);
  const pnlTone = pnlToday < 0 ? "bad" : pnlToday > 0 ? "good" : "";
  const roomHtml = rooms.map((room) => `
    <section class="office-room room-${room.key}" style="--room-color:${room.color}">
      <div class="room-title">${esc(room.title)}</div>
      ${(roomProps[room.key] || []).map((p) => `<div class="room-prop ${p}" aria-hidden="true"></div>`).join("")}
      <div class="room-agents">
        ${room.ids.map((id) => renderAgentCard(id, flatIds.indexOf(id))).join("")}
      </div>
    </section>
  `).join("");
  const engine = state.engine && typeof state.engine === "object" ? state.engine : {};
  const mission = engine.mission || "analyze → plan → learn → hypothesize → reflect → optimize";
  const existingTicker = ui.hermesKanban.querySelector(".trading-scan-ticker");
  ui.hermesKanban.innerHTML = `
    <div class="office-topbar">
      <div class="office-brand"><b>Hermes Trading Lab Virtual Office</b><span>${esc(counts.total)} AI agents working together · ${esc(mission)}</span></div>
      <div class="office-status"><b><span class="dot"></span>${esc(statusText)}</b><span id="hermesKanbanMeta">cycle ${Number(state.cycle || 0) || "—"}</span></div>
      <div class="office-daily-kpis" title="Daily live trade performance">
        <div class="office-daily-kpi"><b>${esc(winToday)}${tradesToday > 0 ? "%" : ""}</b><span>Win วันนี้ · ${winsToday}/${lossesToday}</span></div>
        <div class="office-daily-kpi ${pnlTone}"><b>${pnlToday >= 0 ? "+" : ""}${pnlToday.toFixed(2)}</b><span>PnL วันนี้ USDT</span></div>
      </div>
    </div>
    <div class="office-main">${roomHtml}</div>
    <div class="trading-scan-ticker"><span class="trading-scan-track"></span></div>`;
  const freshTicker = ui.hermesKanban.querySelector(".trading-scan-ticker");
  if (existingTicker && freshTicker) freshTicker.replaceWith(existingTicker);
  updateOfficeSidebar(bot, counts, activeTasks);
  const ticker = scanTickerPayload(bot);
  const tickerTrack = ui.hermesKanban.querySelector(".trading-scan-track");
  const tickerAge = Date.now() - Number(scanTickerCache.updatedAt || 0);
  const shouldUpdateTicker = ticker.key !== scanTickerCache.key && tickerAge >= Number(scanTickerCache.durationMs || 56000);
  if (tickerTrack && (!scanTickerCache.key || shouldUpdateTicker || !tickerTrack.innerHTML.trim())) {
    tickerTrack.innerHTML = scanTickerLoopHtml(ticker.html);
    tickerTrack.style.setProperty("--ticker-duration", `${Math.round(ticker.durationMs / 1000)}s`);
    scanTickerCache = { key: ticker.key, html: ticker.html, updatedAt: Date.now(), durationMs: ticker.durationMs };
  }
}

function updateOfficeSidebar(bot, counts, activeTasks) {
  const panel = $("officeStatusPanel");
  const title = $("officeStatusTitle");
  const agentsOnline = $("officeAgentsOnline");
  const activeEl = $("officeActiveTasks");
  const activityEl = $("officeTodayActivity");
  if (!panel || !title) return;
  const total = Number(counts?.total || 0);
  const online = total - Number(counts?.blocked || 0);
  const running = !!bot?.running;
  const pnlToday = num0(kpiSticky.livePnlToday);
  const winsToday = num0(kpiSticky.liveWinsToday);
  const lossesToday = num0(kpiSticky.liveLossToday);
  const activityPct = winsToday + lossesToday > 0
    ? `${((winsToday / (winsToday + lossesToday)) * 100).toFixed(0)}% win`
    : pnlToday !== 0 ? `${pnlToday >= 0 ? "+" : ""}${pnlToday.toFixed(2)} USDT` : "—";
  if (agentsOnline) agentsOnline.textContent = `${online}/${total}`;
  if (activeEl) activeEl.textContent = String(activeTasks ?? 0);
  if (activityEl) activityEl.textContent = activityPct;
  panel.classList.remove("standby", "error");
  if (running) {
    title.textContent = "OPEN & ACTIVE";
  } else if (counts?.blocked) {
    panel.classList.add("error");
    title.textContent = "BLOCKED";
  } else {
    panel.classList.add("standby");
    title.textContent = "STANDBY";
  }
}

function renderHermesSupervisor(bot) {
  if (!ui.hermesSupervisorReview) return;
  const review = bot?.hermesSupervisorReview && typeof bot.hermesSupervisorReview === "object"
    ? bot.hermesSupervisorReview
    : null;
  if (!review) {
    ui.hermesSupervisorReview.innerHTML = `
      <div class="supervisor-head"><span>Hermes Supervisor</span><span class="supervisor-severity">WAIT</span></div>
      <div class="supervisor-summary">รอข้อมูล review จาก Hermes หลัก</div>`;
    return;
  }
  const severity = String(review.severity || "ok").toLowerCase();
  const issues = Array.isArray(review.issues) ? review.issues : [];
  const handoff = Array.isArray(review.cmuxHandoff) ? review.cmuxHandoff : [];
  const issueHtml = issues.length
    ? issues.slice(0, 4).map((x) => `
      <div class="supervisor-item">
        <div class="supervisor-agent">${esc(x.agent || "hermes")}</div>
        <div>
          <div class="supervisor-title">${esc(x.title || "Review issue")}</div>
          <div class="supervisor-detail">${esc(x.detail || x.suggestion || "-")}</div>
        </div>
      </div>`).join("")
    : `<div class="supervisor-item"><div class="supervisor-agent">Hermes</div><div><div class="supervisor-title">Agent team healthy</div><div class="supervisor-detail">ยังไม่พบจุดที่ต้องให้ Cmux แก้โค้ด</div></div></div>`;
  const cmuxHtml = handoff.length
    ? `<div class="supervisor-cmux"><b>Cmux handoff:</b> ${esc(handoff.slice(0, 2).map((x) => x.task || x.agent || "-").join(" · "))}</div>`
    : "";
  const copyHtml = handoff.length
    ? `<button id="supervisorCopyTask" class="supervisor-copy" type="button">Copy Cmux Task</button>`
    : "";
  ui.hermesSupervisorReview.innerHTML = `
    <div class="supervisor-head">
      <span>Hermes Supervisor</span>
      <span class="supervisor-severity ${esc(severity)}">${esc(severity.toUpperCase())}</span>
    </div>
    <div class="supervisor-summary">${esc(review.summary || "ทีม agent ปกติ")}</div>
    ${issueHtml}
    ${cmuxHtml}
    ${copyHtml}`;
  const copyBtn = $("supervisorCopyTask");
  if (copyBtn) {
    copyBtn.addEventListener("click", async () => {
      const prompt = buildHermesSupervisorTaskPrompt(review, bot);
      try {
        await copyText(prompt);
        showToast("ok", "Copied Hermes task for Cmux");
      } catch (e) {
        showToast("err", "Copy failed — select text manually");
      }
    });
  }
}

function buildHermesSupervisorTaskPrompt(review, bot) {
  const handoff = Array.isArray(review?.cmuxHandoff) ? review.cmuxHandoff : [];
  const issues = Array.isArray(review?.issues) ? review.issues : [];
  const runs = review?.agentRuns && typeof review.agentRuns === "object" ? review.agentRuns : {};
  const lines = [
    "Hermes Supervisor Review task",
    "",
    `Severity: ${review?.severity || "unknown"}`,
    `Summary: ${review?.summary || "-"}`,
    `ReviewedAt: ${review?.reviewedAt || "-"}`,
    "",
    "Cmux tasks:",
    ...(handoff.length ? handoff.map((x, i) => `${i + 1}. [${x.severity || "n/a"}] ${x.agent || "hermes"} — ${x.task || "-"}`) : ["- ไม่มี cmux handoff"]),
    "",
    "Issues:",
    ...(issues.length ? issues.map((x, i) => `${i + 1}. [${x.severity || "n/a"}] ${x.agent || "hermes"} — ${x.title || "-"} | ${x.detail || "-"} | suggestion: ${x.suggestion || "-"}`) : ["- ไม่มี issue"]),
    "",
    `Agent runs: ${JSON.stringify(runs)}`,
    `Bot running: ${!!bot?.running}`,
    `Execution mode: ${bot?.config?.executionMode || bot?.activePosition?.mode || "-"}`,
    "",
    "Please use GitNexus impact analysis first, then fix the code or structure safely, add/update tests, restart Hermes if needed, and summarize changed files.",
  ];
  return lines.join("\n");
}

async function copyText(text) {
  if (navigator.clipboard && window.isSecureContext) {
    await navigator.clipboard.writeText(text);
    return;
  }
  const ta = document.createElement("textarea");
  ta.value = text;
  ta.setAttribute("readonly", "");
  ta.style.position = "fixed";
  ta.style.left = "-9999px";
  document.body.appendChild(ta);
  ta.select();
  const ok = document.execCommand("copy");
  ta.remove();
  if (!ok) throw new Error("copy failed");
}

function normalizedLeverageRange() {
  const cap = 25;
  let min0 = Math.max(1, Math.floor(Number($("cfgLevMin")?.value || 1)));
  let max0 = Math.max(1, Math.floor(Number($("cfgLevMax")?.value || cap)));
  min0 = Math.min(min0, cap);
  max0 = Math.min(Math.max(max0, min0), cap);
  if ($("cfgLevMin")) $("cfgLevMin").value = String(min0);
  if ($("cfgLevMax")) $("cfgLevMax").value = String(max0);
  localStorage.setItem(LEV_MIN_KEY, String(min0));
  localStorage.setItem(LEV_MAX_KEY, String(max0));
  return { min: min0, max: max0 };
}

async function applyLeverageConfigIfRunning() {
  if (levUpdateInFlight) return;
  try {
    levUpdateInFlight = true;
    const rs = await req("/status");
    const lev = normalizedLeverageRange();
    if (!rs?.bot?.running) return;
    const cfgRs = await req("/bot/config", "POST", { leverageMin: lev.min, leverageMax: lev.max, leverage: Math.min(lev.max, Math.max(lev.min, Number($("cfgLevMax")?.value || lev.max))), leverageAutoEnabled: true, adaptiveLeverageEnabled: true, adaptiveLeverageMax: lev.max });
    if (cfgRs?.bot?.ok === false) {
      const e = String(cfgRs?.bot?.error || cfgRs?.bot?.reason || "");
      // Some backend builds do not support /autotrade/config.
      // Keep new values in UI and apply on next Start payload.
      if (e.includes("HTTP 404")) {
        showToast("info", `Lev max saved x${lev.max} (apply on next Start)`);
        return;
      }
      throw new Error(e || "Leverage update failed");
    }
    showToast("ok", `Lev max x${lev.max}`);
    await refreshStatus();
  } catch (e) { showToast("err", String(e.message || e).slice(0, 100)); }
  finally { levUpdateInFlight = false; }
}

function cmuxUnreachableHint(base) {
  return `ไม่เชื่อมต่อ cmux (${base}) — รัน: cd backend && python cmux_service.py serve`;
}

function reqTimeoutByPath(path, fallbackMs = 15000) {
  const p = String(path || "");
  if (p === "/status/quick") return 12000;
  if (p === "/status") return 18000;
  if (p.startsWith("/bot/start") || p.startsWith("/service/start")) return 45000;
  return fallbackMs;
}

async function req(path, method = "GET", payload = null, timeoutMs = 15000) {
  const primary = (ui.baseUrl.value || "").replace(/\/$/, "") || defaultCmuxUrl;
  const fallback = defaultCmuxUrl.replace(/\/$/, "");
  const targets = primary === fallback ? [primary] : [primary, fallback];
  const effectiveTimeout = reqTimeoutByPath(path, timeoutMs);
  let lastErr = null;
  for (let i = 0; i < targets.length; i++) {
    const base = targets[i];
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), effectiveTimeout);
    try {
      const res = await fetch(base + path, {
        method,
        signal: ctrl.signal,
        headers: { "Content-Type": "application/json" },
        body: payload ? JSON.stringify(payload) : undefined,
      });
      const txt = await res.text();
      const data = txt ? JSON.parse(txt) : { ok: res.ok };
      if (!res.ok || data?.ok === false) {
        throw new Error(humanizeErr(data?.error || data?.detail || `HTTP ${res.status}`));
      }
      if (ui.baseUrl.value !== base) ui.baseUrl.value = base;
      return data;
    } catch (e) {
      if (e?.name === "AbortError") lastErr = new Error(`cmux timeout (${base}) — ${cmuxUnreachableHint(base)}`);
      else if (!e?.message || e.message === "Failed to fetch") lastErr = new Error(cmuxUnreachableHint(base));
      else lastErr = e;
      if (i < targets.length - 1) await new Promise((r) => setTimeout(r, 300));
    } finally {
      clearTimeout(timer);
    }
  }
  throw lastErr || new Error(cmuxUnreachableHint(primary));
}

function humanizeErr(detail) {
  const s = String(detail || "");
  if (s.includes("-4411") || s.includes("agreement contract fapi")) {
    return "ต้องยอมรับข้อตกลง Binance Futures ก่อน LIVE";
  }
  if (s.includes("-2015") || s.includes("Invalid API-key, IP, or permissions")) {
    return "API-key / IP / permission ของ Binance Futures ยังไม่ผ่าน";
  }
  return s;
}

async function precheckLiveBeforeStart(payload) {
  if (String(payload?.executionMode || "PAPER").toUpperCase() !== "LIVE") return;
  let sym = String(payload?.symbol || "BTCUSDT").toUpperCase();
  if (!sym || sym === "AUTO" || sym === "SCAN") sym = "BTCUSDT";
  try {
    const rs = await req(`/bot/precheck-live?symbol=${encodeURIComponent(sym)}`);
    const pre = rs?.precheck || {};
    // Hard block only when server explicitly confirms agreement is required.
    if (pre.agreementRequired === true) {
      throw new Error("LIVE pre-check ไม่ผ่าน — ยอมรับข้อตกลง Binance ก่อน");
    }
    // If precheck endpoint is unavailable/404, continue start (server has its own fallback).
    if (pre.ok === false) {
      showToast("info", "LIVE pre-check ไม่สมบูรณ์ — จะเริ่มต่อและให้ระบบตรวจซ้ำ");
    }
  } catch (e) {
    const msg = String(e?.message || e);
    if (msg.includes("LIVE pre-check ไม่ผ่าน")) throw e;
    showToast("info", "ข้าม pre-check ชั่วคราว แล้วเริ่ม LIVE ต่อ");
  }
}

function showToast(type, message) {
  const root = $("toasts");
  if (!root) return;
  const el = document.createElement("div");
  el.className = `toast ${type}`;
  el.textContent = message;
  root.appendChild(el);
  while (root.children.length > 3) root.removeChild(root.firstChild);
  setTimeout(() => el.remove(), 2400);
}

function safeAppendLog(message, type = "info") {
  if (!ui.log) return;
  const color = type === "ok" ? "var(--good)" : type === "err" ? "var(--bad)" : "var(--muted)";
  ui.log.innerHTML = `<div class="log-line" style="color:${color}">[${new Date().toLocaleTimeString()}] ${esc(message)}</div>` + ui.log.innerHTML;
}

async function withBusy(button, fn) {
  if (!button || button.dataset.busy === "1" || isControlLocked()) return;
  const oldText = button.textContent;
  button.dataset.busy = "1";
  button.disabled = true;
  button.classList.add("busy");
  activeActionCount++;
  setGlobalControlLock(true);
  try {
    return await fn();
  } catch (e) {
    showToast("err", String(e.message || e).slice(0, 140));
    throw e;
  } finally {
    button.dataset.busy = "0";
    button.disabled = false;
    button.classList.remove("busy");
    button.textContent = oldText;
    activeActionCount = Math.max(0, activeActionCount - 1);
    if (!activeActionCount) setGlobalControlLock(false);
  }
}

function statusHtml(color, text) {
  return `<span class="dot" style="background:${color}"></span>${text}`;
}

function positionRenderKey(items) {
  return items.map((x) => [
    String(x.symbol || "").toUpperCase(),
    String(x.side || "").toUpperCase(),
    Number(x.qty || 0).toFixed(6),
    Number(x.leverage || 0).toFixed(0),
    Number(x.unRealizedProfit || 0).toFixed(6),
    Number(x.profitLockUsdt || 0).toFixed(6),
    Number(x.peakUnrealizedPnl || 0).toFixed(6),
    x.profitLockArmed ? "1" : "0",
  ].join("|")).join(";");
}

function renderOpenPositions(opens, liveCtx) {
  if (!Array.isArray(opens) || !ui.openPositions) return;
  const key = positionRenderKey(opens);
  if (key && key === openPositionsCache.key) return;
  const live = liveCtx || {};
  const liveSide = String(live.side || "FLAT").toUpperCase();
  const liveQty = Number(live.qty || 0);
  const liveHeader = `open ${opens.length} · live ${liveSide}${liveQty > 0 ? ` x${liveQty.toFixed(4)}` : ""}`;
  const err = String(live.openLivePositionsError || "").trim();
  const body = opens.length
    ? opens.map((x, i) => {
      let lock = "";
      const armed = !!x.profitLockArmed;
      const lockUsdt = Number(x.profitLockUsdt || 0);
      const peak = Number(x.peakUnrealizedPnl || 0);
      if (armed || peak !== 0 || lockUsdt > 0) {
        lock = armed
          ? ` · <span style="color:var(--good)">LOCK ${lockUsdt.toFixed(3)} USDT</span> peak=${peak.toFixed(3)}`
          : ` · <span style="color:var(--muted)">guarding</span> peak=${peak.toFixed(3)}`;
      }
      const upnl = Number(x.unRealizedProfit || 0);
      const upnlColor = upnl < 0 ? "var(--bad)" : upnl > 0 ? "var(--good)" : "var(--text)";
      const lev = Number(x.leverage || 0);
      const levText = lev > 0 ? ` <span style="color:var(--accent);font-weight:800">x${Math.floor(lev)}</span>` : "";
      return `<div class="log-line">#${i + 1} ${x.symbol}${levText} ${pillSignal(x.side)} uPnL <b style="color:${upnlColor}">${upnl.toFixed(3)}</b>${lock}</div>`;
    }).join("")
    : `<div class="log-line" style="color:var(--muted)">ไม่มี position ค้าง</div>
       <div class="log-line" style="color:var(--muted)">สถานะ live: ${liveHeader}</div>`;
  const errLine = err ? `<div class="log-line" style="color:var(--warn)">อ่าน Binance positions ไม่ได้: ${err}</div>` : "";
  const html = `<div class="log-line" style="color:var(--muted)">${liveHeader}</div>${errLine}${body}`;
  ui.openPositions.innerHTML = html;
  openPositionsCache = { key, html, updatedAt: Date.now() };
}




function renderSymbolProfileSummary(bot) {
  const wrap = $("symbolProfileSummary");
  if (!wrap) return;
  const symbols = Array.isArray(bot?.openLivePositions) ? bot.openLivePositions : [];
  const fallback = Array.isArray(bot?.symbolProfiles) ? bot.symbolProfiles : [];
  if (!symbols.length && !fallback.length) {
    wrap.innerHTML = `<div class="metric-sub">ยังไม่มี symbol profile — เริ่ม trade เพื่อสะสม samples หรือตั้ง override ผ่าน /hermes/symbol/profile</div>`;
    return;
  }
  const rows = symbols.length ? symbols.map((p) => ({
    symbol: p.symbol || "—",
    group: p.group || (p.state?.entryVolatilityTier) || "—",
    source: p.source || "—",
    samples: p.sampleTrades ?? "—",
    winRate: p.perf?.winRatePct ?? null,
    pnl: p.perf?.pnl ?? null,
  })) : fallback.slice(0, 8);
  rows.sort((a, b) => (Number(b.samples) || 0) - (Number(a.samples) || 0));
  wrap.innerHTML = `
    <div class="vo-profile-head">
      <span>Symbol Profile (3-tier)</span>
      <span class="vo-profile-hint">low-vol → tight TP, high-vol → wide TP, low-liq → tiny cap</span>
    </div>
    <div class="vo-profile-list">
      ${rows.slice(0, 8).map((r) => `
        <div class="vo-profile-row">
          <span class="vo-profile-sym">${esc(r.symbol)}</span>
          <span class="vo-profile-grp">${esc(r.group)}</span>
          <span class="vo-profile-src">${esc(r.source)}</span>
          <span class="vo-profile-n">${r.samples}</span>
          ${r.winRate !== null ? `<span class="vo-profile-wr">WR ${(r.winRate || 0).toFixed(1)}%</span>` : ''}
          ${r.pnl !== null ? `<span class="vo-profile-pnl ${(r.pnl||0) >= 0 ? 'pos' : 'neg'}">${(r.pnl||0).toFixed(2)}</span>` : ''}
        </div>
      `).join("")}
    </div>
  `;
}


function marketContextMcpSummary(bot) {


function renderKpiCards(bot) {
  const liveStats = bot?.liveStatsAll || bot?.liveStats || {};
  const kpiToday = bot?.kpiTodayAllSymbols?.live || {};
  const incoming = {
    liveWinAll: num0(liveStats.winRatePct),
    livePnlAll: num0(liveStats.realizedPnl),
    liveWinsToday: num0(kpiToday.wins),
    liveLossToday: num0(kpiToday.losses),
    liveWinToday: num0(liveStats.winRatePctToday),
    livePnlToday: num0(liveStats.realizedPnlToday),
    lastTradeAt: num0(bot?.lastTradeAt, 0),
  };
  const hasData = (incoming.liveWinsToday + incoming.liveLossToday) > 0 || incoming.lastTradeAt > 0;
  if (hasData || kpiSticky.lastTradeAt === 0) kpiSticky = incoming;
  const pnl = num0(kpiSticky.livePnlAll);
  if (ui.winRate) ui.winRate.textContent = `${num0(kpiSticky.liveWinAll).toFixed(1)}%`;
  if (ui.winRateSub) ui.winRateSub.textContent = `วันนี้ ${num0(kpiSticky.liveWinsToday, 0)}/${num0(kpiSticky.liveLossToday, 0)} · ${num0(kpiSticky.liveWinToday).toFixed(0)}%`;
  if (ui.realizedPnl) {
    ui.realizedPnl.textContent = `${pnl >= 0 ? "+" : ""}${pnl.toFixed(2)}`;
    ui.realizedPnl.style.color = pnl >= 0 ? "var(--good)" : "var(--bad)";
  }
  if (ui.pnlSub) ui.pnlSub.textContent = `วันนี้ ${num0(kpiSticky.livePnlToday) >= 0 ? "+" : ""}${num0(kpiSticky.livePnlToday).toFixed(2)} USDT`;
}

function paintStatus(data) {
  data = withLastRichBotStatus(data);
  const hermes = data?.hermes || {};
  const bot = data?.bot || {};
  if (data && !data.dashboardSoftStale) lastGoodStatus = data;
  const stale = !!data?.stale || !!bot?.stale || !hermes.healthy;
  const activeLevMax = Number(bot?.config?.leverageMax ?? bot?.config?.adaptiveLeverageMax ?? SERVER_MAX_LEVERAGE ?? 25);
  SERVER_MAX_LEVERAGE = 25;
  if ($("cfgLevMin")) $("cfgLevMin").max = "25";
  if ($("cfgLevMax")) $("cfgLevMax").max = "25";
  if (ui.levCapHint) ui.levCapHint.textContent = `Lev.max x${Math.max(1, Math.min(25, Math.floor(activeLevMax || 25)))}`;
  normalizedLeverageRange();
  updateSymbolOptions(bot);

  const svcText = hermes.healthy ? "Healthy" : hermes.running ? "Degraded" : "Off";
  if (ui.svcStatus) ui.svcStatus.textContent = svcText;

  const running = !!bot.running;
  const botText = running ? "Running" : stale ? "Cached" : "Idle";
  if (ui.botStatus) ui.botStatus.textContent = botText;

  if (ui.backendBanner) {
    if (stale) {
      const reason = !hermes.healthy
        ? "Hermes/backend ไม่พร้อม"
        : "ข้อมูลอาจยังไม่สด";
      ui.backendBanner.textContent = `${reason} - ระบบกำลังพยายามกู้ตัวเองอัตโนมัติ`;
      ui.backendBanner.classList.add("show");
    } else {
      ui.backendBanner.textContent = "";
      ui.backendBanner.classList.remove("show");
    }
  }

  if (ui.serviceToggle) {
    const on = !!(hermes.running || hermes.healthy);
    ui.serviceToggle.textContent = on ? "Off" : "Svc";
    ui.serviceToggle.title = on ? "Stop Hermes" : "Start Hermes";
    ui.serviceToggle.className = on ? "btn-danger sm" : "btn-good sm";
  }
  if (ui.botToggle) {
    ui.botToggle.textContent = running ? "Halt" : "Bot";
    ui.botToggle.title = running ? "Stop bot" : "Start bot";
    ui.botToggle.className = running ? "btn-warn sm" : "btn-primary sm";
  }

  const pub = data?.network?.publicIp || {};
  const ip = String(pub.ip || "").trim();
  if (ui.publicIp) ui.publicIp.textContent = ip ? `IP ${ip}` : "";
  if (ui.publishIpHeader) {
    ui.publishIpHeader.textContent = ip ? `IP ${ip}` : "IP —";
  }
  if (ip && lastSeenPublicIp && ip !== lastSeenPublicIp) showToast("info", `IP ${lastSeenPublicIp} → ${ip}`);
  if (ip) lastSeenPublicIp = ip;

  const liveMode = String(bot?.activePosition?.mode || bot?.config?.executionMode || "").toUpperCase();
  if ($("cfgMode") && running && (liveMode === "LIVE" || liveMode === "PAPER")) {
    $("cfgMode").value = liveMode;
    localStorage.setItem(MODE_KEY, liveMode);
  }
  if (bot?.config && !levUpdateInFlight) {
    const f = document.activeElement;
    if (f !== $("cfgLevMin") && f !== $("cfgLevMax")) {
      const savedMin = Number(localStorage.getItem(LEV_MIN_KEY) || "");
      const savedMax = Number(localStorage.getItem(LEV_MAX_KEY) || "");
      const lmin = Number(bot.config.leverageMin ?? 1);
      const serverMax = Number(bot.config.adaptiveLeverageMax ?? bot.config.leverageMax ?? bot.config.leverage ?? Math.max(lmin, 25));
      const cap = 25;
      const displayMin = Number.isFinite(savedMin) && savedMin >= 1 ? Math.min(cap, Math.floor(savedMin)) : lmin;
      const displayMax = Math.max(
        Number.isFinite(serverMax) ? Math.floor(serverMax) : 1,
        Number.isFinite(savedMax) && savedMax >= 1 ? Math.floor(savedMax) : 1
      );
      if (Number.isFinite(displayMin) && $("cfgLevMin")) $("cfgLevMin").value = String(Math.max(1, Math.min(cap, Math.floor(displayMin))));
      if (Number.isFinite(displayMax) && $("cfgLevMax")) $("cfgLevMax").value = String(Math.max(1, Math.min(cap, Math.floor(displayMax))));
      normalizedLeverageRange();
    }
    if ($("cfgBenchmarkSymbol") && bot.config.benchmarkSymbol) $("cfgBenchmarkSymbol").value = bot.config.benchmarkSymbol;
    if ($("cfgBenchmarkPenalty") && bot.config.benchmarkConflictPenaltyPct != null) $("cfgBenchmarkPenalty").value = String(bot.config.benchmarkConflictPenaltyPct);
    if ($("cfgMaxOpenPositions") && bot.config.maxOpenPositions != null) $("cfgMaxOpenPositions").value = String(bot.config.maxOpenPositions);
    if ($("cfgPairLockStreak") && bot.config.pairLockLossStreak != null) $("cfgPairLockStreak").value = String(bot.config.pairLockLossStreak);
    if ($("cfgPairLockMinutes") && bot.config.pairLockMinutes != null) $("cfgPairLockMinutes").value = String(bot.config.pairLockMinutes);
    if ($("cfgBenchmarkEnabled") && bot.config.benchmarkFilterEnabled != null) $("cfgBenchmarkEnabled").checked = !!bot.config.benchmarkFilterEnabled;
    if ($("cfgPairLockEnabled") && bot.config.pairLockEnabled != null) $("cfgPairLockEnabled").checked = !!bot.config.pairLockEnabled;
  }

  renderKpiCards(bot);
  renderSymbolProfileSummary(bot);
  renderHermesKanban(bot);
  renderHermesSupervisor(bot);

  const logs = Array.isArray(bot.log) ? bot.log : [];
  const extra = bot?.continuity?.orphanLive;
  const extraLine = extra ? `<div class="log-line">orphan ${esc(extra.symbol)} ${esc(extra.side)} qty=${Number(extra.qty || 0).toFixed(4)}</div>` : "";
  ui.log.innerHTML = logs.length
    ? extraLine + logs.slice(0, 80).map((x) => `<div class="log-line">[${new Date((x.ts || 0) * 1000).toLocaleTimeString()}] ${esc(x.msg || "-")}</div>`).join("")
    : extraLine || `<div class="log-line" style="color:var(--muted)">ยังไม่มี log</div>`;

  try {
    const board = Array.isArray(bot.scanBoard) ? bot.scanBoard : [];
    const scanOn = !!bot?.config?.marketScan;
    const orderedBoard = board.slice().sort((a, b) => Number(!!b.qualified) - Number(!!a.qualified) || Number(b.score || 0) - Number(a.score || 0));
    const scanSkip = bot.scanStatus || bot.lastSkip || null;
    const scanCode = String(scanSkip?.code || "").toUpperCase();
    const scanMsg = String(scanSkip?.msg || "");
    const scanRelevant = /SCAN|TIMEOUT|ANALYZE|INTEL/i.test(`${scanCode} ${scanMsg}`);
    const scanFallback = stale
      ? "backend stale / waiting recovery…"
      : !scanOn
        ? "scan ปิด"
        : scanRelevant && (scanCode || scanMsg)
          ? `${scanCode || "SCAN"}${scanMsg ? ` · ${scanMsg.replace(/^Skip:\s*/i, "")}` : ""}`
          : "scan กำลังทำงาน · ยังไม่มี candidate";
  if (ui.scanBoard) {
    ui.scanBoard.innerHTML = board.length
      ? orderedBoard.slice(0, 10).map((x, i) => {
          const extra = x.hardLocked ? " 🔒" : "";
          const q = x.qualified ? `<span style="color:var(--good)">READY</span>` : `<span style="color:var(--muted)">${esc(x.rejectReason || "-")}</span>`;
          const perf = (x.perfTrades || x.perfPnl || x.perfWinRatePct)
            ? ` · perf ${Number(x.perfWinRatePct || 0).toFixed(0)}%/${Number(x.perfPnl || 0).toFixed(2)} (${Number(x.perfTrades || 0)})`
            : "";
          return `<div class="log-line">#${i + 1} <b>${esc(x.symbol)}</b> ${pillSignal(x.signal)} c=${Number(x.confidence || 0).toFixed(2)} s=${Number(x.spreadBps || 0).toFixed(1)} ${q}${extra}${perf}</div>`;
        }).join("")
        : `<div class="log-line" style="color:var(--muted)">${scanFallback}</div>`;
    }
  } catch (_) {
    if (ui.scanBoard) ui.scanBoard.innerHTML = `<div class="log-line" style="color:var(--muted)">scan render error</div>`;
  }

  const hasOpenArray = Array.isArray(bot.openLivePositions);
  let opens = hasOpenArray ? bot.openLivePositions.slice() : [];
  // Fallback for builds that return only activePosition.live (no openLivePositions array).
  if (!hasOpenArray) {
    const live = bot?.activePosition?.live || {};
    const side = String(live.side || "FLAT").toUpperCase();
    const qty = Number(live.qty || 0);
    if ((side === "LONG" || side === "SHORT") && qty > 0) {
      opens = [{
        symbol: bot?.config?.symbol || "LIVE",
        side,
        qty,
        unRealizedProfit: Number(live.unRealizedProfit ?? live.uPnL ?? 0),
        leverage: Number(live.leverage || bot?.config?.leverage || 0),
      }];
    }
  }
  const shouldHoldLast = (
    opens.length === 0
    && openPositionsCache.html
    && (stale || !!bot.running)
    && (Date.now() - openPositionsCache.updatedAt) < 15000
  );
  if (!shouldHoldLast) {
    renderOpenPositions(opens, bot);
  }

  const ld = bot.lastDecision && typeof bot.lastDecision === "object" ? bot.lastDecision : {};
  let pipe = Array.isArray(ld.entryPipeline) ? ld.entryPipeline : [];
  if (!pipe.length && ld && Object.keys(ld).length) {
    const cfg = bot?.config || {};
    const conf = Number(ld.confidence ?? 0);
    const minConf = Number(cfg.minConfidence ?? 0.65);
    const ex = (ld.execution && typeof ld.execution === "object") ? ld.execution : {};
    const spread = Number(ex.spreadBps ?? 0);
    const maxSpread = Number(cfg.maxSpreadBps ?? 999);
    const sig = String(ld.signal || "WAIT").toUpperCase();
    pipe = [
      { gate: "SIGNAL", passed: sig === "LONG" || sig === "SHORT", detail: sig },
      { gate: "CONF", passed: conf >= minConf, detail: `${conf.toFixed(2)} >= ${minConf.toFixed(2)}` },
      { gate: "SPREAD", passed: spread <= maxSpread, detail: `${spread.toFixed(2)} <= ${maxSpread.toFixed(2)}` },
    ];
    if (bot?.lastSkip?.code) {
      pipe.push({ gate: "RESULT", passed: false, detail: String(bot.lastSkip.code) });
    } else {
      pipe.push({ gate: "RESULT", passed: true, detail: "pass" });
    }
  }
  // Fallback: when lastDecision is absent (timeout/scan-only cycle),
  // still show pipeline context from scanBoard + lastSkip.
  if (!pipe.length) {
    const board0 = Array.isArray(bot.scanBoard) ? bot.scanBoard : [];
    const top = board0[0] || {};
    const sig0 = String(top.signal || "WAIT").toUpperCase();
    const conf0 = Number(top.confidence || 0);
    const spread0 = Number(top.spreadBps || 0);
    const cfg0 = bot?.config || {};
    const minConf0 = Number(cfg0.minConfidence ?? 0.65);
    const maxSpread0 = Number(cfg0.maxSpreadBps ?? 999);
    pipe = [
      { gate: "SIGNAL", passed: sig0 === "LONG" || sig0 === "SHORT", detail: sig0 || "WAIT" },
      { gate: "CONF", passed: conf0 >= minConf0, detail: `${conf0.toFixed(2)} >= ${minConf0.toFixed(2)}` },
      { gate: "SPREAD", passed: spread0 <= maxSpread0, detail: `${spread0.toFixed(2)} <= ${maxSpread0.toFixed(2)}` },
    ];
    if (bot?.lastSkip?.code) pipe.push({ gate: "RESULT", passed: false, detail: String(bot.lastSkip.code) });
  }
  const ev = ld.engineVersion || bot.config?.engineVersion || "—";
  if (ui.engineVer) ui.engineVer.textContent = ev;
  if (ui.engineBadge) ui.engineBadge.textContent = String(ev).replace("pro-", "") || "desk";

  try {
    const skip = bot.lastSkip;
    if (ui.pipelineSkip) {
      ui.pipelineSkip.textContent = skip?.code ? `Skip: ${skip.code} - ${(skip.msg || "").slice(0, 80)}` : "";
    }
    if (ui.entryPipeline) {
      ui.entryPipeline.innerHTML = pipe.length
        ? pipe.map((g) => {
          const ok = !!g.passed;
          const short = (g.detail || "").slice(0, 28);
          return `<span class="gate-chip ${ok ? "ok" : "fail"}" title="${esc(g.detail || "")}">${ok ? "✓" : "✗"} ${esc(g.gate)}${short ? " · " + esc(short) : ""}</span>`;
        }).join("")
        : `<span class="gate-chip" style="color:var(--muted)">idle</span>`;
    }
  } catch (_) {
    if (ui.entryPipeline) ui.entryPipeline.innerHTML = `<span class="gate-chip" style="color:var(--muted)">pipeline render error</span>`;
  }
  const la = data?.learning?.lastApplied || {};
  if (ui.learningApplied) {
    const ts = Number(la.ts || 0);
    const dt = ts > 0 ? new Date(ts * 1000).toLocaleString("th-TH", { dateStyle: "short", timeStyle: "short" }) : "—";
    const sym = String(la.symbol || "-");
    const st = la.applied ? "applied" : (la.note || "idle");
    ui.learningApplied.textContent = `Applied: ${st} · ${sym} · ${dt}`;
  }
  if (ui.learningAppliedBadge) {
    if (la.applied === true) {
      ui.learningAppliedBadge.className = "learn-badge ok";
      ui.learningAppliedBadge.textContent = "APPLIED";
    } else if (la.note === "train_now_completed") {
      ui.learningAppliedBadge.className = "learn-badge warn";
      ui.learningAppliedBadge.textContent = "PENDING";
    } else {
      ui.learningAppliedBadge.className = "learn-badge idle";
      ui.learningAppliedBadge.textContent = "IDLE";
    }
  }
}

function safePaintStatus(data) {
  try {
    paintStatus(data);
    return true;
  } catch (e) {
    console.error("dashboard paintStatus failed", e);
    if (ui.backendBanner) {
      ui.backendBanner.textContent = `Dashboard render error - ${String(e?.message || e).slice(0, 100)}`;
      ui.backendBanner.classList.add("show");
    }
    if (ui.log) {
      ui.log.innerHTML = `<div class="log-line" style="color:var(--bad)">Dashboard render error: ${esc(e?.message || e)}</div>`;
    }
    return false;
  }
}

function paintLearning(report) {
  if (!report?.ok) {
    ["learningLastTrain","learningSymbols","learningPromoted","learningThreshold"].forEach((id) => { if ($(id)) $(id).textContent = "—"; });
    ui.learningRows.innerHTML = `<div class="log-line" style="color:var(--muted)">ไม่มีรายงาน</div>`;
    if (ui.learningPromotedList) ui.learningPromotedList.innerHTML = `<div class="log-line" style="color:var(--muted)">ยังไม่มี promoted</div>`;
    return;
  }
  ui.learningLastTrain.textContent = new Date((report.trainedAt || 0) * 1000).toLocaleString("th-TH", { dateStyle: "short", timeStyle: "short" });
  ui.learningSymbols.textContent = String(report.symbolsScanned ?? 0);
  ui.learningPromoted.textContent = String(report.promotedCount ?? 0);
  ui.learningThreshold.textContent = `${report.promoteHitRatePct ?? 0}%`;
  const promoted = Array.isArray(report.promoted) ? report.promoted : [];
  if (ui.learningPromotedList) {
    ui.learningPromotedList.innerHTML = promoted.length
      ? promoted.slice(0, 8).map((p, i) => {
        const rec = p.recommended || {};
        const m = rec.loosen ? "loosen" : rec.tighten ? "tighten" : "hold";
        return `<div class="log-line">#${i + 1} <b>${p.symbol}</b> hit=${Number(p.hitRatePct || 0).toFixed(0)}% ${m}</div>`;
      }).join("")
      : `<div class="log-line" style="color:var(--muted)">ไม่มี promoted ในรอบล่าสุด</div>`;
  }
  const rows = report.results || [];
  ui.learningRows.innerHTML = rows.length
    ? rows.slice(0, 12).map((r) => `<div class="log-line">${r.symbol} wf=${Number(r.walkForwardHitRatePct || 0).toFixed(0)}% ${r.proposalOk ? "✓" : "—"}</div>`).join("")
    : `<div class="log-line" style="color:var(--muted)">ไม่มีผล symbol</div>`;
}

async function refreshStatus() {
  if (statusRefreshInFlight) return;
  statusRefreshInFlight = true;
  try {
    let rs;
    try {
      rs = await req("/status/quick");
    } catch (quickErr) {
      if (lastGoodStatus) {
        safePaintStatus({
          ...lastGoodStatus,
          stale: false,
          dashboardSoftStale: true,
          dashboardWarning: String(quickErr?.message || quickErr || "status quick timeout").slice(0, 120),
        });
        return;
      }
      rs = await req("/status", "GET", null, 18000);
    }
    safePaintStatus(rs);
  } catch (e) {
    if (lastGoodStatus) {
      safePaintStatus({ ...lastGoodStatus, dashboardSoftStale: true });
    }
    ui.svcStatus.innerHTML = statusHtml("var(--bad)", "Error");
    ui.botStatus.textContent = "—";
    if (ui.log) ui.log.innerHTML = `<div class="log-line">${esc(e)}</div>`;
    if ((Date.now() - lastStatusToastAt) > 15000) {
      lastStatusToastAt = Date.now();
      showToast("err", String(e.message || e).slice(0, 80));
    }
  } finally {
    statusRefreshInFlight = false;
  }
}

async function refreshLearningReport() {
  try { paintLearning(await req("/learning/report")); }
  catch (e) {
    ui.learningRows.innerHTML = `<div class="log-line">${e}</div>`;
    showToast("err", "Learning error");
  }
}

async function ensureLearningAuto() {
  if (learningAutoTrainInFlight) return;
  try {
    const rep = await req("/learning/report");
    const trainedAt = Number(rep?.trainedAt || 0);
    const now = Math.floor(Date.now() / 1000);
    const staleSec = 30 * 60;
    const needsTrain = !rep?.ok || trainedAt <= 0 || (now - trainedAt) > staleSec;
    if (!needsTrain) return;
    if ((Date.now() - learningLastAutoTrainAt) < 10 * 60 * 1000) return;
    learningAutoTrainInFlight = true;
    learningLastAutoTrainAt = Date.now();
    await req("/learning/train-now", "POST", {});
    await refreshLearningReport();
    await refreshStatus();
    showToast("info", "Auto learning train complete");
  } catch (_) {
  } finally {
    learningAutoTrainInFlight = false;
  }
}

const PRO_FORM_PRESET = {
  cfgLevMin: 1, cfgLevMax: 25,
  minConfidence: 0.66, intervalSec: 25, cooldownSec: 20,
  maxSpreadBps: 16, maxSlippageBps: 18, slToTpRatio: 0.5, minRiskRewardRatio: 1.5,
  earlyEntryScoreGapMin: 1.4, earlyEntryMinConfidence: 0.60,
  lateEntryMaxBbPctB: 0.90, lateEntryMaxVwapDistancePct: 0.32,
};

function applyProFormPreset() {
  if ($("cfgLevMin")) $("cfgLevMin").value = String(PRO_FORM_PRESET.cfgLevMin);
  if ($("cfgLevMax")) $("cfgLevMax").value = String(PRO_FORM_PRESET.cfgLevMax);
  if ($("cfgMaxOpenPositions")) $("cfgMaxOpenPositions").value = "6";
  if ($("cfgPairLockMinutes")) $("cfgPairLockMinutes").value = "60";
  showToast("info", "โหลด PRO preset แล้ว");
}

function botPayload() {
  const lev = normalizedLeverageRange();
  return {
    symbol: $("cfgSymbol").value || "AUTO",
    usdtAmount: Number($("cfgUsdt").value || 50),
    leverage: Math.min(lev.max, Math.max(lev.min, Number($("cfgLevMax")?.value || lev.max))), leverageMin: lev.min, leverageMax: lev.max, leverageAutoEnabled: true, adaptiveLeverageEnabled: true, adaptiveLeverageMax: lev.max,
    marginType: "CROSSED",
    minRiskRewardRatio: PRO_FORM_PRESET.minRiskRewardRatio,
    atrTpSlEnabled: true, ema200StrictEnabled: true, engineVersion: "pro-2.0",
    intervalSec: PRO_FORM_PRESET.intervalSec, minConfidence: PRO_FORM_PRESET.minConfidence,
    htfStrictEnabled: true, htfMinStrength: 0.22,
    cooldownSec: PRO_FORM_PRESET.cooldownSec,
    allowFlip: false, strongFlipEnabled: true, strongFlipMinConfidence: 0.76, strongFlipMinScoreGap: 1.5, strongFlipUltraScoreGap: 2.2, strongFlipUltraConfRelax: 0.08,
    maxSpreadBps: PRO_FORM_PRESET.maxSpreadBps, maxSlippageBps: PRO_FORM_PRESET.maxSlippageBps,
    earlyEntryEnabled: true, earlyEntryScoreGapMin: PRO_FORM_PRESET.earlyEntryScoreGapMin, earlyEntryMinConfidence: PRO_FORM_PRESET.earlyEntryMinConfidence,
    lateEntryMaxBbPctB: PRO_FORM_PRESET.lateEntryMaxBbPctB, lateEntryMaxVwapDistancePct: PRO_FORM_PRESET.lateEntryMaxVwapDistancePct,
    executionMode: $("cfgMode").value || "PAPER",
    holdWinners: true, holdMinConfidence: 0.78, holdTrailPct: 0.32,
    marketScan: true, scanTopLiquid: 25, scanAnalyzeTop: 6,
    slToTpRatio: PRO_FORM_PRESET.slToTpRatio,
    benchmarkFilterEnabled: !!$("cfgBenchmarkEnabled").checked,
    benchmarkSymbol: ($("cfgBenchmarkSymbol").value || "BTCUSDT").toUpperCase(),
    benchmarkConflictPenaltyPct: Number($("cfgBenchmarkPenalty").value || 6),
    liveBadUtcHours: [],
    pairLockEnabled: !!$("cfgPairLockEnabled").checked,
    pairLockLossStreak: Number($("cfgPairLockStreak").value || 2),
    pairLockMinutes: Number($("cfgPairLockMinutes").value || 45),
    aiTpSlFromLearning: true, autoLearn: !!$("cfgAutoLearn").checked,
    feeMinNetProfitUSDT: 0.1, feeMinEdgeVsCostMultiple: 1.55,
    tpSlTargetUsdtEnabled: true, tpTargetMinUsdt: 0.55, tpTargetMaxUsdt: 2.2,
    volTargetEnabled: true, volTargetPct: 0.2, maxOpenPositions: Math.max(1, Math.min(20, Number($("cfgMaxOpenPositions")?.value || 6))),
    orphanAutoAdoptEnabled: true, orphanAutoAdoptForceSingleSymbol: false, orphanAutoAdoptMultiEnabled: true, learningRewardEnabled: true,
  };
}

$("loadProPreset")?.addEventListener("click", applyProFormPreset);
$("btnRefresh")?.addEventListener("click", () => withBusy($("btnRefresh"), refreshStatus));
$("serviceToggle")?.addEventListener("click", () => withBusy($("serviceToggle"), async () => {
  const rs = await req("/status");
  const on = !!(rs?.hermes?.running || rs?.hermes?.healthy);
  await req(on ? "/service/stop" : "/service/start", "POST", {});
  await refreshStatus();
}));
$("botToggle")?.addEventListener("click", () => withBusy($("botToggle"), async () => {
  const rs = await req("/status");
  if (rs?.bot?.running) {
    await req("/bot/stop", "POST", { force: true });
  } else {
    let payload = botPayload();
    await precheckLiveBeforeStart(payload);
    try {
      await req("/bot/start", "POST", payload);
    } catch (e) {
      const m = String(e?.message || "").match(/server max\s+(\d+)/i);
      if (m) {
        SERVER_MAX_LEVERAGE = Math.min(25, Math.max(1, Number(m[1])));
        normalizedLeverageRange();
        await req("/bot/start", "POST", botPayload());
      } else throw e;
    }
  }
  await refreshStatus();
}));
$("learningTrainNow")?.addEventListener("click", () => withBusy($("learningTrainNow"), async () => {
  await req("/learning/train-now", "POST", {});
  await refreshLearningReport();
  await refreshStatus();
}));
$("learningRefresh")?.addEventListener("click", () => withBusy($("learningRefresh"), refreshLearningReport));
$("cfgLevMin")?.addEventListener("change", () => { normalizedLeverageRange(); applyLeverageConfigIfRunning(); });
$("cfgLevMax")?.addEventListener("change", () => { normalizedLeverageRange(); applyLeverageConfigIfRunning(); });
$("cfgMaxOpenPositions")?.addEventListener("change", async () => {
  try {
    const v = Math.max(1, Math.min(20, Number($("cfgMaxOpenPositions").value || 6)));
    $("cfgMaxOpenPositions").value = String(Math.floor(v));
    const rs = await req("/status");
    if (rs?.bot?.running) await req("/bot/config", "POST", { maxOpenPositions: Math.floor(v) });
    await refreshStatus();
  } catch (e) {
    showToast("err", String(e.message || e).slice(0, 120));
  }
});

setInterval(refreshStatus, 5000);
setInterval(refreshLearningReport, 15000);
setInterval(ensureLearningAuto, 60000);
refreshStatus();
refreshLearningReport();
ensureLearningAuto();
