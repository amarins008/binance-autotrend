<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **binance-autotrend** (3979 symbols, 9647 relationships, 344 execution flows).

> Index stale? Run `node .gitnexus/run.cjs analyze --index-only` from the project root — it auto-selects an available runner. No `.gitnexus/run.cjs` yet? Bootstrap with `npx`, `bunx`, or `pnpm dlx` — e.g. `bunx gitnexus@latest analyze` (npm 11 npx crash; #1939).

## Always Do

- **MUST run impact before editing.** Use `impact({target: "symbolName", direction: "upstream"})` or `node .gitnexus/run.cjs impact "symbolName" --direction upstream --repo .`; report callers, processes, and risk. Never substitute grep for graph analysis.
- **MUST analyze graph changes before committing.** Use `detect_changes({scope: "all"})` (MCP) or `node .gitnexus/run.cjs detect-changes --scope all --repo .` (CLI fallback). `partial: true` or `truncated: true` is not a clean check — a zero means unseen, not unaffected; re-run it. For regression review: `detect_changes({scope: "compare", base_ref: "main"})` or `node .gitnexus/run.cjs detect-changes --scope compare --base-ref "main" --repo .`.
- MUST warn on HIGH/CRITICAL `risk` pre-edit; never use `riskSharedAxes` to waive a HIGH/CRITICAL `risk` warning. Compare File/symbol: MCP File omits axes; Graph-RAG expands File.
- **MUST treat `risk: UNKNOWN` as unresolved, not as low.** An empty caller set is not evidence the symbol is unused — it can also mean the callers are not resolvable by the index (plain-object property access, dynamic dispatch, cross-language calls). `impact` pairs `UNKNOWN` with a `riskNote` saying so. Confirm with a text search before treating the symbol as safe to change or delete; do not proceed on the strength of a zero.
- **MUST use `query({search_query: "concept"})` for concepts/flows, `context({name: "symbolName"})` for a named symbol, or `impact` for blast radius, on read-only callers, dependencies, imports, or execution flow.** Graph first; text search only for empty/`UNKNOWN`/literals.
- For security review, `explain({target: "fileOrSymbol"})` lists taint findings (source→sink flows; needs `analyze --pdg`).

## Never Do

- NEVER edit a function, class, or method before MCP/CLI impact analysis.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis, and never read `UNKNOWN` as an all-clear — it means the walk could not answer, which is the one verdict that requires confirming by other means.
- NEVER rename symbols with find-and-replace — use `rename` which understands the call graph.
- NEVER commit before MCP/CLI graph change analysis.

## Resources

| Resource | Use for |
| --- | --- |
| `gitnexus://repo/binance-autotrend/context` | Codebase overview, check index freshness |
| `gitnexus://repo/binance-autotrend/clusters` | All functional areas |
| `gitnexus://repo/binance-autotrend/processes` | All execution flows |
| `gitnexus://repo/binance-autotrend/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
| --- | --- |
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->

---

# AGENTS.md — กติกาถาวร + สถานะปัจจุบัน

> Session log ประวัติทั้งหมดย้ายไป `docs/session-notes-archive.md` (ยังอยู่ครบ ไม่ได้ลบ)
> ประวัติเต็มใน git: `git log -p -- AGENTS.md` · งานที่ยังค้างดู `backend/TASKS.md`

## โปรเจค

- Binance Autotrend (Cmux + Hermes) — เทรด futures หลายสัญลักษณ์, per-symbol autotrade
- Python backend FastAPI: `backend/main.py` + modules ใต้ `backend/` (exchange/, trading/, services/, analysis/, routers/)
- CLI: `backend/cmux_cli.py` · config runtime: `backend/autotrade.standalone.json`
- trade log: `backend/obsidian_vault/trades_log.jsonl` (per-symbol) + `shared/all_trades.jsonl`
- GitHub: repo `amarins008/binance-autotrend`, branch `main`
- ถ้า `gh` push 403 → `gh auth switch --user amarins008 && gh auth setup-git`

## กติกาที่ห้ามละเมิด (HARD)

1. **ห้ามแก้ live config ขณะบอทรันอยู่** — tuners mutate `cfg` in-place ผ่าน `set_float`/`set_int`
   ก่อนเรียก `_commit_supervisor_config_tune` ค่า drift ไปแล้ว → guard ที่ commit อย่างเดียวไม่พอ
2. **fail-closed ต้องคงอยู่** — kill switch + LIVE pause ทุกจุด (12 จุด) ห้ามลดเป็น fail-open
3. **Binance -2015 = dynamic IP** — IP หมุนไปมา ห้าม whitelist IP เดี่ยว ต้อง whitelist CIDR / ปิด IP restriction / static IP
4. **ห้าม commit `backend/autotrade_snapshot.json`** + ไม่แตะ `.hermes-backups/` + `recovery_artifacts/`
5. **ห้ามแก้โค้ดแบบ state-machine script** — เคยทำ `exchange/futures_orders.py` บูด (`frasync def`) ต้อง `git checkout --` กู้
6. **ทุก commit ต้องผ่าน `gitnexus detect-changes`** และรายงาน blast radius เมื่อ risk HIGH/CRITICAL
7. **แก้ symbol ใดต้องรัน impact ก่อน** — ห้าม rename ด้วย find-and-replace
8. **ไม่ swap `http_client.py`** — wiring ไป `_BINANCE_HTTP` ผ่าน `configure_clients()` อยู่แล้ว
9. **อย่า migrate `trading/learning.py` / `trade_stats.py` / `trade_log.py` ไป container** — ผูก `cache_registry`
   ที่ import-time บังคับผ่าน container ต้อง deferred binding = เปลี่ยน resolution มีความเสี่ยง ไม่มี benefit

## สถานะปัจจุบัน (ยืนยัน 2026-10-04)

Phase 1 เสร็จ — บอทรัน LIVE อยู่

**Settled invariants**

- Entry pipeline: TP = SL = ±2 USDT (`tpTargetMin/Max=2.0`, `slToTpRatio=1.0`, `minRR=1.0`)
- Sizing per-symbol scale ตาม notional; notional ~51 ต้องมี move 3.9% ให้ถึง TP
- `trading/close_reconciler.py` = source of truth จาก `/fapi/v1/userTrades`
  (`_close_events` จัดกลุ่ม fills gap ≤120s, `_event_trade` derive entry/exit/qty)
- `exchange/futures_orders.py` canonical เดียว — มี `_extract_fill_price` บันทึก PnL ด้วยราคา fill จริง
  (`_close_position` / `_cancel_all_open_orders` ซ้ำใน main.py ถูกลบแล้ว)
- Binance ตอบ HTTP 200 ทุก DELETE โดยไม่ยกเลิก → TP/SL ใช้ `timeInForce=GTD`
  (`protectiveOrderGtdSec` default 7200, 0=GTC) ไม่งั้น order ค้างเป็นอมตะและตัดไม้ใหม่ได้
- Protective order ต้องเช็ค `algoStatus` (`_raise_if_algo_rejected`) + verify ด้วย
  `openAlgoOrders` — Binance ตอบ 200 + `REJECTED` แบบ async
- `sweep_orphan_protective_orders` วนทุก 600s ใน lifespan, sweep ก่อน sleep,
  อ่าน positionRisk ไม่ได้ = fail-closed, รายงาน `unclearedSymbols` แทน success ปลอม
- big-loss cooldown: `_recent_big_losses_by_symbol` เฉพาะ close ล่าสุดต่อ symbol
  (`bigLossCooldownUsdt` 1.0 / 30 นาที / ย้อน 2 ชม.)
- TV confirm gate: `_tv_confirmation_streak`, `tvConfirmReadings=2` ใน `tvConfirmWindowSec=180`,
  reset เมื่อสัญญาณพลิก; TV=WAIT ต้อง `tvWaitMinConf=0.88`
- direction-bias entry gate อยู่ main.py (`biasGateEnabled`): bias==side +0.033, NEUTRAL/mismatch -0.024
- config drift 0/381 fields ใน 210s · kill switch False fail-closed ครบ 12 จุด
- API endpoint ที่ใช้ได้: `openOrders`/`openAlgoOrders` (ไม่ใช่ `allOpenOrders`/`algoOpenOrders` ที่ 404)

**ระบบที่ใช้งานจริง (verify 2026-10-04)**

- 17 ไม้ +6.26 gross / ~+4.2 net — `EXCHANGE_CLOSE` ลง log ครบพร้อม netPnl, reconciler ทำงานจริง
- adaptive release ฟื้นแล้ว (เคยตาย 6 สัปดาห์จาก NameError `_risk_cooldown_resume_ok`)
- LINK loss-streak cooldown ทำงานจน 11:27 · fee-edge gate ตัด ONEUSDT (edge -0.29 ≤ 1.96)
- test suite 417 passed / 0 failed (integration 26 failed = baseline สองฝั่ง, stash พิสูจน์)

## งานค้าง

1. **backfill ไม้ ghost** (QNT −1.084 + 2 ครั้งก่อนหน้า) — ต้องเปิด `record=True` พร้อม `since_ms`
   เจาะจง ทำแยกจากงานอื่น เพราะเสี่ยงบันทึกซ้ำ
2. **orphan GTC 67 ตัว** — API ลบไม่ได้ ต้องกด Cancel All บนเว็บ Binance + ticket แนบ
   algoId `4000001944597475`
3. **บัญชีเทรดนอกบอท** — 09-20→10-03 มี REALIZED_PNL +30.94 / COMMISSION −46.56 (net −15.62,
   3521 income rows) แต่ trades_log ว่าง; fingerprint ไม่ตรงบอท (notional median 16 vs 100-400)
   → มี client อื่นใช้ key นี้ ยังไม่ระบุตัวตน
4. **SHORT ขาดทุนเชิงโครงสร้าง** — all-time 1,926 ไม้ −17.15 (WR 48.9%), ไม่มี SHORT ตั้งแต่ 09-17
5. **live_guardian `_lazy_main()` → 0 delegates**

## หมายเหตุ

- `stopLossPct=0.176` เป็นค่า autotuner runtime ปกติ ไม่ต้องแก้
- `execution_agent` state=blocked เป็น runtime health-mark จริง (main.py:7939) ไม่กระทบ trading = non-issue
- `liveProfitLocks` leak มาจาก `test_guardian_performance.py::TestParallelIntelDispatch`
  ที่รัน `_live_multi_profit_lock_manage` จริง → เขียน `AUTO_TRADE["liveProfitLocks"]` (SYM0-2USDT)
- vault layout: `obsidian_vault/symbols/<SYM>/` (profile.json, symbol_profile.json, trades.jsonl,
  windows.json, risk_tune.json) + `obsidian_vault/shared/` (config.json, risk.json, daily_stats.json,
  all_trades.jsonl)
