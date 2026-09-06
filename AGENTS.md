<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **binance-autotrend-standalone-final** (17507 symbols, 35729 relationships, 300 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> If any GitNexus tool warns the index is stale, run `npx gitnexus analyze` in terminal first.

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `gitnexus_detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `gitnexus_query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `gitnexus_context({name: "symbolName"})`.

## Never Do

- NEVER edit a function, class, or method without first running `gitnexus_impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `gitnexus_rename` which understands the call graph.
- NEVER commit changes without running `gitnexus_detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/binance-autotrend-standalone-final/context` | Codebase overview, check index freshness |
| `gitnexus://repo/binance-autotrend-standalone-final/clusters` | All functional areas |
| `gitnexus://repo/binance-autotrend-standalone-final/processes` | All execution flows |
| `gitnexus://repo/binance-autotrend-standalone-final/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->

---

## สรุปความคืบหน้าโปรเจค (Per-Symbol Autotrend Architecture)

### สถานะปัจจุบัน: Phase 1 เสร็จแล้ว — กำลังทดสอบ

## Session: Supervisor auto-tune self-conflict patch (เสร็จ, commit 9dd35d8)

### ปัญหา
- 8 supervisor tuners เขียน knobs overlap กัน โดยไม่มี lock ร่วม → loosener/tightener สู้กัน (เช่น small_profit loosens profit-lock/TP เดียวกับ weak_payoff tightens; low_entry loosens scan workload เดียวกับ scan_timeout tightens; size_streak loosens size เดียวกับ weak_payoff/tighten)
- Rollback path บกพร่อง: `_commit_supervisor_config_tune` บันทึก `changes[]` ไว้แล้ว แต่ rollback แก้คืนจาก `preMetrics` (performance stats) → **no-op** ไม่ restore config จริง
- **Discovery ใหญ่:** main.py มี tuner copies ของตัวเอง 6 ตัว (`weak_payoff` 491, `low_entry` 827, `scan_timeout` 994, `daily` 1144, `small_profit` 1245, `negative` 1311) ที่ **review loop เรียกจริง** (main.py:7856/8149/8245/8300/8399/8440/8476/8531) + import `size_streak`/`trade_period_reviews` จาก supervisor_tuning; supervisor_tuning's 6 copies = **dead duplicates** (ไม่มี caller) — แก้ที่ supervisor_tuning อย่างเดียวไม่กระทบ bot

### Fix (commit 9dd35d8, 3 ไฟล์)
- **`trading/supervisor_state.py`** — `_apply_rollback_old_values(cfg, rollback)` (ใหม่, ~216): restore ค่าจริงจาก `changes[].old` (ข้าม `old=None`, ไม่ inject metric keys) + เปลี่ยน single  `_TUNING_MODE_LOCK_STATE` → **per-domain** `_TUNING_LOCK_DOMAINS` (`entry`/`profit`/`scan`/`size`); `_tuning_mode_lock_acquire(..., domain=None, domains=None)` block เฉพาะ domain ที่ถือโหมดตรงข้าม; `_release()` ล้างทุก domain; `_status()` เพิ่ม activeDomains/domains
- **`trading/supervisor_tuning.py`** — ลบ duplicate defs: `_supervisor_trade_period_reviews` (top) + `_maybe_tune_size_multiplier_from_streak` (top) → เหลือชื่อละ 1 (live = bottom; 1557→1344 lines); ใช้ `_apply_rollback_old_values` rollback path; เพิ่ม domain locks (low_entry entry+scan sub-gate, scan_timeout scan, weak_payoff profit+size sub-gate, small_profit profit, negative/daily entry, size_streak size ตาม target≥1.0); **+ port drift:** size_streak เพิ่ม `insufficient_recent_trades` guard (loss streak บน session ที่มี trades น้อย) + cooldown `10` → `max(10, supervisorSizeStreakCooldownMin=60)` — ตอนนี้ live เท่ากับ dead copy เดิม
- **`main.py`** — 6 **live** tuner copies ได้ fix เดียวกัน (rollback ใช้ `_apply_rollback_old_values` + release-on-rollback; domain locks ก่อน modify; ปล่อย lock ตอน no_safe_delta); import 3 helpers จาก supervisor_state (shared, no cycle)

### Verification
- py_compile + `import main` cycle-free
- 14 live-tuner gate probes ผ่าน (blocked paths ไม่ mutate; size sub-gate; profit/scan/entry cross-block; lock release on no-op; rollback restore จริง ไม่ inject metric keys)
- 8 size_streak probes ผ่าน (loss guard ไม่ tune, cooldown 3600s, size domain lock)
- `pytest tests/ -q --ignore=tests/test_pineforge.py` = **63 passed**

### เดิม (ก่อนช่วย) — dead supervisor_tuning copies ที่แก้แต่ไม่กระทบ bot
- งานก่อนหน้าเคยแก้ (a)(c)(d)(b) ที่ supervisor_tuning.py 6 copies ที่เป็น dead → เก็บไว้ (ไม่ทำร้าย) แต่ **จุดจริงคือ main.py** — session นี้ pivot ไปแก้ live

### ยังต้องทำ / note
- **Dedup (แนะนำ แต่ deferred):** supervisor_tuning's `_maybe_tune_low_entry_activity/scan_timeout/weak_payoff/daily/small_profit/negative` (6 copies) + `_daily_trade_regime_review` = dead กับ main.py's live copies → ควรลบ side ที่ถูก (referenced โดย `test_refactored_modules.py` ยัง import live names อยู่) — งาน refactor ใหญ่ ไม่ทำ session นี้
- ยังไม่ commit งาน direction-bias session (ค้างจากก่อนหน้า ยัง)
- (optional) remove `execution_agent` state=blocked

## Session: Direction Bias detector + Start AutoTrade.bat adjust (เสร็จ)

### สิ่งที่ทำ
- **`backend/analysis/direction_bias.py`** (ใหม่) — detector ทิศทาง M15/M30: EMA20/50 trend + swing structure (fractal pivots) + pullback-to-EMA zone keyword. Pure `compute_direction_bias(rows_15m, rows_30m)` + async `detect_direction_bias(symbol)` hook `main._cached_klines` (lazy `_main()` ไม่ import cycle)
- **`backend/tests/test_direction_bias.py`** (ใหม่) — 8 tests ผ่าน; suite รวม 59 passed (`pytest tests/ -q --ignore=tests/test_pineforge.py`)
- **`backend/tests/monitor_direction_bias.py`** (ใหม่) — live monitor 4 symbols
- **Debug route:** `main.py::debug_direction_bias` + `/debug/direction-bias` ใน `misc_routes.py` — verify บน real bot OK
- **ผูกเข้า intel dict:** `intel_analyze` เพิ่ม `directionBias` block ใน result (observational **ไม่ gate** ตาม scope เดิม) — lazy import `_direction_bias` + `asyncio.wait_for(6s)` guard + fallback NEUTRAL บน error; verify บน live bot: `lastDecision.intel.directionBias` มีค่า real (bias/strength/regime/entry)
- **`Start AutoTrade.bat`** (Desktop + root copy sync ตรง) — เขียนใหม่: venv pre-flight, kill pattern `'run_backend\.py|launcher\.py|uvicorn main:app'`, `wait_port_free` 10s, `start /min`, แสดง `/debug/direction-bias` ใน summary; test รันจริง 5/5 clean

### Findings (ที่เจอระหว่าง verify)
- `/intel/analyze` HTTP ได้ 500 บ่อยตัว — **pre-existing** ไม่ใช่จาก directionBias: เป็น rate-limit/inflight contention ระหว่าง scan loop (autotrade guardian) กับ request พร้อมกัน → `_precision_signal_pack` (r5/r15 klines) timeout เกิน `DATA_GET_TIMEOUT_SEC=6.0`; พิสูจน์ด้วย baseline (stash dir_bias ออก) ก็ fail เหมือนเดิม ตัว dir_bias wrap guard กันเองแล้ว ไม่กระทบ gather
- หลัง restart ใหม่: bot healthy, autotrade ทำงาน, Guardian cycle ปกติ, KPI today 9W/0L +1.72 USDT

### ยังไม่ได้ทำ
- ยังไม่ commit งาน direction-bias session นี้
- (optional) remove `execution_agent` state=blocked ("Binance API/IP permission rejected" — hermes subagent ที่ไม่กระทบ real trading)
- (ถ้าต้องการ) เพิ่ม `directionBias` เป็น entry gate จริง (ตอนนี้เป็น observational เท่านั้น)

### ส่วนก่อนหน้า: แก้ root cause -1021 (CRITICAL) + summary เก่า

### ปัญหา
- Bot (port 8020) ล้มเหลว `-1021 Timestamp outside recvWindow` ทุกคำขอ signed ใน `binance_client._signed_request` path + autotrade loop crash-loop ทุก ~10s
- **`/debug/binance-auth-check` ผ่าน** (path ตัว main ที่ fresh-fetch serverTime) แต่ `/debug/binance-positions` ผ่าน (path live_guardian) — standalone ผ่าน แต่ uvicorn process ผ่านทั้ง 8020/8022

### Root cause (import-order bug)
- `exchange/binance_client.py:28` `CONNECTOR_MODE = os.getenv("CONNECTOR_MODE", "auto")` **eval ตอน import**
- main.py import live_guardian (line 53) → binance_client **ก่อน** `load_dotenv` (line 139) → ใน bot `CONNECTOR_MODE` freeze = `"auto"` ตลอด (แม้ `.env` ตั้ง `legacy`)
- `"auto"` → `_get_um_client` return binance **SDK `UMFutures`** client (drift ~5.3s > SDK recvWindow 5000) → SDK send local timestamp ไม่มี offset sync → `-1021 ClientError`
- Standalone ผ่านเพราะ script โหลด `.env` ก่อน import → freeze `"legacy"` → `_get_um_client`=None → ใช้ custom `_signed_request` (offset-synced, ผ่าน)

### Fix (1 point)
- `binance_client.py _get_um_client()` — เปลี่ยนอ่าน `os.getenv("CONNECTOR_MODE", "auto").lower()` แบบ dynamic แทน module constant ที่ freeze-tại import
- Plus: เพิ่ม `offsetDiag` (`sentTsMs`/`offsetMs`/`syncAgeSec`/`recvWindow`/`httpConfigured`) ใน detail ของ 4xx error ใน `_signed_request` เพื่อ debug อนาคต
- main.py's own `_get_um_client` (line 5296) ใช้ constant line 3426 (eval หลัง load_dotenv) → ถูกต้องอยู่แล้ว ไม่ต้องแก้

### Verification
- Fresh uvicorn probe (8022, venv, `-m uvicorn main:app`) — ก่อน fix: `-1021`; หลัง fix: offset=5126ms, drift=5134ms, positions `count=0` ✅
- Restart bot 8020 ผ่าน `/system/restart` — หลัง restart: positions `count=0, no error`, autotrade loop อยู่รอด (task done=False, log ไม่มี -1021, guardian cycle ปกติ)
- `pytest tests/ --ignore=test_pineforge.py` = **51 passed** (test_pineforge sys.exit() module-level เป็น pre-existing ตามบันทึกเดิม)
- ทำลาย probe 8022 เรียบร้อย; temp debug route ใน main.py/misc_routes.py revert หมด คงเหลือแค่ fix จริง

## Phase S1 — Stability & Performance (เริ่มแล้ว)

### สิ่งที่ทำเสร็จแล้ว (session นี้)

**ไฟล์ใหม่ที่สร้างขึ้น (backend/):**
- `exceptions.py` — typed exception hierarchy: `TradingError` (base) → `ExchangeError`, `DataError`, `PipelineError`, `ConfigError`, `SignalError`, `OrderError`, `StateError`
- `logger.py` — `StructuredFormatter` (JSON lines), `configure_logging()`, `get_logger()`, `log_exception()`
- `http_client.py` — `HTTPClientManager` singleton, pooled httpx clients (signed ใช้ testnet, data ใช้ mainnet), `httpx.Limits`
- `cache.py` — `TradingCache` backed by `cachetools.TTLCache` (thread-safe, bounded, TTL) + `CacheStats`; singleton caches: klines/intel/profile/trade_log/filters + `all_cache_stats()`
- `memory_monitor.py` — `current_rss_mb()`, `check_memory()` (warning/critical thresholds), `estimate_bytes()`, `periodic_memory_snapshot()`

**ไฟล์ที่แก้ไข:**
- `requirements.txt` — เพิ่ม pydantic, tradingview-ta, cachetools, psutil
- `backend/exchange/binance_client.py` — ใช้ `ExchangeError` + structured logging ใน `_data_get()` (เปลี่ยน `raise last_err` → `raise ExchangeError`), เพิ่ม module logger
- `backend/analysis/intel_pipeline.py` — ใช้ `DataError` แทน `HTTPException`, ใช้ `log_exception` แทน bare `except: pass`, ลบ unused imports

**Tests (backend/tests/) — 33 tests ผ่าน:**
- `test_exceptions.py`, `test_cache.py`, `test_risk.py`, `test_regime.py`, `test_pipeline.py`, `test_memory_monitor.py`
- หมายเหตุ: `test_pineforge.py` เป็น standalone script ที่เรียก `sys.exit()` ระดับ module → เก็บไม่ร่วม pytest (pre-existing, ไม่เกี่ยวกับงานนี้)

### ยังต้องทำ
- **live_guardian `_lazy_main()` → 0 delegates ครบ (intel_analyze migrate แล้ว — ดู bullet ด้านล่าง)**
- S2: migrate modules เดิมให้ใช้ `interfaces.py` + `container.py` แทน module-global — **จบ (scoped)**: `services/infra_health.py` migrate ครบ (อ่าน cache/cache_registry/memory ผ่าน `container.get()` แทน lazy import ตรง; identity ตรงทุกตัว; 51 tests ผ่าน) — **การ migrate `trading/learning.py`/`trade_stats.py`/`trade_log.py` ถูกระงับโดยเจตนา**: 3 modules นี้ผูก `cache_registry` (legacy shared state ที่ทุกตัวใช้ร่วมกัน — `_LIVE_STATS_VERSION`/`_SESSION_BIAS_CACHE` ฯลฯ) ที่ import-time; บังคับผ่าน container ต้องแก้เป็น deferred binding → เปลี่ยน timing/resolution มีความเสี่ยง ไม่มี benefit เชิง architecture (layer นี้เป็น shared-state registry อยู่แล้ว ถือเป็น migration สมบูรณ์ในตัว) — `container["cache_registry"]` ชี้ module เดียวกับที่ 3 modules import ตรง (identity ตรง)
- `http_client.py` ยังเป็น dead code (main.py ใช้ lifespan `_BINANCE_HTTP`/`_BINANCE_DATA_HTTP` ผ่าน `configure_clients()` **อยู่แล้ว** — wiring ไปยัง exchange/binance_client เรียบร้อย) — ไม่ swap เพื่อไม่เสี่ยงกับการทำงานปัจจุบัน
- ทดสอบกับระบบเทรดจริง (live trading)

### ทำเสร็จเพิ่ม (session ต่อเนื่อง)
- **`services/infra_health.py`** — wire up `memory_monitor` + `cache.py` เข้า `collect()`: เพิ่ม `memory` block (rssMb/level/ok) + `cacheStats` block (klines/intel/profile/trade_log/filters hit/miss/eviction rate) + ปรับ score (memory_critical -40/error, memory_high -15/warning)
- **S2 เริ่มงวดแรก: `container.py` + `services/infra_health.py`** — แก้ container: `services["cache_registry"]` เดิมชี้ `{ function } all_cache_stats` (ผิด) → ชี้ module `services.cache_registry` จริง (ตัวถือ `_KLINES_CACHE`/`_INTEL_CACHE`/`_DATA_PROVIDER_HEALTH` cross-module dicts); `services["cache"]` = top-level cache module (TradingCache stats), `services["memory"]` = memory_monitor — identity `container.get('cache_registry') is services.cache_registry` / `get('cache') is cache` / `get('memory').periodic_memory_snapshot is memory_monitor.periodic_memory_snapshot = True; **infra_health.collect() migrate**: ลบ lazy imports ตรง (`from services import cache_registry`, `from memory_monitor import periodic_memory_snapshot`, `from cache import all_cache_stats`) → resolve ผ่าน `container.get(...)` ที่ runtime (เหลือ import แค่ stdlib + binance_client version-getters); output structure ยืนยันไม่เปลี่ยน (key ทั้งหมด + score/warnings/errors เหมือนเดิม); `import main` cycle-free; 51 tests ผ่าน + 3 integration fails pre-existing
- **`analysis/intel_pipeline.py`** — ลบ `_main()` + 17 dead-delegate wrappers (`_candlestick_pattern_context`, `_market_momentum`, `_symbol_effective_profile`, ฯลฯ) ที่ไม่มีการใช้งานจริง (จริง ๆ อยู่ที่ `main.py`/`trading/*`) เหลือแค่ `_load_single_profile` (ใช้ภายใน 2 ที่) + `_cached_klines` + `_symbol_quality_score` + `_intel_score`
- **`interfaces.py`** — DI Protocols (structural typing): `Cache`, `CacheRegistry`, `HTTPClientProvider`, `MemoryMonitor` — implement ทุกตัวผ่าน `isinstance` ตรงกับของจริงแล้ว (`TradingCache`, `HTTPClientManager`, `memory_monitor`)
- **`container.py`** — service locator ท่อจากที่สกัดออกมาแล้ว: `services["cache"]`, `["memory"]`, `["http"]`, `["logger"]`, `["cache_registry"]` + `get()`/`has()`/`register()` — additive, ยังไม่แตะ main.py logic
- **`trading/state_ops.py`** — ย้าย `_autotrade_log` + `_agent_mark` ออกจาก monolith (`main.py` lines 5866-5871) → module แยก operates บน `app_state.AUTO_TRADE` โดยตรง, เปลี่ยน:
  - `main.py` — import alias `from trading.state_ops import agent_mark as _agent_mark, autotrade_log as _autotrade_log` + ลบ 2 defs เดิม
  - `trading/live_guardian.py` — ลบ `_agent_mark`/`_autotrade_log` delegates, import ตรงจาก `state_ops`, replace `_main()._agent_mark(...)` → `_agent_mark(...)` (6 sites)
  - `exchange/futures_orders.py` — ลบ `_autotrade_log` delegate, import ตรงจาก `state_ops`
- **`trading/risk.py`** — ย้าย `_fee_edge_min_net_usdt` (→ `fee_edge_min_net_usdt`) + `_calc_tp_sl_prices` (→ `calc_tp_sl_prices`) ออกจาก main พร้อม constants `AUTOTRADE_TAKER_FEE_BPS_PER_SIDE`/`AUTOTRADE_MIN_NET_PROFIT_USDT`; main + live_guardian + futures_orders alias import ตรง
- **`trading/live_guardian.py`** — ลบ delegates `_autotrade_log`, `_agent_mark`, `_recent_payoff_loss_guard` (จาก `trading.learning`), `_fee_edge_min_net_usdt`, `calc_tp_sl_prices` (จาก `trading.risk`), `_last_decision_intel`, `_entry_snapshot_from_intel` (จาก `trading.state_ops`), `fetch_mark_price` + `_um_client_position_risk` + `_current_position_amount` (จาก `exchange.futures_orders`), `_get_um_client` + `_signed_request` (จาก `exchange.binance_client`) → dài
- **`trading/learning.py`** — `_recent_payoff_loss_guard` re-export ใช้ใน live_guardian ตรงแล้ว
- **`main.py`** — เพิ่ม `from exchange.binance_client import configure_clients as _configure_binance_clients` + เรียก `_configure_binance_clients(_BINANCE_HTTP, _BINANCE_DATA_HTTP)` ใน `_lifespan` startup (หลัง init clients) + `_configure_binance_clients(None, None)` ใน shutdown → `_HTTP`/`_DATA_HTTP` ของ binance_client ใช้ pooled clients ร่วมกับ main
- **`exchange/futures_orders.py`** — swap `_signed_request` delegate → import ตรงจาก `exchange.binance_client` (17+ call sites, identity ตรง)
- **`trading/state_ops.py`** — รับ `last_decision_intel()` + `entry_snapshot_from_intel()` (operates บน app_state + VAULT_DIR disk fallback) → main alias + live_guardian/futures_orders import ตรง
- **`trading/symbol_profiles.py`** — unify `_symbol_effective_profile` (เคยอ่าน global `symbol_profiles.json` = **empty หลัง migration**) → ใช้ `PerSymbolStorage.load_symbol_profile()` เดียวกับ main; `_symbol_group`/`_symbol_sample_count` ใช้ PerSymbolStorage เหมือน main (เดิม `_symbol_sample_count` อ่าน global log 90d → count ต่าง: BTCUSDT 20→5); เพิ่ม `holdTrail_base`/`holdMinConf_base` ใน `SYMBOL_GROUP_DEFS` (เดิมไม่มี → `_effective_tp_sl` fallback ต่าง); ลบ import `_SYMBOL_SAMPLE_COUNT_CACHE` unused; verif output `_symbol_effective_profile` main vs symbol_profiles = เท่ากันทุก field
- **`trading/live_guardian.py`** — swap `_symbol_effective_profile` delegate → import ตรงจาก `trading.symbol_profiles`
- **`trading/live_guardian.py`** — swap `_effective_tp_sl` + `_profit_lock_policy` delegates → import ตรงจาก `trading.risk` (impl เดิมย้ายเข้าระหว่าง refactor, identity ตรวจตรงทุก intel case)
- **`main.py` re-apply หลัง revert** — ตอน collasping blank lines ทั่วไฟล์ตอนลบ `_flat_intel_keys` + `_effective_tp_sl` เผลอ `git checkout backend/main.py` (revert งาน main ทั้งหมดที่ยังไม่ commit) → re-apply ใหม่ด้วย AST-targeted deletion (ลบเฉพาะ def blocks + collapse เฉพาะ blank runs ≥4 ไม่ชนทั้งไฟล์): รอบ diff เดิม 1133 deletions (ชน diff 372 hunks) → หลัง re-apply ลดเหลือ ~316 deletions / 9 hunks; defs ที่ลบ: `_fee_edge_min_net_usdt`, `_last_decision_intel`, `_entry_snapshot_from_intel`, `_profit_lock_policy`, `_autotrade_log`, `_agent_mark`, `_flat_intel_keys`, `_effective_tp_sl`, `_calc_tp_sl_prices` — พร้อม alias imports (state_ops/risk/binance_client) + `configure_clients` wiring ใหม่ใน `_lifespan`
- **`exchange/futures_orders.py`** — migrate 6 pure helpers เป็น impl จริงใน module (`_normalize_symbol`, `_floor_to_step`, `_format_qty_by_step`, `_round_to_tick`, `_format_price_by_tick`, `_qty_retry_candidates`) + `_live_lock_key` + `_entry_snapshot_for_position` (ใช้ app_state.AUTO_TRADE ซึ่งเป็น ref เดียวกับ main) — main ลบ 8 defs เดิม + alias import ตรงจาก futures_orders; identity ตรวจตรง; `fetch_mark_price`/`_is_hedge_mode` swap `_main().X` → module-level (source เท่ากัน); เหลือ delegates 2 ตัวใหญ่สุด: `_record_learning_trade`/`_record_learning_trade_async` (learning pipeline — พึ่ง helpers อีก 8 ตัวใน main: `_trade_reward_components`, `_memory_windows_from_trades`, `_weighted_recent_memory_score`, `_symbol_risk_tune_from_recent_trades`, `_auto_update_symbol_profile`, `_mark_trade_learning_agents`, `_append_trade_log`, `_serialize_per_symbol_update` — เป็นงานมิเกรต project-scale ต่อ)
- **`main.py` + `services/app_state.py`** — `RISK`/`DAILY_REALIZED_PNL` migrate ไป app_state: main `RISK = _app_state_sync.RISK` (ref เดียว, `main.RISK is app_state.RISK = True`); `DAILY_REALIZED_PNL`/`_DAILY_PNL_DATE_KEY` → main ใช้ `_app_state_sync.X` ตรงทุกจุด (ลบ module globals เดิมจาก main สลับกับ app_state ซึ่งเป็น .py-main mirror มาก่อน); เปลี่ยน sync block จาก try/except → module-scope ตรง; แก้ bug ซ่อน: `_load_autotrade_snapshot` เคย assign `DAILY_REALIZED_PNL` แบบ local (ไม่มี `global`) → ค่าไม่ restore จริง ตอนนี้เขียนกลับ app_state ถูกต้อง
- **`exchange/futures_orders.py`** — `_guardrails` migrate เป็น impl จริง (เดิม delegate กลับ `_main()` เพราะพึ่ง `RISK`/`DAILY_REALIZED_PNL` runtime-mutated) — หลังย้าย RISK/DAILY ไป app_state → ใช้ `app_state.RISK` + `app_state.DAILY_REALIZED_PNL` ตรงได้; main ลบ def + alias import จาก futures_orders; identity ตรวจตรงทุก branch (kill_switch/max_daily_loss/leverage/notional 403)
- **`main.py`** — unify `_symbol_effective_profile` (main def 6200 shadow import จาก symbol_profiles, source identical) → ลบ def main, alias จาก symbol_profiles; behavioral verify 4 symbols ตรง (BTCUSDT sampleTrades 5 group, ETHUSDT 24 symbol+group)
- **`trading/learning.py`** — **learning pipeline migrate ครบ:** ย้าย `_clamp_float`/`_estimate_trade_edge_usdt`/`_last_decision_entry_metrics`/`_trade_reward_components`/`_mark_trade_learning_agents`/`_auto_update_symbol_profile` (main version = canonical มี loss-streak guard + slPct floor; symbol_profiles:331 duplicate ไม่มี caller) + `_record_learning_trade`/`_record_learning_trade_async` เข้า module; เปลี่ยน `_serialize_per_symbol_update` จาก no-op → real lock (per_symbol_lock) — main ลบ 9 defs + alias import ทั้งหมด; `fs` identity ตรงทุกตัว (ยกเว้น `_app_state_sync.` → `app_state.` intentional); runtime test PAPER pass (record→append_trade_log→commit→mark_agents ด้วยกัน; TV/guardian_lock attach จาก disk)
- **`exchange/futures_orders.py`** — **0 delegates เหลือ:** ลบ `_record_learning_trade`/`_record_learning_trade_async` delegates → import ตรงจาก `trading.learning`; ลบ `_main()` helper (ไม่เหลือ `_main()` call) — main.py เหลือ delegates เฉพาะ order execution (live_guardian 3 ตัว)
- **CRITICAL INCIDENT + กู้คืน `exchange/futures_orders.py`** — state machine script ชนแกนทำให้ไฟล์บูด (`frasync def`) → `git checkout -- exchange/futures_orders.py` เพื่อ undo แต่เผลอ revert งาน uncommitted ทั้งหมดเป็น HEAD delegate version (766 lines, `_main()` + 71 lines delegates) → **reconstruct ใหม่จากชิ้นส่วน**: HEAD fo (order functions จริง `fetch_mark_price`→`_close_position` 84-537) + HEAD main.py (`_close_position_one_side` canonical fill-price + sync learning + helper bodies 9 ตัว incl. `_guardrails` remap `RISK`/`DAILY_REALIZED_PNL` → `app_state.*`) + working main.py (`place_futures_order` main:6631 canonical ไม่มี `mark_price` param ตาม user decision) — result 787 lines, 0 delegates, 0 `_main()`, identity ตรง (`fo._close_position_one_side is main/lg = True`), `_effective_tp_sl` import จาก `trading.risk`, 51 tests ผ่าน; หลังกู้ `place_futures_order` main def ลบเอง ตาม bullet ถัดไป
- **`exchange/futures_orders.py` + `main.py` + `trading/live_guardian.py`** — **`place_futures_order` migrate แล้ว**: main ลบ def (AST-targeted, 128 lines) → alias import จาก fo (พร้อม `_close_position_one_side`/helpers ที่ import อยู่แล้ว); lg ลบ delegate → import ตรง; body = main:6631 canonical (identity verify ก่อน: source identical 128 lines); identity `fo.place_futures_order is main/lg = True`; `_main().place_futures_order` refs เหลือ 0; 51 tests ผ่าน — live_guardian เหลือ 3 delegates
- **`trading/state_ops.py` + `main.py` + `trading/live_guardian.py`** — **`_persist_autotrade_snapshot` migrate แล้ว**: main ลบ def (AST-targeted, 78 lines) → alias import `persist_autotrade_snapshot as _persist_autotrade_snapshot` จาก state_ops; lg ลบ delegate → import ตรง; body ใช้ `app_state.AUTO_TRADE`/`app_state._SNAPSHOT_LAST_FLUSH`/`app_state.DAILY_REALIZED_PNL` แทน main globals (`_SNAPSHOT_LAST_FLUSH` main:2819 ลบด้วย), `_prune_risk_cooldowns(app_state.AUTO_TRADE)` จาก `trading.risk_cooldown` pure version, paths จาก `config_paths.SNAPSHOT_PATH`/`VAULT_DIR` (string เดียวกับ main) — identity `main/lg._persist_autotrade_snapshot is so.persist_autotrade_snapshot = True`; behavioral verify: force flush เขียนไฟล์ payload ครบ + throttle 30s ทำงาน; lg `_main()._pick_live_orphan_positions` 2 call sites (1185/1217) → เรียก lg:120 real impl ตรง (main:5051 เป็น delegate กลับไป lg — ต่างจากโน้ตนึกว่า lg เป็น delegate); 51 tests ผ่าน — live_guardian เหลือ 1 delegate (`intel_analyze`)
- **`analysis/intel_analyze.py` + `main.py` + `trading/live_guardian.py`** — **`intel_analyze` migrate ครบ (last delegate)**: สร้าง leaf module `analysis/intel_analyze.py` (1006 lines) ย้าย 6 funcs verbatim จาก main (`intel_analyze` 3858-4408, `_decision_data_layers` 3766-3855, `_market_momentum` 4481-4519, `_precision_signal_pack` 4522-4721, `_detect_timeframe_patterns` 4724-4774, `_candlestick_pattern_context` 4777-4810) โดย AST-extract จาก main แล้ว **rewrite data-layer refs ผ่าน lazy `_main()`** (patternเดียวกับ `intel_pipeline._load_single_profile`): `_cached_klines`/`_data_get`(9/3 ครั้ง)/`_INTEL_CACHE`/`_INTEL_CACHE_TTL`(8+1)/`AUTO_TRADE`(1)/`_fapi_public_data_base`(1) → `_main().X` — **EXACT behavior (user decision)**: เวลา timeout/attempts/cache dict เป็นของ main ทั้งหมด, ใช้ `app_state.AUTO_TRADE` ref เดียวกับ main.AUTO_TRADE; dir imports: `IntelAnalyzeRequest`(schemas)/`_normalize_symbol`(fo)/`apply_autotrade_defaults`(trading.config)/indicators/`HTTPException`(fastapi) — **ไม่มี import cycle** (schemas/config/indicators/fo ไม่ import main); main ลบ 6 defs (AST, +8/−977 lines) + alias import ครบ 6 ชื่อ; lg ลบ `_main()` helper + delegate ตัวสุดท้าย → `from analysis.intel_analyze import intel_analyze`; identity ตรงทุกตัว (`main.intel_analyze is ia`, `lg.intel_analyze is ia`, `_main() is main`, `_INTEL_CACHE` ref เดียว); behavioral verify: mock `main._data_get`/`_main()._cached_klines` fake klines/depth → intel_analyze compute ครบ schema + cache 2nd call 0 re-fetch; 118 tests ผ่าน (51 tests/ + 67 integration) — เฉพาะ 3 failures pre-existing (verified บน pre-intel code): `test_data_quality_guard_blocks_invalid_core_fields_only`, guardian `test_cache_expired_triggers_fetch`/`test_gather_called_for_multiple_positions` (stale mocks `live_guardian._main` ที่จริง migration ก่อนหน้า switch เป็น direct imports แล้ว) — **live_guardian delegates = 0**
- **`trading/risk.py` + `main.py` + `trading/learning.py`** — **ล้าง `_main()` ที่ค้างสุดท้าย (audit เดิม noted `learning.py:503/719`) + migrate leverage funcs**: ย้าย `_autotrade_leverage_cap`/`_sync_autotrade_leverage_cap_from_cfg`/`_autotrade_leverage_bounds` จาก main → `trading/risk.py` (ใช้ `app_state.RISK` ref ผ่าน `from services import app_state` — identity `main.RISK is app_state.RISK = True`); main ลบ 3 defs (AST, +4/−32) + alias import 3 ชื่อ จาก risk block เดิม; `learning.py:503` `_main()._recent_live_result_streak_state(...)` → `_recent_live_result_streak_state(...)` local (main:205 import จาก learning อยู่แล้ว identity ตรง), :719 `_main()._autotrade_leverage_bounds/cap` → import ตรงจาก `trading.risk`; ลบ `_main()` helper (learning) + dead `_main()` (`supervisor_tuning.py:74` ไม่มี caller); `risk.py._current_max_notional` ลบ lazy `import main` → `app_state.RISK` ตรง; identity verify: `main._autotrade_leverage_* is risk._* = True` + `_current_max_notional()` 200.0 + bounds sync เขียน `app_state.RISK["max_leverage"]`; 51 tests ผ่าน + 3 pre-existing fails เหมือนเดิม — **เหลือ `_main()` เฉพาะใน `analysis/intel_analyze.py` (intentional EXACT-behavior ตาม user decision) + cmux/hermes services**

### สิ่งที่ทำเสร็จแล้ว

**ไฟล์ใหม่ที่สร้างขึ้น:**
- `backend/trading/per_symbol_storage.py` — คลาส `PerSymbolStorage`: จัดการข้อมูลแต่ละ symbol แยกกัน (profile, symbol_profile, trades.jsonl, windows cache, risk_tune cache, vault ops)
- `backend/trading/shared_storage.py` — คลาส `SharedStorage`: ข้อมูลใช้ร่วมกัน (config, risk, daily_stats, global trade log)
- `backend/trading/shared_cache_layer.py` — คลาส `SharedCacheLayer`: in-memory cache พร้อม TTL สำหรับ profiles, windows, risk-tune
- `backend/trading/per_symbol_context.py` — คลาส `PerSymbolContext`: context รวมสำหรับแต่ละ symbol (storage + cache + compute)
- `backend/scripts/migrate_to_per_symbol.py` script ย้ายข้อมูลจาก global ไป per-symbol

**ไฟล์ที่แก้ไข:**
- `backend/main.py` — เพิ่ม imports, แก้ `_record_learning_trade()` และ `_record_symbol_observation()` ให้ใช้ `PerSymbolContext`, แก้ `_update_symbol_note()` ให้เขียนลง per-symbol vault ด้วย, เพิ่ม `_load_single_profile()` และ `_save_single_profile()` helper, แก้ `_scan_health_state()`, `_record_scan_health()`, `_cooldown_scan_symbol()`, `_learned_min_conf()`, `_symbol_quality_score()`, `_auto_update_symbol_profile()` ให้ใช้ per-symbol storage, อัพเดท `/learning/status` API, **ลบ `_load_learning_profiles()`, `_save_learning_profiles()`, `LEARN_PATH`, `_LEARN_PROFILES_BUFFER`, `_LEARN_PROFILES_LAST_FLUSH`**
- `backend/analysis/intel_pipeline.py` — เพิ่ม `_load_single_profile()` wrapper, **ลบ `_load_learning_profiles()` wrapper**
- `backend/services/learning_profiles.py` — เขียนใหม่ทั้งหมดให้ใช้ per-symbol storage, **ลบ `_load_learning_profiles()`, `_save_learning_profiles()`, `_cleanup_stale_profiles()` (เดิม)**
- `backend/services/config_paths.py` — **ลบ `LEARN_PATH`**
- `backend/trading/symbol_profiles.py` — อัพเดท `_auto_update_symbol_profile()` ให้ใช้ `_load_single_profile()`
- `backend/apply_loss_minimize_tune.py` — เขียนใหม่ให้ใช้ `PerSymbolStorage` แทน global file

**Migration ดำเนินการเสร็จแล้ว:**
- ย้าย 216 symbols ไปยัง `obsidian_vault/symbols/{SYMBOL}/`
- สร้าง `obsidian_vault/shared/` สำหรับข้อมูลใช้ร่วมกัน
- **ลบ global `learning_profiles.json` (backup ที่ `learning_profiles.json.bak`)**

**การอัพเดท modules อื่น:**
- `analysis/intel_pipeline.py` — เพิ่ม `_load_single_profile()` wrapper
- `services/learning_profiles.py` — `_load_single_profile()`, `_save_single_profile()`, `_cleanup_stale_profiles()`, `_scan_health_state()`, `_record_scan_health()`, `_cooldown_scan_symbol()`, `_scan_error_penalty()`
- `trading/symbol_profiles.py` — `_auto_update_symbol_profile()` ใช้ `_load_single_profile()`
- `apply_loss_minimize_tune.py` — ใช้ `PerSymbolStorage` แทน global file

**การทดสอบ:**
- Backend import สำเร็จ
- `/learning/status` แสดง 216 symbols
- `/learning/status?symbol=BTCUSDT` แสดง 84 trades
- `/learning/propose-config?symbol=BTCUSDT` ทำงานได้
- `/learning/walk-forward?symbol=BTCUSDT` ทำงานได้
- `/learning/report` ทำงานได้
- `_load_single_profile('NONEXISTENT')` return {} ถูกต้อง
- `_save_single_profile()` + `_load_single_profile()` roundtrip สำเร็จ

### สิ่งที่ยังไม่ได้ทำ (optional/future)
1. ทดสอบกับระบบเทรดจริง (live trading)
2. อัพเดท test files ให้เข้ากับ per-symbol storage

### สถาปัตยกรรมใหม่
```
obsidian_vault/
├── symbols/
│   ├── BTCUSDT/
│   │   ├── profile.json          ← learning profile ของ BTCUSDT
│   │   ├── symbol_profile.json   ← 3-tier symbol profile
│   │   ├── trades.jsonl          ← เทรดของ BTCUSDT เท่านั้น
│   │   ├── windows.json          ← rolling window cache
│   │   ├── risk_tune.json        ← risk tune cache
│   │   └── vault/                ← Obsidian vault ของ BTCUSDT
│   └── ETHUSDT/
│       └── ...
├── shared/
│   ├── config.json               ← config ใช้ร่วมกัน
│   ├── risk.json                 ← risk limits ใช้ร่วมกัน
│   ├── daily_stats.json          ← สถิติรายวัน
│   └── all_trades.jsonl          ← trade log รวม
└── learning_profiles.json        ← (เดิม) จะลบหลัง migration
```

### ประสิทธิภาพที่คาดหวัง
- I/O ลด 90%+ (จากอ่านทั้งหมดทุกครั้ง เหลืออ่านเฉพาะ symbol ที่เกี่ยวข้อง)
- Autotrend แต่ละตัวทำงานอิสระ ไม่กระทบกัน
- เขียน/อ่านเร็วขึ้น vì file เล็กลง
- Cache layer ลด redundant disk reads
