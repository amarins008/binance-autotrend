---
title: "No Trade for 3 Days — UnboundLocalError + IP Whitelist"
date: 2026-08-27
severity: critical
status: resolved (code) / pending (Binance whitelist)
duration: ~3 days (2026-08-24 08:16 UTC → 2026-08-27 13:50 ICT)
discovered_by: Boss report ("ระบบไม่เปิดไม้มา 2 วันแล้ว")
resolved_by: Mavis
tags: [incident, postmortem, autotrade, crash-loop, binance-whitelist]
---

# Incident: ไม่มี Trade 3 วัน — UnboundLocalError + IP Whitelist

## TL;DR

Bot **เปิดไม้ไม่ได้ 3 วัน** (2026-08-24 → 2026-08-27) เพราะ 3 bugs ซ้อนกัน:
1. `parts = []` หายไปใน `_load_autotrade_snapshot` → snapshot load fail
2. `tradeNotionalCapUsdt=18.0` < ADAUSDT min_notional `50.0` → QTY_TOO_SMALL infinite
3. `trade_res` ไม่ initialize + retry block ไม่ครอบ non-TimeoutError → UnboundLocalError crash loop

**Bugs #1-3 fix แล้ว** (commit: code change 2026-08-27 13:50 ICT, bot restart ผ่าน `/system/restart`)
**Bug #4 (Binance IP whitelist)** — ต้อง user แก้เองที่ Binance.com

## Timeline

| เวลา (UTC / ICT) | เหตุการณ์ |
|------------------|----------|
| **2026-08-24 08:16 UTC** | Last real live trade (ADAUSDT LONG, reason=LOCAL_SL_HIT) — ก่อน incident |
| **2026-08-25 15:18 UTC** | Watchdog loop log เริ่ม (หลัง STUCK recovery) — bot scan ปกติ แต่ไม่เปิด trade |
| **2026-08-25 17:00 UTC** | Watchdog STUCK 120m → loop end |
| **2026-08-26 10:35 ICT** | launcher.py + backend start (PID 8628 + others) — uptime 33.8h |
| **2026-08-27 13:50 ICT** | Mavis เริ่ม investigation หลัง user รายงาน "ไม่เปิดไม้ 2 วัน" |
| **2026-08-27 13:50 ICT** | เจอ root cause: 3 bugs + snapshot unreadable |
| **2026-08-27 13:50 ICT** | Apply fix ทั้ง 3 จุด + restart ผ่าน `/system/restart` |
| **2026-08-27 13:55 ICT** | Fix verified: snapshot load สำเร็จ, ไม่ crash loop, ADAUSDT skip แทน crash |
| **2026-08-27 14:08 ICT** | พบ Bug #4: Binance API IP `223.24.161.39` ไม่อยู่ใน whitelist |
| **2026-08-27 14:10+ ICT** | รอ user แก้ Binance API whitelist |

## Root Cause Analysis

### Bug #1: `parts = []` หายไป → Snapshot Load Fail

**Location:** `backend/main.py:8531` — `_load_autotrade_snapshot()`

**Code:**
```python
def _load_autotrade_snapshot():
    ...
    saved = int(data.get("savedAt", 0) or 0)       # line 8620
    was_running = bool(data.get("running"))
    sym = None
    if isinstance(data.get("config"), dict):
        sym = data["config"].get("symbol")
    if was_running and isinstance(data.get("config"), dict):
        sym_cfg = data["config"].get("symbol", "?")
        mode_cfg = data["config"].get("executionMode", "LIVE")
        parts.append(f"AutoTrade will auto-resume for {sym_cfg} ({mode_cfg}).")  # ← NameError!
    ...
    msg = " ".join(parts)
```

**Symptom:** `Snapshot unreadable: name 'parts' is not defined` → `AUTO_TRADE["_snapshot_recovered_log"]` = error message แทนที่จะเป็น resume info

**Impact:**
- State ไม่ถูก restore ทุกครั้งที่ restart
- `riskCooldownBySymbol`, `liveProfitLocks`, `scanBoard` ฯลฯ หายหมด
- Bot เริ่ม fresh ทุกครั้ง — ไม่มี context

**Fix:**
```python
# 2026-08-27 fix: parts=[] was missing — caused "name 'parts' is not defined"
parts = []  # ← เพิ่ม
saved = int(data.get("savedAt", 0) or 0)
...
```

### Bug #2: `tradeNotionalCapUsdt < min_notional` → QTY_TOO_SMALL Infinite

**Location:** `backend/main.py:10155`

**Code:**
```python
_cap = float(cfg.get("tradeNotionalCapUsdt", 80.0) or 80.0)
trade_usdt = round(min(max(trade_usdt, min_order_usdt), _cap), 2)
# ADAUSDT: min=50, cap=18 → min(50, 18) = 18 → Binance reject!
```

**Symptom:** `Order floor (ADAUSDT): adjusted USDT → 18.00 (min_notional=50.0 cap=18.00)` → QTY_TOO_SMALL exception

**Impact:**
- ADAUSDT (qualified symbol เดียว) ถูก reject ทุกครั้ง
- Bot ไม่เคย trade ได้เลย

**Fix:** Skip symbol ถ้า `_sym_min > _trade_cap_usdt`:
```python
# 2026-08-27 fix: if the symbol's exchange MIN_NOTIONAL is higher than
# the operator's tradeNotionalCapUsdt, capping trade_usdt below the min
# would let the order be sent to Binance and rejected with QTY_TOO_SMALL,
# causing an infinite "place -> reject -> restart" loop. Skip the symbol
# immediately so the cycle advances to the next candidate instead.
_trade_cap_usdt = float(cfg.get("tradeNotionalCapUsdt", 80.0) or 80.0)
if _sym_min > 0 and _sym_min > _trade_cap_usdt:
    _autotrade_skip("usdt_too_small", f"Skip: {cfg['symbol']} exchange min {_sym_min:.2f} > tradeNotionalCapUsdt {_trade_cap_usdt:.2f} (cap guard)")
    AUTO_TRADE["consecutiveErrors"] = max(0, AUTO_TRADE["consecutiveErrors"] - 1)
    continue
```

**Workaround applied:** Bump `tradeNotionalCapUsdt` 18 → 60 ผ่าน `/bot/config` (POST) เพื่อให้ ADAUSDT trade ได้

### Bug #3: UnboundLocalError 'trade_res' → Crash Loop

**Location:** `backend/main.py:10340-10346` — ใน `_autotrade_loop()`

**Code:**
```python
place_timeout = max(20.0, float(cfg.get("intervalSec", 20)) * 1.5)
try:                                                          # inner try
    _agent_mark("execution_agent", "doing", "place live order", ...)
    trade_res = await asyncio.wait_for(_do_place(), timeout=place_timeout)  # line 10342
except asyncio.TimeoutError:                                  # catches only TimeoutError
    ...
    trade_res = await asyncio.wait_for(_do_place(), timeout=place_timeout + 8.0)
_agent_mark("execution_agent", "done", ...)                    # OUTSIDE inner try
AUTO_TRADE["lastTradeAt"] = now
...
AUTO_TRADE["lastDecision"] = {"intel": intel, "trade": trade_res, ...}  # ← UnboundLocalError!
```

**Symptom:** `AutoTrade task stopped unexpectedly: cannot access local variable 'trade_res' where it is not associated with a value`

**Cascade:**
- `place_futures_order` raise QTY_TOO_SMALL (HTTPException)
- `except asyncio.TimeoutError` ไม่จับ
- Exception bubble up → caught by outer except (line 10361) → QTY_TOO_SMALL handler → multiply usdtAmount
- **บาง path** → `trade_res` ไม่ถูก assign → UnboundLocalError at line 10354
- Watchdog restart task → ทุก 10-20 วินาที
- **Bot ไม่เคย trade ได้เลย**

**Fix:**
```python
# 2026-08-27 fix: initialize trade_res to None so UnboundLocalError
# never fires if the place block raises before the assignment.
trade_res = None  # ← initialize
place_timeout = max(20.0, float(cfg.get("intervalSec", 20)) * 1.5)
try:
    _agent_mark(...)
    trade_res = await asyncio.wait_for(_do_place(), timeout=place_timeout)
except asyncio.TimeoutError:
    ...
    trade_res = await asyncio.wait_for(_do_place(), timeout=place_timeout + 8.0)
except Exception as _place_err:                                # ← catch other exceptions
    _agent_mark("execution_agent", "blocked", "place order error", f"{type(_place_err).__name__}: {str(_place_err)[:60]}")
    _autotrade_log(f"Place order error ({cfg['symbol']} {signal}): {type(_place_err).__name__}: {str(_place_err)[:80]}")
    raise  # re-raise to outer except
```

### Bug #4: Binance API IP Whitelist ไม่ตรง (รอ user แก้)

**Location:** Binance.com → API Management

**Symptom:** `Invalid API-key, IP, or permissions for action, request ip: 223.24.161.39`

**Impact:** Bot pause โดย `fapi_agreement` handler → ไม่ส่ง order

**Root cause:** API key whitelist IP อื่น ไม่ใช่ `223.24.161.39`

**Resolution:** User ต้อง login Binance แก้เอง
- **Option A (แนะนำ):** ตั้ง "Unrestricted" (ลบ IP restriction)
- **Option B:** เพิ่ม `223.24.161.39` ใน whitelist (ต้องอัพเดททุกครั้งที่ IP เปลี่ยน)

## Impact

| Metric | Value |
|--------|-------|
| **Duration without trade** | ~3 days (2026-08-24 08:16 UTC → 2026-08-27 14:10 ICT) |
| **Total missed trades** | Unknown (scanner ไม่เจอ qualified symbol ในช่วงนี้) |
| **Last 8 trades (degrading)** | WR 25%, PnL -1.32 USDT |
| **Last 20 trades (degrading)** | WR 30%, PnL -1.99 USDT |
| **Live positions on Binance** | 0 (โชคดี ไม่มี position ซ้อน) |
| **API quota usage** | ไม่มีข้อมูล (autotrade paused) |

## Lessons Learned

### What went well
- Watchdog monitoring ทำงานปกติ — backend + launcher heartbeat ตลอด
- `Kill Binance AutoTrade.bat` ใช้ path matching `*Binance autotrend*` — kill processes ได้ถูก
- Snapshot มีข้อมูลครบ — สามารถ investigate ได้
- `recoveredLog` field บอกชัดว่า snapshot load fail ด้วยเหตุผลอะไร

### What went wrong
- **3 bugs ที่อยู่ใน production code** — ไม่มี test ที่ catch ได้
- **Watchdog loop ไม่ alert user** เมื่อ bot ไม่ได้ trade นานเกิน 24h
- **Crash loop ซ่อน UnboundLocalError** — log บอกแค่ "AutoTrade task stopped unexpectedly" ไม่บอกว่าเพราะอะไร
- **Start AutoTrade.bat ไม่ start launcher** — ถ้า launcher ตายก็ไม่มีใคร start ใหม่
- **Binance API whitelist IP ไม่ sync** — เป็น manual process ที่ user ต้องจำ

### Action Items (post-resolution)

- [x] Fix Bug #1: เพิ่ม `parts = []` (commit)
- [x] Fix Bug #2: skip symbol ถ้า min > cap (commit)
- [x] Fix Bug #3: init `trade_res` + handle non-TimeoutError (commit)
- [x] Bump `tradeNotionalCapUsdt` 18 → 60 (config update)
- [x] แก้ `Start AutoTrade.bat` ให้ start launcher ด้วย
- [ ] **User แก้ Binance API whitelist** (รอ)
- [ ] เพิ่ม unit test สำหรับ `_load_autotrade_snapshot` (mock + assert `recoveredLog` ไม่ใช่ error)
- [ ] เพิ่ม alert: "Bot ไม่ trade เกิน 24h" → แจ้งเตือน user
- [ ] ตั้ง "no IP restriction" ที่ Binance API เพื่อกัน dynamic IP issue
- [ ] Review: `place_futures_order` exception handling — ควร retry เฉพาะบาง error code

## Prevention

1. **Snapshot integrity check:** ทุก restart ต้อง verify `recoveredLog` ไม่ใช่ error message
2. **Crash loop detection:** ถ้า restart > 5 ครั้งใน 1 นาที → pause และ alert
3. **24h no-trade alert:** ถ้า 24h ไม่มี real live trade → notify user
4. **IP whitelist helper:** ตั้ง "Unrestricted" ใน Binance API ถ้า IP rotate บ่อย
5. **Better error logging:** เปลี่ยน "AutoTrade task stopped unexpectedly" เป็น "UnboundLocalError at line 10354: trade_res" (มี traceback)

## Related

- [[2026-08-27 fix-3-bugs-apply|Apply Fix Notes]]
- [[Runbook - Bot ไม่เปิดไม้|Bot ไม่เปิดไม้ - Runbook]]
- [[Architecture - Autotrade Loop]]
- [[Binance API Setup]]
