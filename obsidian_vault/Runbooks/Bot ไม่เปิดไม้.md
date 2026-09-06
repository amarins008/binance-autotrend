---
title: "Runbook — Bot ไม่เปิดไม้"
type: runbook
category: ops
tags: [autotrade, runbook, troubleshooting]
---

# Runbook: Bot ไม่เปิดไม้

## Quick Diagnostic

### 1. ตรวจสอบ bot state
```powershell
# Backend health
Invoke-RestMethod http://localhost:8020/health

# Status overview
Invoke-RestMethod http://localhost:8020/status | ConvertTo-Json

# Quick check
Invoke-RestMethod http://localhost:8020/status/quick
```

### 2. ดู scan board (มี symbol qualified ไหม)
```powershell
$r = Invoke-RestMethod http://localhost:8020/autotrade/status
$r.scanBoard | Format-Table symbol, signal, confidence, qualified, rejectReason -AutoSize
```

**ถ้า qualified = False ทุกตัว** → bot ไม่มี trade candidate (อาจจะปกติ)
**ถ้า rejectReason = "conf_too_high_late"** → confidence > maxEntryConfidence (0.88)
**ถ้า rejectReason = "perf_lock"** → เหรียญถูก lock เพราะ performance แย่

### 3. ดู log ล่าสุด
```powershell
$r = Invoke-RestMethod http://localhost:8020/autotrade/status
$r.log | Select-Object -Last 20 | ForEach-Object {
    $ts = [DateTimeOffset]::FromUnixTimeSeconds($_.ts).ToString('yyyy-MM-dd HH:mm:ss')
    "$ts $($_.msg)"
}
```

**Pattern ที่ต้องระวัง:**

| Pattern | Root Cause | Fix |
|---------|-----------|-----|
| `AutoTrade task stopped unexpectedly: cannot access local variable 'trade_res'` | UnboundLocalError crash loop (Bug #3) | แก้ main.py:10340-10346 |
| `LIVE paused: Binance API key/IP/permission rejected (-2015)` | IP whitelist ไม่ตรง | Login Binance แก้ whitelist |
| `Order floor (X): adjusted USDT → X (min_notional=Y cap=Z)` แล้วตามด้วย `Skip: X exchange min Y > tradeNotionalCapUsdt Z` | Cap < min_notional (Bug #2) | Bump `tradeNotionalCapUsdt` หรือ skip symbol |
| `Snapshot unreadable: name 'parts' is not defined` | Bug #1 — `parts = []` หาย | แก้ main.py:8620 (เพิ่ม `parts = []`) |
| `Skip: scan found no clear symbol` | ไม่มี symbol qualified (อาจจะปกติ) | รอสัญญาณหรือปรับ min_confidence |

### 4. ตรวจสอบ Binance API
```powershell
Invoke-RestMethod http://localhost:8020/debug/binance-auth-check
Invoke-RestMethod http://localhost:8020/debug/binance-positions
```

**ถ้า `ok: false` และมี `-2015`** → IP whitelist issue (ดู section ด้านล่าง)

## การแก้ปัญหาตามอาการ

### Issue 1: Crash Loop (UnboundLocalError 'trade_res')

**อาการ:** Log เต็มไปด้วย "AutoTrade task stopped unexpectedly" ทุก 10-20 วินาที

**ตรวจสอบ:**
```powershell
$r = Invoke-RestMethod http://localhost:8020/autotrade/status
$r.log | Where-Object { $_.msg -match 'trade_res' } | Select-Object -Last 5
```

**แก้ไข:** ดู [[2026-08-27 no-trade-3-days]] — Fix #3 หรือ apply patch ใน main.py:10340-10346

### Issue 2: Binance API IP Whitelist

**อาการ:** `Invalid API-key, IP, or permissions for action, request ip: X.X.X.X`

**ตรวจ IP ปัจจุบัน:**
```powershell
(Invoke-RestMethod https://api.ipify.org?format=json).ip
Invoke-RestMethod http://localhost:8020/api/ip-info
```

**แก้ไข:**
1. ไป https://www.binance.com/en/my/settings/api-management
2. เลือก API key (ขึ้นต้น `FsMzAh...`)
3. **IP access restrictions:**
   - **Unrestricted (แนะนำ):** ลบ IP restriction
   - **Restrict:** เพิ่ม IP ปัจจุบันใน whitelist
4. Save

**Verify:**
```powershell
$r = Invoke-RestMethod http://localhost:8020/debug/binance-auth-check
$r.ok  # ต้องเป็น true
```

### Issue 3: Cap < Min Notional

**อาการ:** `Skip: SYM exchange min 50.00 > tradeNotionalCapUsdt 18.00 (cap guard)`

**แก้ไข Option A (ขอ bump cap):**
```powershell
$body = @{ tradeNotionalCapUsdt = 60.0; autoScanTradeNotionalCapUsdt = 60.0; usdtAmount = 60.0 } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8020/bot/config -Method POST -Body $body -ContentType "application/json"
```

**แก้ไข Option B (deny symbol):**
```powershell
$body = @{ scanDenySymbols = @("ADAUSDT", "KORUUSDT", ...) } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8020/bot/config -Method POST -Body $body -ContentType "application/json"
```

### Issue 4: Snapshot Load Fail

**อาการ:** `recoveredLog: "Snapshot unreadable: ..."` ใน `/autotrade/status` → `continuity.recoveredLog`

**แก้ไข:** ดู main.py บรรทัดที่ขึ้นต้น `_load_autotrade_snapshot` — ตรวจสอบว่า `parts = []` ถูก initialize ก่อน `parts.append(...)`

**Verify:**
```powershell
$r = Invoke-RestMethod http://localhost:8020/autotrade/status
$r.continuity.recoveredLog  # ต้องไม่ขึ้นต้นด้วย "Snapshot unreadable:"
```

### Issue 5: No Qualified Symbol

**อาการ:** `Skip: scan found no clear symbol` ตลอด

**สาเหตุที่เป็นไปได้:**
1. **Market นิ่ง** — confidence ต่ำ → ปกติ รอสัญญาณ
2. **min_confidence สูงเกิน** — ลองลด `minConfidence` หรือ `earlyEntryMinConfidence`
3. **Evening volatility guard** — ช่วง 16-23 ICT → ลด size + เพิ่ม conf
4. **Performance lock** — เหรียญที่ performance แย่ถูก lock 45 นาที → รอ
5. **All symbols denied** — เช็ค `scanDenySymbols` (มี 28+ symbols)

**Verify scan board:**
```powershell
$r = Invoke-RestMethod http://localhost:8020/autotrade/status
$r.scanBoard | Format-Table symbol, signal, confidence, qualified, rejectReason
```

## Operations

### Restart Bot
```powershell
# ใช้ Desktop shortcut
# Double-click: "Start AutoTrade.bat" (kill + start backend + launcher)

# หรือ API
Invoke-RestMethod -Uri http://localhost:8020/system/restart -Method POST
```

### Stop Bot
```powershell
# Double-click: "Kill Binance AutoTrade.bat" (Desktop)
# หรือ
Get-Process python | Where-Object { $_.Path -like '*Binance autotrend*' } | Stop-Process -Force
```

### View Live Positions
```powershell
Invoke-RestMethod http://localhost:8020/debug/binance-positions
```

### View Trades Today
```powershell
$r = Invoke-RestMethod http://localhost:8020/autotrade/status
$r.kpiTodayAllSymbols.live
```

## Configuration Tips

### Bump notional cap (สำหรับ ADAUSDT min=50)
```powershell
$body = @{
    tradeNotionalCapUsdt = 60.0
    autoScanTradeNotionalCapUsdt = 60.0
    usdtAmount = 60.0
} | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8020/bot/config -Method POST -Body $body -ContentType "application/json"
```

### Lower confidence floor (เพื่อให้ trade ง่ายขึ้น)
```powershell
$body = @{
    minConfidence = 0.72
    minConfidenceHardFloor = 0.65
} | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8020/bot/config -Method POST -Body $body -ContentType "application/json"
```

### Add symbol to deny list
```powershell
$r = Invoke-RestMethod http://localhost:8020/autotrade/status
$deny = $r.config.scanDenySymbols + @("NEWUSDT")
$body = @{ scanDenySymbols = $deny } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8020/bot/config -Method POST -Body $body -ContentType "application/json"
```

## Health Check Cron

ถ้าต้องการ monitor แบบเรียลไทม์:
```powershell
# ตรวจทุก 5 นาที — ถ้า bot ไม่ตอบ → alert
while ($true) {
    try {
        $r = Invoke-RestMethod http://localhost:8020/health -TimeoutSec 5
        if (-not $r.ok) { Write-Host "[ALERT] Bot not OK at $(Get-Date)" }
    } catch {
        Write-Host "[ALERT] Bot unreachable at $(Get-Date): $_"
    }
    Start-Sleep -Seconds 300
}
```

## See Also

- [[2026-08-27 no-trade-3-days]] — postmortem ของ incident นี้
- [[Architecture - Autotrade Loop]]
- [[Binance API Setup]]
- [[Per-Symbol Storage Architecture]]
