<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **binance-autotrend-standalone-final** (3735 symbols, 5773 relationships, 132 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

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
