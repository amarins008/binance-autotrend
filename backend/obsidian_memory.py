from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Any


TRADING_VAULT_FOLDERS = (
    "Journal",
    "Strategies",
    "MarketPatterns",
    "Failures",
    "Improvements",
    "Backtests",
    "RiskRules",
    "AI-Thoughts",
    "DailyReports",
)


def trading_vault_dir(vault_dir: Path) -> Path:
    return Path(vault_dir) / "TradingVault"


def ensure_trading_vault(vault_dir: Path) -> Path:
    root = trading_vault_dir(vault_dir)
    root.mkdir(parents=True, exist_ok=True)
    for folder in TRADING_VAULT_FOLDERS:
        (root / folder).mkdir(parents=True, exist_ok=True)
    index = root / "README.md"
    if not index.exists():
        index.write_text(
            "\n".join(
                [
                    "# TradingVault",
                    "",
                    "Hermes long-term memory for the self-improving trading bot.",
                    "",
                    "## Structure",
                    "- Journal: chronological events and trade observations",
                    "- Strategies: active strategy notes and hypotheses",
                    "- MarketPatterns: symbol behavior and pattern memory",
                    "- Failures: loss reviews and mistakes to avoid",
                    "- Improvements: applied tuning and optimization history",
                    "- Backtests: validation and walk-forward findings",
                    "- RiskRules: active safety rules and cooldown logic",
                    "- AI-Thoughts: Hermes reflections and hypotheses",
                    "- DailyReports: daily summaries for human review",
                    "",
                ]
            ),
            encoding="utf-8",
        )
    return root


def safe_note_name(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value or "").strip())
    return text.strip("-") or "UNKNOWN"


def today_key(now: int | None = None) -> str:
    return time.strftime("%Y-%m-%d", time.localtime(int(now or time.time())))


def append_section(path: Path, heading: str, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "\n".join([f"## {heading}", *lines, ""])
    if path.exists():
        existing = path.read_text(encoding="utf-8")
        sep = "" if existing.endswith("\n") else "\n"
        path.write_text(existing + sep + body, encoding="utf-8")
    else:
        path.write_text(f"# {path.stem}\n\n{body}", encoding="utf-8")


def write_symbol_memory(vault_dir: Path, symbol: str, profile: dict[str, Any], last_trade: dict[str, Any] | None = None) -> Path:
    root = ensure_trading_vault(vault_dir)
    symbol_name = safe_note_name(symbol).upper()
    wins = int(profile.get("wins", 0) or 0)
    losses = int(profile.get("losses", 0) or 0)
    trades = int(profile.get("trades", 0) or 0)
    total = wins + losses
    win_rate = round((wins / total) * 100, 2) if total > 0 else 0.0
    pnl = round(float(profile.get("realizedPnl", 0.0) or 0.0), 6)
    observations = int(profile.get("observations", 0) or 0)
    picked = int(profile.get("pickedCount", 0) or 0)
    pick_rate = round((picked / observations) * 100, 2) if observations > 0 else 0.0
    lines = [
        f"# {symbol_name}",
        "",
        "## Memory Type",
        "- Folder: MarketPatterns",
        "- Owner: Hermes AI Decision Engine",
        "",
        "## Performance",
        f"- Wins: {wins}",
        f"- Losses: {losses}",
        f"- Trades: {trades}",
        f"- WinRate: {win_rate}%",
        f"- RealizedPnL: {pnl}",
        f"- RewardScore: {round(float(profile.get('rewardScore', 0.0) or 0.0), 6)}",
        "",
        "## Scan Behavior",
        f"- Observations: {observations}",
        f"- PickedCount: {picked}",
        f"- PickRate: {pick_rate}%",
        f"- LastSignal: {profile.get('lastSignal', 'WAIT')}",
        f"- LastConfidence: {profile.get('lastConfidence', 0.0)}",
        f"- LastScanScore: {profile.get('lastScanScore', 0.0)}",
        f"- UpdatedAt: {int(time.time())}",
        "",
        "## Linked Memory",
        f"- [[../Journal/{today_key()}]]",
        f"- [[../DailyReports/{today_key()}]]",
        "",
    ]
    if last_trade:
        lines += [
            "## Last Trade",
            f"- Side: {last_trade.get('side')}",
            f"- Entry: {last_trade.get('entry')}",
            f"- Exit: {last_trade.get('exit')}",
            f"- PnL: {last_trade.get('pnl')}",
            f"- Reason: {last_trade.get('reason')}",
            f"- ClosedAt: {last_trade.get('closedAt')}",
            f"- PatternTags: {last_trade.get('patternTags', [])}",
            f"- PatternBias: {last_trade.get('patternBias', 0.0)}",
            f"- PatternScore: {last_trade.get('patternScore', 0.0)}",
            "",
        ]
    path = root / "MarketPatterns" / f"{symbol_name}.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def append_trade_memory(vault_dir: Path, trade: dict[str, Any], mode: str) -> None:
    root = ensure_trading_vault(vault_dir)
    now = int(trade.get("closedAt") or trade.get("ts") or time.time())
    day = today_key(now)
    symbol = safe_note_name(str(trade.get("symbol", "UNKNOWN"))).upper()
    pnl = float(trade.get("pnl", 0.0) or 0.0)
    side = str(trade.get("side", "-") or "-")
    reason = str(trade.get("reason", "-") or "-")
    outcome = "WIN" if pnl >= 0 else "LOSS"
    lines = [
        f"- Time: {time.strftime('%H:%M:%S', time.localtime(now))}",
        f"- Symbol: [[../MarketPatterns/{symbol}|{symbol}]]",
        f"- Mode: {str(mode).upper()}",
        f"- Side: {side}",
        f"- Outcome: {outcome}",
        f"- PnL: {round(pnl, 6)}",
        f"- Reason: {reason}",
    ]
    append_section(root / "Journal" / f"{day}.md", f"{symbol} {outcome}", lines)
    append_section(root / "DailyReports" / f"{day}.md", f"{symbol} trade", lines)
    if pnl < 0:
        append_section(
            root / "Failures" / f"{day}-{symbol}.md",
            "Failure Review",
            [
                *lines,
                "- Reflection: Hermes should compare this loss with recent scan context before next entry.",
                "- Next Check: raise confidence, reduce size, or pause symbol if loss pattern repeats.",
            ],
        )
    else:
        append_section(
            root / "Improvements" / f"{day}-{symbol}.md",
            "Positive Reinforcement",
            [
                *lines,
                "- Learning: reinforce the setup only if follow-up trades keep positive expectancy.",
            ],
        )


def append_scan_memory(vault_dir: Path, symbol: str, intel: dict[str, Any], chosen: bool, score: float) -> None:
    if not chosen:
        return
    root = ensure_trading_vault(vault_dir)
    day = today_key()
    symbol_name = safe_note_name(symbol).upper()
    append_section(
        root / "Journal" / f"{day}.md",
        f"{symbol_name} scan picked",
        [
            f"- Symbol: [[../MarketPatterns/{symbol_name}|{symbol_name}]]",
            f"- Signal: {intel.get('signal', 'WAIT')}",
            f"- Confidence: {intel.get('confidence', 0.0)}",
            f"- Score: {round(float(score or 0.0), 6)}",
            "- Memory Intent: candidate selected for future outcome comparison.",
        ],
    )


def write_self_review_memory(vault_dir: Path, review: dict[str, Any]) -> None:
    root = ensure_trading_vault(vault_dir)
    now = int(review.get("ts") or time.time())
    day = today_key(now)
    loss_streak = int(review.get("lossStreak", 0) or 0)
    actions = [str(x) for x in (review.get("actions") or [])]
    action_text = ", ".join(actions) if actions else "no new action"
    cause_category = str(review.get("causeCategory", "market_strategy") or "market_strategy")
    if cause_category == "infra_auth":
        hypothesis = "Infrastructure/auth control issue; do not tune market strategy from this window."
        operator_action = str(review.get("operatorAction", "") or "Recheck exchange API permission and whitelist IP.")
    else:
        hypothesis = "current regime or entry filter may be misaligned."
        operator_action = ""
    lines = [
        f"- Time: {time.strftime('%H:%M:%S', time.localtime(now))}",
        f"- LossStreak: {loss_streak}",
        f"- CauseCategory: {cause_category}",
        f"- CauseTitle: {review.get('causeTitle', '')}",
        f"- CauseDetail: {review.get('causeDetail', '')}",
        f"- Hypothesis: {hypothesis}",
        f"- OperatorAction: {operator_action}",
        f"- Actions: {action_text}",
        f"- MinConfidence: {review.get('minConfidence')}",
        f"- MaxOpenPositions: {review.get('maxOpenPositions')}",
        f"- NoTradeWindows: {review.get('noTradeWindows', [])}",
    ]
    append_section(root / "AI-Thoughts" / f"{day}.md", "Self Review", lines)
    append_section(root / "RiskRules" / "ActiveRiskRules.md", f"Review {day}", lines)
    append_section(root / "Backtests" / f"{day}-loss-streak.md", "Loss Window Review", lines)
    if actions:
        append_section(root / "Improvements" / f"{day}-self-review.md", "Applied Optimization", lines)
