"""Verify the Hermes multi-agent kanban works correctly.

Pinned after a live observation (2026-08-28) where strategy_builder sat in
'blocked' because entries were pre-reversal-guarded — we confirm the kanban
state machine (mark_agent / rebuild_kanban / move_agent_in_kanban) reflects
agent marks accurately and does not corrupt buckets.
"""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import hermes_agents  # noqa: E402


class TestKanbanStateMachine(unittest.TestCase):
    def setUp(self):
        self.state = hermes_agents.new_agent_state()

    def test_initial_kanban_all_todo(self):
        kb = self.state["kanban"]
        self.assertEqual(len(kb["todo"]), 12)
        self.assertEqual(kb["doing"], [])
        self.assertEqual(kb["done"], [])
        self.assertEqual(kb["blocked"], [])

    def test_mark_agent_moves_to_done(self):
        st = hermes_agents.mark_agent(self.state, "market_analyst", "done", "scan completed", "BICOUSDT")
        self.assertIn("market_analyst", st["kanban"]["done"])
        self.assertNotIn("market_analyst", st["kanban"]["todo"])
        self.assertEqual(st["agents"]["market_analyst"]["state"], "done")

    def test_mark_agent_moves_to_blocked(self):
        st = hermes_agents.mark_agent(self.state, "strategy_builder", "blocked",
                                      "no clear symbol", "scan_none")
        self.assertIn("strategy_builder", st["kanban"]["blocked"])
        # live symptom we observed: pre-reversal/scan-none leaves it blocked
        self.assertEqual(st["agents"]["strategy_builder"]["state"], "blocked")

    def test_mark_agent_blocked_then_done_transitions(self):
        st = hermes_agents.mark_agent(self.state, "strategy_builder", "blocked", "pre-reversal")
        st = hermes_agents.mark_agent(st, "strategy_builder", "done", "entry placed", "BICOUSDT")
        self.assertIn("strategy_builder", st["kanban"]["done"])
        self.assertNotIn("strategy_builder", st["kanban"]["blocked"])

    def test_unknown_stage_falls_back_to_todo(self):
        st = hermes_agents.mark_agent(self.state, "risk_manager", "frobnicate", "x")
        self.assertIn("risk_manager", st["kanban"]["todo"])
        self.assertEqual(st["agents"]["risk_manager"]["state"], "todo")

    def test_rebuild_kanban_reads_state_field(self):
        # mutate agent state directly, then rebuild must reflect it
        self.state["agents"]["execution_agent"]["state"] = "doing"
        st = hermes_agents.rebuild_kanban(self.state)
        self.assertIn("execution_agent", st["kanban"]["doing"])

    def test_move_agent_in_kanban_o1(self):
        st = hermes_agents.move_agent_in_kanban(self.state, "risk_manager", "todo", "doing")
        self.assertIn("risk_manager", st["kanban"]["doing"])
        self.assertNotIn("risk_manager", st["kanban"]["todo"])

    def test_mark_dedupe_suppresses_redundant_rebuild(self):
        # same event within dedupe window -> state unchanged, no error
        st1 = hermes_agents.mark_agent(self.state, "memory_agent", "done", "persist snapshot")
        st2 = hermes_agents.mark_agent(st1, "memory_agent", "done", "persist snapshot")
        self.assertIn("memory_agent", st2["kanban"]["done"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
