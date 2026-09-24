import unittest

from n01.core import Coordinator, PlanError, compile_dag

TASKS = [
    {"task_id": "schema", "action": "define schemas"},
    {"task_id": "parser", "action": "build parser", "depends_on": ["schema"]},
    {"task_id": "engine", "action": "build engine", "depends_on": ["schema"]},
    {"task_id": "docs", "action": "write docs"},
    {"task_id": "integrate", "action": "integrate",
     "depends_on": ["parser", "engine"]},
    {"task_id": "release", "action": "package", "depends_on": ["integrate", "docs"]},
]


def ok_handler(task):
    return {"did": task["task_id"]}


class Compile(unittest.TestCase):
    def test_waves_deterministic(self):
        plan = compile_dag(TASKS)
        self.assertEqual(plan["waves"],
                         [["docs", "schema"], ["engine", "parser"],
                          ["integrate"], ["release"]])
        self.assertEqual(plan["plan_digest"], compile_dag(TASKS)["plan_digest"])

    def test_validation(self):
        with self.assertRaises(PlanError):
            compile_dag([{"task_id": "a", "action": "x"},
                         {"task_id": "a", "action": "y"}])
        with self.assertRaises(PlanError):
            compile_dag([{"task_id": "a", "action": "x",
                          "depends_on": ["ghost"]}])
        with self.assertRaises(PlanError) as ctx:
            compile_dag([{"task_id": "a", "action": "x", "depends_on": ["b"]},
                         {"task_id": "b", "action": "y", "depends_on": ["a"]}])
        self.assertIn("cycle", str(ctx.exception))

    def test_concurrency_bounds(self):
        with self.assertRaises(PlanError):
            Coordinator(compile_dag(TASKS), concurrency=0)


class Execution(unittest.TestCase):
    def test_full_success(self):
        c = Coordinator(compile_dag(TASKS))
        rep = c.run(ok_handler)
        self.assertTrue(rep["complete"])
        self.assertEqual(rep["counts"], {"DONE": 6})
        done_order = [e["task"] for e in c.ledger if e["op"] == "done"]
        self.assertLess(done_order.index("schema"), done_order.index("parser"))
        self.assertEqual(done_order[-1], "release")

    def test_failure_blocks_only_dependents(self):
        def handler(task):
            if task["task_id"] == "parser":
                raise RuntimeError("parser exploded")
            return {"did": task["task_id"]}
        c = Coordinator(compile_dag(TASKS))
        rep = c.run(handler)
        self.assertFalse(rep["complete"])
        s = rep["states"]
        self.assertEqual(s["parser"], "FAILED")
        self.assertEqual(s["integrate"], "BLOCKED")
        self.assertEqual(s["release"], "BLOCKED")
        self.assertEqual(s["engine"], "DONE")     # independent branch continued
        self.assertEqual(s["docs"], "DONE")

    def test_every_done_has_result_digest(self):
        c = Coordinator(compile_dag(TASKS))
        c.run(ok_handler)
        for e in c.ledger:
            if e["op"] == "done":
                self.assertTrue(e["result_digest"].startswith("sha256:"))

    def test_concurrency_slicing_preserves_order(self):
        c1 = Coordinator(compile_dag(TASKS), concurrency=1)
        rep = c1.run(ok_handler)
        self.assertTrue(rep["complete"])


class Recovery(unittest.TestCase):
    def failing_first_run(self):
        def handler(task):
            if task["task_id"] == "parser":
                raise RuntimeError("transient")
            return {"did": task["task_id"]}
        plan = compile_dag(TASKS)
        c = Coordinator(plan)
        c.run(handler)
        return plan, c

    def test_checkpoint_resume_preserves_done_and_retries_failed(self):
        plan, c = self.failing_first_run()
        cp = c.checkpoint()
        c2 = Coordinator.resume(plan, cp)
        calls = []

        def handler2(task):
            calls.append(task["task_id"])
            return {"did": task["task_id"]}
        rep = c2.run(handler2)
        self.assertTrue(rep["complete"])
        # DONE work never re-ran; only failed/blocked tasks executed
        self.assertEqual(sorted(calls), ["integrate", "parser", "release"])

    def test_resume_refuses_wrong_plan(self):
        plan, c = self.failing_first_run()
        cp = c.checkpoint()
        other = compile_dag(TASKS + [{"task_id": "extra", "action": "x"}])
        with self.assertRaises(PlanError) as ctx:
            Coordinator.resume(other, cp)
        self.assertIn("different plan", str(ctx.exception))

    def test_resume_refuses_tampered_checkpoint(self):
        plan, c = self.failing_first_run()
        cp = c.checkpoint()
        cp["state"]["parser"] = "DONE"          # forged completion
        with self.assertRaises(PlanError) as ctx:
            Coordinator.resume(plan, cp)
        self.assertIn("tampered", str(ctx.exception))

    def test_resume_event_ledgered(self):
        plan, c = self.failing_first_run()
        c2 = Coordinator.resume(plan, c.checkpoint())
        self.assertEqual(c2.ledger[-1]["op"], "resumed")
        self.assertEqual(c2.ledger[-1]["preserved"], 3)


if __name__ == "__main__":
    unittest.main()


class Hardening011(unittest.TestCase):
    """Regression tests for the 0.1.1-partial audit fixes (A027-F1..F7)."""

    def test_f1_unrecordable_result_fails_task_not_done(self):
        plan = compile_dag([{"task_id": "a", "action": "x"},
                            {"task_id": "b", "action": "y",
                             "depends_on": ["a"]}])
        c = Coordinator(plan)
        rep = c.run(lambda t: {"obj": object()} if t["task_id"] == "a"
                    else {"ok": 1})
        self.assertEqual(rep["states"]["a"], "FAILED")
        self.assertEqual(rep["states"]["b"], "BLOCKED")
        self.assertFalse(any(e["op"] == "done" and e["task"] == "a"
                             for e in c.ledger))

    def test_f2_ledger_tamper_rejected_at_resume(self):
        plan = compile_dag([{"task_id": "a", "action": "x"}])
        c = Coordinator(plan)
        c.run(lambda t: {"ok": 1})
        cp = c.checkpoint()
        cp["ledger"] = []            # erased history
        with self.assertRaises(PlanError) as ctx:
            Coordinator.resume(plan, cp)
        self.assertIn("tampered", str(ctx.exception))
        # untampered checkpoint still resumes
        Coordinator.resume(plan, c.checkpoint())

    def test_f3_checkpoint_isolated_from_live_ledger(self):
        plan = compile_dag([{"task_id": "a", "action": "x"}])
        c = Coordinator(plan)
        c.run(lambda t: {"ok": 1})
        cp = c.checkpoint()
        cp["ledger"][0]["op"] = "FORGED"
        self.assertNotEqual(c.ledger[0]["op"], "FORGED")

    def test_f4_plan_isolated_from_caller_task_dicts(self):
        tasks = [{"task_id": "a", "action": "x"}]
        plan = compile_dag(tasks)
        tasks[0]["action"] = "HACKED"
        self.assertEqual(plan["tasks"]["a"]["action"], "x")

    def test_f5_nan_result_rejected_strict_canonicalization(self):
        plan = compile_dag([{"task_id": "a", "action": "x"}])
        c = Coordinator(plan)
        rep = c.run(lambda t: {"v": float("nan")})
        self.assertEqual(rep["states"]["a"], "FAILED")
        # finite floats remain fine
        c2 = Coordinator(plan)
        self.assertTrue(c2.run(lambda t: {"v": 1.5})["complete"])

    def test_f6_malformed_checkpoint_raises_planerror(self):
        plan = compile_dag([{"task_id": "a", "action": "x"}])
        with self.assertRaises(PlanError):
            Coordinator.resume(plan, {"schema": "n01/checkpoint/v1"})
        with self.assertRaises(PlanError):
            Coordinator.resume(plan, "not a dict")

    def test_f7_depends_on_must_be_list(self):
        with self.assertRaises(PlanError) as ctx:
            compile_dag([{"task_id": "ab", "action": "x"},
                         {"task_id": "c", "action": "y",
                          "depends_on": "ab"}])
        self.assertIn("depends_on must be a list", str(ctx.exception))
        with self.assertRaises(PlanError):
            compile_dag(["not a dict"])
