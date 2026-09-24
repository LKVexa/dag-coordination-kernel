import copy
import itertools
import json
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from n01.core import Coordinator, PlanError, IntegrityError, compile_dag, _digest


def tasks():
    return [{"task_id": "a", "action": "prepare", "inputs": {"n": 1}},
            {"task_id": "b", "action": "build", "depends_on": ["a"]},
            {"task_id": "c", "action": "independent"}]


def resign(cp):
    cp["checkpoint_digest"] = _digest({k: v for k, v in cp.items() if k != "checkpoint_digest"})
    return cp


class Compilation(unittest.TestCase):
    def test_plan_hash_binds_action_and_all_inputs(self):
        baseline = compile_dag(tasks())["plan_digest"]
        for change in (lambda t: t[0].update(action="different"),
                       lambda t: t[0]["inputs"].update(n=2),
                       lambda t: t[0].update(extra="metadata")):
            changed = tasks()
            change(changed)
            self.assertNotEqual(baseline, compile_dag(changed)["plan_digest"])

    def test_order_and_missing_empty_dependencies_normalize(self):
        a = tasks() + [{"task_id": "d", "action": "join", "depends_on": ["c", "b"]}]
        b = copy.deepcopy(a[::-1])
        for t in b:
            t["depends_on"] = list(reversed(t.get("depends_on", [])))
        self.assertEqual(compile_dag(a), compile_dag(b))

    def test_empty_and_nonlist_tasks_refused(self):
        for value in ([], None, {}, (), "tasks"):
            with self.subTest(value=value), self.assertRaises(PlanError):
                compile_dag(value)

    def test_id_and_action_validation(self):
        for field, values in (("task_id", [[], True, 1, "", " a", "a\n", "a"*129, "é"*65, "\ud800"]),
                              ("action", [None, {}, " ", "a"*4097, "x\x00"])):
            for value in values:
                with self.subTest(field=field, value=value), self.assertRaises(PlanError):
                    compile_dag([dict(task_id="a", action="x", **{}) | {field: value}])

    def test_dependency_duplicates_and_types(self):
        for value in (["a", "a"], [1], [[]], ("a",), "a", None):
            with self.subTest(value=value), self.assertRaises(PlanError):
                compile_dag([{"task_id": "a", "action": "x"},
                             {"task_id": "b", "action": "y", "depends_on": value}])

    def test_cycles_and_downstream_are_unscheduled(self):
        ts = [{"task_id": "a", "action": "x", "depends_on": ["b"]},
              {"task_id": "b", "action": "x", "depends_on": ["a"]},
              {"task_id": "c", "action": "x", "depends_on": ["b"]}]
        with self.assertRaisesRegex(PlanError, "unscheduled: a, b, c"):
            compile_dag(ts)
        with self.assertRaises(PlanError):
            compile_dag([{"task_id": "a", "action": "x", "depends_on": ["a"]}])

    def test_metadata_strict_json(self):
        for value in ({1: "x"}, (1, 2), object(), float("inf"), 2**53, "\ud800"):
            with self.subTest(value=value), self.assertRaises(PlanError):
                compile_dag([{"task_id": "a", "action": "x", "value": value}])

    def test_json_cycles_depth_count_and_bytes(self):
        cycle = []; cycle.append(cycle)
        deep = []
        for _ in range(20): deep = [deep]
        for value in (cycle, deep, [0]*10001, "x"*65536, "é"*12000):
            with self.assertRaises(PlanError):
                compile_dag([{"task_id": "a", "action": "x", "value": value}])

    def test_task_and_edge_caps(self):
        with self.assertRaises(PlanError):
            compile_dag([{"task_id": str(i), "action": "x"} for i in range(501)])
        with patch("n01.core.MAX_EDGES", 0), self.assertRaises(PlanError):
            compile_dag(tasks())

    def test_compiled_plan_byte_budget(self):
        with patch("n01.core.MAX_PLAN_BYTES", 100), self.assertRaises(PlanError):
            compile_dag(tasks())

    def test_long_chain_is_iterative(self):
        ts = [{"task_id": str(i), "action": "x", "depends_on": [str(i-1)] if i else []} for i in range(500)]
        plan = compile_dag(ts)
        self.assertEqual(len(plan["waves"]), 500)
        self.assertEqual(Coordinator(plan).verify()["verdict"], "PASS")

    def test_all_64_four_node_dags_respect_dependencies(self):
        edges = list(itertools.combinations("abcd", 2))
        for mask in range(64):
            deps = {t: [] for t in "abcd"}
            for bit, (a, b) in enumerate(edges):
                if mask & (1 << bit): deps[b].append(a)
            plan = compile_dag([{"task_id": t, "action": "x", "depends_on": deps[t]} for t in "dcba"])
            seen = []
            def run(t):
                self.assertTrue(set(deps[t["task_id"]]) <= set(seen))
                seen.append(t["task_id"])
                return {}
            c = Coordinator(plan)
            self.assertTrue(c.run(run)["complete"])
            self.assertEqual(len(seen), 4)
            self.assertEqual(c.verify()["verdict"], "PASS")

    def test_compiled_structure_cannot_be_forged_even_rehashed(self):
        for mutate in (lambda p: p["waves"].reverse(),
                       lambda p: p["dependents"].update(a=[]),
                       lambda p: p.update(schema="n01/plan/v1"),
                       lambda p: p["tasks"]["a"].update(task_id="other"),
                       lambda p: p.update(extra=True)):
            p = compile_dag(tasks()); mutate(p)
            p["plan_digest"] = _digest({k: v for k, v in p.items() if k != "plan_digest"})
            with self.assertRaises(PlanError): Coordinator(p)

    def test_concurrency_exact_integer(self):
        p = compile_dag(tasks())
        for n in (True, 1.0, "1", 0, 65, None):
            with self.assertRaises(PlanError): Coordinator(p, n)
        c = Coordinator(p, 64)
        with self.assertRaises(AttributeError): c.concurrency = 1


class Execution(unittest.TestCase):
    def test_plan_state_ledger_and_handler_inputs_are_detached(self):
        p = compile_dag(tasks()); c = Coordinator(p)
        p["tasks"]["a"]["action"] = "changed"
        c.plan["tasks"]["a"]["action"] = "changed"
        c.state["a"] = "DONE"
        def handler(t):
            t["action"] = "changed"
            return {"id": t["task_id"]}
        c.run(handler)
        c.ledger[0]["op"] = "changed"
        self.assertEqual(c.plan["tasks"]["a"]["action"], "prepare")
        self.assertEqual(c.verify()["verdict"], "PASS")

    def test_handler_result_requires_bounded_strict_object(self):
        for result in (None, [], 3, {1: "x"}, {"x": (1,)}, {"x": 2**53}, {"x": "a"*65536}):
            c = Coordinator(compile_dag(tasks()))
            r = c.run(lambda t: result)
            self.assertEqual(r["states"], {"a": "FAILED", "b": "BLOCKED", "c": "FAILED"})
            self.assertEqual(c.verify()["verdict"], "PASS")

    def test_result_receipt_exact_canonical_digest_and_size(self):
        c = Coordinator(compile_dag(tasks()))
        value = {"z": [1, True, None, 1.5], "a": "é"}
        c.run(lambda t: value)
        done = [e for e in c.ledger if e["op"] == "done"]
        self.assertTrue(all(e["result_digest"] == _digest(value) for e in done))
        self.assertEqual(done[0]["result_bytes"], len(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()))
        self.assertNotIn("result", done[0])

    def test_failure_never_calls_exception_string(self):
        class SecretError(Exception):
            def __str__(self): raise RuntimeError("do not format")
        def handler(t): raise SecretError("SECRET")
        c = Coordinator(compile_dag(tasks()))
        c.run(handler)
        self.assertNotIn("SECRET", json.dumps(c.ledger))
        self.assertEqual(c.ledger[1]["reason"], "HANDLER_ERROR")

    def test_repeat_run_does_not_retry_until_resume(self):
        c = Coordinator(compile_dag(tasks()))
        def bad(t): raise ValueError()
        c.run(bad)
        before = c.checkpoint()
        self.assertFalse(c.run(lambda t: self.fail("unexpected rerun"))["complete"])
        self.assertEqual(c.checkpoint(), before)
        resumed = Coordinator.resume(c.plan, before)
        self.assertTrue(resumed.run(lambda t: {})["complete"])

    def test_interrupt_records_failed_and_propagates(self):
        for exception in (KeyboardInterrupt, SystemExit):
            c = Coordinator(compile_dag(tasks()))
            def interrupt(t): raise exception()
            with self.assertRaises(exception): c.run(interrupt)
            self.assertEqual(c.state, {"a": "FAILED", "b": "PENDING", "c": "PENDING"})
            self.assertEqual(c.ledger[-1]["reason"], "INTERRUPTED")
            self.assertEqual(c.verify()["verdict"], "PASS")
            self.assertTrue(Coordinator.resume(c.plan, c.checkpoint()).run(lambda t: {})["complete"])

    def test_reentrant_run_and_inflight_checkpoint_refused(self):
        c = Coordinator(compile_dag(tasks()))
        def handler(t):
            with self.assertRaises(PlanError): c.run(lambda t: {})
            with self.assertRaises(PlanError): c.checkpoint()
            self.assertEqual(c.state[t["task_id"]], "RUNNING")
            self.assertEqual(c.verify()["verdict"], "PASS")
            return {}
        self.assertTrue(c.run(handler)["complete"])

    def test_concurrent_runs_do_not_duplicate_execution(self):
        c = Coordinator(compile_dag(tasks())); calls = []
        def handler(t): calls.append(t["task_id"]); return {}
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: c.run(handler), range(4)))
        self.assertEqual(calls, ["a", "c", "b"])
        self.assertTrue(all(r["complete"] for r in results))

    def test_capacity_refuses_before_handler_and_leaves_state(self):
        for variable, limit in (("MAX_EVENTS", 5), ("MAX_LEDGER_BYTES", 100)):
            c = Coordinator(compile_dag(tasks())); before = c.checkpoint()
            with patch("n01.core."+variable, limit), self.assertRaises(PlanError):
                c.run(lambda t: self.fail("handler called without receipt capacity"))
            self.assertEqual(c.checkpoint(), before)

    def test_exact_event_reservation_allows_terminal_receipts(self):
        c = Coordinator(compile_dag(tasks()))
        with patch("n01.core.MAX_EVENTS", 6):
            self.assertTrue(c.run(lambda t: {})["complete"])
            self.assertEqual(len(c.ledger), 6)
            self.assertTrue(c.run(lambda t: self.fail("rerun"))["complete"])

    def test_noncallable_handler_is_atomic(self):
        c = Coordinator(compile_dag(tasks())); before = c.checkpoint()
        with self.assertRaises(PlanError): c.run(None)
        self.assertEqual(c.checkpoint(), before)

    def test_callback_corruption_is_not_resealed_on_return_or_exception(self):
        for raises in (False, True):
            c = Coordinator(compile_dag(tasks()))
            def handler(t):
                c._state["b"] = "DONE"
                if raises: raise ValueError()
                return {}
            with self.assertRaises(IntegrityError): c.run(handler)
            self.assertEqual(c.verify()["verdict"], "FAIL")
            with self.assertRaises(IntegrityError): c.checkpoint()

    def test_all_retained_state_tampering_blocks_use(self):
        for mutate in (lambda c: c._state.update(a="DONE"),
                       lambda c: c._plan["tasks"]["a"].update(action="evil"),
                       lambda c: c._ledger.append({}),
                       lambda c: setattr(c, "_concurrency", 1)):
            c = Coordinator(compile_dag(tasks())); mutate(c)
            self.assertEqual(c.verify()["verdict"], "FAIL")
            for action in (c.report, c.checkpoint, lambda: c.run(lambda t: {}), lambda: c.plan):
                with self.assertRaises(IntegrityError): action()

    def test_report_digest_and_detachment(self):
        c = Coordinator(compile_dag(tasks())); report = c.run(lambda t: {})
        self.assertEqual(report["report_digest"], _digest({k: v for k, v in report.items() if k != "report_digest"}))
        self.assertEqual(report["execution_mode"], "sequential")
        self.assertFalse(report["exactly_once_effects"])
        report["states"]["a"] = "PENDING"
        self.assertEqual(c.state["a"], "DONE")

    def test_deleted_private_state_fails_closed(self):
        c = Coordinator(compile_dag(tasks()))
        del c._plan
        self.assertEqual(c.verify()["verdict"], "FAIL")
        with self.assertRaises(IntegrityError): c.checkpoint()


class Recovery(unittest.TestCase):
    def setUp(self):
        self.plan = compile_dag(tasks())
        c = Coordinator(self.plan); c.run(lambda t: {"id": t["task_id"]})
        self.cp = c.checkpoint()

    def refuse(self, cp):
        with self.assertRaises(PlanError): Coordinator.resume(self.plan, resign(cp))

    def test_action_and_input_changes_refuse_resume(self):
        for field in ("action", "inputs"):
            ts = tasks(); ts[0][field] = "changed"
            with self.assertRaisesRegex(PlanError, "different plan"):
                Coordinator.resume(compile_dag(ts), self.cp)

    def test_rehashed_done_without_receipt_refused(self):
        self.cp["ledger"] = []
        self.refuse(self.cp)

    def test_extra_missing_unknown_states_refused(self):
        for state in ({}, {"a": "DONE", "b": "DONE", "c": "DONE", "d": "DONE"},
                      {"a": "RUNNING", "b": "DONE", "c": "DONE"},
                      {"a": True, "b": "DONE", "c": "DONE"}):
            cp = copy.deepcopy(self.cp); cp["state"] = state; self.refuse(cp)

    def test_schema_unknown_fields_and_malformed_shapes(self):
        for change in ({"schema": "n01/checkpoint/v1"}, {"other": 1}, {"ledger": {}}, {"state": []}):
            cp = copy.deepcopy(self.cp); cp.update(change); self.refuse(cp)

    def test_sequence_type_gaps_duplicates(self):
        for seq in (True, 1.0, 0, 99):
            cp = copy.deepcopy(self.cp); cp["ledger"][0]["seq"] = seq; self.refuse(cp)

    def test_wrong_task_wave_and_extra_event_fields(self):
        for change in ({"task": "ghost"}, {"wave": 2}, {"wave": True}, {"extra": 1}, {"op": "hacked"}):
            cp = copy.deepcopy(self.cp); cp["ledger"][0].update(change); self.refuse(cp)

    def test_result_receipt_bounds(self):
        for change in ({"result_digest": "sha256:bad"}, {"result_bytes": True}, {"result_bytes": 0}, {"result_bytes": 65537}):
            cp = copy.deepcopy(self.cp); cp["ledger"][1].update(change); self.refuse(cp)

    def test_done_without_start_refused(self):
        cp = copy.deepcopy(self.cp); cp["ledger"].pop(0)
        for i, e in enumerate(cp["ledger"], 1): e["seq"] = i
        self.refuse(cp)

    def test_start_before_dependency_done_refused(self):
        cp = copy.deepcopy(self.cp)
        cp["ledger"] = cp["ledger"][4:] + cp["ledger"][:4]
        for i, e in enumerate(cp["ledger"], 1): e["seq"] = i
        self.refuse(cp)

    def test_block_without_upstream_failure_refused(self):
        cp = Coordinator(self.plan).checkpoint()
        cp["ledger"] = [{"seq": 1, "op": "blocked", "task": "a", "wave": 1}]
        cp["state"]["a"] = "BLOCKED"
        self.refuse(cp)

    def test_repeated_start_of_done_task_refused(self):
        cp = copy.deepcopy(self.cp)
        cp["ledger"].append({"seq": 7, "op": "start", "task": "a", "wave": 1})
        self.refuse(cp)

    def test_unfinished_start_refused(self):
        cp = Coordinator(self.plan).checkpoint()
        cp["ledger"] = [{"seq": 1, "op": "start", "task": "a", "wave": 1}]
        self.refuse(cp)

    def test_resume_preserved_count_checked(self):
        cp = copy.deepcopy(self.cp)
        cp["ledger"].append({"seq": 7, "op": "resumed", "preserved": 2})
        self.refuse(cp)

    def test_roundtrip_serialization_and_repeated_resume(self):
        c = Coordinator.resume(self.plan, json.loads(json.dumps(self.cp)))
        for _ in range(3):
            c = Coordinator.resume(self.plan, c.checkpoint(), concurrency=1)
        self.assertTrue(c.run(lambda t: self.fail("DONE must remain done"))["complete"])
        self.assertEqual(c.verify()["verdict"], "PASS")

    def test_resume_capacity_atomic_original_checkpoint(self):
        before = copy.deepcopy(self.cp)
        with patch("n01.core.MAX_EVENTS", 6), self.assertRaises(PlanError):
            Coordinator.resume(self.plan, self.cp)
        self.assertEqual(self.cp, before)

    def test_resumed_state_and_ledger_detached_from_checkpoint(self):
        c = Coordinator.resume(self.plan, self.cp)
        self.cp["state"]["a"] = "FAILED"; self.cp["ledger"][0]["op"] = "evil"
        self.assertEqual(c.verify()["verdict"], "PASS")

    def test_consistent_rewritten_history_is_not_authentication(self):
        # A valid recomputed unsigned history is intentionally accepted; provenance
        # and exactly-once external effects require an external trust boundary.
        cp = copy.deepcopy(self.cp)
        cp["ledger"][1]["result_digest"] = "sha256:" + "0"*64
        c = Coordinator.resume(self.plan, resign(cp))
        self.assertEqual(c.verify()["verdict"], "PASS")
