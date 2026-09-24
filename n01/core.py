"""Deterministic sequential DAG execution with bounded, replay-checked recovery."""
from __future__ import annotations

import copy
import hashlib
import json
import math
import re
import threading

VERSION = "0.1.2a1"
MAX_TASKS = 500
MAX_EDGES = 10000
MAX_VALUE_BYTES = 65536
MAX_PLAN_BYTES = 4194304
MAX_EVENTS = 20000
MAX_LEDGER_BYTES = 8388608
MAX_EVENT_BYTES = 2048


class PlanError(Exception):
    pass


class IntegrityError(PlanError):
    pass


def _canonical(obj):
    try:
        return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=True, allow_nan=False).encode()
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise PlanError("value is not canonical JSON") from exc


def _digest(obj):
    return "sha256:" + hashlib.sha256(_canonical(obj)).hexdigest()


def _json(value, maximum=MAX_VALUE_BYTES, max_values=10000, max_depth=16):
    stack = [(value, 0)]
    count = text_bytes = 0
    while stack:
        item, depth = stack.pop()
        count += 1
        if depth > max_depth or count > max_values:
            raise PlanError("JSON exceeds depth/value bounds or contains a cycle")
        kind = type(item)
        if kind is dict:
            if len(item) > max_values or any(type(k) is not str for k in item):
                raise PlanError("JSON object keys must be strings")
            stack.extend((v, depth + 1) for v in item.values())
            stack.extend((k, depth + 1) for k in item)
        elif kind is list:
            if len(item) > max_values:
                raise PlanError("JSON exceeds value bounds")
            stack.extend((v, depth + 1) for v in item)
        elif kind is str:
            if len(item) > maximum:
                raise PlanError("JSON text exceeds byte bounds")
            try:
                text_bytes += len(item.encode())
            except UnicodeError as exc:
                raise PlanError("invalid Unicode") from exc
            if text_bytes > maximum:
                raise PlanError("JSON exceeds byte bounds")
        elif kind is int:
            if abs(item) > 2**53 - 1:
                raise PlanError("integer exceeds exact JSON number range")
        elif kind is float:
            if not math.isfinite(item):
                raise PlanError("nonfinite JSON number")
        elif item is not None and kind is not bool:
            raise PlanError("non-JSON value")
    if len(_canonical(value)) > maximum:
        raise PlanError("canonical JSON exceeds byte bounds")
    return copy.deepcopy(value)


def _label(value, field, maximum=128):
    if (type(value) is not str or not value or value != value.strip()
            or len(value) > maximum or any(ord(c) < 32 or ord(c) == 127 for c in value)):
        raise PlanError(field + " must be bounded nonblank control-free text")
    try:
        if len(value.encode()) > maximum:
            raise PlanError(field + " exceeds byte bounds")
    except UnicodeError as exc:
        raise PlanError(field + " contains invalid Unicode") from exc
    return value


def compile_dag(tasks):
    if type(tasks) is not list or not 1 <= len(tasks) <= MAX_TASKS:
        raise PlanError("tasks must be a nonempty list of at most 500 tasks")
    clean, edge_count = {}, 0
    for task in tasks:
        if type(task) is not dict or not {"task_id", "action"} <= task.keys():
            raise PlanError("task must contain task_id and action")
        tid = _label(task["task_id"], "task_id")
        _label(task["action"], "action", 4096)
        if tid in clean:
            raise PlanError("duplicate task_id")
        deps = task.get("depends_on", [])
        if type(deps) is not list or len(deps) > MAX_TASKS:
            raise PlanError("depends_on must be a list of at most 500 IDs")
        for dep in deps:
            _label(dep, "dependency")
        if len(set(deps)) != len(deps):
            raise PlanError("duplicate dependency")
        edge_count += len(deps)
        if edge_count > MAX_EDGES:
            raise PlanError("plan exceeds 10000 dependency edges")
        item = _json(dict(task, depends_on=sorted(deps)))
        clean[tid] = item
    clean = dict(sorted(clean.items()))
    dependents = {tid: [] for tid in clean}
    indegree = {}
    for tid, task in clean.items():
        indegree[tid] = len(task["depends_on"])
        for dep in task["depends_on"]:
            if dep not in clean:
                raise PlanError("unknown dependency: " + dep)
            dependents[dep].append(tid)
    ready = sorted(tid for tid, degree in indegree.items() if degree == 0)
    waves, placed = [], set()
    while ready:
        waves.append(ready)
        nxt = []
        for tid in ready:
            placed.add(tid)
            for child in dependents[tid]:
                indegree[child] -= 1
                if indegree[child] == 0:
                    nxt.append(child)
        ready = sorted(nxt)
    if len(placed) != len(clean):
        # The residual includes cycles AND tasks downstream of them.
        raise PlanError("dependency cycle leaves tasks unscheduled: " + ", ".join(sorted(set(clean)-placed)))
    plan = {"schema": "n01/plan/v2", "version": VERSION, "tasks": clean,
            "dependents": dependents, "waves": waves}
    plan["plan_digest"] = _digest(plan)
    return _json(plan, MAX_PLAN_BYTES, 500000, 24)


def _validated_plan(plan):
    candidate = _json(plan, MAX_PLAN_BYTES, 500000, 24)
    if type(candidate) is not dict or type(candidate.get("tasks")) is not dict:
        raise PlanError("invalid compiled plan")
    rebuilt = compile_dag(list(candidate["tasks"].values()))
    if _canonical(candidate) != _canonical(rebuilt):
        raise PlanError("compiled plan differs from canonical task compilation")
    return rebuilt


def _replay(plan, ledger, allow_running=False):
    """Validate transition grammar independently of recorded final states."""
    state = {tid: "PENDING" for tid in plan["tasks"]}
    wave_map = {tid: n for n, wave in enumerate(plan["waves"], 1) for tid in wave}
    active = None
    for seq, event in enumerate(ledger, 1):
        if type(event) is not dict or type(event.get("seq")) is not int or event["seq"] != seq:
            raise PlanError("invalid ledger sequence")
        op = event.get("op")
        if op == "resumed":
            if (set(event) != {"op", "seq", "preserved"} or active is not None
                    or type(event["preserved"]) is not int
                    or event["preserved"] != sum(s == "DONE" for s in state.values())):
                raise PlanError("invalid resume event")
            state = {t: "PENDING" if s in ("FAILED", "BLOCKED") else s for t, s in state.items()}
            continue
        tid = event.get("task")
        if type(tid) is not str or tid not in state or type(event.get("wave")) is not int or event["wave"] != wave_map[tid]:
            raise PlanError("invalid ledger task or wave")
        keys = {"seq", "op", "task", "wave"}
        deps = plan["tasks"][tid]["depends_on"]
        if op == "start":
            if set(event) != keys or active is not None or state[tid] != "PENDING" or any(state[d] != "DONE" for d in deps):
                raise PlanError("invalid task start")
            state[tid], active = "RUNNING", tid
        elif op == "blocked":
            if set(event) != keys or active is not None or state[tid] != "PENDING" or not any(state[d] in ("FAILED", "BLOCKED") for d in deps):
                raise PlanError("invalid blocked transition")
            state[tid] = "BLOCKED"
        elif op in ("done", "failed"):
            if active != tid or state[tid] != "RUNNING":
                raise PlanError("terminal event without its active start")
            if op == "done":
                if (set(event) != keys | {"result_digest", "result_bytes"}
                        or type(event["result_digest"]) is not str
                        or re.fullmatch(r"sha256:[0-9a-f]{64}", event["result_digest"]) is None
                        or type(event["result_bytes"]) is not int
                        or not 2 <= event["result_bytes"] <= MAX_VALUE_BYTES):
                    raise PlanError("invalid result receipt")
                state[tid] = "DONE"
            else:
                if set(event) != keys | {"reason"} or event["reason"] not in ("HANDLER_ERROR", "INVALID_RESULT", "INTERRUPTED"):
                    raise PlanError("invalid failure receipt")
                state[tid] = "FAILED"
            active = None
        else:
            raise PlanError("unknown ledger operation")
    if active is not None and not allow_running:
        raise PlanError("checkpoint contains an unfinished task")
    return state


class Coordinator:
    def __init__(self, plan, concurrency=4):
        if type(concurrency) is not int or not 1 <= concurrency <= 64:
            raise PlanError("concurrency must be an exact integer in [1, 64]")
        self._plan = _validated_plan(plan)
        self._concurrency = concurrency
        self._state = {tid: "PENDING" for tid in self._plan["tasks"]}
        self._ledger = []
        self._lock = threading.RLock()
        self._running = False
        self._seal = self._state_hash()

    @property
    def plan(self):
        with self._lock:
            self._require_integrity()
            return copy.deepcopy(self._plan)

    @property
    def concurrency(self):
        return self._concurrency

    @property
    def state(self):
        with self._lock:
            self._require_integrity()
            return dict(self._state)

    @property
    def ledger(self):
        with self._lock:
            self._require_integrity()
            return copy.deepcopy(self._ledger)

    def _state_hash(self):
        return _digest({"plan": self._plan, "concurrency": self._concurrency,
                        "state": self._state, "ledger": self._ledger})

    def _require_integrity(self):
        try:
            valid = self._state_hash() == self._seal
        except (PlanError, AttributeError):
            valid = False
        if not valid:
            raise IntegrityError("coordinator state integrity failed")

    def _capacity(self, events):
        # Reserve worst-case bounded receipts before invoking any handler.
        if (len(self._ledger) + events > MAX_EVENTS
                or len(_canonical(self._ledger)) + events * (MAX_EVENT_BYTES + 1) > MAX_LEDGER_BYTES):
            raise PlanError("insufficient ledger capacity for this operation")

    def _event(self, op, tid=None, wave=None, **fields):
        event = {"seq": len(self._ledger) + 1, "op": op, **fields}
        if tid is not None:
            event.update(task=tid, wave=wave)
        if len(_canonical(event)) > MAX_EVENT_BYTES:
            raise PlanError("event exceeds reserved receipt size")
        self._ledger.append(event)
        if tid is not None:
            self._state[tid] = {"start": "RUNNING", "done": "DONE", "failed": "FAILED", "blocked": "BLOCKED"}[op]
        self._seal = self._state_hash()

    def run(self, handler):
        if not callable(handler):
            raise PlanError("handler must be callable")
        with self._lock:
            self._require_integrity()
            if self._running:
                raise PlanError("reentrant run is not supported")
            self._capacity(2 * sum(s == "PENDING" for s in self._state.values()))
            self._running = True
            try:
                for wave_no, wave in enumerate(self._plan["waves"], 1):
                    for batch_start in range(0, len(wave), self._concurrency):
                        for tid in wave[batch_start:batch_start+self._concurrency]:
                            if self._state[tid] != "PENDING":
                                continue
                            deps = self._plan["tasks"][tid]["depends_on"]
                            if any(self._state[d] in ("FAILED", "BLOCKED") for d in deps):
                                self._event("blocked", tid, wave_no)
                                continue
                            self._event("start", tid, wave_no)
                            try:
                                result = handler(copy.deepcopy(self._plan["tasks"][tid]))
                            except BaseException as exc:
                                self._require_integrity()
                                self._event("failed", tid, wave_no, reason="HANDLER_ERROR" if isinstance(exc, Exception) else "INTERRUPTED")
                                if not isinstance(exc, Exception):
                                    raise
                                continue
                            self._require_integrity()
                            try:
                                if type(result) is not dict:
                                    raise PlanError("handler result must be an exact JSON object")
                                result = _json(result)
                                encoded = _canonical(result)
                            except PlanError:
                                self._event("failed", tid, wave_no, reason="INVALID_RESULT")
                                continue
                            self._event("done", tid, wave_no,
                                        result_digest=_digest(result), result_bytes=len(encoded))
            finally:
                self._running = False
            return self.report()

    def report(self):
        with self._lock:
            self._require_integrity()
            counts = {}
            for state in self._state.values():
                counts[state] = counts.get(state, 0) + 1
            result = {"schema": "n01/report/v2", "plan_digest": self._plan["plan_digest"],
                      "states": dict(sorted(self._state.items())), "counts": counts,
                      "complete": all(s == "DONE" for s in self._state.values()),
                      "ledger_length": len(self._ledger), "state_digest": self._seal,
                      "execution_mode": "sequential", "exactly_once_effects": False}
            result["report_digest"] = _digest(result)
            return result

    def checkpoint(self):
        with self._lock:
            self._require_integrity()
            if self._running or any(s == "RUNNING" for s in self._state.values()):
                raise PlanError("checkpoint requires a quiescent coordinator")
            result = {"schema": "n01/checkpoint/v2", "plan_digest": self._plan["plan_digest"],
                      "state": dict(self._state), "ledger": copy.deepcopy(self._ledger)}
            result["checkpoint_digest"] = _digest(result)
            return result

    @classmethod
    def resume(cls, plan, checkpoint, concurrency=4):
        c = cls(plan, concurrency)
        cp = _json(checkpoint, MAX_LEDGER_BYTES + 262144, 500000, 24)
        if (type(cp) is not dict or set(cp) != {"schema", "plan_digest", "state", "ledger", "checkpoint_digest"}
                or cp["schema"] != "n01/checkpoint/v2"):
            raise PlanError("invalid checkpoint schema or fields")
        if cp["plan_digest"] != c._plan["plan_digest"]:
            raise PlanError("checkpoint belongs to a different plan")
        if cp["checkpoint_digest"] != _digest({k: v for k, v in cp.items() if k != "checkpoint_digest"}):
            raise PlanError("checkpoint digest mismatch: tampered checkpoint")
        if (type(cp["state"]) is not dict or set(cp["state"]) != set(c._state)
                or any(type(s) is not str or s not in ("PENDING", "DONE", "FAILED", "BLOCKED") for s in cp["state"].values())
                or type(cp["ledger"]) is not list or len(cp["ledger"]) > MAX_EVENTS
                or len(_canonical(cp["ledger"])) > MAX_LEDGER_BYTES):
            raise PlanError("invalid checkpoint state or ledger bounds")
        for event in cp["ledger"]:
            if len(_canonical(event)) > MAX_EVENT_BYTES:
                raise PlanError("oversized checkpoint event")
        if _replay(c._plan, cp["ledger"]) != cp["state"]:
            raise PlanError("checkpoint states disagree with ledger replay")
        c._ledger = cp["ledger"]
        c._capacity(1)
        c._state = {tid: "PENDING" if s in ("FAILED", "BLOCKED") else s for tid, s in cp["state"].items()}
        c._event("resumed", preserved=sum(s == "DONE" for s in cp["state"].values()))
        return c

    def verify(self):
        with self._lock:
            problems = []
            try:
                self._require_integrity()
                _validated_plan(self._plan)
                if _replay(self._plan, self._ledger, allow_running=self._running) != self._state:
                    problems.append("ledger replay")
            except (PlanError, KeyError, TypeError):
                problems.append("state")
            return {"schema": "n01/integrity/v2", "verdict": "FAIL" if problems else "PASS", "problems": problems}
