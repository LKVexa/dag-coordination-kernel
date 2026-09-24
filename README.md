# DAG Coordination Kernel

**0.1.2a1 — experimental partial candidate, JY-S037-P001 / N01**

Compile a bounded task DAG, run a trusted synchronous handler in deterministic
dependency order, continue independent branches after failures and resume from
replay-checked in-memory checkpoints. Python 3.10+; no runtime dependencies.

## Use

~~~sh
python -m pip install .
python -m unittest discover -s tests -t .
~~~

~~~python
from n01.core import compile_dag, Coordinator

plan = compile_dag([
    {"task_id": "prepare", "action": "prepare inputs", "inputs": {"revision": 1}},
    {"task_id": "build", "action": "build artifact", "depends_on": ["prepare"]},
])
coordinator = Coordinator(plan, concurrency=4)
report = coordinator.run(lambda task: {"handled": task["task_id"]})
assert report["complete"]
checkpoint = coordinator.checkpoint()
resumed = Coordinator.resume(plan, checkpoint)
assert resumed.run(lambda task: {"handled": task["task_id"]})["complete"]
assert resumed.verify()["verdict"] == "PASS"
~~~

The example returns receipts only; action text is never interpreted or executed
by the kernel. The supplied handler decides what work to perform.

## Plans

compile_dag accepts a nonempty exact list of task dictionaries. Each requires
task_id and action; optional depends_on is an exact list of unique known task IDs.
Additional fields are retained as bounded JSON data and passed to the handler.
IDs are exact case-sensitive strings of at most 128 UTF-8 bytes; action text is
at most 4096 bytes. Both must be nonblank, with no outer whitespace, C0/DEL controls
or invalid Unicode. Dependencies may not repeat; cycles, including self-cycles,
are refused. Cycle errors name all unscheduled tasks, including downstream tasks.

Limits: 500 tasks, 10000 dependency edges, 64 KiB normalized canonical JSON per
task and 4 MiB for the complete compiled plan. Every task supports depth 16 and
10000 visited values/keys; a compiled plan is additionally limited to 500000
visited values/keys and depth 24. Missing dependencies normalize to an empty list;
dependency names, task maps and waves are sorted deterministically.

JSON data uses exact built-in dictionaries with string keys, lists, strings,
booleans, null, finite floats and integers within -(2**53-1)..(2**53-1). Custom
objects, tuples, non-string keys, cycles and nonfinite numbers are refused.
Canonical JSON sorts keys, uses compact separators and ASCII escapes. Escapes
count toward byte budgets. No raw JSON parser is provided; the caller handles
duplicate textual keys before passing decoded objects.

Plan schema n01/plan/v2 hashes the complete normalized plan, including actions,
inputs, extra metadata, dependency indexes, waves, schema and version. Input
permutations do not change the plan; changed task content does. Coordinator
recompiles and compares the supplied plan, refusing forged waves/indexes even
when their hash is recomputed. External data not embedded in the task is not bound.

## Execution and failures

Execution is **sequential**. The compatibility parameter concurrency is an exact
integer 1..64 that slices waves into sequential batches; it creates no parallel
workers and provides no throughput guarantee. Alphabetical tasks within each
dependency wave run in order. Handler input is a detached task copy.

States: PENDING, RUNNING, DONE, FAILED, BLOCKED. A task starts only after all its
dependencies are DONE. Handler exceptions or invalid results fail that task;
dependent branches block transitively while independent branches continue.
Results must be exact JSON objects within 64 KiB, depth 16 and 10000 visited
values/keys. A DONE event stores the canonical result digest and byte size, not
the result body. Preserve bodies externally when later verification needs them.

Exception messages are not formatted or recorded, which avoids leaking their
contents or invoking custom exception string methods. Failure reason codes are
HANDLER_ERROR, INVALID_RESULT and INTERRUPTED. KeyboardInterrupt/SystemExit raised
by the handler produce an interrupted failure receipt and are re-raised. Remaining
tasks stay pending. Abrupt process termination or asynchronous interruption at
arbitrary machine instructions has no transactional durability guarantee.

Repeated run calls execute only PENDING tasks. DONE, FAILED and BLOCKED tasks
do not automatically rerun. Resume explicitly resets failures and blocks. This
prevents an accidental repeat run from silently retrying failed side effects.

An RLock serializes calls for the full run, including handlers. Reentrant run and
in-flight checkpoint calls are refused. A handler may read reports on its calling
thread; do not wait for another thread that needs this coordinator's lock.
There are no handler timeouts, cancellation workers or isolation. Only supply
trusted handlers and do not mutate input objects concurrently during validation.

## Recovery, integrity and capacity

checkpoint() exports schema n01/checkpoint/v2 only while quiescent. It binds the
plan digest, exact task state map and complete ledger. resume validates schema,
fields, bounds and hash, then independently replays every event. Sequence numbers,
task/wave identity, starts, dependencies, terminal receipts, block causes and
resume counts must be consistent with the claimed final states. A recomputed hash
does not permit an impossible transition or a DONE state with no receipt.

Valid resume preserves DONE work, resets FAILED/BLOCKED to PENDING and appends a
resumed event. Different actions or embedded inputs require a new plan; the old
checkpoint is refused. Checkpoints, reports, plan/state properties and ledger
views are detached. No disk persistence or durable append-only ledger is supplied.

Each coordinator permits 20000 events, 8 MiB canonical ledger bytes and 2048 bytes
per event. Before a run calls any handler it reserves a conservative worst case:
two 2048-byte events plus JSON separators per pending task. A resume reserves one
event. Insufficient capacity refuses the operation before work begins; this may
refuse earlier than the eventual compact receipts would require. History is not
pruned. Checkpoint decoding also bounds total size, nesting and visited values.

Runtime state hashing binds the full plan, concurrency value, states and ledger;
detected corruption blocks normal use with IntegrityError (a PlanError subclass).
The post-handler check prevents corrupted private state from being silently
sealed as valid. verify() additionally recompiles the plan and replays history.
Reports have complete report/state digests and explicitly declare sequential
execution and exactly_once_effects=false.

Hashes are unkeyed consistency checks. Someone able to rewrite a complete valid
history and its hashes can fabricate it; receipts do not prove that work happened.
Resume can repeat external effects if a process dies after an effect but before
persisting its completion, or if an old checkpoint is replayed. Use idempotency
keys and transactional external storage where needed. There is no checkpoint
freshness/authentication, rollback defense or distributed exactly-once guarantee.
Full-state hashing/copying grows with retained data; capacities are normal-workload
bounds, not OS memory/time limits or a performance qualification.

## Verification and migration

64 tests: 18 inherited plus 46 new, covering changed plan content, all 64 forward-edge
four-node DAGs, a 500-task chain, malformed/rehashed checkpoint transitions, strict
JSON, capacity reservation, interrupted handlers, reentrancy, concurrent calls,
detached views and private-state corruption. [CHECK_RUNS](docs/CHECK_RUNS.json)
records source and installed-wheel results. CI covers Linux Python 3.10/3.12/3.14
and Windows 3.12. See [AUDIT](docs/AUDIT.md).

0.1.1-partial -> 0.1.2a1 changes plan/checkpoint/report schemas to v2. Recompile
plans and create fresh checkpoints; old hashes/checkpoints are incompatible.
Use explicit resume for retries, exact-list dependencies and bounded JSON object
results. Public state/plan/ledger edits no longer modify the coordinator.

QAM integration, true parallel execution, 2PC, the original 968-item program,
its gate/risk sign-offs and ADR closures remain outside this partial candidate.

## License

Copyright 2026 **RUSSELL PHILIP SMITHSON**.
[Apache License 2.0](LICENSE), with [NOTICE](NOTICE).
No third-party source is vendored; see [THIRD-PARTY-NOTICES](THIRD-PARTY-NOTICES.md).
