# Audit and hardening — 0.1.2a1

Date: 2026-09-23. Source: JY-S037-P001 / 0.1.1-partial / run-0001 / product.
Reviewed compilation, scheduling, callback boundaries, result receipts, capacity,
state integrity and checkpoint recovery. Original source remains separate.

## Repaired findings

- Plan hashes covered IDs/dependencies but omitted actions and inputs. Full v2
  normalized-plan hashes now bind all task metadata and compiled structures.
- Coordinator trusted mutable supplied plan indexes/waves and passed live tasks
  into handlers. It now recompiles/compares plans and detaches input/public views.
- IDs/actions/dependencies and JSON metadata lacked strict types and budgets.
  Bounded exact types, unique dependencies, safe JSON integers and explicit caps apply.
- Checkpoint loading only compared hashes and trusted final states. It now checks
  exact v2 schemas and independently replays sequence/task/wave/start/dependency/
  completion/block/resume semantics before accepting a matching state map.
- Repeated runs implicitly retried failures without a resume marker. Only PENDING
  tasks run; explicit resume resets FAILED/BLOCKED and preserves DONE work.
- Callbacks could corrupt state, reenter execution or expose exception messages.
  A run lock, reentrancy guard, post-callback integrity check and fixed failure codes
  close these accidental-corruption and disclosure paths. Interrupted callbacks
  get failure receipts before control exceptions propagate.
- Unbounded history could exhaust resources after effects began. Event/byte caps
  and conservative whole-run receipt reservation now precede callback execution.
- Documentation overstated concurrency and append-only/durable recovery. Execution
  is sequential; hash-checked memory history cannot guarantee exactly-once effects,
  checkpoint authenticity/freshness or process-crash persistence.

## Verification

All 18 baseline tests passed and remain intact. All 64 source and installed-wheel
tests pass, adding 46 regressions. These include all 64 forward-edge four-node DAGs
checked against dependency readiness, a 500-task iterative chain, complete plan
binding, strict JSON/budgets, rehashed impossible checkpoints, malformed receipts,
interrupted callbacks, explicit retry, reservation atomicity, concurrent/reentrant
runs, detached exports and corruption without accidental resealing.

CHECK_RUNS.json records current evidence; BASELINE_CHECK_RUNS.json retains historical
evidence. CI covers Linux Python 3.10/3.12/3.14 and Windows 3.12. No throughput benchmark,
distributed/process-crash experiment, independent security audit, build-tool
vulnerability scan, QAM/2PC integration or 968-item certification was performed.
There are no third-party runtime dependencies to upgrade.

The dependency-before-task invariant follows the topological-order definition in
the primary [Python graphlib documentation](https://docs.python.org/3/library/graphlib.html).
The implementation adds sorted wave tie breaks and validates its own graph model;
it does not claim graphlib integration or parallel worker execution.

Version advanced from 0.1.1-partial to 0.1.2a1; plan/checkpoint/report schemas are v2.
Added packaging, pinned-action CI, README, security guidance and Apache 2.0
LICENSE/NOTICE naming RUSSELL PHILIP SMITHSON.
