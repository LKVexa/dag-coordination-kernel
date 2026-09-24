# 0.1.2a1 — 2026-09-23

- Bind all task inputs/actions and validate compiled plan structures.
- Replay bounded checkpoint history before restoring state.
- Require explicit recovery for failed tasks and detached strict JSON results.
- Guard reentrant/concurrent execution, callback corruption and receipt capacity.
- Add 46 regressions, packaging, cross-platform CI and explicit recovery limits.
- Include README and Apache 2.0 LICENSE/NOTICE; original certification remains open.

# Changelog — n01-executive-coordination (JY-S037-P001)

## 0.1.1-partial (2026-09-14) — audit repairs (A027)

Baseline fingerprint: build-0001 product.zip
sha256 896cdc0194dc38a005ffcbdb630571792e30daadaa1759e0f770ce387d6ee15e
(baseline version 0.1.0-partial; all 7 findings reproduced on baseline
before fixing).

Findings fixed (all FIXED_VERIFIED, repairs only — patch bump):

- **A027-F1** (high): a handler result that was not JSON-serializable
  raised a bare TypeError out of `run()` AFTER the task was already
  marked DONE with no `done` ledger entry — violating "nothing is ever
  marked complete without a recorded handler result". Expected: the
  task fails cleanly. Fix: digest is computed before the DONE
  transition; an unrecordable result marks the task FAILED.
- **A027-F2** (high): the checkpoint digest covered only plan digest +
  state, so a tampered or erased ledger was accepted at resume.
  Expected: tamper detection at load covers the whole checkpoint. Fix:
  digest now covers plan_digest, state and ledger.
- **A027-F3** (medium): `checkpoint()` shallow-copied the ledger;
  mutating a returned checkpoint entry mutated the live coordinator's
  ledger (and vice versa). Fix: deep copy on checkpoint and resume.
- **A027-F4** (medium): the compiled plan aliased the caller's task
  dicts; mutating a task after `compile_dag` silently changed the plan
  while `plan_digest` stayed stale. Fix: tasks are deep-copied into
  the plan.
- **A027-F5** (medium): NaN/Infinity in handler results were digested
  via non-strict JSON, producing non-canonical digests. Fix: `_digest`
  uses `allow_nan=False`; such results now fail the task.
- **A027-F6** (low): a malformed checkpoint (missing keys / non-dict)
  leaked a bare KeyError from `resume()` instead of the documented
  PlanError. Fix: explicit field validation raising PlanError.
- **A027-F7** (low): a string `depends_on` was iterated character-wise
  (silently misread as single-character task ids). Fix: `depends_on`
  must be a list/tuple; non-dict tasks also rejected with PlanError.

Compatibility: no public API removed or changed for well-formed
inputs; checkpoints written by 0.1.0-partial are **not resumable** by
0.1.1-partial (digest formula now covers the ledger) — re-checkpoint
from a fresh run. Rollback: restore build-0001 product tree
(fingerprint above); no data migration involved.

## 0.1.0-partial

Baseline partial candidate (build-0001).
