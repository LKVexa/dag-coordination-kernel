# Security boundaries

Handlers are trusted synchronous Python callbacks with the process's full access.
The kernel interprets no action text and supplies no sandbox, timeout, resource
isolation, authorization or network transport. An RLock is held through callbacks;
avoid cross-thread waits that require the same coordinator. Input validation does
not make an arbitrary handler safe.

Unsigned plan/checkpoint/state hashes provide consistency checks, not authenticated
provenance. Replay validation rejects impossible state transitions but cannot detect
an invented internally consistent history. No freshness, rollback protection or
durable persistence is provided. A retained result digest cannot recover the body
or prove execution. Caller code controls external result storage and sensitive data.

Task effects may precede recorded/persisted completion. Reusing old checkpoints or
retrying failures can repeat effects. Handlers need appropriate idempotency and
external transactions; exactly-once external effects are not guaranteed. Bounds
limit accepted normal workloads, not OS resources. Full-state hashing has growing
cost. No independent security audit, build-tool vulnerability scan or original
968-item program certification is claimed.
