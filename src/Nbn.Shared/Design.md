# Nbn.Shared

Owns the stable shared contracts and helper layer consumed by runtime services, tools, and tests.

## Ownership

- Shared identifiers and addressing helpers such as `Address32`, `ShardId32`, UUID/SHA-256 encoding, and canonical settings keys.
- Canonical binary format readers, writers, and validators for `.nbn` and `.nbs` artifacts.
- Service endpoint discovery contracts and settings helpers used to locate core runtime services.
- Protobuf schemas, generated models, and shared conversion helpers that bridge protobuf wrappers to shared value objects.

## Stable invariants

- Public shared helpers must preserve current wire, file-format, and validation semantics unless a broader spec change explicitly updates them.
- Input region `0`, output region `31`, and illegal IO-region axon rules remain fixed across shared validation and format helpers.
- Cleanup in this project should favor clearer organization, docs, and annotations over behavior changes.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
