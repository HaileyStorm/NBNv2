# Nbn.Tests

Owns format/runtime/reproduction parity coverage and regression protection surface.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.

`tests/Nbn.Tests/TestSupport/*` owns cross-suite test infrastructure such as async wait/poll helpers, temporary path/database scopes, artifact-store harnesses, and repeated actor bootstrap helpers. Keep scenario-specific assertions and domain-only probes local to the suite that owns them.
