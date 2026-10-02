# Canonical Beads tracker

Beads 1.3.1 uses host-local embedded Dolt here. The database is ignored by Git;
`issues.jsonl` is a deliberately published interchange/viewer snapshot, not a full backup.
See [migration-20261001.md](migration-20261001.md) for provenance and recovery evidence.

- Audit actual checkout, task/claim, Git status and tracker routing before writes. Run `bd where` from this canonical repository root; from elsewhere use `bd -C <canonical-root>`.
- Serialize embedded database writers and Git index/ref mutations. A worktree does not isolate or authorize tracker writes. Never initialize another tracker in a subfolder or worktree.
- Use `bd ready`, `bd show <id>`, and `bd history <id> --events` for current work. Preserve user-approved states; migration does not close or reopen issues.
- After intentional tracker changes, run `bd export --all -o .beads/issues.jsonl`, inspect the diff, and stage only authorized files. Exports include tombstones but omit full event/schema/history provenance.
- Run `bd backup sync` for the configured local full-database backup. On a new host, configure an explicitly approved local destination with `bd backup init <local-directory>` first. Verify restoration in an isolated fixture; do not infer an off-host backup exists.
- `bd sync` now transfers Dolt remotes. No remote is configured by this migration. Do not use it as a legacy JSONL flush, contact DoltHub, or add a remote without authorization.
- Automatic legacy flush/import/staging hooks are disabled, not deleted. Do not blindly import a pulled JSONL: normal import skips tombstones and is not a full restore.
- Before compaction or task rollover, checkpoint objective, acceptance, owner/host, exact checkout/ref, claims, changed files, tracker/test evidence, unresolved risks and next action. A successor reconstructs from durable artifacts and live read-only checks, not chat history. Release only your own claim after verified transfer.
