# Merge Audit Tooling

Use this directory for conflict-heavy `origin/main` merges where incoming changes
must be accounted for surgically.

## Before Merging Main

Default workflow:

```sh
make merge-audit-snapshot
```

Equivalent explicit command:

```sh
python3 scripts/merge_audit/audit_main_capture.py \
  --incoming-ref origin/main \
  --previous-main-merge <last-main-merge> \
  --write-snapshot /tmp/antfly-main-merge-snapshot.json
```

The snapshot freezes the incoming SHA and path list, so the later audit is not
affected if `origin/main` moves.

Use this to fail fast if `origin/main` has moved since the snapshot:

```sh
make merge-audit-check-snapshot
```

## After Resolving Conflicts

Default workflow:

```sh
make merge-audit
```

Equivalent explicit commands:

```sh
python3 scripts/merge_audit/audit_main_capture.py \
  --snapshot /tmp/antfly-main-merge-snapshot.json \
  --strict-snapshot-ref \
  --json-out /tmp/antfly-main-merge-audit.json \
  --write-report /tmp/antfly-main-merge-audit.md

python3 scripts/merge_audit/audit_zig_split_merge.py \
  --write-report /tmp/antfly-zig-split-merge-audit.md
```

Then run the focused build/tests appropriate for the conflicts and finish with:

```sh
git diff --cached --check
git diff --check
```

## After Committing A Merge

Use the merge commit parents explicitly so the audit still sees the original
ours and incoming sides:

```sh
make merge-audit-post-commit
```

Equivalent explicit commands:

```sh
python3 scripts/merge_audit/audit_main_capture.py \
  --incoming-ref HEAD^2 \
  --ours-ref HEAD^1 \
  --previous-main-merge <last-main-merge> \
  --json-out /tmp/antfly-main-merge-audit.json \
  --write-report /tmp/antfly-main-merge-audit.md

python3 scripts/merge_audit/audit_zig_split_merge.py \
  --origin HEAD^2 \
  --base <previous-main-parent-sha> \
  --write-report /tmp/antfly-zig-split-merge-audit.md
```

## Policy

`policy.json` holds repo-specific merge knowledge: moved paths, intentional
side choices, renamed helpers/tests, high-risk paths requiring explicit review,
and generated-file provenance. Keep policy changes specific and documented.

## Make Variables

Override these when the default refs or output paths are not what you want:

- `MERGE_AUDIT_INCOMING`, default `origin/main`
- `MERGE_AUDIT_PREVIOUS_MAIN_MERGE`, default last prior `origin/main` merge
- `MERGE_AUDIT_BASE`, default second parent of `MERGE_AUDIT_PREVIOUS_MAIN_MERGE`
- `MERGE_AUDIT_SNAPSHOT`, default `/tmp/antfly-main-merge-snapshot.json`
- `MERGE_AUDIT_JSON`, default `/tmp/antfly-main-merge-audit.json`
- `MERGE_AUDIT_REPORT`, default `/tmp/antfly-main-merge-audit.md`
- `MERGE_AUDIT_ZIG_REPORT`, default `/tmp/antfly-zig-split-merge-audit.md`
