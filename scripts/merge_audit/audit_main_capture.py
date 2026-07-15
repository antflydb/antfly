#!/usr/bin/env python3
"""Three-way audit for whether a merge captured incoming main changes.

The audit classifies files changed by the incoming ref since the selected base
and verifies files untouched by our side landed byte-for-byte, moved/split into
documented paths, or were explicitly documented as intentional integrations.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import pathlib
import subprocess
import sys
from datetime import datetime, timezone
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[2]
DEFAULT_POLICY_PATH = ROOT / "scripts/merge_audit/policy.json"

MOVED_PATHS = {
    "zig/pkg/antfly/src/api/tables.zig": [
        "zig/pkg/antfly/src/metadata/catalog/table_ddl.zig",
    ],
    "zig/pkg/antfly/src/api/table_reads.zig": [
        "zig/pkg/antfly/src/api/table_reads.zig",
        "zig/pkg/antfly/src/api/table_reads/core.zig",
        "zig/pkg/antfly/src/api/table_reads/document_sql.zig",
        "zig/pkg/antfly/src/api/table_reads/remote_wire.zig",
        "zig/pkg/antfly/src/api/table_reads/sources.zig",
    ],
    "zig/pkg/antfly/src/api/table_writes.zig": [
        "zig/pkg/antfly/src/api/table_writes.zig",
        "zig/pkg/antfly/src/api/table_writes/bulk_ingest.zig",
        "zig/pkg/antfly/src/api/table_writes/cache.zig",
        "zig/pkg/antfly/src/api/table_writes/core.zig",
        "zig/pkg/antfly/src/api/table_writes/integrity.zig",
        "zig/pkg/antfly/src/api/table_writes/managed_db.zig",
        "zig/pkg/antfly/src/api/table_writes/sources.zig",
    ],
    "zig/pkg/antfly/src/storage/db/db.zig": [
        "zig/pkg/antfly/src/storage/db/db.zig",
        "zig/pkg/antfly/src/storage/db/artifact_repair.zig",
        "zig/pkg/antfly/src/storage/db/artifact_replay.zig",
        "zig/pkg/antfly/src/storage/db/catalog/index_manager.zig",
        "zig/pkg/antfly/src/storage/db/core.zig",
        "zig/pkg/antfly/src/storage/db/derived_async.zig",
        "zig/pkg/antfly/src/storage/db/document_mapper.zig",
        "zig/pkg/antfly/src/storage/db/enrichment/enrichment_runtime.zig",
        "zig/pkg/antfly/src/storage/db/graph_runtime.zig",
        "zig/pkg/antfly/src/storage/db/internal.zig",
        "zig/pkg/antfly/src/storage/db/lifecycle.zig",
        "zig/pkg/antfly/src/storage/db/query/graph_exec.zig",
        "zig/pkg/antfly/src/storage/db/query/search_exec.zig",
        "zig/pkg/antfly/src/storage/db/search_runtime.zig",
        "zig/pkg/antfly/src/storage/db/split_restore.zig",
        "zig/pkg/antfly/src/storage/db/types.zig",
        "zig/pkg/antfly/src/storage/db/write_path.zig",
    ],
}

EXPECTED_INCOMING_ONLY_INTEGRATION_DIFFS = {
    "docs/guides/document-engine.mdx": "trivial EOF blank-line normalization; content is otherwise identical to incoming main.",
    "py/packages/sdk/src/antfly/client_generated/models/algebraic_index_stats.py": "regenerated from merged specs; adds planner_last_unsupported_reason from current schema.",
    "py/packages/sdk/src/antfly/client_generated/models/embeddings_index_stats.py": "regenerated from merged specs; adds embeddings coverage from merged indexes schema.",
    "zig/pkg/antfly/src/api_artifact_reprocess_jobs_test_root.zig": "test root wiring for merged table repair cancel callback coverage.",
    "zig/pkg/antfly/src/api/distributed_candidate_source.zig": "merged scan-row identity contract: incoming candidate-source behavior adapted from legacy key field to branch `_id` scan rows via table_reads.appendScanLine.",
    "zig/ENRICHMENTS.md": "incoming enrichment sync docs adjusted to branch public sync-level naming: query/full_index instead of stale full_text/embeddings aliases.",
    "zig/pkg/antfly/src/public_test_helpers.zig": "generated OpenAPI total relation became an enum; helper must compare enum value.",
    "zig/pkg/inference/src/native_generate.zig": "merged inference OpenAPI generation made request fields enums; client source converts CLI strings to generated enum values.",
    "zig/pkg/antfly/src/inference/managed_embedder.zig": "incoming managed embedder changes are integrated with branch embedding_name alias lookup, collision validation, and owned slice transfer cleanup.",
    "zig/pkg/inference/src/server/server.zig": "merged inference OpenAPI generation made request/response literals enums; server source converts generated enum values.",
}

EXPECTED_OURS_ONLY_RESOLUTIONS = {
    "docs/architecture.mdx": "branch sync-level naming uses query while still retaining main's aknn removal via full_index.",
    "examples/epstein/main.go": "branch sync-level naming uses query/full_index and rejects stale full_text/embeddings aliases.",
    "examples/epstein/main_test.go": "branch sync-level naming regression test rejects stale full_text/embeddings/aknn aliases.",
    "go/e2e/backup_restore_test.go": "branch sync-level naming uses SyncLevelQuery for query visibility; main's SyncLevelAknn removal is already represented by SyncLevelFullIndex where needed.",
    "py/packages/sdk/src/antfly/client_generated/models/batch_request.py": "generated from merged specs with branch query sync-level naming and no aknn enum.",
    "py/packages/sdk/src/antfly/client_generated/models/linear_merge_request.py": "generated from merged specs with branch query sync-level naming and no aknn enum.",
    "py/packages/sdk/src/antfly/client_generated/models/multi_batch_request.py": "generated from merged specs with branch query sync-level naming and no aknn enum.",
    "py/packages/sdk/src/antfly/client_generated/models/sync_level.py": "generated enum intentionally keeps query naming and omits aknn.",
    "py/packages/sdk/src/antfly/client_generated/models/transaction_begin_request.py": "generated from merged specs with branch query sync-level naming and no aknn enum.",
    "py/packages/sdk/src/antfly/client_generated/models/transaction_commit_request.py": "generated from merged specs with branch query sync-level naming and no aknn enum.",
    "zig/DOCID.md": "branch DOCID evidence text keeps query sync-level naming and has no aknn sync-level residue.",
    "zig/pkg/antfly/src/api/tables.zig": "tables.zig is intentionally removed without a shim; split-aware table contract/schema symbol audits cover incoming changes.",
    "zig/pkg/antfly/src/api/table_writes.zig": "split facade is authoritative on this branch; split-aware public symbol and vtable audits cover incoming monolith changes.",
    "zig/pkg/antfly/src/api_table_reads_test_root.zig": "split test root stays decomposed on this branch; table_router is compiled through api/mod and api_rows roots.",
    "zig/pkg/antfly/src/cmd/cli/data.zig": "CLI parser intentionally keeps branch query sync-level naming and rejects aknn.",
}

EXPECTED_MAIN_ONLY_RESOLUTIONS = {
    "go/pkg/antfly/src/metadata/api_document_artifacts.go": "main-only stub ordering is acceptable; all artifact enrichment handlers are present.",
    "scripts/bench_gemma4_mtp.sh": "incoming bench helper is accepted exactly from main.",
}

MANUAL_REVIEW_REQUIRED_PATHS: dict[str, str] = {}
MANUAL_REVIEW_ACKNOWLEDGEMENTS: dict[str, str] = {}
GENERATED_FILE_PROVENANCE: dict[str, list[str]] = {}
GENERATED_PROVENANCE_ALLOWLIST: dict[str, str] = {}

MANIFEST_SCHEMA: dict[str, str] = {
    "moved_paths": "map_list",
    "expected_incoming_only_integration_diffs": "map_string",
    "expected_ours_only_resolutions": "map_string",
    "expected_main_only_resolutions": "map_string",
    "expected_generated_without_spec_changes": "map_string",
    "manual_review_required_paths": "map_string",
    "manual_review_acknowledgements": "map_string",
    "generated_file_provenance": "map_list",
    "same_path_const_false_positives": "map_list",
    "same_path_const_aliases": "nested_map_list",
    "test_name_aliases": "map_list",
    "changed_helper_aliases": "map_list",
    "generated_workflows": "list_string",
}


def ensure_manifest_shape(data: object, path: pathlib.Path) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError(f"{path}: manifest root must be a JSON object")

    errors: list[str] = []
    for key in sorted(data):
        if key not in MANIFEST_SCHEMA:
            errors.append(f"unknown key {key!r}")
            continue
        expected = MANIFEST_SCHEMA[key]
        value = data[key]
        if expected == "map_string":
            if not isinstance(value, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in value.items()):
                errors.append(f"{key}: expected object of string -> string")
        elif expected == "map_list":
            if not isinstance(value, dict) or not all(
                isinstance(k, str) and isinstance(v, list) and all(isinstance(item, str) for item in v)
                for k, v in value.items()
            ):
                errors.append(f"{key}: expected object of string -> list[string]")
        elif expected == "nested_map_list":
            if not isinstance(value, dict) or not all(
                isinstance(k, str)
                and isinstance(nested, dict)
                and all(
                    isinstance(nested_key, str)
                    and isinstance(nested_value, list)
                    and all(isinstance(item, str) for item in nested_value)
                    for nested_key, nested_value in nested.items()
                )
                for k, nested in value.items()
            ):
                errors.append(f"{key}: expected object of string -> object of string -> list[string]")
        elif expected == "list_string":
            if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
                errors.append(f"{key}: expected list[string]")
        else:
            errors.append(f"{key}: internal schema error for {expected!r}")

    if errors:
        joined = "\n  - ".join(errors)
        raise ValueError(f"{path}: invalid manifest\n  - {joined}")
    return data


def load_policy(path: pathlib.Path) -> None:
    if not path.exists():
        return
    data = ensure_manifest_shape(json.loads(path.read_text()), path)
    MOVED_PATHS.update({key: list(value) for key, value in data.get("moved_paths", {}).items()})
    EXPECTED_INCOMING_ONLY_INTEGRATION_DIFFS.update(data.get("expected_incoming_only_integration_diffs", {}))
    EXPECTED_OURS_ONLY_RESOLUTIONS.update(data.get("expected_ours_only_resolutions", {}))
    EXPECTED_MAIN_ONLY_RESOLUTIONS.update(data.get("expected_main_only_resolutions", {}))
    GENERATED_PROVENANCE_ALLOWLIST.update(data.get("expected_generated_without_spec_changes", {}))
    MANUAL_REVIEW_REQUIRED_PATHS.update(data.get("manual_review_required_paths", {}))
    MANUAL_REVIEW_ACKNOWLEDGEMENTS.update(data.get("manual_review_acknowledgements", {}))
    GENERATED_FILE_PROVENANCE.update({key: list(value) for key, value in data.get("generated_file_provenance", {}).items()})


def run_git(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {proc.stderr.strip()}")
    return proc


def lines(args: list[str]) -> list[str]:
    return [line for line in run_git(args).stdout.splitlines() if line]


def blob(rev: str, path: str) -> str | None:
    proc = run_git(["rev-parse", f"{rev}:{path}"], check=False)
    if proc.returncode != 0:
        return None
    return proc.stdout.strip()


def index_blob(path: str) -> str | None:
    proc = run_git(["rev-parse", f":{path}"], check=False)
    if proc.returncode != 0:
        return None
    return proc.stdout.strip()


def path_category(path: str) -> str:
    if path.startswith("zig/pkg/antfly/src/storage/db/"):
        return "zig-db"
    if path.startswith("zig/pkg/antfly/src/api/table_reads"):
        return "zig-table-reads"
    if path.startswith("zig/pkg/antfly/src/api/table_writes"):
        return "zig-table-writes"
    if path.startswith("zig/pkg/antfly/src/api/") or path.startswith("zig/pkg/antfly/src/query/"):
        return "zig-api-query"
    if path.startswith("zig/pkg/antfly/src/openapi/generated/"):
        return "zig-generated-openapi"
    if path.startswith("specs/openapi/"):
        return "spec"
    if path.startswith("go/"):
        return "go"
    if path.startswith("py/"):
        return "python"
    if path.startswith("ts/"):
        return "typescript"
    if path.startswith(".github/"):
        return "ci"
    return path.split("/", 1)[0]


def result_captured_moved_path(incoming_path: str) -> bool:
    paths = MOVED_PATHS.get(incoming_path)
    if not paths:
        return False
    return any(index_blob(path) is not None for path in paths)


def audited_ref(raw: str) -> str:
    return run_git(["rev-parse", "--verify", raw]).stdout.strip()


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def matches_pattern(path: str, pattern: str) -> bool:
    if pattern.endswith("/"):
        return path.startswith(pattern)
    if pattern.endswith("*") or any(token in pattern for token in ("?", "[")):
        return fnmatch.fnmatch(path, pattern)
    return path == pattern or path.startswith(pattern.rstrip("/") + "/")


def matching_policy(path: str, policy: dict[str, str]) -> str | None:
    for pattern, reason in policy.items():
        if matches_pattern(path, pattern):
            return reason
    return None


def matching_provenance(path: str) -> tuple[str, list[str]] | None:
    for pattern, specs in GENERATED_FILE_PROVENANCE.items():
        if matches_pattern(path, pattern):
            return pattern, specs
    return None


def merge_parents(rev: str) -> list[str]:
    parent_line = run_git(["show", "--no-patch", "--pretty=%P", rev]).stdout.strip()
    return parent_line.split() if parent_line else []


def is_ancestor(candidate: str, descendant: str) -> bool:
    return run_git(["merge-base", "--is-ancestor", candidate, descendant], check=False).returncode == 0


def resolved_previous_main_baseline(previous_main_merge: str, incoming_ref: str) -> str:
    """Resolve a prior `origin/main` merge commit to the main-side parent.

    Comparing a feature-branch merge commit directly to a newer main ref can
    misclassify branch-only changes as incoming deletions. For ordinary
    `git merge origin/main` commits, the main-side parent is the parent that is
    also an ancestor of the newer incoming main ref; prefer the second parent
    when it satisfies that contract.
    """
    rev = audited_ref(previous_main_merge)
    parents = merge_parents(rev)
    if len(parents) < 2:
        return rev

    for parent in parents[1:] + parents[:1]:
        if is_ancestor(parent, incoming_ref):
            return parent
    return rev


def load_snapshot(path: pathlib.Path) -> dict[str, Any]:
    data = json.loads(path.read_text())
    required = {
        "schema_version": int,
        "mode": str,
        "base": str,
        "incoming_ref": str,
        "incoming_sha": str,
        "incoming_paths": list,
    }
    for key, expected_type in required.items():
        if key not in data or not isinstance(data[key], expected_type):
            raise ValueError(f"{path}: snapshot missing {key!r} with type {expected_type.__name__}")
    if data["schema_version"] != 1:
        raise ValueError(f"{path}: unsupported snapshot schema_version {data['schema_version']!r}")
    if not all(isinstance(item, str) for item in data["incoming_paths"]):
        raise ValueError(f"{path}: incoming_paths must be list[string]")
    return data


def validate_snapshot_ref(snapshot: dict[str, Any]) -> None:
    current_sha = audited_ref(snapshot["incoming_ref"])
    if current_sha != snapshot["incoming_sha"]:
        raise ValueError(
            "snapshot incoming ref moved: "
            f"{snapshot['incoming_ref']} is {current_sha}, snapshot recorded {snapshot['incoming_sha']}"
        )


def write_snapshot(path: pathlib.Path, incoming_ref: str, previous_main_merge: str | None) -> dict[str, Any]:
    incoming_sha = audited_ref(incoming_ref)
    previous_baseline_input = audited_ref(previous_main_merge) if previous_main_merge else None
    base = (
        resolved_previous_main_baseline(previous_main_merge, incoming_sha)
        if previous_main_merge
        else run_git(["merge-base", "HEAD", incoming_ref]).stdout.strip()
    )
    payload: dict[str, Any] = {
        "schema_version": 1,
        "mode": "previous-main-baseline" if previous_main_merge else "active-merge-base",
        "created_at": now_utc(),
        "base": base,
        "head_at_snapshot": audited_ref("HEAD"),
        "incoming_ref": incoming_ref,
        "incoming_sha": incoming_sha,
        "previous_main_merge_input": previous_baseline_input,
        "incoming_paths": lines(["diff", "--name-only", f"{base}..{incoming_sha}"]),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return payload


def audit_merge(
    incoming_ref: str,
    previous_main_merge: str | None,
    ours_ref: str,
    snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if snapshot:
        merge_head = snapshot["incoming_sha"]
        base = snapshot["base"]
        incoming_paths = list(snapshot["incoming_paths"])
        previous_baseline_input = snapshot.get("previous_main_merge_input")
        mode = f"snapshot:{snapshot['mode']}"
    else:
        merge_head = audited_ref(incoming_ref)
        previous_baseline_input = audited_ref(previous_main_merge) if previous_main_merge else None
        base = (
            resolved_previous_main_baseline(previous_main_merge, merge_head)
            if previous_main_merge
            else run_git(["merge-base", "HEAD", incoming_ref]).stdout.strip()
        )
        incoming_paths = lines(["diff", "--name-only", f"{base}..{merge_head}"])
        mode = "previous-main-baseline" if previous_main_merge else "active-merge"

    head = audited_ref(ours_ref)
    staged_paths = set(lines(["diff", "--cached", "--name-only"]))
    result_changed_paths = set(incoming_paths) | staged_paths
    both_changed: list[str] = []
    ours_only_resolutions: list[str] = []
    main_only_resolutions: list[str] = []
    same_as_both_resolutions: list[str] = []
    integrated_resolutions = 0
    incoming_only_mismatches: list[str] = []
    expected_integration_diffs: list[str] = []
    incoming_deleted_not_deleted: list[str] = []
    result_missing: list[str] = []
    moved_paths: list[str] = []
    manual_review_required: dict[str, str] = {}
    manual_review_missing: dict[str, str] = {}
    generated_without_spec_changes: dict[str, str] = {}
    incoming_only_ok = 0
    both_counts: dict[str, int] = {}
    incoming_counts: dict[str, int] = {}

    for path in incoming_paths:
        incoming_counts[path_category(path)] = incoming_counts.get(path_category(path), 0) + 1
        base_blob = blob(base, path)
        head_blob = blob(head, path)
        merge_blob = blob(merge_head, path)
        result_blob = index_blob(path)
        ours_changed = head_blob != base_blob

        if not ours_changed:
            if merge_blob is None:
                if result_blob is not None:
                    incoming_deleted_not_deleted.append(path)
                else:
                    incoming_only_ok += 1
                continue
            if result_blob == merge_blob:
                incoming_only_ok += 1
                continue
            if result_captured_moved_path(path):
                moved_paths.append(path)
                continue
            if path in EXPECTED_INCOMING_ONLY_INTEGRATION_DIFFS:
                expected_integration_diffs.append(path)
            elif result_blob is None:
                result_missing.append(path)
            else:
                incoming_only_mismatches.append(path)
            continue

        both_changed.append(path)
        both_counts[path_category(path)] = both_counts.get(path_category(path), 0) + 1
        if result_blob == head_blob and result_blob != merge_blob:
            ours_only_resolutions.append(path)
        elif result_blob == merge_blob and result_blob != head_blob:
            main_only_resolutions.append(path)
        elif result_blob == head_blob and result_blob == merge_blob:
            same_as_both_resolutions.append(path)
        elif result_blob != head_blob and result_blob != merge_blob:
            integrated_resolutions += 1

    for path in sorted(result_changed_paths):
        review_reason = matching_policy(path, MANUAL_REVIEW_REQUIRED_PATHS)
        if review_reason:
            manual_review_required[path] = review_reason
            if not matching_policy(path, MANUAL_REVIEW_ACKNOWLEDGEMENTS):
                manual_review_missing[path] = review_reason

        provenance = matching_provenance(path)
        if provenance is None:
            continue
        generated_pattern, spec_patterns = provenance
        relevant_spec_changed = any(
            any(matches_pattern(changed_path, spec_pattern) for changed_path in result_changed_paths)
            for spec_pattern in spec_patterns
        )
        if not relevant_spec_changed and not matching_policy(path, GENERATED_PROVENANCE_ALLOWLIST):
            generated_without_spec_changes[path] = generated_pattern

    failures: dict[str, list[str]] = {}
    if result_missing:
        failures["incoming-only result missing"] = result_missing
    if incoming_only_mismatches:
        failures["incoming-only result differs from incoming ref"] = incoming_only_mismatches
    if incoming_deleted_not_deleted:
        failures["incoming-only deletion not reflected"] = incoming_deleted_not_deleted
    unexpected_ours_only = [
        path for path in ours_only_resolutions
        if path not in EXPECTED_OURS_ONLY_RESOLUTIONS
    ]
    unexpected_main_only = [
        path for path in main_only_resolutions
        if path not in EXPECTED_MAIN_ONLY_RESOLUTIONS
    ]
    if unexpected_ours_only:
        failures["unexpected both-changed result identical to ours"] = unexpected_ours_only
    if unexpected_main_only:
        failures["unexpected both-changed result identical to main"] = unexpected_main_only
    if manual_review_missing:
        failures["manual review acknowledgement missing"] = sorted(manual_review_missing)
    if generated_without_spec_changes:
        failures["generated file changed without matching spec provenance"] = sorted(generated_without_spec_changes)

    return {
        "schema_version": 1,
        "generated_at": now_utc(),
        "mode": mode,
        "base": base,
        "head": head,
        "ours_ref": ours_ref,
        "incoming_ref": incoming_ref,
        "incoming_sha": merge_head,
        "previous_main_merge_input": previous_baseline_input,
        "previous_main_baseline_resolved": base if previous_baseline_input else None,
        "incoming_changed_files": len(incoming_paths),
        "incoming_only_exact_or_deleted_ok": incoming_only_ok,
        "incoming_only_moved_ok": len(moved_paths),
        "both_changed_files": len(both_changed),
        "incoming_categories": dict(sorted(incoming_counts.items())),
        "both_changed_categories": dict(sorted(both_counts.items())),
        "moved_paths": moved_paths,
        "expected_integration_diffs": {
            path: EXPECTED_INCOMING_ONLY_INTEGRATION_DIFFS[path]
            for path in expected_integration_diffs
        },
        "both_changed_paths": both_changed,
        "both_changed_exact_side_resolution": {
            "integrated_other": integrated_resolutions,
            "same_as_both": same_as_both_resolutions,
            "ours_only": {
                path: EXPECTED_OURS_ONLY_RESOLUTIONS.get(path, "UNEXPECTED")
                for path in ours_only_resolutions
            },
            "main_only": {
                path: EXPECTED_MAIN_ONLY_RESOLUTIONS.get(path, "UNEXPECTED")
                for path in main_only_resolutions
            },
        },
        "manual_review": {
            "required": manual_review_required,
            "acknowledged": {
                path: reason
                for path, reason in MANUAL_REVIEW_ACKNOWLEDGEMENTS.items()
                if any(matches_pattern(changed_path, path) for changed_path in result_changed_paths)
            },
            "missing": manual_review_missing,
        },
        "generated_provenance": {
            "rules": GENERATED_FILE_PROVENANCE,
            "allowlist": GENERATED_PROVENANCE_ALLOWLIST,
            "changed_without_matching_spec": generated_without_spec_changes,
        },
        "failures": failures,
        "ok": not failures,
    }


def render_text(result: dict[str, Any], summary_only: bool) -> str:
    out: list[str] = [
        f"mode={result['mode']}",
        f"base={result['base']}",
    ]
    if result.get("previous_main_merge_input"):
        out.append(f"previous_main_merge_input={result['previous_main_merge_input']}")
        out.append(f"previous_main_baseline_resolved={result['previous_main_baseline_resolved']}")
    out.extend([
        f"ours_ref={result['ours_ref']}",
        f"head={result['head']}",
        f"incoming_sha={result['incoming_sha']}",
        f"incoming_changed_files={result['incoming_changed_files']}",
        f"incoming_only_exact_or_deleted_ok={result['incoming_only_exact_or_deleted_ok']}",
        f"incoming_only_moved_ok={result['incoming_only_moved_ok']}",
        f"both_changed_files={result['both_changed_files']}",
        "",
        "incoming categories:",
    ])
    for category, count in result["incoming_categories"].items():
        out.append(f"  {category}: {count}")
    out.append("both-changed categories:")
    for category, count in result["both_changed_categories"].items():
        out.append(f"  {category}: {count}")

    if result["moved_paths"] and not summary_only:
        out.extend(["", "moved/split incoming paths accepted for manual symbol audit:"])
        out.extend(f"  {path}" for path in result["moved_paths"])
    if result["expected_integration_diffs"] and not summary_only:
        out.extend(["", "expected incoming-only integration diffs:"])
        out.extend(f"  {path}: {reason}" for path, reason in result["expected_integration_diffs"].items())
    if not summary_only:
        out.extend(["", "both-changed paths requiring semantic review:"])
        paths = result["both_changed_paths"]
        out.extend(f"  {path}" for path in paths[:120])
        if len(paths) > 120:
            out.append(f"  ... +{len(paths) - 120} more")

    exact = result["both_changed_exact_side_resolution"]
    out.extend([
        "",
        "both-changed exact-side resolution audit:",
        f"  integrated_other: {exact['integrated_other']}",
        f"  same_as_both: {len(exact['same_as_both'])}",
        f"  ours_only: {len(exact['ours_only'])}",
        f"  main_only: {len(exact['main_only'])}",
        "",
        "manual-review audit:",
        f"  required_changed_paths: {len(result['manual_review']['required'])}",
        f"  missing_acknowledgements: {len(result['manual_review']['missing'])}",
        "",
        "generated provenance audit:",
        f"  changed_without_matching_spec: {len(result['generated_provenance']['changed_without_matching_spec'])}",
    ])

    if result["failures"]:
        out.extend(["", "FAILURES:"])
        for title, paths in result["failures"].items():
            out.append(f"{title}:")
            out.extend(f"  {path}" for path in paths)
    else:
        out.extend([
            "",
            "OK: every incoming-only path is exact, deleted, moved/split, integrated by policy, and required review/provenance checks are satisfied.",
        ])
    return "\n".join(out)


def render_markdown(result: dict[str, Any]) -> str:
    out: list[str] = [
        "# Merge Capture Audit",
        "",
        f"Generated: {result['generated_at']}",
        f"Mode: `{result['mode']}`",
        f"Base: `{result['base']}`",
        f"Incoming: `{result['incoming_ref']}` (`{result['incoming_sha']}`)",
        f"Ours ref: `{result['ours_ref']}`",
        f"HEAD: `{result['head']}`",
        "",
        "## Summary",
        "",
        f"- Incoming changed files: {result['incoming_changed_files']}",
        f"- Both-changed files: {result['both_changed_files']}",
        f"- Incoming-only exact/deleted: {result['incoming_only_exact_or_deleted_ok']}",
        f"- Incoming-only moved/split: {result['incoming_only_moved_ok']}",
        f"- Manual-review paths touched: {len(result['manual_review']['required'])}",
        f"- Missing manual-review acknowledgements: {len(result['manual_review']['missing'])}",
        f"- Generated files without matching spec provenance: {len(result['generated_provenance']['changed_without_matching_spec'])}",
        "",
        "## Categories",
        "",
    ]
    for category, count in result["incoming_categories"].items():
        out.append(f"- {category}: {count}")
    if result["expected_integration_diffs"]:
        out.extend(["", "## Documented Integrations", ""])
        for path, reason in result["expected_integration_diffs"].items():
            out.append(f"- `{path}`: {reason}")
    if result["manual_review"]["required"]:
        out.extend(["", "## Manual Review", ""])
        for path, reason in result["manual_review"]["required"].items():
            status = "acknowledged" if path not in result["manual_review"]["missing"] else "missing"
            out.append(f"- `{path}` ({status}): {reason}")
    if result["failures"]:
        out.extend(["", "## Failures", ""])
        for title, paths in result["failures"].items():
            out.append(f"### {title}")
            out.extend(f"- `{path}`" for path in paths)
    else:
        out.extend(["", "## Result", "", "OK"])
    out.append("")
    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", default=str(DEFAULT_POLICY_PATH), help="JSON merge-audit policy manifest")
    parser.add_argument("--incoming-ref", default="MERGE_HEAD", help="incoming ref to audit; defaults to active MERGE_HEAD")
    parser.add_argument("--ours-ref", default="HEAD", help="ours/pre-merge ref; use HEAD^1 when auditing after a merge commit")
    parser.add_argument(
        "--previous-main-merge",
        help="baseline mode: audit changes from this previous main-merge baseline through --incoming-ref",
    )
    parser.add_argument("--snapshot", help="read pre-merge snapshot JSON and audit against its frozen incoming sha/path set")
    parser.add_argument("--write-snapshot", help="write a pre-merge snapshot JSON and exit")
    parser.add_argument("--strict-snapshot-ref", action="store_true", help="fail when --snapshot incoming_ref no longer points at the recorded incoming_sha")
    parser.add_argument("--check-snapshot-ref", action="store_true", help="only validate that --snapshot incoming_ref still points at incoming_sha")
    parser.add_argument("--json-out", help="write machine-readable audit JSON")
    parser.add_argument("--write-report", help="write a markdown audit report")
    parser.add_argument("--validate-manifest", action="store_true", help="validate manifest and exit")
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="omit long path listings and print only counts plus failures",
    )
    args = parser.parse_args()

    try:
        load_policy(pathlib.Path(args.manifest))
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(f"manifest error: {exc}", file=sys.stderr)
        return 2

    if args.validate_manifest:
        print(f"OK: manifest valid: {args.manifest}")
        return 0

    if args.write_snapshot:
        try:
            snapshot_payload = write_snapshot(pathlib.Path(args.write_snapshot), args.incoming_ref, args.previous_main_merge)
        except (RuntimeError, ValueError) as exc:
            print(f"snapshot error: {exc}", file=sys.stderr)
            return 2
        print(f"Wrote snapshot {args.write_snapshot} for {snapshot_payload['incoming_ref']} ({snapshot_payload['incoming_sha']})")
        return 0

    try:
        snapshot_payload = load_snapshot(pathlib.Path(args.snapshot)) if args.snapshot else None
        if args.check_snapshot_ref:
            if not snapshot_payload:
                raise ValueError("--check-snapshot-ref requires --snapshot")
            validate_snapshot_ref(snapshot_payload)
            print(f"OK: snapshot incoming ref unchanged: {snapshot_payload['incoming_ref']} ({snapshot_payload['incoming_sha']})")
            return 0
        if args.strict_snapshot_ref:
            if not snapshot_payload:
                raise ValueError("--strict-snapshot-ref requires --snapshot")
            validate_snapshot_ref(snapshot_payload)
        result = audit_merge(args.incoming_ref, args.previous_main_merge, args.ours_ref, snapshot_payload)
    except (OSError, json.JSONDecodeError, RuntimeError, ValueError) as exc:
        print(f"audit error: {exc}", file=sys.stderr)
        return 2

    print(render_text(result, args.summary_only))

    if args.json_out:
        path = pathlib.Path(args.json_out)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    if args.write_report:
        path = pathlib.Path(args.write_report)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render_markdown(result))

    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
