#!/usr/bin/env python3
"""Audit merge coverage for Zig db/table read/write split work.

This script compares selected public/internal surfaces from origin/main's
monolithic files with the current split tree. It is intentionally conservative:
critical missing request fields or exact-sort execution symbols are reported as
open gaps instead of silently allowlisted.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone


ROOT = pathlib.Path(__file__).resolve().parents[2]
DEFAULT_POLICY_PATH = ROOT / "scripts/merge_audit/policy.json"

FALLBACK_ORIGIN = "origin/main"
AUDIT_BASE_OVERRIDE: str | None = None

DB_PUB_FN_FALSE_POSITIVES = {
    "deinit",
    "eql",
    "hash",
    "notify",
    "replay",
    "wait",
}

SAME_PATH_CONST_FALSE_POSITIVES = {
    "zig/pkg/antfly/src/api/distributed_txn.zig": {
        # Branch participant IDs moved to the length-prefixed table2: format and
        # intentionally stopped exporting the old legacy parse markers.
        "group_participant_marker",
        "table_participant_prefix",
    },
    "zig/pkg/antfly/src/api/mod.zig": {
        "tables",
    },
}

SAME_PATH_FUNCTION_FALSE_POSITIVES: dict[str, set[str]] = {}

SAME_PATH_FUNCTION_ALIASES: dict[str, dict[str, set[str]]] = {}

SAME_PATH_CONST_ALIASES = {
    "zig/pkg/antfly/src/openapi/generated/antfly_client_openapi/root.zig": {
        "InferenceTextChunkOptions": {"TextChunkOptions"},
        "InferenceVADOptions": {"VADOptions"},
    },
    "zig/pkg/antfly/src/openapi/generated/antfly_client_openapi/types.zig": {
        "InferenceTextChunkOptions": {"TextChunkOptions"},
        "InferenceVADOptions": {"VADOptions"},
    },
    "zig/pkg/antfly/src/storage/db/enrichment/enrichment_runtime.zig": {
        "DerivedRecordWriter": {"GeneratedRecordWriter"},
    },
}

CRITICAL_SORT_EXEC_SYMBOLS = {
    "requestHasSortPageOptions",
    "effectiveSortRequestAlloc",
    "validateComposedSortPageOptions",
    "rejectApproximateSortPageOptions",
    "sortResultProfile",
    "sortAndPageSearchResult",
    "sortAndPageMatchAllIdSeekAlloc",
    "sortAndPageMatchAllCandidateStreamAlloc",
    "compareSearchHitSortValues",
    "decorateVectorScoreOrderIfRequested",
}

CRITICAL_QUERY_CONTRACT_SORT_SYMBOLS = {
    "cloneSortFieldsWithStableTiebreaker",
    "cloneDefaultIdSortField",
    "validatePublicSortCursorTuple",
    "unsupportedExactSort",
    "invalidExactSortCursor",
    "validatePublicQuerySortTupleContract",
    "publicExactSortRejection",
    "buildSortProfileValue",
}

CHANGED_HELPER_CURRENT_ALIASES = {
    "testTableReadIdSortedHitAlloc": {"testTableReadSortedHitAlloc"},
    "testTableReadScoreSortedHitAlloc": {"testTableReadSortedHitAlloc"},
    "testTableReadStringSortedHitAlloc": {"testTableReadSortedHitAlloc"},
    "abortLocalRestoreMutation": {"abortLocalStructuralCachedDbMutation"},
    "beginLocalRestoreMutation": {"beginLocalStructuralCachedDbMutation"},
    "finishLocalRestoreMutation": {"finishLocalStructuralCachedDbMutation"},
    "getOrOpenCachedDbForLocalMutation": {"getOrOpenCachedDbModeAtGeneration"},
    "getOrOpenCachedDbForLocalMutationAlreadyLocked": {"getOrOpenCachedDbModeAtGeneration"},
    "getOrOpenCachedDbModeAlreadyLocked": {"getOrOpenCachedDbModeAtGeneration"},
    "leaseLiveEntryForLocalMutationLocked": {"leaseEntryLocked"},
    "publishCachedLeaseGeneration": {"publishRuntimeStatusSnapshot"},
    "retireInactiveEntryAtIndexForCloseLocked": {"closing_entries", "failed to queue inactive writer-cache entry for close"},
    "replaceTableMetadataAndEntrySchemasLocked": {"replaceTableMetadataLocked"},
    "applyIndexCreateToCachedDb": {"db.addIndex", "createManagedDbEnrichments"},
    "reconcileDbArtifactEnrichmentsFromIndexesJson": {"collectArtifactEnrichmentsFromTableIndexesJson", "createManagedDbEnrichments"},
    "isTransientWriterOpenConflict": {"LsmRootWriterAlreadyOpen", "WriterLocked"},
    "isTerminalStartupCatchUpOpenFailure": {"terminal_degraded", "managed startup catch-up marks FileNotFound index open terminal degraded"},
    "publishTerminalStartupCatchUpRuntimeStatus": {"publishManagedDbTerminalLoadFailureStatus", "terminal_degraded"},
    "publishRuntimeStatusSnapshotToCacheConsistent": {"publishRuntimeStatusSnapshotConsistent"},
    "publishRuntimeStatusSnapshotToCacheWithStartupPhaseMode": {"publishRuntimeStatusSnapshotWithStartupPhaseMode"},
    "indexHasVisibilityFactsForStatus": {"stats.indexes[0].doc_count > 0", "root_node > 0"},
    "runtimeStatusNeedsColdVisibilityRefresh": {"runtime status snapshot with idle phase refreshes live stats after startup catch-up"},
    "runtimeStatusHasNonReplayBackfillSignal": {"preserveNonReplayBackfill"},
    "applySplitGraphArtifactsForIndex": {"applySplitGraphArtifactsInRange"},
    "applySplitGraphArtifactsForIndexStreaming": {"applySplitGraphArtifactsForIndexStreamingContext"},
    "artifactRepairKindHasReprocessor": {"artifactRepairKindHasAutomatedReprocessor"},
    "artifactRepairSummaryRootCount": {"ArtifactRepairSummarySnapshot"},
    "loadArtifactRepairSummaryCountOrScan": {"loadArtifactRepairSummaryCountByKey", "scanArtifactRepairIssueCountBounded"},
    "decodeArtifactRepairIssueAlloc": {"decodeArtifactRepairIssueValueAlloc"},
    "encodeArtifactRepairIssueAlloc": {"encodeArtifactRepairIssueValueAlloc"},
    "clearArtifactRepairIssue": {"clearArtifactRepairIssueWithSummary"},
    "recordArtifactRepairIssueContext": {"recordArtifactRepairIssueForReplay"},
    "recordArtifactRepairIssueForRefContext": {"recordArtifactRepairIssueForRefReplay"},
    "recordEmbeddingArtifactRepairIssue": {"recordEmbeddingArtifactRepairIssueForReplay"},
    "recordEmbeddingArtifactRepairIssueContext": {"recordEmbeddingArtifactRepairIssueForReplay"},
    "repairIssueKeyForIssueAlloc": {"artifactRepairIssueKeyAlloc"},
    "repairIssueKindKeyForIssueAlloc": {"artifactRepairIssueKindKeyAlloc"},
    "generatedRequestMatchesForcedArtifact": {"requestMatchesForcedGeneratedArtifact"},
    "appendGeneratedEnrichmentsFromStoredDocs": {"replayGeneratedEnrichmentsFromStoredDocs", "appendGeneratedEnrichments"},
    "loadDocumentExtractionPreviousState": {"loadRuntimeDocumentExtractionPreviousState"},
    "loadDocumentExtractionPreviousStateFromJson": {"loadRuntimeDocumentExtractionPreviousStateFromJson"},
    "scanDocumentExtractionPreviousStateFromStore": {"scanRuntimeDocumentExtractionPreviousStateFromStore"},
    "asyncContextHasActiveExternalDenseBulkWork": {"active_external_dense_bulk_sessions"},
    "initStoppedTtlRuntimeForTest": {"ttl_cleanup"},
    "ttlVisibleResolvedDocSetNoLockAlloc": {"ttlFilterResolvedDocSetAlloc"},
    "replayDocumentKeyInRange": {"replayRangeHasManagedIndexApplicableRecord"},
    "shouldAppendSplitDeltaForContext": {"shouldAppendSplitDelta"},
    "freeConstDocIds": {"freeResolvedDocIds", "freeConstDocIdsAlloc"},
    "freeKeys": {"freeDocIds"},
    "freeWrites": {"freeDenseArtifactRebuildWrites", "freeSparseArtifactRebuildWrites"},
    "jsonTestNumber": {".integer", ".float"},
    "appendJsonBool": {"appendJsonValue"},
    "appendOptionalJsonStringField": {"appendJsonString"},
    "appendRuntimeFieldCapabilities": {"runtimeFieldCapabilitiesJsonValueAlloc"},
    "appendRuntimeFieldCapability": {"metadataFieldCapability"},
    "generatedAntflyType": {"metadataAntflyType", "antflyTypeName"},
}

INTENTIONAL_TEXT = {
    "aknn_sync_level_removed": "aknn sync_level is intentionally removed; full_index is equivalent.",
}

MOVED_CURRENT_PATHS = {
    "zig/pkg/antfly/src/api/tables.zig": [
        "zig/pkg/antfly/src/api/table_contract.zig",
        "zig/pkg/antfly/src/metadata/catalog/table_ddl.zig",
        "zig/pkg/antfly/src/schema",
        "zig/pkg/antfly/src/storage/schema.zig",
    ],
    "zig/pkg/antfly/src/api/query_contract.zig": [
        "zig/pkg/antfly/src/api/query_contract.zig",
        "zig/pkg/antfly/src/query/contract.zig",
    ],
}


def merge_set_map(target: dict[str, set[str]], source: dict[str, object]) -> None:
    for key, values in source.items():
        if not isinstance(values, list):
            continue
        target.setdefault(key, set()).update(str(value) for value in values)


def merge_nested_set_map(target: dict[str, dict[str, set[str]]], source: dict[str, object]) -> None:
    for path, aliases_by_name in source.items():
        if not isinstance(aliases_by_name, dict):
            continue
        path_map = target.setdefault(path, {})
        for name, aliases in aliases_by_name.items():
            if not isinstance(aliases, list):
                continue
            path_map.setdefault(name, set()).update(str(alias) for alias in aliases)


def load_manifest_policy(path: pathlib.Path) -> None:
    if not path.exists():
        return
    data = json.loads(path.read_text())
    if isinstance(data.get("moved_paths"), dict):
        MOVED_CURRENT_PATHS.update({key: list(value) for key, value in data["moved_paths"].items() if isinstance(value, list)})
    merge_set_map(SAME_PATH_CONST_FALSE_POSITIVES, data.get("same_path_const_false_positives", {}))
    merge_set_map(SAME_PATH_FUNCTION_FALSE_POSITIVES, data.get("same_path_function_false_positives", {}))
    merge_nested_set_map(SAME_PATH_FUNCTION_ALIASES, data.get("same_path_function_aliases", {}))
    merge_nested_set_map(SAME_PATH_CONST_ALIASES, data.get("same_path_const_aliases", {}))
    merge_set_map(TEST_NAME_ALIASES, data.get("test_name_aliases", {}))
    merge_set_map(CHANGED_HELPER_CURRENT_ALIASES, data.get("changed_helper_aliases", {}))

PRODUCT_TEXT_EXTENSIONS = {
    ".go",
    ".md",
    ".mdx",
    ".py",
    ".ts",
    ".tsx",
    ".yaml",
    ".yml",
    ".zig",
}

PRODUCT_SCAN_ROOTS = (
    "docs",
    "examples",
    "go",
    "py",
    "specs",
    "ts",
    "zig",
)


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str


@dataclass
class ChangedFile:
    path: str
    source: str


def run_git(args: list[str]) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {proc.stderr.strip()}")
    return proc.stdout


def git_lines(args: list[str]) -> list[str]:
    return [line for line in run_git(args).splitlines() if line]


def audit_origin() -> str:
    proc = subprocess.run(
        ["git", "rev-parse", "--verify", "MERGE_HEAD"],
        cwd=ROOT,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode == 0:
        return "MERGE_HEAD"
    return FALLBACK_ORIGIN


def merge_base(origin: str) -> str:
    if AUDIT_BASE_OVERRIDE:
        return AUDIT_BASE_OVERRIDE
    return run_git(["merge-base", "HEAD", origin]).strip()


def origin_text(origin: str, path: str) -> str:
    return run_git(["show", f"{origin}:{path}"])


def current_text(paths: list[str]) -> str:
    parts: list[str] = []
    for raw in paths:
        path = ROOT / raw
        if path.is_dir():
            for child in sorted(path.rglob("*.zig")):
                parts.append(child.read_text(errors="replace"))
        elif path.exists():
            parts.append(path.read_text(errors="replace"))
    return "\n".join(parts)


def current_text_for_origin_path(path: str) -> str:
    return current_text(MOVED_CURRENT_PATHS.get(path, [path]))


def current_path_present_for_origin_path(path: str) -> bool:
    return any((ROOT / current_path).exists() for current_path in MOVED_CURRENT_PATHS.get(path, [path]))


def product_text_files() -> list[pathlib.Path]:
    files: list[pathlib.Path] = []
    for root_name in PRODUCT_SCAN_ROOTS:
        root = ROOT / root_name
        if not root.exists():
            continue
        for path in sorted(root.rglob("*")):
            if path.is_file() and path.suffix in PRODUCT_TEXT_EXTENSIONS:
                files.append(path)
    for path_name in ("Makefile", "openapi.yaml"):
        path = ROOT / path_name
        if path.exists():
            files.append(path)
    return files


def all_current_zig_test_names() -> set[str]:
    names: set[str] = set()
    for child in sorted((ROOT / "zig").rglob("*.zig")):
        try:
            child_names = zig_test_names(child.read_text(errors="replace"))
        except OSError:
            continue
        names |= child_names
        try:
            rel = child.relative_to(ROOT / "zig/pkg/antfly/src").with_suffix("")
            module_name = ".".join(rel.parts)
            for name in child_names:
                names.add(f"{module_name}.test.{name}")
        except ValueError:
            pass
    return names


def changed_files(origin: str) -> list[ChangedFile]:
    seen: dict[str, set[str]] = {}
    for source, args in [
        ("incoming", ["diff", "--name-only", f"{merge_base(origin)}..{origin}"]),
        ("staged", ["diff", "--cached", "--name-only"]),
        ("unstaged", ["diff", "--name-only"]),
        ("unmerged", ["diff", "--name-only", "--diff-filter=U"]),
    ]:
        for path in git_lines(args):
            seen.setdefault(path, set()).add(source)
    return [
        ChangedFile(path, "+".join(sorted(sources)))
        for path, sources in sorted(seen.items())
    ]


def changed_files_from_base(base: str, ref: str, source: str) -> list[ChangedFile]:
    return [
        ChangedFile(path, source)
        for path in git_lines(["diff", "--name-only", f"{base}..{ref}"])
    ]


def relabel_checks(prefix: str, checks: list[CheckResult]) -> list[CheckResult]:
    return [
        CheckResult(f"{prefix}: {check.name}", check.ok, check.detail)
        for check in checks
    ]


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


def changed_file_inventory(files: list[ChangedFile]) -> list[str]:
    counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    unstaged: list[str] = []
    for item in files:
        counts[path_category(item.path)] = counts.get(path_category(item.path), 0) + 1
        for source in item.source.split("+"):
            source_counts[source] = source_counts.get(source, 0) + 1
        if "unstaged" in item.source:
            unstaged.append(item.path)
    lines = [f"{len(files)} files in merge audit scope"]
    if source_counts:
        lines.append("sources: " + ", ".join(f"{name}={count}" for name, count in sorted(source_counts.items())))
    for category, count in sorted(counts.items()):
        lines.append(f"- {category}: {count}")
    if unstaged:
        lines.append("")
        lines.append("Tracked files with unstaged changes:")
        lines.extend(f"- {path}" for path in unstaged[:80])
        if len(unstaged) > 80:
            lines.append(f"- ... +{len(unstaged) - 80} more")
    return lines


def text_file_has_conflict_markers(path: str) -> bool:
    full = ROOT / path
    if not full.exists() or not full.is_file():
        return False
    try:
        data = full.read_text(errors="replace")
    except OSError:
        return False
    return re.search(
        r"^(?:<<<<<<< .+|\|\|\|\|\|\|\| .+|=======|>>>>>>> .+)$",
        data,
        re.MULTILINE,
    ) is not None


def pub_fns(text: str) -> set[str]:
    return set(re.findall(r"\bpub\s+(?:(?:extern|inline|noinline)\s+)*fn\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", text))


def fn_ptr_fields(text: str) -> set[str]:
    return set(re.findall(r"\n\s*([a-z][A-Za-z0-9_]*)\s*:\s*(?:\?|\*|const|anyerror|void|!|\[|\])*fn\b", text))


def all_fns(text: str) -> set[str]:
    return set(re.findall(r"\n(?:pub\s+)?(?:extern\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", text))


def zig_function_spans(text: str) -> dict[str, str]:
    starts = [
        (match.start(), match.group(1))
        for match in re.finditer(r"(?m)^\s*(?:pub\s+)?(?:extern\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", text)
    ]
    spans: dict[str, str] = {}
    for index, (start, name) in enumerate(starts):
        end = starts[index + 1][0] if index + 1 < len(starts) else len(text)
        spans[name] = text[start:end]
    return spans


def pub_consts(text: str) -> set[str]:
    return set(re.findall(r"\n\s*pub\s+const\s+([A-Za-z_][A-Za-z0-9_]*)\s*=", text))


def pub_members(text: str) -> set[str]:
    return pub_fns(text) | pub_consts(text)


def pub_members_of_file(path: str, seen: set[str] | None = None) -> set[str]:
    seen = seen or set()
    if path in seen:
        return set()
    seen.add(path)
    try:
        text = git_index_or_worktree_text(path)
    except (RuntimeError, OSError, UnicodeDecodeError):
        return set()
    members = pub_members(text)
    for import_path in re.findall(r"\b(?:pub\s+)?usingnamespace\s+@import\(\"([^\"]+)\"\)", text):
        resolved = resolve_local_import(path, import_path)
        if resolved:
            members |= pub_members_of_file(resolved, seen)
    return members


def zig_test_names(text: str) -> set[str]:
    names = set(re.findall(r'\btest\s+"([^"]+)"\s*\{', text))
    if re.search(r"\btest\s*\{", text):
        names.add("<anonymous-ref-all-decls>")
    return names


def non_zig_test_names(path: str, text: str) -> set[str]:
    if path.endswith("_test.go"):
        return set(re.findall(r"\bfunc\s+(Test[A-Za-z0-9_]+)\s*\(", text))
    if path.endswith(".py") and pathlib.PurePosixPath(path).name.startswith("test_"):
        return set(re.findall(r"^\s*def\s+(test_[A-Za-z0-9_]+)\s*\(", text, re.MULTILINE))
    if path.endswith((".test.ts", ".test.tsx", ".spec.ts", ".spec.tsx")):
        return set(re.findall(r"\b(?:test|it|describe)\s*\(\s*['\"]([^'\"]+)", text))
    return set()


TEST_NAME_PREFIXES = (
    "api.table_reads.docid ",
    "remote wire doc identity ",
    "api.table_writes.docid ",
    "api.table_writes.query_visibility ",
)

DB_TEST_MOVE_PREFIXES = (
    "db generation repair ",
    "db artifact repair ",
    "db split restore ",
    "db derived async replay ",
    "db derived async dense artifact rebuild ",
    "db derived async ",
    "db write path replay ",
    "db write path extract enrichments ",
    "db write path transform relational batch ",
    "db write path transform ",
    "db write path bulk ingest ",
    "db write path bulk ingest dense auto ",
    "db write path document artifact child range ",
    "db write path document artifact child range dispatches generated artifacts ",
    "db write path doc identity ",
    "db write path direct graph writes ",
    "db write path ",
    "db split restore doc identity ",
    "db transactions ",
    "db lifecycle open ",
    "db lifecycle quarantine ",
    "db lifecycle doc identity ",
    "db lifecycle ",
    "db search runtime graph composition ",
    "db search runtime projection ",
    "db search runtime reopen ",
    "db search runtime dense chunk ",
    "db search runtime full-text chunk ",
    "db search runtime text schema root ",
    "db search runtime text schema ",
    "db search runtime identity ",
    "db search runtime indexing ",
    "db search runtime scan ",
    "db search runtime preflight ",
    "db search runtime ",
    "db schema runtime enrichment catalog add ",
    "db schema runtime enrichment catalog delete ",
    "db schema runtime enrichment catalog upsert ",
    "db schema runtime algebraic ",
    "db schema runtime ",
    "db enrichment runtime asset producer ",
    "db enrichment runtime chunked dense ",
    "db enrichment runtime chunked sparse ",
    "db enrichment runtime dense ",
    "db enrichment runtime document extraction async ",
    "db enrichment runtime document extraction ",
    "db enrichment runtime enrichments sync ",
    "db enrichment runtime full_index sync ",
    "db enrichment runtime managed dense ",
    "db enrichment runtime precomputed ",
    "db enrichment runtime sparse ",
    "db enrichment runtime ",
    "db resolution runtime ",
    "db graph runtime ",
    "db query result shape ",
    "db internal ",
    "db relational rows ",
)

TEST_NAME_ALIASES = {
    "document SQL lowers null equality comparisons to policy bounded residual scan": {
        "document SQL lowers null equality comparisons to indexed match none without scan",
    },
    "document SQL lowers null range and pattern predicates to policy bounded residual scan": {
        "document SQL lowers null range and pattern predicates to indexed match none under bounded policy",
    },
    "native dense constraints fail closed without ordinal vector mapping": {
        "db query result shape native dense constraints fail closed without ordinal vector mapping",
    },
    # The split branch intentionally prevents read-side replay while preserving
    # the origin/main asset-producer config coverage.
    "provisioned query db installs asset producer from indexes_json and replays assets": {
        "provisioned query db installs asset producer from indexes_json without read-side replay",
    },
    "provisioned read cache clear preserves in-flight pending opens and bumps epoch": {
        "provisioned read cache clear preserves in-flight pending opens and bumps table epoch",
    },
    "simple vector shard request lowers to vector worker envelope": {
        "vector worker envelope converts to constrained search request",
    },
    "simple vector shard request carries serializable resolved doc filter": {
        "vector worker request carries serializable resolved doc filter",
    },
    "encode query request serializes internal resolved doc filters with wire context": {
        "query request serializes internal resolved doc filters with wire context",
    },
    "parseRemoteSearchResult preserves fused index scores": {
        "remote query parser preserves total relation and fused index scores",
    },
    "public sync level text accepts full_index and rejects removed aknn alias": {
        "public sync level text maps query and rejects deprecated spellings",
    },
    "table contract ignores create-table full text entries and preserves non-full-text indexes": {
        "table contract ignores create-table full text entries and preserves typed path metadata",
    },
    "schema capability plan emits non-materializing algebraic config skeleton": {
        "schema capability plan emits default materializations from group and measure fields",
    },
    "client chunker config keeps flattened provider-specific fields": {
        "public chunker config keeps flattened provider-specific fields",
    },
    "dirty auto bulk writer publishes runtime status before read invalidation closes it": {
        "dirty auto bulk writer publishes runtime status before read preparation clears dirty state",
    },
    "provisioned table write source backs up a portable local table": {
        "provisioned table write source backs up and restores a portable local table",
    },
    "write cache adopts active just-created db across generation bump": {
        "write cache adopts just-created db across reconcile generation bump",
    },
    "write cache transfers adoptable provisioned db to raft apply source": {
        "write cache transfers adoptable provisioned db to destination cache",
    },
    "write cache local mutation preempts stale startup writer": {
        "write cache adopts just-created db across reconcile generation bump",
    },
    "provisioned txn resolve invalidates cached writer state on commit": {
        "provisioned txn commit reuses cached writer state",
    },
    "provisioned table write source startup snapshot builds synthetic status from object-form indexes json": {
        "runtime status startup snapshot builds synthetic status from object-form indexes json",
    },
    "provisioned table write source startup snapshot builds synthetic status from array-form indexes json": {
        "runtime status startup snapshot builds synthetic status from array-form indexes json",
    },
    "db doc set planning stats record ordinal bitmap promotion": {
        "doc set planning stats record ordinal bitmap promotion",
    },
    "db batch treats reserved namespace bytes as user document ids": {
        "db search runtime identity treats reserved namespace bytes as user document ids",
    },
    "db asset producer enrichments execute fake providers and skip unchanged state": {
        "db enrichment runtime asset producer executes fake providers and skips unchanged state",
    },
    "db graph methods expose edges, neighbors, and shortest path": {
        "db graph runtime helpers expose edges neighbors and shortest path",
    },
    "db lsm primary compaction preserves doc identity ordinals": {
        "db lifecycle doc identity lsm primary compaction preserves ordinals",
    },
    "db replicated apply decouples client enrichment sync from raft apply execution": {
        "db enrichment runtime replicated apply decouples client sync from raft apply execution",
    },
    "db transaction intent writes reject new documents at ordinal exhaustion": {
        "db transactions doc identity intent writes reject new documents at ordinal exhaustion",
    },
    "db exposes local transaction lifecycle": {
        "db transactions local lifecycle exposes committed and deleted documents",
    },
    "db query_readonly reopen during active bulk ingest serves empty dense search instead of index-not-found": {
        "db write path bulk ingest query_readonly reopen serves empty dense search instead of index-not-found",
    },
    "db retries remote document child range dispatch from durable outbox": {
        "db write path document artifact child range retries remote dispatch from durable outbox",
    },
    "db stats report engine-owned algebraic adaptive observation status": {
        "db schema runtime algebraic adaptive stats report engine-owned observation status",
    },
    "db ttl cleanup background worker starts and deletes with manual clock": {
        "db ttl cleanup can run with manual clock",
    },
    "db stats expose document identity coverage and tombstones": {
        "db lifecycle doc identity stats expose coverage and tombstones",
    },
    "data runtime structural changes preserve writer-published runtime status": {
        "data runtime structural changes preserve writer-published runtime status and schedule catch-up",
    },
    "api index status falls through to read runtime status when write cache is empty": {
        "api index status uses read runtime status without consulting write source",
    },
    "db replay skips dense embedding writes when artifact payload is missing": {
        "db replay blocks dense embedding writes when artifact payload is missing",
    },
    "replay skips dense embedding writes when artifact payload is missing": {
        "db replay blocks dense embedding writes when artifact payload is missing",
    },
    "skips dense embedding writes when artifact payload is missing": {
        "db replay blocks dense embedding writes when artifact payload is missing",
    },
    "db skips dense embedding writes when artifact payload is missing": {
        "db replay blocks dense embedding writes when artifact payload is missing",
    },
    "db derived async replay skips dense embedding writes when artifact payload is missing": {
        "db replay blocks dense embedding writes when artifact payload is missing",
    },
    "db replay skips and deletes corrupt dense embedding artifacts": {
        "db replay blocks and preserves corrupt dense embedding artifacts",
    },
    "replay skips and deletes corrupt dense embedding artifacts": {
        "db replay blocks and preserves corrupt dense embedding artifacts",
    },
    "skips and deletes corrupt dense embedding artifacts": {
        "db replay blocks and preserves corrupt dense embedding artifacts",
    },
    "db skips and deletes corrupt dense embedding artifacts": {
        "db replay blocks and preserves corrupt dense embedding artifacts",
    },
    "db derived async replay skips and deletes corrupt dense embedding artifacts": {
        "db replay blocks and preserves corrupt dense embedding artifacts",
    },
    "db replay skips and deletes corrupt sparse embedding artifacts": {
        "db replay blocks and preserves corrupt sparse embedding artifacts",
    },
    "replay skips and deletes corrupt sparse embedding artifacts": {
        "db replay blocks and preserves corrupt sparse embedding artifacts",
    },
    "skips and deletes corrupt sparse embedding artifacts": {
        "db replay blocks and preserves corrupt sparse embedding artifacts",
    },
    "db skips and deletes corrupt sparse embedding artifacts": {
        "db replay blocks and preserves corrupt sparse embedding artifacts",
    },
    "db derived async replay skips and deletes corrupt sparse embedding artifacts": {
        "db replay blocks and preserves corrupt sparse embedding artifacts",
    },
    "db deletes corrupt stored embedding artifacts": {
        "db rebuild dense indexes preserves corrupt stored embedding artifacts",
    },
    "deletes corrupt stored embedding artifacts": {
        "db rebuild dense indexes preserves corrupt stored embedding artifacts",
    },
    "db dense artifact rebuild deletes corrupt stored embedding artifacts": {
        "db rebuild dense indexes preserves corrupt stored embedding artifacts",
    },
    "dense artifact rebuild deletes corrupt stored embedding artifacts": {
        "db rebuild dense indexes preserves corrupt stored embedding artifacts",
    },
    "db derived async dense artifact rebuild deletes corrupt stored embedding artifacts": {
        "db rebuild dense indexes preserves corrupt stored embedding artifacts",
    },
    "db dense target advance is blocked while catch-up bulk session is active": {
        "dense target advance is blocked while external bulk session is active",
    },
    "db lifecycle dense target advance is blocked while catch-up bulk session is active": {
        "dense target advance is blocked while external bulk session is active",
    },
    "dense target advance is blocked while catch-up bulk session is active": {
        "dense target advance is blocked while external bulk session is active",
    },
    "db stats flag document identity ordinal capacity exhaustion": {
        "db lifecycle doc identity stats flag ordinal capacity exhaustion",
    },
    "db merge-style cutover fences enrichment to the merged receiver range": {
        "db merge-style cutover enrichment fence owns merged receiver range",
    },
    "db merge-style cutover fences enrichment to the merged receiver range with durable lsm primary backend": {
        "db merge-style cutover enrichment fence owns merged receiver range with durable lsm primary backend",
    },
    "db merge-style cutover preserves enrichment resume and fencing across reopen": {
        "db merge-style cutover enrichment resume and fencing across reopen",
    },
    "db merge-style cutover preserves enrichment resume and fencing across reopen with durable lsm primary backend": {
        "db merge-style cutover enrichment resume and fencing across reopen with durable lsm primary backend",
    },
    "db split cutover fences enrichment to the owning range": {
        "db split cutover enrichment fence owns split range",
    },
    "db split cutover fences enrichment to the owning range with durable lsm primary backend": {
        "db split cutover enrichment fence owns split range with durable lsm primary backend",
    },
    "db split cutover preserves enrichment resume and fencing across reopen": {
        "db split cutover enrichment resume and fencing across reopen",
    },
    "db split cutover preserves enrichment resume and fencing across reopen with durable lsm primary backend": {
        "db split cutover enrichment resume and fencing across reopen with durable lsm primary backend",
    },
    "generated SQL parser reports malformed unsupported trigger diagnostics": {
        "generated SQL parser reports bounded diagnostics for malformed corpus",
    },
    "public api e2e restores managed sparse embeddings from table backup": {
        "public api e2e restores sparse embeddings from table backup",
    },
    "public api e2e supports template chunked remote text enrichment and query helper failures": {
        "public api e2e supports template chunked enrichment and remote text query helper failures",
    },
    "query encoder omits _source for key-only hits": {
        "query encoder emits null _source for key-only hits",
    },
    "query parser defaults to key-only when fields are omitted": {
        "query parser includes stored source when fields are omitted",
    },
}


def canonical_test_name(name: str) -> str:
    current = name
    changed = True
    while changed:
        changed = False
        for prefix in TEST_NAME_PREFIXES:
            if current.startswith(prefix):
                current = current[len(prefix) :]
                changed = True
    return current


def normalized_test_names(names: set[str]) -> set[str]:
    normalized = {canonical_test_name(name) for name in names}
    expanded = set(normalized)
    for name in normalized:
        if name.startswith("table write source "):
            expanded.add("provisioned table write source " + name[len("table write source ") :])
        if name.startswith("read preparation "):
            expanded.add("provisioned read preparation " + name[len("read preparation ") :])
        if name.startswith("managed publish hook "):
            expanded.add("managed visibility publish hook " + name[len("managed publish hook ") :])
        for prefix in DB_TEST_MOVE_PREFIXES:
            if not name.startswith(prefix):
                continue
            stripped = name[len(prefix) :]
            expanded.add(stripped)
            expanded.add("db " + stripped)
            if prefix == "db graph runtime ":
                expanded.add("db graph " + stripped)
                if stripped.startswith("async asset producer source "):
                    expanded.add("db async asset producer graph source " + stripped[len("async asset producer source ") :])
                if stripped.startswith("helpers expose "):
                    expanded.add("db graph methods expose " + stripped[len("helpers expose ") :])
            if prefix == "db transactions ":
                expanded.add("db transaction " + stripped)
                if stripped.startswith("resolve transforms "):
                    expanded.add("db transaction resolves transforms " + stripped[len("resolve transforms ") :])
                if stripped.startswith("created identity rows "):
                    expanded.add("db transaction-created identity rows " + stripped[len("created identity rows ") :])
            if prefix == "db write path transform ":
                expanded.add("db batch " + stripped)
            if prefix == "db write path bulk ingest " and stripped.startswith("algebraic "):
                expanded.add("db algebraic bulk ingest " + stripped[len("algebraic ") :])
            if prefix == "db write path bulk ingest dense auto ":
                expanded.add("db dense auto bulk " + stripped)
            if prefix == "db write path document artifact child range ":
                expanded.add("db applies document artifact child range " + stripped)
                if stripped.startswith("applies batch "):
                    expanded.add("db applies document artifact child range batch " + stripped[len("applies batch ") :])
            if prefix == "db enrichment runtime document extraction async ":
                expanded.add("db async document extraction " + stripped)
            if prefix == "db enrichment runtime document extraction " and stripped.startswith("unit payload "):
                expanded.add("db document unit payload " + stripped[len("unit payload ") :])
            if prefix == "db enrichment runtime asset producer ":
                expanded.add("db asset producer enrichments " + stripped)
                if stripped.startswith("executes "):
                    expanded.add("db asset producer enrichments execute " + stripped[len("executes ") :])
            if prefix == "db enrichment runtime chunked dense ":
                expanded.add("db chunked dense enrichment " + stripped)
            if prefix == "db enrichment runtime chunked sparse ":
                expanded.add("db chunked sparse enrichment " + stripped)
            if prefix == "db enrichment runtime dense ":
                expanded.add("db dense enrichment " + stripped)
            if prefix == "db enrichment runtime sparse ":
                expanded.add("db sparse enrichment " + stripped)
            if prefix == "db enrichment runtime managed dense ":
                expanded.add("db managed dense enrichment " + stripped)
            if prefix == "db enrichment runtime full_index sync ":
                expanded.add("db full_index " + stripped)
            if prefix == "db enrichment runtime enrichments sync ":
                expanded.add("db enrichments " + stripped)
            if prefix == "db enrichment runtime precomputed ":
                expanded.add("db enrichments precomputed " + stripped)
            if prefix == "db enrichment runtime " and stripped.startswith("status changes "):
                expanded.add("db enrichment status changes " + stripped[len("status changes ") :])
            if prefix == "db search runtime dense chunk ":
                expanded.add("db " + stripped.replace("supports parent search", "supports dense parent search"))
            if prefix == "db search runtime full-text chunk " and stripped.startswith("default index "):
                expanded.add("db default full text index " + stripped[len("default index ") :])
            if prefix == "db search runtime identity " and stripped.startswith("algebraic doc facts "):
                expanded.add("db dense algebraic doc facts " + stripped[len("algebraic doc facts ") :])
            if prefix == "db search runtime preflight ":
                expanded.add("db preflightSearchRequest " + stripped)
            if prefix == "db schema runtime enrichment catalog upsert ":
                expanded.add("db upsertEnrichment " + stripped)
            if prefix == "db schema runtime algebraic " and stripped.startswith("evaluates policy-gated "):
                expanded.add("db evaluates policy-gated algebraic " + stripped[len("evaluates policy-gated ") :])
            if prefix == "db lifecycle quarantine " and stripped.startswith("drops dense index "):
                expanded.add("db drops quarantined dense index " + stripped[len("drops dense index ") :])
            if prefix == "db lifecycle quarantine " and stripped.startswith("self-heals "):
                expanded.add("db quarantined index self-heals " + stripped[len("self-heals ") :])
            if prefix == "db lifecycle open ":
                if stripped.startswith("status_only "):
                    expanded.add("db status_only open " + stripped[len("status_only ") :])
                if stripped.startswith("query_readonly "):
                    expanded.add("db query_readonly open " + stripped[len("query_readonly ") :])
                if stripped.startswith("read-only "):
                    expanded.add("db read-only open " + stripped[len("read-only ") :])
                if stripped.startswith("writer_no_replay "):
                    expanded.add("db writer_no_replay open " + stripped[len("writer_no_replay ") :])
            if prefix == "db split restore doc identity ":
                if stripped.startswith("deferred restore rejects strict "):
                    expanded.add("db deferred restore rejects strict doc identity " + stripped[len("deferred restore rejects strict ") :])
                if stripped.startswith("snapshot rejects invalid "):
                    expanded.add("db restore snapshot rejects invalid doc identity " + stripped[len("snapshot rejects invalid ") :])
                if stripped.startswith("runtime repair "):
                    expanded.add("db explicit restore runtime repair " + stripped[len("runtime repair ") :])
            if stripped.startswith("addEnrichment "):
                expanded.add("db " + stripped)
            if prefix == "db schema runtime enrichment catalog add ":
                expanded.add("db addEnrichment " + stripped)
            if prefix == "db schema runtime enrichment catalog delete ":
                expanded.add("db deleteEnrichment " + stripped)
            if prefix == "db write path extract enrichments ":
                expanded.add("db extractEnrichments " + stripped)
            if prefix == "db write path document artifact child range dispatches generated artifacts ":
                expanded.add("db dispatches generated document child range artifacts " + stripped)
    normalized = expanded
    for old, aliases in TEST_NAME_ALIASES.items():
        if aliases & normalized:
            normalized.add(old)
    return normalized


def changed_test_inventory(files: list[ChangedFile]) -> list[str]:
    counts = {
        "zig": 0,
        "go": 0,
        "python": 0,
        "typescript": 0,
        "other": 0,
    }
    samples: list[str] = []
    for item in files:
        path = item.path
        is_test = False
        category = "other"
        if path.endswith(".zig"):
            try:
                is_test = "test " in git_index_or_worktree_text(path)
            except (RuntimeError, OSError, UnicodeDecodeError):
                is_test = False
            category = "zig"
        elif path.endswith("_test.go") or path.endswith("/test.go"):
            is_test = True
            category = "go"
        elif pathlib.PurePosixPath(path).name.startswith("test_") and path.endswith(".py"):
            is_test = True
            category = "python"
        elif path.endswith((".test.ts", ".test.tsx", ".spec.ts", ".spec.tsx")):
            is_test = True
            category = "typescript"
        if not is_test:
            continue
        counts[category] += 1
        if len(samples) < 30:
            samples.append(path)
    total = sum(counts.values())
    lines = [f"{total} changed test-bearing files"]
    lines.extend(f"- {name}: {count}" for name, count in counts.items() if count)
    if samples:
        lines.append("samples: " + ", ".join(samples))
    return lines


def file_exists_at_origin(origin: str, path: str) -> bool:
    proc = subprocess.run(
        ["git", "cat-file", "-e", f"{origin}:{path}"],
        cwd=ROOT,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return proc.returncode == 0


def git_index_or_worktree_text(path: str) -> str:
    full = ROOT / path
    if full.exists():
        return full.read_text(errors="replace")
    return run_git(["show", f":{path}"])


def resolve_local_import(from_path: str, import_path: str) -> str | None:
    if not import_path.endswith(".zig"):
        return None
    base = (ROOT / from_path).parent
    resolved = (base / import_path).resolve()
    try:
        rel = resolved.relative_to(ROOT)
    except ValueError:
        return None
    rel_text = rel.as_posix()
    if not (ROOT / rel_text).exists():
        return None
    return rel_text


def struct_body(text: str, name: str) -> str:
    marker = f"pub const {name} = struct"
    start = text.find(marker)
    if start < 0:
        marker = f"const {name} = struct"
        start = text.find(marker)
    if start < 0:
        return ""
    brace = text.find("{", start)
    if brace < 0:
        return ""
    depth = 0
    for idx in range(brace, len(text)):
        ch = text[idx]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[brace + 1 : idx]
    return ""


def struct_fields(text: str, name: str) -> set[str]:
    body = struct_body(text, name)
    fields: set[str] = set()
    depth = 0
    for line in body.splitlines():
        top_level = depth == 0
        if top_level:
            match = re.match(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s*:", line)
        else:
            match = None
        if top_level and match:
            fields.add(match.group(1))
        depth += line.count("{") - line.count("}")
    return fields


def source_vtable_bindings(text: str, source_name: str) -> set[str]:
    body = struct_body(text, source_name)
    if not body:
        return set()
    match = re.search(r"pub\s+fn\s+source\([^)]*\).*?\.vtable\s*=\s*&\.\{", body, re.S)
    if not match:
        return set()
    start = match.end()
    depth = 1
    idx = start
    while idx < len(body) and depth:
        if body[idx] == "{":
            depth += 1
        elif body[idx] == "}":
            depth -= 1
        idx += 1
    initializer = body[start : idx - 1]
    return set(re.findall(r"\.([a-z][A-Za-z0-9_]*)\s*=", initializer))


def compare_named_sets(name: str, old: set[str], new: set[str], allow_missing: set[str] | None = None) -> CheckResult:
    allow_missing = allow_missing or set()
    missing = sorted((old - new) - allow_missing)
    if not missing:
        return CheckResult(name, True, "ok")
    sample = ", ".join(missing[:80])
    suffix = "" if len(missing) <= 80 else f" ... +{len(missing) - 80} more"
    return CheckResult(name, False, f"{len(missing)} missing: {sample}{suffix}")


def compare_missing_by_file(name: str, missing_by_file: dict[str, list[str]]) -> CheckResult:
    if not missing_by_file:
        return CheckResult(name, True, "ok")
    parts: list[str] = []
    for path, values in list(missing_by_file.items())[:20]:
        parts.append(f"{path}: {', '.join(values[:20])}")
        if len(values) > 20:
            parts[-1] += f" ... +{len(values) - 20} more"
    detail = "; ".join(parts)
    if len(missing_by_file) > 20:
        detail += f"; ... +{len(missing_by_file) - 20} files"
    return CheckResult(name, False, detail)


def check_surface_symbols(origin: str) -> list[CheckResult]:
    checks: list[CheckResult] = []
    if file_exists_at_origin(origin, "zig/pkg/antfly/src/api/tables.zig"):
        checks.append(compare_named_sets(
            "tables public functions",
            pub_fns(origin_text(origin, "zig/pkg/antfly/src/api/tables.zig")),
            pub_fns(current_text(MOVED_CURRENT_PATHS["zig/pkg/antfly/src/api/tables.zig"])),
        ))
        checks.append(compare_named_sets(
            "tables public consts",
            pub_consts(origin_text(origin, "zig/pkg/antfly/src/api/tables.zig")),
            pub_consts(current_text(MOVED_CURRENT_PATHS["zig/pkg/antfly/src/api/tables.zig"])),
        ))
    if file_exists_at_origin(origin, "zig/pkg/antfly/src/api/table_reads.zig"):
        checks.append(compare_named_sets(
            "table_reads public functions",
            pub_fns(origin_text(origin, "zig/pkg/antfly/src/api/table_reads.zig")),
            pub_fns(current_text([
                "zig/pkg/antfly/src/api/table_reads.zig",
                "zig/pkg/antfly/src/api/table_reads",
            ])),
        ))
        checks.append(compare_named_sets(
            "table_reads vtable functions",
            fn_ptr_fields(origin_text(origin, "zig/pkg/antfly/src/api/table_reads.zig")),
            fn_ptr_fields(current_text([
                "zig/pkg/antfly/src/api/table_reads.zig",
                "zig/pkg/antfly/src/api/table_reads",
            ])),
        ))
    if file_exists_at_origin(origin, "zig/pkg/antfly/src/api/table_writes.zig"):
        checks.append(compare_named_sets(
            "table_writes public functions",
            pub_fns(origin_text(origin, "zig/pkg/antfly/src/api/table_writes.zig")),
            pub_fns(current_text([
                "zig/pkg/antfly/src/api/table_writes.zig",
                "zig/pkg/antfly/src/api/table_writes",
            ])),
        ))
        checks.append(compare_named_sets(
            "table_writes vtable functions",
            fn_ptr_fields(origin_text(origin, "zig/pkg/antfly/src/api/table_writes.zig")),
            fn_ptr_fields(current_text([
                "zig/pkg/antfly/src/api/table_writes.zig",
                "zig/pkg/antfly/src/api/table_writes",
            ])),
        ))
    if file_exists_at_origin(origin, "zig/pkg/antfly/src/storage/db/db.zig"):
        db_ignored = (
            DB_PUB_FN_FALSE_POSITIVES
            | SAME_PATH_FUNCTION_FALSE_POSITIVES.get(
                "zig/pkg/antfly/src/storage/db/db.zig", set()
            )
        )
        checks.append(compare_named_sets(
            "db public functions",
            pub_fns(origin_text(origin, "zig/pkg/antfly/src/storage/db/db.zig")),
            pub_fns(current_text(["zig/pkg/antfly/src/storage/db"])),
            db_ignored,
        ))
    return checks


def helper_alias_present(name: str, current: str) -> bool:
    return any(token in current for token in CHANGED_HELPER_CURRENT_ALIASES.get(name, set()))


def current_symbol_alias_exists(alias: str, same_path_text: str, kind: str) -> bool:
    """Resolve a same-path or `path::symbol` merge-audit alias."""

    target_text = same_path_text
    symbol = alias
    if "::" in alias:
        target_path, symbol = alias.rsplit("::", 1)
        try:
            target_text = git_index_or_worktree_text(target_path)
        except (RuntimeError, OSError, UnicodeDecodeError):
            return False

    if kind == "fn":
        return re.search(
            rf"\b(?:pub\s+)?fn\s+{re.escape(symbol)}\b",
            target_text,
        ) is not None
    if kind == "const":
        return re.search(
            rf"\bpub\s+const\s+{re.escape(symbol)}\b",
            target_text,
        ) is not None
    raise ValueError(f"unsupported Zig alias kind: {kind}")


def check_changed_split_helper_names(origin: str) -> list[CheckResult]:
    base = merge_base(origin)
    origin_to_current = {
        "zig/pkg/antfly/src/api/table_reads.zig": [
            "zig/pkg/antfly/src/api/table_reads.zig",
            "zig/pkg/antfly/src/api/table_reads",
        ],
        "zig/pkg/antfly/src/api/table_writes.zig": [
            "zig/pkg/antfly/src/api/table_writes.zig",
            "zig/pkg/antfly/src/api/table_writes",
        ],
        "zig/pkg/antfly/src/api/tables.zig": MOVED_CURRENT_PATHS["zig/pkg/antfly/src/api/tables.zig"],
        "zig/pkg/antfly/src/storage/db/db.zig": ["zig/pkg/antfly/src/storage/db"],
    }
    missing_by_file: dict[str, list[str]] = {}
    changed_count = 0
    for path, current_paths in origin_to_current.items():
        if not file_exists_at_origin(origin, path):
            continue
        old = zig_function_spans(origin_text(origin, path))
        try:
            old_base = zig_function_spans(run_git(["show", f"{base}:{path}"]))
        except RuntimeError:
            old_base = {}
        current = current_text(current_paths)
        missing: list[str] = []
        for name, body in sorted(old.items()):
            if old_base.get(name) == body:
                continue
            changed_count += 1
            if re.search(r"\b" + re.escape(name) + r"\b", current):
                continue
            if helper_alias_present(name, current):
                continue
            missing.append(name)
        if missing:
            missing_by_file[path] = missing

    if missing_by_file:
        return [compare_missing_by_file("changed split helper function names from incoming", missing_by_file)]
    return [
        CheckResult(
            "changed split helper function names from incoming",
            True,
            f"{changed_count} changed helper functions covered by current name or documented split alias",
        )
    ]


def check_request_and_hit_fields(origin: str) -> list[CheckResult]:
    old_types = origin_text(origin, "zig/pkg/antfly/src/storage/db/types.zig")
    new_types = current_text(["zig/pkg/antfly/src/storage/db/types.zig"])
    return [
        compare_named_sets(
            "SearchRequest fields",
            struct_fields(old_types, "SearchRequest"),
            struct_fields(new_types, "SearchRequest"),
        ),
        compare_named_sets(
            "SearchHit fields",
            struct_fields(old_types, "SearchHit"),
            struct_fields(new_types, "SearchHit"),
        ),
    ]


def check_exact_sort_gap(origin: str) -> list[CheckResult]:
    old_search_exec = origin_text(origin, "zig/pkg/antfly/src/storage/db/query/search_exec.zig")
    new_search_exec = current_text(["zig/pkg/antfly/src/storage/db/query/search_exec.zig"])
    old_contract = origin_text(origin, "zig/pkg/antfly/src/api/query_contract.zig")
    new_contract = current_text([
        "zig/pkg/antfly/src/query/contract.zig",
        "zig/pkg/antfly/src/api/query_contract.zig",
    ])
    index_manager = current_text(["zig/pkg/antfly/src/storage/db/catalog/index_manager.zig"])
    dense_exact = current_text(["zig/pkg/antfly/src/storage/db/dense_exact.zig"])
    missing_exec = sorted(CRITICAL_SORT_EXEC_SYMBOLS & (all_fns(old_search_exec) - all_fns(new_search_exec)))
    missing_contract = sorted(CRITICAL_QUERY_CONTRACT_SORT_SYMBOLS & (all_fns(old_contract) - all_fns(new_contract)))
    exact_bridge_ok = (
        "pub fn exactScoreDenseEntryWithRequest" in index_manager
        and (
            "resolveExactDenseDocKeyAlloc" in index_manager
            or (
                "CandidateDifference.init" in index_manager
                and "getMetadataManySortedInTxn" in index_manager
                and "lookupDocIdTxn" in index_manager
                and "pub const CandidateDifference" in dense_exact
            )
        )
        and "exactScoreRequest" not in index_manager
    )
    return [
        CheckResult(
            "critical exact-sort executor symbols",
            not missing_exec,
            "ok" if not missing_exec else "missing: " + ", ".join(missing_exec),
        ),
        CheckResult(
            "critical exact-sort query-contract symbols",
            not missing_contract,
            "ok" if not missing_contract else "missing: " + ", ".join(missing_contract),
        ),
        CheckResult(
            "critical exact-sort dense bridge",
            exact_bridge_ok,
            "ok" if exact_bridge_ok else "index_manager exact dense scoring bridge is missing or calls stale exactScoreRequest",
        ),
    ]


def check_deadline_and_aknn() -> list[CheckResult]:
    src = current_text([
        "zig/pkg/antfly/src/api",
        "zig/pkg/antfly/src/query",
        "zig/pkg/antfly/src/storage/db",
    ])
    deadline_hits = src.count("execution_deadline_ns")
    aknn_sync_level = [
        line
        for line in src.splitlines()
        if re.search(r"aknn.*sync_level|sync_level.*aknn", line, flags=re.IGNORECASE)
        and "batch parser rejects removed aknn sync level" not in line
        and '\\"sync_level\\":\\"aknn\\"' not in line
        and '"sync_level":"aknn"' not in line
    ]
    return [
        CheckResult(
            "execution_deadline_ns wiring",
            deadline_hits >= 5,
            f"{deadline_hits} occurrences",
        ),
        CheckResult(
            "aknn sync_level residue",
            len(aknn_sync_level) == 0,
            INTENTIONAL_TEXT["aknn_sync_level_removed"] if len(aknn_sync_level) == 0 else f"{len(aknn_sync_level)} suspicious references",
        ),
    ]


def check_public_sync_level_naming() -> list[CheckResult]:
    failures: list[str] = []

    expected_tokens_by_path = {
        "examples/epstein/main.go": [
            'case "query":',
            'return antfly.SyncLevelQuery, nil',
            'case "full_index":',
            'return antfly.SyncLevelFullIndex, nil',
            "expected propose, write, query, enrichments, or full_index",
        ],
        "examples/epstein/main_test.go": [
            '{in: " query ", want: antfly.SyncLevelQuery}',
            'parseSyncLevelFlag("full_text")',
            'parseSyncLevelFlag("embeddings")',
            'parseSyncLevelFlag("aknn")',
        ],
        "specs/openapi/antfly/metadata.yaml": [
            "- query",
            "- full_index",
            '"query": Wait until affected documents are visible to query paths',
            '"full_index": Wait for all index writes to complete',
        ],
        "zig/ENRICHMENTS.md": [
            "`sync_level=query`",
            "`sync_level=enrichments` or `sync_level=full_index`",
        ],
        "go/pkg/antfly/src/metadata/api.go": [
            'case "query":',
            'return db.Op_SyncLevelFullText, nil',
            'case "full_index":',
            'return db.Op_SyncLevelEmbeddings, nil',
        ],
        "go/pkg/antfly/src/metadata/api_helpers_test.go": [
            '{"Query", "query", db.Op_SyncLevelFullText, false}',
            '{"FullIndex", "full_index", db.Op_SyncLevelEmbeddings, false}',
            '{"OldFullText", "full_text", db.Op_SyncLevelPropose, true}',
            '{"OldEmbeddings", "embeddings", db.Op_SyncLevelPropose, true}',
            '{"OldAknn", "aknn", db.Op_SyncLevelPropose, true}',
        ],
    }
    forbidden_tokens_by_path = {
        "examples/epstein/main.go": [
            "SyncLevelFullText",
            'case "full_text":',
            'case "embeddings":',
            "full_text, enrichments",
            "or embeddings",
        ],
        "specs/openapi/antfly/metadata.yaml": [
            '"aknn"',
        ],
        "ts/packages/sdk/src/public-api.d.ts": [
            '"full_text" | "enrichments"',
            '"embeddings" | "full_index"',
            '"aknn"',
        ],
        "go/pkg/sdk/requests.go": [
            "SyncLevelFullText",
            "SyncLevelAknn",
        ],
        "go/pkg/sdk/types.go": [
            "SyncLevelFullText",
            "SyncLevelEmbeddings",
            "SyncLevelAknn",
        ],
        "zig/ENRICHMENTS.md": [
            "sync_level=full_text",
            "sync_level=embeddings",
            "sync_level=aknn",
        ],
        "go/pkg/antfly/src/metadata/api.go": [
            'case "full_text":',
            'case "embeddings":',
            'case "aknn":',
        ],
        "go/pkg/antfly/src/metadata/api_helpers_test.go": [
            '{"FullText", "full_text", db.Op_SyncLevelFullText, false}',
            '{"Embeddings", "embeddings", db.Op_SyncLevelEmbeddings, false}',
            '{"Aknn", "aknn", db.Op_SyncLevelPropose, true}',
        ],
    }

    for path, tokens in expected_tokens_by_path.items():
        text = current_text([path])
        for token in tokens:
            if token not in text:
                failures.append(f"{path}: missing {token!r}")

    for path, tokens in forbidden_tokens_by_path.items():
        text = current_text([path])
        for token in tokens:
            if token in text:
                failures.append(f"{path}: stale public sync-level token {token!r}")

    return [
        CheckResult(
            "public sync-level naming: query/full_index without stale full_text/embeddings/aknn aliases",
            not failures,
            "ok" if not failures else "; ".join(failures[:30]),
        )
    ]


def check_known_merge_risk_wiring() -> list[CheckResult]:
    """Pin explicit invariants for issues found during this merge review.

    These checks are intentionally more concrete than the broad surface diffs
    above. They protect against repeat regressions in the exact areas we had to
    hand-merge: public exact sort, structured filters, and scan-row identity.
    """

    failures: list[str] = []

    types_text = current_text(["zig/pkg/antfly/src/storage/db/types.zig"])
    search_fields = struct_fields(types_text, "SearchRequest")
    if "order_by" not in search_fields:
        failures.append("SearchRequest.order_by missing")
    if "filter_query_json" not in search_fields:
        failures.append("SearchRequest.filter_query_json missing")

    required_tokens_by_path = {
        "zig/pkg/antfly/src/query/contract.zig": [
            "request.order_by",
            "cloneSortFieldsWithStableTiebreaker",
            "cloneDefaultIdSortField",
            "filter_query_json",
            "test \"api query contract maps timeout_ms to execution deadline\"",
        ],
        "zig/pkg/antfly/src/storage/db/query/search_exec.zig": [
            "effectiveSortRequestAlloc",
            "filter_query_json_unresolved",
            "requestWithoutResolvedStoredFilters",
        ],
        "zig/pkg/antfly/src/storage/db/search_runtime.zig": [
            "fn freeConstDocIdsAlloc",
            "db_query_graph.executeSinglePatternQueryWithSets",
            "composedQueryMetricIndexName(req)",
            "observeSearchFailureMetric(metric_name, .search, platform_time.monotonicNs() -| start_ns)",
            "db_query_metrics.observeSortProfile(metric_name, .search, platform_time.monotonicNs() -| start_ns, result.sort_profile)",
            "execution_req.resolved_text_doc_filter = &resolved_text_filter_storage",
            "resolveStructuredTextDocNumFilterForComposedAlloc(alloc, execution_req",
        ],
        "zig/pkg/antfly/src/storage/db/enrichment/enrichment_runtime.zig": [
            "RuntimeDocumentExtractionPreviousState",
            "loadRuntimeDocumentExtractionPreviousState(runtime, request.doc_key, artifact_name, state)",
            "loadRuntimeDocumentExtractionPreviousState(runtime, doc_key, artifact_name, state)",
            "scanRuntimeDocumentExtractionPreviousStateFromStore",
            "recovered_from_store_scan = true",
        ],
        "zig/pkg/antfly/src/storage/db/write_path.zig": [
            "const PendingArtifactWriteIndex",
            "fn appendFullTextDeleteDocument",
            "fn appendInlineFullTextDocument",
            "fn fullTextTargetRefsAlloc",
            "fn requestMatchesForcedGeneratedArtifact",
            "const embedding_name = requestEmbeddingName(request)",
            "if (!requestMatchesForcedGeneratedArtifact(request, force_generated_artifact_names)) continue",
            "fn storedOrPendingEmbeddingSourceHash",
            "fn computeDenseMaterializedChunkRequestImpl",
            "fn computeSparseMaterializedChunkRequest",
            "fn scanMaterializedChunkSourceStoreBatch",
            "skip_unchanged_artifacts",
            "pending_chunk_keys",
            "source_indexes",
        ],
        "zig/pkg/antfly/src/storage/db/internal.zig": [
            "snapshot_read_txn: ?*docstore_mod.DocStore.Txn = null",
            "repair_sequence: u64 = 0",
            "target_advance_repair_last_ns: std.StringHashMapUnmanaged(u64) = .empty",
            "repair_options: types.ArtifactRepairRunOptions = .{}",
        ],
        "zig/pkg/antfly/src/storage/db/artifact_replay.zig": [
            "const GraphArtifactRefView",
            "pub const OwnedEmbeddingArtifactWriteIdentity",
            "pub fn decodeEmbeddingArtifactWriteIdentityAlloc",
            "test \"artifact replay dense embedding write identity accepts legacy embedding artifact keys\"",
            "pub fn graphArtifactSourceConsumesArtifactKey",
            "fn decodeArtifactRefViewForGraphApplicability",
            "fn graphAssetSourceConsumesAssetRefView",
            "producer_cfg.type != .document_extraction",
            "pub const GraphMutationCollectionOptions = struct",
            "repair: GraphReplayRepairOptions = .{}",
            "pub const GraphMaterializationOptions = struct",
        ],
        "zig/pkg/antfly/src/storage/db/artifact_repair.zig": [
            "fn scanStoreForRebuildContext",
            "fn applySplitGraphArtifactsForIndexStreamingContext",
            "pub fn recordEmbeddingArtifactRepairIssueForReplay",
            "pub fn repairEmbeddingArtifactIssues",
        ],
        "zig/pkg/antfly/src/storage/db/derived_async.zig": [
            "pub fn appendGeneratedBatchFromEnrichment",
            "fn appliedSequenceUpdatesWithConfigHashes",
            "fn checkpointManagedProjectionEffectsForAppliedSequenceUpdates",
            "fn denseEmbeddingArtifactRepairReason",
            "fn sparseEmbeddingArtifactRepairReason",
            "fn filterAndRecordDenseEmbeddingArtifactRepairIssuesForReplay",
            "fn filterAndRecordSparseEmbeddingArtifactRepairIssuesForReplay",
            "fn storeHasReplayRecordForHintAfter",
            "fn managedIndexRecordApplicability",
            "fn replayRangeHasManagedIndexApplicableRecord",
            "fn shouldDeferBacklogPressureForExternalDenseBulk",
            "fn saveDenseProjectionMetadataForAppliedSequenceUpdates",
            "pub fn shouldRunTargetAdvanceRepair",
            "pub fn noteTargetAdvanceRepairRun",
            "artifact_replay.graphArtifactSourceConsumesArtifactKey(index_manager, source, artifact_key)",
            "artifact_replay.decodeEmbeddingArtifactWriteIdentityAlloc(state.ctx.alloc, key, state.expected_name)",
            "fn deleteDerivedCoverageForDocKeys",
            "deleteDerivedCoverageForDocKeys(ctx.alloc, ctx.store, ctx.index_manager, index_ref.name, batch.deleted_keys)",
            "deleteDerivedCoverageForDocKeys(ctx.alloc, ctx.store, ctx.index_manager, index_ref.name, batch.overwritten_doc_keys)",
            "test \"db derived async deletes coverage outcome markers for replay deletes once\"",
            "test \"db derived async graph replay ignores document extraction parent asset but tracks units\"",
            "derivedAsyncBatchAffectsManagedIndexForReplay(parent_batch, graph_ref)",
            "derivedAsyncBatchAffectsManagedIndexForReplay(unit_batch, graph_ref)",
        ],
        "zig/pkg/antfly/src/storage/db/split_restore.zig": [
            "pub fn shouldAppendSplitDelta",
            "pub fn applySplitGraphArtifactsInRange",
        ],
        "zig/pkg/antfly/src/api/table_writes/cache.zig": [
            "pub const RuntimeStatusSnapshotMode = enum",
            "pub fn publishRuntimeStatusSnapshotConsistent",
            "pub fn tryPublishRuntimeStatusSnapshotConsistent",
            "pub fn publishRuntimeStatusSnapshotWithStartupPhaseMode",
            "pub fn publishStartupCatchUpRuntimeStatusSnapshot",
            "fn cachedBestEffortStartupPlaceholderSource",
            "fn markClearedStartupRuntimeStatus",
            "fn startupRuntimeStatusFreshness",
            "fn publishRuntimeStatusGroupAfterObservation",
            "const publication_fence = try snapshot_cache.capturePublicationToken(table_name)",
            "test \"runtime status best effort preserves startup placeholder freshness transitions\"",
            "fn entryActiveLeasesLocked",
            "fn tableHasOnlyInactiveAdoptableEntriesLocked",
            "fn reserveLifecycleRetireCapacityLocked",
            "self.entryActiveLeasesLocked(entry) > 1",
            "self.entryActiveLeasesLocked(entry) != 0 and !entry.allow_active_generation_adoption",
        ],
        "zig/pkg/antfly/src/api/table_writes/backup_restore.zig": [
            "const restore_trash_dir_name = \".antfly-restore-trash\"",
            "fn moveRestorePathToTrashIfPresent",
            "try moveRestorePathToTrashIfPresent(alloc, io, path, indexes_path, \"indexes\")",
            "test \"prepare restore moves existing indexes to restore trash\"",
        ],
        "zig/pkg/antfly/src/api/table_writes/managed_db.zig": [
            "pub const ManagedDbEnrichmentSet = struct",
            "pub fn createManagedDbEnrichments",
            "fn managedIndexEmbeddingArtifactName",
            "fn repairManagedEmbeddingArtifactsForIndex",
            "fn markManagedIndexRepairRequired",
            "db.reprocessGeneratedEnrichmentFromStoredDocs(alloc, managedIndexEmbeddingArtifactName(db, index_name))",
            "try db.core.index_manager.syncAll(true)",
        ],
        "zig/pkg/antfly/src/api/table_writes/sources.zig": [
            "fn preemptStartupWriteCacheForLocalMutation",
            "fn publishManagedDbTerminalLoadFailureStatus",
            "test \"managed startup catch-up marks FileNotFound index open terminal degraded\"",
            "try std.testing.expect(result.terminal_degraded)",
            "try std.testing.expectEqualStrings(\"FileNotFound\", status.stats.indexes[0].load_error.?)",
            "fn publishRuntimeStatusSnapshotWithStartupPhaseMode",
            "fn preserveNonReplayBackfill",
            "const createManagedDbEnrichments = table_write_managed_db.createManagedDbEnrichments",
            "self.preemptStartupWriteCacheForLocalMutation(group_id, table_name)",
            "test \"write cache local mutation preempts stale startup writer\"",
            "test \"write cache metadata refresh preserves inactive adoptable seed\"",
        ],
        "zig/pkg/antfly/src/api/table_writes/managed_db.zig": [
            "try db.reconfigureEnrichmentRuntime(enrichments.takeConfig())",
        ],
        "zig/pkg/antfly/src/api/table_reads.zig": [
            "pub const appendScanLine = table_read_core.appendScanLine;",
        ],
        "zig/pkg/antfly/src/api/table_reads/sources.zig": [
            "fn testTableReadSortedHitAlloc",
            "test \"distributed query shard request preserves sorted cursor contract\"",
            "test \"table read distributed sorted merge uses catalog runtime schema and rejects incomplete shard windows\"",
            "try std.testing.expectEqualStrings(\"distributed_k_way_merge\", sort_profile.plan)",
            "try std.testing.expectError(error.UnsupportedQueryRequest, mergeSearchResultsWithTableRuntimeSchema",
            "const RemoteDocumentArtifactManifest = struct",
            "fn parseRemoteDocumentArtifactManifest",
            "fn parseRemoteDocumentArtifactManifests",
            "test \"remote document artifact manifest parser owns storage manifest fields\"",
            "test \"remote document artifact manifest list parser owns artifact array\"",
        ],
        "zig/pkg/antfly/src/metadata/catalog/table_ddl.zig": [
            "fn generatedFieldCapabilitiesAlloc",
            "const GeneratedFieldCapability = struct",
            "fn generatedFieldCapabilityAlloc",
            "fn runtimeFieldCapabilitiesJsonValueAlloc",
            "metadataAntflyType(capability.field_type)",
            "observed_dynamic_field_capability_sets: []table_reads.ObservedDynamicFieldCapabilitySet = &.{}",
        ],
        "zig/pkg/antfly/src/api/table_reads/cache.zig": [
            "table_epochs: std.StringHashMapUnmanaged(u64) = .empty",
            "fn epochForTableLocked",
            "const pending_open_wait_timeout_ns: u64 = 5 * std.time.ns_per_s",
            "return error.TableReadChurn",
            "test \"provisioned read cache clear preserves in-flight pending opens and bumps table epoch\"",
        ],
        "zig/pkg/antfly/src/api/distributed_candidate_source.zig": [
            "object.get(\"_id\")",
            "table_reads.appendScanLine",
        ],
        "zig/pkg/antfly/src/api/http_server.zig": [
            "test \"api http invalid query with sort diagnostic returns exact sort response\"",
            "test \"api http plain public query preserves outer absolute request deadline\"",
            "test \"api http retry sleep is bounded by request deadline\"",
            "test \"api http transient read retry honors expired request deadline before source query\"",
            "test \"api http unsupported sorted query response surfaces exact sort diagnostics\"",
            "test \"api http server forwards cluster backup mutations to metadata leader\"",
            "test \"api http server returns retryable not leader for local public metadata mutation\"",
            "test \"api http server returns retryable not leader when metadata proposal is dropped\"",
            "test \"api http server returns retryable not leader through public table adapter mutation\"",
            "test \"api http server returns retryable not leader through public cluster adapter mutation\"",
            "test \"api http server authenticates trusted principal\"",
        ],
        "zig/pkg/antfly/src/metadata/http_client.zig": [
            "test \"metadata http client retries explicit metadata not leader response\"",
        ],
        "zig/pkg/antfly/src/metadata/runtime.zig": [
            "test \"metadata runtime preserves trusted principal auth material bytes\"",
        ],
        "zig/pkg/antfly/src/metadata/server.zig": [
            "test \"metadata admin mux maps admin not leader through metadata executor\"",
        ],
        "zig/lib/image/src/bmp.zig": [
            "pub fn decodeRgbaLimited",
            "test \"decode manifest-backed bmp success fixtures\"",
            "test \"decode manifest-backed bmp unsupported fixtures return typed unsupported error\"",
            "test \"decode manifest-backed bmp invalid fixtures return typed decode error\"",
        ],
        "zig/lib/image/src/webp.zig": [
            "pub fn decodeRgbaLimited",
            "test \"decode minimal vp8l literal image to rgba\"",
            "test \"decode explicitly rejects animated webp\"",
            "test \"decode limited rejects oversized webp before full decode\"",
        ],
        "zig/lib/image/src/limits.zig": [
            "pub const DecodeLimits = struct",
            "test \"decode limits reject oversized dimensions\"",
        ],
        "zig/lib/image/src/mod.zig": [
            "pub const bmp = @import(\"bmp.zig\");",
            "pub const webp = @import(\"webp.zig\");",
            "pub const DecodeLimits = limits.DecodeLimits;",
        ],
        "specs/openapi/antfly/metadata.yaml": [
            "order_by:",
            "filter_query_json_unresolved",
            "full_index",
        ],
        "openapi.yaml": [
            "order_by:",
            "filter_query_json_unresolved",
            "full_index",
        ],
        "py/packages/sdk/src/antfly/client_generated/models/sync_level.py": [
            'FULL_INDEX = "full_index"',
        ],
        "ts/packages/sdk/src/public-api.d.ts": [
            'SyncLevel: "propose" | "write" | "query" | "enrichments" | "full_index"',
            "order_by?: components[\"schemas\"][\"SortField\"][]",
        ],
        "go/pkg/sdk/requests.go": [
            "OrderBy []oapi.SortField `json:\"order_by,omitempty\"`",
            "SyncLevel SyncLevel `json:\"sync_level,omitempty\"`",
        ],
        "go/pkg/sdk/oapi/client.gen.go": [
            "type QueryHitsTotal struct",
            "type SortProfile struct",
            "type TableRepairJob struct",
            "type RelationalIndexRepairStatus struct",
            "SyncLevelFullIndex",
        ],
        "py/packages/sdk/src/antfly/client_generated/models/query_hits_total.py": [
            "class QueryHitsTotal",
        ],
        "py/packages/sdk/src/antfly/client_generated/models/table_repair_job.py": [
            "class TableRepairJob",
        ],
        "py/packages/sdk/src/antfly/client_generated/models/relational_index_repair_status.py": [
            "class RelationalIndexRepairStatus",
        ],
        "ts/packages/sdk/src/types.ts": [
            "export type QueryHitsTotal",
            "export type SortProfile",
        ],
    }
    forbidden_tokens_by_path = {
        "zig/pkg/antfly/src/api/table_writes/sources.zig": [
            "read_cache.epoch",
        ],
    }

    for path, tokens in required_tokens_by_path.items():
        try:
            text = current_text([path])
        except (OSError, UnicodeDecodeError):
            failures.append(f"{path}: unreadable")
            continue
        for token in tokens:
            if token not in text:
                failures.append(f"{path}: missing {token!r}")

    for path, tokens in forbidden_tokens_by_path.items():
        text = current_text([path])
        for token in tokens:
            if token in text:
                failures.append(f"{path}: stale token {token!r}")

    return [
        CheckResult(
            "known merge-risk wiring: order_by/filter/_id scan/full_index/generated repair+totals/graph replay",
            not failures,
            "ok" if not failures else "; ".join(failures[:30]),
        )
    ]


def extract_zig_function_body(text: str, fn_name: str) -> str | None:
    match = re.search(rf"\b(?:pub\s+)?fn\s+{re.escape(fn_name)}\b[^\{{]*\{{", text)
    if not match:
        return None
    start = match.end() - 1
    depth = 0
    for index in range(start, len(text)):
        char = text[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start : index + 1]
    return None


def check_route_matcher_shadowing() -> list[CheckResult]:
    text = current_text(["zig/pkg/antfly/src/api/http_server.zig"])
    body = extract_zig_function_body(text, "requiredPermissionForRequest")
    if body is None:
        return [CheckResult("permission route matcher shadowing", False, "requiredPermissionForRequest not found")]

    seen: dict[str, int] = {}
    duplicates: list[str] = []
    for matcher in re.findall(r"\broutes\.Routes\.(match[A-Za-z0-9_]+)\s*\(", body):
        seen[matcher] = seen.get(matcher, 0) + 1
        if seen[matcher] == 2:
            duplicates.append(matcher)

    return [
        CheckResult(
            "permission route matcher shadowing",
            not duplicates,
            "ok" if not duplicates else "duplicate matchers in requiredPermissionForRequest: " + ", ".join(duplicates),
        )
    ]


def check_removed_file_reference_hygiene() -> list[CheckResult]:
    failures: list[str] = []
    if (ROOT / "zig/pkg/antfly/src/api/tables.zig").exists():
        failures.append("zig/pkg/antfly/src/api/tables.zig still exists")

    stale_patterns = [
        "zig/pkg/antfly/src/api/tables.zig",
        "src/api/tables.zig",
        "api/tables.zig",
        '@import("tables.zig")',
    ]
    hits: list[str] = []
    for path in product_text_files():
        rel = path.relative_to(ROOT).as_posix()
        try:
            text = path.read_text(errors="replace")
        except OSError:
            continue
        for pattern in stale_patterns:
            if pattern in text:
                hits.append(f"{rel}: {pattern}")
                break
    if hits:
        failures.extend(hits[:20])

    return [
        CheckResult(
            "removed tables.zig reference hygiene",
            not failures,
            "ok" if not failures else "; ".join(failures),
        )
    ]


def check_generated_rename_hygiene() -> list[CheckResult]:
    stale_tokens = [
        "inference_text_chunk_options",
        "InferenceTextChunkOptions",
        "inference_vad_options",
        "InferenceVADOptions",
    ]
    hits: list[str] = []
    for path in product_text_files():
        rel = path.relative_to(ROOT).as_posix()
        try:
            text = path.read_text(errors="replace")
        except OSError:
            continue
        for token in stale_tokens:
            if token in text:
                hits.append(f"{rel}: {token}")
                break
    return [
        CheckResult(
            "generated rename hygiene",
            not hits,
            "ok" if not hits else "; ".join(hits[:20]),
        )
    ]


def check_sdk_contract_hygiene() -> list[CheckResult]:
    failures: list[str] = []
    required_tokens_by_path = {
        "go/pkg/sdk/oapi/client.gen.go": [
            "SyncLevelFullIndex   SyncLevel = \"full_index\"",
            "SyncLevelQuery       SyncLevel = \"query\"",
            "type ExactSortError struct",
            "type QueryHitsTotal struct",
            "RowsQueryRequestTotalModeExact",
            "type SortProfile struct",
            "RepairTargetArtifact RepairTarget = \"artifact\"",
            "type TableRepairJob struct",
            "type RelationalIndexRepairStatus struct",
            "type TextChunkOptions struct",
            "type VADOptions struct",
            "StartTableRepairJob(ctx context.Context",
            "GetTableRepairJob(ctx context.Context",
            "AdvanceTableRepairJob(ctx context.Context",
            "CancelTableRepairJob(ctx context.Context",
        ],
        "go/pkg/sdk/types.go": [
            "TextChunkOptions    = oapi.TextChunkOptions",
            "QueryHitsTotal         = oapi.QueryHitsTotal",
            "QueryHitsTotalRelation = oapi.QueryHitsTotalRelation",
            "SyncLevelFullIndex   = oapi.SyncLevelFullIndex",
            "SyncLevelQuery       = oapi.SyncLevelQuery",
        ],
        "py/packages/sdk/src/antfly/client_generated/models/__init__.py": [
            "from .exact_sort_error import ExactSortError",
            "from .query_hits_total import QueryHitsTotal",
            "from .rows_query_request_total_mode import RowsQueryRequestTotalMode",
            "from .sort_profile import SortProfile",
            "from .repair_target import RepairTarget",
            "from .table_repair_job import TableRepairJob",
            "from .relational_index_repair_status import RelationalIndexRepairStatus",
            "from .text_chunk_options import TextChunkOptions",
            "from .vad_options import VADOptions",
        ],
        "py/packages/sdk/src/antfly/client_generated/models/sync_level.py": [
            'FULL_INDEX = "full_index"',
            'QUERY = "query"',
        ],
        "py/packages/sdk/src/antfly/client_generated/models/rows_query_request.py": [
            "order_by:",
            "total_mode:",
            '"order_by"',
            '"total_mode"',
        ],
        "py/packages/sdk/src/antfly/client_generated/api/data_operations/start_table_repair_job.py": [
            "TableRepairJobStartRequest",
            "TableRepairJob.from_dict",
        ],
        "py/packages/sdk/src/antfly/client_generated/api/data_operations/advance_table_repair_job.py": [
            "TableRepairJob.from_dict",
            "response_202",
        ],
        "py/packages/sdk/src/antfly/client_generated/api/index_management/repair_relational_column_backed_index.py": [
            "RelationalColumnBackedIndexRepairRequest",
            "RelationalColumnBackedIndexRepairResponse",
        ],
        "ts/packages/sdk/src/public-api.d.ts": [
            'SyncLevel: "propose" | "write" | "query" | "enrichments" | "full_index"',
            "ExactSortError:",
            "QueryHitsTotal:",
            "SortProfile:",
            "RowsQueryRequest:",
            "total_mode?: \"exact\" | \"bounded\" | \"none\"",
            "RepairTarget: \"artifact\" | \"index\"",
            "TableRepairJob:",
            "RelationalIndexRepairStatus:",
            "TextChunkOptions:",
            "VADOptions:",
            "startTableRepairJob:",
            "advanceTableRepairJob:",
            "cancelTableRepairJob:",
        ],
        "ts/packages/sdk/src/types.ts": [
            "export type QueryHitsTotal",
            "export function formatQueryHitsTotal",
            "export type SortProfile",
        ],
        "ts/packages/sdk/src/index.ts": [
            "QueryHitsTotal",
            "SortProfile",
            "formatQueryHitsTotal",
        ],
        "specs/openapi/antfly/metadata.yaml": [
            "SyncLevel:",
            "- full_index",
            "order_by:",
            "total_mode:",
            "QueryHitsTotal:",
            "SortProfile:",
            "ExactSortError:",
            "TableRepairJob:",
            "RepairTarget:",
        ],
        "specs/openapi/antfly/indexes.yaml": [
            "RelationalIndexRepairStatus:",
        ],
        "specs/openapi/inference/api.yaml": [
            "TextChunkOptions:",
            "VADOptions:",
        ],
        "specs/openapi/inference/config.yaml": [
            "VADOptions:",
        ],
        "specs/openapi/shared/chunking.yaml": [
            "TextChunkOptions:",
            "VADOptions:",
        ],
    }
    forbidden_tokens_by_path = {
        "go/pkg/sdk/oapi/client.gen.go": [
            "SyncLevelAknn",
            "SyncLevelFullText",
        ],
        "py/packages/sdk/src/antfly/client_generated/models/sync_level.py": [
            'AKNN = "aknn"',
            'FULL_TEXT = "full_text"',
        ],
        "ts/packages/sdk/src/public-api.d.ts": [
            '"aknn"',
            '"full_text" | "enrichments"',
        ],
        "specs/openapi/antfly/metadata.yaml": [
            '"aknn"',
        ],
    }

    for path, tokens in required_tokens_by_path.items():
        text = current_text([path])
        for token in tokens:
            if token not in text:
                failures.append(f"{path}: missing {token!r}")

    for path, tokens in forbidden_tokens_by_path.items():
        text = current_text([path])
        for token in tokens:
            if token in text:
                failures.append(f"{path}: stale generated contract token {token!r}")

    return [
        CheckResult(
            "SDK/spec contract hygiene: sync-level/order_by/totals/sort/repair/inference rename",
            not failures,
            "ok" if not failures else "; ".join(failures[:30]),
        )
    ]


def check_generated_api_surface_matrix() -> list[CheckResult]:
    failures: list[str] = []

    surface_tokens_by_path = {
        "specs/openapi/antfly/metadata.yaml": [
            "QueryHitsTotal:",
            "SortProfile:",
            "total_mode:",
            "operationId: startTableRepairJob",
            "operationId: getTableRepairJob",
            "operationId: advanceTableRepairJob",
            "operationId: cancelTableRepairJob",
            "operationId: runTableRepair",
            "operationId: listTableRepairIssues",
            "operationId: repairRelationalColumnBackedIndex",
            "operationId: repairNamespaceTableRelationalColumnBackedIndex",
        ],
        "openapi.yaml": [
            "QueryHitsTotal:",
            "SortProfile:",
            "total_mode:",
            "operationId: startTableRepairJob",
            "operationId: getTableRepairJob",
            "operationId: advanceTableRepairJob",
            "operationId: cancelTableRepairJob",
            "operationId: runTableRepair",
            "operationId: listTableRepairIssues",
            "operationId: repairRelationalColumnBackedIndex",
            "operationId: repairNamespaceTableRelationalColumnBackedIndex",
        ],
        "zig/pkg/antfly/src/openapi/generated/antfly_metadata_openapi/types.zig": [
            "pub const QueryHitsTotal",
            "pub const RowsQueryRequestTotalMode",
            "pub const SortProfile",
        ],
        "zig/pkg/antfly/src/openapi/generated/antfly_metadata_openapi/server.zig": [
            "startTableRepairJob",
            "getTableRepairJob",
            "advanceTableRepairJob",
            "cancelTableRepairJob",
            "runTableRepair",
            "listTableRepairIssues",
            "repairRelationalColumnBackedIndex",
            "repairNamespaceTableRelationalColumnBackedIndex",
        ],
        "zig/pkg/antfly/src/openapi/generated/antfly_client_openapi/types.zig": [
            "pub const QueryHitsTotal",
            "pub const RowsQueryRequestTotalMode",
            "pub const SortProfile",
        ],
        "zig/pkg/antfly/src/openapi/generated/antfly_client_openapi/client.zig": [
            "pub fn startTableRepairJob",
            "pub fn getTableRepairJob",
            "pub fn advanceTableRepairJob",
            "pub fn cancelTableRepairJob",
            "pub fn runTableRepair",
            "pub fn listTableRepairIssues",
            "pub fn repairRelationalColumnBackedIndex",
            "pub fn repairNamespaceTableRelationalColumnBackedIndex",
        ],
        "go/pkg/antfly/src/metadata/api.gen.go": [
            "type QueryHitsTotal struct",
            "type RowsQueryRequestTotalMode string",
            "type SortProfile struct",
            "StartTableRepairJob(w http.ResponseWriter, r *http.Request",
            "GetTableRepairJob(w http.ResponseWriter, r *http.Request",
            "AdvanceTableRepairJob(w http.ResponseWriter, r *http.Request",
            "CancelTableRepairJob(w http.ResponseWriter, r *http.Request",
            "RunTableRepair(w http.ResponseWriter, r *http.Request",
            "ListTableRepairIssues(w http.ResponseWriter, r *http.Request",
            "RepairRelationalColumnBackedIndex(w http.ResponseWriter, r *http.Request",
            "RepairNamespaceTableRelationalColumnBackedIndex(w http.ResponseWriter, r *http.Request",
        ],
        "go/pkg/sdk/oapi/client.gen.go": [
            "type QueryHitsTotal struct",
            "type RowsQueryRequestTotalMode string",
            "type SortProfile struct",
            "StartTableRepairJob(ctx context.Context",
            "GetTableRepairJob(ctx context.Context",
            "AdvanceTableRepairJob(ctx context.Context",
            "CancelTableRepairJob(ctx context.Context",
            "RunTableRepair(ctx context.Context",
            "ListTableRepairIssues(ctx context.Context",
            "RepairRelationalColumnBackedIndex(ctx context.Context",
            "RepairNamespaceTableRelationalColumnBackedIndex(ctx context.Context",
        ],
        "go/pkg/sdk/types.go": [
            "QueryHitsTotal         = oapi.QueryHitsTotal",
            "QueryHitsTotalRelation = oapi.QueryHitsTotalRelation",
            "QueryHitsTotalValue",
        ],
        "py/packages/sdk/src/antfly/client_generated/models/__init__.py": [
            "from .query_hits_total import QueryHitsTotal",
            "from .rows_query_request_total_mode import RowsQueryRequestTotalMode",
            "from .sort_profile import SortProfile",
        ],
        "py/packages/sdk/src/antfly/client_generated/models/query_hits_total.py": [
            "class QueryHitsTotal",
        ],
        "py/packages/sdk/src/antfly/client_generated/models/rows_query_request_total_mode.py": [
            "class RowsQueryRequestTotalMode",
        ],
        "py/packages/sdk/src/antfly/client_generated/models/sort_profile.py": [
            "class SortProfile",
        ],
        "py/packages/sdk/src/antfly/client_generated/api/data_operations/start_table_repair_job.py": [
            "def sync_detailed",
            "TableRepairJob.from_dict",
        ],
        "py/packages/sdk/src/antfly/client_generated/api/data_operations/get_table_repair_job.py": [
            "def sync_detailed",
            "TableRepairJob.from_dict",
        ],
        "py/packages/sdk/src/antfly/client_generated/api/data_operations/advance_table_repair_job.py": [
            "def sync_detailed",
            "TableRepairJob.from_dict",
        ],
        "py/packages/sdk/src/antfly/client_generated/api/data_operations/cancel_table_repair_job.py": [
            "def sync_detailed",
            "TableRepairJob.from_dict",
        ],
        "py/packages/sdk/src/antfly/client_generated/api/data_operations/run_table_repair.py": [
            "def sync_detailed",
            "TableRepairRunResponse.from_dict",
        ],
        "py/packages/sdk/src/antfly/client_generated/api/data_operations/list_table_repair_issues.py": [
            "def sync_detailed",
            "TableRepairIssueList.from_dict",
        ],
        "py/packages/sdk/src/antfly/client_generated/api/index_management/repair_relational_column_backed_index.py": [
            "def sync_detailed",
            "RelationalColumnBackedIndexRepairResponse.from_dict",
        ],
        "ts/packages/sdk/src/public-api.d.ts": [
            "QueryHitsTotal:",
            "SortProfile:",
            "total_mode?: \"exact\" | \"bounded\" | \"none\"",
            "startTableRepairJob:",
            "getTableRepairJob:",
            "advanceTableRepairJob:",
            "cancelTableRepairJob:",
            "runTableRepair:",
            "listTableRepairIssues:",
            "repairRelationalColumnBackedIndex:",
            "repairNamespaceTableRelationalColumnBackedIndex:",
        ],
        "ts/packages/sdk/src/types.ts": [
            "export type QueryHitsTotal",
            "export function formatQueryHitsTotal",
            "export type SortProfile",
        ],
    }

    for path, tokens in surface_tokens_by_path.items():
        text = current_text([path])
        if not text:
            failures.append(f"{path}: missing or empty")
            continue
        for token in tokens:
            if token not in text:
                failures.append(f"{path}: missing {token!r}")

    return [
        CheckResult(
            "generated API surface matrix: repair/totals/sort across spec and SDKs",
            not failures,
            "ok" if not failures else "; ".join(failures[:30]),
        )
    ]


def exported_sdk_names(text: str, path: str) -> set[str]:
    suffix = ".d.ts" if path.endswith(".d.ts") else pathlib.Path(path).suffix
    patterns = {
        ".go": [
            r"(?m)^type\s+([A-Z][A-Za-z0-9_]*)\b",
            r"(?m)^func\s+([A-Z][A-Za-z0-9_]*)\s*\(",
            r"(?ms)^const\s+\((.*?)^\)",
            r"(?m)^const\s+([A-Z][A-Za-z0-9_]*)\b",
        ],
        ".py": [
            r"(?m)^class\s+([A-Z][A-Za-z0-9_]*)\b",
            r"(?m)^def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(",
        ],
        ".ts": [
            r"(?m)^export\s+(?:declare\s+)?(?:type|interface|class|function|const|enum)\s+([A-Za-z_][A-Za-z0-9_]*)\b",
        ],
        ".d.ts": [
            r"(?m)^\s*(?:export\s+)?(?:declare\s+)?(?:type|interface|class|function|const|enum)\s+([A-Za-z_][A-Za-z0-9_]*)\b",
        ],
    }
    names: set[str] = set()
    for pattern in patterns.get(suffix, []):
        for match in re.finditer(pattern, text):
            value = match.group(1)
            if suffix == ".go" and "\n" in value:
                names.update(re.findall(r"(?m)^\s*([A-Z][A-Za-z0-9_]*)\b", value))
            else:
                names.add(value)
    return names


def check_sdk_export_surface(origin: str) -> list[CheckResult]:
    base = merge_base(origin)
    sdk_prefixes = (
        "go/pkg/sdk/",
        "py/packages/sdk/src/antfly/client_generated/",
        "ts/packages/sdk/src/",
    )
    sdk_suffixes = (".go", ".py", ".ts", ".d.ts")
    paths = [
        path
        for path in git_lines(["diff", "--name-only", f"{base}..{origin}"])
        if path.startswith(sdk_prefixes) and path.endswith(sdk_suffixes)
    ]
    intentional_missing = {
        ("go/pkg/sdk/oapi/client.gen.go", "SyncLevelFullText"),
        ("go/pkg/sdk/oapi/client.gen.go", "SyncLevelEmbeddings"),
        ("go/pkg/sdk/types.go", "SyncLevelFullText"),
        ("go/pkg/sdk/types.go", "SyncLevelEmbeddings"),
    }
    missing: list[str] = []
    checked = 0
    for path in paths:
        try:
            incoming = origin_text(origin, path)
        except RuntimeError:
            continue
        current_path = ROOT / path
        current = current_path.read_text(errors="replace") if current_path.exists() else ""
        for name in sorted(exported_sdk_names(incoming, path)):
            checked += 1
            if (path, name) in intentional_missing:
                continue
            if name not in current:
                missing.append(f"{path}: {name}")

    return [
        CheckResult(
            "SDK exported surface from incoming main",
            not missing,
            f"{len(paths)} files, {checked} incoming exports"
            if not missing
            else "; ".join(missing[:30]),
        )
    ]


def check_generated_protobuf_drift() -> list[CheckResult]:
    staged_files = git_lines(["diff", "--cached", "--name-only"])
    staged_pb = [path for path in staged_files if path.endswith(".pb.go")]
    staged_proto = {path for path in staged_files if path.endswith(".proto")}

    suspicious: list[str] = []
    for path in staged_pb:
        expected_proto = f"{path[:-len('.pb.go')]}.proto"
        if expected_proto not in staged_proto:
            suspicious.append(path)

    return [
        CheckResult(
            "generated protobuf drift without proto changes",
            not suspicious,
            "ok" if not suspicious else "; ".join(suspicious[:20]),
        )
    ]


def check_go_duplicate_declarations(files: list[ChangedFile]) -> list[CheckResult]:
    changed_go = [
        ROOT / item.path
        for item in files
        if item.path.endswith(".go") and (ROOT / item.path).exists()
    ]
    duplicates: list[str] = []
    for path in changed_go:
        rel = path.relative_to(ROOT).as_posix()
        try:
            text = path.read_text(errors="replace")
        except OSError:
            continue
        seen: dict[str, int] = {}
        for receiver, name in re.findall(r"(?m)^func\s+(?:\(([^)]*)\)\s*)?([A-Za-z_][A-Za-z0-9_]*)\s*\(", text):
            if name == "init" and not receiver:
                continue
            receiver_type = ""
            if receiver:
                pieces = receiver.replace("*", " ").split()
                receiver_type = pieces[-1] if pieces else receiver.strip()
            key = f"{receiver_type}.{name}" if receiver_type else name
            seen[key] = seen.get(key, 0) + 1
            if seen[key] == 2:
                duplicates.append(f"{rel}: {key}")
    return [
        CheckResult(
            "changed Go duplicate declarations",
            not duplicates,
            "ok" if not duplicates else "; ".join(duplicates[:30]),
        )
    ]


def check_openapi_operation_hygiene() -> list[CheckResult]:
    failures: list[str] = []
    spec_files = sorted((ROOT / "specs/openapi").rglob("*.yaml"))
    spec_files.append(ROOT / "openapi.yaml")
    for path in spec_files:
        if not path.exists():
            continue
        rel = path.relative_to(ROOT).as_posix()
        text = path.read_text(errors="replace")
        seen: dict[str, int] = {}
        duplicates: list[str] = []
        for operation_id in re.findall(r"(?m)^\s*operationId:\s*([A-Za-z0-9_]+)\s*$", text):
            seen[operation_id] = seen.get(operation_id, 0) + 1
            if seen[operation_id] == 2:
                duplicates.append(operation_id)
        if duplicates:
            failures.append(f"{rel}: duplicate operationId {', '.join(duplicates)}")

    required_operation_ids = [
        "repairNamespaceTableRelationalColumnBackedIndex",
        "getNamespaceTableRelationalIndexRepairJob",
        "repairRelationalColumnBackedIndex",
        "getRelationalIndexRepairJob",
        "listTableRepairIssues",
        "runTableRepair",
        "startTableRepairJob",
        "getTableRepairJob",
        "advanceTableRepairJob",
        "cancelTableRepairJob",
    ]
    for path_name in ("specs/openapi/antfly/metadata.yaml", "openapi.yaml"):
        text = (ROOT / path_name).read_text(errors="replace")
        for operation_id in required_operation_ids:
            if f"operationId: {operation_id}" not in text:
                failures.append(f"{path_name}: missing operationId {operation_id}")

    required_paths = {
        "specs/openapi/antfly/metadata.yaml": [
            "/tables/{tableName}/repair/issues:",
            "/tables/{tableName}/repair/run:",
            "/tables/{tableName}/repair/jobs:",
            "/tables/{tableName}/repair/jobs/{jobId}:",
            "/tables/{tableName}/repair/jobs/{jobId}/advance:",
            "/tables/{tableName}/repair/jobs/{jobId}/cancel:",
            "/tables/{tableName}/relational-column-backed-index-repair:",
        ],
        "openapi.yaml": [
            "/db/v1/tables/{tableName}/repair/issues:",
            "/db/v1/tables/{tableName}/repair/run:",
            "/db/v1/tables/{tableName}/repair/jobs:",
            "/db/v1/tables/{tableName}/repair/jobs/{jobId}:",
            "/db/v1/tables/{tableName}/repair/jobs/{jobId}/advance:",
            "/db/v1/tables/{tableName}/repair/jobs/{jobId}/cancel:",
            "/db/v1/tables/{tableName}/relational-column-backed-index-repair:",
        ],
    }
    for path_name, tokens in required_paths.items():
        text = (ROOT / path_name).read_text(errors="replace")
        for token in tokens:
            if token not in text:
                failures.append(f"{path_name}: missing path token {token}")

    generated_go = current_text(["go/pkg/antfly/src/metadata/api.gen.go"])
    generated_zig = current_text(["zig/pkg/antfly/src/openapi/generated/antfly_metadata_openapi/server.zig"])
    for operation_id in required_operation_ids:
        go_name = operation_id[:1].upper() + operation_id[1:]
        if go_name not in generated_go:
            failures.append(f"go metadata generated server missing {go_name}")
        if operation_id not in generated_zig:
            failures.append(f"zig metadata generated server missing {operation_id}")

    return [
        CheckResult(
            "OpenAPI operation and repair route hygiene",
            not failures,
            "ok" if not failures else "; ".join(failures[:30]),
        )
    ]


def check_table_write_repair_ledger_hygiene() -> list[CheckResult]:
    failures: list[str] = []
    core_text = current_text(["zig/pkg/antfly/src/api/table_writes/core.zig"])
    sources_text = current_text(["zig/pkg/antfly/src/api/table_writes/sources.zig"])

    free_body = extract_zig_function_body(core_text, "freeRelationalIndexRepairJobRecord")
    if free_body is None:
        failures.append("freeRelationalIndexRepairJobRecord missing")
    else:
        for token in [
            "record.access_method",
            "record.index_name",
            "record.cursor",
            "record.failure_reason",
            "record.last_error",
        ]:
            if token not in free_body:
                failures.append(f"freeRelationalIndexRepairJobRecord missing {token}")

    clone_body = extract_zig_function_body(sources_text, "cloneRelationalIndexRepairJobRecord")
    if clone_body is None:
        failures.append("cloneRelationalIndexRepairJobRecord missing")
    else:
        for token in [
            ".access_method = \"\"",
            ".index_name = \"\"",
            ".generation = record.generation",
            ".cursor = \"\"",
            ".failure_reason = null",
            ".stale_generation = record.stale_generation",
            ".pass_count = record.pass_count",
            ".last_units_queued = record.last_units_queued",
            ".total_units_completed = record.total_units_completed",
            "cloned.access_method = try alloc.dupe(u8, record.access_method)",
            "cloned.index_name = try alloc.dupe(u8, record.index_name)",
            "cloned.cursor = try alloc.dupe(u8, record.cursor)",
            "cloned.failure_reason = if (record.failure_reason)",
        ]:
            if token not in clone_body:
                failures.append(f"cloneRelationalIndexRepairJobRecord missing {token}")

    for token in [
        ".relational_index_repair_job_begin_catalog = relationalIndexRepairJobBeginCatalog",
        ".relational_index_repair_job_record_pass_catalog = relationalIndexRepairJobRecordPassCatalog",
        ".relational_index_repair_job_load_catalog = relationalIndexRepairJobLoadCatalog",
        ".relational_index_repair_job_list_catalog = relationalIndexRepairJobListCatalog",
        ".request_table_structural_reconcile = requestTableStructuralReconcile",
        ".request_table_index_structural_reconcile = requestTableIndexStructuralReconcile",
        "fn relationalIndexRepairJobBeginCatalog(",
        "fn relationalIndexRepairJobRecordPassCatalog(",
        "fn relationalIndexRepairJobLoadCatalog(",
        "fn relationalIndexRepairJobListCatalog(",
        "fn reconcileTableStructureLocal(",
        "test \"hosted relational index repair job ledger exposes public pass progress\"",
    ]:
        if token not in sources_text:
            failures.append(f"table_writes/sources.zig missing {token}")
    for token in [
        ".request_table_structural_reconcile = requestTableStructuralReconcile",
        ".request_table_index_structural_reconcile = requestTableIndexStructuralReconcile",
    ]:
        if sources_text.count(token) < 2:
            failures.append(f"table_writes/sources.zig should wire provisioned and hosted {token}")

    return [
        CheckResult(
            "table_writes repair ledger hygiene",
            not failures,
            "ok" if not failures else "; ".join(failures[:30]),
        )
    ]


def check_table_reads_lsm_storage_stats_hygiene() -> list[CheckResult]:
    failures: list[str] = []
    core_text = current_text(["zig/pkg/antfly/src/api/table_reads/core.zig"])
    sources_text = current_text(["zig/pkg/antfly/src/api/table_reads/sources.zig"])

    if "lsm_storage_stats: ?*const fn (\n            ptr: *anyopaque,\n            table_name: []const u8," not in core_text:
        failures.append("core lsm_storage_stats vtable signature should be ptr/table_name only")
    if "pub fn lsmStorageStats(\n        self: TableReadSource,\n        table_name: []const u8," not in core_text:
        failures.append("core TableReadSource.lsmStorageStats wrapper should be self/table_name only")
    if sources_text.count(".lsm_storage_stats = lsmStorageStats") < 2:
        failures.append("sources should wire bound and provisioned lsmStorageStats")
    if "fn lsmStorageStats(\n        ptr: *anyopaque,\n        alloc: std.mem.Allocator," in sources_text:
        failures.append("sources lsmStorageStats implementation still has removed allocator parameter")

    return [
        CheckResult(
            "table_reads lsm_storage_stats hygiene",
            not failures,
            "ok" if not failures else "; ".join(failures),
        )
    ]


def check_table_writes_document_artifact_hook_hygiene() -> list[CheckResult]:
    failures: list[str] = []
    core_text = current_text(["zig/pkg/antfly/src/api/table_writes/core.zig"])
    sources_text = current_text(["zig/pkg/antfly/src/api/table_writes/sources.zig"])

    full_document_artifact_hooks = {
        "reprocess_document_artifact",
        "reprocess_document_artifact_range",
        "reprocess_document_artifact_group_local",
        "reprocess_document_artifact_range_group_local",
        "update_document_artifact_child_range_placement",
        "update_document_artifact_child_range_placement_group_local",
        "apply_document_artifact_child_range_batch",
        "apply_document_artifact_child_range_batch_group_local",
    }
    bound_document_artifact_hooks = {
        "reprocess_document_artifact",
        "reprocess_document_artifact_range",
        "update_document_artifact_child_range_placement",
        "update_document_artifact_child_range_placement_group_local",
        "apply_document_artifact_child_range_batch",
        "apply_document_artifact_child_range_batch_group_local",
    }

    core_vtable_fields = set(re.findall(r"\n\s*([a-z][A-Za-z0-9_]*)\s*:\s*\?\*const\s+fn\s*\(", core_text))
    missing_core = sorted(full_document_artifact_hooks - core_vtable_fields)
    if missing_core:
        failures.append("core vtable missing " + ", ".join(missing_core))

    for source_name in ("ProvisionedTableWriteSource", "HostedProvisionedTableWriteSource"):
        bindings = source_vtable_bindings(sources_text, source_name)
        missing = sorted(full_document_artifact_hooks - bindings)
        if missing:
            failures.append(f"{source_name} source vtable missing " + ", ".join(missing))

    bound_bindings = source_vtable_bindings(sources_text, "BoundTableWriteSource")
    missing_bound = sorted(bound_document_artifact_hooks - bound_bindings)
    if missing_bound:
        failures.append("BoundTableWriteSource source vtable missing " + ", ".join(missing_bound))

    for token in [
        "fetchGroupDocumentArtifactReprocess",
        "fetchGroupDocumentArtifactRangeReprocess",
        "fetchGroupDocumentArtifactChildRangePlacementUpdate",
        "fetchGroupDocumentArtifactChildRangeBatchApply",
        "parseDocumentArtifactTableReprocessResultAlloc",
        "encodeRemoteDocumentArtifactChildRangeApplyBatch",
        "applyDocumentArtifactChildRangeBatchGroupLocal",
    ]:
        if token not in sources_text:
            failures.append(f"table_writes/sources.zig missing {token}")

    return [
        CheckResult(
            "table_writes document artifact hook hygiene",
            not failures,
            "ok" if not failures else "; ".join(failures[:30]),
        )
    ]


def check_db_pending_work_stats_hygiene() -> list[CheckResult]:
    failures: list[str] = []
    required_tokens_by_path = {
        "zig/pkg/antfly/src/storage/db/core.zig": [
            "repair_metadata_rebuild_pending: bool = false",
        ],
        "zig/pkg/antfly/src/storage/db/db.zig": [
            "pub fn artifactRepairMetadataRebuildPending",
            "artifact_repair_impl.artifactRepairMetadataRebuildPending",
        ],
        "zig/pkg/antfly/src/storage/db/lifecycle.zig": [
            ".repair_metadata_rebuild_pending = self.artifactRepairMetadataRebuildPending()",
        ],
        "zig/pkg/antfly/src/storage/db/artifact_repair.zig": [
            "pub fn artifactRepairMetadataRebuildPending",
            "test \"db artifact repair metadata maintenance drains summary rebuild without restart\"",
            "pendingWorkStats().repair_metadata_rebuild_pending",
        ],
    }
    for path, tokens in required_tokens_by_path.items():
        text = current_text([path])
        for token in tokens:
            if token not in text:
                failures.append(f"{path}: missing {token!r}")

    return [
        CheckResult(
            "db pending work stats hygiene",
            not failures,
            "ok" if not failures else "; ".join(failures),
        )
    ]


def check_db_restore_runtime_repair_drain_hygiene() -> list[CheckResult]:
    path = "zig/pkg/antfly/src/storage/db/split_restore.zig"
    text = current_text([path])
    failures: list[str] = []
    required_tokens = [
        "restore runtime repair drain async work",
        "drain_replay_stages_until_stable_without_truncation",
        "flush_applied_sequences_for_idle",
        'updateRestoreRuntimeRepairPhaseWithIo(self, alloc, io, "rebuild_replayed_artifacts", false)',
    ]
    for token in required_tokens:
        if token not in text:
            failures.append(f"{path}: missing {token!r}")

    drain_idx = text.find('"drain_async"')
    sync_idx = text.find('"sync_indexes"', drain_idx + 1) if drain_idx != -1 else -1
    if drain_idx == -1 or sync_idx == -1:
        failures.append(f"{path}: could not isolate restore drain_async phase")
    else:
        drain_block = text[drain_idx:sync_idx]
        if "runUntilIdle()" in drain_block:
            failures.append(f"{path}: restore drain_async phase calls runUntilIdle()")

    return [
        CheckResult(
            "db restore runtime repair drain hygiene",
            not failures,
            "ok" if not failures else "; ".join(failures),
        )
    ]


def check_db_terminal_load_failure_stats_hygiene() -> list[CheckResult]:
    path = "zig/pkg/antfly/src/storage/db/lifecycle.zig"
    text = current_text([path])
    failures: list[str] = []
    required_tokens = [
        "fn applyTerminalLoadFailureStatus",
        "item.replay_catch_up_required = false",
        "if (item.load_error != null) applyTerminalLoadFailureStatus(&item)",
        "if (item.load_error != null) applyTerminalLoadFailureStatus(item)",
        "try std.testing.expect(!item.replay_catch_up_required)",
        "try std.testing.expect(!item.catch_up_active)",
    ]
    for token in required_tokens:
        if token not in text:
            failures.append(f"{path}: missing {token!r}")

    return [
        CheckResult(
            "db terminal load failure stats hygiene",
            not failures,
            "ok" if not failures else "; ".join(failures),
        )
    ]


def check_generated_read_validation_hygiene() -> list[CheckResult]:
    failures: list[str] = []
    lowering_text = current_text(["zig/pkg/antfly/src/sql/lowering_context.zig"])
    document_text = current_text(["zig/pkg/antfly/src/sql/document_plan.zig"])

    required_tokens = [
        "pub fn validateGeneratedReadAstRanges",
        "fn validateGeneratedReadTokenRangeAllowEmpty",
        "try validateGeneratedReadTokenRangeAllowEmpty(tokens, read_ast, window.definition_tokens);",
    ]
    for token in required_tokens:
        if token not in lowering_text:
            failures.append(f"lowering_context.zig missing {token!r}")

    forbidden_tokens_by_path = {
        "zig/pkg/antfly/src/sql/lowering_context.zig": [
            "window.definition_tokens.start == window.definition_tokens.end",
        ],
        "zig/pkg/antfly/src/sql/document_plan.zig": [
            "documentProducerHasAnyScalarIndexCapability",
        ],
    }
    for path, tokens in forbidden_tokens_by_path.items():
        text = lowering_text if path.endswith("lowering_context.zig") else document_text
        for token in tokens:
            if token in text:
                failures.append(f"{path}: stale generated-read validation token {token!r}")

    for token in [
        'if (object.get("match_none") != null) return true;',
        'if (object.get("match_all") != null) return true;',
        "generated_read_validate.validateGeneratedReadAstPayloads",
        "lowering_context.validateGeneratedReadAstRanges",
    ]:
        if token not in document_text:
            failures.append(f"document_plan.zig missing {token!r}")

    return [
        CheckResult(
            "generated-read validation hygiene",
            not failures,
            "ok" if not failures else "; ".join(failures[:30]),
        )
    ]


def check_sql_row_policy_grammar_hygiene() -> list[CheckResult]:
    failures: list[str] = []
    grammar = current_text(["zig/pkg/antfly/src/sql/grammar/antfly_sql.y"])
    generated_root = current_text(["zig/pkg/antfly/src/sql/grammar/generated/root.zig"])
    generated_parser = current_text(["zig/pkg/antfly/src/sql/generated_parser.zig"])
    fingerprint = current_text(["zig/pkg/antfly/src/sql/fingerprint.zig"])

    required_grammar_tokens = [
        "policy_mode_opt policy_command_opt policy_to_opt policy_using_opt policy_with_check_opt",
        "policy_mode_opt:",
        "| AS identifier_name",
        "policy_command_opt:",
        "| FOR policy_command",
        "policy_command:",
        "| SELECT",
        "| DELETE",
    ]
    for token in required_grammar_tokens:
        if token not in grammar:
            failures.append(f"grammar missing row-policy token {token!r}")

    for token in ("policy_mode_opt", "policy_command_opt", "policy_command"):
        if token not in generated_root:
            failures.append(f"generated grammar root missing {token}")

    required_parser_tokens = [
        "fn generatedStatementAllowsLexicalGrammarFallback",
        ".ddl => |ddl| ddl == .create_policy",
        "if (findTopLevelKeyword(tokens, operation.start, operation.end, .as))",
        "if (findTopLevelKeyword(tokens, operation.start, operation.end, .@\"for\"))",
        "if (findTopLevelKeyword(tokens, operation.start, operation.end, .to))",
        "tokens[to_index].matchesKeywordTag(.to)",
        "tokens[role_end].matchesKeywordTag(.using)",
        "tokens[role_end].matchesKeywordTag(.with)",
    ]
    for token in required_parser_tokens:
        if token not in generated_parser:
            failures.append(f"generated parser missing row-policy token {token!r}")

    stale_parser_tokens = [
        'tokens[to_index].matchesKeyword("to")',
        'tokens[role_end].matchesKeyword("using")',
        'tokens[role_end].matchesKeyword("with")',
    ]
    for token in stale_parser_tokens:
        if token in generated_parser:
            failures.append(f"generated parser has stale row-policy raw keyword check {token!r}")

    required_fixture_tokens = [
        "CREATE POLICY usage_records_select_policy ON usage_records AS RESTRICTIVE FOR SELECT TO app_reader, app_writer",
        "WITH CHECK (status = 'active')",
        "mode=restrictive:command=select:roles=2:role=app_reader:role=app_writer",
        "check=kind=literal_eq:field=status",
    ]
    for token in required_fixture_tokens:
        if token not in fingerprint:
            failures.append(f"fingerprint fixture missing row-policy coverage {token!r}")

    return [
        CheckResult(
            "SQL row-policy grammar/parser hygiene",
            not failures,
            "ok" if not failures else "; ".join(failures[:30]),
        )
    ]


def check_sql_native_requirement_fixture_hygiene() -> list[CheckResult]:
    failures: list[str] = []
    diagnostics = current_text(["zig/pkg/antfly/src/sql/diagnostics.zig"])
    required_path = ROOT / "zig/pkg/antfly/src/sql/fixtures/sql_api_required_native_requirements.json"
    resolved_path = ROOT / "zig/pkg/antfly/src/sql/fixtures/sql_api_resolved_native_requirements.json"
    coverage_path = ROOT / "zig/pkg/antfly/src/sql/fixtures/sql_api_required_coverage.json"

    enum_match = re.search(r"pub const SqlAdapterClassificationReason = enum \{(.*?)\n\};", diagnostics, re.S)
    false_match = re.search(
        r"pub fn classificationReasonIsUnsupportedRequirement.*?return switch \(reason\) \{(.*?)=> false,",
        diagnostics,
        re.S,
    )
    if not enum_match or not false_match:
        return [CheckResult("SQL native requirement fixture hygiene", False, "unable to parse diagnostics requirement policy")]

    enum_names = {
        item.strip().strip(",")
        for item in enum_match.group(1).splitlines()
        if item.strip() and not item.strip().startswith("//")
    }
    non_requirements = set(re.findall(r"\.([A-Za-z_][A-Za-z0-9_]*)", false_match.group(1)))
    expected_requirements = enum_names - non_requirements

    try:
        required_json = json.loads(required_path.read_text())
        resolved_json = json.loads(resolved_path.read_text())
        coverage_json = json.loads(coverage_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        return [CheckResult("SQL native requirement fixture hygiene", False, f"unable to parse fixture json: {exc}")]

    required = required_json.get("required", [])
    resolved_items = resolved_json.get("resolved", [])
    required_coverage = set(coverage_json.get("required", []))
    resolved = [item.get("reason", "") for item in resolved_items if isinstance(item, dict)]
    required_set = set(required)
    resolved_set = set(resolved)
    fixture_union = required_set | resolved_set

    if required != sorted(required):
        failures.append("required native requirements are not sorted")
    if resolved != sorted(resolved):
        failures.append("resolved native requirements are not sorted by reason")
    if len(required) != len(required_set):
        failures.append("required native requirements contain duplicates")
    if len(resolved) != len(resolved_set):
        failures.append("resolved native requirements contain duplicates")

    overlap = sorted(required_set & resolved_set)
    missing = sorted(expected_requirements - fixture_union)
    extra = sorted(fixture_union - expected_requirements)
    if overlap:
        failures.append("requirements present in both unresolved/resolved: " + ", ".join(overlap[:30]))
    if missing:
        failures.append("requirements missing from fixture partition: " + ", ".join(missing[:30]))
    if extra:
        failures.append("unknown requirements in fixture partition: " + ", ".join(extra[:30]))

    for expected_resolved in ("bulk_io_plan", "foreign_data_catalog_plan"):
        if expected_resolved not in resolved_set:
            failures.append(f"{expected_resolved} should remain resolved native coverage")

    for item in resolved_items:
        if not isinstance(item, dict):
            failures.append("resolved native requirement entry is not an object")
            continue
        reason = item.get("reason", "")
        coverage = item.get("coverage", [])
        if reason not in expected_requirements:
            failures.append(f"resolved native requirement has unknown reason {reason!r}")
        if not isinstance(coverage, list) or not coverage:
            failures.append(f"resolved native requirement {reason!r} has empty/non-list coverage")
            continue
        unknown_coverage = sorted(set(coverage) - required_coverage)
        if unknown_coverage:
            failures.append(f"{reason}: coverage buckets not in required coverage: {', '.join(unknown_coverage[:20])}")

    detail = "ok"
    if failures:
        detail = "; ".join(failures[:40])
    else:
        detail = f"{len(required)} unresolved + {len(resolved)} resolved = {len(fixture_union)} stable requirements"
    return [CheckResult("SQL native requirement fixture hygiene", not failures, detail)]


def check_ci_build_harness_hygiene() -> list[CheckResult]:
    failures: list[str] = []

    required_tokens_by_path = {
        ".github/workflows/zig-tests.yml": [
            "zig-base:",
            "name: zig-base",
            "zig-full:",
            "name: zig-full",
            "submodules: true",
            "run: make zig-generated-check",
            "ANTFLY_TLA_MODEL_CHECK:",
            "ANTFLY_TLA_TRACE_VALIDATE:",
            "run: scripts/ci/zig-tla-verify.sh",
        ],
        "scripts/ci/zig-tla-verify.sh": [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            'run_model_check="${ANTFLY_TLA_MODEL_CHECK:-false}"',
            'run_trace_validate="${ANTFLY_TLA_TRACE_VALIDATE:-false}"',
            "make -C zig tla-check",
            "ANTFLY_TRACE_FILE=/tmp/raft-trace.ndjson zig build -Dwith_tla=true raft-test",
            "ANTFLY_TRACE_FILE=/tmp/txn-trace.ndjson zig build -Dwith_tla=true lib-db-txn-test",
            "make -C zig tla-trace-raft TRACE_FILES=/tmp/raft-trace.ndjson",
            "make -C zig tla-trace-txn TRACE_FILES=/tmp/txn-trace.ndjson",
        ],
        "zig/Makefile": [
            "GIT ?= git",
            "OPENAPI_CODEGEN_CACHE_KEY",
            "OPENAPI_CODEGEN_CACHE_DIR",
            "$(OPENAPI_ZIG_BUILD) regen-openapi",
            "$(SCRIPTS_PY) ../scripts/join_public_openapi.py --compare openapi.yaml",
            "generated-check: openapi-check snowball-check cabi-header-check sql-api-parity-fixture-check",
        ],
        "Makefile": [
            "./go/pkg/termite",
            "zig-generated-check:",
            "$(ZIG_MAKE) generated-check",
        ],
        "zig/pkg/antfly/build/tests.zig": [
            "pub var selected_test_filter: ?[]const u8 = null;",
            '.name = "db-artifact-repair-test"',
            "fn addSelectedRunTestFilter",
            "build_test_filters.select",
            'run.addArgs(&.{ "--test-filter", filter });',
        ],
    }

    for path, tokens in required_tokens_by_path.items():
        text = current_text([path])
        for token in tokens:
            if token not in text:
                failures.append(f"{path}: missing {token!r}")

    mode_lines = git_lines(["ls-files", "--stage", "scripts/ci/zig-tla-verify.sh"])
    if not mode_lines:
        failures.append("scripts/ci/zig-tla-verify.sh: not tracked")
    elif not mode_lines[0].startswith("100755 "):
        failures.append("scripts/ci/zig-tla-verify.sh: must be executable for direct workflow invocation")

    build_tests_text = current_text(["zig/pkg/antfly/build/tests.zig"])
    api_filters_start = build_tests_text.find("pub const APITestFilters = struct {")
    api_filters_end = build_tests_text.find("\npub const RootTestFilters = struct {", api_filters_start)
    if api_filters_start == -1 or api_filters_end == -1:
        failures.append("zig/pkg/antfly/build/tests.zig: unable to locate APITestFilters block")
    else:
        declared_tests = all_current_zig_test_names()
        api_filters_text = build_tests_text[api_filters_start:api_filters_end]
        unmatched_filters = [
            test_filter
            for test_filter in re.findall(r'"([^"]+)"', api_filters_text)
            if not any(test_filter in declared for declared in declared_tests)
        ]
        if unmatched_filters:
            failures.append(
                "zig/pkg/antfly/build/tests.zig: APITestFilters entries match no declared Zig tests: "
                + ", ".join(repr(item) for item in unmatched_filters[:20])
            )

    return [
        CheckResult(
            "ci/build harness hygiene",
            not failures,
            "ok" if not failures else "; ".join(failures[:40]),
        )
    ]


def check_concrete_source_vtable_bindings(origin: str) -> list[CheckResult]:
    source_sets = [
        (
            "table_reads BoundTableReadSource vtable bindings",
            "zig/pkg/antfly/src/api/table_reads.zig",
            ["zig/pkg/antfly/src/api/table_reads.zig", "zig/pkg/antfly/src/api/table_reads"],
            "BoundTableReadSource",
        ),
        (
            "table_reads ProvisionedTableReadSource vtable bindings",
            "zig/pkg/antfly/src/api/table_reads.zig",
            ["zig/pkg/antfly/src/api/table_reads.zig", "zig/pkg/antfly/src/api/table_reads"],
            "ProvisionedTableReadSource",
        ),
        (
            "table_reads HostedProvisionedTableReadSource vtable bindings",
            "zig/pkg/antfly/src/api/table_reads.zig",
            ["zig/pkg/antfly/src/api/table_reads.zig", "zig/pkg/antfly/src/api/table_reads"],
            "HostedProvisionedTableReadSource",
        ),
        (
            "table_writes ProvisionedTableWriteSource vtable bindings",
            "zig/pkg/antfly/src/api/table_writes.zig",
            ["zig/pkg/antfly/src/api/table_writes.zig", "zig/pkg/antfly/src/api/table_writes"],
            "ProvisionedTableWriteSource",
        ),
        (
            "table_writes HostedProvisionedTableWriteSource vtable bindings",
            "zig/pkg/antfly/src/api/table_writes.zig",
            ["zig/pkg/antfly/src/api/table_writes.zig", "zig/pkg/antfly/src/api/table_writes"],
            "HostedProvisionedTableWriteSource",
        ),
        (
            "table_writes BoundTableWriteSource vtable bindings",
            "zig/pkg/antfly/src/api/table_writes.zig",
            ["zig/pkg/antfly/src/api/table_writes.zig", "zig/pkg/antfly/src/api/table_writes"],
            "BoundTableWriteSource",
        ),
    ]
    checks: list[CheckResult] = []
    for name, origin_path, current_paths, source_name in source_sets:
        if not file_exists_at_origin(origin, origin_path):
            continue
        checks.append(compare_named_sets(
            name,
            source_vtable_bindings(origin_text(origin, origin_path), source_name),
            source_vtable_bindings(current_text(current_paths), source_name),
        ))
    return checks


def check_changed_files(origin: str, files: list[ChangedFile]) -> list[CheckResult]:
    markers = [item.path for item in files if text_file_has_conflict_markers(item.path)]
    unstaged = [item.path for item in files if "unstaged" in item.source]
    missing_incoming = [
        item.path
        for item in files
        if "incoming" in item.source and file_exists_at_origin(origin, item.path) and not current_path_present_for_origin_path(item.path)
    ]
    generated = [item.path for item in files if path_category(item.path) == "zig-generated-openapi"]
    specs = [item.path for item in files if path_category(item.path) == "spec"]
    return [
        CheckResult(
            "changed-file inventory",
            True,
            "; ".join(changed_file_inventory(files)[:12]),
        ),
        CheckResult(
            "conflict markers across all changed tracked files",
            not markers,
            "ok" if not markers else ", ".join(markers[:40]),
        ),
        CheckResult(
            "incoming files present in current tree",
            not missing_incoming,
            "ok" if not missing_incoming else ", ".join(missing_incoming[:40]),
        ),
        CheckResult(
            "unstaged tracked merge files",
            not unstaged,
            "ok" if not unstaged else ", ".join(unstaged[:40]),
        ),
        CheckResult(
            "generated/spec merge note",
            True,
            f"{len(specs)} spec files and {len(generated)} generated OpenAPI files changed; prefer merging specs then running make generate",
        ),
        CheckResult(
            "changed test inventory",
            True,
            "; ".join(changed_test_inventory(files)),
        ),
    ]


def check_same_path_public_surface(origin: str, files: list[ChangedFile]) -> list[CheckResult]:
    missing_pub_fn: dict[str, list[str]] = {}
    missing_pub_const: dict[str, list[str]] = {}
    for item in files:
        path = item.path
        if not path.endswith(".zig"):
            continue
        if path in {
            "zig/pkg/antfly/src/api/table_catalog.zig",
            "zig/pkg/antfly/src/api/table_reads.zig",
            "zig/pkg/antfly/src/api/table_writes.zig",
            "zig/pkg/antfly/src/api/query_contract.zig",
            "zig/pkg/antfly/src/storage/db/db.zig",
        }:
            continue
        if not file_exists_at_origin(origin, path):
            continue
        try:
            old = origin_text(origin, path)
            new = current_text_for_origin_path(path)
        except (RuntimeError, OSError, UnicodeDecodeError):
            continue
        allow_missing = DB_PUB_FN_FALSE_POSITIVES if path == "zig/pkg/antfly/src/storage/db/db.zig" else set()
        new_public_functions = pub_fns(new)
        fn_missing = []
        for name in sorted(
            pub_fns(old)
            - allow_missing
            - SAME_PATH_FUNCTION_FALSE_POSITIVES.get(path, set())
        ):
            aliases = SAME_PATH_FUNCTION_ALIASES.get(path, {}).get(name, set())
            if name in new_public_functions or any(
                current_symbol_alias_exists(alias, new, "fn") for alias in aliases
            ):
                continue
            fn_missing.append(name)
        new_members = pub_members(new)
        const_missing_items: list[str] = []
        for name in sorted(pub_consts(old) - SAME_PATH_CONST_FALSE_POSITIVES.get(path, set())):
            aliases = SAME_PATH_CONST_ALIASES.get(path, {}).get(name, set())
            if name in new_members or any(
                current_symbol_alias_exists(alias, new, "const") for alias in aliases
            ):
                continue
            const_missing_items.append(name)
        const_missing = const_missing_items
        if fn_missing:
            missing_pub_fn[path] = fn_missing
        if const_missing:
            missing_pub_const[path] = const_missing

    fn_detail = "ok"
    if missing_pub_fn:
        parts = []
        for path, values in list(missing_pub_fn.items())[:20]:
            parts.append(f"{path}: {', '.join(values[:20])}")
        fn_detail = "; ".join(parts)
        if len(missing_pub_fn) > 20:
            fn_detail += f"; ... +{len(missing_pub_fn) - 20} files"

    const_detail = "ok"
    if missing_pub_const:
        parts = []
        for path, values in list(missing_pub_const.items())[:20]:
            parts.append(f"{path}: {', '.join(values[:20])}")
        const_detail = "; ".join(parts)
        if len(missing_pub_const) > 20:
            const_detail += f"; ... +{len(missing_pub_const) - 20} files"

    return [
        CheckResult(
            "same-path changed Zig public functions from incoming",
            not missing_pub_fn,
            fn_detail,
        ),
        CheckResult(
            "same-path changed Zig public consts from incoming",
            not missing_pub_const,
            const_detail,
        ),
    ]


def check_same_path_zig_tests(origin: str, files: list[ChangedFile], global_current_tests: set[str]) -> list[CheckResult]:
    missing_tests: dict[str, list[str]] = {}
    skip_paths = {
        "zig/pkg/antfly/src/api/table_reads.zig",
        "zig/pkg/antfly/src/api/table_writes.zig",
        "zig/pkg/antfly/src/storage/db/db.zig",
    }
    for item in files:
        path = item.path
        if not path.endswith(".zig") or path in skip_paths:
            continue
        if not file_exists_at_origin(origin, path):
            continue
        try:
            old_tests = zig_test_names(origin_text(origin, path))
            new_tests = zig_test_names(current_text_for_origin_path(path))
        except (RuntimeError, OSError, UnicodeDecodeError):
            continue
        missing = sorted(normalized_test_names(old_tests) - normalized_test_names(new_tests | global_current_tests))
        if missing:
            missing_tests[path] = missing
    return [compare_missing_by_file("same-path changed Zig test names from incoming", missing_tests)]


def check_non_zig_test_names(origin: str, files: list[ChangedFile]) -> list[CheckResult]:
    incoming_paths = {
        path
        for path in git_lines(["diff", "--name-only", f"{merge_base(origin)}..{origin}"])
        if path.endswith(("_test.go", ".py", ".test.ts", ".test.tsx", ".spec.ts", ".spec.tsx"))
    }
    current_global_by_kind: dict[str, set[str]] = {
        "go": set(),
        "python": set(),
        "typescript": set(),
    }
    for item in files:
        path = item.path
        try:
            text = git_index_or_worktree_text(path)
        except (RuntimeError, OSError, UnicodeDecodeError):
            continue
        if path.endswith("_test.go"):
            current_global_by_kind["go"] |= non_zig_test_names(path, text)
        elif path.endswith(".py"):
            current_global_by_kind["python"] |= non_zig_test_names(path, text)
        elif path.endswith((".test.ts", ".test.tsx", ".spec.ts", ".spec.tsx")):
            current_global_by_kind["typescript"] |= non_zig_test_names(path, text)

    missing: dict[str, list[str]] = {}
    checked = 0
    incoming_name_count = 0
    for path in sorted(incoming_paths):
        if path.endswith(".zig"):
            continue
        if not file_exists_at_origin(origin, path):
            continue
        old_names = non_zig_test_names(path, origin_text(origin, path))
        if not old_names:
            continue
        checked += 1
        incoming_name_count += len(old_names)
        try:
            current_names = non_zig_test_names(path, git_index_or_worktree_text(path))
        except (RuntimeError, OSError, UnicodeDecodeError):
            current_names = set()
        if path.endswith("_test.go"):
            kind = "go"
        elif path.endswith(".py"):
            kind = "python"
        else:
            kind = "typescript"
        path_missing = sorted(old_names - current_names - current_global_by_kind[kind])
        if path_missing:
            missing[path] = path_missing

    detail = f"{checked} files, {incoming_name_count} incoming test names"
    if missing:
        detail += "; " + "; ".join(f"{path}: {', '.join(values[:20])}" for path, values in list(missing.items())[:20])
        if len(missing) > 20:
            detail += f"; ... +{len(missing) - 20} files"
    return [CheckResult("non-Zig changed test names from incoming", not missing, detail)]


def non_generated_source_path(path: str) -> bool:
    if path.endswith((".gen.go", ".pb.go", ".d.ts")):
        return False
    skipped_parts = (
        "/client_generated/",
        "/generated/",
        "/antfarm/assets/",
        "/node_modules/",
    )
    return not any(part in path for part in skipped_parts)


def non_zig_source_symbol_names(path: str, text: str) -> set[str]:
    names: set[str] = set()
    if path.endswith(".go"):
        for receiver, name in re.findall(r"(?m)^func\s+(?:\(([^)]*)\)\s*)?([A-Za-z_][A-Za-z0-9_]*)\s*\(", text):
            if name == "init" and not receiver:
                continue
            names.add(name)
    elif path.endswith(".py"):
        names |= set(re.findall(r"(?m)^\s*(?:async\s+)?def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", text))
        names |= set(re.findall(r"(?m)^class\s+([A-Za-z_][A-Za-z0-9_]*)\b", text))
    elif path.endswith((".ts", ".tsx")):
        names |= set(re.findall(r"(?m)^export\s+(?:async\s+)?function\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", text))
        names |= set(re.findall(r"(?m)^export\s+(?:class|interface|type|enum)\s+([A-Za-z_][A-Za-z0-9_]*)\b", text))
        names |= set(re.findall(r"(?m)^export\s+const\s+([A-Za-z_][A-Za-z0-9_]*)\s*=", text))
        names |= set(re.findall(r"(?m)^function\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", text))
        names |= set(re.findall(r"(?m)^const\s+([A-Z][A-Za-z0-9_]*)\s*=", text))
    return names


def non_zig_source_kind(path: str) -> str | None:
    if path.endswith(".go"):
        return "go"
    if path.endswith(".py"):
        return "python"
    if path.endswith((".ts", ".tsx")):
        return "typescript"
    return None


def check_non_zig_source_symbols(origin: str, files: list[ChangedFile]) -> list[CheckResult]:
    incoming_paths = [
        path
        for path in git_lines(["diff", "--name-only", f"{merge_base(origin)}..{origin}"])
        if non_zig_source_kind(path) != None and non_generated_source_path(path)
    ]
    current_global_by_kind: dict[str, set[str]] = {
        "go": set(),
        "python": set(),
        "typescript": set(),
    }
    for item in files:
        path = item.path
        kind = non_zig_source_kind(path)
        if kind == None or not non_generated_source_path(path):
            continue
        try:
            text = git_index_or_worktree_text(path)
        except (RuntimeError, OSError, UnicodeDecodeError):
            continue
        current_global_by_kind[kind] |= non_zig_source_symbol_names(path, text)

    missing: dict[str, list[str]] = {}
    checked_files = 0
    checked_symbols = 0
    for path in incoming_paths:
        kind = non_zig_source_kind(path)
        if kind == None or not file_exists_at_origin(origin, path):
            continue
        incoming_names = non_zig_source_symbol_names(path, origin_text(origin, path))
        if not incoming_names:
            continue
        checked_files += 1
        checked_symbols += len(incoming_names)
        try:
            current_names = non_zig_source_symbol_names(path, git_index_or_worktree_text(path))
        except (RuntimeError, OSError, UnicodeDecodeError):
            current_names = set()
        missing_here = sorted(incoming_names - current_names - current_global_by_kind[kind])
        if missing_here:
            missing[path] = missing_here

    detail = f"{checked_files} files, {checked_symbols} incoming source symbols"
    if missing:
        detail += "; " + "; ".join(
            f"{path}: {', '.join(values[:20])}"
            for path, values in list(missing.items())[:20]
        )
        if len(missing) > 20:
            detail += f"; ... +{len(missing) - 20} files"
    return [CheckResult("non-Zig changed source symbols from incoming", not missing, detail)]


def check_split_zig_tests(origin: str, global_current_tests: set[str]) -> list[CheckResult]:
    checks: list[CheckResult] = []
    checks.append(compare_named_sets(
        "table_reads split Zig test names",
        normalized_test_names(zig_test_names(origin_text(origin, "zig/pkg/antfly/src/api/table_reads.zig"))),
        normalized_test_names(zig_test_names(current_text([
            "zig/pkg/antfly/src/api/table_reads.zig",
            "zig/pkg/antfly/src/api/table_reads",
        ])) | global_current_tests),
    ))
    checks.append(compare_named_sets(
        "table_writes split Zig test names",
        normalized_test_names(zig_test_names(origin_text(origin, "zig/pkg/antfly/src/api/table_writes.zig"))),
        normalized_test_names(zig_test_names(current_text([
            "zig/pkg/antfly/src/api/table_writes.zig",
            "zig/pkg/antfly/src/api/table_writes",
        ])) | global_current_tests),
    ))
    checks.append(compare_named_sets(
        "db split Zig test names",
        normalized_test_names(zig_test_names(origin_text(origin, "zig/pkg/antfly/src/storage/db/db.zig"))),
        normalized_test_names(zig_test_names(current_text(["zig/pkg/antfly/src/storage/db"])) | global_current_tests),
    ))
    return checks


def mask_zig_comments_and_strings(text: str, *, mask_strings: bool = True) -> str:
    """Replace Zig comments and optionally literals with spaces, preserving lines."""

    chars = list(text)
    index = 0
    block_depth = 0
    quote: str | None = None
    escaped = False
    line_has_code = False

    while index < len(chars):
        char = chars[index]
        following = chars[index + 1] if index + 1 < len(chars) else ""

        if char == "\n":
            escaped = False
            line_has_code = False
            index += 1
            continue

        if block_depth:
            if char == "/" and following == "*":
                chars[index] = chars[index + 1] = " "
                block_depth += 1
                index += 2
            elif char == "*" and following == "/":
                chars[index] = chars[index + 1] = " "
                block_depth -= 1
                index += 2
            else:
                chars[index] = " "
                index += 1
            continue

        if quote:
            if mask_strings:
                chars[index] = " "
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            index += 1
            continue

        if not line_has_code and char in " \t\r":
            index += 1
            continue
        if not line_has_code and char == "\\" and following == "\\":
            if mask_strings:
                while index < len(chars) and chars[index] != "\n":
                    chars[index] = " "
                    index += 1
            else:
                while index < len(chars) and chars[index] != "\n":
                    index += 1
            continue
        line_has_code = True

        if char == "/" and following == "/":
            while index < len(chars) and chars[index] != "\n":
                chars[index] = " "
                index += 1
            continue
        if char == "/" and following == "*":
            chars[index] = chars[index + 1] = " "
            block_depth = 1
            index += 2
            continue
        if char in {'"', "'"}:
            quote = char
            if mask_strings:
                chars[index] = " "
            index += 1
            continue
        index += 1

    return "".join(chars)


def check_current_imported_function_refs(files: list[ChangedFile]) -> list[CheckResult]:
    """Find calls to local imported module functions that no longer exist.

    This catches a concrete class of split-merge mistakes: wrapper files and
    test roots can keep calling a function that was dropped while a module was
    split. It intentionally only checks `alias.fnName(...)` calls where `alias`
    is a local Zig `@import`, because those members must be public on the
    imported file.
    """

    missing: dict[str, list[str]] = {}
    member_cache: dict[str, set[str]] = {}
    import_re = re.compile(
        r"(?m)^\s*(?:const|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*@import\(\"([^\"]+)\"\)\s*;\s*$"
    )

    for item in files:
        path = item.path
        if not path.endswith(".zig"):
            continue
        try:
            text = git_index_or_worktree_text(path)
        except (RuntimeError, OSError, UnicodeDecodeError):
            continue

        imports: dict[str, str] = {}
        comment_free_text = mask_zig_comments_and_strings(text, mask_strings=False)
        for alias, import_path in import_re.findall(comment_free_text):
            if not alias[:1].islower():
                continue
            resolved = resolve_local_import(path, import_path)
            if resolved:
                imports[alias] = resolved
        if not imports:
            continue

        code_text = mask_zig_comments_and_strings(text)
        for alias, member in re.findall(r"(?<!\.)\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s*\(", code_text):
            imported_path = imports.get(alias)
            if not imported_path:
                continue
            if imported_path not in member_cache:
                member_cache[imported_path] = pub_members_of_file(imported_path)
            if member not in member_cache[imported_path]:
                missing.setdefault(path, []).append(f"{alias}.{member} -> {imported_path}")

    if not missing:
        return [CheckResult("current local imported function references", True, "ok")]

    parts: list[str] = []
    for path, values in list(missing.items())[:30]:
        uniq = sorted(set(values))
        parts.append(f"{path}: {', '.join(uniq[:30])}")
        if len(uniq) > 30:
            parts[-1] += f" ... +{len(uniq) - 30} more"
    detail = "; ".join(parts)
    if len(missing) > 30:
        detail += f"; ... +{len(missing) - 30} files"
    return [CheckResult("current local imported function references", False, detail)]


def report_markdown(origin: str, checks: list[CheckResult], files: list[ChangedFile]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    origin_rev = run_git(["rev-parse", "--verify", origin]).strip()
    out: list[str] = [
        "# Antfly Merge Audit",
        "",
        f"Generated: {now}",
        f"Incoming audited: {origin} ({origin_rev})",
        "",
        "## Changed Files",
        "",
        *changed_file_inventory(files),
        "",
        "## Checks",
        "",
    ]
    for check in checks:
        status = "OK" if check.ok else "FAIL"
        out.append(f"- {status}: {check.name}: {check.detail}")
    failed = [check for check in checks if not check.ok]
    if failed:
        out.extend(["", "## Open Gaps", ""])
        for check in failed:
            out.append(f"- {check.name}: {check.detail}")
    out.extend([
        "",
        "## Validation Commands",
        "",
        "```sh",
        "python3 scripts/merge_audit/audit_zig_split_merge.py --report-only",
        "zig build",
        "git diff --cached --check",
        "```",
        "",
        "Note: this report is generated by the tracked merge-audit tooling under scripts/merge_audit/.",
        "",
    ])
    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-only", action="store_true", help="print findings but always exit 0")
    parser.add_argument("--write-report", help="write a markdown report to this path")
    parser.add_argument("--manifest", default=str(DEFAULT_POLICY_PATH), help="JSON merge-audit policy manifest")
    parser.add_argument("--origin", help="incoming ref to audit; defaults to MERGE_HEAD when active, otherwise origin/main")
    parser.add_argument("--base", help="override merge base; useful for post-merge audits such as --base HEAD^2^")
    args = parser.parse_args()

    load_manifest_policy(pathlib.Path(args.manifest))

    global AUDIT_BASE_OVERRIDE
    if args.base:
        AUDIT_BASE_OVERRIDE = run_git(["rev-parse", "--verify", args.base]).strip()
    origin = args.origin or audit_origin()
    base = merge_base(origin)
    files = changed_files(origin)
    ours_files = changed_files_from_base(base, "HEAD", "ours")
    global_current_tests = all_current_zig_test_names()
    checks: list[CheckResult] = []
    checks.extend(check_changed_files(origin, files))
    checks.extend(relabel_checks("incoming", check_same_path_public_surface(origin, files)))
    checks.extend(relabel_checks("incoming", check_same_path_zig_tests(origin, files, global_current_tests)))
    checks.extend(check_non_zig_test_names(origin, files))
    checks.extend(check_non_zig_source_symbols(origin, files))
    checks.extend(relabel_checks("incoming", check_split_zig_tests(origin, global_current_tests)))
    checks.extend(check_changed_split_helper_names(origin))
    checks.extend(relabel_checks("ours", check_same_path_public_surface("HEAD", ours_files)))
    checks.extend(relabel_checks("ours", check_same_path_zig_tests("HEAD", ours_files, global_current_tests)))
    checks.extend(relabel_checks("ours", check_split_zig_tests("HEAD", global_current_tests)))
    checks.extend(check_current_imported_function_refs(files))
    checks.extend(relabel_checks("incoming", check_surface_symbols(origin)))
    checks.extend(relabel_checks("ours", check_surface_symbols("HEAD")))
    checks.extend(relabel_checks("incoming", check_request_and_hit_fields(origin)))
    checks.extend(relabel_checks("ours", check_request_and_hit_fields("HEAD")))
    checks.extend(relabel_checks("incoming", check_exact_sort_gap(origin)))
    checks.extend(relabel_checks("ours", check_exact_sort_gap("HEAD")))
    checks.extend(check_deadline_and_aknn())
    checks.extend(check_public_sync_level_naming())
    checks.extend(check_known_merge_risk_wiring())
    checks.extend(check_route_matcher_shadowing())
    checks.extend(check_removed_file_reference_hygiene())
    checks.extend(check_generated_rename_hygiene())
    checks.extend(check_sdk_contract_hygiene())
    checks.extend(check_generated_api_surface_matrix())
    checks.extend(check_sdk_export_surface(origin))
    checks.extend(check_generated_protobuf_drift())
    checks.extend(check_go_duplicate_declarations(files))
    checks.extend(check_openapi_operation_hygiene())
    checks.extend(check_table_write_repair_ledger_hygiene())
    checks.extend(check_table_reads_lsm_storage_stats_hygiene())
    checks.extend(check_table_writes_document_artifact_hook_hygiene())
    checks.extend(check_db_pending_work_stats_hygiene())
    checks.extend(check_db_restore_runtime_repair_drain_hygiene())
    checks.extend(check_db_terminal_load_failure_stats_hygiene())
    checks.extend(check_generated_read_validation_hygiene())
    checks.extend(check_sql_row_policy_grammar_hygiene())
    checks.extend(check_sql_native_requirement_fixture_hygiene())
    checks.extend(check_ci_build_harness_hygiene())
    checks.extend(relabel_checks("incoming", check_concrete_source_vtable_bindings(origin)))
    checks.extend(relabel_checks("ours", check_concrete_source_vtable_bindings("HEAD")))

    failed = [check for check in checks if not check.ok]
    for check in checks:
        status = "OK" if check.ok else "FAIL"
        print(f"{status:4} {check.name}: {check.detail}")

    if failed:
        print()
        print("Open merge gaps:")
        for check in failed:
            print(f"- {check.name}: {check.detail}")

    if args.write_report:
        path = pathlib.Path(args.write_report)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(report_markdown(origin, checks, files))
    return 0 if args.report_only or not failed else 1


if __name__ == "__main__":
    sys.exit(main())
