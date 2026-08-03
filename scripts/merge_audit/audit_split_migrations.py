#!/usr/bin/env python3
"""Run every manifest-declared split migration and aggregate its audit."""

from __future__ import annotations

import argparse
from collections import Counter
import fnmatch
import hashlib
import json
import pathlib
import subprocess
import sys
from typing import Any

try:
    from .audit_main_capture import ensure_manifest_shape, load_json_file
except ImportError:  # Direct script execution.
    from audit_main_capture import ensure_manifest_shape, load_json_file


ROOT = pathlib.Path(__file__).resolve().parents[2]
SPLIT_AUDITOR = ROOT / "scripts/merge_audit/audit_split_declarations.py"

# These statuses establish presence or preserve a branch-side declaration while
# the incoming declaration is unchanged. Every other status requires resolution.
ACCEPTED_STATUSES = frozenset(
    {
        "carried_branch_changed",
        "companion_added_exact",
        "companion_deleted_absent",
        "companion_exact",
        "container_review_fields_present",
        "deleted_absent",
        "deleted_retained_reviewed",
        "exact",
        "integrated",
        "intentional_deletion",
        "split_binding_alias_adapted",
        "split_import_path_adapted",
        "split_module_reference_adapted",
        "split_test_name_adapted",
        "split_visibility_adapted",
        "semantic_resolution_reviewed",
        "three_way_composition_reviewed",
    }
)


def parse_name_status(raw: bytes) -> set[str]:
    fields = [field for field in raw.split(b"\0") if field]
    paths: set[str] = set()
    index = 0
    while index < len(fields):
        status = fields[index].decode("ascii")
        index += 1
        if not status:
            raise ValueError("git diff emitted an empty companion status")
        if status[0] in {"R", "C"}:
            if index + 1 >= len(fields):
                raise ValueError("git diff emitted a truncated rename/copy record")
            old_path = fields[index].decode("utf-8")
            new_path = fields[index + 1].decode("utf-8")
            index += 2
            if status[0] == "R":
                paths.add(old_path)
            paths.add(new_path)
            continue
        if index >= len(fields):
            raise ValueError("git diff emitted a truncated companion record")
        paths.add(fields[index].decode("utf-8"))
        index += 1
    return paths


def changed_paths(base: str, incoming: str) -> set[str]:
    proc = subprocess.run(
        [
            "git",
            "diff",
            "--name-status",
            "-z",
            "--find-renames",
            f"{base}..{incoming}",
            "--",
        ],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if proc.returncode != 0:
        detail = proc.stderr.decode(errors="replace").strip()
        raise RuntimeError(
            f"git diff failed while inventorying declaration companions: {detail}"
        )
    return parse_name_status(proc.stdout)


def declaration_companion_paths(
    policy: dict[str, Any],
    selected: list[str],
    incoming_paths: set[str],
) -> dict[str, list[str]]:
    migrations = policy["split_migrations"]
    migration_sources = {
        migration.get("source")
        for migration in migrations.values()
        if isinstance(migration, dict)
        and isinstance(migration.get("source"), str)
    }
    result: dict[str, list[str]] = {}
    owners: dict[str, str] = {}
    for name in selected:
        migration = migrations[name]
        patterns = migration.get("declaration_companion_globs", [])
        if not isinstance(patterns, list) or any(
            not isinstance(pattern, str)
            or not pattern
            or pattern.startswith("/")
            or ".." in pathlib.PurePosixPath(pattern).parts
            for pattern in patterns
        ):
            raise ValueError(
                f"split_migrations.{name}.declaration_companion_globs must "
                "contain repository-relative patterns"
            )
        source = migration["source"]
        matched = sorted(
            path
            for path in incoming_paths
            if path.endswith(".zig")
            and path != source
            and path not in migration_sources
            and any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)
        )
        for path in matched:
            previous = owners.setdefault(path, name)
            if previous != name:
                raise ValueError(
                    f"declaration companion {path} has ambiguous owners: "
                    f"{previous}, {name}"
                )
        result[name] = matched
    return result


def ref_blob(ref: str, path: str) -> bytes | None:
    proc = subprocess.run(
        ["git", "show", f"{ref}:{path}"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if proc.returncode == 0:
        return proc.stdout
    missing = subprocess.run(
        ["git", "cat-file", "-e", f"{ref}:{path}"],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if missing.returncode != 0:
        return None
    detail = proc.stderr.decode(errors="replace").strip()
    raise RuntimeError(f"git show failed for {ref}:{path}: {detail}")


def destination_blob(path: str, destination_ref: str | None) -> bytes | None:
    if destination_ref is not None:
        return ref_blob(destination_ref, path)
    candidate = ROOT / path
    try:
        candidate.relative_to(ROOT)
    except ValueError as exc:
        raise ValueError(f"companion path escapes repository: {path}") from exc
    return candidate.read_bytes() if candidate.is_file() else None


def blob_sha256(blob: bytes | None) -> str | None:
    return hashlib.sha256(blob).hexdigest() if blob is not None else None


def classify_companion_file(
    base_blob: bytes | None,
    incoming_blob: bytes | None,
    current_blob: bytes | None,
) -> str:
    if incoming_blob == current_blob:
        if incoming_blob is None:
            return "companion_deleted_absent"
        if base_blob is None:
            return "companion_added_exact"
        return "companion_exact"
    if current_blob is None:
        return "companion_missing_current"
    if incoming_blob is None:
        return "companion_deleted_retained"
    if base_blob is None:
        return "companion_added_diverged"
    return "companion_declaration_audit_required"


def load_migration_names(
    policy: dict[str, Any],
    selected: list[str],
) -> list[str]:
    migrations = policy.get("split_migrations")
    if not isinstance(migrations, dict) or not migrations:
        raise ValueError("policy split_migrations must be a non-empty object")
    if any(not isinstance(name, str) or not name for name in migrations):
        raise ValueError("policy split_migrations keys must be non-empty strings")

    required = policy.get("required_split_migrations", [])
    if not isinstance(required, list) or any(
        not isinstance(name, str) or not name for name in required
    ):
        raise ValueError(
            "policy required_split_migrations must be a list of non-empty strings"
        )
    duplicate_required = sorted(
        name for name, count in Counter(required).items() if count > 1
    )
    if duplicate_required:
        raise ValueError(
            "duplicate required split migrations: "
            + ", ".join(duplicate_required)
        )
    missing_required = sorted(set(required) - set(migrations))
    if missing_required:
        raise ValueError(
            "required split migrations are not configured: "
            + ", ".join(missing_required)
        )

    names = selected or list(migrations)
    duplicates = sorted(
        name for name, count in Counter(names).items() if count > 1
    )
    if duplicates:
        raise ValueError(
            "duplicate --migration values: " + ", ".join(duplicates)
        )
    unknown = sorted(set(names) - set(migrations))
    if unknown:
        raise ValueError(
            "unknown split migrations: "
            + ", ".join(unknown)
            + "; available: "
            + ", ".join(migrations)
        )
    return names


def unresolved_statuses(statuses: dict[str, Any]) -> dict[str, int]:
    unresolved: dict[str, int] = {}
    for status, count in statuses.items():
        if (
            not isinstance(status, str)
            or not isinstance(count, int)
            or isinstance(count, bool)
            or count < 0
        ):
            raise ValueError("split audit summary has invalid status counts")
        if count and status not in ACCEPTED_STATUSES:
            unresolved[status] = count
    return dict(sorted(unresolved.items()))


def review_queue_items(
    migration: str,
    report: dict[str, Any],
    unresolved: dict[str, int],
) -> list[dict[str, Any]]:
    obligations = report.get("obligations")
    if not isinstance(obligations, list):
        raise ValueError("split audit report obligations must be a list")
    items: list[dict[str, Any]] = []
    for obligation in obligations:
        if not isinstance(obligation, dict):
            raise ValueError("split audit obligations must be objects")
        status = obligation.get("status")
        if status not in unresolved:
            continue
        base_line = obligation.get("base_line")
        if base_line is not None and (
            not isinstance(base_line, int)
            or isinstance(base_line, bool)
            or base_line < 1
        ):
            raise ValueError("split audit obligation has invalid base_line")
        incoming_line = obligation.get("incoming_line")
        if incoming_line is not None and (
            not isinstance(incoming_line, int)
            or isinstance(incoming_line, bool)
            or incoming_line < 1
        ):
            raise ValueError("split audit obligation has invalid incoming_line")
        items.append(
            {
                "migration": migration,
                "key": obligation.get("key"),
                "kind": obligation.get("kind"),
                "change": obligation.get("change"),
                "status": status,
                "base_line": base_line,
                "incoming_line": incoming_line,
                "current_path": obligation.get("current_path"),
                "suggested_path": obligation.get("suggested_path"),
                "previous_anchor": obligation.get("previous_anchor"),
                "next_anchor": obligation.get("next_anchor"),
                "detail": obligation.get("detail"),
            }
        )
    return items


def child_command(
    args: argparse.Namespace,
    migration: str,
    report_path: pathlib.Path,
) -> list[str]:
    command = [
        sys.executable,
        str(SPLIT_AUDITOR),
        "--base",
        args.base,
        "--incoming",
        args.incoming,
        "--migration",
        migration,
        "--policy",
        str(args.policy),
        "--json-out",
        str(report_path),
        "--minimum-similarity",
        str(args.minimum_similarity),
        "--ambiguity-margin",
        str(args.ambiguity_margin),
        "--summary-only",
    ]
    if args.include_unchanged:
        command.append("--include-unchanged")
    if args.include_missing_candidates:
        command.append("--include-missing-candidates")
    if args.include_review_bodies:
        command.append("--include-review-bodies")
    if args.destination_ref:
        command.extend(["--destination-ref", args.destination_ref])
    if args.candidate_dir:
        command.extend(
            [
                "--candidate-dir",
                str(args.candidate_dir / migration),
            ]
        )
    return command


def companion_child_command(
    args: argparse.Namespace,
    label: str,
    owner_migration: str,
    path: str,
    report_path: pathlib.Path,
) -> list[str]:
    command = [
        sys.executable,
        str(SPLIT_AUDITOR),
        "--base",
        args.base,
        "--incoming",
        args.incoming,
        "--source",
        path,
        "--destination",
        path,
        "--companion-owner",
        owner_migration,
        "--policy",
        str(args.policy),
        "--json-out",
        str(report_path),
        "--minimum-similarity",
        str(args.minimum_similarity),
        "--ambiguity-margin",
        str(args.ambiguity_margin),
        "--summary-only",
    ]
    if args.include_unchanged:
        command.append("--include-unchanged")
    if args.include_missing_candidates:
        command.append("--include-missing-candidates")
    if args.include_review_bodies:
        command.append("--include-review-bodies")
    if args.destination_ref:
        command.extend(["--destination-ref", args.destination_ref])
    if args.candidate_dir:
        command.extend(["--candidate-dir", str(args.candidate_dir / label)])
    return command


def report_slug(path: str) -> str:
    readable = "".join(
        char if char.isalnum() else "_" for char in pathlib.PurePosixPath(path).stem
    ).strip("_") or "companion"
    digest = hashlib.sha256(path.encode()).hexdigest()[:12]
    return f"{readable[:48]}_{digest}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", required=True, help="pinned previous-main base")
    parser.add_argument("--incoming", required=True, help="pinned incoming revision")
    parser.add_argument(
        "--policy",
        required=True,
        type=pathlib.Path,
        help="merge audit policy containing split_migrations",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        type=pathlib.Path,
        help="directory for per-migration JSON reports and index.json",
    )
    parser.add_argument(
        "--migration",
        action="append",
        default=[],
        help="audit only this migration; repeat as needed (default: every entry)",
    )
    parser.add_argument(
        "--candidate-dir",
        type=pathlib.Path,
        help="optional root for per-migration candidate trees",
    )
    parser.add_argument("--minimum-similarity", type=float, default=0.70)
    parser.add_argument("--ambiguity-margin", type=float, default=0.05)
    parser.add_argument("--include-unchanged", action="store_true")
    parser.add_argument("--include-missing-candidates", action="store_true")
    parser.add_argument("--include-review-bodies", action="store_true")
    parser.add_argument(
        "--destination-ref",
        help=(
            "read every migration's destinations from this clean pinned ref "
            "instead of the worktree"
        ),
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="fail after all reports are written if any obligation needs review",
    )
    args = parser.parse_args()

    if args.destination_ref and args.candidate_dir:
        parser.error("--destination-ref cannot be combined with --candidate-dir")

    policy_path = args.policy
    if not policy_path.is_absolute():
        policy_path = ROOT / policy_path
    args.policy = policy_path.resolve()
    if not args.policy.is_file():
        parser.error(f"policy does not exist: {args.policy}")
    if not 0 <= args.minimum_similarity <= 1:
        parser.error("--minimum-similarity must be between 0 and 1")
    if not 0 <= args.ambiguity_margin <= 1:
        parser.error("--ambiguity-margin must be between 0 and 1")

    try:
        policy = ensure_manifest_shape(
            load_json_file(args.policy),
            args.policy,
        )
        migrations = load_migration_names(policy, args.migration)
        companions = declaration_companion_paths(
            policy,
            migrations,
            changed_paths(args.base, args.incoming),
        )
    except (json.JSONDecodeError, RuntimeError, ValueError) as exc:
        parser.error(str(exc))

    args.output_dir.mkdir(parents=True, exist_ok=True)
    if args.candidate_dir:
        args.candidate_dir.mkdir(parents=True, exist_ok=True)

    aggregate_statuses: Counter[str] = Counter()
    reports: dict[str, dict[str, Any]] = {}
    child_failures: list[str] = []
    total_unresolved = 0
    review_queue: list[dict[str, Any]] = []
    audit_order = list(migrations)

    for migration in migrations:
        report_path = args.output_dir / f"{migration}.json"
        result = subprocess.run(
            child_command(args, migration, report_path),
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            child_failures.append(migration)
            reports[migration] = {
                "report": str(report_path),
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
            print(
                f"{migration}: audit failed with exit code {result.returncode}",
                file=sys.stderr,
            )
            continue

        try:
            report = json.loads(report_path.read_text())
            summary = report["summary"]
            if not isinstance(summary, dict):
                raise ValueError("summary must be an object")
            statuses = summary["statuses"]
            if not isinstance(statuses, dict):
                raise ValueError("summary/statuses must be objects")
            unresolved = unresolved_statuses(statuses)
            review_queue.extend(
                review_queue_items(migration, report, unresolved)
            )
        except (
            OSError,
            KeyError,
            TypeError,
            json.JSONDecodeError,
            ValueError,
        ) as exc:
            child_failures.append(migration)
            reports[migration] = {
                "report": str(report_path),
                "returncode": 0,
                "error": f"invalid child report: {exc}",
            }
            print(f"{migration}: invalid child report: {exc}", file=sys.stderr)
            continue

        aggregate_statuses.update(statuses)
        unresolved_count = sum(unresolved.values())
        total_unresolved += unresolved_count
        reports[migration] = {
            "report": str(report_path),
            "summary": summary,
            "unresolved_statuses": unresolved,
            "unresolved_obligations": unresolved_count,
        }
        print(
            f"{migration}: declarations={summary['changed_declarations']} "
            f"unresolved={unresolved_count}"
        )
        if unresolved:
            print(
                "  "
                + ", ".join(
                    f"{status}={count}"
                    for status, count in unresolved.items()
                )
            )

    for migration in migrations:
        for path in companions[migration]:
            label = f"{migration}::{path}"
            audit_order.append(label)
            report_path = args.output_dir / (
                f"{migration}__companion__{report_slug(path)}.json"
            )
            try:
                base_blob = ref_blob(args.base, path)
                incoming_blob = ref_blob(args.incoming, path)
                current_blob = destination_blob(path, args.destination_ref)
                file_status = classify_companion_file(
                    base_blob,
                    incoming_blob,
                    current_blob,
                )
            except (OSError, RuntimeError, ValueError) as exc:
                child_failures.append(label)
                reports[label] = {
                    "report": str(report_path),
                    "error": f"companion inventory failed: {exc}",
                }
                print(f"{label}: companion inventory failed: {exc}", file=sys.stderr)
                continue

            if file_status == "companion_declaration_audit_required":
                result = subprocess.run(
                    companion_child_command(
                        args,
                        report_slug(path),
                        migration,
                        path,
                        report_path,
                    ),
                    cwd=ROOT,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                if result.returncode != 0:
                    child_failures.append(label)
                    reports[label] = {
                        "report": str(report_path),
                        "returncode": result.returncode,
                        "stdout": result.stdout,
                        "stderr": result.stderr,
                    }
                    print(
                        f"{label}: audit failed with exit code {result.returncode}",
                        file=sys.stderr,
                    )
                    continue
                try:
                    report = json.loads(report_path.read_text())
                    summary = report["summary"]
                    statuses = summary["statuses"]
                    if not isinstance(summary, dict) or not isinstance(statuses, dict):
                        raise ValueError("summary/statuses must be objects")
                    unresolved = unresolved_statuses(statuses)
                    review_queue.extend(
                        review_queue_items(label, report, unresolved)
                    )
                except (
                    OSError,
                    KeyError,
                    TypeError,
                    json.JSONDecodeError,
                    ValueError,
                ) as exc:
                    child_failures.append(label)
                    reports[label] = {
                        "report": str(report_path),
                        "returncode": 0,
                        "error": f"invalid companion report: {exc}",
                    }
                    print(
                        f"{label}: invalid companion report: {exc}",
                        file=sys.stderr,
                    )
                    continue
                aggregate_statuses.update(statuses)
                unresolved_count = sum(unresolved.values())
                total_unresolved += unresolved_count
                reports[label] = {
                    "path": path,
                    "owner_migration": migration,
                    "report": str(report_path),
                    "summary": summary,
                    "unresolved_statuses": unresolved,
                    "unresolved_obligations": unresolved_count,
                }
                print(
                    f"{label}: declarations={summary['changed_declarations']} "
                    f"unresolved={unresolved_count}"
                )
                if unresolved:
                    print(
                        "  "
                        + ", ".join(
                            f"{status}={count}"
                            for status, count in unresolved.items()
                        )
                    )
                continue

            statuses = {file_status: 1}
            unresolved = unresolved_statuses(statuses)
            unresolved_count = sum(unresolved.values())
            aggregate_statuses.update(statuses)
            total_unresolved += unresolved_count
            synthetic_report = {
                "base": args.base,
                "incoming": args.incoming,
                "owner_migration": migration,
                "path": path,
                "status": file_status,
                "base_sha256": blob_sha256(base_blob),
                "incoming_sha256": blob_sha256(incoming_blob),
                "current_sha256": blob_sha256(current_blob),
            }
            report_path.write_text(
                json.dumps(synthetic_report, indent=2, sort_keys=True) + "\n"
            )
            reports[label] = {
                "path": path,
                "owner_migration": migration,
                "report": str(report_path),
                "summary": {"statuses": statuses},
                "unresolved_statuses": unresolved,
                "unresolved_obligations": unresolved_count,
            }
            if unresolved:
                review_queue.append(
                    {
                        "migration": label,
                        "key": f"file:{path}",
                        "kind": "file",
                        "change": "companion",
                        "status": file_status,
                        "base_line": None,
                        "incoming_line": None,
                        "current_path": path if current_blob is not None else None,
                        "suggested_path": path,
                        "previous_anchor": None,
                        "next_anchor": None,
                        "detail": "companion file requires explicit resolution",
                    }
                )
            print(f"{label}: {file_status} unresolved={unresolved_count}")

    review_queue.sort(
        key=lambda item: (
            audit_order.index(item["migration"]),
            (item["incoming_line"] or item["base_line"]) is None,
            item["incoming_line"] or item["base_line"] or 0,
            item["key"] or "",
        )
    )
    review_queue_path = args.output_dir / "review-queue.json"
    review_queue_path.write_text(
        json.dumps(
            {
                "base": args.base,
                "incoming": args.incoming,
                "items": review_queue,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )

    index = {
        "base": args.base,
        "incoming": args.incoming,
        "policy": str(args.policy),
        "migrations": reports,
        "summary": {
            "audited_migrations": len(reports) - len(child_failures),
            "child_failures": child_failures,
            "statuses": dict(sorted(aggregate_statuses.items())),
            "unresolved_obligations": total_unresolved,
        },
        "review_queue": str(review_queue_path),
        "strict": args.strict,
    }
    index_path = args.output_dir / "index.json"
    index_path.write_text(json.dumps(index, indent=2, sort_keys=True) + "\n")
    print(f"aggregate report: {index_path}")

    if child_failures:
        return 2
    if args.strict and total_unresolved:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
