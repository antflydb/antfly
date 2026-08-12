#!/usr/bin/env python3
"""Report potential, analyzed, and emitted reachability in Antfly's Zig graph.

The source graph follows literal relative ``@import("*.zig")`` edges. It makes
accidental barrel imports and surprising paths visible, but includes imports in
lazy declarations and tests. A Zig ``--time-report`` JSON is the authoritative
view of files that a particular compiler invocation loaded and declarations it
analyzed. A cross-compiled ELF object is the authoritative view of emitted
machine-code/data sections. Passing multiple reports ranks both lazy file
overlap and actual duplicate emitted modules.
"""

from __future__ import annotations

import argparse
import collections
import json
import re
import struct
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Iterator


SCRIPT = Path(__file__).resolve()
REPO_ROOT = SCRIPT.parents[2]
DEFAULT_SOURCE_ROOT = REPO_ROOT / "zig/pkg/antfly/src"
IMPORT_RE = re.compile(r'@import\(\s*"([^"]+)"\s*\)')

DEFAULT_ROOTS = {
    "data": "data/mod.zig",
    "metadata": "metadata/mod.zig",
    "serverless": "serverless/mod.zig",
    "standalone": "standalone/mod.zig",
    "inference": "inference_runtime/runtime.zig",
    "ha": "storage/ha/mod.zig",
    "lite": "storage/lite/mod.zig",
    "public_api": "api/mod.zig",
    "db": "storage/db/mod.zig",
    "admin": "admin/mod.zig",
    "common": "common/mod.zig",
    "raft": "raft/mod.zig",
}

SERVER_ROLES = ("data", "metadata", "serverless", "standalone", "inference", "ha")
RUNTIME_BOUNDARIES = (
    "data/runtime.zig",
    "metadata/runtime.zig",
    "standalone/runtime.zig",
)
CODEGEN_BOUNDARIES = (
    ("cli_runtime.zig", "standalone/runtime.zig"),
    ("cli_runtime.zig", "inference_runtime/runtime.zig"),
    ("data/domain.zig", "data/runtime.zig"),
    ("metadata/domain.zig", "metadata/runtime.zig"),
    ("data/runtime.zig", "metadata/runtime.zig"),
    ("metadata/runtime.zig", "data/runtime.zig"),
    ("raft/mod.zig", "metadata/sim_harness.zig"),
    ("standalone/inference_host.zig", "standalone/runtime.zig"),
    ("standalone/inference_host.zig", "data/runtime.zig"),
    ("standalone/inference_host.zig", "metadata/runtime.zig"),
)

# These files form the source-level ABI between the separately code-generated
# API and distributed runtime units. Keep their direct imports data-only. The
# lexical graph intentionally sees lazy/test imports, so transitive enforcement
# is performed against an API compiler time report when one is supplied.
API_KERNEL_CONTRACTS = (
    "api/kernel_exports.zig",
    "api/lite_sql_source.zig",
    "api/table_read_source.zig",
    "api/table_write_source.zig",
)
API_KERNEL_IMPLEMENTATIONS = (
    "api/table_reads.zig",
    "api/table_writes.zig",
    "sql/ddl_plan.zig",
    "sql/plan.zig",
    "sql/relational_rows.zig",
    "query/row_execution_contract.zig",
    "storage/db/mod.zig",
    "storage/db/db.zig",
    "storage/db/types.zig",
    "storage/docstore.zig",
    "storage/lite/sql_source.zig",
    "storage/lmdb.zig",
    "storage/lmdb_backend.zig",
    "storage/schema.zig",
)
SQL_STORAGE_RUNTIME_IMPORT_ALLOWLIST = frozenset(
    {
        "sql/document_runtime.zig",
        "sql/routines.zig",
    }
)
@dataclass(frozen=True)
class GraphStats:
    files: int
    lines: int


@dataclass(frozen=True)
class TimeReport:
    name: str
    path: Path
    raw: dict[str, object]
    repo_files: frozenset[Path]
    has_file_list: bool


@dataclass(frozen=True)
class ModuleEmission:
    bytes: int
    text_bytes: int
    sections: int


@dataclass(frozen=True)
class ObjectReport:
    name: str
    path: Path
    modules: dict[str, ModuleEmission]
    alloc_bytes: int
    text_bytes: int
    unassigned_bytes: int
    named_sections: dict[str, ModuleEmission] = field(default_factory=dict)


class ImportGraph:
    def __init__(self, source_root: Path):
        self.source_root = source_root.resolve()
        if not self.source_root.is_dir():
            raise ValueError(f"source root is not a directory: {self.source_root}")
        self._edge_cache: dict[Path, tuple[Path, ...]] = {}
        self._line_cache: dict[Path, int] = {}

    def resolve_source(self, value: str) -> Path:
        path = (self.source_root / value).resolve()
        try:
            path.relative_to(self.source_root)
        except ValueError as error:
            raise ValueError(f"source path escapes {self.source_root}: {value}") from error
        if not path.is_file():
            raise ValueError(f"source file does not exist: {path}")
        return path

    def maybe_resolve_source(self, value: str) -> Path | None:
        try:
            return self.resolve_source(value)
        except ValueError:
            return None

    def relative_name(self, path: Path) -> str:
        return path.relative_to(self.source_root).as_posix()

    def relative_edges(self, path: Path) -> tuple[Path, ...]:
        cached = self._edge_cache.get(path)
        if cached is not None:
            return cached

        targets: list[Path] = []
        text = path.read_text(encoding="utf-8", errors="replace")
        for value in IMPORT_RE.findall(text):
            if not value.endswith(".zig"):
                continue
            target = (path.parent / value).resolve()
            try:
                target.relative_to(self.source_root)
            except ValueError:
                continue
            if target.is_file():
                targets.append(target)
        result = tuple(dict.fromkeys(targets))
        self._edge_cache[path] = result
        return result

    def closure(self, start_paths: Iterable[Path]) -> set[Path]:
        seen: set[Path] = set()
        queue = collections.deque(start_paths)
        while queue:
            path = queue.popleft()
            if path in seen:
                continue
            seen.add(path)
            queue.extend(self.relative_edges(path))
        return seen

    def shortest_path(self, start: Path, target: Path) -> list[Path]:
        parent: dict[Path, Path | None] = {start: None}
        queue = collections.deque([start])
        while queue:
            path = queue.popleft()
            if path == target:
                result: list[Path] = []
                cursor: Path | None = path
                while cursor is not None:
                    result.append(cursor)
                    cursor = parent[cursor]
                result.reverse()
                return result
            for child in self.relative_edges(path):
                if child not in parent:
                    parent[child] = path
                    queue.append(child)
        return []

    def line_count(self, path: Path) -> int:
        cached = self._line_cache.get(path)
        if cached is None:
            cached = len(path.read_text(encoding="utf-8", errors="replace").splitlines())
            self._line_cache[path] = cached
        return cached

    def stats(self, paths: Iterable[Path]) -> GraphStats:
        materialized = tuple(paths)
        return GraphStats(len(materialized), sum(self.line_count(path) for path in materialized))


def parse_named_root(value: str) -> tuple[str, str]:
    name, separator, path = value.partition("=")
    if not separator or not name or not path:
        raise argparse.ArgumentTypeError("values must use NAME=PATH")
    return name, path


def parse_comparison(value: str) -> tuple[str, str]:
    base, separator, candidate = value.partition(",")
    if not separator or not base or not candidate:
        raise argparse.ArgumentTypeError("comparisons must use BASE,CANDIDATE")
    return base, candidate


def arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-root",
        type=Path,
        default=DEFAULT_SOURCE_ROOT,
        help=f"Zig source tree to analyze (default: {DEFAULT_SOURCE_ROOT.relative_to(REPO_ROOT)})",
    )
    parser.add_argument(
        "--root",
        action="append",
        type=parse_named_root,
        metavar="NAME=PATH",
        help="replace the default report roots; may be repeated",
    )
    parser.add_argument(
        "--show-path",
        action="append",
        nargs=2,
        metavar=("SOURCE", "TARGET"),
        help="show a shortest relative-import path; may be repeated",
    )
    parser.add_argument(
        "--time-report",
        action="append",
        type=parse_named_root,
        default=[],
        metavar="NAME=PATH",
        help="summarize a Zig --time-report JSON; may be repeated",
    )
    parser.add_argument(
        "--object",
        action="append",
        type=parse_named_root,
        default=[],
        metavar="NAME=PATH",
        help="summarize emitted modules in an ELF Zig object; may be repeated",
    )
    parser.add_argument(
        "--compare",
        action="append",
        type=parse_comparison,
        default=[],
        metavar="BASE,CANDIDATE",
        help="compare analyzed files in two named time reports; may be repeated",
    )
    parser.add_argument(
        "--top-groups",
        type=int,
        default=12,
        metavar="N",
        help="show the N largest or most-changed repository source groups",
    )
    parser.add_argument(
        "--check-runtime-boundary",
        action="store_true",
        help="fail if a production runtime can statically reach the public root.zig barrel",
    )
    parser.add_argument(
        "--check-codegen-boundary",
        action="store_true",
        help="fail if focused codegen/domain units can reach excluded runtime or simulation roots",
    )
    parser.add_argument(
        "--check-api-kernel-boundary",
        action="store_true",
        help=(
            "fail on direct storage/table implementation imports from API ABI files"
        ),
    )
    parser.add_argument(
        "--check-sql-storage-boundary",
        action="store_true",
        help="fail if a SQL contract/planner file directly imports the full DB implementation barrel",
    )
    parser.add_argument("--largest", type=int, default=0, metavar="N", help="show the N largest files per graph")
    parser.add_argument("--json", action="store_true", help="emit the summary as JSON")
    return parser.parse_args(argv)


def analyze(graph: ImportGraph, roots: dict[str, str]) -> dict[str, set[Path]]:
    return {name: graph.closure([graph.resolve_source(path)]) for name, path in roots.items()}


def load_time_report(name: str, path: Path, repo_root: Path = REPO_ROOT) -> TimeReport:
    repo_root = repo_root.resolve()
    with path.open(encoding="utf-8") as source:
        raw = json.load(source)
    if not isinstance(raw, dict):
        raise ValueError(f"time report is not a JSON object: {path}")

    file_values = raw.get("all_files")
    has_file_list = isinstance(file_values, list)
    repo_files: set[Path] = set()
    for value in file_values if isinstance(file_values, list) else []:
        if not isinstance(value, str):
            continue
        candidate = Path(value)
        if not candidate.is_absolute():
            # Antfly's build is normally invoked from zig/, and Zig preserves
            # source paths relative to that working directory in the report.
            choices = (repo_root / "zig" / candidate, repo_root / candidate)
            candidate = next((item for item in choices if item.is_file()), choices[0])
        candidate = candidate.resolve()
        try:
            candidate.relative_to(repo_root)
        except ValueError:
            continue
        if candidate.suffix == ".zig":
            repo_files.add(candidate)
    return TimeReport(name, path, raw, frozenset(repo_files), has_file_list)


def elf_sections(path: Path) -> list[tuple[str, int, int]]:
    """Return (name, size, flags) for an ELF64 little-endian object."""

    data = path.read_bytes()
    if len(data) < 64 or data[:4] != b"\x7fELF":
        raise ValueError(f"object is not ELF: {path}")
    if data[4] != 2 or data[5] != 1:
        raise ValueError(f"object must be ELF64 little-endian: {path}")
    header = struct.unpack_from("<16sHHIQQQIHHHHHH", data)
    section_offset = header[6]
    section_entry_size = header[11]
    section_count = header[12]
    string_table_index = header[13]
    if section_entry_size < 64 or section_count == 0 or string_table_index >= section_count:
        raise ValueError(f"unsupported ELF section table: {path}")
    section_table_end = section_offset + section_entry_size * section_count
    if section_table_end > len(data):
        raise ValueError(f"truncated ELF section table: {path}")

    headers = [
        struct.unpack_from("<IIQQQQIIQQ", data, section_offset + index * section_entry_size)
        for index in range(section_count)
    ]
    strings_header = headers[string_table_index]
    strings_offset = strings_header[4]
    strings_size = strings_header[5]
    strings_end = strings_offset + strings_size
    if strings_end > len(data):
        raise ValueError(f"truncated ELF section-name table: {path}")
    strings = data[strings_offset:strings_end]

    def section_name(offset: int) -> str:
        if offset >= len(strings):
            return ""
        end = strings.find(b"\0", offset)
        if end < 0:
            end = len(strings)
        return strings[offset:end].decode("utf-8", errors="replace")

    return [(section_name(item[0]), item[5], item[2]) for item in headers]


def source_module_tokens(source_root: Path) -> tuple[str, ...]:
    tokens = []
    for path in source_root.rglob("*.zig"):
        token = path.relative_to(source_root).with_suffix("").as_posix().replace("/", ".")
        # One-segment names cannot be distinguished from dependency package names.
        if "." in token:
            tokens.append(token)
    return tuple(sorted(tokens, key=lambda value: (-len(value), value)))


def section_source_module(section_name: str, module_tokens: Iterable[str]) -> str | None:
    return next((token for token in module_tokens if token in section_name), None)


def named_alloc_section(section_name: str) -> bool:
    prefixes = (".text.", ".rodata.", ".data.", ".bss.", ".tdata.", ".tbss.")
    return any(section_name.startswith(prefix) and len(section_name) > len(prefix) for prefix in prefixes)


def section_namespace(section_name: str) -> str:
    local_marker = section_name.find(".L")
    if local_marker >= 0:
        symbol = section_name[local_marker + 2 :]
    else:
        symbol = section_name.rsplit(".", 1)[-1]
    return symbol.split(".", 1)[0] or "(anonymous)"


def load_object_report(name: str, path: Path, source_root: Path = DEFAULT_SOURCE_ROOT) -> ObjectReport:
    module_tokens = source_module_tokens(source_root.resolve())
    mutable: dict[str, list[int]] = collections.defaultdict(lambda: [0, 0, 0])
    named_sections: dict[str, list[int]] = collections.defaultdict(lambda: [0, 0, 0])
    alloc_bytes = 0
    text_bytes = 0
    unassigned_bytes = 0
    for section_name, size, flags in elf_sections(path):
        if not flags & 0x2 or size == 0:
            continue
        alloc_bytes += size
        is_text = section_name.startswith(".text")
        if is_text:
            text_bytes += size
        if named_alloc_section(section_name):
            section = named_sections[section_name]
            section[0] += size
            section[1] += size if is_text else 0
            section[2] += 1
        module = section_source_module(section_name, module_tokens)
        if module is None:
            unassigned_bytes += size
            continue
        row = mutable[module]
        row[0] += size
        row[1] += size if is_text else 0
        row[2] += 1
    modules = {
        module: ModuleEmission(bytes=row[0], text_bytes=row[1], sections=row[2])
        for module, row in mutable.items()
    }
    sections = {
        section_name: ModuleEmission(bytes=row[0], text_bytes=row[1], sections=row[2])
        for section_name, row in named_sections.items()
    }
    return ObjectReport(name, path, modules, alloc_bytes, text_bytes, unassigned_bytes, sections)


def integer(value: object) -> int:
    return value if isinstance(value, int) else 0


def seconds(value: object) -> float:
    return integer(value) / 1_000_000_000


def source_lines(path: Path) -> int:
    try:
        with path.open("rb") as source:
            return sum(1 for _ in source)
    except OSError:
        return 0


def source_group(path: Path, repo_root: Path = REPO_ROOT) -> str:
    parts = path.relative_to(repo_root).parts
    if parts[:4] == ("zig", "pkg", "antfly", "src") and len(parts) > 4:
        return f"zig/pkg/antfly/src/{parts[4]}"
    if parts[:2] == ("zig", "lib") and len(parts) > 2:
        return f"zig/lib/{parts[2]}"
    if parts[:2] == ("zig", "pkg") and len(parts) > 2:
        return f"zig/pkg/{parts[2]}"
    return "/".join(parts[:2])


def report_stats(report: TimeReport) -> dict[str, object]:
    stats_value = report.raw.get("stats")
    stats = stats_value if isinstance(stats_value, dict) else {}
    total = seconds(report.raw.get("total_ns"))
    llvm = seconds(stats.get("real_ns_llvm_emit"))
    return {
        "total_seconds": total,
        "llvm_emit_seconds": llvm,
        "llvm_emit_fraction": llvm / total if total else 0,
        "sema_cpu_seconds": seconds(stats.get("cpu_ns_sema")),
        "imported_files": integer(stats.get("imported_files")) or integer(report.raw.get("file_count")),
        "declarations": integer(report.raw.get("declaration_count")),
        "generic_instances": integer(stats.get("generic_instances")),
        "inline_calls": integer(stats.get("inline_calls")),
        "repo_file_list_available": report.has_file_list,
        "repo_zig_files": len(report.repo_files) if report.has_file_list else None,
        "repo_zig_lines": sum(source_lines(path) for path in report.repo_files) if report.has_file_list else None,
    }


def grouped_files(paths: Iterable[Path], repo_root: Path = REPO_ROOT) -> list[tuple[str, int, int]]:
    groups: dict[str, list[Path]] = collections.defaultdict(list)
    for path in paths:
        groups[source_group(path, repo_root)].append(path)
    return sorted(
        (
            (name, len(group_paths), sum(source_lines(path) for path in group_paths))
            for name, group_paths in groups.items()
        ),
        key=lambda row: (-row[2], row[0]),
    )


def aggregate_overlap_stats(
    reports: Iterable[TimeReport],
    repo_root: Path = REPO_ROOT,
) -> dict[str, object]:
    repo_root = repo_root.resolve()
    materialized = tuple(reports)
    available = bool(materialized) and all(report.has_file_list for report in materialized)
    result: dict[str, object] = {"available": available}
    if not available:
        return result

    occurrences = collections.Counter(
        path
        for report in materialized
        for path in report.repo_files
    )
    duplicated = {path: count for path, count in occurrences.items() if count > 1}
    groups: dict[str, list[tuple[Path, int]]] = collections.defaultdict(list)
    for path, count in duplicated.items():
        groups[source_group(path, repo_root)].append((path, count))

    result.update(
        {
            "report_count": len(materialized),
            "file_instances": sum(occurrences.values()),
            "unique_files": len(occurrences),
            "duplicate_instances": sum(count - 1 for count in occurrences.values()),
            "duplicated_files": len(duplicated),
            "groups": sorted(
                (
                    {
                        "name": name,
                        "files": len(entries),
                        "lines": sum(source_lines(path) for path, _ in entries),
                        "duplicate_instances": sum(count - 1 for _, count in entries),
                        "duplicate_lines": sum(
                            (count - 1) * source_lines(path)
                            for path, count in entries
                        ),
                    }
                    for name, entries in groups.items()
                ),
                key=lambda row: (-int(row["duplicate_lines"]), str(row["name"])),
            ),
        }
    )
    return result


def print_aggregate_overlap(reports: Iterable[TimeReport], top_groups: int) -> None:
    stats = aggregate_overlap_stats(reports)
    if not stats["available"]:
        return
    print(f"\naggregate compiler overlap ({stats['report_count']} reports)")
    print(f"repository file instances\t{stats['file_instances']}")
    print(f"unique repository files\t{stats['unique_files']}")
    print(f"duplicate instances\t{stats['duplicate_instances']}")
    print("top duplicated repository groups\tfiles\tduplicate instances\tlines\tduplicate lines")
    groups = stats["groups"]
    assert isinstance(groups, list)
    for row in groups[:top_groups]:
        assert isinstance(row, dict)
        print(
            f"{row['name']}\t{row['files']}\t{row['duplicate_instances']}\t"
            f"{row['lines']}\t{row['duplicate_lines']}"
        )


def object_report_stats(report: ObjectReport) -> dict[str, object]:
    return {
        "alloc_bytes": report.alloc_bytes,
        "text_bytes": report.text_bytes,
        "assigned_bytes": sum(item.bytes for item in report.modules.values()),
        "unassigned_bytes": report.unassigned_bytes,
        "emitting_modules": len(report.modules),
        "modules": [
            {
                "name": name,
                "bytes": emission.bytes,
                "text_bytes": emission.text_bytes,
                "sections": emission.sections,
            }
            for name, emission in sorted(
                report.modules.items(),
                key=lambda item: (-item[1].bytes, item[0]),
            )
        ],
    }


def aggregate_object_overlap_stats(reports: Iterable[ObjectReport]) -> dict[str, object]:
    materialized = tuple(reports)
    occurrences: dict[str, list[ModuleEmission]] = collections.defaultdict(list)
    for report in materialized:
        for module, emission in report.modules.items():
            occurrences[module].append(emission)
    duplicated = {module: emissions for module, emissions in occurrences.items() if len(emissions) > 1}
    rows = []
    for module, emissions in duplicated.items():
        total_bytes = sum(item.bytes for item in emissions)
        total_text_bytes = sum(item.text_bytes for item in emissions)
        rows.append(
            {
                "name": module,
                "instances": len(emissions),
                "total_bytes": total_bytes,
                "duplicate_bytes": total_bytes - max(item.bytes for item in emissions),
                "duplicate_text_bytes": total_text_bytes - max(item.text_bytes for item in emissions),
            }
        )
    rows.sort(key=lambda row: (-int(row["duplicate_bytes"]), str(row["name"])))
    return {
        "available": bool(materialized),
        "report_count": len(materialized),
        "module_instances": sum(len(items) for items in occurrences.values()),
        "unique_modules": len(occurrences),
        "duplicated_modules": len(duplicated),
        "duplicate_bytes": sum(int(row["duplicate_bytes"]) for row in rows),
        "duplicate_text_bytes": sum(int(row["duplicate_text_bytes"]) for row in rows),
        "modules": rows,
    }


def aggregate_named_section_overlap_stats(reports: Iterable[ObjectReport]) -> dict[str, object]:
    materialized = tuple(reports)
    occurrences: dict[str, list[ModuleEmission]] = collections.defaultdict(list)
    for report in materialized:
        for section_name, emission in report.named_sections.items():
            occurrences[section_name].append(emission)

    rows = []
    for section_name, emissions in occurrences.items():
        if len(emissions) < 2:
            continue
        total_bytes = sum(item.bytes for item in emissions)
        total_text_bytes = sum(item.text_bytes for item in emissions)
        rows.append(
            {
                "name": section_name,
                "namespace": section_namespace(section_name),
                "instances": len(emissions),
                "duplicate_bytes": total_bytes - max(item.bytes for item in emissions),
                "duplicate_text_bytes": total_text_bytes - max(item.text_bytes for item in emissions),
            }
        )
    rows.sort(key=lambda row: (-int(row["duplicate_bytes"]), str(row["name"])))

    namespaces: dict[str, list[int]] = collections.defaultdict(lambda: [0, 0, 0])
    for row in rows:
        namespace = namespaces[str(row["namespace"])]
        namespace[0] += 1
        namespace[1] += int(row["duplicate_bytes"])
        namespace[2] += int(row["duplicate_text_bytes"])
    namespace_rows = [
        {
            "name": name,
            "sections": values[0],
            "duplicate_bytes": values[1],
            "duplicate_text_bytes": values[2],
        }
        for name, values in namespaces.items()
    ]
    namespace_rows.sort(key=lambda row: (-int(row["duplicate_bytes"]), str(row["name"])))

    return {
        "available": bool(materialized),
        "report_count": len(materialized),
        "duplicated_sections": len(rows),
        "duplicate_bytes": sum(int(row["duplicate_bytes"]) for row in rows),
        "duplicate_text_bytes": sum(int(row["duplicate_text_bytes"]) for row in rows),
        "namespaces": namespace_rows,
        "sections": rows,
    }


def print_object_report(report: ObjectReport, top_groups: int) -> None:
    stats = object_report_stats(report)
    print(f"\nobject report {report.name}: {report.path}")
    print(f"alloc section bytes\t{stats['alloc_bytes']}")
    print(f"text section bytes\t{stats['text_bytes']}")
    print(f"assigned Antfly bytes\t{stats['assigned_bytes']}")
    print(f"unassigned bytes\t{stats['unassigned_bytes']}")
    print(f"emitting Antfly modules\t{stats['emitting_modules']}")
    print("top emitting modules\tbytes\ttext bytes\tsections")
    modules = stats["modules"]
    assert isinstance(modules, list)
    for row in modules[:top_groups]:
        assert isinstance(row, dict)
        print(f"{row['name']}\t{row['bytes']}\t{row['text_bytes']}\t{row['sections']}")


def print_aggregate_object_overlap(reports: Iterable[ObjectReport], top_groups: int) -> None:
    stats = aggregate_object_overlap_stats(reports)
    if not stats["available"] or int(stats["report_count"]) < 2:
        return
    print(f"\naggregate emitted overlap ({stats['report_count']} objects)")
    print(f"module instances\t{stats['module_instances']}")
    print(f"unique modules\t{stats['unique_modules']}")
    print(f"duplicated modules\t{stats['duplicated_modules']}")
    print(f"duplicate alloc bytes\t{stats['duplicate_bytes']}")
    print(f"duplicate text bytes\t{stats['duplicate_text_bytes']}")
    print("top duplicated emitting modules\tinstances\tduplicate bytes\tduplicate text bytes")
    modules = stats["modules"]
    assert isinstance(modules, list)
    for row in modules[:top_groups]:
        assert isinstance(row, dict)
        print(
            f"{row['name']}\t{row['instances']}\t"
            f"{row['duplicate_bytes']}\t{row['duplicate_text_bytes']}"
        )


def print_aggregate_named_section_overlap(reports: Iterable[ObjectReport], top_groups: int) -> None:
    stats = aggregate_named_section_overlap_stats(reports)
    if not stats["available"] or int(stats["report_count"]) < 2:
        return
    print(f"\naggregate same-named section overlap ({stats['report_count']} objects)")
    print(f"duplicated named sections\t{stats['duplicated_sections']}")
    print(f"potential duplicate alloc bytes\t{stats['duplicate_bytes']}")
    print(f"potential duplicate text bytes\t{stats['duplicate_text_bytes']}")
    print("top duplicated section namespaces\tsections\tduplicate bytes\tduplicate text bytes")
    namespaces = stats["namespaces"]
    assert isinstance(namespaces, list)
    for row in namespaces[:top_groups]:
        assert isinstance(row, dict)
        print(
            f"{row['name']}\t{row['sections']}\t"
            f"{row['duplicate_bytes']}\t{row['duplicate_text_bytes']}"
        )


def print_time_report(report: TimeReport, top_groups: int) -> None:
    stats = report_stats(report)
    print(f"\ntime report {report.name}: {report.path}")
    print(f"total seconds\t{stats['total_seconds']:.3f}")
    print(
        f"LLVM emit seconds\t{stats['llvm_emit_seconds']:.3f}\t"
        f"{stats['llvm_emit_fraction']:.1%}"
    )
    print(f"sema CPU seconds\t{stats['sema_cpu_seconds']:.3f}")
    print(f"imported files\t{stats['imported_files']}")
    print(f"declarations\t{stats['declarations']}")
    print(f"generic instances\t{stats['generic_instances']}")
    print(f"inline calls\t{stats['inline_calls']}")
    if not report.has_file_list:
        print("repository Zig files\tunavailable (report has no all_files field)")
        return
    print(f"repository Zig files\t{stats['repo_zig_files']}\t{stats['repo_zig_lines']} lines")
    print("top repository groups\tfiles\tlines")
    for name, files, lines in grouped_files(report.repo_files)[:top_groups]:
        print(f"{name}\t{files}\t{lines}")


def comparison_stats(base: TimeReport, candidate: TimeReport) -> dict[str, object]:
    base_total = seconds(base.raw.get("total_ns"))
    candidate_total = seconds(candidate.raw.get("total_ns"))
    result: dict[str, object] = {
        "base": base.name,
        "candidate": candidate.name,
        "base_total_seconds": base_total,
        "candidate_total_seconds": candidate_total,
        "total_seconds_delta": candidate_total - base_total,
        "file_comparison_available": base.has_file_list and candidate.has_file_list,
    }
    if base.has_file_list and candidate.has_file_list:
        shared = base.repo_files & candidate.repo_files
        added = candidate.repo_files - base.repo_files
        removed = base.repo_files - candidate.repo_files
        smaller_graph_files = min(len(base.repo_files), len(candidate.repo_files))
        result.update(
            {
                "repo_zig_files_delta": len(candidate.repo_files) - len(base.repo_files),
                "shared_files": len(shared),
                "shared_lines": sum(source_lines(path) for path in shared),
                "shared_fraction_of_smaller_graph": len(shared) / smaller_graph_files if smaller_graph_files else 0,
                "added_files": len(added),
                "added_lines": sum(source_lines(path) for path in added),
                "removed_files": len(removed),
                "removed_lines": sum(source_lines(path) for path in removed),
            }
        )
    return result


def print_comparison(base: TimeReport, candidate: TimeReport, top_groups: int) -> None:
    stats = comparison_stats(base, candidate)
    print(f"\ncompare {base.name} -> {candidate.name}")
    print(
        f"total seconds\t{stats['base_total_seconds']:.3f}\t"
        f"{stats['candidate_total_seconds']:.3f}\t{stats['total_seconds_delta']:+.3f}"
    )
    if not stats["file_comparison_available"]:
        print("file comparison unavailable: both reports must contain all_files")
        return

    print(
        f"shared\t{stats['shared_files']} files\t{stats['shared_lines']} lines\t"
        f"{stats['shared_fraction_of_smaller_graph']:.1%} of smaller graph"
    )
    added = candidate.repo_files - base.repo_files
    removed = base.repo_files - candidate.repo_files
    print(f"repository Zig files delta\t{stats['repo_zig_files_delta']:+d}")
    print(f"added\t{stats['added_files']} files\t{stats['added_lines']} lines")
    print(f"removed\t{stats['removed_files']} files\t{stats['removed_lines']} lines")

    changes: dict[str, list[int]] = collections.defaultdict(lambda: [0, 0, 0, 0])
    for path in added:
        row = changes[source_group(path)]
        row[0] += 1
        row[1] += source_lines(path)
    for path in removed:
        row = changes[source_group(path)]
        row[2] += 1
        row[3] += source_lines(path)
    ranked = sorted(changes.items(), key=lambda item: (-(item[1][1] + item[1][3]), item[0]))
    print("changed repository groups\tadded files/lines\tremoved files/lines")
    for name, (added_files, added_lines, removed_files, removed_lines) in ranked[:top_groups]:
        print(f"{name}\t+{added_files}/+{added_lines}\t-{removed_files}/-{removed_lines}")


def json_report(
    graph: ImportGraph,
    graphs: dict[str, set[Path]],
    time_reports: Iterable[TimeReport] = (),
    object_reports: Iterable[ObjectReport] = (),
    comparisons: Iterable[tuple[TimeReport, TimeReport]] = (),
) -> dict[str, object]:
    report: dict[str, object] = {
        "source_root": str(graph.source_root),
        "roots": {},
    }
    root_report = report["roots"]
    assert isinstance(root_report, dict)
    for name, paths in sorted(graphs.items()):
        stats = graph.stats(paths)
        root_report[name] = {"files": stats.files, "lines": stats.lines}
    available_roles = [name for name in SERVER_ROLES if name in graphs]
    role_union = set().union(*(graphs[name] for name in available_roles)) if available_roles else set()
    union_stats = graph.stats(role_union)
    report["server_role_union"] = {"files": union_stats.files, "lines": union_stats.lines}
    report["time_reports"] = {item.name: report_stats(item) for item in time_reports}
    report["aggregate_compiler_overlap"] = aggregate_overlap_stats(time_reports)
    report["object_reports"] = {item.name: object_report_stats(item) for item in object_reports}
    report["aggregate_emitted_overlap"] = aggregate_object_overlap_stats(object_reports)
    report["aggregate_named_section_overlap"] = aggregate_named_section_overlap_stats(object_reports)
    report["comparisons"] = [comparison_stats(base, candidate) for base, candidate in comparisons]
    return report


def print_report(graph: ImportGraph, graphs: dict[str, set[Path]], largest: int) -> None:
    print("root\tfiles\tlines")
    for name, paths in sorted(graphs.items(), key=lambda item: (-len(item[1]), item[0])):
        stats = graph.stats(paths)
        print(f"{name}\t{stats.files}\t{stats.lines}")

    print("\noverlap (shared files / smaller graph)")
    names = list(graphs)
    for index, left in enumerate(names):
        for right in names[index + 1 :]:
            shared = graphs[left] & graphs[right]
            denominator = min(len(graphs[left]), len(graphs[right])) or 1
            ratio = len(shared) / denominator
            if ratio >= 0.35:
                print(f"{left}\t{right}\t{len(shared)}\t{ratio:.1%}")

    available_roles = [name for name in SERVER_ROLES if name in graphs]
    role_union = set().union(*(graphs[name] for name in available_roles)) if available_roles else set()
    stats = graph.stats(role_union)
    print(f"\nserver-role union\t{stats.files} files\t{stats.lines} lines")
    for name in available_roles:
        other = set().union(*(graphs[role] for role in available_roles if role != name))
        unique = graphs[name] - other
        unique_stats = graph.stats(unique)
        print(f"unique to {name}\t{unique_stats.files} files\t{unique_stats.lines} lines")

    if largest > 0:
        for name, paths in graphs.items():
            print(f"\n[{name} largest files]")
            for path in sorted(paths, key=graph.line_count, reverse=True)[:largest]:
                print(f"{graph.line_count(path):7d} {graph.relative_name(path)}")


def show_paths(graph: ImportGraph, requests: Iterable[tuple[str, str]]) -> bool:
    all_found = True
    for source, target in requests:
        path = graph.shortest_path(graph.resolve_source(source), graph.resolve_source(target))
        print(f"\n{source} -> {target}")
        if not path:
            print("  no path")
            all_found = False
            continue
        print("  " + " -> ".join(graph.relative_name(item) for item in path))
    return all_found


def check_runtime_boundary(graph: ImportGraph) -> bool:
    public_root = graph.resolve_source("root.zig")
    clean = True
    for source_name in RUNTIME_BOUNDARIES:
        source = graph.resolve_source(source_name)
        path = graph.shortest_path(source, public_root)
        if not path:
            continue
        clean = False
        rendered = " -> ".join(graph.relative_name(item) for item in path)
        print(f"runtime boundary violation: {rendered}", file=sys.stderr)
    return clean


def check_codegen_boundary(graph: ImportGraph) -> bool:
    clean = True
    checked_boundaries = 0
    for source_name, target_name in CODEGEN_BOUNDARIES:
        source = graph.maybe_resolve_source(source_name)
        target = graph.maybe_resolve_source(target_name)
        if source is None or target is None:
            continue
        checked_boundaries += 1
        path = graph.shortest_path(source, target)
        if not path:
            continue
        clean = False
        rendered = " -> ".join(graph.relative_name(item) for item in path)
        print(f"codegen boundary violation: {rendered}", file=sys.stderr)
    if checked_boundaries == 0:
        print("codegen boundary check skipped: no boundary files found", file=sys.stderr)
    return clean


def check_api_kernel_boundary(graph: ImportGraph) -> bool:
    """Enforce the compiled API/distributed ownership boundary.

    Direct imports are safe to check lexically. Transitive lexical reachability
    and compiler ``all_files`` lists are deliberately not gates because both
    include lazy/test-only imports which do not contribute emitted code.
    """

    clean = True
    implementations = {
        path: name
        for name in API_KERNEL_IMPLEMENTATIONS
        if (path := graph.maybe_resolve_source(name)) is not None
    }
    checked_contracts = 0
    for source_name in API_KERNEL_CONTRACTS:
        source = graph.maybe_resolve_source(source_name)
        if source is None:
            continue
        checked_contracts += 1
        for target in graph.relative_edges(source):
            target_name = implementations.get(target)
            if target_name is None:
                continue
            clean = False
            print(
                f"API kernel boundary violation: {source_name} directly imports {target_name}",
                file=sys.stderr,
            )

    if checked_contracts == 0:
        print("API kernel boundary check skipped: no contract files found", file=sys.stderr)
    return clean


def check_sql_storage_boundary(graph: ImportGraph) -> bool:
    """Keep DB implementation reachability out of SQL contracts and planners."""

    db_implementation = graph.maybe_resolve_source("storage/db/mod.zig")
    rows_implementation = graph.maybe_resolve_source("storage/db/relational_rows.zig")
    sql_rows_runtime = graph.maybe_resolve_source("sql/relational_rows.zig")
    schema_contract = graph.maybe_resolve_source("schema/mod.zig")
    sql_root = graph.source_root / "sql"
    if db_implementation is None or not sql_root.is_dir():
        print("SQL storage boundary check skipped: boundary files not found", file=sys.stderr)
        return True

    clean = True
    for source in sorted(sql_root.rglob("*.zig")):
        source_name = graph.relative_name(source)
        if db_implementation not in graph.relative_edges(source):
            continue
        clean = False
        print(
            f"SQL storage boundary violation: {source_name} directly imports storage/db/mod.zig",
            file=sys.stderr,
        )

    if rows_implementation is not None:
        for source in sorted(sql_root.rglob("*.zig")):
            source_name = graph.relative_name(source)
            if source_name in SQL_STORAGE_RUNTIME_IMPORT_ALLOWLIST:
                continue
            if rows_implementation not in graph.relative_edges(source):
                continue
            clean = False
            print(
                f"SQL storage boundary violation: {source_name} directly imports storage/db/relational_rows.zig",
                file=sys.stderr,
            )

    if schema_contract is not None and sql_rows_runtime is not None and sql_rows_runtime in graph.relative_edges(schema_contract):
        clean = False
        print(
            "SQL storage boundary violation: schema/mod.zig directly imports sql/relational_rows.zig",
            file=sys.stderr,
        )
    return clean


def main(argv: list[str] | None = None) -> int:
    args = arguments(argv)
    try:
        graph = ImportGraph(args.source_root)
        roots = dict(args.root) if args.root else DEFAULT_ROOTS
        graphs = analyze(graph, roots)
        reports = {
            name: load_time_report(name, Path(path))
            for name, path in args.time_report
        }
        objects = {
            name: load_object_report(name, Path(path), graph.source_root)
            for name, path in args.object
        }
        comparisons: list[tuple[TimeReport, TimeReport]] = []
        for base_name, candidate_name in args.compare:
            if base_name not in reports or candidate_name not in reports:
                raise ValueError(
                    "comparison names must match --time-report names: "
                    f"{base_name},{candidate_name}"
                )
            comparisons.append((reports[base_name], reports[candidate_name]))
        if args.json:
            print(
                json.dumps(
                    json_report(
                        graph,
                        graphs,
                        reports.values(),
                        objects.values(),
                        comparisons,
                    ),
                    indent=2,
                    sort_keys=True,
                )
            )
        else:
            print_report(graph, graphs, args.largest)
            for report in reports.values():
                print_time_report(report, args.top_groups)
            print_aggregate_overlap(reports.values(), args.top_groups)
            for report in objects.values():
                print_object_report(report, args.top_groups)
            print_aggregate_object_overlap(objects.values(), args.top_groups)
            print_aggregate_named_section_overlap(objects.values(), args.top_groups)
            for base, candidate in comparisons:
                print_comparison(base, candidate, args.top_groups)
        if args.show_path:
            show_paths(graph, args.show_path)
        if args.check_runtime_boundary and not check_runtime_boundary(graph):
            return 1
        if args.check_codegen_boundary and not check_codegen_boundary(graph):
            return 1
        if args.check_api_kernel_boundary and not check_api_kernel_boundary(graph):
            return 1
        if args.check_sql_storage_boundary and not check_sql_storage_boundary(graph):
            return 1
        return 0
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
