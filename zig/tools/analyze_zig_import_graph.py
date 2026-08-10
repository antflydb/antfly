#!/usr/bin/env python3
"""Report static relative-import reachability in the Antfly Zig source tree.

This is intentionally a source-graph tool, not a model of Zig semantic analysis
or LLVM code generation. It follows literal relative ``@import("*.zig")``
edges, which makes accidental barrel/root imports and surprising dependency
paths visible before measuring an optimized compiler invocation.
"""

from __future__ import annotations

import argparse
import collections
import json
import re
import sys
from dataclasses import dataclass
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


@dataclass(frozen=True)
class GraphStats:
    files: int
    lines: int


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
        raise argparse.ArgumentTypeError("roots must use NAME=RELATIVE/PATH.zig")
    return name, path


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
        "--check-runtime-boundary",
        action="store_true",
        help="fail if a production runtime can statically reach the public root.zig barrel",
    )
    parser.add_argument("--largest", type=int, default=0, metavar="N", help="show the N largest files per graph")
    parser.add_argument("--json", action="store_true", help="emit the summary as JSON")
    return parser.parse_args(argv)


def analyze(graph: ImportGraph, roots: dict[str, str]) -> dict[str, set[Path]]:
    return {name: graph.closure([graph.resolve_source(path)]) for name, path in roots.items()}


def json_report(graph: ImportGraph, graphs: dict[str, set[Path]]) -> dict[str, object]:
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


def main(argv: list[str] | None = None) -> int:
    args = arguments(argv)
    try:
        graph = ImportGraph(args.source_root)
        roots = dict(args.root) if args.root else DEFAULT_ROOTS
        graphs = analyze(graph, roots)
        if args.json:
            print(json.dumps(json_report(graph, graphs), indent=2, sort_keys=True))
        else:
            print_report(graph, graphs, args.largest)
        if args.show_path:
            show_paths(graph, args.show_path)
        if args.check_runtime_boundary and not check_runtime_boundary(graph):
            return 1
        return 0
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
