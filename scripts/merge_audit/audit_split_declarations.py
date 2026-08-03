#!/usr/bin/env python3
"""Audit and prepare declaration-level merges for split Zig modules.

The tool compares a monolithic Zig source at two pinned revisions with the
current split destinations. It never edits the worktree. When requested, it
writes only unambiguous, conflict-free three-way merge candidates to a separate
directory for review.
"""

from __future__ import annotations

import argparse
import dataclasses
import difflib
import hashlib
import json
import pathlib
import posixpath
import re
import subprocess
import tempfile
import textwrap
from collections import Counter, defaultdict
from typing import Iterable

try:
    from .audit_main_capture import load_json_file
except ImportError:  # Direct script execution.
    from audit_main_capture import load_json_file


ROOT = pathlib.Path(__file__).resolve().parents[2]

FUNCTION_RE = re.compile(
    r"(?m)^(?P<indent>[ \t]*)(?:(?:pub|inline|noinline|extern|export)\s+)*"
    r"fn\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*\("
)
TEST_RE = re.compile(r'(?m)^(?P<indent>[ \t]*)test\s+"(?P<name>[^"]+)"\s*\{')
CONTAINER_RE = re.compile(
    r"(?m)^(?P<indent>[ \t]*)(?:pub\s+)?const\s+"
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*"
    r"(?P<container_kind>struct|enum|union|opaque)\b"
)
ALIAS_RE = re.compile(
    r"(?m)^(?P<indent>[ \t]*)(?:pub\s+)?const\s+"
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*"
    r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+\s*;"
)
BINDING_RE = re.compile(
    r"(?m)^(?P<indent>[ \t]*)(?:pub\s+)?(?:threadlocal\s+)?"
    r"(?P<binding_kind>const|var)\s+"
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)\b"
)
CONFLICT_RE = re.compile(r"(?m)^(?:<<<<<<<|=======|>>>>>>>)")
IMPORT_RE = re.compile(r'@import\("(?P<path>[^"]+)"\)')
INLINE_IMPORT_MEMBER_RE = re.compile(
    r'@import\("(?P<path>[^"]+)"\)(?=\s*\.)'
)

REVIEWABLE_RESOLUTION_STATUSES = {
    "added_name_collision",
    "clean_candidate",
    "three_way_conflict",
    "container_fields_diverged",
    "container_variants_diverged",
}
IMPORT_BINDING_RE = re.compile(
    r'(?m)^[ \t]*(?:pub\s+)?const\s+'
    r'(?P<binding>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*'
    r'@import\("(?P<path>[^"]+)"\)\s*;'
)
QUALIFIED_BINDING_ALIAS_RE = re.compile(
    r"(?s)^\s*(?:pub\s+)?const\s+[A-Za-z_][A-Za-z0-9_]*\s*=\s*"
    r"(?P<module>[A-Za-z_][A-Za-z0-9_]*)\."
    r"(?P<member>[A-Za-z_][A-Za-z0-9_]*)\s*;\s*$"
)


@dataclasses.dataclass(frozen=True)
class Declaration:
    kind: str
    name: str
    body: str
    indent: int
    path: str
    start: int
    end: int
    owner: str | None = None

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.body.encode()).hexdigest()


@dataclasses.dataclass
class Obligation:
    key: str
    kind: str
    name: str
    change: str
    status: str
    base_sha256: str | None
    incoming_sha256: str | None
    base_line: int | None = None
    incoming_line: int | None = None
    current_path: str | None = None
    current_sha256: str | None = None
    similarity: float | None = None
    second_similarity: float | None = None
    detail: str = ""
    candidate: str | None = None
    owner: str | None = None
    current_owner: str | None = None
    suggested_path: str | None = None
    previous_anchor: str | None = None
    next_anchor: str | None = None
    base_body: str | None = None
    incoming_body: str | None = None
    current_body: str | None = None


@dataclasses.dataclass(frozen=True)
class CandidateEdit:
    key: str
    old: str
    new: str
    source_order: int
    start: int = -1
    end: int = -1
    insert_before: bool = False


def run_git(args: list[str], cwd: pathlib.Path = ROOT) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {proc.stderr.strip()}")
    return proc.stdout


def resolve_ref(ref: str) -> str:
    return run_git(["rev-parse", "--verify", f"{ref}^{{commit}}"]).strip()


def ref_text(ref: str, path: str) -> str:
    return run_git(["show", f"{ref}:{path}"])


def normalized(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


ZIG_TOKEN_RE = re.compile(
    r'"(?:\\.|[^"\\])*"'
    r"|'(?:\\.|[^'\\])*'"
    r"|[A-Za-z_][A-Za-z0-9_]*"
    r"|(?:0[xob])?[0-9][A-Za-z0-9_.]*"
    r"|[^\s]"
)


def normalized_zig_tokens(text: str) -> str:
    tokens = ZIG_TOKEN_RE.findall(text)
    tokens = [
        token
        for index, token in enumerate(tokens)
        if not (
            token == ","
            and index + 1 < len(tokens)
            and tokens[index + 1] in {")", "]", "}"}
        )
    ]
    return "\x1f".join(tokens)


def normalized_split_visibility(text: str) -> str:
    without_pub = re.sub(
        r"(?m)^(?P<indent>[ \t]*)pub\s+(?=(?:fn|const|var)\b)",
        r"\g<indent>",
        text,
    )
    return normalized_zig_tokens(without_pub)


def resolved_import_path(declaration: Declaration) -> str | None:
    match = IMPORT_RE.search(declaration.body)
    if match is None:
        return None
    imported = match.group("path")
    if not imported.startswith(".") and not imported.endswith(".zig"):
        return imported
    return posixpath.normpath(
        posixpath.join(posixpath.dirname(declaration.path), imported)
    )


def canonical_module_path(
    path: str,
    module_path_migrations: dict[str, list[str]] | None = None,
) -> str:
    for source, destinations in (module_path_migrations or {}).items():
        if path == source or path in destinations:
            return source
    return path


def normalized_import_binding(
    declaration: Declaration,
    module_path_migrations: dict[str, list[str]] | None = None,
) -> str | None:
    resolved = resolved_import_path(declaration)
    if resolved is None:
        return None
    resolved = canonical_module_path(resolved, module_path_migrations)
    return normalized_split_visibility(
        IMPORT_RE.sub(f'@import("{resolved}")', declaration.body, count=1)
    )


def normalized_module_references(
    declaration: Declaration,
    import_paths: dict[tuple[str, str], str],
    module_path_migrations: dict[str, list[str]] | None = None,
    symbol_call_migrations: dict[str, list[str]] | None = None,
    symbol_reference_migrations: dict[str, list[str]] | None = None,
) -> str:
    def canonical_inline(match: re.Match[str]) -> str:
        imported = match.group("path")
        if not imported.startswith(".") and not imported.endswith(".zig"):
            resolved = imported
        else:
            resolved = posixpath.normpath(
                posixpath.join(posixpath.dirname(declaration.path), imported)
            )
        return f'@import("{canonical_module_path(resolved, module_path_migrations)}")'

    body = declaration.body
    body = re.sub(
        r"\bSelf\.([A-Za-z_][A-Za-z0-9_]*)\(\s*self\s*,",
        r"self.\1(",
        body,
    )
    body = re.sub(
        r"\bSelf\.([A-Za-z_][A-Za-z0-9_]*)\(\s*self\s*\)",
        r"self.\1()",
        body,
    )
    body = re.sub(
        r"\bSelf\.([A-Za-z_][A-Za-z0-9_]*)\(",
        r"\1(",
        body,
    )
    for source, targets in (symbol_reference_migrations or {}).items():
        canonical = f"__merge_audit_symbol_{source}"
        for symbol in sorted([source, *targets], key=len, reverse=True):
            body = re.sub(
                rf"(?<![A-Za-z0-9_]){re.escape(symbol)}(?![A-Za-z0-9_])",
                canonical,
                body,
            )
    # Canonicalize moved helper calls before expanding their module aliases.
    # Once an alias becomes @import("...") the manifest's exact call symbol no
    # longer appears as a contiguous token sequence.
    for source, targets in (symbol_call_migrations or {}).items():
        canonical = f"__merge_audit_call_{source}"
        for symbol in sorted([source, *targets], key=len, reverse=True):
            body = re.sub(
                rf"(?<![A-Za-z0-9_]){re.escape(symbol)}(?=\s*\()",
                canonical,
                body,
            )
    body = INLINE_IMPORT_MEMBER_RE.sub(canonical_inline, body)
    import_placeholders: list[tuple[str, str]] = []
    for (path, binding), resolved in import_paths.items():
        if path != declaration.path:
            continue
        placeholder = f"__merge_audit_import_{len(import_placeholders)}"
        body = re.sub(
            rf"\b{re.escape(binding)}(?=\s*\.)",
            placeholder,
            body,
        )
        import_placeholders.append(
            (
                placeholder,
                f'@import("{canonical_module_path(resolved, module_path_migrations)}")',
            )
        )
    # `_1` is a prefix of `_10`; restore longer placeholders first so a file
    # with ten or more imports cannot corrupt a later placeholder.
    for placeholder, replacement in sorted(
        import_placeholders,
        key=lambda item: len(item[0]),
        reverse=True,
    ):
        body = body.replace(placeholder, replacement)
    return normalized_split_visibility(body)


def normalized_binding_definition(
    declaration: Declaration,
    import_paths: dict[tuple[str, str], str] | None = None,
    module_path_migrations: dict[str, list[str]] | None = None,
    symbol_reference_migrations: dict[str, list[str]] | None = None,
) -> str:
    without_pub = re.sub(
        r"(?m)^(?P<indent>[ \t]*)pub\s+(?=(?:const|var)\b)",
        r"\g<indent>",
        declaration.body,
    )
    without_name = re.sub(
        r"(?m)^([ \t]*(?:(?:threadlocal)\s+)?(?:const|var)\s+)"
        r"[A-Za-z_][A-Za-z0-9_]*",
        r"\1__split_binding__",
        without_pub,
        count=1,
    )
    synthetic = dataclasses.replace(declaration, body=without_name)
    return normalized_module_references(
        synthetic,
        import_paths or {},
        module_path_migrations,
        symbol_reference_migrations=symbol_reference_migrations,
    )


def qualified_binding_alias(declaration: Declaration) -> tuple[str, str] | None:
    if declaration.kind != "binding":
        return None
    match = QUALIFIED_BINDING_ALIAS_RE.match(declaration.body)
    if match is None:
        return None
    return match.group("module"), match.group("member")


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def normalized_lines(text: str) -> list[str]:
    return [
        normalized(line)
        for line in text.splitlines()
        if normalized(line)
    ]


LARGE_DECLARATION_LINE_THRESHOLD = 2_000


def similarity(left: str, right: str) -> float:
    left_lines = normalized_lines(left)
    right_lines = normalized_lines(right)
    # Disabling autojunk gives better scores for ordinary Zig declarations,
    # but becomes quadratic on very large brace-heavy functions and tests.
    # Names and owners already constrain candidates, so the frequency heuristic
    # is safe for ranking declarations above this size.
    autojunk = max(len(left_lines), len(right_lines)) > LARGE_DECLARATION_LINE_THRESHOLD
    return difflib.SequenceMatcher(
        a=left_lines,
        b=right_lines,
        autojunk=autojunk,
    ).ratio()


def zig_multiline_string_starts(text: str, index: int) -> bool:
    if text[index:index + 2] != "\\\\":
        return False
    line_start = text.rfind("\n", 0, index) + 1
    return not text[line_start:index].strip()


def next_code_delimiter(
    text: str,
    start: int,
    delimiters: set[str],
) -> tuple[int, str] | None:
    index = start
    state = "code"
    block_comment_depth = 0
    while index < len(text):
        ch = text[index]
        nxt = text[index + 1] if index + 1 < len(text) else ""
        if state == "code":
            if ch == "/" and nxt == "/":
                state = "line_comment"
                index += 2
                continue
            if ch == "/" and nxt == "*":
                state = "block_comment"
                block_comment_depth = 1
                index += 2
                continue
            if ch == "\\" and nxt == "\\" and zig_multiline_string_starts(text, index):
                state = "multiline_string"
                index += 2
                continue
            if ch == '"':
                state = "string"
            elif ch == "'":
                state = "char"
            elif ch in delimiters:
                return index, ch
        elif state in {"line_comment", "multiline_string"}:
            if ch == "\n":
                state = "code"
        elif state == "block_comment":
            if ch == "/" and nxt == "*":
                block_comment_depth += 1
                index += 2
                continue
            if ch == "*" and nxt == "/":
                block_comment_depth -= 1
                index += 2
                if block_comment_depth == 0:
                    state = "code"
                continue
        elif state in {"string", "char"}:
            if ch == "\\":
                index += 2
                continue
            if (state == "string" and ch == '"') or (
                state == "char" and ch == "'"
            ):
                state = "code"
        index += 1
    return None


def matching_brace(text: str, start: int) -> int | None:
    depth = 0
    index = start
    state = "code"
    block_comment_depth = 0
    while index < len(text):
        ch = text[index]
        nxt = text[index + 1] if index + 1 < len(text) else ""
        if state == "code":
            if ch == "/" and nxt == "/":
                state = "line_comment"
                index += 2
                continue
            if ch == "/" and nxt == "*":
                state = "block_comment"
                block_comment_depth = 1
                index += 2
                continue
            if ch == "\\" and nxt == "\\" and zig_multiline_string_starts(text, index):
                state = "multiline_string"
                index += 2
                continue
            if ch == '"':
                state = "string"
            elif ch == "'":
                state = "char"
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return index + 1
        elif state in {"line_comment", "multiline_string"}:
            if ch == "\n":
                state = "code"
        elif state == "block_comment":
            if ch == "/" and nxt == "*":
                block_comment_depth += 1
                index += 2
                continue
            if ch == "*" and nxt == "/":
                block_comment_depth -= 1
                index += 2
                if block_comment_depth == 0:
                    state = "code"
                continue
        elif state in {"string", "char"}:
            if ch == "\\":
                index += 2
                continue
            if (state == "string" and ch == '"') or (
                state == "char" and ch == "'"
            ):
                state = "code"
        index += 1
    return None


def function_body_brace(text: str, start: int) -> int | None:
    paren_depth = 1
    bracket_depth = 0
    index = start
    state = "code"
    block_comment_depth = 0
    while index < len(text):
        ch = text[index]
        nxt = text[index + 1] if index + 1 < len(text) else ""
        if state == "code":
            if ch == "/" and nxt == "/":
                state = "line_comment"
                index += 2
                continue
            if ch == "/" and nxt == "*":
                state = "block_comment"
                block_comment_depth = 1
                index += 2
                continue
            if ch == "\\" and nxt == "\\" and zig_multiline_string_starts(text, index):
                state = "multiline_string"
                index += 2
                continue
            if ch == '"':
                state = "string"
            elif ch == "'":
                state = "char"
            elif ch == "(":
                paren_depth += 1
            elif ch == ")":
                if paren_depth == 0:
                    return None
                paren_depth -= 1
            elif ch == "[":
                bracket_depth += 1
            elif ch == "]":
                if bracket_depth == 0:
                    return None
                bracket_depth -= 1
            elif ch == "{" and paren_depth == 0 and bracket_depth == 0:
                prefix = text[:index].rstrip()
                identifier_match = re.search(
                    r"([A-Za-z_][A-Za-z0-9_]*)$",
                    prefix,
                )
                if (
                    identifier_match is not None
                    and identifier_match.group(1)
                    in {"enum", "error", "opaque", "struct", "union"}
                ):
                    end = matching_brace(text, index)
                    if end is None:
                        return None
                    index = end
                    continue
                return index
            elif ch == ";" and paren_depth == 0 and bracket_depth == 0:
                return None
        elif state in {"line_comment", "multiline_string"}:
            if ch == "\n":
                state = "code"
        elif state == "block_comment":
            if ch == "/" and nxt == "*":
                block_comment_depth += 1
                index += 2
                continue
            if ch == "*" and nxt == "/":
                block_comment_depth -= 1
                index += 2
                if block_comment_depth == 0:
                    state = "code"
                continue
        elif state in {"string", "char"}:
            if ch == "\\":
                index += 2
                continue
            if (state == "string" and ch == '"') or (
                state == "char" and ch == "'"
            ):
                state = "code"
        index += 1
    return None


def terminating_semicolon(text: str, start: int) -> int | None:
    depths = {"(": 0, "[": 0, "{": 0}
    closing = {")": "(", "]": "[", "}": "{"}
    index = start
    state = "code"
    block_comment_depth = 0
    while index < len(text):
        ch = text[index]
        nxt = text[index + 1] if index + 1 < len(text) else ""
        if state == "code":
            if ch == "/" and nxt == "/":
                state = "line_comment"
                index += 2
                continue
            if ch == "/" and nxt == "*":
                state = "block_comment"
                block_comment_depth = 1
                index += 2
                continue
            if ch == "\\" and nxt == "\\" and zig_multiline_string_starts(text, index):
                state = "multiline_string"
                index += 2
                continue
            if ch == '"':
                state = "string"
            elif ch == "'":
                state = "char"
            elif ch in depths:
                depths[ch] += 1
            elif ch in closing:
                opener = closing[ch]
                if depths[opener] == 0:
                    return None
                depths[opener] -= 1
            elif ch == ";" and all(depth == 0 for depth in depths.values()):
                return index + 1
        elif state in {"line_comment", "multiline_string"}:
            if ch == "\n":
                state = "code"
        elif state == "block_comment":
            if ch == "/" and nxt == "*":
                block_comment_depth += 1
                index += 2
                continue
            if ch == "*" and nxt == "/":
                block_comment_depth -= 1
                index += 2
                if block_comment_depth == 0:
                    state = "code"
                continue
        elif state in {"string", "char"}:
            if ch == "\\":
                index += 2
                continue
            if (state == "string" and ch == '"') or (
                state == "char" and ch == "'"
            ):
                state = "code"
        index += 1
    return None


def extract_declarations(
    text: str,
    path: str,
    kind: str,
    pattern: re.Pattern[str],
    max_indent: int | None,
) -> list[Declaration]:
    declarations: list[Declaration] = []
    for match in pattern.finditer(text):
        indent = len(match.group("indent").replace("\t", "    "))
        if max_indent is not None and indent > max_indent:
            continue
        if kind == "function":
            open_brace = function_body_brace(text, match.end())
            if open_brace is None:
                continue
        elif match.end() > match.start() and text[match.end() - 1] == "{":
            open_brace = match.end() - 1
        else:
            delimiter = next_code_delimiter(text, match.end(), {"{", ";"})
            if delimiter is None or delimiter[1] == ";":
                continue
            open_brace = delimiter[0]
        end = matching_brace(text, open_brace)
        if end is None:
            continue
        if kind == "container":
            semicolon = end
            while semicolon < len(text) and text[semicolon] in " \t":
                semicolon += 1
            if semicolon < len(text) and text[semicolon] == ";":
                end = semicolon + 1
        if end < len(text) and text[end] == "\n":
            end += 1
        declarations.append(
            Declaration(
                kind=kind,
                name=match.group("name"),
                body=text[match.start():end],
                indent=indent,
                path=path,
                start=match.start(),
                end=end,
            )
        )
    return declarations


def extract_aliases(text: str, path: str) -> list[Declaration]:
    aliases: list[Declaration] = []
    for match in ALIAS_RE.finditer(text):
        end = match.end()
        if end < len(text) and text[end] == "\n":
            end += 1
        aliases.append(
            Declaration(
                kind="alias",
                name=match.group("name"),
                body=text[match.start():end],
                indent=len(match.group("indent").replace("\t", "    ")),
                path=path,
                start=match.start(),
                end=end,
            )
        )
    return aliases


def extract_bindings(
    text: str,
    path: str,
    max_indent: int | None,
) -> list[Declaration]:
    bindings: list[Declaration] = []
    for match in BINDING_RE.finditer(text):
        indent = len(match.group("indent").replace("\t", "    "))
        if max_indent is not None and indent > max_indent:
            continue
        end = terminating_semicolon(text, match.end())
        if end is None:
            continue
        if end < len(text) and text[end] == "\n":
            end += 1
        bindings.append(
            Declaration(
                kind="binding",
                name=match.group("name"),
                body=text[match.start():end],
                indent=indent,
                path=path,
                start=match.start(),
                end=end,
            )
        )
    return bindings


def all_declarations(
    text: str,
    path: str,
    max_indent: int | None = None,
) -> list[Declaration]:
    tests = extract_declarations(text, path, "test", TEST_RE, max_indent)
    test_ranges = [(item.start, item.end) for item in tests]
    functions = extract_declarations(
        text, path, "function", FUNCTION_RE, max_indent
    )
    function_ranges = [(item.start, item.end) for item in functions]
    containers = extract_declarations(
        text, path, "container", CONTAINER_RE, max_indent
    )
    container_starts = {item.start for item in containers}
    bindings = extract_bindings(text, path, max_indent)
    declarations: list[Declaration] = list(tests)
    for declaration in functions:
        if any(start <= declaration.start < end for start, end in test_ranges):
            continue
        if any(
            start < declaration.start < end
            for start, end in function_ranges
        ):
            continue
        declarations.append(declaration)
    for declaration in containers:
        if any(start <= declaration.start < end for start, end in test_ranges):
            continue
        if any(
            start <= declaration.start < end
            for start, end in function_ranges
        ):
            continue
        declarations.append(declaration)
    for declaration in bindings:
        if declaration.start in container_starts:
            continue
        if any(start <= declaration.start < end for start, end in test_ranges):
            continue
        if any(
            start <= declaration.start < end
            for start, end in function_ranges
        ):
            continue
        declarations.append(declaration)

    owned: list[Declaration] = []
    for declaration in declarations:
        owners = [
            container
            for container in containers
            if container.start < declaration.start < container.end
        ]
        owner = max(owners, key=lambda item: item.start).name if owners else None
        owned.append(dataclasses.replace(declaration, owner=owner))
    return owned


def mixin_declarations(
    text: str,
    path: str,
    factory_name: str,
    target_owner: str,
) -> list[Declaration]:
    factories = [
        declaration
        for declaration in all_declarations(text, path, max_indent=0)
        if declaration.kind == "function"
        and declaration.name == factory_name
    ]
    if len(factories) != 1:
        raise ValueError(
            f"{path}: expected exactly one top-level mixin factory "
            f"{factory_name!r}, found {len(factories)}"
        )
    factory = factories[0]
    returned = re.search(r"\breturn\s+struct\s*\{", factory.body)
    if returned is None:
        raise ValueError(
            f"{path}: mixin factory {factory_name!r} must return struct"
        )
    relative_brace = factory.body.find("{", returned.start())
    brace_start = factory.start + relative_brace
    brace_end = matching_brace(text, brace_start)
    if brace_end is None or brace_end > factory.end:
        raise ValueError(
            f"{path}: unterminated returned struct in mixin "
            f"{factory_name!r}"
        )
    body_start = brace_start + 1
    body = text[body_start : brace_end - 1]
    declarations = all_declarations(body, path)
    return [
        dataclasses.replace(
            declaration,
            owner=(
                target_owner
                if declaration.owner is None
                else declaration.owner
            ),
            start=declaration.start + body_start,
            end=declaration.end + body_start,
        )
        for declaration in declarations
    ]


def augment_with_manifest_mixins(
    declarations: list[Declaration],
    current_text_by_path: dict[str, str],
    rules: list[dict[str, str]],
    forced_targets: set[tuple[str, str, str, str | None]] | None = None,
) -> list[Declaration]:
    forced_targets = forced_targets or set()
    normal_identities = {
        (declaration.kind, declaration.name, declaration.owner)
        for declaration in declarations
    }
    extracted: list[Declaration] = []
    for rule in rules:
        path = rule["path"]
        text = current_text_by_path.get(path)
        if text is None:
            raise ValueError(
                f"mixin path is not a split destination: {path}"
            )
        extracted.extend(
            mixin_declarations(
                text,
                path,
                rule["factory"],
                rule["owner"],
            )
        )
    declarations.extend(
        declaration
        for declaration in extracted
        if (
            declaration.kind,
            declaration.name,
            declaration.owner,
        )
        not in normal_identities
        or (
            declaration.path,
            declaration.kind,
            declaration.name,
            declaration.owner,
        )
        in forced_targets
    )
    return declarations


def expand_destinations(
    raw_paths: list[str],
    destination_ref: str | None = None,
) -> list[pathlib.Path]:
    files: set[pathlib.Path] = set()
    missing_at_ref: list[str] = []
    for raw in raw_paths:
        pure = pathlib.PurePosixPath(raw)
        if pure.is_absolute() or ".." in pure.parts:
            raise ValueError(f"destination escapes repository: {raw}")
        if destination_ref is not None:
            names = [
                line
                for line in run_git(
                    [
                        "ls-tree",
                        "-r",
                        "--name-only",
                        destination_ref,
                        "--",
                        raw,
                    ]
                ).splitlines()
                if line.endswith(".zig")
            ]
            if not names:
                missing_at_ref.append(raw)
                continue
            files.update(ROOT / name for name in names)
            continue
        path = (ROOT / raw).resolve()
        try:
            path.relative_to(ROOT)
        except ValueError as exc:
            raise ValueError(f"destination escapes repository: {raw}") from exc
        if path.is_dir():
            files.update(path.rglob("*.zig"))
        elif path.is_file():
            files.add(path)
        else:
            raise ValueError(f"destination does not exist: {raw}")
    if destination_ref is not None and not files:
        raise ValueError(
            f"no destinations exist at {destination_ref}: "
            + ", ".join(missing_at_ref)
        )
    return sorted(files)


def group_by_name(
    declarations: Iterable[Declaration],
) -> dict[tuple[str, str], list[Declaration]]:
    grouped: dict[tuple[str, str], list[Declaration]] = defaultdict(list)
    for declaration in declarations:
        grouped[(declaration.kind, declaration.name)].append(declaration)
    return grouped


def declaration_identity(declaration: Declaration) -> tuple[str, str, str | None]:
    return declaration.kind, declaration.name, declaration.owner


def canonical_declaration_name(
    name: str | None,
    declaration_name_aliases: dict[str, list[str]] | None,
) -> str | None:
    if name is None:
        return None
    for source, aliases in (declaration_name_aliases or {}).items():
        if name == source or name in aliases:
            return source
    return name


def canonical_declaration_identity(
    declaration: Declaration,
    declaration_name_aliases: dict[str, list[str]] | None,
    test_name_aliases: dict[str, list[str]] | None = None,
) -> tuple[str, str, str | None]:
    name_aliases = dict(declaration_name_aliases or {})
    if declaration.kind == "test":
        name_aliases.update(test_name_aliases or {})
    return (
        declaration.kind,
        canonical_declaration_name(
            declaration.name,
            name_aliases,
        ) or declaration.name,
        canonical_declaration_name(
            declaration.owner,
            declaration_name_aliases,
        ),
    )


def candidate_names(
    declaration: Declaration,
    test_name_aliases: dict[str, list[str]],
    declaration_name_aliases: dict[str, list[str]] | None = None,
) -> list[str]:
    names = [declaration.name]
    if declaration.kind == "test":
        names.extend(test_name_aliases.get(declaration.name, []))
    names.extend((declaration_name_aliases or {}).get(declaration.name, []))
    return names


def rewritten_test_names(
    declaration: Declaration,
    rules: list[dict[str, str]],
) -> list[tuple[str, str]]:
    if declaration.kind != "test":
        return []
    rewritten: list[tuple[str, str]] = []
    for rule in rules:
        source_prefix = rule["source_prefix"]
        if not declaration.name.startswith(source_prefix):
            continue
        rewritten.append(
            (
                rule["destination_prefix"]
                + declaration.name[len(source_prefix):],
                rule["path"],
            )
        )
    return rewritten


def declaration_name_adapted_body(
    declaration: Declaration,
    canonical_name: str,
) -> str:
    if declaration.name == canonical_name:
        return declaration.body
    if declaration.kind == "test":
        pattern = re.compile(
            rf'(?m)^([ \t]*test\s+")'
            rf"{re.escape(declaration.name)}"
            rf'("\s*\{{)',
        )
    elif declaration.kind == "function":
        pattern = re.compile(
            rf"(?m)^([ \t]*(?:(?:pub|inline|noinline|extern|export)\s+)*"
            rf"fn\s+){re.escape(declaration.name)}(?=\s*\()"
        )
    elif declaration.kind in {"binding", "container", "alias"}:
        pattern = re.compile(
            rf"(?m)^([ \t]*(?:pub\s+)?(?:threadlocal\s+)?"
            rf"(?:const|var)\s+){re.escape(declaration.name)}\b"
        )
    else:
        return declaration.body
    body, count = pattern.subn(
        lambda match: match.group(1)
        + canonical_name
        + (match.group(2) if match.lastindex == 2 else ""),
        declaration.body,
        count=1,
    )
    if count != 1:
        raise ValueError(
            f"{declaration.path}: cannot canonicalize parsed "
            f"{declaration.kind} {declaration.name!r}"
        )
    return body


def declaration_for_match(
    declaration: Declaration,
    incoming: Declaration,
    owner_migration: dict[str, object] | None = None,
) -> Declaration:
    adapt_name = declaration.kind == "test" and incoming.kind == "test"
    if owner_migration is not None and "name" in owner_migration:
        adapt_name = True
    if not adapt_name:
        return declaration
    return dataclasses.replace(
        declaration,
        name=incoming.name,
        body=declaration_name_adapted_body(declaration, incoming.name),
    )


def declaration_for_test_match(
    declaration: Declaration,
    incoming: Declaration,
) -> Declaration:
    return declaration_for_match(declaration, incoming)


def test_body_fingerprint(declaration: Declaration) -> str | None:
    if declaration.kind != "test":
        return None
    canonical = declaration_name_adapted_body(
        declaration,
        "__merge_audit_test_name__",
    )
    return normalized_split_visibility(canonical)


def restore_split_declaration_name(
    body: str,
    incoming: Declaration,
    current: Declaration,
    owner_migration: dict[str, object] | None = None,
) -> str:
    adapt_name = incoming.kind == "test" and current.kind == "test"
    if owner_migration is not None and "name" in owner_migration:
        adapt_name = True
    if not adapt_name or incoming.name == current.name:
        return body
    canonical = dataclasses.replace(
        current,
        name=incoming.name,
        body=body,
    )
    return declaration_name_adapted_body(canonical, current.name)


def restore_split_test_name(
    body: str,
    incoming: Declaration,
    current: Declaration,
) -> str:
    return restore_split_declaration_name(body, incoming, current)


def matching_owner_candidates(
    incoming: Declaration,
    candidates: list[Declaration],
    owner_migration: dict[str, object] | None = None,
    declaration_name_aliases: dict[str, list[str]] | None = None,
) -> list[Declaration]:
    if owner_migration is not None:
        target_owner = owner_migration.get("owner")
        target_path = owner_migration.get("path")
        target_name = owner_migration.get("name", incoming.name)
        target_kind = owner_migration.get("kind", incoming.kind)
        return [
            candidate
            for candidate in candidates
            if candidate.owner == target_owner
            and candidate.path == target_path
            and candidate.name == target_name
            and (
                candidate.kind == target_kind
                or (incoming.kind == "container" and candidate.kind == "alias")
            )
        ]
    if incoming.owner is None:
        return candidates
    accepted_owners = {
        incoming.owner,
        *(declaration_name_aliases or {}).get(incoming.owner, []),
    }
    return [
        candidate
        for candidate in candidates
        if candidate.owner in accepted_owners
    ]


def pair_declarations(
    base: list[Declaration],
    incoming: list[Declaration],
    include_unchanged: bool,
    declaration_name_aliases: dict[str, list[str]] | None = None,
    test_name_aliases: dict[str, list[str]] | None = None,
) -> list[tuple[Declaration | None, Declaration, int]]:
    base_by_name: dict[tuple[str, str, str | None], list[Declaration]] = defaultdict(list)
    incoming_by_name: dict[tuple[str, str, str | None], list[Declaration]] = defaultdict(list)
    for declaration in base:
        base_by_name[
            canonical_declaration_identity(
                declaration,
                declaration_name_aliases,
                test_name_aliases,
            )
        ].append(declaration)
    for declaration in incoming:
        incoming_by_name[
            canonical_declaration_identity(
                declaration,
                declaration_name_aliases,
                test_name_aliases,
            )
        ].append(declaration)
    changed: list[tuple[Declaration | None, Declaration, int]] = []
    for name_key, incoming_items in sorted(
        incoming_by_name.items(),
        key=lambda item: (
            item[0][0],
            item[0][1],
            item[0][2] or "",
        ),
    ):
        remaining = list(base_by_name.get(name_key, []))
        for ordinal, incoming_item in enumerate(incoming_items, start=1):
            base_item: Declaration | None = None
            if len(remaining) == 1:
                base_item = remaining.pop()
            elif remaining:
                base_item = max(
                    remaining,
                    key=lambda item: similarity(item.body, incoming_item.body),
                )
                remaining.remove(base_item)
            if (
                not include_unchanged
                and base_item is not None
                and normalized(base_item.body) == normalized(incoming_item.body)
            ):
                continue
            changed.append((base_item, incoming_item, ordinal))
    return changed


def pair_changed_declarations(
    base: list[Declaration],
    incoming: list[Declaration],
) -> list[tuple[Declaration | None, Declaration, int]]:
    return pair_declarations(base, incoming, include_unchanged=False)


def removed_declarations(
    base: list[Declaration],
    incoming: list[Declaration],
    declaration_name_aliases: dict[str, list[str]] | None = None,
    test_name_aliases: dict[str, list[str]] | None = None,
) -> list[tuple[Declaration, int]]:
    """Return base declarations with no same-identity incoming partner."""
    base_by_name: dict[
        tuple[str, str, str | None], list[Declaration]
    ] = defaultdict(list)
    incoming_by_name: dict[
        tuple[str, str, str | None], list[Declaration]
    ] = defaultdict(list)
    for declaration in base:
        base_by_name[
            canonical_declaration_identity(
                declaration,
                declaration_name_aliases,
                test_name_aliases,
            )
        ].append(declaration)
    for declaration in incoming:
        incoming_by_name[
            canonical_declaration_identity(
                declaration,
                declaration_name_aliases,
                test_name_aliases,
            )
        ].append(declaration)

    removed: list[tuple[Declaration, int]] = []
    for name_key, base_items in sorted(
        base_by_name.items(),
        key=lambda item: (
            item[0][0],
            item[0][1],
            item[0][2] or "",
        ),
    ):
        remaining = list(base_items)
        for incoming_item in incoming_by_name.get(name_key, []):
            if len(remaining) == 1:
                remaining.pop()
            elif remaining:
                base_item = max(
                    remaining,
                    key=lambda item: similarity(item.body, incoming_item.body),
                )
                remaining.remove(base_item)
        for base_item in remaining:
            removed.append((base_item, base_items.index(base_item) + 1))
    return removed


def merge_declarations(
    current: Declaration,
    base: Declaration,
    incoming: Declaration,
) -> tuple[int, str]:
    inputs = [
        textwrap.dedent(item.body)
        for item in (current, base, incoming)
    ]
    with tempfile.TemporaryDirectory(prefix="antfly-split-decl-") as raw_dir:
        directory = pathlib.Path(raw_dir)
        paths = [
            directory / name
            for name in ("current.zig", "base.zig", "incoming.zig")
        ]
        for path, body in zip(paths, inputs, strict=True):
            path.write_text(body)
        proc = subprocess.run(
            ["git", "merge-file", "-p", *(str(path) for path in paths)],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    if proc.returncode < 0 or proc.returncode == 255:
        raise RuntimeError(f"git merge-file failed: {proc.stderr.strip()}")
    merged = textwrap.indent(proc.stdout.rstrip("\n"), " " * current.indent) + "\n"
    return proc.returncode, merged


def merge_preserves_current(current: Declaration, merged: str) -> bool:
    return normalized(current.body) == normalized(merged)


def canonicalized_merge_declaration(
    declaration: Declaration,
    import_paths: dict[tuple[str, str], str],
    module_path_migrations: dict[str, list[str]],
    symbol_call_migrations: dict[str, list[str]],
    symbol_reference_migrations: dict[str, list[str]],
) -> Declaration:
    canonical_tokens = normalized_module_references(
        declaration,
        import_paths,
        module_path_migrations,
        symbol_call_migrations,
        symbol_reference_migrations,
    ).split("\x1f")
    return dataclasses.replace(
        declaration,
        body="\n".join(canonical_tokens) + "\n",
        indent=0,
    )


def canonicalized_incoming_is_preserved(
    current: Declaration,
    base: Declaration,
    incoming: Declaration,
) -> bool:
    current_tokens = current.body.split()
    base_tokens = base.body.split()
    incoming_tokens = incoming.body.split()

    cursor = 0
    for token in current_tokens:
        if cursor < len(incoming_tokens) and token == incoming_tokens[cursor]:
            cursor += 1
    if cursor != len(incoming_tokens):
        return False

    base_counts = Counter(base_tokens)
    incoming_counts = Counter(incoming_tokens)
    current_counts = Counter(current_tokens)
    for token, base_count in base_counts.items():
        if not (token[0].isalnum() or token[0] in {'"', "'", '_'}):
            continue
        if (
            base_count > incoming_counts[token]
            and current_counts[token] > incoming_counts[token]
        ):
            return False
    return True


def reindent_declaration(body: str, indent: int) -> str:
    return textwrap.indent(textwrap.dedent(body).rstrip("\n"), " " * indent) + "\n"


def direct_container_field_lines(
    declaration: Declaration,
    import_paths: dict[tuple[str, str], str] | None = None,
    module_path_migrations: dict[str, list[str]] | None = None,
    symbol_reference_migrations: dict[str, list[str]] | None = None,
) -> dict[str, str]:
    body = declaration.body
    container_indent = declaration.indent
    if declaration.kind == "function":
        returned = re.search(
            r"(?m)^(?P<indent>[ \t]*)return\s+struct\b",
            declaration.body,
        )
        if returned is None:
            return {}
        open_brace = declaration.body.find("{", returned.end())
        if open_brace < 0:
            return {}
        end = matching_brace(declaration.body, open_brace)
        if end is None:
            return {}
        body = declaration.body[returned.start():end]
        container_indent = len(
            returned.group("indent").replace("\t", "    ")
        )
        open_brace = body.find("{")
    else:
        open_brace = body.find("{")
    if open_brace < 0:
        return {}
    fields: dict[str, str] = {}
    field_indent = container_indent + 4
    for line in body[open_brace + 1:].splitlines():
        expanded = line.expandtabs(4)
        indentation = len(expanded) - len(expanded.lstrip(" "))
        if indentation != field_indent:
            continue
        match = re.match(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s*:", line)
        if match:
            if (
                import_paths
                or module_path_migrations
                or symbol_reference_migrations
            ):
                synthetic = dataclasses.replace(declaration, body=line)
                value = normalized_module_references(
                    synthetic,
                    import_paths or {},
                    module_path_migrations,
                    symbol_reference_migrations=symbol_reference_migrations,
                )
            else:
                value = normalized(line)
            field_name = match.group(1)
            for source, aliases in (symbol_reference_migrations or {}).items():
                if field_name == source or field_name in aliases:
                    field_name = source
                    break
            fields[field_name] = value
    return fields


def direct_container_fields(declaration: Declaration) -> set[str]:
    return set(direct_container_field_lines(declaration))


def direct_container_variant_lines(declaration: Declaration) -> dict[str, str]:
    if declaration.kind != "container":
        return {}
    kind_match = re.search(
        r"=\s*(?P<kind>struct|enum|union|opaque)\b",
        declaration.body,
    )
    if kind_match is None or kind_match.group("kind") not in {"enum", "union"}:
        return {}
    open_brace = declaration.body.find("{", kind_match.end())
    if open_brace < 0:
        return {}

    variants: dict[str, str] = {}
    member_indent = declaration.indent + 4
    variant_re = re.compile(
        r"\s*([A-Za-z_][A-Za-z0-9_]*)"
        r"(?:\s*=\s*[^,]+)?\s*,(?:\s*//.*)?$"
    )
    for line in declaration.body[open_brace + 1:].splitlines():
        expanded = line.expandtabs(4)
        indentation = len(expanded) - len(expanded.lstrip(" "))
        if indentation != member_indent:
            continue
        match = variant_re.fullmatch(line)
        if match:
            variants[match.group(1)] = normalized(line)
    return variants


def direct_container_variants(declaration: Declaration) -> set[str]:
    return set(direct_container_variant_lines(declaration))


def direct_container_members(declaration: Declaration) -> set[str]:
    return {
        *(f"field:{name}" for name in direct_container_fields(declaration)),
        *(f"variant:{name}" for name in direct_container_variants(declaration)),
    }


def split_visibility_required(
    declaration: Declaration,
    declarations: list[Declaration],
    text_by_path: dict[str, str],
) -> bool:
    if declaration.kind != "function":
        return False
    if re.search(
        r"(?m)^[ \t]*pub\s+(?:(?:inline|noinline)\s+)?fn\b",
        declaration.body,
    ):
        return False

    for path, text in text_by_path.items():
        if path == declaration.path:
            continue
        for match in IMPORT_BINDING_RE.finditer(text):
            imported = match.group("path")
            if imported.startswith(".") or imported.endswith(".zig"):
                imported = posixpath.normpath(
                    posixpath.join(posixpath.dirname(path), imported)
                )
            if imported != declaration.path:
                continue
            reference = re.compile(
                rf"\b{re.escape(match.group('binding'))}\s*\.\s*"
                rf"{re.escape(declaration.name)}\b"
            )
            if reference.search(text) is not None:
                return True

    if declaration.owner is None:
        return False
    if sum(
        item.kind == "function" and item.name == declaration.name
        for item in declarations
    ) != 1:
        return False
    reference = re.compile(rf"\.\s*{re.escape(declaration.name)}\s*\(")
    return any(
        path != declaration.path
        and declaration.owner in text
        and reference.search(text) is not None
        for path, text in text_by_path.items()
    )


def neighbor_placement(
    index: int,
    declarations: list[Declaration],
    anchor_paths: list[str | None],
) -> tuple[str, str, str] | None:
    owner = declarations[index].owner
    previous = next(
        (
            i
            for i in range(index - 1, -1, -1)
            if anchor_paths[i] is not None and declarations[i].owner == owner
        ),
        None,
    )
    following = next(
        (
            i
            for i in range(index + 1, len(declarations))
            if anchor_paths[i] is not None and declarations[i].owner == owner
        ),
        None,
    )
    if previous is None or following is None:
        return None
    previous_path = anchor_paths[previous]
    following_path = anchor_paths[following]
    if previous_path != following_path:
        return None
    assert previous_path is not None
    def anchor_name(declaration: Declaration) -> str:
        owner = f"{declaration.owner}." if declaration.owner else ""
        return f"{declaration.kind}:{owner}{declaration.name}"

    return (
        previous_path,
        anchor_name(declarations[previous]),
        anchor_name(declarations[following]),
    )


def candidate_path(candidate_root: pathlib.Path, current_path: str) -> pathlib.Path:
    return candidate_root / current_path


def prepare_candidate_root(path: pathlib.Path) -> pathlib.Path:
    resolved = path.resolve()
    try:
        resolved.relative_to(ROOT)
    except ValueError:
        pass
    else:
        raise ValueError("candidate directory must be outside the repository")
    if resolved.exists():
        if not resolved.is_dir():
            raise ValueError(f"candidate path is not a directory: {resolved}")
        if any(resolved.iterdir()):
            raise ValueError(f"candidate directory is not empty: {resolved}")
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def analyze(
    source: str,
    base_sha: str,
    incoming_sha: str,
    destination_files: list[pathlib.Path],
    minimum_similarity: float,
    ambiguity_margin: float,
    include_unchanged: bool = False,
    include_missing_candidates: bool = False,
    test_name_aliases: dict[str, list[str]] | None = None,
    declaration_placements: dict[str, str] | None = None,
    declaration_placement_ranges: list[dict[str, str]] | None = None,
    include_review_bodies: bool = False,
    declaration_owner_migrations: dict[str, dict[str, object]] | None = None,
    declaration_mixins: list[dict[str, str]] | None = None,
    test_name_rewrites: list[dict[str, str]] | None = None,
    incoming_replacement_keys: set[str] | None = None,
    retained_deletions: dict[str, dict[str, str]] | None = None,
    destination_ref: str | None = None,
    reviewed_compositions: dict[str, dict[str, str]] | None = None,
    module_path_migrations: dict[str, list[str]] | None = None,
    symbol_call_migrations: dict[str, list[str]] | None = None,
    reviewed_resolutions: dict[str, dict[str, object]] | None = None,
    symbol_reference_migrations: dict[str, list[str]] | None = None,
    base_source: str | None = None,
    incoming_source: str | None = None,
    declaration_name_aliases: dict[str, list[str]] | None = None,
    container_field_migrations: dict[
        str, dict[str, dict[str, str]]
    ] | None = None,
    intentional_declaration_deletions: dict[str, dict[str, str]] | None = None,
) -> tuple[list[Obligation], dict[str, list[CandidateEdit]]]:
    test_name_aliases = test_name_aliases or {}
    declaration_placements = dict(declaration_placements or {})
    declaration_placement_ranges = declaration_placement_ranges or []
    declaration_owner_migrations = declaration_owner_migrations or {}
    declaration_mixins = declaration_mixins or []
    test_name_rewrites = test_name_rewrites or []
    incoming_replacement_keys = incoming_replacement_keys or set()
    retained_deletions = retained_deletions or {}
    reviewed_compositions = reviewed_compositions or {}
    module_path_migrations = module_path_migrations or {}
    symbol_call_migrations = symbol_call_migrations or {}
    reviewed_resolutions = reviewed_resolutions or {}
    symbol_reference_migrations = symbol_reference_migrations or {}
    declaration_name_aliases = declaration_name_aliases or {}
    container_field_migrations = container_field_migrations or {}
    intentional_declaration_deletions = intentional_declaration_deletions or {}
    symbol_reference_migrations = {
        name: list(dict.fromkeys([
            *symbol_reference_migrations.get(name, []),
            *aliases,
        ]))
        for name, aliases in {
            **declaration_name_aliases,
            **symbol_reference_migrations,
        }.items()
    }
    base_source = base_source or source
    incoming_source = incoming_source or source
    base_text = ref_text(base_sha, base_source)
    incoming_text = ref_text(incoming_sha, incoming_source)
    base_declarations = all_declarations(base_text, base_source, max_indent=4)
    incoming_declarations = all_declarations(
        incoming_text,
        incoming_source,
        max_indent=4,
    )
    current_declarations: list[Declaration] = []
    current_text_by_path: dict[str, str] = {}
    for path in destination_files:
        relative = str(path.relative_to(ROOT))
        text = (
            ref_text(destination_ref, relative)
            if destination_ref is not None
            else path.read_text(errors="replace")
        )
        current_text_by_path[relative] = text
        current_declarations.extend(all_declarations(text, relative))
    forced_mixin_targets = {
        (
            target["path"],
            target.get("kind", key.split(":", 1)[0]),
            target.get(
                "name",
                key.split(":", 1)[1].rsplit(".", 1)[-1],
            ),
            target["owner"],
        )
        for key, target in declaration_owner_migrations.items()
    }
    current_declarations = augment_with_manifest_mixins(
        current_declarations,
        current_text_by_path,
        declaration_mixins,
        forced_mixin_targets,
    )
    current_by_name = group_by_name(current_declarations)
    current_by_path_name: dict[tuple[str, str, str], list[Declaration]] = (
        defaultdict(list)
    )
    import_paths: dict[tuple[str, str], str] = {}
    for declaration in current_declarations:
        current_by_path_name[
            (declaration.path, declaration.kind, declaration.name)
        ].append(declaration)
        if declaration.kind != "binding":
            continue
        resolved = resolved_import_path(declaration)
        if resolved is not None:
            import_paths[(declaration.path, declaration.name)] = resolved
    incoming_import_paths = {
        (declaration.path, declaration.name): resolved
        for declaration in incoming_declarations
        if declaration.kind == "binding"
        and (resolved := resolved_import_path(declaration)) is not None
    }
    base_import_paths = {
        (declaration.path, declaration.name): resolved
        for declaration in base_declarations
        if declaration.kind == "binding"
        and (resolved := resolved_import_path(declaration)) is not None
    }
    current_aliases: dict[str, list[Declaration]] = defaultdict(list)
    for path, text in current_text_by_path.items():
        for alias in extract_aliases(text, path):
            current_aliases[alias.name].append(alias)
    incoming_counts = Counter(
        declaration_identity(item) for item in incoming_declarations
    )
    ordered_incoming = sorted(incoming_declarations, key=lambda item: item.start)

    incoming_keys = [
        f"{item.kind}:"
        f"{item.owner + '.' if item.owner is not None else ''}{item.name}"
        for item in ordered_incoming
    ]
    for placement_range in declaration_placement_ranges:
        start_key = placement_range["start"]
        end_key = placement_range["end"]
        start_count = incoming_keys.count(start_key)
        end_count = incoming_keys.count(end_key)
        if start_count == 0 and end_count == 0:
            # Historical branch-baseline audits can predate a later incoming
            # declaration cluster. A partially present range is still invalid.
            continue
        if start_count != 1 or end_count != 1:
            raise ValueError(
                "declaration placement range boundaries must each identify one "
                f"incoming declaration: {start_key}..{end_key}"
            )
        start_index = incoming_keys.index(start_key)
        end_index = incoming_keys.index(end_key)
        if start_index > end_index:
            raise ValueError(
                f"declaration placement range is reversed: {start_key}..{end_key}"
            )
        path = placement_range["path"]
        for key in incoming_keys[start_index : end_index + 1]:
            existing = declaration_placements.get(key)
            if existing is not None and existing != path:
                raise ValueError(
                    f"conflicting declaration placement for {key}: "
                    f"{existing} vs {path}"
                )
            declaration_placements[key] = path

    current_tests_by_body: dict[str, list[Declaration]] = defaultdict(list)
    for declaration in current_declarations:
        fingerprint = test_body_fingerprint(declaration)
        if fingerprint is not None:
            current_tests_by_body[fingerprint].append(declaration)

    def current_candidates(incoming: Declaration) -> list[Declaration]:
        qualified_name = (
            f"{incoming.owner}.{incoming.name}"
            if incoming.owner is not None
            else incoming.name
        )
        key = f"{incoming.kind}:{qualified_name}"
        owner_migration = declaration_owner_migrations.get(key)
        candidates = [
            candidate
            for name in candidate_names(
                incoming,
                test_name_aliases,
                declaration_name_aliases,
            )
            for candidate in current_by_name.get((incoming.kind, name), [])
        ]
        if owner_migration is not None:
            target_kind = owner_migration.get("kind", incoming.kind)
            target_name = owner_migration.get("name", incoming.name)
            candidates.extend(
                current_by_name.get((target_kind, target_name), [])
            )
        for rewritten_name, rewritten_path in rewritten_test_names(
            incoming,
            test_name_rewrites,
        ):
            candidates.extend(
                candidate
                for candidate in current_by_name.get(
                    (incoming.kind, rewritten_name),
                    [],
                )
                if candidate.path == rewritten_path
            )
        fingerprint = test_body_fingerprint(incoming)
        if fingerprint is not None:
            body_matches = current_tests_by_body.get(fingerprint, [])
            if len(body_matches) == 1:
                candidates.extend(body_matches)
        if incoming.kind == "container":
            candidates.extend(
                candidate
                for candidate in current_by_name.get(
                    ("function", incoming.name), []
                )
                if direct_container_field_lines(candidate)
            )
            candidates.extend(current_aliases.get(incoming.name, []))
        candidates = list(
            {
                (
                    candidate.path,
                    candidate.start,
                    candidate.end,
                ): candidate
                for candidate in candidates
            }.values()
        )
        return matching_owner_candidates(
            incoming,
            candidates,
            owner_migration,
            declaration_name_aliases,
        )

    anchor_paths: list[str | None] = []
    for incoming in ordered_incoming:
        paths = {item.path for item in current_candidates(incoming)}
        anchor_paths.append(next(iter(paths)) if len(paths) == 1 else None)
    incoming_order = {
        (item.kind, item.name, item.owner, item.start): index
        for index, item in enumerate(ordered_incoming)
    }

    obligations: list[Obligation] = []
    replacements: dict[str, list[CandidateEdit]] = defaultdict(list)
    reviewed_composition_keys: set[str] = set()
    reviewed_resolution_keys: set[str] = set()
    reviewed_container_field_migration_keys: set[str] = set()
    reviewed_intentional_deletion_keys: set[str] = set()

    def apply_reviewed_resolution(
        obligation: Obligation,
        base: Declaration | None,
        incoming: Declaration,
        current: Declaration,
    ) -> None:
        review = reviewed_resolutions.get(obligation.key)
        if review is None:
            return
        if obligation.status not in REVIEWABLE_RESOLUTION_STATUSES:
            raise ValueError(
                f"reviewed resolution {obligation.key!r} targets non-reviewable "
                f"status {obligation.status!r}"
            )
        expected: dict[str, object] = {
            "status": obligation.status,
            "base_sha256": base.digest if base is not None else None,
            "incoming_sha256": incoming.digest,
            "current_sha256": current.digest,
            "path": current.path,
        }
        mismatches = [
            field for field, value in expected.items() if review[field] != value
        ]
        if mismatches:
            raise ValueError(
                f"reviewed resolution {obligation.key!r} has stale "
                + ", ".join(mismatches)
            )
        reviewed_resolution_keys.add(obligation.key)
        obligation.status = "semantic_resolution_reviewed"
        obligation.detail += (
            "; status- and hash-locked semantic review: " + str(review["reason"])
        )
    changed = pair_declarations(
        base_declarations,
        incoming_declarations,
        include_unchanged=include_unchanged,
        declaration_name_aliases=declaration_name_aliases,
        test_name_aliases=test_name_aliases,
    )
    for base, incoming, ordinal in changed:
        qualified_name = (
            f"{incoming.owner}.{incoming.name}"
            if incoming.owner is not None
            else incoming.name
        )
        key = f"{incoming.kind}:{qualified_name}"
        if incoming_counts[declaration_identity(incoming)] > 1:
            key += f"#{ordinal}"
        if base is None:
            change = "added"
        elif normalized(base.body) == normalized(incoming.body):
            change = "unchanged"
        else:
            change = "modified"
        candidates = current_candidates(incoming)
        obligation = Obligation(
            key=key,
            kind=incoming.kind,
            name=incoming.name,
            change=change,
            status="open",
            base_sha256=base.digest if base else None,
            incoming_sha256=incoming.digest,
            base_line=(
                base_text.count("\n", 0, base.start) + 1
                if base is not None
                else None
            ),
            incoming_line=incoming_text.count("\n", 0, incoming.start) + 1,
            owner=incoming.owner,
            base_body=base.body if include_review_bodies and base else None,
            incoming_body=incoming.body if include_review_bodies else None,
        )
        intentional_deletion = intentional_declaration_deletions.get(key)
        if intentional_deletion is not None:
            if candidates:
                raise ValueError(
                    f"intentional declaration deletion {key!r} still has a "
                    "split destination candidate"
                )
            if intentional_deletion["incoming_sha256"] != incoming.digest:
                raise ValueError(
                    f"intentional declaration deletion {key!r} has stale "
                    "incoming_sha256"
                )
            reviewed_intentional_deletion_keys.add(key)
            obligation.status = "intentional_deletion"
            obligation.detail = (
                "hash-locked intentional split-refactor deletion: "
                + intentional_deletion["reason"]
            )
            obligations.append(obligation)
            continue
        if not candidates:
            obligation.status = (
                "missing_carried"
                if change == "unchanged"
                else f"missing_{change}"
            )
            obligation.detail = "no same-name declaration in split destinations"
            placement = neighbor_placement(
                incoming_order[
                    (incoming.kind, incoming.name, incoming.owner, incoming.start)
                ],
                ordered_incoming,
                anchor_paths,
            )
            explicit_path = declaration_placements.get(key)
            if explicit_path is not None:
                if explicit_path not in current_text_by_path:
                    raise ValueError(
                        f"declaration placement for {key} is not a destination: "
                        f"{explicit_path}"
                    )
                placement = None
                obligation.suggested_path = explicit_path
                obligation.detail += (
                    f"; policy assigns declaration to {explicit_path}"
                )
                if (
                    include_missing_candidates
                    and incoming_counts[declaration_identity(incoming)] == 1
                ):
                    text = current_text_by_path[explicit_path]
                    if CONFLICT_RE.search(text):
                        obligation.detail += "; destination contains conflict markers"
                    elif incoming.owner is not None:
                        owner_anchors = [
                            candidate
                            for candidate in current_declarations
                            if candidate.path == explicit_path
                            and candidate.owner == incoming.owner
                        ]
                        if not owner_anchors:
                            obligation.detail += (
                                f"; destination has no {incoming.owner} mixin anchor"
                            )
                        else:
                            anchor = min(owner_anchors, key=lambda item: item.start)
                            obligation.status = "clean_insertion_candidate"
                            obligation.detail += (
                                f"; insert at start of {incoming.owner} mixin"
                            )
                            replacements[explicit_path].append(
                                CandidateEdit(
                                    key=obligation.key,
                                    old=anchor.body,
                                    new=reindent_declaration(
                                        incoming.body,
                                        anchor.indent,
                                    ),
                                    source_order=incoming.start,
                                    start=anchor.start,
                                    end=anchor.end,
                                    insert_before=True,
                                )
                            )
                    else:
                        obligation.status = "clean_insertion_candidate"
                        obligation.detail += "; append at end of destination"
                        replacements[explicit_path].append(
                            CandidateEdit(
                                key=obligation.key,
                                old="",
                                new=reindent_declaration(incoming.body, 0),
                                source_order=incoming.start,
                                start=len(text),
                                end=len(text),
                                insert_before=True,
                            )
                        )
            if placement is not None:
                (
                    obligation.suggested_path,
                    obligation.previous_anchor,
                    obligation.next_anchor,
                ) = placement
                obligation.detail += (
                    f"; neighboring anchors agree on {obligation.suggested_path}"
                )
                if (
                    include_missing_candidates
                    and incoming_counts[declaration_identity(incoming)] == 1
                ):
                    incoming_index = incoming_order[
                        (
                            incoming.kind,
                            incoming.name,
                            incoming.owner,
                            incoming.start,
                        )
                    ]
                    following_index = next(
                        (
                            index
                            for index in range(
                                incoming_index + 1,
                                len(ordered_incoming),
                            )
                            if anchor_paths[index] == obligation.suggested_path
                            and ordered_incoming[index].owner == incoming.owner
                        ),
                        None,
                    )
                    if following_index is not None:
                        following = ordered_incoming[following_index]
                        current_anchors = [
                            candidate
                            for candidate in current_candidates(following)
                            if candidate.path == obligation.suggested_path
                        ]
                        if len(current_anchors) == 1:
                            anchor = current_anchors[0]
                            inserted = reindent_declaration(
                                incoming.body,
                                anchor.indent,
                            )
                            obligation.status = "clean_insertion_candidate"
                            obligation.detail += (
                                f"; insert before {obligation.next_anchor}"
                            )
                            replacements[anchor.path].append(
                                CandidateEdit(
                                    key=obligation.key,
                                    old=anchor.body,
                                    new=inserted,
                                    source_order=incoming.start,
                                    start=anchor.start,
                                    end=anchor.end,
                                    insert_before=True,
                                )
                            )
            obligations.append(obligation)
            continue

        reference = base.body if base is not None else incoming.body
        owner_migration = declaration_owner_migrations.get(key)
        def candidate_similarity(candidate: Declaration) -> float:
            if incoming.kind != "container":
                return similarity(
                    reference,
                    declaration_for_match(
                        candidate,
                        incoming,
                        owner_migration,
                    ).body,
                )
            reference_members = direct_container_members(base or incoming)
            candidate_members = direct_container_members(candidate)
            union = reference_members | candidate_members
            return 1.0 if not union else len(reference_members & candidate_members) / len(union)

        ranked = sorted(
            (
                (candidate_similarity(candidate), candidate)
                for candidate in candidates
            ),
            key=lambda item: item[0],
            reverse=True,
        )
        score, current = ranked[0]
        second_score = ranked[1][0] if len(ranked) > 1 else None
        obligation.current_path = current.path
        obligation.current_sha256 = current.digest
        obligation.current_owner = current.owner
        obligation.current_body = current.body if include_review_bodies else None
        obligation.similarity = score
        obligation.second_similarity = second_score

        exact = [
            candidate
            for candidate in candidates
            if normalized(
                declaration_for_match(
                    candidate,
                    incoming,
                    owner_migration,
                ).body
            )
            == normalized(incoming.body)
        ]
        if exact:
            obligation.current_path = exact[0].path
            obligation.current_sha256 = exact[0].digest
            obligation.current_owner = exact[0].owner
            obligation.current_body = (
                exact[0].body if include_review_bodies else None
            )
            obligation.similarity = 1.0
            obligation.second_similarity = 1.0 if len(exact) > 1 else second_score
            if split_visibility_required(
                exact[0],
                current_declarations,
                current_text_by_path,
            ):
                obligation.status = "split_visibility_missing"
                obligation.detail = (
                    "private declaration has a unique qualified call from "
                    "another split destination"
                )
            elif exact[0].name != incoming.name:
                obligation.status = "split_test_name_adapted"
                obligation.detail = (
                    f"test name maps to {exact[0].name!r} in split destination"
                )
            else:
                obligation.status = "exact"
            obligations.append(obligation)
            continue
        visibility_adapted = [
            candidate
            for candidate in candidates
            if normalized_split_visibility(
                declaration_for_match(
                    candidate,
                    incoming,
                    owner_migration,
                ).body
            )
            == normalized_split_visibility(incoming.body)
        ]
        if visibility_adapted:
            obligation.current_path = visibility_adapted[0].path
            obligation.current_sha256 = visibility_adapted[0].digest
            obligation.current_owner = visibility_adapted[0].owner
            obligation.current_body = (
                visibility_adapted[0].body if include_review_bodies else None
            )
            obligation.status = "split_visibility_adapted"
            obligation.detail = (
                "declaration matches incoming with split-module public visibility"
            )
            obligations.append(obligation)
            continue
        module_reference_adapted = [
            candidate
            for candidate in candidates
            if normalized_module_references(
                declaration_for_match(
                    candidate,
                    incoming,
                    owner_migration,
                ),
                import_paths,
                module_path_migrations,
                symbol_call_migrations,
                symbol_reference_migrations,
            )
            == normalized_module_references(
                incoming,
                incoming_import_paths,
                module_path_migrations,
                symbol_call_migrations,
                symbol_reference_migrations,
            )
        ]
        if module_reference_adapted:
            obligation.current_path = module_reference_adapted[0].path
            obligation.current_sha256 = module_reference_adapted[0].digest
            obligation.current_owner = module_reference_adapted[0].owner
            obligation.current_body = (
                module_reference_adapted[0].body
                if include_review_bodies
                else None
            )
            obligation.status = "split_module_reference_adapted"
            obligation.detail = (
                "qualified module references resolve to the same repository paths"
            )
            obligations.append(obligation)
            continue
        import_adapted = [
            candidate
            for candidate in candidates
            if incoming.kind == "binding"
            and normalized_import_binding(incoming, module_path_migrations)
            is not None
            and normalized_import_binding(candidate, module_path_migrations)
            == normalized_import_binding(incoming, module_path_migrations)
        ]
        if import_adapted:
            obligation.current_path = import_adapted[0].path
            obligation.current_sha256 = import_adapted[0].digest
            obligation.current_owner = import_adapted[0].owner
            obligation.current_body = (
                import_adapted[0].body if include_review_bodies else None
            )
            obligation.status = "split_import_path_adapted"
            obligation.detail = (
                "binding resolves to the same imported module from a split path"
            )
            obligations.append(obligation)
            continue
        binding_alias_adapted: list[Declaration] = []
        if incoming.kind == "binding":
            for candidate in candidates:
                alias = qualified_binding_alias(candidate)
                if alias is None:
                    continue
                module_name, member_name = alias
                target_path = import_paths.get((candidate.path, module_name))
                if target_path is None:
                    continue
                targets = current_by_path_name.get(
                    (target_path, "binding", member_name),
                    [],
                )
                if any(
                    normalized_binding_definition(
                        target,
                        import_paths,
                        module_path_migrations,
                        symbol_reference_migrations,
                    )
                    == normalized_binding_definition(
                        incoming,
                        incoming_import_paths,
                        module_path_migrations,
                        symbol_reference_migrations,
                    )
                    for target in targets
                ):
                    binding_alias_adapted.append(candidate)
        if binding_alias_adapted:
            obligation.current_path = binding_alias_adapted[0].path
            obligation.current_sha256 = binding_alias_adapted[0].digest
            obligation.current_owner = binding_alias_adapted[0].owner
            obligation.current_body = (
                binding_alias_adapted[0].body
                if include_review_bodies
                else None
            )
            obligation.status = "split_binding_alias_adapted"
            obligation.detail = (
                "binding aliases an equivalent declaration owned by a split module"
            )
            obligations.append(obligation)
            continue
        if (
            second_score is not None
            and score - second_score < ambiguity_margin
        ):
            obligation.status = "ambiguous_destination"
            obligation.detail = (
                f"best two similarities differ by less than {ambiguity_margin:.3f}"
            )
            obligations.append(obligation)
            continue
        if CONFLICT_RE.search(current_text_by_path[current.path]):
            obligation.status = "destination_conflicted"
            obligation.detail = "destination file still contains conflict markers"
            obligations.append(obligation)
            continue
        if key in incoming_replacement_keys:
            if incoming_counts[declaration_identity(incoming)] > 1:
                obligation.status = "duplicate_source_name"
                obligation.detail = (
                    "reviewed incoming replacement rejected for duplicate "
                    "source declaration name"
                )
            else:
                obligation.status = "reviewed_incoming_replacement_candidate"
                obligation.detail = (
                    "explicit reviewed replacement with the pinned incoming "
                    "declaration"
                )
                replacements[current.path].append(
                    CandidateEdit(
                        key=obligation.key,
                        old=current.body,
                        new=restore_split_declaration_name(
                            reindent_declaration(
                                incoming.body,
                                current.indent,
                            ),
                            incoming,
                            current,
                            owner_migration,
                        ),
                        source_order=incoming.start,
                        start=current.start,
                        end=current.end,
                    )
                )
            obligations.append(obligation)
            continue
        if current.kind == "alias":
            obligation.status = "split_alias_review"
            obligation.detail = (
                "monolith container is represented by split alias: "
                + normalized(current.body)
            )
            obligations.append(obligation)
            continue
        if incoming.kind == "container":
            incoming_fields = direct_container_field_lines(
                incoming,
                incoming_import_paths,
                module_path_migrations,
                symbol_reference_migrations,
            )
            current_fields = direct_container_field_lines(
                current,
                import_paths,
                module_path_migrations,
                symbol_reference_migrations,
            )
            base_fields = (
                direct_container_field_lines(
                    base,
                    base_import_paths,
                    module_path_migrations,
                    symbol_reference_migrations,
                )
                if base is not None
                else {}
            )
            incoming_variants = direct_container_variant_lines(incoming)
            current_variants = direct_container_variant_lines(current)
            base_variants = (
                direct_container_variant_lines(base) if base is not None else {}
            )
            field_migrations = container_field_migrations.get(key, {})
            for source_field, migration in field_migrations.items():
                if source_field not in incoming_fields:
                    raise ValueError(
                        f"container field migration {key}.{source_field} has "
                        "no incoming source field"
                    )
                target_field = migration["target"]
                if target_field not in current_fields:
                    raise ValueError(
                        f"container field migration {key}.{source_field} has "
                        f"missing current target field {target_field!r}"
                    )
            if field_migrations:
                reviewed_container_field_migration_keys.add(key)
            migrated_fields = set(field_migrations)
            missing_fields = sorted(
                set(incoming_fields) - set(current_fields) - migrated_fields
            )
            stale_fields: list[str] = []
            diverged_fields: list[str] = []
            for field_name in sorted(
                (set(incoming_fields) & set(current_fields)) - migrated_fields
            ):
                if current_fields[field_name] == incoming_fields[field_name]:
                    continue
                if (
                    field_name in base_fields
                    and current_fields[field_name] == base_fields[field_name]
                ):
                    stale_fields.append(field_name)
                else:
                    diverged_fields.append(field_name)
            missing_variants = sorted(
                set(incoming_variants) - set(current_variants)
            )
            stale_variants: list[str] = []
            diverged_variants: list[str] = []
            for variant_name in sorted(
                set(incoming_variants) & set(current_variants)
            ):
                if current_variants[variant_name] == incoming_variants[variant_name]:
                    continue
                if (
                    variant_name in base_variants
                    and current_variants[variant_name] == base_variants[variant_name]
                ):
                    stale_variants.append(variant_name)
                else:
                    diverged_variants.append(variant_name)
            if missing_fields:
                obligation.status = "container_fields_missing"
                obligation.detail = "missing direct fields: " + ", ".join(
                    missing_fields
                )
            elif stale_fields:
                obligation.status = "container_fields_stale"
                obligation.detail = (
                    "fields still match base instead of incoming: "
                    + ", ".join(stale_fields)
                )
            elif diverged_fields:
                obligation.status = "container_fields_diverged"
                obligation.detail = (
                    "fields differ from both base and incoming: "
                    + ", ".join(diverged_fields)
                )
            elif missing_variants:
                obligation.status = "container_variants_missing"
                obligation.detail = "missing direct variants: " + ", ".join(
                    missing_variants
                )
            elif stale_variants:
                obligation.status = "container_variants_stale"
                obligation.detail = (
                    "variants still match base instead of incoming: "
                    + ", ".join(stale_variants)
                )
            elif diverged_variants:
                obligation.status = "container_variants_diverged"
                obligation.detail = (
                    "variants differ from both base and incoming: "
                    + ", ".join(diverged_variants)
                )
            else:
                obligation.status = "container_review_fields_present"
                obligation.detail = (
                    "direct fields and variants match; container behavior requires review"
                )
            if field_migrations:
                migrated_summary = ", ".join(
                    f"{source}->{migration['target']}"
                    for source, migration in sorted(field_migrations.items())
                )
                obligation.detail += (
                    "; policy-verified field migrations: " + migrated_summary
                )
            if current.kind == "function":
                obligation.detail += "; represented by a returned anonymous container"
            apply_reviewed_resolution(obligation, base, incoming, current)
            obligations.append(obligation)
            continue
        if base is None:
            obligation.status = "added_name_collision"
            obligation.detail = "incoming added declaration differs from current one"
            apply_reviewed_resolution(obligation, base, incoming, current)
            obligations.append(obligation)
            continue

        if change == "unchanged":
            obligation.status = "carried_branch_changed"
            obligation.detail = (
                "baseline declaration is present with branch-specific changes"
            )
            obligations.append(obligation)
            continue

        current_for_merge = declaration_for_match(
            current,
            incoming,
            owner_migration,
        )
        merge_status, merged = merge_declarations(
            current_for_merge,
            base,
            incoming,
        )
        semantic_merge_preserves_current = False
        if merge_status != 0 and (
            module_path_migrations
            or symbol_call_migrations
            or symbol_reference_migrations
        ):
            semantic_current = canonicalized_merge_declaration(
                current_for_merge,
                import_paths,
                module_path_migrations,
                symbol_call_migrations,
                symbol_reference_migrations,
            )
            semantic_base = canonicalized_merge_declaration(
                base,
                base_import_paths,
                module_path_migrations,
                symbol_call_migrations,
                symbol_reference_migrations,
            )
            semantic_incoming = canonicalized_merge_declaration(
                incoming,
                incoming_import_paths,
                module_path_migrations,
                symbol_call_migrations,
                symbol_reference_migrations,
            )
            semantic_status, semantic_merged = merge_declarations(
                semantic_current,
                semantic_base,
                semantic_incoming,
            )
            semantic_merge_preserves_current = (
                semantic_status == 0
                and merge_preserves_current(semantic_current, semantic_merged)
            ) or canonicalized_incoming_is_preserved(
                semantic_current,
                semantic_base,
                semantic_incoming,
            )
        if semantic_merge_preserves_current:
            obligation.status = "integrated"
            obligation.detail = (
                "incoming delta is already present after manifest-declared "
                "split module and symbol rewrites"
            )
            obligations.append(obligation)
            continue
        if merge_status != 0:
            obligation.status = "three_way_conflict"
            obligation.detail = "declaration-level three-way merge requires review"
            if obligation.key in reviewed_compositions:
                review = reviewed_compositions[obligation.key]
                expected = {
                    "base_sha256": base.digest,
                    "incoming_sha256": incoming.digest,
                    "current_sha256": current.digest,
                    "path": current.path,
                }
                mismatches = [
                    field
                    for field, value in expected.items()
                    if review[field] != value
                ]
                if mismatches:
                    raise ValueError(
                        f"reviewed composition {obligation.key!r} has stale "
                        + ", ".join(mismatches)
                    )
                reviewed_composition_keys.add(obligation.key)
                obligation.status = "three_way_composition_reviewed"
                obligation.detail += (
                    "; hash-locked composition after semantic review: "
                    + review["reason"]
                )
            else:
                apply_reviewed_resolution(obligation, base, incoming, current)
        elif merge_preserves_current(current_for_merge, merged):
            obligation.status = "integrated"
            obligation.detail = (
                "incoming delta is already present with branch-specific changes"
            )
        elif score < minimum_similarity:
            obligation.status = "clean_below_similarity"
            obligation.detail = (
                f"clean merge withheld below similarity threshold {minimum_similarity:.3f}"
            )
        elif incoming_counts[declaration_identity(incoming)] > 1:
            obligation.status = "duplicate_source_name"
            obligation.detail = "candidate withheld for duplicate source declaration name"
        else:
            obligation.status = "clean_candidate"
            apply_reviewed_resolution(obligation, base, incoming, current)
            if obligation.status == "clean_candidate":
                replacements[current.path].append(
                    CandidateEdit(
                        key=obligation.key,
                        old=current.body,
                        new=restore_split_declaration_name(
                            merged,
                            incoming,
                            current,
                            owner_migration,
                        ),
                        source_order=incoming.start,
                        start=current.start,
                        end=current.end,
                    )
                )
        obligations.append(obligation)

    base_counts = Counter(declaration_identity(item) for item in base_declarations)
    reviewed_retained_deletions: set[str] = set()
    for base, ordinal in removed_declarations(
        base_declarations,
        incoming_declarations,
        declaration_name_aliases,
        test_name_aliases,
    ):
        qualified_name = (
            f"{base.owner}.{base.name}"
            if base.owner is not None
            else base.name
        )
        key = f"{base.kind}:{qualified_name}"
        if base_counts[declaration_identity(base)] > 1:
            key += f"#{ordinal}"
        candidates = current_candidates(base)
        obligation = Obligation(
            key=key,
            kind=base.kind,
            name=base.name,
            change="deleted",
            status="deleted_absent",
            base_sha256=base.digest,
            incoming_sha256=None,
            base_line=base_text.count("\n", 0, base.start) + 1,
            owner=base.owner,
            base_body=base.body if include_review_bodies else None,
        )
        if not candidates:
            obligation.detail = (
                "declaration deleted by incoming main and absent from split destinations"
            )
            obligations.append(obligation)
            continue

        owner_migration = declaration_owner_migrations.get(key)
        ranked = sorted(
            (
                (
                    similarity(
                        base.body,
                        declaration_for_match(
                            candidate,
                            base,
                            owner_migration,
                        ).body,
                    ),
                    candidate,
                )
                for candidate in candidates
            ),
            key=lambda item: item[0],
            reverse=True,
        )
        score, current = ranked[0]
        second_score = ranked[1][0] if len(ranked) > 1 else None
        obligation.current_path = current.path
        obligation.current_sha256 = current.digest
        obligation.current_owner = current.owner
        obligation.current_body = current.body if include_review_bodies else None
        obligation.similarity = score
        obligation.second_similarity = second_score

        if (
            second_score is not None
            and score - second_score < ambiguity_margin
        ):
            obligation.status = "deleted_ambiguous"
            obligation.detail = (
                "incoming deleted declaration maps to multiple split destinations"
            )
        elif CONFLICT_RE.search(current_text_by_path[current.path]):
            obligation.status = "destination_conflicted"
            obligation.detail = "destination file still contains conflict markers"
        else:
            current_for_match = declaration_for_match(
                current,
                base,
                owner_migration,
            )
            if (
                normalized(current_for_match.body) == normalized(base.body)
                or normalized_split_visibility(current_for_match.body)
                == normalized_split_visibility(base.body)
                or (
                    base.kind == "binding"
                    and normalized_import_binding(base) is not None
                    and normalized_import_binding(current_for_match)
                    == normalized_import_binding(base)
                )
            ):
                obligation.status = "deleted_still_present"
                obligation.detail = (
                    "incoming main deleted this declaration, but the baseline "
                    "declaration remains in a split destination"
                )
            else:
                obligation.status = "deleted_branch_changed"
                obligation.detail = (
                    "incoming main deleted this declaration, but a branch-modified "
                    "version remains in a split destination"
                )
            if key in retained_deletions:
                retention = retained_deletions[key]
                expected = {
                    "base_sha256": base.digest,
                    "current_sha256": current.digest,
                    "path": current.path,
                }
                mismatches = [
                    field
                    for field, value in expected.items()
                    if retention[field] != value
                ]
                if mismatches:
                    raise ValueError(
                        f"retained deletion {key!r} has stale "
                        + ", ".join(mismatches)
                    )
                reviewed_retained_deletions.add(key)
                obligation.status = "deleted_retained_reviewed"
                obligation.detail += (
                    "; hash-locked retention after semantic review: "
                    + retention["reason"]
                )
        obligations.append(obligation)
    unknown_retained_deletions = (
        set(retained_deletions) - reviewed_retained_deletions
    )
    if unknown_retained_deletions:
        raise ValueError(
            "retained deletion entries do not identify uniquely retained, "
            "conflict-free deleted declarations: "
            + ", ".join(sorted(unknown_retained_deletions))
        )
    unknown_reviewed_compositions = (
        set(reviewed_compositions) - reviewed_composition_keys
    )
    if unknown_reviewed_compositions:
        raise ValueError(
            "reviewed composition entries do not identify live three-way "
            "conflicts: "
            + ", ".join(sorted(unknown_reviewed_compositions))
        )
    unknown_reviewed_resolutions = (
        set(reviewed_resolutions) - reviewed_resolution_keys
    )
    if unknown_reviewed_resolutions:
        raise ValueError(
            "reviewed resolution entries do not identify live reviewable "
            "semantic conflicts: "
            + ", ".join(sorted(unknown_reviewed_resolutions))
        )
    unknown_container_field_migrations = (
        set(container_field_migrations) - reviewed_container_field_migration_keys
    )
    if unknown_container_field_migrations:
        raise ValueError(
            "container field migration entries do not identify live container "
            "obligations: "
            + ", ".join(sorted(unknown_container_field_migrations))
        )
    unknown_intentional_deletions = (
        set(intentional_declaration_deletions)
        - reviewed_intentional_deletion_keys
    )
    if unknown_intentional_deletions:
        raise ValueError(
            "intentional declaration deletion entries do not identify absent "
            "incoming declarations: "
            + ", ".join(sorted(unknown_intentional_deletions))
        )
    return obligations, replacements


def write_candidates(
    candidate_root: pathlib.Path,
    replacements: dict[str, list[CandidateEdit]],
    obligations: list[Obligation],
    metadata: dict[str, object],
) -> None:
    current_file_sha256: dict[str, str] = {}
    candidate_file_sha256: dict[str, str] = {}
    emitted_keys = {
        edit.key
        for edits in replacements.values()
        for edit in edits
    }
    for relative, edits in replacements.items():
        source_path = ROOT / relative
        text = source_path.read_text()
        current_file_sha256[relative] = sha256_text(text)
        grouped: dict[tuple[int, int], list[CandidateEdit]] = defaultdict(list)
        for edit in edits:
            if edit.start < 0 or edit.end < edit.start:
                raise RuntimeError(
                    f"{relative}: candidate for {edit.key} has no parsed declaration span"
                )
            grouped[(edit.start, edit.end)].append(edit)

        spans = sorted(grouped, reverse=True)
        for index, (start, end) in enumerate(spans):
            if index > 0:
                previous_start, _ = spans[index - 1]
                if end > previous_start:
                    keys = ", ".join(edit.key for edit in grouped[(start, end)])
                    raise RuntimeError(
                        f"{relative}: overlapping candidate declaration span for {keys}"
                    )

            original = text[start:end]
            span_edits = grouped[(start, end)]
            if any(edit.old != original for edit in span_edits):
                keys = ", ".join(edit.key for edit in span_edits)
                raise RuntimeError(
                    f"{relative}: parsed declaration span changed for {keys}"
                )
            replacements_for_span = [
                edit for edit in span_edits if not edit.insert_before
            ]
            if len(replacements_for_span) > 1:
                keys = ", ".join(edit.key for edit in replacements_for_span)
                raise RuntimeError(
                    f"{relative}: multiple replacements target one declaration: {keys}"
                )
            replacement = (
                replacements_for_span[0].new
                if replacements_for_span
                else original
            )
            insertions = sorted(
                (edit for edit in span_edits if edit.insert_before),
                key=lambda item: item.source_order,
            )
            if insertions:
                replacement = "".join(edit.new + "\n" for edit in insertions) + replacement
            for edit in span_edits:
                if CONFLICT_RE.search(edit.new):
                    raise RuntimeError(
                        f"{relative}: candidate for {edit.key} has conflict markers"
                    )
            text = text[:start] + replacement + text[end:]
        output = candidate_path(candidate_root, relative)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text)
        candidate_file_sha256[relative] = sha256_text(text)

    manifest = dict(metadata)
    manifest["candidate_files"] = sorted(replacements)
    manifest["current_file_sha256"] = current_file_sha256
    manifest["candidate_file_sha256"] = candidate_file_sha256
    manifest["candidate_obligations"] = [
        dataclasses.asdict(item)
        for item in obligations
        if item.key in emitted_keys
    ]
    (candidate_root / "split-declaration-candidates.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )


def select_candidate_edits(
    replacements: dict[str, list[CandidateEdit]],
    selected_keys: list[str],
    excluded_keys: list[str] | None = None,
) -> dict[str, list[CandidateEdit]]:
    excluded = set(excluded_keys or [])
    available = {
        edit.key
        for edits in replacements.values()
        for edit in edits
    }
    unknown_excluded = excluded - available
    if unknown_excluded:
        raise ValueError(
            "unknown or unavailable excluded candidate key(s): "
            + ", ".join(sorted(unknown_excluded))
        )
    if not selected_keys:
        return {
            path: [edit for edit in edits if edit.key not in excluded]
            for path, edits in replacements.items()
            if any(edit.key not in excluded for edit in edits)
        }
    selected = set(selected_keys)
    unknown = selected - available
    if unknown:
        raise ValueError(
            "unknown or unavailable candidate key(s): " + ", ".join(sorted(unknown))
        )
    overlap = selected & excluded
    if overlap:
        raise ValueError(
            "candidate key cannot be both selected and excluded: "
            + ", ".join(sorted(overlap))
        )
    return {
        path: [
            edit
            for edit in edits
            if edit.key in selected and edit.key not in excluded
        ]
        for path, edits in replacements.items()
        if any(
            edit.key in selected and edit.key not in excluded
            for edit in edits
        )
    }


def split_migration(
    policy: dict[str, object],
    name: str,
) -> tuple[str, list[str]]:
    migrations = policy.get("split_migrations")
    if not isinstance(migrations, dict):
        raise ValueError("policy split_migrations must be an object")
    migration = migrations.get(name)
    if not isinstance(migration, dict):
        available = ", ".join(sorted(str(key) for key in migrations))
        raise ValueError(
            f"unknown split migration {name!r}; available: {available or '<none>'}"
        )
    source = migration.get("source")
    destinations = migration.get("destinations")
    if (
        not isinstance(source, str)
        or not source
        or not isinstance(destinations, list)
        or not destinations
        or any(not isinstance(path, str) or not path for path in destinations)
    ):
        raise ValueError(
            f"policy split_migrations.{name} requires a source string "
            "and non-empty destinations string list"
        )
    return source, destinations


def companion_policy_scope(
    policy: dict[str, object],
    owner_name: str,
    path: str,
) -> tuple[dict[str, object], str]:
    migrations = policy.get("split_migrations")
    if not isinstance(migrations, dict) or owner_name not in migrations:
        raise ValueError(f"unknown companion owner migration: {owner_name}")
    owner = migrations[owner_name]
    if not isinstance(owner, dict):
        raise ValueError(f"split migration {owner_name!r} must be an object")
    raw_policies = owner.get("declaration_companion_policies", {})
    if not isinstance(raw_policies, dict):
        raise ValueError(
            f"split_migrations.{owner_name}.declaration_companion_policies must be an object"
        )
    raw_scope = raw_policies.get(path, {})
    if not isinstance(raw_scope, dict):
        raise ValueError(
            f"declaration companion policy for {path!r} must be an object"
        )

    destinations = raw_scope.get("destinations", [path])
    if (
        not isinstance(destinations, list)
        or not destinations
        or not all(isinstance(item, str) and item for item in destinations)
        or path not in destinations
    ):
        raise ValueError(
            f"declaration companion policy destinations for {path!r} must "
            "be a non-empty string list containing the companion path"
        )

    scoped_mixins = raw_scope.get("declaration_mixins")
    if scoped_mixins is None:
        owner_mixins = owner.get("declaration_mixins", [])
        if not isinstance(owner_mixins, list):
            raise ValueError(
                f"split_migrations.{owner_name}.declaration_mixins must be a list"
            )
        scoped_mixins = [
            item
            for item in owner_mixins
            if isinstance(item, dict) and item.get("path") in destinations
        ]

    scope_name = f"{owner_name}::companion::{path}"
    scoped_migrations = dict(migrations)
    scoped_migrations[scope_name] = {
        **raw_scope,
        "source": path,
        "destinations": destinations,
        "declaration_mixins": scoped_mixins,
    }
    scoped_policy = dict(policy)
    scoped_policy["split_migrations"] = scoped_migrations
    return scoped_policy, scope_name


def split_declaration_placements(
    policy: dict[str, object],
    migration_name: str | None,
) -> dict[str, str]:
    if migration_name is None:
        return {}
    migrations = policy.get("split_migrations")
    if not isinstance(migrations, dict):
        raise ValueError("policy split_migrations must be an object")
    migration = migrations.get(migration_name)
    if not isinstance(migration, dict):
        raise ValueError(f"unknown split migration {migration_name!r}")
    raw = migration.get("declaration_placements", {})
    if (
        not isinstance(raw, dict)
        or any(
            not isinstance(key, str)
            or not key
            or not isinstance(path, str)
            or not path
            for key, path in raw.items()
        )
    ):
        raise ValueError(
            f"policy split_migrations.{migration_name}."
            "declaration_placements must map strings to strings"
        )
    return raw


def split_declaration_placement_ranges(
    policy: dict[str, object],
    migration_name: str | None,
) -> list[dict[str, str]]:
    if migration_name is None:
        return []
    migrations = policy.get("split_migrations")
    if not isinstance(migrations, dict):
        raise ValueError("policy split_migrations must be an object")
    migration = migrations.get(migration_name)
    if not isinstance(migration, dict):
        raise ValueError(f"unknown split migration {migration_name!r}")
    raw = migration.get("declaration_placement_ranges", [])
    required = {"start", "end", "path", "reason"}
    if (
        not isinstance(raw, list)
        or any(
            not isinstance(item, dict)
            or set(item) != required
            or not all(isinstance(item[field], str) and item[field] for field in required)
            for item in raw
        )
    ):
        raise ValueError(
            f"policy split_migrations.{migration_name}."
            "declaration_placement_ranges must contain exact start, end, path, "
            "and reason strings"
        )
    return raw


def split_declaration_owner_migrations(
    policy: dict[str, object],
    migration_name: str | None,
) -> dict[str, dict[str, object]]:
    if migration_name is None:
        return {}
    migrations = policy.get("split_migrations")
    if not isinstance(migrations, dict):
        raise ValueError("policy split_migrations must be an object")
    migration = migrations.get(migration_name)
    if not isinstance(migration, dict):
        raise ValueError(f"unknown split migration {migration_name!r}")
    raw = migration.get("declaration_owner_migrations", {})
    if not isinstance(raw, dict):
        raise ValueError(
            f"policy split_migrations.{migration_name}."
            "declaration_owner_migrations must be an object"
        )
    for key, target in raw.items():
        if (
            not isinstance(key, str)
            or not key
            or not isinstance(target, dict)
            or "owner" not in target
            or (
                target["owner"] is not None
                and not isinstance(target["owner"], str)
            )
            or not isinstance(target.get("path"), str)
            or not target["path"]
            or (
                "name" in target
                and (
                    not isinstance(target["name"], str)
                    or not target["name"]
                )
            )
            or (
                "kind" in target
                and target["kind"]
                not in {"binding", "container", "function", "test"}
            )
        ):
            raise ValueError(
                f"invalid declaration owner migration for {key!r}"
            )
    return raw


def split_intentional_declaration_deletions(
    policy: dict[str, object],
    migration_name: str | None,
) -> dict[str, dict[str, str]]:
    if migration_name is None:
        return {}
    migrations = policy.get("split_migrations")
    if not isinstance(migrations, dict):
        raise ValueError("policy split_migrations must be an object")
    migration = migrations.get(migration_name)
    if not isinstance(migration, dict):
        raise ValueError(f"unknown split migration {migration_name!r}")
    raw = migration.get("intentional_declaration_deletions", {})
    required = {"incoming_sha256", "reason"}
    if not isinstance(raw, dict) or any(
        not isinstance(key, str)
        or not key
        or not isinstance(review, dict)
        or set(review) != required
        or not all(
            isinstance(review[field], str) and review[field]
            for field in required
        )
        or len(review["incoming_sha256"]) != 64
        for key, review in raw.items()
    ):
        raise ValueError(
            f"policy split_migrations.{migration_name}."
            "intentional_declaration_deletions must map declaration keys to "
            "exact non-empty incoming_sha256 and reason strings"
        )
    return raw


def split_declaration_mixins(
    policy: dict[str, object],
    migration_name: str | None,
) -> list[dict[str, str]]:
    if migration_name is None:
        return []
    migrations = policy.get("split_migrations")
    if not isinstance(migrations, dict):
        raise ValueError("policy split_migrations must be an object")
    migration = migrations.get(migration_name)
    if not isinstance(migration, dict):
        raise ValueError(f"unknown split migration {migration_name!r}")
    raw = migration.get("declaration_mixins", [])
    if not isinstance(raw, list):
        raise ValueError(
            f"policy split_migrations.{migration_name}."
            "declaration_mixins must be a list"
        )
    required = {"path", "factory", "owner"}
    for rule in raw:
        if (
            not isinstance(rule, dict)
            or set(rule) != required
            or any(
                not isinstance(rule.get(key), str) or not rule[key]
                for key in required
            )
        ):
            raise ValueError(
                f"policy split_migrations.{migration_name}."
                "declaration_mixins entries require exactly non-empty "
                "path, factory, and owner strings"
            )
    return raw


def split_test_name_rewrites(
    policy: dict[str, object],
    migration_name: str | None,
) -> list[dict[str, str]]:
    if migration_name is None:
        return []
    migrations = policy.get("split_migrations")
    if not isinstance(migrations, dict):
        raise ValueError("policy split_migrations must be an object")
    migration = migrations.get(migration_name)
    if not isinstance(migration, dict):
        raise ValueError(f"unknown split migration {migration_name!r}")
    raw = migration.get("test_name_rewrites", [])
    if not isinstance(raw, list):
        raise ValueError(
            f"policy split_migrations.{migration_name}."
            "test_name_rewrites must be a list"
        )
    required = {"path", "source_prefix", "destination_prefix"}
    for rule in raw:
        if (
            not isinstance(rule, dict)
            or set(rule) != required
            or not isinstance(rule.get("path"), str)
            or not rule["path"]
            or not isinstance(rule.get("source_prefix"), str)
            or not rule["source_prefix"]
            or not isinstance(rule.get("destination_prefix"), str)
        ):
            raise ValueError(
                f"policy split_migrations.{migration_name}."
                "test_name_rewrites entries require exactly a non-empty path "
                "and source_prefix, plus a string destination_prefix"
            )
    return raw


def split_retained_deletions(
    policy: dict[str, object],
    migration_name: str | None,
) -> dict[str, dict[str, str]]:
    if migration_name is None:
        return {}
    migrations = policy.get("split_migrations")
    if not isinstance(migrations, dict):
        raise ValueError("policy split_migrations must be an object")
    migration = migrations.get(migration_name)
    if not isinstance(migration, dict):
        raise ValueError(f"unknown split migration {migration_name!r}")
    raw = migration.get("retained_deletions", {})
    if not isinstance(raw, dict):
        raise ValueError(
            f"policy split_migrations.{migration_name}.retained_deletions "
            "must be an object"
        )
    required = {"base_sha256", "current_sha256", "path", "reason"}
    for key, retention in raw.items():
        if (
            not isinstance(key, str)
            or not key
            or not isinstance(retention, dict)
            or set(retention) != required
            or not all(
                isinstance(retention.get(field), str)
                and retention[field]
                for field in required
            )
            or any(
                len(retention[field]) != 64
                or any(char not in "0123456789abcdef" for char in retention[field])
                for field in ("base_sha256", "current_sha256")
            )
        ):
            raise ValueError(
                f"policy split_migrations.{migration_name}.retained_deletions."
                f"{key}: expected exact base_sha256, current_sha256, path, "
                "and reason fields with lowercase SHA-256 hashes"
            )
    return raw


def split_reviewed_compositions(
    policy: dict[str, object],
    migration_name: str | None,
) -> dict[str, dict[str, str]]:
    if migration_name is None:
        return {}
    migrations = policy.get("split_migrations")
    if not isinstance(migrations, dict):
        raise ValueError("policy split_migrations must be an object")
    migration = migrations.get(migration_name)
    if not isinstance(migration, dict):
        raise ValueError(f"unknown split migration {migration_name!r}")
    raw = migration.get("reviewed_compositions", {})
    if not isinstance(raw, dict):
        raise ValueError(
            f"policy split_migrations.{migration_name}.reviewed_compositions "
            "must be an object"
        )
    required = {
        "base_sha256",
        "incoming_sha256",
        "current_sha256",
        "path",
        "reason",
    }
    for key, review in raw.items():
        if (
            not isinstance(key, str)
            or not key
            or not isinstance(review, dict)
            or set(review) != required
            or not all(
                isinstance(review.get(field), str) and review[field]
                for field in required
            )
            or any(
                len(review[field]) != 64
                or any(char not in "0123456789abcdef" for char in review[field])
                for field in (
                    "base_sha256",
                    "incoming_sha256",
                    "current_sha256",
                )
            )
        ):
            raise ValueError(
                f"policy split_migrations.{migration_name}."
                f"reviewed_compositions.{key}: expected exact base_sha256, "
                "incoming_sha256, current_sha256, path, and reason fields "
                "with lowercase SHA-256 hashes"
            )
    return raw


def split_module_path_migrations(
    policy: dict[str, object],
    migration_name: str | None,
) -> dict[str, list[str]]:
    if migration_name is None:
        return {}
    migrations = policy.get("split_migrations")
    if not isinstance(migrations, dict):
        raise ValueError("policy split_migrations must be an object")
    migration = migrations.get(migration_name)
    if not isinstance(migration, dict):
        raise ValueError(f"unknown split migration {migration_name!r}")
    raw = migration.get("module_path_migrations", {})
    if (
        not isinstance(raw, dict)
        or any(
            not isinstance(source, str)
            or not source
            or not isinstance(destinations, list)
            or not destinations
            or any(
                not isinstance(destination, str) or not destination
                for destination in destinations
            )
            for source, destinations in raw.items()
        )
    ):
        raise ValueError(
            f"policy split_migrations.{migration_name}.module_path_migrations "
            "must map non-empty source paths to non-empty path lists"
        )
    return raw


def split_container_field_migrations(
    policy: dict[str, object],
    migration_name: str | None,
) -> dict[str, dict[str, dict[str, str]]]:
    if migration_name is None:
        return {}
    migrations = policy.get("split_migrations")
    if not isinstance(migrations, dict):
        raise ValueError("policy split_migrations must be an object")
    migration = migrations.get(migration_name)
    if not isinstance(migration, dict):
        raise ValueError(f"unknown split migration {migration_name!r}")
    raw = migration.get("container_field_migrations", {})
    if not isinstance(raw, dict):
        raise ValueError(
            f"policy split_migrations.{migration_name}."
            "container_field_migrations must be an object"
        )
    for container_key, fields in raw.items():
        if (
            not isinstance(container_key, str)
            or not container_key.startswith("container:")
            or not isinstance(fields, dict)
            or not fields
        ):
            raise ValueError(
                f"policy split_migrations.{migration_name}."
                "container_field_migrations must map container obligations "
                "to non-empty objects"
            )
        for source_field, field_migration in fields.items():
            if (
                not isinstance(source_field, str)
                or not source_field
                or not isinstance(field_migration, dict)
                or set(field_migration) != {"target", "reason"}
                or not isinstance(field_migration.get("target"), str)
                or not field_migration["target"]
                or not isinstance(field_migration.get("reason"), str)
                or not field_migration["reason"]
            ):
                raise ValueError(
                    f"policy split_migrations.{migration_name}."
                    f"container_field_migrations.{container_key}.{source_field} "
                    "must contain exact non-empty target and reason strings"
                )
    return raw


def split_reviewed_resolutions(
    policy: dict[str, object],
    migration_name: str | None,
) -> dict[str, dict[str, object]]:
    if migration_name is None:
        return {}
    migrations = policy.get("split_migrations")
    if not isinstance(migrations, dict):
        raise ValueError("policy split_migrations must be an object")
    migration = migrations.get(migration_name)
    if not isinstance(migration, dict):
        raise ValueError(f"unknown split migration {migration_name!r}")
    raw = migration.get("reviewed_resolutions", {})
    if not isinstance(raw, dict):
        raise ValueError(
            f"policy split_migrations.{migration_name}.reviewed_resolutions "
            "must be an object"
        )
    required = {
        "status",
        "base_sha256",
        "incoming_sha256",
        "current_sha256",
        "path",
        "reason",
    }
    for key, review in raw.items():
        base_sha256 = review.get("base_sha256") if isinstance(review, dict) else None
        hashes = []
        if isinstance(review, dict):
            hashes = [review.get("incoming_sha256"), review.get("current_sha256")]
            if base_sha256 is not None:
                hashes.append(base_sha256)
        if (
            not isinstance(key, str)
            or not key
            or not isinstance(review, dict)
            or set(review) != required
            or review.get("status") not in REVIEWABLE_RESOLUTION_STATUSES
            or base_sha256 is not None and not isinstance(base_sha256, str)
            or not isinstance(review.get("path"), str)
            or not review["path"]
            or not isinstance(review.get("reason"), str)
            or not review["reason"]
            or any(
                not isinstance(value, str)
                or len(value) != 64
                or any(char not in "0123456789abcdef" for char in value)
                for value in hashes
            )
        ):
            raise ValueError(
                f"policy split_migrations.{migration_name}."
                f"reviewed_resolutions.{key}: expected exact reviewable status, "
                "nullable base_sha256, incoming_sha256, current_sha256, path, "
                "and reason fields with lowercase SHA-256 hashes"
            )
    return raw


def split_symbol_call_migrations(
    policy: dict[str, object],
    migration_name: str | None,
) -> dict[str, list[str]]:
    if migration_name is None:
        return {}
    migrations = policy.get("split_migrations")
    if not isinstance(migrations, dict):
        raise ValueError("policy split_migrations must be an object")
    migration = migrations.get(migration_name)
    if not isinstance(migration, dict):
        raise ValueError(f"unknown split migration {migration_name!r}")
    raw = migration.get("symbol_call_migrations", {})
    if (
        not isinstance(raw, dict)
        or any(
            not isinstance(source, str)
            or not source
            or not isinstance(targets, list)
            or not targets
            or any(not isinstance(target, str) or not target for target in targets)
            for source, targets in raw.items()
        )
    ):
        raise ValueError(
            f"policy split_migrations.{migration_name}.symbol_call_migrations "
            "must map non-empty source symbols to non-empty symbol lists"
        )
    return raw


def split_symbol_reference_migrations(
    policy: dict[str, object],
    migration_name: str | None,
) -> dict[str, list[str]]:
    if migration_name is None:
        return {}
    migrations = policy.get("split_migrations")
    if not isinstance(migrations, dict):
        raise ValueError("policy split_migrations must be an object")
    migration = migrations.get(migration_name)
    if not isinstance(migration, dict):
        raise ValueError(f"unknown split migration {migration_name!r}")
    raw = migration.get("symbol_reference_migrations", {})
    if (
        not isinstance(raw, dict)
        or any(
            not isinstance(source, str)
            or not source
            or not isinstance(targets, list)
            or not targets
            or any(not isinstance(target, str) or not target for target in targets)
            for source, targets in raw.items()
        )
    ):
        raise ValueError(
            f"policy split_migrations.{migration_name}."
            "symbol_reference_migrations must map non-empty source symbols "
            "to non-empty symbol lists"
        )
    return raw


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", required=True, help="pinned previous-main base")
    parser.add_argument("--incoming", required=True, help="pinned incoming revision")
    parser.add_argument("--source", help="monolithic Zig source path")
    parser.add_argument(
        "--base-source",
        help="source path at --base when the file was renamed",
    )
    parser.add_argument(
        "--incoming-source",
        help="source path at --incoming when the file was renamed",
    )
    parser.add_argument(
        "--destination",
        action="append",
        help="split destination file or directory; repeat as needed",
    )
    parser.add_argument(
        "--migration",
        help="named split_migrations entry from --policy",
    )
    parser.add_argument(
        "--companion-owner",
        help=(
            "apply the path-scoped declaration_companion_policies entry from "
            "this owning migration; requires explicit matching source/destination"
        ),
    )
    parser.add_argument("--json-out")
    parser.add_argument(
        "--destination-ref",
        help=(
            "read split destinations from this pinned revision instead of "
            "the worktree; read-only pre-merge inventory mode"
        ),
    )
    parser.add_argument(
        "--policy",
        help="policy manifest containing test_name_aliases",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="suppress per-obligation console output; JSON remains complete",
    )
    parser.add_argument(
        "--candidate-dir",
        help="outside-repo directory for reviewed clean candidates",
    )
    parser.add_argument(
        "--candidate-key",
        action="append",
        default=[],
        help=(
            "emit only this exact candidate obligation key; repeat as needed"
        ),
    )
    parser.add_argument(
        "--exclude-candidate-key",
        action="append",
        default=[],
        help=(
            "exclude this exact candidate obligation key; repeat as needed"
        ),
    )
    parser.add_argument(
        "--incoming-replacement-key",
        action="append",
        default=[],
        help=(
            "emit an explicit whole-declaration replacement from pinned "
            "incoming for this exact obligation key; repeat as needed"
        ),
    )
    parser.add_argument("--minimum-similarity", type=float, default=0.70)
    parser.add_argument("--ambiguity-margin", type=float, default=0.05)
    parser.add_argument(
        "--include-unchanged",
        action="store_true",
        help="also verify declarations carried unchanged from the pinned base",
    )
    parser.add_argument(
        "--include-missing-candidates",
        action="store_true",
        help=(
            "emit review-only insertions for uniquely anchored missing declarations"
        ),
    )
    parser.add_argument(
        "--include-review-bodies",
        action="store_true",
        help=(
            "include exact base, incoming, and selected current declaration "
            "bodies in JSON output"
        ),
    )
    args = parser.parse_args()

    if not 0 <= args.minimum_similarity <= 1:
        parser.error("--minimum-similarity must be between 0 and 1")
    if not 0 <= args.ambiguity_margin <= 1:
        parser.error("--ambiguity-margin must be between 0 and 1")
    if args.destination_ref and (
        args.candidate_dir
        or args.include_missing_candidates
        or args.incoming_replacement_key
    ):
        parser.error(
            "--destination-ref is read-only and cannot emit candidates or "
            "reviewed replacements"
        )

    test_name_aliases: dict[str, list[str]] = {}
    declaration_name_aliases: dict[str, list[str]] = {}
    policy: dict[str, object] = {}
    if args.policy:
        policy_path = pathlib.Path(args.policy)
        if not policy_path.is_absolute():
            policy_path = ROOT / policy_path
        policy = load_json_file(policy_path)
        raw_aliases = policy.get("test_name_aliases", {})
        if not isinstance(raw_aliases, dict) or any(
            not isinstance(name, str)
            or not isinstance(aliases, list)
            or any(not isinstance(alias, str) for alias in aliases)
            for name, aliases in raw_aliases.items()
        ):
            parser.error("policy test_name_aliases must map strings to string lists")
        test_name_aliases = raw_aliases
        raw_declaration_aliases = policy.get("declaration_name_aliases", {})
        if not isinstance(raw_declaration_aliases, dict) or any(
            not isinstance(name, str)
            or not isinstance(aliases, list)
            or any(not isinstance(alias, str) for alias in aliases)
            for name, aliases in raw_declaration_aliases.items()
        ):
            parser.error(
                "policy declaration_name_aliases must map strings to string lists"
            )
        declaration_name_aliases = raw_declaration_aliases
    if args.migration and args.companion_owner:
        parser.error("--migration cannot be combined with --companion-owner")
    if args.migration:
        if not args.policy:
            parser.error("--migration requires --policy")
        if args.source or args.destination:
            parser.error("--migration cannot be combined with --source/--destination")
        try:
            args.source, args.destination = split_migration(policy, args.migration)
        except ValueError as exc:
            parser.error(str(exc))
    elif not args.source or not args.destination:
        parser.error("provide --migration or both --source and --destination")

    policy_scope = args.migration
    if args.companion_owner:
        if not args.policy:
            parser.error("--companion-owner requires --policy")
        if len(args.destination) != 1 or args.destination[0] != args.source:
            parser.error(
                "--companion-owner requires one destination identical to --source"
            )
        try:
            policy, policy_scope = companion_policy_scope(
                policy,
                args.companion_owner,
                args.source,
            )
            args.destination = policy["split_migrations"][policy_scope]["destinations"]
        except ValueError as exc:
            parser.error(str(exc))

    base_sha = resolve_ref(args.base)
    incoming_sha = resolve_ref(args.incoming)
    destination_ref = (
        resolve_ref(args.destination_ref) if args.destination_ref else None
    )
    destination_files = expand_destinations(
        args.destination,
        destination_ref,
    )
    try:
        declaration_placements = split_declaration_placements(
            policy,
            policy_scope,
        )
        declaration_placement_ranges = split_declaration_placement_ranges(
            policy,
            policy_scope,
        )
        declaration_owner_migrations = split_declaration_owner_migrations(
            policy,
            policy_scope,
        )
        intentional_declaration_deletions = (
            split_intentional_declaration_deletions(
                policy,
                policy_scope,
            )
        )
        declaration_mixins = split_declaration_mixins(
            policy,
            policy_scope,
        )
        test_name_rewrites = split_test_name_rewrites(
            policy,
            policy_scope,
        )
        retained_deletions = split_retained_deletions(
            policy,
            policy_scope,
        )
        reviewed_compositions = split_reviewed_compositions(
            policy,
            policy_scope,
        )
        reviewed_resolutions = split_reviewed_resolutions(
            policy,
            policy_scope,
        )
        module_path_migrations = split_module_path_migrations(
            policy,
            policy_scope,
        )
        symbol_call_migrations = split_symbol_call_migrations(
            policy,
            policy_scope,
        )
        symbol_reference_migrations = split_symbol_reference_migrations(
            policy,
            policy_scope,
        )
        container_field_migrations = split_container_field_migrations(
            policy,
            policy_scope,
        )
    except ValueError as exc:
        parser.error(str(exc))
    obligations, replacements = analyze(
        args.source,
        base_sha,
        incoming_sha,
        destination_files,
        args.minimum_similarity,
        args.ambiguity_margin,
        args.include_unchanged,
        args.include_missing_candidates,
        test_name_aliases,
        declaration_placements,
        declaration_placement_ranges,
        args.include_review_bodies,
        declaration_owner_migrations,
        declaration_mixins,
        test_name_rewrites,
        set(args.incoming_replacement_key),
        retained_deletions,
        destination_ref,
        reviewed_compositions,
        module_path_migrations,
        symbol_call_migrations,
        reviewed_resolutions,
        symbol_reference_migrations,
        args.base_source,
        args.incoming_source,
        declaration_name_aliases,
        container_field_migrations,
        intentional_declaration_deletions,
    )
    obligation_keys = {item.key for item in obligations}
    unknown_incoming_replacements = (
        set(args.incoming_replacement_key) - obligation_keys
    )
    if unknown_incoming_replacements:
        parser.error(
            "unknown incoming replacement keys: "
            + ", ".join(sorted(unknown_incoming_replacements))
        )
    try:
        replacements = select_candidate_edits(
            replacements,
            args.candidate_key,
            args.exclude_candidate_key,
        )
    except ValueError as exc:
        parser.error(str(exc))
    status_counts = Counter(item.status for item in obligations)
    metadata: dict[str, object] = {
        "base_sha": base_sha,
        "incoming_sha": incoming_sha,
        "source": args.source,
        "base_source": args.base_source or args.source,
        "incoming_source": args.incoming_source or args.source,
        "migration": args.migration,
        "destinations": args.destination,
        "destination_ref": destination_ref,
        "destination_files": [
            str(path.relative_to(ROOT)) for path in destination_files
        ],
        "minimum_similarity": args.minimum_similarity,
        "ambiguity_margin": args.ambiguity_margin,
        "include_unchanged": args.include_unchanged,
        "include_missing_candidates": args.include_missing_candidates,
        "include_review_bodies": args.include_review_bodies,
        "candidate_keys": args.candidate_key,
        "excluded_candidate_keys": args.exclude_candidate_key,
        "incoming_replacement_keys": args.incoming_replacement_key,
        "policy": args.policy,
        "summary": {
            "changed_declarations": len(obligations),
            "changes": dict(
                sorted(Counter(item.change for item in obligations).items())
            ),
            "candidate_declarations": sum(
                len(items) for items in replacements.values()
            ),
            "candidate_files": len(replacements),
            "suggested_placements": sum(
                item.suggested_path is not None for item in obligations
            ),
            "statuses": dict(sorted(status_counts.items())),
        },
        "obligations": [dataclasses.asdict(item) for item in obligations],
    }

    if args.candidate_dir:
        candidate_root = prepare_candidate_root(pathlib.Path(args.candidate_dir))
        write_candidates(candidate_root, replacements, obligations, metadata)
        metadata["candidate_dir"] = str(candidate_root)
    if args.json_out:
        output = pathlib.Path(args.json_out)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")

    print(
        json.dumps(
            metadata["summary"],
            indent=2,
            sort_keys=True,
        )
    )
    if not args.summary_only:
        for obligation in obligations:
            if obligation.status in {
                "exact",
                "integrated",
                "split_visibility_adapted",
                "split_alias_review",
                "carried_branch_changed",
                "clean_candidate",
                "clean_insertion_candidate",
            }:
                continue
            destination = obligation.current_path or "<missing>"
            print(
                f"{obligation.status:28} {obligation.key:72} {destination}"
            )
            if obligation.detail:
                print(f"  {obligation.detail}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
