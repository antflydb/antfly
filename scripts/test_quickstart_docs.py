#!/usr/bin/env python3
"""Release-readiness checks for the public quickstart."""

from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
QUICKSTART = REPO / "docs" / "guides" / "quickstart.mdx"
# Pages on the launch path (home -> install -> docs). Their fenced blocks and
# cURL payloads get the same static checks as the quickstart.
LAUNCH_PAGES = (
    REPO / "docs" / "index.mdx",
    REPO / "docs" / "downloads.mdx",
    REPO / "docs" / "guides" / "inference.mdx",
    REPO / "docs" / "guides" / "hybrid-search.mdx",
)
DOCS_DIR = REPO / "docs"
# Terminology is fixed, not decorative. Prose must not use these forms.
BANNED_PHRASES = {
    "swarm": "say `antfly standalone` or `cluster`; the word swarm does not appear in prose",
    "pre-alpha": "no release hedges next to a download",
    "coming soon": "no scarcity phrasing; say invite-only or say nothing",
    "cutting-edge": "no marketing adjectives in docs",
    "Search AF": "SearchAF is one word",
    "Antfly inference": "the ML engine is `Antfly Inference` (capital I)",
    "Termite": "the ML engine is Antfly Inference, not a separate product",
}
GO_EXAMPLE = REPO / "go" / "pkg" / "sdk" / "examples" / "quickstart" / "main.go"


def fail(message: str) -> None:
    raise AssertionError(message)


def fenced_blocks(source: str) -> list[tuple[str, str]]:
    matches = list(re.finditer(r"^```([^\n]*)\n(.*?)^```\s*$", source, re.MULTILINE | re.DOTALL))
    fence_count = len(re.findall(r"^```", source, re.MULTILINE))
    if fence_count != len(matches) * 2:
        fail(f"unpaired or malformed code fence: found {fence_count} fence markers")
    return [(match.group(1).strip(), match.group(2)) for match in matches]


def check_component_balance(source: str) -> None:
    for component in ("Tabs", "TabsList", "TabsContent", "Steps", "Callout", "Questions"):
        opened = len(re.findall(rf"<{component}(?:\s|>)", source))
        closed = source.count(f"</{component}>")
        if opened != closed:
            fail(f"unbalanced <{component}> components: {opened} open, {closed} close")


def check_shell(block: str, index: object) -> None:
    result = subprocess.run(
        ["bash", "-n"],
        input=block,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        fail(f"bash block {index} does not parse: {result.stderr.strip()}")


def check_json_payloads(source: str) -> None:
    payloads = re.findall(r"\s-d '(\{.*?\})'", source, re.DOTALL)
    if not payloads:
        fail("no cURL JSON payloads found")
    for index, payload in enumerate(payloads, start=1):
        try:
            json.loads(payload)
        except json.JSONDecodeError as error:
            fail(f"cURL JSON payload {index} is invalid: {error}")


def check_page_blocks(path: Path) -> None:
    source = path.read_text(encoding="utf-8")
    check_component_balance(source)
    for index, (language, block) in enumerate(fenced_blocks(source), start=1):
        if language in {"bash", "sh", "shell"}:
            check_shell(block, f"{path.name}#{index}")
        elif language == "python":
            try:
                ast.parse(block)
            except SyntaxError as error:
                fail(f"{path.name} Python block {index} does not parse: {error}")
    for index, payload in enumerate(re.findall(r"\s-d '(\{.*?\})'", source, re.DOTALL), start=1):
        try:
            json.loads(payload)
        except json.JSONDecodeError as error:
            fail(f"{path.name} cURL JSON payload {index} is invalid: {error}")


def check_terminology() -> None:
    # `antfly inference <cmd>` (the CLI) is fine; the banned form is the prose name.
    cli_form = re.compile(r"`[^`]*antfly inference[^`]*`|^\s*antfly inference\b", re.MULTILINE)
    for path in sorted(DOCS_DIR.rglob("*.md*")):
        source = cli_form.sub("", path.read_text(encoding="utf-8"))
        for phrase, reason in BANNED_PHRASES.items():
            for line_number, line in enumerate(source.splitlines(), start=1):
                if phrase in line:
                    fail(f"{path.relative_to(REPO)}:{line_number}: {phrase!r} — {reason}")


def main() -> int:
    for page in LAUNCH_PAGES:
        check_page_blocks(page)
    check_terminology()

    source = QUICKSTART.read_text(encoding="utf-8")
    go_example = GO_EXAMPLE.read_text(encoding="utf-8")

    check_component_balance(source)
    blocks = fenced_blocks(source)
    for index, (language, block) in enumerate(blocks, start=1):
        if language in {"bash", "sh", "shell"}:
            check_shell(block, index)
        elif language == "python":
            try:
                ast.parse(block)
            except SyntaxError as error:
                fail(f"Python block {index} does not parse: {error}")

    check_json_payloads(source)

    forbidden = {
        "'provider': 'inference'": "Python examples must use the antfly provider",
        '"provider": "inference"': "examples must use the antfly provider",
        "strPtr(": "Go snippets must use the high-level SDK",
        "intPtr(": "Go snippets must use the high-level SDK",
        "boolPtr(": "Go snippets must use the high-level SDK",
        "float64Ptr(": "Go snippets must use the high-level SDK",
        "CreateTableWithResponse(": "Go snippets must use the high-level SDK",
        "QueryWithResponse(": "Go snippets must use the high-level SDK",
        "wiki-articles.json ": "the downloaded fixture must be identified as JSONL",
    }
    for token, message in forbidden.items():
        if token in source:
            fail(f"{message}: found {token!r}")

    required = (
        "export PATH=\"$HOME/.local/bin:$PATH\"",
        "wiki-articles.jsonl",
        "2,477 of the 10,000",
        "full_text_index_v0",
        "antfly index wait --table wikipedia",
        "--queryable",
        "physical chunks or vector",
        "## Troubleshooting",
        "standalone inference paths",
    )
    for token in required:
        if token not in source:
            fail(f"quickstart is missing required release guidance: {token!r}")

    if source.count("antfly load --table wikipedia") != 1:
        fail("the quickstart must have exactly one shared Wikipedia ingestion step")
    if source.index("antfly load --table wikipedia") > source.index("## Search"):
        fail("the shared ingestion step must precede every search example")
    load_blocks = [
        block
        for language, block in blocks
        if language in {"bash", "sh", "shell"} and "antfly load --table wikipedia" in block
    ]
    if len(load_blocks) != 1 or "--sync-level full_text" not in load_blocks[0]:
        fail("the shared load must fence full-text visibility without waiting for embeddings")
    wait_blocks = [
        block
        for language, block in blocks
        if language in {"bash", "sh", "shell"} and "antfly index wait" in block
    ]
    if len(wait_blocks) < 2 or any("--queryable" not in block for block in wait_blocks):
        fail("every quickstart index wait must stop at first safe queryability")
    if "rg '" in source:
        fail("quickstart troubleshooting must not require undeclared ripgrep tooling")

    for token in (
        "NewAntflyClient(\"http://localhost:8080\", http.DefaultClient)",
        "client.CreateTable(ctx",
        "client.Query(ctx",
        "client.RetrievalAgent(ctx",
        "DerivedCoveragePolicyPartial",
    ):
        if token not in source or token not in go_example:
            fail(f"Go documentation and compile-checked example must both contain {token!r}")

    print("quickstart documentation checks passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1)
