#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Release-readiness checks for the public quickstart."""

from __future__ import annotations

import ast
import json
import re
import shlex
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
QUICKSTART = REPO / "docs" / "guides" / "quickstart.mdx"
# Pages on the launch path (home -> install -> docs). Their fenced blocks and
# cURL payloads get the same static checks as the quickstart.
LAUNCH_PAGES = (
    REPO / "docs" / "introduction.mdx",
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
QUICKSTART_TAPE = REPO / "quickstart.tape"


def fail(message: str) -> None:
    raise AssertionError(message)


def fenced_blocks(source: str) -> list[tuple[str, str]]:
    matches = list(
        re.finditer(r"^```([^\n]*)\n(.*?)^```\s*$", source, re.MULTILINE | re.DOTALL)
    )
    fence_count = len(re.findall(r"^```", source, re.MULTILINE))
    if fence_count != len(matches) * 2:
        fail(f"unpaired or malformed code fence: found {fence_count} fence markers")
    return [(match.group(1).strip(), match.group(2)) for match in matches]


def check_component_balance(source: str) -> None:
    for component in (
        "Tabs",
        "TabsList",
        "TabsContent",
        "Steps",
        "Callout",
        "Questions",
    ):
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
    for index, payload in enumerate(
        re.findall(r"\s-d '(\{.*?\})'", source, re.DOTALL), start=1
    ):
        try:
            json.loads(payload)
        except json.JSONDecodeError as error:
            fail(f"{path.name} cURL JSON payload {index} is invalid: {error}")


def check_terminology() -> None:
    # `antfly inference <cmd>` (the CLI) is fine; the banned form is the prose name.
    cli_form = re.compile(
        r"`[^`]*antfly inference[^`]*`|^\s*antfly inference\b", re.MULTILINE
    )
    for path in sorted(DOCS_DIR.rglob("*.md*")):
        # Archived implementation logs preserve historical names and commands.
        # Enforce current branding on live documentation, not quoted history.
        if "history" in path.relative_to(DOCS_DIR).parts[:-1]:
            continue
        source = cli_form.sub("", path.read_text(encoding="utf-8"))
        fence = None
        prose = []
        for line in source.splitlines():
            marker = re.match(r"^\s*(`{3,}|~{3,})", line)
            if marker:
                if fence is None:
                    fence = marker[1]
                elif marker[1][0] == fence[0] and len(marker[1]) >= len(fence):
                    fence = None
                prose.append("")
            else:
                prose.append("" if fence else re.sub(r"`+[^`\n]*`+", "", line))
        for phrase, reason in BANNED_PHRASES.items():
            for line_number, line in enumerate(prose, start=1):
                if phrase in line:
                    fail(
                        f"{path.relative_to(REPO)}:{line_number}: {phrase!r}: {reason}"
                    )


def check_recording_coverage_wait(tape: str) -> None:
    # VHS Line waits inspect the cursor's current line. Completion prose can
    # already have scrolled away when the next directive starts, so follow the
    # prompt instead. Bind that wait to its command, not unrelated tape text.
    waits = re.findall(
        r"^Type(?:@\S+)? `([^`\n]*antfly index wait[^`\n]*)`\n"
        r"((?:(?!^Type).*(?:\n|$))*)",
        tape,
        re.MULTILINE,
    )
    if len(waits) != 1:
        fail("the quickstart recording must have one source-coverage wait")
    command, following = waits[0]
    args = shlex.split(command)
    for flags, expected in (
        (("-t", "--table"), "wikipedia"),
        (("-i", "--index"), "title_body"),
        (("--until",), "source-covered=10%"),
        (("--timeout",), "45m"),
    ):
        values = [args[i + 1] for i, arg in enumerate(args[:-1]) if arg in flags]
        if values != [expected]:
            fail(f"the quickstart recording wait requires {flags[-1]} {expected}")
    directives = [
        line.strip()
        for line in following.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if (
        len(directives) < 2
        or directives[0] != "Enter"
        or directives[1] not in {"Wait@2700s /❯/", "Wait+Line@2700s /❯/"}
    ):
        fail("the quickstart recording must wait for the prompt after source coverage")


def main() -> int:
    for page in LAUNCH_PAGES:
        check_page_blocks(page)
    check_terminology()

    source = QUICKSTART.read_text(encoding="utf-8")
    go_example = GO_EXAMPLE.read_text(encoding="utf-8")
    quickstart_tape = QUICKSTART_TAPE.read_text(encoding="utf-8")

    embedding_model = "google/embeddinggemma-2:bf16-safetensors-bundle-v1"
    if source.count(f"antfly inference pull hf:{embedding_model}") != 1:
        fail("install EmbeddingGemma2 once, before indexing; reuse it for image search")
    demo_config = json.loads((REPO / "wikipedia-config.json").read_text())
    for name, config in demo_config["indexes"].items():
        if config["embedder"]["model"] != embedding_model:
            fail(f"the {name} recording index must use the quickstart embedding model")
        if config.get("dimension", 768) != 768:
            fail(f"the {name} recording index must match EmbeddingGemma2's dimensions")
    for label, example in (("quickstart", source), ("Go example", go_example)):
        if embedding_model not in example:
            fail(f"{label} must use EmbeddingGemma2")
        if "clipclap" in example.lower() or "Qwen3-Embedding" in example:
            fail(f"{label} retains a superseded embedding model")

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
        "--streaming=false": "the retrieval CLI uses the --no-streaming switch",
        "eligible-source-covered": "source-covered already excludes skipped documents",
        "Q4_K_M": "the Gemma quickstart must use the published Q4_0 variant",
        "--followup": "the quickstart should request only the grounded answer",
        "--reasoning": "the quickstart should request only the grounded answer",
    }
    for token, message in forbidden.items():
        if token in source:
            fail(f"{message}: found {token!r}")

    required = (
        'export PATH="$HOME/.local/bin:$PATH"',
        "wiki-articles.jsonl",
        "2,446 of the 10,000",
        "hf:ggml-org/Qwen3-Reranker-0.6B-Q8_0-GGUF:gguf:Q8_0 --tasks rerank",
        "full_text_index_v0",
        "antfly index wait --table wikipedia",
        "--until source-covered=10%",
        "Articles can produce multiple chunks",
        "## Troubleshooting",
        "standalone inference paths",
        "--inference-host-budget-mb 8192",
        "--no-streaming",
        "antfly agents query-builder --table wikipedia",
        "--max-internal-iterations 8",
        "Add `--execute`",
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
        if language in {"bash", "sh", "shell"}
        and "antfly load --table wikipedia" in block
    ]
    if len(load_blocks) != 1 or "--sync-level full_text" not in load_blocks[0]:
        fail(
            "the shared load must fence full-text visibility without waiting for embeddings"
        )
    wait_blocks = [
        block
        for language, block in blocks
        if language in {"bash", "sh", "shell"} and "antfly index wait" in block
    ]
    expected_waits = {
        "title_body": "--until source-covered=10%",
        "thumbnail": "--until searchable-artifacts=250",
    }
    if len(wait_blocks) != len(expected_waits):
        fail("the quickstart must have one text wait and one image wait")
    for index, condition in expected_waits.items():
        matching = [block for block in wait_blocks if f"--index {index}" in block]
        if (
            len(matching) != 1
            or condition not in matching[0]
            or "--timeout 45m" not in matching[0]
        ):
            fail(f"the {index} wait must use {condition} with a 45-minute timeout")
    if "rg '" in source:
        fail("quickstart troubleshooting must not require undeclared ripgrep tooling")
    check_recording_coverage_wait(quickstart_tape)
    if "--until searchable-artifacts=1" in quickstart_tape:
        fail("the quickstart recording must not retain the legacy single-artifact wait")
    if "wiki-articles.jsonl" not in quickstart_tape:
        fail("the quickstart recording must load the documented JSONL fixture")
    if "--sync-level full_text" not in quickstart_tape:
        fail("the quickstart recording must fence full-text visibility before waiting")
    if "Wait@1200s /ready/" in quickstart_tape:
        fail("the quickstart recording must not wait for the complete-only ready state")

    for token in (
        'NewAntflyClient("http://127.0.0.1:8080", http.DefaultClient)',
        "client.CreateTable(ctx",
        "client.Query(ctx",
        "client.RetrievalAgent(ctx",
        "DerivedCoveragePolicyPartial",
    ):
        if token not in source or token not in go_example:
            fail(
                f"Go documentation and compile-checked example must both contain {token!r}"
            )

    print("quickstart documentation checks passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1)
