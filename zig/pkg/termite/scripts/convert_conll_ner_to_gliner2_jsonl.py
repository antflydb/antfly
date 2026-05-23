#!/usr/bin/env python3
"""Convert CoNLL-style token/BIO NER data to GLiNER2 finetune JSONL."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


LABEL_MAP = {
    "PER": "person",
    "ORG": "organization",
    "LOC": "location",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--max-examples", type=int, default=0)
    parser.add_argument("--keep-no-entity", action="store_true")
    parser.add_argument("--include-misc", action="store_true")
    return parser.parse_args()


def iter_sentences(path: Path):
    words: list[str] = []
    tags: list[str] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                if words:
                    yield words, tags
                    words, tags = [], []
                continue
            if line.startswith("-DOCSTART-"):
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            words.append(parts[0])
            tags.append(parts[-1])
    if words:
        yield words, tags


def mapped_label(raw_label: str, include_misc: bool) -> str | None:
    if raw_label == "MISC" and include_misc:
        return "misc"
    return LABEL_MAP.get(raw_label)


def build_example(words: list[str], tags: list[str], include_misc: bool):
    text_parts: list[str] = []
    starts: list[int] = []
    ends: list[int] = []
    cursor = 0
    for word in words:
        if text_parts:
            text_parts.append(" ")
            cursor += 1
        starts.append(cursor)
        text_parts.append(word)
        cursor += len(word)
        ends.append(cursor)

    entities = []
    active_label: str | None = None
    active_start_idx: int | None = None

    def close_entity(end_idx: int) -> None:
        nonlocal active_label, active_start_idx
        if active_label is None or active_start_idx is None:
            return
        start = starts[active_start_idx]
        end = ends[end_idx]
        text = "".join(text_parts)
        entities.append(
            {
                "text": text[start:end],
                "label": active_label,
                "start": start,
                "end": end,
            }
        )
        active_label = None
        active_start_idx = None

    for idx, tag in enumerate(tags):
        if tag == "O" or "-" not in tag:
            close_entity(idx - 1)
            continue
        prefix, raw_label = tag.split("-", 1)
        label = mapped_label(raw_label, include_misc)
        if label is None:
            close_entity(idx - 1)
            continue
        if prefix == "B" or active_label != label:
            close_entity(idx - 1)
            active_label = label
            active_start_idx = idx
        elif prefix != "I":
            close_entity(idx - 1)
    close_entity(len(tags) - 1)

    return {"text": "".join(text_parts), "entities": entities}


def main() -> None:
    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)

    written = 0
    with args.output.open("w", encoding="utf-8") as out:
        for words, tags in iter_sentences(args.input):
            example = build_example(words, tags, args.include_misc)
            if not args.keep_no_entity and not example["entities"]:
                continue
            out.write(json.dumps(example, ensure_ascii=False, separators=(",", ":")))
            out.write("\n")
            written += 1
            if args.max_examples > 0 and written >= args.max_examples:
                break

    print(f"wrote_examples={written}")
    print(f"output={args.output}")


if __name__ == "__main__":
    main()
