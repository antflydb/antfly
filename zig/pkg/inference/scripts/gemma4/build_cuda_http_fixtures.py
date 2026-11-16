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

"""Rebuild the Gemma HTTP workloads from a retained Gutenberg pg1228 snapshot.

Text sampling follows the supplied termite-circus recipe (seed 20261103).
Media uses the repository's three OCR images and English/Spanish WAV fixtures;
these are substitutes for the unavailable release benchmark media snapshots.
The source snapshot and every media file are hashed in the output provenance.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import random
import re


def paragraphs(text):
    return [p for value in re.split(r"\n\s*\n", text)
            if (p := re.sub(r"\s+", " ", value).strip())]


def text_cells(snapshot):
    text = snapshot.read_text(encoding="utf-8")
    body = text[text.index("*** START OF THE PROJECT GUTENBERG"):text.index("*** END OF THE PROJECT GUTENBERG")]
    introductions = list(re.finditer(r"^INTRODUCTION\.\s*$", body, re.M))
    if len(introductions) < 2:
        raise ValueError("snapshot is missing the expected introduction markers")
    index = re.search(r"^INDEX\.\s*$", body, re.M)
    if index is None:
        raise ValueError("snapshot is missing its index marker")
    body = body[introductions[1].start():index.start()]
    headings = list(re.finditer(r"^(INTRODUCTION\.|CHAPTER [IVXL]+\.)\s*$", body, re.M))
    if len(headings) != 15:
        raise ValueError("expected fifteen book sections")
    sections = [body[h.start():headings[i + 1].start() if i + 1 < len(headings) else len(body)].replace("\r", "").strip()
                for i, h in enumerate(headings)]
    all_paragraphs = [p for section in sections for p in paragraphs(section)[1:]]
    sentences = [s for p in all_paragraphs for s in re.split(r"(?<=[.!?])\s+(?=[A-Z])", p)
                 if 8 <= len(s.split()) <= 60]
    selected_paragraphs = [p for p in all_paragraphs if 60 <= len(p.split()) <= 250]
    words = " ".join(all_paragraphs).split()
    pages = [" ".join(words[i:i + 350]) for i in range(0, len(words) - 350, 350)]
    randomizer = random.Random(20261103)
    # Preserve all three shuffles: the discarded paragraph shuffle advances
    # the same PRNG state as the original corpus recipe before page sampling.
    for values in (sentences, selected_paragraphs, pages):
        randomizer.shuffle(values)
    if len(sentences) < 12 or len(pages) < 12:
        raise ValueError("snapshot is too short")
    chapter_words = " ".join(sections).split()
    if len(chapter_words) < 9000:
        raise ValueError("snapshot is too short for six long requests")
    return [
        {"id": "text_short_256", "max_tokens": 256, "warmup": 2,
         "calls": [{"text": f"Explain this sentence to a curious twelve-year-old:\n\n{s}"} for s in sentences[:12]]},
        {"id": "page_summary_128", "max_tokens": 128, "warmup": 1,
         "calls": [{"text": f"Summarise this passage in one paragraph.\n\n{s}"} for s in pages[:12]]},
        {"id": "long2k_summary_64", "max_tokens": 64, "warmup": 1,
         "calls": [{"text": "Summarise the main argument of this passage in two sentences.\n\n" +
                            " ".join(chapter_words[i * 1500:(i + 1) * 1500])} for i in range(6)]},
    ]


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--darwin-snapshot", required=True, type=Path)
    parser.add_argument("--expected-sha256", help="Reject a different retained source snapshot")
    parser.add_argument("--output", required=True, type=Path, help="Fresh output directory")
    args = parser.parse_args()
    digest = sha256(args.darwin_snapshot)
    if args.expected_sha256 and digest != args.expected_sha256:
        parser.error("retained source checksum mismatch")
    repo = Path(__file__).resolve().parents[5]
    images = [repo / "zig/pkg/inference/scripts/lightonocr3/corpus" / (name + ".png")
              for name in ("letter", "receipt", "shipping")]
    audio = [repo / "zig/e2e/inference/testdata" / (name + ".wav")
             for name in ("whisper_quality", "whisper_spanish_quality")]
    cells = text_cells(args.darwin_snapshot)
    cells += [
        {"id": "image_describe_64", "max_tokens": 64, "warmup": 1,
         "calls": [{"text": "Describe this image in one paragraph.", "image": str(p)} for p in images]},
        {"id": "audio_transcribe_64", "max_tokens": 64, "warmup": 1,
         "calls": [{"text": "Transcribe this audio clip.", "audio": str(p)} for p in audio]},
    ]
    provenance = {"seed": 20261103, "darwin_sha256": digest,
                  "source_url": "https://www.gutenberg.org/cache/epub/1228/pg1228.txt",
                  "media_note": "Repository fixtures substitute for the unavailable release media snapshots.",
                  "media_sha256": {str(p.relative_to(repo)): sha256(p) for p in images + audio}}
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "campaign.json").write_text(json.dumps(cells, ensure_ascii=False, indent=2) + "\n")
    (args.output / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    print(json.dumps({"requests": sum(len(c["calls"]) for c in cells), "darwin_sha256": digest}))


if __name__ == "__main__":
    main()
