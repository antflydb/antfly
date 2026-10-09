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

# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=15", "tokenizers>=0.19"]
# ///
"""Build a large typed-decision training mix from permissively licensed data.

uv run scripts/laya/build_decision_mix.py --output mix.jsonl [--exclude td/eval.jsonl]

Each source becomes typed questions (choice, score, yes/no) about its texts,
written as native Laya records (finetune/laya/data.zig) with every question
about one text sharing a `group_id`, so packed training shares the state.
Instructions and option descriptions vary across a few phrasings, and large
label sets are sampled down to 3-12 options that always include the answer.

Targets are label-smoothed gold (`--smoothing`, 0.1 by default), except
where a source carries annotator agreement (Civil Comments), which is used
directly. A teacher can later replace or blend targets
(prepare_laya_longcontext_teacher.py).

Every file comes from a pinned revision and is checked against its SHA-256
(--print-pins prints the digests of the files as fetched).
Licenses are all permissive; CC BY-SA sets are deliberately left out, and so
are CLINC150 and SST-5, which Antenna holds out for evaluation. Texts that
appear in any --exclude file (typed-decisions test, Banking77 test) are
dropped.

| Source | License | Questions |
| --- | --- | --- |
| MASSIVE intents (via antenna_training_sets) | Apache 2.0 | intent, scenario |
| HuffPost News Category (via antenna_training_sets) | CC BY 4.0 | topic |
| Banking77 train (via antenna_datasets) | CC BY 4.0 | intent |
| GoEmotions (simplified) | Apache 2.0 | emotion, one emotion yes/no |
| Civil Comments | CC0 1.0 | toxicity level, insult and threat yes/no |
| SMS Spam Collection (UCI) | CC BY 4.0 | spam yes/no |
| WANLI | CC BY 4.0 | inference relation, entailment yes/no |
| PAWS (labeled_final) | free with attribution to Google LLC | paraphrase yes/no |
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import random
import re
import sys
import urllib.request
import zipfile
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Iterable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "zig/pkg/inference/scripts/antenna"))

HF = "https://huggingface.co/datasets"
SOURCES: dict[str, tuple[str, str | None]] = {
    "goemotions": (
        f"{HF}/google-research-datasets/go_emotions/resolve/add492243ff905527e67aeb8b80c082af02207c3/simplified/train-00000-of-00001.parquet",
        "b7d74279616ae7c9b8374ab62ea9f9d6504d36a577bb17f745d720dc2b0d4e76",
    ),
    "civil0": (
        f"{HF}/google/civil_comments/resolve/f2970eb3a55777454c94069077cc8d9b5866312d/data/train-00000-of-00002.parquet",
        "c20f01c3aecbdd942886cacb0ee67e995df33bc712a622fde75927ed3d6ccefe",
    ),
    "wanli": (
        f"{HF}/alisawuffles/WANLI/resolve/61c95318fd71c55b6ba355d76253254615f387ec/train.jsonl",
        "85058cf017a911e89242dc29fa0a4ddaad3664cb923dc0a82145fdda14b694e5",
    ),
    "paws": (
        f"{HF}/google-research-datasets/paws/resolve/161ece9501cf0a11f3e48bd356eaa82de46d6a09/labeled_final/train-00000-of-00001.parquet",
        "8dc9ad3e5f30ad9a86b290fe236d528ef23a5751fec9a35d99cbacf68ba277cf",
    ),
    "sms": (
        "https://archive.ics.uci.edu/static/public/228/sms+spam+collection.zip",
        "1587ea43e58e82b14ff1f5425c88e17f8496bfcdb67a583dbff9eefaf9963ce3",
    ),
}

GOEMOTIONS = (
    "admiration amusement anger annoyance approval caring confusion curiosity desire "
    "disappointment disapproval disgust embarrassment excitement fear gratitude grief joy love "
    "nervousness optimism pride realization relief remorse sadness surprise neutral"
).split()


def cache_dir() -> Path:
    import antenna_datasets

    return antenna_datasets.cache_dir()


def fetch(name: str) -> bytes:
    url, expected = SOURCES[name]
    path = cache_dir() / hashlib.sha256(url.encode()).hexdigest()
    if path.exists():
        data = path.read_bytes()
    else:
        with urllib.request.urlopen(url, timeout=300) as response:
            data = response.read()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.with_suffix(".partial").write_bytes(data)
        path.with_suffix(".partial").replace(path)
    digest = hashlib.sha256(data).hexdigest()
    if expected is not None and digest != expected:
        raise ValueError(f"SHA-256 mismatch for {name}: {digest} != {expected}")
    return data


def parquet(name: str) -> list[dict[str, Any]]:
    import pyarrow.parquet as pq

    return pq.read_table(io.BytesIO(fetch(name))).to_pylist()


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().lower()


class Mix:
    """Accumulates records; one case (group) per text."""

    def __init__(self, rng: random.Random, smoothing: float, exclude: set[str]):
        self.rng, self.smoothing, self.exclude = rng, smoothing, exclude
        self.records: list[dict[str, Any]] = []
        self.seen: set[str] = set()
        self.counts: Counter[str] = Counter()

    def case(self, source: str, key: str, text: str) -> str | None:
        text = text.strip()
        norm = normalize(text)
        if not text or norm in self.exclude or norm in self.seen:
            return None
        self.seen.add(norm)
        # Source row ids are not always unique; the case count always is.
        return f"{source}/{len(self.seen)}-{key}"

    def smoothed(self, n: int, gold: int) -> list[float]:
        eps = self.smoothing
        return [(1 - eps) * (i == gold) + eps / n for i in range(n)]

    def add(
        self,
        source: str,
        group: str,
        text: str,
        qid: str,
        kind: str,
        instruction: str,
        labels: list[str],
        descriptions: list[str],
        target: list[float],
    ) -> None:
        total = sum(target)
        self.records.append(
            {
                "id": f"{group}/{qid}",
                "group_id": group,
                "text": text.strip(),
                "kind": kind,
                "instruction": instruction,
                "labels": labels,
                "descriptions": descriptions,
                "target": [t / total for t in target],
            }
        )
        self.counts[source] += 1

    def choice(
        self,
        source: str,
        group: str,
        text: str,
        qid: str,
        instructions: list[str],
        names: list[str],
        gold: int,
        describe: Callable[[str], str] = lambda n: "",
    ) -> None:
        """A choice question over a sample of `names` that contains the answer."""
        k = min(len(names), self.rng.randint(3, 12))
        others = [i for i in range(len(names)) if i != gold]
        picked = self.rng.sample(others, k - 1) + [gold]
        self.rng.shuffle(picked)
        labels = [re.sub(r"\W+", "_", names[i].lower()).strip("_") for i in picked]
        if len(set(labels)) != len(labels):
            return
        self.add(
            source,
            group,
            text,
            qid,
            "choice",
            self.rng.choice(instructions),
            labels,
            [describe(names[i]) for i in picked],
            self.smoothed(k, picked.index(gold)),
        )

    def noul(
        self,
        source: str,
        group: str,
        text: str,
        qid: str,
        instruction: str,
        yes: float,
        true_desc: str = "",
        false_desc: str = "",
    ) -> None:
        """A yes/no question; `yes` is the probability of true (gold 0/1 is smoothed)."""
        if yes in (0.0, 1.0):
            yes = self.smoothed(2, int(yes))[1]
        self.add(
            source,
            group,
            text,
            qid,
            "noul",
            instruction,
            ["false", "true"],
            [false_desc, true_desc],
            [1 - yes, yes],
        )

    def score(
        self,
        source: str,
        group: str,
        text: str,
        qid: str,
        instruction: str,
        levels: list[str],
        target: list[float],
    ) -> None:
        self.add(
            source,
            group,
            text,
            qid,
            "score",
            instruction,
            [str(i) for i in range(len(levels))],
            levels,
            target,
        )


def label_index(names: list[str], label: Any) -> int:
    """Antenna's loaders give label names; parquet sources give indices."""
    return names.index(label) if isinstance(label, str) else int(label)


def take(rng: random.Random, rows: list[Any], cap: int) -> list[Any]:
    rows = list(rows)
    rng.shuffle(rows)
    return rows[:cap]


def intent_source(
    mix: Mix,
    source: str,
    rows: Iterable[dict[str, Any]],
    names: list[str],
    cap: int,
    what: str,
) -> None:
    instructions = [
        f"Which {what} best matches this request?",
        f"What is the user's {what}?",
        f"Classify the {what} of this message.",
    ]
    for row in take(mix.rng, rows, cap):
        group = mix.case(source, str(row["id"]), row["text"])
        if group:
            mix.choice(
                source,
                group,
                row["text"],
                what.replace(" ", "_"),
                instructions,
                names,
                label_index(names, row["label"]),
            )


def build_massive(mix: Mix, cap: int) -> None:
    import antenna_training_sets as sets

    names = sets.label_names("massive_intent")
    rows = sets.load_classification("massive_intent")
    scenarios = sorted({n.split(" ")[0] for n in names})
    for row in take(mix.rng, rows, cap):
        group = mix.case("massive", str(row["id"]), row["text"])
        if not group:
            continue
        mix.choice(
            "massive",
            group,
            row["text"],
            "intent",
            [
                "What does the user want to do?",
                "Which intent fits this request?",
                "Classify the user's intent.",
            ],
            names,
            label_index(names, row["label"]),
        )
        scenario = names[label_index(names, row["label"])].split(" ")[0]
        mix.choice(
            "massive",
            group,
            row["text"],
            "scenario",
            [
                "Which area does this request belong to?",
                "What domain is this request about?",
            ],
            scenarios,
            scenarios.index(scenario),
        )


def build_huffpost(mix: Mix, cap: int) -> None:
    import antenna_training_sets as sets

    intent_source(
        mix,
        "huffpost",
        sets.load_classification("huffpost"),
        sets.label_names("huffpost"),
        cap,
        "news topic",
    )


def build_banking77(mix: Mix, cap: int) -> None:
    import antenna_datasets

    rows = [
        dict(r, id=f"train{i}")
        for i, r in enumerate(
            antenna_datasets.load_classification("banking77", "train")
        )
    ]
    intent_source(
        mix,
        "banking77",
        rows,
        antenna_datasets.label_names("banking77"),
        cap,
        "banking intent",
    )


def build_goemotions(mix: Mix, cap: int) -> None:
    rows = [r for r in parquet("goemotions") if len(r["labels"]) == 1]
    for row in take(mix.rng, rows, cap):
        group = mix.case("goemotions", row["id"], row["text"])
        if not group:
            continue
        gold = row["labels"][0]
        mix.choice(
            "goemotions",
            group,
            row["text"],
            "emotion",
            [
                "Which emotion does the writer express?",
                "What is the main emotion in this text?",
            ],
            GOEMOTIONS,
            gold,
        )
        probe = gold if mix.rng.random() < 0.5 else mix.rng.randrange(len(GOEMOTIONS))
        if GOEMOTIONS[probe] != "neutral":
            mix.noul(
                "goemotions",
                group,
                row["text"],
                "expresses",
                f"Does the writer express {GOEMOTIONS[probe]}?",
                float(probe == gold),
            )


TOXICITY = [
    "not toxic",
    "slightly toxic",
    "moderately toxic",
    "very toxic",
    "extremely toxic",
]


def build_civil(mix: Mix, cap: int) -> None:
    rows = parquet("civil0")
    # Keep toxic comments well represented: about half the sample is toxic.
    toxic = [r for r in rows if r["toxicity"] >= 0.3]
    clean = [r for r in rows if r["toxicity"] < 0.3]
    picked = take(mix.rng, toxic, cap // 2) + take(
        mix.rng, clean, cap - min(len(toxic), cap // 2)
    )
    for i, row in enumerate(picked):
        text = row["text"]
        if len(text) > 1500:
            continue
        group = mix.case("civil", str(i), text)
        if not group:
            continue
        # Annotator agreement as a distribution over five levels, centered on
        # the toxic fraction.
        center = row["toxicity"] * (len(TOXICITY) - 1)
        weights = [max(0.0, 1 - abs(level - center)) for level in range(len(TOXICITY))]
        mix.score(
            "civil",
            group,
            text,
            "toxicity",
            mix.rng.choice(
                ["How toxic is this comment?", "Rate the toxicity of this comment."]
            ),
            TOXICITY,
            weights,
        )
        mix.noul(
            "civil",
            group,
            text,
            "insult",
            "Is this comment insulting?",
            min(max(row["insult"], 0.02), 0.98),
        )
        if row["threat"] > 0 or mix.rng.random() < 0.2:
            mix.noul(
                "civil",
                group,
                text,
                "threat",
                "Does this comment threaten anyone?",
                min(max(row["threat"], 0.02), 0.98),
            )


def build_sms(mix: Mix, cap: int) -> None:
    with zipfile.ZipFile(io.BytesIO(fetch("sms"))) as archive:
        lines = (
            archive.read("SMSSpamCollection").decode("utf-8", "replace").splitlines()
        )
    rows = [line.split("\t", 1) for line in lines if "\t" in line]
    for i, (label, text) in enumerate(take(mix.rng, rows, cap)):
        group = mix.case("sms", str(i), text)
        if group:
            mix.noul(
                "sms",
                group,
                text,
                "spam",
                mix.rng.choice(
                    [
                        "Is this message spam?",
                        "Is this an unsolicited or promotional message?",
                    ]
                ),
                float(label == "spam"),
            )


NLI = {
    "entailment": "the hypothesis must be true given the premise",
    "neutral": "the hypothesis may or may not be true",
    "contradiction": "the hypothesis cannot be true given the premise",
}


def build_wanli(mix: Mix, cap: int) -> None:
    rows = [
        json.loads(line)
        for line in fetch("wanli").decode().splitlines()
        if line.strip()
    ]
    names = list(NLI)
    for row in take(mix.rng, rows, cap):
        text = f"Premise: {row['premise']}\nHypothesis: {row['hypothesis']}"
        group = mix.case("wanli", str(row.get("pairID", row.get("id"))), text)
        if not group or row["gold"] not in NLI:
            continue
        gold = names.index(row["gold"])
        mix.add(
            "wanli",
            group,
            text,
            "relation",
            "choice",
            mix.rng.choice(
                [
                    "How does the hypothesis relate to the premise?",
                    "Does the premise support, contradict, or leave open the hypothesis?",
                ]
            ),
            names,
            [NLI[n] for n in names],
            mix.smoothed(3, gold),
        )
        mix.noul(
            "wanli",
            group,
            text,
            "entails",
            "Does the premise imply the hypothesis?",
            float(row["gold"] == "entailment"),
        )


def build_paws(mix: Mix, cap: int) -> None:
    for row in take(mix.rng, parquet("paws"), cap):
        text = f"Sentence A: {row['sentence1']}\nSentence B: {row['sentence2']}"
        group = mix.case("paws", str(row["id"]), text)
        if group:
            mix.noul(
                "paws",
                group,
                text,
                "paraphrase",
                mix.rng.choice(
                    [
                        "Do the two sentences mean the same thing?",
                        "Is sentence B a paraphrase of sentence A?",
                    ]
                ),
                float(row["label"] == 1),
            )


BUILDERS: dict[str, tuple[Callable[[Mix, int], None], int]] = {
    "massive": (build_massive, 11000),
    "huffpost": (build_huffpost, 20000),
    "banking77": (build_banking77, 10000),
    "goemotions": (build_goemotions, 20000),
    "civil": (build_civil, 12000),
    "sms": (build_sms, 5574),
    "wanli": (build_wanli, 12000),
    "paws": (build_paws, 15000),
}


def fits(
    records: list[dict[str, Any]],
    tokenizer_path: Path,
    max_len: int,
    head_max_len: int = 192,
) -> list[dict[str, Any]]:
    """Records whose unpacked Laya sequence fits `max_len`, computed exactly
    as pipelines/laya.zig `questionTokens` and `prepare` do: the
    "<kind> question: ..." head, upstream's option text (including the
    default yes/no descriptions), the shared `head_max_len` budget that caps
    option runs and the head, the state and four special tokens."""
    from tokenizers import Tokenizer

    tok = Tokenizer.from_file(str(tokenizer_path))

    def count(text: str) -> int:
        return len(
            tok.encode(text.replace("[MASK]", " "), add_special_tokens=False).ids
        )

    def option_text(kind: str, i: int, label: str, desc: str) -> str:
        if kind == "choice":
            return f" {label}: {desc}" if desc else f" {label}"
        if kind == "score":
            return f" level {i}: {desc or label}"
        default = (
            "no, the statement does not hold" if i == 0 else "yes, the statement holds"
        )
        return f" {label}: {desc or default}"

    state_tokens: dict[str, int] = {}
    kept = []
    for record in records:
        text, kind = record["text"], record["kind"]
        if text not in state_tokens:
            state_tokens[text] = count(text)
        lengths = [
            count(option_text(kind, i, label, desc))
            for i, (label, desc) in enumerate(
                zip(record["labels"], record["descriptions"])
            )
        ]
        options = sum(1 + min(n, 48) for n in lengths)
        per = (
            max(4, (head_max_len - 16) // len(lengths))
            if options + 16 > head_max_len
            else 49
        )
        options = sum(min(1 + min(n, 48), per) for n in lengths)
        head = min(
            count(f"{kind} question: {record['instruction']}"),
            max(8, head_max_len - options),
        )
        if 4 + head + options + state_tokens[text] <= max_len:
            kept.append(record)
    return kept


def exclusions(paths: list[Path]) -> set[str]:
    texts = set()
    for path in paths:
        for line in path.read_text().splitlines():
            if line.strip():
                row = json.loads(line)
                state = row.get("text", row.get("state", ""))
                texts.add(
                    normalize(
                        state
                        if isinstance(state, str)
                        else json.dumps(state, ensure_ascii=False)
                    )
                )
    return texts


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--exclude",
        type=Path,
        action="append",
        default=[],
        help="records or cases whose texts must not be trained on",
    )
    parser.add_argument("--sources", default=",".join(BUILDERS))
    parser.add_argument(
        "--scale", type=float, default=1.0, help="multiply every source cap"
    )
    parser.add_argument("--smoothing", type=float, default=0.1)
    parser.add_argument("--seed", type=int, default=20261004)
    parser.add_argument("--print-pins", action="store_true")
    parser.add_argument(
        "--tokenizer",
        type=Path,
        help="student tokenizer.json; drops records that would not fit --max-len",
    )
    parser.add_argument("--max-len", type=int, default=512)
    args = parser.parse_args()
    if args.print_pins:
        for name in SOURCES:
            print(name, hashlib.sha256(fetch(name)).hexdigest())
        return
    if args.output is None:
        parser.error("--output is required")
    if args.output.exists():
        parser.error(f"{args.output} exists")
    mix = Mix(random.Random(args.seed), args.smoothing, exclusions(args.exclude))
    for name in args.sources.split(","):
        build, cap = BUILDERS[name]
        build(mix, int(cap * args.scale))
        print(f"{name}: {mix.counts[name]} decisions", file=sys.stderr)
    dropped = 0
    if args.tokenizer:
        kept = fits(mix.records, args.tokenizer, args.max_len)
        dropped = len(mix.records) - len(kept)
        mix.records = kept
        mix.counts = Counter(r["group_id"].split("/", 1)[0] for r in kept)
    mix.rng.shuffle(mix.records)
    with args.output.open("x") as out:
        for record in mix.records:
            out.write(json.dumps(record, ensure_ascii=False) + "\n")
    kinds = Counter(r["kind"] for r in mix.records)
    meta = {
        "records": len(mix.records),
        "cases": len({r["group_id"] for r in mix.records}),
        "by_source": dict(mix.counts),
        "by_kind": dict(kinds),
        "seed": args.seed,
        "smoothing": args.smoothing,
        "sources": {name: url for name, (url, _) in SOURCES.items()},
        "excluded_texts": len(mix.exclude),
        "dropped_overlong": dropped,
        "max_len": args.max_len if args.tokenizer else None,
    }
    args.output.with_suffix(".json").write_text(json.dumps(meta, indent=2) + "\n")
    print(json.dumps(meta, indent=2), file=sys.stderr)


if __name__ == "__main__":
    main()
