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
| MultiWOZ 2.2 train | MIT | active intent, slot given, slot value, slot requested |
| Taskmaster-2 | CC BY 4.0 | field given, field value |
| OpenAssistant oasst2 (English) | Apache 2.0 | reply quality scores, task failure, best reply |
| HelpSteer2 train | CC BY 4.0 | helpfulness, correctness, coherence, complexity, verbosity |
| HelpSteer3 preference train | CC BY 4.0 | better response |

Dialogue sources become typed-decision-style JSON states (a `thread` of
customer and agent turns, cut at a customer turn); response-rating sources
put the prompt and response(s) in the state.
"""

from __future__ import annotations

import argparse
import gzip
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
MULTIWOZ = "https://raw.githubusercontent.com/budzianowski/multiwoz/fe0c8e65cfcd8462bd33c86e35f21addc84ca82b/data/MultiWOZ_2.2"
TASKMASTER = "https://raw.githubusercontent.com/google-research-datasets/Taskmaster/d92cb6af3005f1dc09c39e75e7daf4a04905e00b/TM-2-2020/data"
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
    "multiwoz-schema": (
        f"{MULTIWOZ}/schema.json",
        "ae9e2390f38fb967af64623c2f4f7e0c636fb377ad523b582a03161d3ddbdf68",
    ),
    "multiwoz01": (
        f"{MULTIWOZ}/train/dialogues_001.json",
        "895a8109bf01fa5ecf15ccdbd2dfe1628bd923f6b61dcd2e26b10ee5076a1596",
    ),
    "multiwoz02": (
        f"{MULTIWOZ}/train/dialogues_002.json",
        "2f3ea771d4e01cb2780357738cff7f7496b87d34c221cc240df74501312438d3",
    ),
    "multiwoz03": (
        f"{MULTIWOZ}/train/dialogues_003.json",
        "da24961d28486be2d8462ee4d86a809db819d588ba90ae1a783383d95eb85daa",
    ),
    "multiwoz04": (
        f"{MULTIWOZ}/train/dialogues_004.json",
        "30c1172db1071c853b16215d1946de908d68d2b6ff8da7801de307000a179106",
    ),
    "multiwoz05": (
        f"{MULTIWOZ}/train/dialogues_005.json",
        "eaf58716df5de99524b3e0e7edf477b74749512788a6a51f53f2bdd76768d39a",
    ),
    "multiwoz06": (
        f"{MULTIWOZ}/train/dialogues_006.json",
        "8e75fd543b1964bc5e7118085d977f479c98fcdf6d606b971f67a45fb1745c83",
    ),
    "multiwoz07": (
        f"{MULTIWOZ}/train/dialogues_007.json",
        "02323f8298439d713c6d7d226f4bd7ec246ec993ee11911b54f98cb8a598f206",
    ),
    "multiwoz08": (
        f"{MULTIWOZ}/train/dialogues_008.json",
        "1129fbed480352ae304f0ae5b4915c194e9619c43f1577ccb0d450e10d24ea98",
    ),
    "multiwoz09": (
        f"{MULTIWOZ}/train/dialogues_009.json",
        "87d9e43b1ba51a4a4688703da79d3a09b14d8013570545da24c597daa18e2f45",
    ),
    "multiwoz10": (
        f"{MULTIWOZ}/train/dialogues_010.json",
        "e7ad0d5da2909b08197295e45fe4695b9dc2f67d458374b2aab8db5094e97b26",
    ),
    "multiwoz11": (
        f"{MULTIWOZ}/train/dialogues_011.json",
        "82e2d2900a037b866a01d05974734dd419e89b329fe29ef93b35eea96d27feb8",
    ),
    "multiwoz12": (
        f"{MULTIWOZ}/train/dialogues_012.json",
        "b6bf292325db67682dd7b6fafbf1051cc2262e92f7c37cab213e975888594bb2",
    ),
    "multiwoz13": (
        f"{MULTIWOZ}/train/dialogues_013.json",
        "c33fe4b3952c016e1e1645f472a7097f93dfb476a19940fd9386865ef9adf685",
    ),
    "multiwoz14": (
        f"{MULTIWOZ}/train/dialogues_014.json",
        "ce33dbbf93a40d0dcc671a9d6a2ed1f987914f5b5f05f6df493a5b342efda954",
    ),
    "multiwoz15": (
        f"{MULTIWOZ}/train/dialogues_015.json",
        "d895c0439bc2ad89ef1689896e3be630eadebc33dae77b42a426fd16a271718e",
    ),
    "multiwoz16": (
        f"{MULTIWOZ}/train/dialogues_016.json",
        "3e6bc0bca4262022ccbce0d5ce3150e536e7d21aeb9fbdef9e83cced4dfd124b",
    ),
    "multiwoz17": (
        f"{MULTIWOZ}/train/dialogues_017.json",
        "b6ab2cd9b6e8983364526845b7cbec1749338209bf7ac2313c25e1e2226ebab5",
    ),
    "taskmaster-flights": (
        f"{TASKMASTER}/flights.json",
        "86b37b5ae25f530fd18ced78800d30c3b54f7b34bb208ecb51842718f04e760b",
    ),
    "taskmaster-food-ordering": (
        f"{TASKMASTER}/food-ordering.json",
        "0a042e566a816a5d0abebe6f7e8cfd6abaa89729ffc42f433d327df7342b12f8",
    ),
    "taskmaster-hotels": (
        f"{TASKMASTER}/hotels.json",
        "975b0242f1e37ea1ab94ccedd7e0d6ee5831599d5df1f16143e71110d6c6006a",
    ),
    "taskmaster-movies": (
        f"{TASKMASTER}/movies.json",
        "6f67c9a1f04abc111186e5bcfbe3050be01d0737fd6422901402715bc1f3dd0d",
    ),
    "taskmaster-music": (
        f"{TASKMASTER}/music.json",
        "e5db60d6576fa010bef87a70a8b371d293d48cde8524c1d3ed7c3022f079d95d",
    ),
    "taskmaster-restaurant-search": (
        f"{TASKMASTER}/restaurant-search.json",
        "fb9735f89e7ebc7c877f976da4c30391af6a6277991b597c0755564657ff8f47",
    ),
    "taskmaster-sports": (
        f"{TASKMASTER}/sports.json",
        "8191531bfa5a8426b1508c396ab9886a19c7c620b443c436ec10d8d4708d0eac",
    ),
    "oasst2": (
        f"{HF}/OpenAssistant/oasst2/resolve/179dd21fc55192153d94adb0e0ce8f69e222bf75/2023-11-05_oasst2_ready.trees.jsonl.gz",
        "7a886a16ccfc1173c4f00a6897523e3c95b2785a86ee44a18a98f4f2807ee29b",
    ),
    "helpsteer2": (
        f"{HF}/nvidia/HelpSteer2/resolve/990b2711a36180dd19d9c94b8627844866f8982a/train.jsonl.gz",
        "c0d7e91d738d42e8a08070db26c4c09a9c7631308e1f0fd380ff43d130c9f713",
    ),
    "helpsteer3": (
        f"{HF}/nvidia/HelpSteer3/resolve/f6d145777bcbde96137596340fab89793acd1031/preference/train.jsonl.gz",
        "32b52e1d378f8dab1e4c9ae549da49a5d6fc0875aeafe3f9139e6053beb906bb",
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


def jsonl_gz(name: str) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in gzip.decompress(fetch(name)).decode().split("\n")
        if line.strip()
    ]


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
        json.loads(line) for line in fetch("wanli").decode().split("\n") if line.strip()
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


def thread(turns: list[tuple[str, str]], last: int = 8) -> str:
    """A typed-decisions-style state: the most recent turns of a dialogue."""
    return json.dumps(
        {"thread": [{"role": r, "text": t} for r, t in turns[-last:]]},
        ensure_ascii=False,
    )


def balanced_probe(rng: random.Random, present: list[str], every: list[str]) -> str:
    """Ask about a present item or an absent one equally often, so yes/no
    answers stay balanced (E2 learned a yes prior from skewed questions)."""
    absent = [n for n in every if n not in present]
    if present and (not absent or rng.random() < 0.5):
        return rng.choice(present)
    return rng.choice(absent)


def build_multiwoz(mix: Mix, cap: int) -> None:
    schema = {s["service_name"]: s for s in json.loads(fetch("multiwoz-schema"))}
    intents = {
        i["name"]: i["description"] for s in schema.values() for i in s["intents"]
    }
    intent_names = sorted(intents)
    slots = {slot["name"]: slot for s in schema.values() for slot in s["slots"]}
    dialogues = [d for i in range(1, 18) for d in json.loads(fetch(f"multiwoz{i:02d}"))]
    role = {"USER": "customer", "SYSTEM": "agent"}
    for dialogue in take(mix.rng, dialogues, cap):
        turns = dialogue["turns"]
        users = [i for i, t in enumerate(turns) if t["speaker"] == "USER" and i > 0]
        if not users:
            continue
        t = mix.rng.choice(users)
        frames = [
            f for f in turns[t]["frames"] if f["state"]["active_intent"] != "NONE"
        ]
        # The service this turn is about: the one whose spans or state moved.
        moved = [f for f in frames if f["slots"]] or frames
        if not moved:
            continue
        frame = mix.rng.choice(moved)
        state = frame["state"]
        text = thread([(role[x["speaker"]], x["utterance"]) for x in turns[: t + 1]])
        group = mix.case("multiwoz", f"{dialogue['dialogue_id']}-{t}", text)
        if not group:
            continue
        # A few states name intents the schema lacks (find_taxi).
        if state["active_intent"] in intents:
            mix.choice(
                "multiwoz",
                group,
                text,
                "intent",
                [
                    "What is the customer trying to do right now?",
                    "Which task should the agent work on next?",
                    "Which intent does the customer's latest request belong to?",
                ],
                intent_names,
                intent_names.index(state["active_intent"]),
                lambda n: intents[n],
            )
        # States also name a few slots the schema lacks; ask only about
        # the schema's.
        service = [n for n in slots if n.startswith(frame["service"] + "-")]
        if not service:
            continue
        given = [n for n in service if n in state["slot_values"]]
        probe = balanced_probe(mix.rng, given, service)
        mix.noul(
            "multiwoz",
            group,
            text,
            "slot_given",
            f"Has the customer specified the {slots[probe]['description']}?",
            float(probe in state["slot_values"]),
        )
        categorical = [
            n
            for n in given
            if slots[n]["is_categorical"]
            and state["slot_values"][n][0] in slots[n]["possible_values"]
        ]
        if categorical:
            name = mix.rng.choice(categorical)
            values = slots[name]["possible_values"]
            mix.choice(
                "multiwoz",
                group,
                text,
                "slot_value",
                [f"What {slots[name]['description']} does the customer want?"],
                values,
                values.index(state["slot_values"][name][0]),
            )
        requested = [n for n in state["requested_slots"] if n in slots]
        if requested:
            probe = balanced_probe(mix.rng, requested, service)
            mix.noul(
                "multiwoz",
                group,
                text,
                "slot_requested",
                f"Is the customer asking the agent for the {slots[probe]['description']}?",
                float(probe in requested),
            )


# Taskmaster-2 user annotations worth asking about, with readable names.
TASKMASTER_FIELDS = {
    "flights": {
        "flight_search.destination1": "destination",
        "flight_search.origin": "departure city",
        "flight_search.date.depart_origin": "departure date",
        "flight_search.date.return": "return date",
        "flight_search.time_of_day": "time of day",
        "flight_search.seating_class": "seating class",
        "flight_search.stops": "number of stops",
        "flight_search.price_range": "price range",
        "flight_search.airline": "airline",
        "flight_search.num.pax": "number of passengers",
    },
    "food-ordering": {
        "food_order.name.item": "food item",
        "food_order.type.food": "cuisine",
        "food_order.num.people": "number of people",
        "food_order.time.pickup": "pickup time",
    },
    "hotels": {
        "hotel_search.location.hotel": "hotel location",
        "hotel_search.date_range": "dates of stay",
        "hotel_search.price_range": "price range",
        "hotel_search.num.guests": "number of guests",
        "hotel_search.star_rating": "star rating",
        "hotel_search.type.bed": "bed type",
        "hotel_search.amenity": "amenity",
    },
    "movies": {
        "movie_search.genre": "genre",
        "movie_search.name.movie": "movie title",
        "movie_search.location.theater": "theater location",
        "movie_search.streaming_service": "streaming service",
        "movie_search.time.start": "showtime",
        "movie_ticket.num.tickets": "number of tickets",
    },
    "music": {
        "music.name.artist": "artist",
        "music.name.track": "song",
        "music.name.genre": "genre",
    },
    "restaurant-search": {
        "restaurant.type.food": "cuisine",
        "restaurant.location": "location",
        "restaurant.type.meal": "meal",
        "restaurant.name.restaurant": "restaurant name",
        "restaurant.price_range": "price range",
        "restaurant.rating": "rating",
    },
    "sports": {
        f"{league}.name.team": f"{league.upper()} team"
        for league in ("epl", "mlb", "mls", "nba", "nfl")
    },
}


def build_taskmaster(mix: Mix, cap: int) -> None:
    role = {"USER": "customer", "ASSISTANT": "agent"}
    dialogues = [
        (domain, d)
        for domain in TASKMASTER_FIELDS
        for d in json.loads(fetch(f"taskmaster-{domain}"))
    ]
    # Every value the customers gave per field, for distractors.
    pool: dict[str, set[str]] = {}
    for domain, d in dialogues:
        for u in d["utterances"]:
            for seg in u.get("segments", []) if u["speaker"] == "USER" else []:
                for a in seg["annotations"]:
                    pool.setdefault(a["name"], set()).add(seg["text"].strip().lower())
    for domain, dialogue in take(mix.rng, dialogues, cap):
        fields = TASKMASTER_FIELDS[domain]
        utterances = dialogue["utterances"]
        users = [
            i for i, u in enumerate(utterances) if u["speaker"] == "USER" and i > 0
        ]
        if not users:
            continue
        t = mix.rng.choice(users)
        given: dict[str, str] = {}
        for u in utterances[: t + 1]:
            if u["speaker"] != "USER":
                continue
            for seg in u.get("segments", []):
                for a in seg["annotations"]:
                    if a["name"] in fields:
                        given[a["name"]] = seg["text"].strip().lower()
        text = thread([(role[u["speaker"]], u["text"]) for u in utterances[: t + 1]])
        group = mix.case("taskmaster", f"{dialogue['conversation_id']}-{t}", text)
        if not group:
            continue
        probe = balanced_probe(mix.rng, sorted(given), sorted(fields))
        mix.noul(
            "taskmaster",
            group,
            text,
            "field_given",
            f"Has the customer told the agent the {fields[probe]}?",
            float(probe in given),
        )
        if given:
            name = mix.rng.choice(sorted(given))
            others = sorted(pool[name] - {given[name]})
            k = min(len(others), mix.rng.randint(2, 7))
            names = [given[name]] + mix.rng.sample(others, k)
            mix.choice(
                "taskmaster",
                group,
                text,
                "field_value",
                [
                    f"Which {fields[name]} did the customer ask for?",
                    f"What is the customer's {fields[name]}?",
                ],
                names,
                0,
            )


QUALITY = ["very poor", "poor", "acceptable", "good", "excellent"]
OASST_SCORES = {
    "quality": ("How good is the assistant's reply overall?", QUALITY),
    "helpfulness": (
        "How helpful is the assistant's reply?",
        [
            "not helpful",
            "slightly helpful",
            "somewhat helpful",
            "helpful",
            "very helpful",
        ],
    ),
    "creativity": (
        "How creative is the assistant's reply?",
        [
            "not creative",
            "slightly creative",
            "somewhat creative",
            "creative",
            "very creative",
        ],
    ),
    "humor": (
        "How humorous is the assistant's reply?",
        ["not funny", "slightly funny", "somewhat funny", "funny", "very funny"],
    ),
}


def level_weights(value: float, levels: int) -> list[float]:
    """Annotator mean in [0, 1] as a distribution centered on it, as for
    Civil Comments' toxicity."""
    center = value * (levels - 1)
    return [max(0.0, 1 - abs(level - center)) for level in range(levels)]


def build_oasst2(mix: Mix, cap: int) -> None:
    replies = []
    ranked = []

    def walk(node: dict[str, Any], history: list[tuple[str, str]]) -> None:
        here = history + [
            ("user" if node["role"] == "prompter" else "assistant", node["text"])
        ]
        children = [c for c in node.get("replies", []) if c["lang"] == "en"]
        if node["role"] == "prompter":
            for c in children:
                replies.append((here, c))
            ranks = [c for c in children if c.get("rank") is not None]
            if len(ranks) >= 2:
                ranked.append((here, ranks))
        for c in children:
            walk(c, here)

    for tree in jsonl_gz("oasst2"):
        if tree["prompt"]["lang"] == "en":
            walk(tree["prompt"], [])

    def state(history: list[tuple[str, str]], **extra: Any) -> str:
        return json.dumps(
            {
                "conversation": [{"role": r, "text": t} for r, t in history[-4:]],
                **extra,
            },
            ensure_ascii=False,
        )

    for history, reply in take(mix.rng, replies, cap):
        labels = reply.get("labels") or {}
        scored = [k for k in OASST_SCORES if labels.get(k, {}).get("count", 0) >= 2]
        if not scored:
            continue
        text = state(history, reply=reply["text"])
        group = mix.case("oasst2", reply["message_id"], text)
        if not group:
            continue
        for key in mix.rng.sample(scored, min(2, len(scored))):
            instruction, levels = OASST_SCORES[key]
            mix.score(
                "oasst2",
                group,
                text,
                key,
                instruction,
                levels,
                level_weights(labels[key]["value"], len(levels)),
            )
        # Few replies fail; keep about as many passes as failures.
        failed = labels.get("fails_task", {})
        if failed.get("count", 0) >= 2 and (
            failed["value"] >= 0.5 or mix.rng.random() < 0.08
        ):
            mix.noul(
                "oasst2",
                group,
                text,
                "fails_task",
                "Does the reply fail to do what the user asked?",
                min(max(failed["value"], 0.02), 0.98),
            )
    for history, siblings in take(mix.rng, ranked, cap // 4):
        siblings = siblings[:5]
        mix.rng.shuffle(siblings)
        names = [f"reply_{chr(ord('a') + i)}" for i in range(len(siblings))]
        text = state(history, replies={n: c["text"] for n, c in zip(names, siblings)})
        group = mix.case("oasst2", siblings[0]["parent_id"], text)
        if not group:
            continue
        best = min(range(len(siblings)), key=lambda i: siblings[i]["rank"])
        mix.add(
            "oasst2",
            group,
            text,
            "best_reply",
            "choice",
            "Which reply answers the user best?",
            names,
            ["" for _ in names],
            mix.smoothed(len(names), best),
        )


HELPSTEER2 = {
    "helpfulness": (
        "How helpful is the response to the prompt?",
        [
            "not helpful",
            "slightly helpful",
            "partly helpful",
            "mostly helpful",
            "fully helpful",
        ],
    ),
    "correctness": (
        "How correct and complete is the response?",
        ["mostly wrong", "many errors", "some errors", "minor errors", "fully correct"],
    ),
    "complexity": (
        "How much expertise does it take to write this response?",
        ["anyone", "basic schooling", "high school", "college", "expert"],
    ),
    "verbosity": (
        "How verbose is the response for what was asked?",
        ["very terse", "terse", "balanced", "verbose", "very verbose"],
    ),
}


def build_helpsteer2(mix: Mix, cap: int) -> None:
    for i, row in enumerate(take(mix.rng, jsonl_gz("helpsteer2"), cap)):
        text = json.dumps(
            {"prompt": row["prompt"], "response": row["response"]}, ensure_ascii=False
        )
        group = mix.case("helpsteer2", str(i), text)
        if not group:
            continue
        for key in mix.rng.sample(sorted(HELPSTEER2), 2):
            instruction, levels = HELPSTEER2[key]
            mix.score(
                "helpsteer2",
                group,
                text,
                key,
                instruction,
                levels,
                mix.smoothed(len(levels), int(row[key])),
            )


def build_helpsteer3(mix: Mix, cap: int) -> None:
    rows = [
        r
        for r in jsonl_gz("helpsteer3")
        if r["domain"] != "multilingual" and r["overall_preference"] != 0
    ]
    for i, row in enumerate(take(mix.rng, rows, cap)):
        # Present the two responses in a random order; the preference is
        # -3 (response 1 much better) to 3 (response 2 much better).
        swap = mix.rng.random() < 0.5
        first, second = (
            (row["response2"], row["response1"])
            if swap
            else (
                row["response1"],
                row["response2"],
            )
        )
        text = json.dumps(
            {
                "conversation": [
                    {"role": t["role"], "text": t["content"]}
                    for t in row["context"][-4:]
                ],
                "response_a": first,
                "response_b": second,
            },
            ensure_ascii=False,
        )
        group = mix.case("helpsteer3", str(i), text)
        if not group:
            continue
        pref = -row["overall_preference"] if swap else row["overall_preference"]
        b = 0.5 + 0.15 * pref  # 1 slightly ... 3 much better: 0.65 ... 0.95
        mix.add(
            "helpsteer3",
            group,
            text,
            "better",
            "choice",
            mix.rng.choice(
                [
                    "Which response is better?",
                    "Which response should be shown to the user?",
                ]
            ),
            ["response_a", "response_b"],
            ["", ""],
            [1 - b, b],
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
    "multiwoz": (build_multiwoz, 8400),
    "taskmaster": (build_taskmaster, 17000),
    "oasst2": (build_oasst2, 12000),
    "helpsteer2": (build_helpsteer2, 20000),
    "helpsteer3": (build_helpsteer3, 15000),
}
DEFAULT_SOURCES = "massive,huffpost,banking77,goemotions,civil,sms,wanli,paws"


def fits(
    records: list[dict[str, Any]],
    tokenizer_path: Path,
    max_len: int,
    head_max_len: int = 192,
    truncate: bool = False,
) -> list[dict[str, Any]]:
    """Records whose unpacked Laya sequence fits `max_len`, computed exactly
    as pipelines/laya.zig `questionTokens` and `prepare` do: the
    "<kind> question: ..." head, upstream's option text (including the
    default yes/no descriptions), the shared `head_max_len` budget that caps
    option runs and the head, the state and four special tokens.

    With `truncate`, an overlong state is cut to the tokens that fit instead
    of the record being dropped, as `--truncate-state` does at evaluation."""
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

    def cut(text: str, budget: int) -> str:
        # Re-tokenizing a cut can merge differently at the boundary; keep a
        # small margin so the cut state always fits.
        offsets = tok.encode(text, add_special_tokens=False).offsets
        return text[: offsets[budget - 3][1]]

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
        budget = max_len - 4 - head - options
        if state_tokens[text] <= budget:
            kept.append(record)
        elif truncate and budget > 8:
            kept.append(dict(record, text=cut(text, budget)))
    return kept


def exclusions(paths: list[Path]) -> set[str]:
    texts = set()
    for path in paths:
        for line in path.read_text().split("\n"):
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
    parser.add_argument(
        "--sources",
        default=DEFAULT_SOURCES,
        help=f"comma-separated, from {','.join(BUILDERS)}",
    )
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
    parser.add_argument(
        "--fit",
        type=Path,
        help="instead of building a mix, fit this records file to --max-len",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="cut overlong states to fit instead of dropping their records",
    )
    args = parser.parse_args()
    if args.print_pins:
        for name in SOURCES:
            print(name, hashlib.sha256(fetch(name)).hexdigest())
        return
    if args.output is None:
        parser.error("--output is required")
    if args.output.exists():
        parser.error(f"{args.output} exists")
    if args.fit:
        if not args.tokenizer:
            parser.error("--fit needs --tokenizer")
        # Split on newlines only: texts can hold U+2028 and other line
        # separators that str.splitlines() would break on.
        rows = [
            json.loads(line)
            for line in args.fit.read_text().split("\n")
            if line.strip()
        ]
        kept = fits(rows, args.tokenizer, args.max_len, truncate=args.truncate)
        texts = {r["id"]: r["text"] for r in rows}
        cut = sum(r["text"] != texts[r["id"]] for r in kept)
        with args.output.open("x") as out:
            for record in kept:
                out.write(json.dumps(record, ensure_ascii=False) + "\n")
        print(
            json.dumps({"records": len(rows), "kept": len(kept), "truncated": cut}),
            file=sys.stderr,
        )
        return
    mix = Mix(random.Random(args.seed), args.smoothing, exclusions(args.exclude))
    for name in args.sources.split(","):
        build, cap = BUILDERS[name]
        build(mix, int(cap * args.scale))
        print(f"{name}: {mix.counts[name]} decisions", file=sys.stderr)
    dropped = 0
    if args.tokenizer:
        kept = fits(mix.records, args.tokenizer, args.max_len, truncate=args.truncate)
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
