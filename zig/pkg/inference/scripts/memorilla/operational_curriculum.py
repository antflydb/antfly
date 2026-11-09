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

"""Generate frozen counterfactual operational-QA worlds for reader adaptation."""

import argparse
import itertools
import json
from pathlib import Path
import random


def build():
    rng = random.Random(918273)
    syllables = [
        "va",
        "li",
        "to",
        "mer",
        "na",
        "ri",
        "kel",
        "dor",
        "si",
        "fen",
        "ma",
        "lu",
    ]
    names = ["".join(s).capitalize() for s in itertools.product(syllables, repeat=3)]
    rng.shuffle(names)
    pools = {"train": names[:256], "dev": names[256:320], "test": names[320:448]}
    codes = {
        "train": list(range(1000, 7000)),
        "dev": list(range(7000, 8500)),
        "test": list(range(8500, 10000)),
    }
    for values in codes.values():
        rng.shuffle(values)
    rows = []
    for split, count in (("train", 128), ("dev", 8), ("test", 16)):
        for family in (
            "owner",
            "code",
            "two_hop",
            "updated_owner",
            "missing",
            "contradiction",
        ):
            for world in range(count):
                a, b, c = rng.sample(pools[split], 3)
                location = "Site " + str(rng.randrange(10000, 99999))
                label = rng.choice(
                    ["copper", "silver", "blue", "green", "amber", "ivory"]
                )
                item = label + " badge"
                targets = (a, b)
                if family == "owner":
                    question = f"Who owns the {item} at {location}? Give only the name."

                    def documents(owner):
                        return [
                            f"At {location}, {owner} owns the {item}.",
                            f"{c} owns the violet pass at {location}.",
                            f"The {item} grants access to the loading bay.",
                        ]

                elif family == "code":
                    targets = (str(codes[split].pop()), str(codes[split].pop()))
                    question = f"What is the unlock code for the {label} locker at {location}? Give only the code."

                    def documents(code):
                        return [
                            f"The {label} locker at {location} has unlock code {code}.",
                            "The black locker has code 0123.",
                            f"{c} inspects the {label} locker every Friday.",
                        ]

                elif family == "two_hop":
                    question = f"Who carries the crate containing the {label} lantern at {location}? Give only the name."

                    def documents(owner):
                        return [
                            f"At {location}, the {label} lantern is inside crate K47.",
                            f"{owner} carries crate K47 at {location}.",
                            f"{c} carries crate J62 at {location}.",
                        ]

                elif family == "updated_owner":
                    question = f"Who currently owns the {item} at {location}? Give only the name."

                    def documents(owner):
                        return [
                            f"On 2025-01-01, {c} owned the {item} at {location}.",
                            f"On 2025-06-01, ownership of the {item} at {location} changed to {owner}.",
                            f"The badge cabinet at {location} was painted white.",
                        ]

                elif family == "missing":
                    targets = ("UNKNOWN", "UNKNOWN")
                    question = f"Who owns the {item} at {location}? If the sources do not say, answer UNKNOWN."

                    def documents(owner):
                        return [
                            f"{owner} inspects the {item} at {location}.",
                            f"The {item} opens the storage room at {location}.",
                        ]

                else:
                    targets = ("UNKNOWN", "UNKNOWN")
                    question = f"Who owns the {item} at {location}? If the sources conflict, answer UNKNOWN."

                    def documents(owner):
                        return [
                            f"One current record says {a} owns the {item} at {location}.",
                            f"Another equally current record says {b} owns the {item} at {location}.",
                        ]

                for variant, target in enumerate(targets):
                    source = documents(
                        target
                        if family not in ("missing", "contradiction")
                        else (a if variant == 0 else b)
                    )
                    rng.shuffle(source)
                    rows.append(
                        {
                            "id": f"{split}_{family}_{world}_{variant}",
                            "split": split,
                            "family": family,
                            "world": f"{split}_{family}_{world}",
                            "question": question,
                            "documents": source,
                            "answer": target,
                        }
                    )
    return {
        "schema": "antfly.memorilla.operational-curriculum.v1",
        "seed": 918273,
        "gate": "all factual and UNKNOWN answers exact on disjoint final worlds; synthetic scope only",
        "splits": {
            "train": "128 worlds/family, counterfactual pairs",
            "dev": "8 worlds/family, never optimized",
            "test": "16 worlds/family, sealed until development gate",
        },
        "entity_pools": pools,
        "rows": rows,
    }


def build_balanced_codes():
    value = build()
    reserved = {
        r["answer"]
        for r in value["rows"]
        if r["split"] != "train" and r["family"] == "code"
    }
    numbers = [f"{i:04}" for i in range(10000) if f"{i:04}" not in reserved]
    random.Random(602337).shuffle(numbers)
    for row in value["rows"]:
        if row["split"] == "train" and row["family"] == "code":
            target = numbers.pop()
            row["documents"] = [
                text.replace(
                    "unlock code " + row["answer"] + ".", "unlock code " + target + "."
                )
                for text in row["documents"]
            ]
            row["answer"] = target
    value["adaptation"] = {
        "parent_curriculum_sha256": "c8130fa67ab43af90a012ebd9f237ee8fa764af05c6a60217697e53c29cab2cf",
        "change": "Only training code values/sources changed to random four-digit values spanning every digit at every position; all dev/test rows and labels unchanged.",
        "seed": 602337,
    }
    return value


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--balanced-codes", action="store_true")
    args = parser.parse_args()
    with args.output.open("x") as output:
        output.write(
            json.dumps(
                build_balanced_codes() if args.balanced_codes else build(), indent=2
            )
            + "\n"
        )
