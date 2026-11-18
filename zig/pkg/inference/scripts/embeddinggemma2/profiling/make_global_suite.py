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

"""Prepared-ID fixtures for global attention performance and mask/replay checks."""

import argparse
import json
from pathlib import Path

from tokenizers import Tokenizer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--validation", action="store_true")
    args = parser.parse_args()
    tok = Tokenizer.from_file(str(args.model / "tokenizer.json"))

    def case(name, length, text):
        # Tokenize the complete rendered string: leading spaces change pieces.
        copies = max(1, length // max(1, len(tok.encode(text).ids) - 2) + 2)
        while True:
            ids = tok.encode("title: none | text: " + (text * copies).strip()).ids
            if len(ids) >= length:
                break
            copies *= 2
        ids = ids[: length - 1] + [1]
        assert len(ids) == length
        return {"name": name, "token_ids": ids}

    if args.validation:
        passage = (
            "The field team measured river temperature at sunrise and again after sunset. "
            "Three sensors reported different values, so they checked calibration before drawing conclusions. "
            "A nearby factory had reduced water use during the same week. "
            "The report separates observations from hypotheses and records each instrument's uncertainty. "
            "Meanwhile, the city council debated funding a new railway station and renovating the library. "
        )
        # Both 2048 entries use identical IDs. The intervening 8192 request
        # grows shared attention scratch while the earlier graph stays cached.
        cases = [
            case(f"replay_{index}_{n}", n, passage)
            for index, n in enumerate((2048, 8192, 2048))
        ]
        cases += [case(f"natural_{n}", n, passage) for n in (511, 513, 2049, 8191)]
        padded = case("right_padding_2049", 2049, passage)
        padded["attention_mask"] = [int(i < 1991) for i in range(2049)]
        cases.append(padded)
    else:
        cases = [case(f"text_{n}", n, "river ") for n in (128, 512, 2048, 8192)]
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("x") as out:
        json.dump({"cases": cases}, out)


if __name__ == "__main__":
    main()
