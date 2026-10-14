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

"""Generate distinct synthetic capacity fixtures with exact encoder lengths.

These cover the loaded-model token/batch matrix, not a natural-language holdout.
Run with the pinned Fastino reference environment. The collator measures complete
schema + text token IDs; word counts are never substituted for encoded length.
"""

import argparse
import json
from pathlib import Path

from family_oracle import FIXTURES, UPSTREAM_COMMIT, verify_model


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument(
        "--model", choices=("decide_1b", "multi_decide"), default="decide_1b"
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--native-output",
        type=Path,
        help="Boundary worker fixture with model provenance",
    )
    args = parser.parse_args()
    if (args.model == "multi_decide") != (args.native_output is not None):
        parser.error("--native-output is required only for Multi-Decide")
    pin = verify_model(args.model_dir, args.model)
    from transformers import AutoTokenizer
    from gliner2.processor import SchemaTransformer
    from gliner2.inference.schema import Schema

    processor = SchemaTransformer(
        tokenizer=AutoTokenizer.from_pretrained(args.model_dir)
    )
    labels = ["refund", "support", "cancel"]
    schema = Schema().classification("intent", labels).build()

    def size(text):
        batch = processor.collate_fn_inference(
            [(text, schema)], max_len=None, error_policy="raise"
        )
        return len(batch.input_ids[0])

    if size("refund word.") - size("refund.") != 1:
        raise ValueError("filler is not one token in the pinned processor")
    cases = []
    cells = [
        (128, 1),
        (128, 8),
        (512, 1),
        (512, 8),
        (2048, 1),
        (2048, 8),
        (128, 32),
        (128, 64),
    ]
    if args.model == "decide_1b":
        cells += [(4096, 1), (7999, 1)]
    for tokens, batch in cells:
        texts = []
        for row in range(batch):
            cue = labels[row % len(labels)]
            words = ["word"] * (tokens - size(cue + "."))
            words.insert(row, cue)
            text = " ".join(words) + "."
            if size(text) != tokens:
                raise ValueError("encoded length differs from the requested cell")
            texts.append(text)
        if len(set(texts)) != batch:
            raise ValueError("capacity batch contains repeated documents")
        cases.append(
            dict(
                id=f"tokens_{tokens}_batch_{batch}",
                texts=texts,
                tasks={"intent": labels},
            )
        )
    args.output.write_text(
        json.dumps(dict(format_version=1, cases=cases), ensure_ascii=False) + "\n"
    )
    if args.native_output:
        metadata = json.loads((FIXTURES / args.model / "cases.json").read_text())
        metadata.update(
            source_commit=UPSTREAM_COMMIT,
            model_id=pin["model_id"],
            revision=pin["revision"],
            model_files=pin["files"],
            cases=[
                dict(
                    id=case["id"],
                    texts=case["texts"],
                    schema=dict(classifications=[dict(name="intent", labels=labels)]),
                )
                for case in cases
            ],
        )
        args.native_output.write_text(json.dumps(metadata, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
