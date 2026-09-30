#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Where does a distilled Antenna student depart from its teacher's encoder?

Measures the distillation loss's z-space error between a (necked) student and
a GLiNER2.5 teacher per text source: the evaluation datasets' test texts
(each under its own label or type names and under pool-style names) and, with
``--pool``, a distillation pool's validation rows. Word rows and schema-marker
rows are reported apart. Every source is scaled by one per-dimension teacher
std taken over the whole probe (word rows and marker rows separately), so
sources compare directly; ``words_cos``/``markers_cos`` are mean cosines.

    ANTFLY_ANTENNA_DATA=<cache> python gap_probe.py --upstream <GLiNER2> \\
        --student <student dir> --teacher <gliner2.5-base dir> \\
        [--pool <pool>/validation.jsonl] --output <report.json>
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "gliner25"))
sys.path.insert(0, str(HERE))

import oracle

CLASSIFICATION = ("banking77", "ag_news", "clinc150", "sst5")


def run(args: argparse.Namespace) -> list[dict[str, Any]]:
    _, torch = oracle.prepare_runtime(args.upstream)
    import antenna_datasets as datasets
    import distill_pool
    import neck
    from gliner2 import AutoExtractor
    from gliner2.training.data import Classification, InputExample

    rng = random.Random(args.seed)
    kwargs = {
        "local_files_only": True,
        "map_location": "cpu",
        "use_flashdeberta": False,
    }
    student = neck.load(args.student, **kwargs).float().to(args.device).eval()
    teacher = (
        AutoExtractor.from_pretrained(str(args.teacher), **kwargs)
        .float()
        .to(args.device)
        .eval()
    )
    for model in (student, teacher):
        model.processor.change_mode(True)

    pool_labels = sorted(
        {n for d in distill_pool.LABEL_SETS for n in datasets.label_names(d)}
        | {k for d in distill_pool.TYPE_SETS for k in datasets.entity_types(d)}
    )
    pool_types = sorted(
        {k for d in distill_pool.TYPE_SETS for k in datasets.entity_types(d)}
        | set(distill_pool.GENERIC_TYPES)
    )

    def classification(text: str, labels: list[str]) -> Any:
        chosen = rng.sample(labels, min(len(labels), rng.randint(4, 16)))
        return InputExample(
            text=text,
            classifications=[
                Classification(task="intent", labels=chosen, true_label=chosen[0])
            ],
        )

    def entities(text: str, types: list[str]) -> Any:
        return InputExample(
            text=text,
            entities={
                k: [] for k in rng.sample(types, min(len(types), rng.randint(2, 10)))
            },
        )

    def sources():
        for name in CLASSIFICATION:
            records = rng.sample(datasets.load_classification(name, "test"), args.rows)
            own = datasets.label_names(name)
            yield name, "own", [classification(r["text"], own) for r in records]
            yield (
                name,
                "pool",
                [classification(r["text"], pool_labels) for r in records],
            )
        for name in datasets.NER:
            records = rng.sample(datasets.load_ner(name, "test"), args.rows)
            own = datasets.entity_types(name)
            yield name, "own", [entities(r["text"], own) for r in records]
            yield name, "pool", [entities(r["text"], pool_types) for r in records]
        if args.pool:
            rows = [
                json.loads(line)
                for line in args.pool.read_text(encoding="utf-8").splitlines()
            ]
            rows = [r for r in rows if len(r["text"].split()) > 40][: args.rows]
            yield (
                "pool",
                "pool",
                [
                    classification(
                        r["text"], r["schema"]["classifications"][0]["labels"]
                    )
                    if "classifications" in r["schema"]
                    else entities(r["text"], r["schema"]["entities"])
                    for r in rows
                ],
            )

    def measure(examples: list[Any]) -> tuple[Any, Any, Any]:
        """Aligned (student, teacher, is_marker) rows; batches whose routes differ are skipped."""
        kept = []
        for start in range(0, len(examples), 8):
            pairs = [
                (e.to_dict()["input"], e.to_dict()["output"])
                for e in examples[start : start + 8]
            ]
            state = random.getstate()
            sb = student.processor.collate_fn_train(
                pairs, max_len=512, architecture="boundary"
            ).to(args.device)
            random.setstate(state)
            tb = teacher.processor.collate_fn_train(
                pairs, max_len=512, architecture="boundary"
            ).to(args.device)
            with torch.inference_mode():
                sh = student.encoder(
                    input_ids=sb.input_ids, attention_mask=sb.attention_mask
                ).last_hidden_state
                th = teacher.encoder(
                    input_ids=tb.input_ids, attention_mask=tb.attention_mask
                ).last_hidden_state
            s_idx, t_idx = [], []
            for j in range(len(pairs)):
                sw, tw = int(sb.text_word_counts[j]), int(tb.text_word_counts[j])
                sm = [p for group in sb.schema_special_indices[j] for p in group]
                tm = [p for group in tb.schema_special_indices[j] for p in group]
                if sw != tw or len(sm) != len(tm):
                    continue
                s_idx += [
                    (j, int(x), 0) for x in sb.text_word_indices[j, :sw].tolist()
                ] + [(j, x, 1) for x in sm]
                t_idx += [
                    (j, int(x), 0) for x in tb.text_word_indices[j, :tw].tolist()
                ] + [(j, x, 1) for x in tm]
            if not s_idx:
                continue
            si = torch.tensor(s_idx, device=args.device)
            ti = torch.tensor(t_idx, device=args.device)
            kept.append(
                (
                    sh[si[:, 0], si[:, 1]].cpu(),
                    th[ti[:, 0], ti[:, 1]].cpu(),
                    (si[:, 2] == 1).cpu(),
                )
            )
        return (
            torch.cat([k[0] for k in kept]),
            torch.cat([k[1] for k in kept]),
            torch.cat([k[2] for k in kept]),
        )

    measured = [
        (name, schema, measure(examples)) for name, schema, examples in sources()
    ]
    teacher_words = torch.cat([t[~m] for _, _, (_, t, m) in measured])
    teacher_markers = torch.cat([t[m] for _, _, (_, t, m) in measured])
    scale = {
        "words": teacher_words.std(0) + 1e-4,
        "markers": teacher_markers.std(0) + 1e-4,
    }
    report = []
    for name, schema, (s, t, markers) in measured:
        row: dict[str, Any] = {"source": name, "schema": schema}
        for part, selected in (("words", ~markers), ("markers", markers)):
            row[part] = round(
                float(((s[selected] - t[selected]) / scale[part]).pow(2).mean()), 4
            )
            row[part + "_cos"] = round(
                float(
                    torch.nn.functional.cosine_similarity(
                        s[selected], t[selected]
                    ).mean()
                ),
                4,
            )
        report.append(row)
        print(json.dumps(row), flush=True)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--student", type=Path, required=True)
    parser.add_argument("--teacher", type=Path, required=True)
    parser.add_argument(
        "--pool", type=Path, help="a distillation pool's validation.jsonl"
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=200, help="texts per source")
    parser.add_argument("--device", default="mps")
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()
    report = run(args)
    args.output.write_text(json.dumps(report, indent=1) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
