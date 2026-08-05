#!/usr/bin/env python3
"""Build synthetic GLiNER2 smoke diagnostics; never use these as release data."""

from __future__ import annotations

import argparse
import itertools
import json
from pathlib import Path


NER_SOURCES = (
    "gliner2_ner_smoke.jsonl",
)

FULL_TASK_SOURCES = (
    "gliner2_ner_smoke.jsonl",
    "gliner2_cls_smoke.jsonl",
    "gliner2_json_smoke.jsonl",
    "gliner2_rel_smoke.jsonl",
    "gliner2_alltask_smoke.jsonl",
    "gliner2_multicount_smoke.jsonl",
)

PROFILES = {
    "ner": NER_SOURCES,
    "full-task": FULL_TASK_SOURCES,
}


def read_jsonl(path: Path) -> list[str]:
    rows: list[str] = []
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            json.loads(line)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"{path}:{line_no}: invalid JSONL: {exc}") from exc
        rows.append(line)
    if not rows:
        raise SystemExit(f"{path}: no JSONL rows")
    return rows


def write_jsonl(path: Path, rows: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--testdata-dir", type=Path, default=Path(__file__).resolve().parents[1] / "testdata")
    parser.add_argument("--out-dir", type=Path, default=Path("/private/tmp/termite-gliner2-smoke-diagnostic"))
    parser.add_argument(
        "--profile",
        choices=sorted(PROFILES),
        default="ner",
        help="diagnostic profile: ner for perf/residency gates, full-task for schema stress",
    )
    parser.add_argument("--train-count", type=int, default=32)
    parser.add_argument("--eval-count", type=int, default=8)
    args = parser.parse_args()

    if args.train_count <= 0 or args.eval_count <= 0:
        parser.error("--train-count and --eval-count must be positive")

    source_names = PROFILES[args.profile]
    fixtures = [args.testdata_dir / name for name in source_names]
    missing = [str(path) for path in fixtures if not path.exists()]
    if missing:
        raise SystemExit("missing GLiNER2 fixture(s):\n  " + "\n  ".join(missing))

    groups = [read_jsonl(path) for path in fixtures]
    train_stream = itertools.cycle(row for group in groups for row in group)
    eval_seed = [group[0] for group in groups]
    eval_stream = itertools.cycle(eval_seed)

    train_rows = [next(train_stream) for _ in range(args.train_count)]
    eval_rows = [next(eval_stream) for _ in range(args.eval_count)]
    write_jsonl(args.out_dir / "train.jsonl", train_rows)
    write_jsonl(args.out_dir / "eval.jsonl", eval_rows)
    print(f"wrote synthetic profile={args.profile} train={len(train_rows)} eval={len(eval_rows)} out_dir={args.out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
