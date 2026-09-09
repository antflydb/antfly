"""Same-binary render-control ablations; keep failed and unequal-output runs."""

import argparse
import copy
import re
from pathlib import Path
from types import SimpleNamespace

from compare import run_subject, save, summarize


def ablation_pair(baseline, candidate):
    """Only the two declared experimental knobs may differ, not memory/quality."""
    candidate = copy.deepcopy(candidate)
    for field in ("render_workers", "render_prefetch"):
        if field not in candidate["provenance"] or field not in baseline["provenance"]:
            raise ValueError(f"Missing ablation control: {field}")
        candidate["provenance"][field] = baseline["provenance"][field]
    if any(
        baseline["provenance"].get(field) != candidate["provenance"].get(field)
        for field in ("binary_sha256", "revision")
    ):
        raise ValueError("Ablations must use the same executable and revision")
    return {"order": [], "main": baseline, "pr": candidate}


def render_observations(log):
    phases = {"pdf_window_grant", "pdf_render_window", "pdf_render", "ocr_batch"}
    rows = []
    for line in log.splitlines():
        if "read-profile" not in line:
            continue
        fields = dict(re.findall(r"(\w+)=([^\s]+)", line))
        if fields.get("phase") in phases:
            rows.append(fields)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--circus-dir", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument(
        "--suite", choices=["text", "throughput", "qualification"], default="text"
    )
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--port", type=int, default=29700)
    parser.add_argument("--profile", action="store_true")
    parser.add_argument(
        "--sync-level", choices=["full_index", "write"], default="full_index"
    )
    parser.add_argument(
        "--workers", type=int, nargs="+", choices=[1, 2, 4, 8], default=[1, 2, 4]
    )
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", args.name):
        parser.error("name must be a single safe directory name")
    if not re.fullmatch(r"[0-9a-f]{40}", args.revision):
        parser.error("revision must be a full commit SHA")
    if min(args.rounds, args.trials, args.timeout) < 1:
        parser.error("rounds, trials and timeout must be positive")
    if len(set(args.workers)) != len(args.workers) or 1 not in args.workers:
        parser.error("workers must be unique and include baseline worker count one")
    args.work_dir = args.work_dir.resolve(strict=True)
    args.circus_dir = args.circus_dir.resolve(strict=True)
    args.binary = args.binary.resolve(strict=True)
    out = args.work_dir / args.name
    out.mkdir()
    save(
        out / "experiment.json",
        {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
    )
    settings = [(w, p) for w in args.workers for p in (1, 0)]
    rounds = []
    for round_index in range(args.rounds):
        current = {}
        rounds.append(current)
        order = settings if round_index % 2 == 0 else list(reversed(settings))
        for workers, prefetch in order:
            label = f"w{workers}-p{prefetch}"
            run_args = SimpleNamespace(**vars(args))
            run_args.name = f"{args.name}-{label}"
            run_args.pr_binary = args.binary
            run_args.pr_revision = args.revision
            run_args.mode = "always"
            run_args.reader_batch_size = 4
            run_args.render_workers = workers
            run_args.render_prefetch = prefetch
            run_args.render_memory_bytes = 268435456
            run_args.read_profile = args.profile
            current[label] = run_subject(run_args, out, round_index, "pr")
            log = (args.work_dir / current[label]["name"] / "antfly.log").read_text()
            current[label]["render_observations"] = render_observations(log)
            save(out / f"round-{round_index:02d}.json", current)
    report = {"baseline": "w1-p1", "profiled": args.profile, "configurations": {}}
    for workers, prefetch in settings:
        label = f"w{workers}-p{prefetch}"
        pairs = [ablation_pair(r["w1-p1"], r[label]) for r in rounds]
        for index, pair in enumerate(pairs):
            pair["order"] = [
                f"{w}/{p}"
                for w, p in (settings if index % 2 == 0 else reversed(settings))
            ]
        result = summarize(pairs, args.trials)
        result["controls"] = {
            "render_workers": workers,
            "render_prefetch": prefetch,
            "render_memory_bytes": 268435456,
        }
        report["configurations"][label] = result
    report["timing_comparable"] = all(
        r["timing_comparable"] for r in report["configurations"].values()
    )
    save(out / "summary.json", report)
    print(
        f"Saved {out / 'summary.json'}; timing_comparable={report['timing_comparable']}",
        flush=True,
    )
    return 0 if report["timing_comparable"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
