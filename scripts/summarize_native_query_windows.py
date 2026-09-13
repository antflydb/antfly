"""Attribute sampled native admission/maintenance metrics to timed VDBBench waves."""

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

PREFIXES = (
    "antfly_dense_search_admission_",
    "antfly_dense_rerank_admission_",
    "antfly_dense_checkpoint_completion_rounds_total",
)


def windows(lines, timezone):
    starts = {}
    found = []
    for line in lines:
        start = re.search(
            r"Syncing all process and start concurrency search, concurrency=(\d+)", line
        )
        end = re.search(
            r"End search in concurrency (\d+): dur=([0-9.]+)s, total_count=(\d+)", line
        )
        if not start and not end:
            continue
        timestamp = (
            datetime.strptime(line[:23], "%Y-%m-%d %H:%M:%S,%f")
            .replace(tzinfo=ZoneInfo(timezone))
            .timestamp()
        )
        if start:
            starts[int(start[1])] = timestamp
        else:
            concurrency = int(end[1])
            if concurrency not in starts or timestamp <= starts[concurrency]:
                raise ValueError("unpaired/nonpositive query window")
            found.append(
                {
                    "concurrency": concurrency,
                    "start": starts.pop(concurrency),
                    "end": timestamp,
                    "reported_seconds": float(end[2]),
                    "reported_count": int(end[3]),
                }
            )
    if starts:
        raise ValueError("unfinished timed query wave")
    return found


def snapshots(lines):
    current = None
    for line in lines:
        if line.startswith("# snapshot_wall_time_s "):
            if current is not None:
                yield current
            current = {"time": float(line.split()[-1]), "values": {}}
        elif current is not None and line.startswith(PREFIXES):
            key, value = line.split()
            current["values"][key] = float(value)
    if current is not None:
        yield current


def summarize(waves, samples):
    samples = list(samples)
    result = []
    for wave in waves:
        included = [s for s in samples if wave["start"] <= s["time"] <= wave["end"]]
        row = {**wave, "snapshot_count": len(included), "metrics": {}}
        if len(included) >= 2:
            row["inner_sample_seconds"] = included[-1]["time"] - included[0]["time"]
            keys = set.intersection(*(set(s["values"]) for s in included))
            for key in sorted(keys):
                values = [s["values"][key] for s in included]
                if key.endswith("_total"):
                    if any(b < a for a, b in zip(values, values[1:])):
                        row["metrics"][key] = {
                            "invalid": "counter reset within timed wave"
                        }
                    else:
                        row["metrics"][key] = {"delta": values[-1] - values[0]}
                else:
                    row["metrics"][key] = {
                        "sample_min": min(values),
                        "sample_max": max(values),
                        "sample_mean": sum(values) / len(values),
                    }
        result.append(row)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("arm", type=Path)
    parser.add_argument("--timezone", default="America/Los_Angeles")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    with (args.arm / "vdbbench-live.log").open() as log:
        waves = windows(log, args.timezone)
    with (args.arm / "metrics-live.prom").open() as metrics:
        result = {
            "arm": str(args.arm),
            "timezone": args.timezone,
            "waves": summarize(waves, snapshots(metrics)),
            "notes": [
                "Counter deltas cover only inner snapshots, not the entire client wave.",
                "Gauges are sampled observations; peak gauges may be cumulative high-water marks.",
                "Missing fields stay missing; no interpolation or host-contention attribution is inferred.",
            ],
        }
    encoded = json.dumps(result, indent=2) + "\n"
    if args.output:
        with args.output.open("x") as output:
            output.write(encoded)
    else:
        print(encoded, end="")


if __name__ == "__main__":
    main()
