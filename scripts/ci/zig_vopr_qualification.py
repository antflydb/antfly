#!/usr/bin/env python3
"""Exercise the real campaign -> validate -> retain -> restore -> campaign boundary."""

import argparse
import json
import shutil
from pathlib import Path

import zig_vopr_soak as soak


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def qualify(binary, output):
    output.mkdir(parents=True, exist_ok=False)
    first, merged, retained = (
        output / name for name in ("first", "merged", "retained")
    )
    require(
        soak.run_shard(binary, "raft", 42, 2, output / "empty", first, timeout=120)
        == 0,
        "first campaign failed",
    )
    phases = [
        json.loads(path.read_text()) for path in first.glob("history-*.progress.json")
    ]
    require(
        len(phases) == 2
        and all(item["phase"].startswith("completed_") for item in phases),
        "campaign did not retain a completed progress record for every history",
    )
    runner_digest = json.loads((first / "run.json").read_text())["binary_sha256"]
    require(
        all(
            json.loads(path.read_text())["binary_sha256"] == runner_digest
            for path in first.glob("history-*.schedule.json")
        ),
        "scheduling provenance lost the producing executable",
    )
    require(
        soak.merge_corpus(binary, first, merged, retained, timeout=120) == 0,
        "first corpus merge failed",
    )
    manifest = json.loads((merged / "index.json").read_text())
    require(
        manifest["exact_replays"] == len(soak.traces(first)),
        "merge replayed an input more than once",
    )
    require(soak.traces(retained), "no working corpus was retained")
    require(
        list(retained.glob("*.schedule.json")), "retention lost scheduling provenance"
    )

    restored = output / "restored"
    shutil.copytree(retained, restored)
    second = output / "second"
    require(
        soak.run_shard(
            binary,
            "raft",
            43,
            2,
            restored,
            second,
            timeout=120,
            policy="adversarial",
            require_seed=True,
        )
        == 0,
        "restored campaign failed",
    )
    consumed = json.loads((second / "run.json").read_text())
    require(
        consumed["seeded_entries_consumed"] > 0,
        "second campaign did not consume the corpus",
    )
    require(
        soak.merge_corpus(
            binary,
            second,
            output / "merged-second",
            output / "retained-second",
            timeout=120,
        )
        == 0,
        "second corpus merge failed",
    )

    # Exercise authority selection with real incompatible, malformed, duplicate
    # and divergent bytes. The first candidate is deliberately not authority.
    cases = output / "cases"
    cases.mkdir()
    original = soak.traces(retained)[0].read_bytes()
    records = [json.loads(line) for line in original.splitlines()]
    for record in records:
        if record["type"] == "property":
            record["condition"] = not record["condition"]
            break
    (cases / "history-0-diverged.voprtrace").write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records)
    )
    (cases / "history-1-valid.voprtrace").write_bytes(original)
    (cases / "seed-duplicate.voprtrace").write_bytes(original)
    records = [json.loads(line) for line in original.splitlines()]
    records[0]["scenario_version"] += 1
    (cases / "seed-incompatible.voprtrace").write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records)
    )
    (cases / "seed-malformed.voprtrace").write_text("incomplete trace\n")
    reviewed = output / "reviewed"
    require(
        soak.merge_corpus(binary, cases, reviewed, timeout=120) != 0,
        "replay divergence was hidden",
    )
    review = json.loads((reviewed / "index.json").read_text())
    require(
        review["exact_replays"] == 2, "duplicate or incompatible traces were replayed"
    )
    require(len(review["artifacts"]) == 1, "valid authority was lost")
    require(
        {item["reason"] for item in review["quarantine"]}
        == {"replay_diverged", "scenario_version_changed", "invalid_trace"},
        "quarantine misclassified rejected inputs",
    )

    invalid = output / "invalid-only"
    invalid.mkdir()
    shutil.copyfile(
        cases / "history-0-diverged.voprtrace", invalid / "history-0.voprtrace"
    )
    rejected = output / "rejected"
    require(
        soak.merge_corpus(binary, invalid, rejected, timeout=120) != 0,
        "invalid-only corpus passed",
    )
    require(
        not (rejected / "index.json").exists(),
        "invalid-only corpus published a completion marker",
    )
    require(
        list((rejected / "quarantine").glob("*.voprquarantine")),
        "rejected authority lost its evidence",
    )
    limited = output / "validation-cutoff"
    limited.mkdir()
    with (limited / "merge.log").open("w") as log:
        result = soak.run_process(
            [
                str(binary),
                "corpus-merge",
                "--trace",
                str(cases / "history-1-valid.voprtrace"),
                "--out-dir",
                str(limited),
                "--max-replay-transitions",
                "0",
            ],
            stdout=log,
            timeout=120,
        )
    require(result.returncode != 0, "validation ignored its transition budget")
    require(
        not (limited / "index.json").exists(),
        "interrupted validation published a completed corpus",
    )
    progress = json.loads((limited / "validation.json").read_text())
    require(
        progress["status"] == "transition_budget_exhausted"
        and progress["exact_replays"] == 0,
        "validation cutoff lost its progress evidence",
    )
    for budget in (1, 2, 4, 8, 16, 32):
        cutoff = output / f"startup-cutoff-{budget}"
        cutoff.mkdir()
        command = [
            str(binary),
            "campaign",
            "--scenario",
            "ha-scaling",
            "--histories",
            "1",
            "--seed",
            "42",
            "--transitions",
            str(budget),
            "--fail-on-findings",
            "--defer-diagnostics",
            "--artifact-dir",
            str(cutoff),
        ]
        with (cutoff / "campaign.log").open("w") as log:
            result = soak.run_process(command, stdout=log, timeout=120)
        require(result.returncode != 0, "incomplete startup history passed")
        report = json.loads((cutoff / "results.json").read_text())
        require(
            report["histories"]["exact_replays"] == 1, "startup cutoff did not replay"
        )
        properties = {item["name"]: item["status"] for item in report["properties"]}
        require(
            properties["production-ha-scaling.owners-quiesce"] == "pass",
            "startup cancellation leaked owners",
        )
        require(
            properties["production-ha-scaling.history-completes"] == "fail",
            "incomplete startup was hidden",
        )

    soak.write_json(
        output / "qualification.json",
        {
            "passed": True,
            "binary_sha256": soak.trace_digest(binary),
            "seeded_entries_consumed": consumed["seeded_entries_consumed"],
        },
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    qualify(args.binary.resolve(strict=True), args.output)


if __name__ == "__main__":
    main()
