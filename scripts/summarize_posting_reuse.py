"""Verify that a posting-suffix experiment actually published reused state.

This is a post-processing gate, not a load generator. It never equates enabling
the flag, completing a worker, or passing recall with exercising publication.
"""

import argparse
import hashlib
import json
import re
from collections import Counter
from pathlib import Path

FLAG = "ANTFLY_EXPERIMENT_COMPACT_POSTING_DELTAS"
KINDS = {"full", "delta", "compact_deltas"}


def summarize_lines(lines, enabled):
    publications = Counter()
    handoffs = Counter()
    written = Counter()
    retained = {}
    invalid = []
    for number, line in enumerate(lines, 1):
        publication = "dense posting checkpoint published " in line
        handoff = "dense checkpoint handoff " in line
        if not (publication or handoff):
            continue
        fields = dict(re.findall(r"(\w+)=([^\s,]+)", line))
        kind = fields.get("kind")
        # The initial empty authority publication predates the worker lane and
        # has a source= field instead. It is not evidence of suffix reuse.
        if publication and kind is None and fields.get("source"):
            continue
        try:
            if kind not in KINDS:
                raise ValueError("kind")
            for name in ("generation", "sequence"):
                if int(fields[name]) < 0:
                    raise ValueError(name)
            if publication:
                publications[kind] += 1
            else:
                size = int(fields["written_bytes"])
                reused = int(fields["retained_bytes"])
                if size < 0 or reused < 0:
                    raise ValueError("bytes")
                handoffs[kind] += 1
                written[kind] += size
                retained.setdefault(kind, []).append(reused)
        except (KeyError, ValueError):
            invalid.append(number)
    observed = publications["compact_deltas"] > 0
    reasons = []
    if enabled is None:
        reasons.append("No complete experiment-environment receipt")
    if invalid:
        reasons.append("Malformed or incomplete checkpoint evidence")
    if enabled and not observed:
        reasons.append("Suffix flag enabled but no suffix publication observed")
    if enabled is False and observed:
        reasons.append("Suffix publication observed with the flag disabled")
    return {
        "suffix_enabled": enabled,
        "suffix_publication_observed": observed,
        "treatment_exercised": enabled is True and observed and not invalid,
        "evidence_consistent": not reasons,
        "reasons": reasons,
        "invalid_lines": invalid,
        "published_by_kind": dict(publications),
        "handoffs_by_kind": dict(handoffs),
        "written_bytes_by_kind": dict(written),
        # Retained bytes are repeated snapshots of live references, not bytes
        # newly saved. Keep samples; summing them would double-count the base.
        "retained_bytes_samples_by_kind": retained,
        "notes": [
            "Publication counts are not a crash/recovery or recall certificate.",
            "Written bytes cover checkpoint handoffs, not WAL or primary write amplification.",
            "Retained-byte samples must not be summed as distinct disk savings.",
            "Suffix folding reuses the base; it is not arbitrary leaf-chunk replacement.",
        ],
    }


def suffix_enabled_for_arm(arm, config, receipts):
    if receipts is None:
        environment = config.get("experiment_environment", {})
        # The older harness records only a short environment allowlist.
        # Absence there does not certify that the process had the flag off.
        return environment[FLAG] == "1" if FLAG in environment else None
    matches = [
        row
        for row in receipts
        if len(row.get("command", [])) > 1
        and Path(row["command"][1]).resolve() == arm.resolve()
    ]
    if len(matches) != 1:
        raise ValueError("Missing or ambiguous matched-arm receipt")
    row = matches[0]
    if row.get("exit_code") != 0 or row.get("invalid_reason"):
        raise ValueError("Arm has not completed with valid measurement inputs")
    environment = row["environment"]
    binary = environment["ANTFLY_BIN"]
    if row["inputs_sha256"].get(binary) != config["antfly_binary_sha256"]:
        raise ValueError("Arm and harness binary receipts disagree")
    # The matched runner scrubs inherited experiment variables and records
    # the complete controlled environment, so absence here does mean off.
    return environment.get(FLAG) == "1"


def summarize_arm(arm):
    config = json.loads((arm / "run-config.json").read_text())
    # This receipt is written only after live, mixed and reopened profiles.
    # Its presence is an end-of-measurement gate, not a correctness verdict.
    receipt = (arm / "qualification-summary.json").read_bytes()
    json.loads(receipt)
    log = arm / "antfly-initial.log"
    # Hash and parse the same bytes; do not combine an old receipt with a
    # concurrently changed log. Run after the arm has completed.
    data = log.read_bytes()
    matrix_receipt = arm.parent / "ab-runs.json"
    receipts = (
        json.loads(matrix_receipt.read_text()) if matrix_receipt.exists() else None
    )
    enabled = suffix_enabled_for_arm(arm, config, receipts)
    return {
        "arm": str(arm),
        "case": config["case"],
        "binary_sha256": config["antfly_binary_sha256"],
        "qualification_receipt_sha256": hashlib.sha256(receipt).hexdigest(),
        "log_sha256": hashlib.sha256(data).hexdigest(),
        **summarize_lines(data.decode(errors="replace").splitlines(), enabled),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("arm", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--require-exercised", action="store_true")
    args = parser.parse_args()
    result = summarize_arm(args.arm)
    encoded = json.dumps(result, indent=2) + "\n"
    if args.output:
        with args.output.open("x") as output:
            output.write(encoded)
    print(encoded, end="")
    if not result["evidence_consistent"] or (
        args.require_exercised and not result["treatment_exercised"]
    ):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
