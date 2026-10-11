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

"""Settle completed pod compute from reviewed billing evidence; retain other holds.

Evidence is an operator-reviewed export, not an automatic refund. Promotion
credits never reduce gross budget accounting. Original runs remain unchanged.
"""

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import time


def money(value):
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError("Invalid monetary amount")
    try:
        result = Decimal(str(value))
    except ArithmeticError as error:
        raise ValueError("Invalid monetary amount") from error
    if not result.is_finite() or result < 0:
        raise ValueError("Invalid monetary amount")
    return result


def exposure(ledger):
    """Unsettled reservations plus latest gross settlement and pending holds."""
    runs = {r["id"]: r for r in ledger["runs"]}
    if len(runs) != len(ledger["runs"]):
        raise ValueError("Duplicate ledger run identity")
    settled = {}
    minimum_hold = money(ledger["budget"]["other_run_allowance_usd"])
    for audit in ledger.get("reconciliations", []):
        for row in audit["runs"]:
            run_id = row["run_id"]
            if run_id not in runs:
                raise ValueError("Unknown reconciled run")
            hold = money(row["pending_noncompute_usd"])
            if hold < minimum_hold:
                raise ValueError("Unexported noncompute allowance cannot be refunded")
            gross = money(row["gross_compute_usd"])
            if run_id in settled and gross < settled[run_id][0]:
                raise ValueError("Posted gross compute cannot decrease")
            settled[run_id] = (gross, hold)
    total = money(ledger["budget"]["prior_work_reserved_usd"])
    for run_id, run in runs.items():
        total += (
            sum(settled[run_id]) if run_id in settled else money(run["reserved_usd"])
        )
    total += sum(
        (money(r["reserved_usd"]) for r in ledger.get("local_diagnostics", [])),
        Decimal(0),
    )
    return float(total)


def reconcile(ledger_path, evidence_path):
    raw = evidence_path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    evidence = json.loads(raw)
    with ledger_path.open("r+") as file:
        fcntl.flock(file, fcntl.LOCK_EX)
        ledger = json.load(file)
        if evidence["project"] != ledger["project"]:
            raise ValueError("Billing evidence project differs")
        if evidence["kind"] != "completed_pod_compute":
            raise ValueError("Unsupported reconciliation evidence")
        export_time = datetime.fromisoformat(
            evidence["export_time_utc"].replace("Z", "+00:00")
        )
        now = datetime.now(timezone.utc)
        if export_time.tzinfo is None or export_time > now:
            raise ValueError("Invalid billing export timestamp")
        if not evidence.get("source_table") or not evidence["runs"]:
            raise ValueError("Missing reconciliation provenance")
        if any(
            audit["evidence_sha256"] == digest
            for audit in ledger.get("reconciliations", [])
        ):
            return {"already_applied": True, "exposure_usd": exposure(ledger)}
        identities = set()
        for row in evidence["runs"]:
            closed = datetime.fromisoformat(row["closed_at_utc"].replace("Z", "+00:00"))
            covered = datetime.fromisoformat(
                row["coverage_through_utc"].replace("Z", "+00:00")
            )
            if (
                row["run_id"] in identities
                or not row.get("pod")
                or row.get("pod_deleted") is not True
                or closed.tzinfo is None
                or covered.tzinfo is None
                or closed > covered
                or covered > export_time
            ):
                raise ValueError("Duplicate, active or incompletely exported pod")
            identities.add(row["run_id"])
        before = exposure(ledger)
        ledger.setdefault("reconciliations", []).append(
            {
                "at_unix": time.time(),
                "evidence_sha256": digest,
                "source_table": evidence["source_table"],
                "export_time_utc": evidence["export_time_utc"],
                "runs": evidence["runs"],
            }
        )
        after = exposure(ledger)
        if not math.isfinite(after):
            raise ValueError("Invalid reconciled exposure")
        ledger["reconciliations"][-1].update(
            exposure_before_usd=before,
            exposure_after_usd=after,
            released_usd=round(before - after, 6),
        )
        file.seek(0)
        json.dump(ledger, file, indent=2)
        file.write("\n")
        file.truncate()
        file.flush()
        os.fsync(file.fileno())
        return {"released_usd": round(before - after, 6), "exposure_usd": after}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(reconcile(args.ledger, args.evidence), indent=2))


if __name__ == "__main__":
    main()
