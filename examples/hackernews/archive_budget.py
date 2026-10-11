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
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING
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
    bounded = {}
    for audit in ledger.get("closed_compute_bounds", []):
        for row in audit["runs"]:
            if row["run_id"] not in runs:
                raise ValueError("Unknown bounded run")
            hold = money(row["pending_noncompute_usd"])
            if hold < minimum_hold:
                raise ValueError("Unexported noncompute allowance cannot be refunded")
            bounded[row["run_id"]] = money(row["maximum_compute_usd"]) + hold
    for run_id, run in runs.items():
        total += (
            sum(settled[run_id])
            if run_id in settled
            else bounded.get(run_id, money(run["reserved_usd"]))
        )
    total += sum(
        (money(r["reserved_usd"]) for r in ledger.get("local_diagnostics", [])),
        Decimal(0),
    )
    return float(total)


def bound_closed_pods(ledger_path, evidence_path):
    """Replace excess compute reservation with a reviewed closed-pod bound.

    This is not billing settlement. Charge two whole hours for each bounded
    one-hour pod, retaining every noncompute allowance until exported. Actual
    gross settlement supersedes this bound, including any overrun.
    """
    raw = evidence_path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    evidence = json.loads(raw)
    with ledger_path.open("r+") as file:
        fcntl.flock(file, fcntl.LOCK_EX)
        ledger = json.load(file)
        if (
            evidence.get("project") != ledger["project"]
            or evidence.get("kind") != "reviewed_closed_pod_compute_bound"
            or not evidence.get("runs")
        ):
            raise ValueError("Unsupported closed-pod evidence")
        if any(
            audit["evidence_sha256"] == digest
            for audit in ledger.get("closed_compute_bounds", [])
        ):
            return {"already_applied": True, "exposure_usd": exposure(ledger)}
        runs = {run["id"]: run for run in ledger["runs"]}
        rows = []
        seen = set()
        for row in evidence["runs"]:
            run_id = row["run_id"]
            if run_id not in runs or run_id in seen:
                raise ValueError("Unknown or duplicate closed run")
            seen.add(run_id)
            worker, cleanup = row["admitted_pod"], row["cleanup"]
            metadata, spec = worker["metadata"], worker["spec"]
            observed = datetime.fromisoformat(
                cleanup["observed_absent_at_utc"].replace("Z", "+00:00")
            )
            created = datetime.fromisoformat(
                metadata["creationTimestamp"].replace("Z", "+00:00")
            )
            if (
                worker.get("kind") != "Pod"
                or not metadata.get("uid")
                or cleanup.get("pod_uid") != metadata["uid"]
                or cleanup.get("pod") != metadata["name"]
                or cleanup.get("deleted") is not True
                or cleanup.get("created") is not True
                or observed.tzinfo is None
                or created.tzinfo is None
                or not created <= observed <= datetime.now(timezone.utc)
                or observed - created > timedelta(hours=2)
                or spec.get("restartPolicy") != "Never"
                or not isinstance(spec.get("activeDeadlineSeconds"), int)
                or isinstance(spec["activeDeadlineSeconds"], bool)
                or not 0 < spec["activeDeadlineSeconds"] <= 3600
                or not 0 <= spec.get("terminationGracePeriodSeconds", 30) <= 60
                or len(spec.get("containers", [])) != 1
                or spec.get("initContainers")
                or spec.get("ephemeralContainers")
                or any("persistentVolumeClaim" in v for v in spec.get("volumes", []))
            ):
                raise ValueError("Unverified, active or unbounded pod")
            requested = spec["containers"][0]["resources"]["requests"]
            cpu = money(requested["cpu"])
            memory = money(requested["memory"].removesuffix("Gi"))
            disk = money(requested["ephemeral-storage"].removesuffix("Gi"))
            if (
                not requested["memory"].endswith("Gi")
                or not requested["ephemeral-storage"].endswith("Gi")
                or not 0 < cpu <= 8
                or not 0 < memory <= 48
                or not 0 < disk <= 10
            ):
                raise ValueError("Unsupported admitted resources")
            b = ledger["budget"]
            maximum = (
                2
                * (
                    cpu * money(b["cpu_hour_usd"])
                    + memory * money(b["memory_gib_hour_usd"])
                    + disk * money(b["disk_gib_hour_usd"])
                )
            ).quantize(Decimal("0.000001"), rounding=ROUND_CEILING)
            hold = money(b["other_run_allowance_usd"])
            if maximum + hold > money(runs[run_id]["reserved_usd"]):
                raise ValueError("Closed-pod bound exceeds its reservation")
            rows.append(
                {
                    "run_id": run_id,
                    "pod": metadata["name"],
                    "pod_uid": metadata["uid"],
                    "maximum_compute_usd": float(maximum),
                    "pending_noncompute_usd": float(hold),
                    "observed_absent_at_utc": cleanup["observed_absent_at_utc"],
                }
            )
        before = exposure(ledger)
        ledger.setdefault("closed_compute_bounds", []).append(
            {
                "at_unix": time.time(),
                "evidence_sha256": digest,
                "runs": rows,
                "basis": "two whole hours at admitted resources; not posted billing",
            }
        )
        after = exposure(ledger)
        ledger["closed_compute_bounds"][-1].update(
            exposure_before_usd=before, exposure_after_usd=after
        )
        file.seek(0)
        json.dump(ledger, file, indent=2)
        file.write("\n")
        file.truncate()
        file.flush()
        os.fsync(file.fileno())
        return {
            "exposure_usd": after,
            "released_usd": round(before - after, 6),
            "posted_billing": False,
        }


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
    parser.add_argument("--closed-pod-bound", action="store_true")
    args = parser.parse_args()
    operation = bound_closed_pods if args.closed_pod_bound else reconcile
    print(json.dumps(operation(args.ledger, args.evidence), indent=2))


if __name__ == "__main__":
    main()
