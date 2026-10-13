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


def validate_future_reservation(ledger, config):
    limits = ledger.get("future_reservation_limits")
    if limits is None:
        return
    resources = config["resources"]
    for key in ("cpu", "memory_gib", "disk_gib"):
        if not 0 < money(resources[key]) <= money(limits[key]):
            raise ValueError("Resources exceed reviewed future reservation bounds")
    if not 0 < config["lifetime_seconds"] <= limits["lifetime_seconds"]:
        raise ValueError("Lifetime exceeds reviewed future reservation bounds")


def replan_future_reservation(ledger_path, evidence_path):
    """Adjust only future deposits; never refund history or expand the ceiling."""
    raw = evidence_path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    evidence = json.loads(raw)
    with ledger_path.open("r+") as file:
        fcntl.flock(file, fcntl.LOCK_EX)
        ledger = json.load(file)
        if (
            evidence.get("project") != ledger["project"]
            or evidence.get("kind") != "reviewed_future_run_reservation"
        ):
            raise ValueError("Unsupported future reservation evidence")
        if any(
            audit["evidence_sha256"] == digest
            for audit in ledger.get("future_reservation_changes", [])
        ):
            return {"already_applied": True, "exposure_usd": exposure(ledger)}
        limits = evidence["resource_limits"]
        for key, maximum in (("cpu", 8), ("memory_gib", 48), ("disk_gib", 10)):
            if isinstance(limits[key], bool) or not 0 < money(limits[key]) <= maximum:
                raise ValueError("Unsupported future resource bounds")
        lifetime = limits["lifetime_seconds"]
        if (
            isinstance(lifetime, bool)
            or not isinstance(lifetime, int)
            or not 0 < lifetime <= 3600
        ):
            raise ValueError("Unsupported future lifetime bound")
        previous = dict(ledger["budget"])
        b = ledger["budget"]
        floor = (
            2
            * (
                money(limits["cpu"]) * money(b["cpu_hour_usd"])
                + money(limits["memory_gib"]) * money(b["memory_gib_hour_usd"])
                + money(limits["disk_gib"]) * money(b["disk_gib_hour_usd"])
            )
            + money(b["other_run_allowance_usd"])
        ).quantize(Decimal("0.000001"), rounding=ROUND_CEILING)
        deposit = money(evidence["reservation_per_run_usd"])
        before = exposure(ledger)
        if not floor <= deposit <= money(b["reservation_per_run_usd"]):
            raise ValueError(
                "Future deposit must cover two hours and the full noncompute hold"
            )
        if money(before) + deposit > money(b["ceiling_usd"]):
            raise ValueError("Future run exceeds the existing spend ceiling")
        b["reservation_per_run_usd"] = float(deposit)
        ledger["future_reservation_limits"] = limits
        ledger.setdefault("future_reservation_changes", []).append(
            {
                "at_unix": time.time(),
                "evidence_sha256": digest,
                "previous_budget": previous,
                "future_reservation_usd": float(deposit),
                "minimum_reservation_usd": float(floor),
                "resource_limits": limits,
                "basis": "two whole hours plus unchanged noncompute allowance; future runs only",
            }
        )
        if exposure(ledger) != before:
            raise ValueError("Future replanning cannot release historical exposure")
        file.seek(0)
        json.dump(ledger, file, indent=2)
        file.write("\n")
        file.truncate()
        file.flush()
        os.fsync(file.fileno())
        return {
            "exposure_usd": before,
            "future_reservation_usd": float(deposit),
            "minimum_reservation_usd": float(floor),
        }


def admit_transport_resume(
    ledger_path, origin, config, binary_sha256, inventory_sha256, output
):
    """One replacement pod shares an unsettled attempt's two-hour deposit."""
    plan = json.loads((origin / "plan.json").read_text())
    cleanup_path = origin / "regional" / "cleanup.json"
    cleanup = json.loads(cleanup_path.read_text())
    worker = json.loads((origin / "regional" / "worker.json").read_text())
    verdict = json.loads((origin / "verdict.json").read_text())
    # Controller stderr need not be available in a worker log. Admit only a
    # terminal, classified control failure bound to this pod and launch record.
    evidence = []
    records = sorted((origin / "regional").glob("*-transport-failure.json"))
    for path in records:
        record = json.loads(path.read_text())
        if not isinstance(record, dict):
            raise ValueError("Invalid transport failure evidence")
        job_path = path.with_name(
            path.name.replace("-transport-failure.json", "-job.json")
        )
        if not job_path.exists():
            continue
        job = json.loads(job_path.read_text())
        events = record.get("events")
        if (
            record.get("version") == 1
            and record.get("kind") == "benchmark_transport_failure"
            and record.get("terminal") is True
            and record.get("pod") == worker["metadata"]["name"]
            and record.get("pod_uid") == worker["metadata"]["uid"]
            and record.get("command_sha256") == job.get("command_sha256")
            and isinstance(record.get("command_sha256"), str)
            and len(record["command_sha256"]) == 64
            and record.get("directory") == job.get("directory")
            and isinstance(events, list)
            and len(events) == 5
            and all(
                isinstance(event, dict)
                and event.get("stage") in ("launch", "poll")
                and event.get("reason")
                in (
                    "connection_reset",
                    "connection_refused",
                    "timeout",
                    "unexpected_eof",
                    "broken_pipe",
                )
                for event in events
            )
        ):
            evidence.append(path)
    # Keep already-retained attempts readable, but new runs use structured data.
    logs = list((origin / "regional").glob("*-*.log"))
    transport_failure = bool(evidence) or (
        not records
        and any(
            "error: error reading from error stream:" in p.read_text()
            and "connection reset by peer" in p.read_text()
            for p in logs
        )
    )
    if (
        plan["config"] != config
        or plan["binary_sha256"] != binary_sha256
        or plan["inventory_sha256"] != inventory_sha256
        or verdict.get("passed") is not False
        or verdict.get("engine_failures") != []
        or not transport_failure
        or cleanup.get("created") is not True
        or cleanup.get("deleted") is not True
        or cleanup.get("pod") != worker["metadata"]["name"]
        or not worker["metadata"].get("uid")
        or worker.get("kind") != "Pod"
        or worker["metadata"].get("labels", {}).get("app") != "hackernews-benchmark"
        or not worker["metadata"]["name"].startswith("hackernews-benchmark-")
        or worker["spec"].get("activeDeadlineSeconds") != config["lifetime_seconds"]
        or not 0 <= worker["spec"].get("terminationGracePeriodSeconds", 30) <= 60
        or worker["spec"]["restartPolicy"] != "Never"
        or worker["spec"].get("initContainers")
        or worker["spec"].get("ephemeralContainers")
        or any("persistentVolumeClaim" in v for v in worker["spec"].get("volumes", []))
        or len(worker["spec"]["containers"]) != 1
    ):
        raise ValueError("Resume must preserve a closed transport-failed attempt")
    created = datetime.fromisoformat(
        worker["metadata"]["creationTimestamp"].replace("Z", "+00:00")
    )
    absent = datetime.fromtimestamp(cleanup_path.stat().st_mtime, timezone.utc)
    elapsed = (absent - created).total_seconds()
    # Add fifteen minutes for bounded scheduling, control calls, report copies
    # and termination on top of the replacement's hard lifetime.
    maximum_seconds = elapsed + config["lifetime_seconds"] + 900
    if (
        not 0 <= elapsed
        or maximum_seconds > 7200
        or absent > datetime.now(timezone.utc)
    ):
        raise ValueError("Replacement exceeds the attempt's aggregate two-hour bound")
    requested = worker["spec"]["containers"][0]["resources"]["requests"]
    r = config["resources"]
    if (
        money(requested["cpu"]) != money(r["cpu"])
        or requested["memory"] != f"{r['memory_gib']}Gi"
        or requested["ephemeral-storage"] != f"{r['disk_gib']}Gi"
    ):
        raise ValueError("Admitted resources differ from the reserved attempt")
    with ledger_path.open("r+") as file:
        fcntl.flock(file, fcntl.LOCK_EX)
        ledger = json.load(file)
        validate_future_reservation(ledger, config)
        run_id = plan["run_prefix"]
        run = next((run for run in ledger["runs"] if run["id"] == run_id), None)
        if (
            run is None
            or ledger["project"] != config["project"]
            or ledger["budget"] != config["budget"]
            or any(
                row["run_id"] == run_id
                for key in ("reconciliations", "closed_compute_bounds")
                for audit in ledger.get(key, [])
                for row in audit["runs"]
            )
            or any(
                audit["run_id"] == run_id
                for audit in ledger.get("transport_resumes", [])
            )
        ):
            raise ValueError(
                "Only one replacement of an unsettled owned attempt is allowed"
            )
        b = ledger["budget"]
        floor = 2 * (
            money(r["cpu"]) * money(b["cpu_hour_usd"])
            + money(r["memory_gib"]) * money(b["memory_gib_hour_usd"])
            + money(r["disk_gib"]) * money(b["disk_gib_hour_usd"])
        ) + money(b["other_run_allowance_usd"])
        if money(run["reserved_usd"]) < floor or money(exposure(ledger)) > money(
            b["ceiling_usd"]
        ):
            raise ValueError(
                "Existing deposit does not cover the aggregate replacement"
            )
        before = exposure(ledger)
        ledger.setdefault("transport_resumes", []).append(
            {
                "run_id": run_id,
                "output": str(output.resolve()),
                "at_unix": time.time(),
                "previous_pod_uid": worker["metadata"]["uid"],
                "maximum_aggregate_pod_seconds": maximum_seconds,
                "reserved_usd": run["reserved_usd"],
                "provenance_sha256": {
                    name: hashlib.sha256((origin / path).read_bytes()).hexdigest()
                    for name, path in (
                        ("plan", "plan.json"),
                        ("worker", "regional/worker.json"),
                        ("cleanup", "regional/cleanup.json"),
                        ("verdict", "verdict.json"),
                    )
                },
                "transport_evidence_sha256": {
                    path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                    for path in (evidence or logs)
                },
            }
        )
        if exposure(ledger) != before:
            raise ValueError("Resume cannot change reserved exposure")
        file.seek(0)
        json.dump(ledger, file, indent=2)
        file.write("\n")
        file.truncate()
        file.flush()
        os.fsync(file.fileno())
        return run_id


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    operations = parser.add_mutually_exclusive_group()
    operations.add_argument("--closed-pod-bound", action="store_true")
    operations.add_argument("--replan-future-reservation", action="store_true")
    args = parser.parse_args()
    operation = (
        replan_future_reservation
        if args.replan_future_reservation
        else bound_closed_pods
        if args.closed_pod_bound
        else reconcile
    )
    print(json.dumps(operation(args.ledger, args.evidence), indent=2))


if __name__ == "__main__":
    main()
