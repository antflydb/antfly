#!/usr/bin/env python3
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

"""Run comparative HN checks in one temporary GKE pod in the bucket's region.

Uses the caller's existing GCS authority via short-lived stdin credentials.
Does not create IAM bindings, secrets, buckets, services, or persistent volumes.
The pod is removed in finally and has a hard lifetime if the caller disconnects.
"""

import argparse
import errno
import hashlib
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import uuid

SDK_IMAGE = "gcr.io/google.com/cloudsdktool/google-cloud-cli@sha256:cde9dbd556000c21c08449d8e5828904ef91e690bec95207f71fa6a6685922c9"


def transport_reason(error):
    """Classify locally; never persist stderr, commands, credentials or endpoints."""
    if isinstance(error, subprocess.TimeoutExpired):
        return "timeout"
    if isinstance(error, OSError):
        return {
            errno.ECONNRESET: "connection_reset",
            errno.ECONNREFUSED: "connection_refused",
            errno.ETIMEDOUT: "timeout",
            errno.EPIPE: "broken_pipe",
        }.get(error.errno, "unclassified")
    diagnostic = error.stderr or error.stdout or ""
    if isinstance(diagnostic, bytes):
        diagnostic = diagnostic.decode(errors="replace")
    diagnostic = diagnostic.lower()
    for marker, reason in (
        ("connection reset by peer", "connection_reset"),
        ("connection refused", "connection_refused"),
        ("i/o timeout", "timeout"),
        ("tls handshake timeout", "timeout"),
        ("unexpected eof", "unexpected_eof"),
        ("broken pipe", "broken_pipe"),
    ):
        if marker in diagnostic:
            return reason
    return "unclassified"


def compare_runs(reports):
    """Compare exact results only when the measured source/query are identical."""
    reference = reports[0]
    expected = [
        (hit["_source"]["hn_id"], hit["_score"])
        for hit in reference["first_response"]["hits"]["hits"]
    ]
    for report in reports[1:]:
        if report.get("cursor_depth", {}).get("term") != reference.get(
            "cursor_depth", {}
        ).get("term"):
            raise ValueError("Cursor workload differs between runs")
        if report.get("store_source", False) != reference.get("store_source", False):
            raise ValueError("Stored-source policy differs between runs")
        for field in ("source", "row_count", "query", "concurrency"):
            if report[field] != reference[field]:
                raise ValueError(f"Benchmark {field} differs between runs")
        actual = [
            (hit["_source"]["hn_id"], hit["_score"])
            for hit in report["first_response"]["hits"]["hits"]
        ]
        if actual != expected:
            raise ValueError("Ranked HN IDs/scores changed between runs")
        expected_workloads = reference.get("workloads", {})
        if report.get("workloads", {}).keys() != expected_workloads.keys():
            raise ValueError("Workload coverage differs between runs")
        for term, result in expected_workloads.items():
            actual_workload = report["workloads"][term]
            if any(
                actual_workload[field] != result[field]
                for field in ("total", "top_hits")
            ):
                raise ValueError(f"Workload results differ for {term}")
        expected_filters = reference["metadata_filters"]
        if report["metadata_filters"].keys() != expected_filters.keys():
            raise ValueError("Filter coverage differs between runs")
        for name, result in expected_filters.items():
            if report["metadata_filters"][name]["total"] != result["total"]:
                raise ValueError(f"Exact filter total differs for {name}")


def fresh_bearer_for_lifetime(invoke, project, required_seconds, *, now=None):
    """Keep one admitted token in memory for the worker's bounded lifetime."""
    completed = invoke(
        [
            "gcloud",
            "config",
            "config-helper",
            "--force-auth-refresh",
            "--project",
            project,
            "--format=json(credential.access_token,credential.token_expiry)",
        ],
        capture_output=True,
        text=True,
    )
    try:
        credential = json.loads(completed.stdout)["credential"]
        token = credential["access_token"]
        expiry = datetime.fromisoformat(
            credential["token_expiry"].replace("Z", "+00:00")
        )
        if (
            not isinstance(token, str)
            or not 0 < len(token) <= 65536
            or expiry.tzinfo is None
        ):
            raise ValueError("Invalid credential metadata")
    except (KeyError, TypeError, ValueError, AttributeError):
        raise RuntimeError("Cannot admit bounded credential lifetime") from None
    remaining = (expiry - (now or datetime.now(timezone.utc))).total_seconds()
    if remaining < required_seconds:
        raise RuntimeError("Credential expires before the worker hard deadline")
    return token, {
        "expires_at": expiry.isoformat(),
        "remaining_seconds": round(remaining, 2),
        "required_seconds": round(required_seconds, 2),
    }


def remote_job(
    run,
    kubectl,
    pod_name,
    command,
    token,
    directory,
    deadline,
    *,
    now=time.monotonic,
    sleep=time.sleep,
    failure_record=None,
    pod_uid=None,
):
    """Short control-plane calls cannot terminate the owned benchmark process."""
    expected = hashlib.sha256(json.dumps(command).encode()).hexdigest()
    launched = False
    failures = 0
    events = []

    def persist(terminal):
        if failure_record is None:
            return
        record = {
            "version": 1,
            "kind": "benchmark_transport_failure",
            "pod": pod_name,
            "pod_uid": pod_uid,
            "directory": directory,
            "command_sha256": expected,
            "terminal": terminal,
            "events": events,
        }
        temporary = failure_record.with_suffix(".tmp")
        with temporary.open("w") as file:
            json.dump(record, file, indent=2)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
        temporary.replace(failure_record)

    while now() < deadline:
        stage = "launch" if not launched else "poll"
        try:
            if not launched:
                run(
                    kubectl
                    + [
                        "exec",
                        "-i",
                        pod_name,
                        "--",
                        "python3",
                        "/workspace/regional_worker.py",
                        "launch",
                        directory,
                        *command,
                    ],
                    input=token + "\n",
                    text=True,
                    capture_output=True,
                    timeout=30,
                )
                launched = True
            stage = "poll"
            code = "import json,sys; from pathlib import Path; p=Path(sys.argv[1])/'exit.json'; print(p.read_text() if p.exists() else 'null')"
            result = run(
                kubectl + ["exec", pod_name, "--", "python3", "-c", code, directory],
                text=True,
                capture_output=True,
                timeout=30,
            )
            status = json.loads(result.stdout)
            failures = 0
            if events:
                events = []
                persist(False)
            if status is not None:
                if status.get("command_sha256") != expected:
                    raise ValueError("Worker completion authority differs")
                return status["exit_code"]
        except (
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            OSError,
        ) as error:
            failures += 1
            events.append({"stage": stage, "reason": transport_reason(error)})
            persist(failures >= 5)
            if failures >= 5:
                raise RuntimeError("Benchmark control plane unavailable") from None
            # Retrying launch is safe: the supervisor's lock prevents duplicate
            # execution even if the first launch's reply was lost.
        sleep(min(2, max(0, deadline - now())))
    raise RuntimeError("Benchmark worker exceeded its owned pod deadline")


def copy_result(run, command):
    for attempt in range(3):
        try:
            return run(command, capture_output=True, timeout=45)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
            if attempt == 2:
                raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--binary", type=Path, required=True, help="Linux x86_64 standalone binary"
    )
    parser.add_argument("--project", default="antfly-dev-01")
    parser.add_argument("--cluster", default="antfly-dev")
    parser.add_argument("--region", default="us-central1")
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--bucket", default="colony-import-sources-antfly-dev-01")
    parser.add_argument("--source-prefix", default="hn-poc/20261007")
    parser.add_argument(
        "--run-prefix",
        required=True,
        help="New GCS artifact namespace; source is read only",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--revision", required=True, help="Binary source revision")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument(
        "--modes",
        nargs="+",
        choices=("text-only", "indexed"),
        default=["text-only", "indexed"],
        help="Run modes separately when a full-archive build needs its own pod lifetime",
    )
    parser.add_argument(
        "--cycles", type=int, default=2, help="Empty-cache/restart cycles per table"
    )
    parser.add_argument("--expected-rows", type=int, default=10000)
    parser.add_argument("--build-timeout", type=int, default=300)
    parser.add_argument("--cursor-retention-ms", type=int, default=300000)
    parser.add_argument("--lifetime", type=int, default=2700)
    parser.add_argument("--cpu", default="2")
    parser.add_argument("--memory", default="8Gi")
    parser.add_argument("--disk", default="4Gi")
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--workload-terms", nargs="+", default=[])
    parser.add_argument("--cursor-pages", type=int, default=0)
    parser.add_argument("--cursor-term")
    parser.add_argument("--qualify-composed-sources", action="store_true")
    parser.add_argument("--benchmark-composed", action="store_true")
    parser.add_argument("--composed-current-rows", type=int, default=1000)
    parser.add_argument("--store-source", action="store_true")
    args = parser.parse_args()
    if args.benchmark_composed and not args.qualify_composed_sources:
        parser.error("composed benchmarks require --qualify-composed-sources")
    if not 2 <= args.composed_current_rows <= 4096:
        parser.error("composed current rows must be between 2 and 4096")
    if len(set(args.modes)) != len(args.modes):
        parser.error("modes must not contain duplicates")
    if not args.binary.is_file():
        parser.error("binary must be an existing Linux executable")
    if args.cycles < 1 or args.repeats < 1:
        parser.error("cycles and repeats must be positive")
    if (
        args.expected_rows < 2
        or args.build_timeout < 1
        or args.lifetime < args.build_timeout + 120
    ):
        parser.error(
            "row count must be >=2 and lifetime must exceed build timeout by 120s"
        )
    if args.concurrency < 1:
        parser.error("concurrency must be positive")
    if not 1000 <= args.cursor_retention_ms <= 3600000:
        parser.error("cursor retention must be between 1000 and 3600000 milliseconds")
    args.output.mkdir(parents=True, exist_ok=False)
    # kubectl's auth plugin may cache credentials beside its kubeconfig. Keep
    # those standard CLI files outside the delivered results and remove them.
    credentials = tempfile.TemporaryDirectory(prefix="hackernews-kube-")
    env = dict(os.environ, KUBECONFIG=str(Path(credentials.name) / "kubeconfig"))

    def run(argv, **kwargs):
        kwargs.setdefault("timeout", 60)
        return subprocess.run(argv, env=env, check=True, **kwargs)

    run(
        [
            "gcloud",
            "container",
            "clusters",
            "get-credentials",
            args.cluster,
            "--region",
            args.region,
            "--project",
            args.project,
        ]
    )
    location = run(
        [
            "gcloud",
            "storage",
            "buckets",
            "describe",
            "gs://" + args.bucket,
            "--project",
            args.project,
            "--format=value(location)",
        ],
        capture_output=True,
        text=True,
    ).stdout.strip()
    if location.lower() != args.region:
        raise ValueError(
            f"Bucket location {location} differs from worker region {args.region}"
        )
    pod_name = "hackernews-benchmark-" + uuid.uuid4().hex[:12]
    kubectl = ["kubectl", "-n", args.namespace]
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": pod_name,
            "labels": {"app": "hackernews-benchmark", "qualification-owner": pod_name},
        },
        "spec": {
            "restartPolicy": "Never",
            "activeDeadlineSeconds": args.lifetime,
            "automountServiceAccountToken": False,
            "nodeSelector": {
                "topology.kubernetes.io/region": args.region,
                "kubernetes.io/arch": "amd64",
            },
            "containers": [
                {
                    "name": "benchmark",
                    "image": SDK_IMAGE,
                    "command": [
                        "python3",
                        "-c",
                        f"import time; time.sleep({args.lifetime})",
                    ],
                    "resources": {
                        "requests": {
                            "cpu": args.cpu,
                            "memory": args.memory,
                            "ephemeral-storage": args.disk,
                        },
                        "limits": {
                            "cpu": args.cpu,
                            "memory": args.memory,
                            "ephemeral-storage": args.disk,
                        },
                    },
                }
            ],
        },
    }
    created = False
    create_attempted = False
    worker = None
    try:
        create_attempted = True
        run(kubectl + ["create", "-f", "-"], input=json.dumps(pod), text=True)
        created = True
        run(
            kubectl
            + ["wait", "--for=condition=Ready", "pod/" + pod_name, "--timeout=300s"],
            timeout=330,
        )
        worker = json.loads(
            run(
                kubectl + ["get", "pod", pod_name, "-o", "json"],
                capture_output=True,
                text=True,
            ).stdout
        )
        (args.output / "worker.json").write_text(json.dumps(worker, indent=2) + "\n")
        run(kubectl + ["exec", pod_name, "--", "mkdir", "-p", "/workspace"])
        run(kubectl + ["cp", str(args.binary), pod_name + ":/workspace/antfly"])
        run(
            kubectl
            + [
                "cp",
                str(Path(__file__).with_name("poc.py")),
                pod_name + ":/workspace/poc.py",
            ]
        )
        copy_result(
            run,
            kubectl
            + [
                "cp",
                str(Path(__file__).with_name("regional_worker.py")),
                pod_name + ":/workspace/regional_worker.py",
            ],
        )
        run(kubectl + ["exec", pod_name, "--", "chmod", "+x", "/workspace/antfly"])
        worker_start = datetime.fromisoformat(
            worker["status"]["startTime"].replace("Z", "+00:00")
        )
        remaining_lifetime = max(
            0,
            args.lifetime - (datetime.now(timezone.utc) - worker_start).total_seconds(),
        )
        token, credential_admission = fresh_bearer_for_lifetime(
            run, args.project, remaining_lifetime + 30
        )
        (args.output / "credential-admission.json").write_text(
            json.dumps(credential_admission, indent=2) + "\n"
        )
        # Reuse this admitted token across cycles. Rotating a bearer also changes
        # the publication's credential identity and would force a rebuild.
        reports = {}
        for mode in args.modes:
            command = kubectl + [
                "exec",
                "-i",
                pod_name,
                "--",
                "python3",
                "/workspace/poc.py",
                "--binary",
                "/workspace/antfly",
                "--project",
                args.project,
                "--bucket",
                args.bucket,
                "--prefix",
                args.source_prefix,
                "--artifact-prefix",
                args.run_prefix + "/" + mode,
                "--state",
                "/workspace/" + mode,
                "--cold-cache",
                "--require-filters",
                "--bearer-stdin",
                "--repeats",
                str(args.repeats),
                "--expected-rows",
                str(args.expected_rows),
                "--build-timeout",
                str(args.build_timeout),
                "--concurrency",
                str(args.concurrency),
                "--cursor-retention-ms",
                str(args.cursor_retention_ms),
            ]
            command.extend(["--cursor-pages", str(args.cursor_pages)])
            if args.store_source:
                command.append("--store-source")
            if args.qualify_composed_sources:
                command.append("--qualify-composed-sources")
            if args.benchmark_composed:
                command.extend(
                    [
                        "--benchmark-composed",
                        "--composed-current-rows",
                        str(args.composed_current_rows),
                    ]
                )
            if args.cursor_term:
                command.extend(["--cursor-term", args.cursor_term])
            if args.workload_terms:
                command.extend(["--workload-terms", *args.workload_terms])
            if mode == "text-only":
                command.append("--text-only")
            reports[mode] = []
            for cycle in range(args.cycles):
                label = mode + "-" + str(cycle + 1)
                directory = "/workspace/jobs/" + uuid.uuid4().hex
                worker_command = command[command.index("--") + 1 :]
                (args.output / (label + "-job.json")).write_text(
                    json.dumps(
                        {
                            "directory": directory,
                            "command_sha256": hashlib.sha256(
                                json.dumps(worker_command).encode()
                            ).hexdigest(),
                        },
                        indent=2,
                    )
                    + "\n"
                )
                deadline = time.monotonic() + max(
                    0,
                    args.lifetime
                    - (datetime.now(timezone.utc) - worker_start).total_seconds(),
                )
                exit_code = remote_job(
                    run,
                    kubectl,
                    pod_name,
                    worker_command,
                    token,
                    directory,
                    deadline,
                    failure_record=args.output / (label + "-transport-failure.json"),
                    pod_uid=worker["metadata"]["uid"],
                )
                copy_result(
                    run,
                    kubectl
                    + [
                        "cp",
                        pod_name + ":" + directory + "/output.log",
                        str(args.output / (label + ".log")),
                    ],
                )
                if exit_code:
                    try:
                        copy_result(
                            run,
                            kubectl
                            + [
                                "cp",
                                pod_name + ":/workspace/" + mode + "/failure.json",
                                str(args.output / (label + "-failure.json")),
                            ],
                        )
                    except subprocess.CalledProcessError:
                        pass
                    copy_result(
                        run,
                        kubectl
                        + [
                            "cp",
                            pod_name + ":/workspace/" + mode + "/server.log",
                            str(args.output / (label + "-server.log")),
                        ],
                    )
                    raise RuntimeError(
                        f"{label} failed; see {args.output / (label + '.log')}"
                    )
                copy_result(
                    run,
                    kubectl
                    + [
                        "cp",
                        pod_name + ":/workspace/" + mode + "/report.json",
                        str(args.output / (label + ".json")),
                    ],
                )
                result = json.loads((args.output / (label + ".json")).read_text())
                reports[mode].append(result)
                print(
                    json.dumps(
                        {
                            "mode": mode,
                            "cycle": cycle + 1,
                            "cold_ms": result["first_search_ms"],
                            "warm_ms": result["warm_search_ms"],
                            "restart_ms": result["after_restart_ms"],
                        }
                    ),
                    flush=True,
                )
        compare_runs([cycle for cycles in reports.values() for cycle in cycles])
        digest = hashlib.sha256()
        with args.binary.open("rb") as binary:
            for chunk in iter(lambda: binary.read(1048576), b""):
                digest.update(chunk)
        report = {
            "project": args.project,
            "cluster": args.cluster,
            "region": args.region,
            "node": worker["spec"]["nodeName"],
            "resources": worker["spec"]["containers"][0]["resources"],
            "image": SDK_IMAGE,
            "credential_admission": credential_admission,
            "revision": args.revision,
            "binary_sha256": digest.hexdigest(),
            "runs": reports,
            "modes": args.modes,
            "cross_mode_comparison": len(args.modes) > 1,
            "row_count": args.expected_rows,
            "note": "Pinned regional qualification; compare only matching source, resources and optimization.",
        }
        (args.output / "regional.json").write_text(json.dumps(report, indent=2) + "\n")
    finally:
        cleanup = {"pod": pod_name, "created": created, "deleted": not create_attempted}
        try:
            if create_attempted:
                current = run(
                    kubectl
                    + ["get", "pod", pod_name, "-o", "json", "--ignore-not-found"],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                if not current.stdout.strip():
                    cleanup["deleted"] = True
                else:
                    observed = json.loads(current.stdout)
                    if (
                        worker is not None
                        and observed["metadata"]["uid"] != worker["metadata"]["uid"]
                    ):
                        cleanup["deleted"] = True  # The owned incarnation is gone.
                    elif (
                        observed["metadata"]
                        .get("labels", {})
                        .get("qualification-owner")
                        != pod_name
                    ):
                        raise RuntimeError("Benchmark cleanup ownership differs")
                    else:
                        cleanup["created"] = True
                        cleanup["pod_uid"] = observed["metadata"]["uid"]
                        run(
                            kubectl
                            + [
                                "delete",
                                "pod",
                                pod_name,
                                "--wait=true",
                                "--timeout=60s",
                                "--ignore-not-found",
                            ],
                            timeout=75,
                        )
                        cleanup["deleted"] = True
        finally:
            (args.output / "cleanup.json").write_text(
                json.dumps(cleanup, indent=2) + "\n"
            )
            credentials.cleanup()


if __name__ == "__main__":
    main()
