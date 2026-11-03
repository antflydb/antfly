#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Production HTTP CPU image correctness, holdout retrieval, load and recovery gates."""

import argparse
import base64
import json
import math
import statistics
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import cpu_http_qualification as common
import qualify
import video_compare
from video_deadline_qualification import declared_request


def load_suite(args):
    suite = json.loads(args.suite.read_text())
    if len({row["name"] for row in suite["cases"]}) != len(suite["cases"]):
        raise ValueError("duplicate image case")
    for row in suite["cases"]:
        if common.digest(Path(row["path"])) != row["sha256"]:
            raise ValueError("image asset digest differs")
    return suite


def payload(suite, case, identity=None, dimensions=768, representation="group"):
    by_name = {row["name"]: row for row in suite["cases"]}
    content = []
    for part in case["parts"]:
        if "text" in part:
            content.append({"type": "text", "text": part["text"]})
        else:
            row = by_name[part["image"]]
            data = base64.b64encode(Path(row["path"]).read_bytes()).decode()
            if representation == "image_url":
                content.append(
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:{row['mime']};base64,{data}"},
                    }
                )
            else:
                content.append(
                    {"type": "media", "mime_type": row["mime"], "data": data}
                )
    body = {
        "model": "embeddinggemma2",
        "input": content if representation == "flat" else [{"content": content}],
        "dimensions": dimensions,
    }
    if identity:
        body["model_identity"] = identity
    return body


def metrics(url):
    group = video_compare.embedding_metrics(url, include_outcomes=True)
    general = common.cpu_metrics(url)
    return {
        **group,
        "request_active": general["active"],
        "requests": general["requests"],
        "units": general["units"],
        "scratch": general["scratch"],
    }


def idle(url):
    until = time.monotonic() + 30
    while True:
        current = metrics(url)
        if all(
            current[key] == 0
            for key in ["active", "request_active", "admitted", "units", "scratch"]
        ):
            return current
        if time.monotonic() > until:
            raise TimeoutError("image request leaked resources")
        time.sleep(0.05)


def call(args, body, expected=None, identity=None):
    result, seconds = qualify.post(args.url + "/ai/v1", "/embed", body)
    common.validate(result, expected or [result["data"][0]["embedding"]], identity)
    return result, seconds


def collect(args):
    suite = load_suite(args)
    report = {
        "version": 1,
        "status": "running",
        "server": common.server_provenance(args),
        "suite_sha256": common.digest(args.suite),
        "cases": {},
    }
    identity = None
    try:
        for case in suite["cases"]:
            result, seconds = call(
                args, payload(suite, case, identity), identity=identity
            )
            identity = result["model_identity"]
            report["model_identity"] = identity
            report["cases"][case["name"]] = {
                "vector": result["data"][0]["embedding"],
                "seconds": seconds,
            }
            common.save(args.output, report)
            print(case["name"], seconds, flush=True)
        report["status"] = "pass"
    except Exception as error:
        report.update(status="fail", failure=repr(error))
        raise
    finally:
        report["memory_at_end"] = common.memory_tree(args.server_pid)
        common.save(args.output, report)


def vector_agreement(actual, expected, max_abs, min_cosine):
    if (
        len(actual) != 768
        or len(expected) != 768
        or not all(math.isfinite(x) for x in actual + expected)
    ):
        raise ValueError("invalid reference/candidate image vector")
    norm = math.sqrt(sum(x * x for x in actual))
    expected_norm = math.sqrt(sum(x * x for x in expected))
    if abs(norm - 1) > 1e-4 or abs(expected_norm - 1) > 1e-4:
        raise ValueError("unnormalized reference/candidate image vector")
    error = max(abs(x - y) for x, y in zip(actual, expected))
    cosine = sum(x * y for x, y in zip(actual, expected)) / (norm * expected_norm)
    if error > max_abs or cosine < min_cosine:
        raise ValueError(f"image numerical gate failed max_abs={error} cosine={cosine}")
    return {"max_abs": error, "cosine": cosine}


def rank_metrics(order, relevant):
    if not relevant:
        raise ValueError("retrieval labels must be nonempty")
    dcg = sum(
        (name in relevant) / math.log2(at + 2) for at, name in enumerate(order[:10])
    )
    ideal = sum(1 / math.log2(at + 2) for at in range(min(10, len(relevant))))
    return {
        "recall1": float(bool(set(order[:1]) & relevant)),
        "recall5": float(bool(set(order[:5]) & relevant)),
        "ndcg10": dcg / ideal,
    }


def retrieval(suite, reference, candidate, query_vectors):
    import numpy as np

    images = [row["name"] for row in suite["cases"] if row["role"] == "retrieval"]
    queries = [row["name"] for row in suite["queries"]]
    if len(images) != 128 or len(queries) != 640 or set(query_vectors) != set(queries):
        raise ValueError("exact held-out image/caption coverage required")
    text = np.array([query_vectors[name] for name in queries], dtype=np.float64)
    if (
        text.shape != (640, 768)
        or not np.isfinite(text).all()
        or np.max(np.abs(np.linalg.norm(text, axis=1) - 1)) > 1e-4
    ):
        raise ValueError("invalid fixed caption vectors")
    text /= np.linalg.norm(text, axis=1, keepdims=True)
    result = {}
    for role, vectors in [
        ("upstream", reference["cases"]),
        ("cpu", candidate["cases"]),
    ]:
        image = np.array([vectors[name]["vector"] for name in images], dtype=np.float64)
        image /= np.linalg.norm(image, axis=1, keepdims=True)
        scores = text @ image.T
        text_to_image = []
        for index, row in enumerate(suite["queries"]):
            order = [images[at] for at in np.argsort(-scores[index], kind="stable")]
            text_to_image.append(rank_metrics(order, {row["relevant_image"]}))
        image_to_text = []
        for index, name in enumerate(images):
            order = [queries[at] for at in np.argsort(-scores[:, index], kind="stable")]
            labels = {
                row["name"] for row in suite["queries"] if row["relevant_image"] == name
            }
            image_to_text.append(rank_metrics(order, labels))
        result[role] = {
            direction: {
                key: sum(row[key] for row in values) / len(values) for key in values[0]
            }
            for direction, values in [
                ("text_to_image", text_to_image),
                ("image_to_text", image_to_text),
            ]
        }
    result["losses"] = {
        direction: {
            key: result["upstream"][direction][key] - result["cpu"][direction][key]
            for key in result["cpu"][direction]
        }
        for direction in result["cpu"]
    }
    return result


def evaluate(args):
    suite = load_suite(args)
    reference = json.loads(args.reference.read_text())
    candidate = json.loads(args.candidate.read_text())
    policy = json.loads(args.policy.read_text())
    expected = {row["name"] for row in suite["cases"]}
    for report in [reference, candidate]:
        if (
            report["status"] != "pass"
            or report["suite_sha256"] != common.digest(args.suite)
            or set(report["cases"]) != expected
        ):
            raise ValueError(
                "complete matching successful image reference/candidate required"
            )
    report = {
        "version": 1,
        "status": "running",
        "reference_sha256": common.digest(args.reference),
        "candidate_sha256": common.digest(args.candidate),
        "policy_sha256": common.digest(args.policy),
        "cases": {},
    }
    try:
        for row in suite["cases"]:
            lossy = any(
                next(c for c in suite["cases"] if c["name"] == part["image"])["mime"]
                == "image/jpeg"
                for part in row["parts"]
                if "image" in part
            )
            limit = policy["numerical"]["jpeg_max_abs" if lossy else "lossless_max_abs"]
            cosine = policy["numerical"][
                "jpeg_min_cosine" if lossy else "lossless_min_cosine"
            ]
            report["cases"][row["name"]] = vector_agreement(
                candidate["cases"][row["name"]]["vector"],
                reference["cases"][row["name"]]["vector"],
                limit,
                cosine,
            )
        query_vectors = {
            name: row["vector"] for name, row in reference["queries"].items()
        }
        report["retrieval"] = retrieval(suite, reference, candidate, query_vectors)
        for direction, losses in report["retrieval"]["losses"].items():
            for key, loss in losses.items():
                if loss > policy["retrieval"]["max_" + key + "_loss"]:
                    raise ValueError(
                        f"retrieval gate failed {direction} {key} loss {loss}"
                    )
        if args.cuda_captions:
            gpu = json.loads(args.cuda_captions.read_text())
            if gpu["status"] != "pass" or gpu["suite_sha256"] != common.digest(
                args.suite
            ):
                raise ValueError("matching completed CUDA caption vectors required")
            fixed = {name: row["vector"] for name, row in gpu["cases"].items()}
            report["cuda_caption_sha256"] = common.digest(args.cuda_captions)
            report["cuda_query_identity"] = gpu["model_identity"]
            report["retrieval_with_cuda_captions"] = retrieval(
                suite, reference, candidate, fixed
            )
            if args.cuda_images:
                image_gpu = json.loads(args.cuda_images.read_text())
                expected_images = {
                    row["path"]: row
                    for row in suite["cases"]
                    if row["role"] == "retrieval"
                }
                if image_gpu["backend"] != "cuda" or len(image_gpu["cases"]) != 128:
                    raise ValueError("complete canonical CUDA image baseline required")
                gpu_cases = {}
                for row in image_gpu["cases"]:
                    case = expected_images.get(row["image"])
                    if (
                        case is None
                        or row["sha256"] != case["sha256"]
                        or case["name"] in gpu_cases
                    ):
                        raise ValueError("CUDA image provenance/coverage differs")
                    vector_agreement(row["output"], row["output"], 1e-4, 0.99999)
                    gpu_cases[case["name"]] = {"vector": row["output"]}
                report["cuda_images_sha256"] = common.digest(args.cuda_images)
                report["cpu_vs_cuda_retrieval"] = retrieval(
                    suite, {"cases": gpu_cases}, candidate, fixed
                )
                report["cpu_vs_cuda_retrieval"]["cuda"] = report[
                    "cpu_vs_cuda_retrieval"
                ].pop("upstream")
            for field in ["retrieval_with_cuda_captions", "cpu_vs_cuda_retrieval"]:
                if field not in report:
                    continue
                for direction, losses in report[field]["losses"].items():
                    for key, loss in losses.items():
                        if loss > policy["retrieval"]["max_" + key + "_loss"]:
                            raise ValueError(
                                f"{field} quality gate failed {direction} {key} loss {loss}"
                            )
        report["status"] = "pass"
    except Exception as error:
        report.update(status="fail", failure=repr(error))
        raise
    finally:
        common.save(args.output, report)
    print(
        json.dumps(
            {
                "status": report["status"],
                "max_abs": max(row["max_abs"] for row in report["cases"].values()),
                "retrieval": report["retrieval"],
            }
        ),
        flush=True,
    )


def candidate(args, suite):
    report = json.loads(args.candidate.read_text())
    if (
        report["status"] != "pass"
        or report["suite_sha256"] != common.digest(args.suite)
        or set(report["cases"]) != {row["name"] for row in suite["cases"]}
    ):
        raise ValueError("complete matching CPU image candidate required")
    return report


def contracts(args):
    suite = load_suite(args)
    baseline = candidate(args, suite)
    rows = {row["name"]: row for row in suite["cases"]}
    identity = baseline["model_identity"]
    report = {
        "version": 1,
        "status": "running",
        "server": common.server_provenance(args),
        "candidate_sha256": common.digest(args.candidate),
        "checks": {},
    }
    try:
        first = rows["flickr-000"]
        expected = baseline["cases"][first["name"]]["vector"]
        for form in ["group", "flat", "image_url"]:
            call(
                args,
                payload(suite, first, identity, representation=form),
                [expected],
                identity,
            )
            report["checks"][form] = "pass"
        for name in ["rgb-96x64", "text-image"]:
            full = baseline["cases"][name]["vector"]
            for dimension in [128, 256, 512, 768]:
                body = payload(suite, rows[name], identity, dimension)
                call(args, body, [common.normalized_prefix(full, dimension)], identity)
        report["checks"]["dimension_projection_and_restoration"] = "pass"
        body = payload(suite, first, identity)
        body["input"].extend(payload(suite, rows["flickr-001"], identity)["input"])
        expected = [
            baseline["cases"][name]["vector"] for name in ["flickr-000", "flickr-001"]
        ]
        call(args, body, expected, identity)
        report["checks"]["batch_order"] = "pass"
        bad = {
            "content": [
                {
                    "type": "media",
                    "mime_type": "image/png",
                    "data": base64.b64encode(b"corrupt image").decode(),
                }
            ]
        }
        partial = {
            **body,
            "input": [body["input"][0], bad, body["input"][1]],
            "error_policy": "per_item",
        }
        result, _ = qualify.post(args.url + "/ai/v1", "/embed", partial)
        if (
            [row["index"] for row in result["data"]] != [0, 2]
            or len(result.get("errors", [])) != 1
            or result["errors"][0]["index"] != 1
            or result["errors"][0]["status"] != 400
            or result["errors"][0]["retryable"]
        ):
            raise ValueError("image partial-error/index contract failed")
        for row, vector in zip(result["data"], expected):
            qualify.compare(row["embedding"], vector, limit=1e-5)
        report["checks"]["partial_errors"] = "pass"
        qualify.post(
            args.url + "/ai/v1",
            "/embed",
            {"model": "embeddinggemma2", "input": [bad]},
            400,
        )
        wrong = payload(suite, first, "a" * 64)
        qualify.post(args.url + "/ai/v1", "/embed", wrong, 409)
        wrong = payload(suite, first, identity, 769)
        qualify.post(args.url + "/ai/v1", "/embed", wrong, 400)
        report["checks"]["invalid_image_dimension_and_identity"] = "pass"
        qualify.compare(
            baseline["cases"]["animated-gif"]["vector"],
            baseline["cases"]["animated-first"]["vector"],
            limit=1e-5,
        )
        report["checks"]["gif_first_frame"] = "pass"
        report["final_metrics"] = idle(args.url)
        report["status"] = "pass"
    except Exception as error:
        report.update(status="fail", failure=repr(error))
        raise
    finally:
        common.save(args.output, report)
    print(json.dumps(report), flush=True)


def load(args):
    suite = load_suite(args)
    baseline = candidate(args, suite)
    policy = json.loads(args.policy.read_text())["serving"]
    identity = baseline["model_identity"]
    by_name = {row["name"]: row for row in suite["cases"]}
    cases = [
        by_name[name]
        for name in ["flickr-000", "flickr-001", "rgb-513x257", "rgba", "animated-gif"]
    ]
    report = {
        "version": 1,
        "status": "running",
        "server": common.server_provenance(args),
        "candidate_sha256": common.digest(args.candidate),
        "policy_sha256": common.digest(args.policy),
        "profiles": [],
    }
    try:
        profiles = [
            (n, policy["profile_seconds"]) for n in policy["profile_clients"]
        ] + [(policy["soak_clients"], policy["soak_seconds"])]
        for clients, seconds in profiles:
            before = idle(args.url)
            memory_before = common.memory_tree(args.server_pid)
            worker = max(memory_before, key=lambda pid: memory_before[pid]["VmRSS"])
            started = time.monotonic()
            stop = started + seconds

            def client(offset, stop=stop, clients=clients):
                samples = []
                index = offset
                while time.monotonic() < stop:
                    row = cases[index % len(cases)]
                    index += clients
                    _, elapsed = call(
                        args,
                        payload(suite, row, identity),
                        [baseline["cases"][row["name"]]["vector"]],
                        identity,
                    )
                    samples.append(elapsed)
                return samples

            with ThreadPoolExecutor(max_workers=clients) as pool:
                futures = [pool.submit(client, i) for i in range(clients)]
                memory_samples = [memory_before]
                while not all(f.done() for f in futures):
                    time.sleep(0.25)
                    memory_samples.append(common.memory_tree(args.server_pid))
                samples = [value for future in futures for value in future.result()]
            after = idle(args.url)
            memory_after = common.memory_tree(args.server_pid)
            if any(
                set(row) != set(memory_before)
                for row in memory_samples + [memory_after]
            ):
                raise ValueError("worker changed during image load")
            peak = max(row[worker]["VmRSS"] for row in memory_samples + [memory_after])
            highwater = max(
                row[worker]["VmHWM"] for row in memory_samples + [memory_after]
            )
            growth = memory_after[worker]["VmRSS"] - memory_before[worker]["VmRSS"]
            result = {
                "clients": clients,
                "requested_seconds": seconds,
                "elapsed_seconds": time.monotonic() - started,
                "requests": len(samples),
                "errors": 0,
                "p50_seconds": statistics.median(samples),
                "p95_seconds": common.percentile(samples, 0.95),
                "p99_seconds": common.percentile(samples, 0.99),
                "max_seconds": max(samples),
                "worker_peak_rss_bytes": peak,
                "worker_lifetime_hwm_bytes": highwater,
                "worker_rss_growth_bytes": growth,
                "metrics_before": before,
                "metrics_after": after,
                "memory_before": memory_before,
                "memory_after": memory_after,
                "latencies_seconds": samples,
            }
            result["requests_per_second"] = (
                result["requests"] / result["elapsed_seconds"]
            )
            report["profiles"].append(result)
            common.save(args.output, report)
            print(
                json.dumps(
                    {
                        k: v
                        for k, v in result.items()
                        if k
                        not in ["latencies_seconds", "memory_before", "memory_after"]
                    }
                ),
                flush=True,
            )
            if (
                after["completed"] - before["completed"] != len(samples)
                or after["requests"] - before["requests"] != len(samples)
                or any(
                    after[key] != before[key]
                    for key in ["failed", "cancelled", "timed_out"]
                )
            ):
                raise ValueError("load completion/outcome accounting failed")
            if (
                result["p95_seconds"] > policy["p95_seconds_per_client"] * clients
                or highwater > policy["max_worker_rss_bytes"]
                or growth > policy["max_profile_rss_growth_bytes"]
            ):
                raise ValueError("image latency/memory acceptance limit exceeded")
        report["status"] = "pass"
    except Exception as error:
        report.update(status="fail", failure=repr(error))
        raise
    finally:
        common.save(args.output, report)


def recovery(args):
    suite = load_suite(args)
    baseline = candidate(args, suite)
    row = next(row for row in suite["cases"] if row["name"] == "flickr-000")
    identity = baseline["model_identity"]
    expected = baseline["cases"][row["name"]]["vector"]
    body = payload(suite, row, identity)
    report = {
        "version": 1,
        "status": "running",
        "server": common.server_provenance(args),
        "candidate_sha256": common.digest(args.candidate),
    }
    try:
        before = idle(args.url)
        started = time.monotonic()
        status, data = declared_request(args.url, body, "100")
        if (
            status != 504
            or data["error"] != "INFERENCE_TIMEOUT"
            or data.get("retryable") is not True
            or time.monotonic() - started > 5
        ):
            raise ValueError(f"image deadline failed: {status} {data}")
        after = idle(args.url)
        if (
            after["timed_out"] != before["timed_out"] + 1
            or after["completed"] != before["completed"]
            or after["failed"] != before["failed"]
        ):
            raise ValueError("image deadline lost attribution or failed cleanup")
        report["deadline"] = {
            "seconds": time.monotonic() - started,
            "status": status,
            "before": before,
            "after": after,
        }
        call(args, body, [expected], identity)
        report["disconnect"] = video_compare.disconnect_and_retry(
            args.url, body, expected, request_timeout=60
        )
        report["half_close"] = video_compare.half_close_and_read(
            args.url, body, expected, request_timeout=60
        )
        # An image that times out while another image owns execution must not
        # cancel the admitted peer or disturb its vector/resource lifetime.
        before = idle(args.url)
        peer = next(item for item in suite["cases"] if item["name"] == "flickr-001")
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                call,
                args,
                payload(suite, peer, identity),
                [baseline["cases"][peer["name"]]["vector"]],
                identity,
            )
            until = time.monotonic() + 5
            while metrics(args.url)["active"] == 0:
                if future.done() or time.monotonic() >= until:
                    raise ValueError("missed concurrent image timeout window")
                time.sleep(0.01)
            status, data = declared_request(args.url, body, "100")
            if status != 504 or data["error"] != "INFERENCE_TIMEOUT":
                raise ValueError("queued image deadline failed")
            future.result()
        after = idle(args.url)
        if (
            after["completed"] != before["completed"] + 1
            or after["timed_out"] != before["timed_out"] + 1
        ):
            raise ValueError("queued image timeout disturbed its admitted peer")
        report["concurrent_timeout_isolation"] = {
            "status": "pass",
            "before": before,
            "after": after,
        }
        flat = payload(suite, row, identity, representation="flat")
        status, data = declared_request(args.url, flat, "100")
        if status != 504 or data["error"] != "INFERENCE_TIMEOUT":
            raise ValueError("flat image deadline failed")
        idle(args.url)
        call(args, flat, [expected], identity)
        report["flat_deadline_and_retry"] = "pass"
        report["final_metrics"] = idle(args.url)
        report["status"] = "pass"
    except Exception as error:
        report.update(status="fail", failure=repr(error))
        raise
    finally:
        common.save(args.output, report)
    print(json.dumps(report), flush=True)


def overload(args):
    suite = load_suite(args)
    baseline = candidate(args, suite)
    row = next(row for row in suite["cases"] if row["name"] == "flickr-000")
    identity = baseline["model_identity"]
    expected = [baseline["cases"][row["name"]]["vector"]]
    encoded = json.dumps(payload(suite, row, identity)).encode()
    barrier = threading.Barrier(8, timeout=10)
    report = {
        "version": 1,
        "status": "running",
        "server": common.server_provenance(args),
        "clients": 8,
        "responses": [],
    }

    def request(_):
        barrier.wait()
        started = time.monotonic()
        req = urllib.request.Request(
            args.url + "/ai/v1/embed", encoded, {"Content-Type": "application/json"}
        )
        try:
            response = urllib.request.urlopen(req, timeout=90)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            result = json.loads(response.read(1024 * 1024))
            record = {"status": response.status, "seconds": time.monotonic() - started}
            if response.status == 200:
                common.validate(result, expected, identity)
            elif response.status == 503:
                if (
                    result.get("reason")
                    != {
                        "SERVICE_UNAVAILABLE": "inference_admission",
                        "MODEL_RESOURCE_BUSY": "inference_capacity",
                    }.get(result["error"], "invalid")
                    or result.get("retryable") is not True
                    or result.get("retry_after_ms", 0) <= 0
                    or int(response.headers.get("Retry-After", "0")) <= 0
                ):
                    raise ValueError(f"invalid retryable image rejection: {result}")
                record["error"] = result
            else:
                raise ValueError(
                    f"unexpected image overload HTTP {response.status}: {result}"
                )
            return record

    try:
        before = idle(args.url)
        memory_before = common.memory_tree(args.server_pid)
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(request, i) for i in range(8)]
            peak = 0
            while not all(f.done() for f in futures):
                peak = max(peak, metrics(args.url)["admitted"])
                time.sleep(0.05)
            report["responses"] = [f.result() for f in futures]
        after = idle(args.url)
        successes = sum(row["status"] == 200 for row in report["responses"])
        denials = 8 - successes
        if (
            not successes
            or not denials
            or peak > 4
            or after["completed"] - before["completed"] != successes
        ):
            raise ValueError("image admission bound or completion accounting failed")
        if set(memory_before) != set(common.memory_tree(args.server_pid)):
            raise ValueError("overload replaced worker")
        call(args, payload(suite, row, identity), expected, identity)
        report.update(
            status="pass",
            successes=successes,
            capacity_denials=denials,
            peak_admitted=peak,
            metrics_before=before,
            metrics_after=after,
            final_metrics=idle(args.url),
        )
    except Exception as error:
        report.update(status="fail", failure=repr(error))
        raise
    finally:
        common.save(args.output, report)
    print(
        json.dumps(
            {
                k: report[k]
                for k in ["status", "successes", "capacity_denials", "peak_admitted"]
            }
        ),
        flush=True,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "operation",
        choices=["collect", "evaluate", "contracts", "load", "recovery", "overload"],
    )
    parser.add_argument("--suite", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--url", default="http://127.0.0.1:18792")
    parser.add_argument("--server-pid", type=int)
    parser.add_argument("--reference", type=Path)
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--policy", type=Path)
    parser.add_argument("--cuda-captions", type=Path)
    parser.add_argument("--cuda-images", type=Path)
    args = parser.parse_args()
    {
        "collect": collect,
        "evaluate": evaluate,
        "contracts": contracts,
        "load": load,
        "recovery": recovery,
        "overload": overload,
    }[args.operation](args)


if __name__ == "__main__":
    main()
