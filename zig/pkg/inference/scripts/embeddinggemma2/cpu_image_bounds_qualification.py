#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Qualify declared image-byte/pixel bounds and the original GIF through data URLs."""

import argparse
import json
import shutil
import zlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import cpu_http_qualification as common
import cpu_image_campaign as campaign
import cpu_image_qualification as image
import cpu_image_reference as upstream
import numpy as np
from PIL import Image

GIF_SHA256 = "b0acdc9a01f91f6123dbff38d4bee59d0d6e4b26071b5c83937b242b6b202378"


def prepare(args):
    if common.digest(args.gif) != GIF_SHA256:
        raise ValueError("original pinned GIF differs")
    args.results.mkdir(parents=True, exist_ok=True)
    shutil.copy2(args.gif, args.results / "earth.gif")
    with Image.open(args.gif) as first:
        first.seek(0)
        first.convert("RGB").save(args.results / "earth-first.png")
    random = np.random.default_rng(17).integers(0, 256, (1024, 1024, 3), dtype=np.uint8)
    path = args.results / "exact-4mib.png"
    Image.fromarray(random).save(path)
    png = path.read_bytes()
    padding = 4 * 1024**2 - len(png) - 12
    data = b"qualification\0" + b"x" * (padding - 14)
    chunk = b"tEXt" + data
    path.write_bytes(
        png[:-12]
        + len(data).to_bytes(4, "big")
        + chunk
        + zlib.crc32(chunk).to_bytes(4, "big")
        + png[-12:]
    )
    if path.stat().st_size != 4 * 1024**2:
        raise ValueError("encoded boundary fixture length differs")
    y, x = np.indices((2048, 2048), dtype=np.uint16)
    rgba = np.stack(
        [(x * 3 + y) % 256, (x + y * 5) % 256, (x * 7 + y * 11) % 256, (x + y) % 256],
        axis=-1,
    ).astype(np.uint8)
    Image.fromarray(rgba).save(args.results / "rgba-2048-square.png")
    suite = {"version": 1, "queries": [], "cases": []}
    for filename in [
        "earth.gif",
        "earth-first.png",
        "exact-4mib.png",
        "rgba-2048-square.png",
    ]:
        path = args.results / filename
        with Image.open(path) as decoded:
            suite["cases"].append(
                {
                    "name": path.stem,
                    "role": "boundary",
                    "path": str(path.resolve()),
                    "sha256": common.digest(path),
                    "mime": Image.MIME[decoded.format],
                    "width": decoded.width,
                    "height": decoded.height,
                    "encoded_bytes": path.stat().st_size,
                    "parts": [{"image": path.stem}],
                }
            )
    common.save(args.results / "suite.json", suite)
    return suite


def run(args):
    suite = prepare(args)
    args.suite = args.results / "suite.json"
    upstream.oracle(args.model_dir, args.suite, args.results / "reference.json")
    oracle = json.loads((args.results / "reference.json").read_text())
    policy = json.loads(args.policy.read_text())
    result = {
        "version": 1,
        "status": "running",
        "scope": "source byte/pixel boundaries and original real GIF; supplements the full image campaign",
        "suite_sha256": common.digest(args.suite),
        "reference_sha256": common.digest(args.results / "reference.json"),
        "cases": {},
    }
    try:
        with campaign.server(args):
            result["server"] = common.server_provenance(args)
            first = suite["cases"][0]
            warm, elapsed = image.call(
                args, image.payload(suite, first, representation="image_url")
            )
            result["cold_first_gif_seconds"] = elapsed
            identity = warm["model_identity"]
            if elapsed > policy["serving"]["cold_first_image_max_seconds"]:
                raise ValueError("boundary cold request exceeded policy")
            for case in suite["cases"]:
                response, elapsed = image.call(
                    args,
                    image.payload(suite, case, identity, representation="image_url"),
                    identity=identity,
                )
                vector = response["data"][0]["embedding"]
                agreement = image.vector_agreement(
                    vector,
                    oracle["cases"][case["name"]]["vector"],
                    policy["numerical"]["lossless_max_abs"],
                    policy["numerical"]["lossless_min_cosine"],
                )
                if elapsed > policy["serving"]["p95_seconds_per_client"]:
                    raise ValueError(
                        "boundary image latency exceeded single-client limit"
                    )
                result["cases"][case["name"]] = {
                    **agreement,
                    "seconds": elapsed,
                    "vector": vector,
                    "encoded_bytes": case["encoded_bytes"],
                    "width": case["width"],
                    "height": case["height"],
                }
                print(case["name"], elapsed, agreement, flush=True)
            common.save(args.results / "vectors.json", result)
            common.qualify.compare(
                result["cases"]["earth"]["vector"],
                result["cases"]["earth-first"]["vector"],
                limit=1e-5,
            )
            largest = next(
                case for case in suite["cases"] if case["name"] == "exact-4mib"
            )
            body = image.payload(suite, largest, identity, representation="image_url")
            before = image.idle(args.url)
            with ThreadPoolExecutor(max_workers=4) as pool:
                outputs = list(
                    pool.map(
                        lambda _: image.call(
                            args,
                            body,
                            [result["cases"]["exact-4mib"]["vector"]],
                            identity,
                        ),
                        range(4),
                    )
                )
            after = image.idle(args.url)
            if after["completed"] - before["completed"] != 4 or any(
                elapsed > 4 * policy["serving"]["p95_seconds_per_client"]
                for _, elapsed in outputs
            ):
                raise ValueError("four-client maximum-byte boundary failed")
            result["four_maximum_byte_requests"] = {
                "status": "pass",
                "seconds": [elapsed for _, elapsed in outputs],
                "before": before,
                "after": after,
            }
            text = json.loads(args.text_oracle.read_text())["cases"]["query_password"]
            image.call(args, common.payload(text, identity), [text["vector"]], identity)
            _, elapsed = image.call(
                args,
                image.payload(suite, first, identity, representation="image_url"),
                [result["cases"][first["name"]]["vector"]],
                identity,
            )
            result["image_after_text_seconds"] = elapsed
            result["final_metrics"] = image.idle(args.url)
            result["memory"] = common.memory_tree(args.server_pid)
            if (
                max(row["VmHWM"] for row in result["memory"].values())
                > policy["serving"]["max_worker_rss_bytes"]
            ):
                raise ValueError("boundary worker RSS exceeded policy")
            result.update(status="pass", model_identity=identity)
    except Exception as error:
        result.update(status="fail", failure=repr(error))
        raise
    finally:
        common.save(args.report, result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in [
        "binary",
        "model-dir",
        "results",
        "policy",
        "text-oracle",
        "gif",
        "report",
    ]:
        parser.add_argument("--" + name, type=Path, required=True)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
