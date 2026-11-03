#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Full SciFact test retrieval: CPU queries against fixed CUDA document vectors.

Reads the publisher-MD5-verified immutable archive from the shared fixture cache.
The CUDA query baseline and CPU query candidate search exactly the same corpus.
This is query compatibility qualification, not an exact vector-identity claim.
"""

import argparse
import csv
import hashlib
import io
import json
import math
import subprocess
import zipfile
from pathlib import Path

SCIFACT_MD5 = "5f7d1de60b170fc8027bb7898e2efca1"
MAX_NDCG10_LOSS = 0.005
MAX_RECALL10_LOSS = 0.01


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def load_dataset(archive):
    if hashlib.md5(archive.read_bytes()).hexdigest() != SCIFACT_MD5:
        raise ValueError("SciFact archive differs from publisher digest")
    with zipfile.ZipFile(archive) as source:
        docs = [
            json.loads(line)
            for line in source.read("scifact/corpus.jsonl").splitlines()
        ]
        queries = {
            row["_id"]: row
            for row in (
                json.loads(line)
                for line in source.read("scifact/queries.jsonl").splitlines()
            )
        }
        labels = {}
        for row in csv.DictReader(
            io.StringIO(source.read("scifact/qrels/test.tsv").decode()), delimiter="\t"
        ):
            relevance = int(row["score"])
            if relevance > 0:
                labels.setdefault(row["query-id"], {})[row["corpus-id"]] = relevance
    if len(docs) != 5183 or len(labels) != 300:
        raise ValueError("full SciFact corpus/test coverage differs")
    return (
        sorted(docs, key=lambda x: x["_id"]),
        [queries[qid] for qid in sorted(labels)],
        labels,
    )


def prepare(args):
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    docs, queries, labels = load_dataset(args.archive)
    dataset = {
        "version": 1,
        "archive_sha256": sha256(args.archive),
        "archive_md5": SCIFACT_MD5,
        "source": "https://huggingface.co/datasets/BeIR/scifact",
        "documents": len(docs),
        "test_queries": len(queries),
        "document_rendering": "title + newline + text, RETRIEVAL_DOCUMENT prefix",
        "policy": {
            "max_ndcg10_loss": MAX_NDCG10_LOSS,
            "max_recall10_loss": MAX_RECALL10_LOSS,
        },
        "qrels": labels,
    }
    (output / "dataset.json").write_text(json.dumps(dataset, indent=2) + "\n")
    rows = {
        "documents": [
            {
                "name": "d:" + row["_id"],
                "text": row["title"] + "\n" + row["text"],
                "task": "RETRIEVAL_DOCUMENT",
                "iterations": 1,
                "warmups": 0,
            }
            for row in docs
        ],
        "queries": [
            {
                "name": "q:" + row["_id"],
                "text": row["text"],
                "task": "RETRIEVAL_QUERY",
                "iterations": 1,
                "warmups": 0,
            }
            for row in queries
        ],
    }
    for role, cases in rows.items():
        (output / f"{role}.json").write_text(
            json.dumps({"cases": cases}, ensure_ascii=False) + "\n"
        )
        for at in range(0, len(cases), 256):
            (output / f"{role}-{at // 256:03}.suite.json").write_text(
                json.dumps({"cases": cases[at : at + 256]}, ensure_ascii=False) + "\n"
            )
    print(
        "prepared",
        len(docs),
        "documents and",
        len(queries),
        "held-out queries",
        flush=True,
    )


def cuda(args):
    output = args.output_dir.resolve()
    manifest = {
        "binary_sha256": sha256(args.binary),
        "checkpoint_receipt_sha256": sha256(
            args.model / "embeddinggemma2_receipt.json"
        ),
        "chunks": {},
    }
    identity = None
    for role in ("documents", "queries"):
        for suite in sorted(output.glob(f"{role}-*.suite.json")):
            receipt = suite.with_name(suite.name.replace(".suite.json", ".cuda.json"))
            expected_hash = sha256(suite)
            if receipt.exists():
                raw = json.loads(receipt.read_text())
                if raw["suite_sha256"] != expected_hash or raw["backend"] != "cuda":
                    raise ValueError("cached CUDA chunk differs")
            else:
                command = [
                    str(args.binary.resolve()),
                    "--model",
                    str(args.model.resolve()),
                    "--backend",
                    "cuda",
                    "--suite",
                    str(suite),
                    "--output",
                    str(receipt),
                    "--warmups",
                    "0",
                    "--iterations",
                    "1",
                ]
                with receipt.with_suffix(".log").open("w") as log:
                    subprocess.run(
                        command,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        check=True,
                        timeout=900,
                    )
                raw = json.loads(receipt.read_text())
            if identity is not None and raw["model_identity"] != identity:
                raise ValueError("CUDA corpus identity changed")
            identity = raw["model_identity"]
            manifest["chunks"][receipt.name] = {
                "sha256": sha256(receipt),
                "suite_sha256": expected_hash,
                "cases": len(raw["cases"]),
            }
            manifest["model_identity"] = identity
            (output / "cuda-manifest.json").write_text(
                json.dumps(manifest, indent=2) + "\n"
            )
            print("completed", receipt.name, len(raw["cases"]), flush=True)


def metrics(order, labels):
    relevant = set(labels)
    dcg = sum(
        (2 ** labels.get(doc, 0) - 1) / math.log2(i + 2)
        for i, doc in enumerate(order[:10])
    )
    ideal = sum(
        (2**rel - 1) / math.log2(i + 2)
        for i, rel in enumerate(sorted(labels.values(), reverse=True)[:10])
    )
    return {
        "ndcg10": dcg / ideal,
        "recall10": len(set(order[:10]) & relevant) / len(relevant),
        "recall100": len(set(order[:100]) & relevant) / len(relevant),
        "mrr10": next(
            (1 / (i + 1) for i, doc in enumerate(order[:10]) if doc in relevant), 0
        ),
    }


def evaluate(args):
    import numpy as np

    output = args.output_dir.resolve()
    dataset = json.loads((output / "dataset.json").read_text())
    manifest = json.loads((output / "cuda-manifest.json").read_text())
    expected_receipts = {
        p.name.replace(".suite.json", ".cuda.json")
        for p in output.glob("*-*.suite.json")
    }
    if set(manifest["chunks"]) != expected_receipts:
        raise ValueError("CUDA manifest coverage differs")
    expected_queries = {
        row["name"]
        for row in json.loads((output / "queries.json").read_text())["cases"]
    }
    expected_docs = {
        row["name"]
        for row in json.loads((output / "documents.json").read_text())["cases"]
    }
    cuda_vectors = {}
    identities = set()
    for receipt in sorted(output.glob("*.cuda.json")):
        if (
            receipt.name not in manifest["chunks"]
            or sha256(receipt) != manifest["chunks"][receipt.name]["sha256"]
        ):
            raise ValueError("CUDA receipt digest differs")
        raw = json.loads(receipt.read_text())
        suite_path = receipt.with_name(
            receipt.name.replace(".cuda.json", ".suite.json")
        )
        if raw["backend"] != "cuda" or raw["suite_sha256"] != sha256(suite_path):
            raise ValueError("CUDA receipt provenance differs")
        expected_cases = json.loads(suite_path.read_text())["cases"]
        if [c["name"] for c in raw["cases"]] != [c["name"] for c in expected_cases]:
            raise ValueError("CUDA case coverage differs")
        identities.add(raw["model_identity"])
        for case in raw["cases"]:
            if case["name"] in cuda_vectors:
                raise ValueError("duplicate CUDA case")
            cuda_vectors[case["name"]] = case["vector"]
    if len(identities) != 1:
        raise ValueError("mixed CUDA identities")
    candidate = json.loads(args.cpu_receipt.read_text())
    if candidate.get("status") != "pass":
        raise ValueError("CPU serving qualification did not pass")
    cpu_vectors = {name: row["vector"] for name, row in candidate["cases"].items()}
    if (
        set(cuda_vectors) != expected_queries | expected_docs
        or set(cpu_vectors) != expected_queries
    ):
        raise ValueError("exact corpus and held-out query coverage required")
    if manifest["model_identity"] not in identities:
        raise ValueError("CUDA manifest identity differs")
    doc_ids = sorted(name[2:] for name in cuda_vectors if name.startswith("d:"))
    query_ids = sorted(dataset["qrels"])
    if len(doc_ids) != 5183 or any(
        "q:" + q not in cpu_vectors or "q:" + q not in cuda_vectors for q in query_ids
    ):
        raise ValueError("full candidate/corpus coverage required")
    matrix = np.array([cuda_vectors["d:" + doc] for doc in doc_ids], dtype=np.float64)
    if (
        matrix.shape != (5183, 768)
        or not np.isfinite(matrix).all()
        or np.max(np.abs(np.linalg.norm(matrix, axis=1) - 1)) > 1e-4
    ):
        raise ValueError("invalid CUDA document vectors")
    matrix /= np.linalg.norm(matrix, axis=1, keepdims=True)
    report = {
        "version": 1,
        "scope": "all 300 SciFact test queries against all 5183 fixed CUDA documents",
        "dataset_sha256": sha256(output / "dataset.json"),
        "cpu_receipt_sha256": sha256(args.cpu_receipt),
        "cuda_model_identity": identities.pop(),
        "cpu_model_identity": candidate["model_identity"],
        "policy": dataset["policy"],
        "queries": {},
        "metrics": {},
    }
    all_metrics = {role: [] for role in ("cpu", "cuda")}
    for qid in query_ids:
        scores = {}
        for role, vectors in (("cpu", cpu_vectors), ("cuda", cuda_vectors)):
            q = np.array(vectors["q:" + qid], dtype=np.float64)
            if (
                q.shape != (768,)
                or not np.isfinite(q).all()
                or abs(np.linalg.norm(q) - 1) > 1e-4
            ):
                raise ValueError("invalid query vector")
            values = matrix @ (q / np.linalg.norm(q))
            order = np.argsort(-values, kind="stable")
            ranking = [doc_ids[i] for i in order]
            scores[role] = ranking
            all_metrics[role].append(metrics(ranking, dataset["qrels"][qid]))
        report["queries"][qid] = {
            "cpu_top10": scores["cpu"][:10],
            "cuda_top10": scores["cuda"][:10],
            "cpu": all_metrics["cpu"][-1],
            "cuda": all_metrics["cuda"][-1],
        }
    for role, rows in all_metrics.items():
        report["metrics"][role] = {
            key: sum(r[key] for r in rows) / len(rows) for key in rows[0]
        }
    losses = {
        key: report["metrics"]["cuda"][key] - report["metrics"]["cpu"][key]
        for key in report["metrics"]["cpu"]
    }
    report["losses"] = losses
    report["status"] = (
        "pass"
        if losses["ndcg10"] <= MAX_NDCG10_LOSS
        and losses["recall10"] <= MAX_RECALL10_LOSS
        else "fail"
    )
    (output / "retrieval.json").write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {"status": report["status"], "metrics": report["metrics"], "losses": losses}
        ),
        flush=True,
    )
    if report["status"] != "pass":
        raise ValueError("cross-backend retrieval quality gate failed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("prepare", "cuda", "evaluate"))
    parser.add_argument(
        "--archive",
        type=Path,
        default=Path.home() / ".cache/antfly/conformance-fixtures/beir/scifact.zip",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--model", type=Path)
    parser.add_argument("--cpu-receipt", type=Path)
    args = parser.parse_args()
    if args.operation == "prepare":
        prepare(args)
    elif args.operation == "cuda":
        cuda(args)
    else:
        evaluate(args)


if __name__ == "__main__":
    main()
