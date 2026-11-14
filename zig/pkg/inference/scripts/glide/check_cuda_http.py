#!/usr/bin/env python3
"""Validate the public GLiDE decisions API against captured FP32 answers.

Start a CUDA-required server separately. This client checks every fixture
request, then an unequal-length batch in two orders and a repeated request.
It records end-to-end HTTP latencies separately from pretokenized benchmarks.
"""

import argparse
import json
import math
import statistics
import time
import urllib.error
import urllib.request
from pathlib import Path


def public_question(name, question):
    result = dict(
        name=name, type=question["type"], instructions=question["instructions"]
    )
    criteria = question.get("criteria")
    if result["type"] == "noul":
        # Custom predicate descriptions are expressed as a two-option choice
        # in the public contract, preserving the exact upstream prompt.
        result["type"] = "choice" if criteria else "predicate"
    if result["type"] == "score":
        descriptions = (
            [criteria[key] for key in sorted(criteria, key=float)]
            if isinstance(criteria, dict)
            else criteria
        )
        result["levels"] = [
            dict(label=str(i), description=value)
            for i, value in enumerate(descriptions)
        ]
    elif isinstance(criteria, dict):
        result["choices"] = [
            dict(value=key, description=value) for key, value in criteria.items()
        ]
    return result


def input_text(state):
    return state if isinstance(state, str) else json.dumps(state, ensure_ascii=False)


def check_answers(actual, expected, tolerance):
    assert len(actual) == len(expected), (actual, expected)
    assert {answer["name"] for answer in actual} == set(expected), (actual, expected)
    maximum = 0.0
    for answer in actual:
        gold = expected[answer["name"]]
        assert answer["decision_method"] == "typed", answer
        if gold["type"] == "noul":
            if answer["type"] != "predicate":
                assert len(answer["probabilities"]) == 2, answer
                assert {p["value"] for p in answer["probabilities"]} == {
                    "true",
                    "false",
                }, answer
                assert all(
                    math.isfinite(p["probability"]) and 0 <= p["probability"] <= 1
                    for p in answer["probabilities"]
                ), answer
                assert (
                    abs(sum(p["probability"] for p in answer["probabilities"]) - 1.0)
                    <= 1e-5
                ), answer
            probability = (
                answer["probability"]
                if answer["type"] == "predicate"
                else next(
                    p["probability"]
                    for p in answer["probabilities"]
                    if p["value"] == "true"
                )
            )
            probabilities = [(probability, gold["noul"])]
            assert (probability >= 0.5) == (gold["noul"] >= 0.5), answer
        else:
            if gold["type"] == "choice":
                assert answer["choice"] == gold["choice"], (answer, gold)
            else:
                # The public API exposes expected level as score; upstream
                # names its argmax level score and expectation expected_level.
                assert abs(answer["score"] - gold["expected_level"]) <= tolerance * len(
                    gold["probabilities"]
                ), (answer, gold)
                assert (
                    max(answer["probabilities"], key=lambda p: p["probability"])[
                        "value"
                    ]
                    == gold["score"]
                ), (answer, gold)
            probabilities = [
                (p["probability"], gold["probabilities"][str(p["value"])])
                for p in answer["probabilities"]
            ]
            assert len(probabilities) == len(gold["probabilities"]), answer
            assert {str(p["value"]) for p in answer["probabilities"]} == set(
                gold["probabilities"]
            ), answer
            assert abs(sum(p[0] for p in probabilities) - 1.0) <= 1e-5, answer
        for value, reference in probabilities:
            assert math.isfinite(value) and 0 <= value <= 1, answer
            maximum = max(maximum, abs(value - reference))
    assert maximum <= tolerance, (maximum, tolerance)
    return maximum


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:18090/ai/v1")
    parser.add_argument("--model", required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--max-dprob", type=float, default=0.01)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--measure", type=int, default=5)
    args = parser.parse_args()
    if args.warmup < 1 or args.measure < 1:
        parser.error("warmup and measure must be positive")
    fixture = json.loads(args.fixture.read_text())
    assert fixture["requests"] and len(fixture["requests"]) == len(fixture["answers"])

    def run(name, body, expected, ids=None):
        payload = json.dumps(dict(model=args.model, **body)).encode()
        times = []
        maximum = 0.0
        for iteration in range(args.warmup + args.measure):
            started = time.perf_counter()
            request = urllib.request.Request(
                args.url + "/decisions",
                data=payload,
                headers={"Content-Type": "application/json"},
            )
            try:
                with urllib.request.urlopen(request, timeout=300) as response:
                    output = json.load(response)
            except urllib.error.HTTPError as error:
                raise RuntimeError(
                    f"HTTP {error.code}: {error.read().decode()}"
                ) from error
            elapsed = (time.perf_counter() - started) * 1000
            if iteration >= args.warmup:
                times.append(elapsed)
            if ids is None:
                maximum = max(
                    maximum,
                    check_answers(output["answers"], expected[0], args.max_dprob),
                )
            else:
                assert len(output["data"]) == len(ids), output
                for row, identity, gold in zip(output["data"], ids, expected):
                    assert row["id"] == identity, output
                    maximum = max(
                        maximum, check_answers(row["answers"], gold, args.max_dprob)
                    )
        print(
            json.dumps(
                dict(
                    cell=name,
                    ms=times,
                    median_ms=statistics.median(times),
                    max_dprob=maximum,
                    requests=len(expected),
                )
            ),
            flush=True,
        )

    for i, (request, expected) in enumerate(
        zip(fixture["requests"], fixture["answers"])
    ):
        run(
            f"request_{i}",
            dict(
                input=input_text(request["state"]),
                questions=[
                    public_question(name, question)
                    for name, question in request["questions"].items()
                ],
            ),
            [expected["answers"]],
        )
    # Different lengths, including a structured state, exercise row isolation
    # and restoration of caller order after length-sorted microbatching.
    question = public_question("intent", fixture["requests"][0]["questions"]["intent"])
    for order in ([0, 1, 2], [2, 0, 1], [1, 1, 0]):
        ids = [f"item-{j}" for j in range(len(order))]
        run(
            "batch_" + "_".join(map(str, order)),
            dict(
                questions=[question],
                inputs=[
                    dict(id=identity, input=input_text(fixture["requests"][i]["state"]))
                    for i, identity in zip(order, ids)
                ],
            ),
            [dict(intent=fixture["answers"][i]["answers"]["intent"]) for i in order],
            ids,
        )


if __name__ == "__main__":
    main()
