import copy
import hashlib
import json
import unittest

from native_quality_score import evaluate, same_json, score, strict_json
from prepare_native_quality_suite import build_cases, task


def response_for(case):
    expected = case["expected"]
    if expected["kind"] == "tool_call":
        return {
            "choices": [
                {
                    "finish_reason": "tool_calls",
                    "message": {
                        "content": None,
                        "tool_calls": [
                            {
                                "id": "call-fixture",
                                "type": "function",
                                "function": {
                                    "name": expected["name"],
                                    "arguments": json.dumps(expected["arguments"]),
                                },
                            }
                        ],
                    },
                }
            ]
        }
    content = (
        expected["value"]
        if expected["kind"] == "text"
        else json.dumps(expected["value"])
    )
    return {"choices": [{"finish_reason": "stop", "message": {"content": content}}]}


class QualityTests(unittest.TestCase):
    def setUp(self):
        self.cases = build_cases()
        self.requests = [
            {"fixture": c["case_id"], "body": c["body"], "response": response_for(c)}
            for c in self.cases
        ]

    def test_fixed_suite_covers_families_positions_and_critical_cases(self):
        encoded = "".join(
            json.dumps(c, separators=(",", ":")) + "\n" for c in self.cases
        )
        self.assertEqual(
            hashlib.sha256(encoded.encode()).hexdigest(),
            "11b587964826b3e84a90881363b845964e5d857294ec148d2bf799a12dd8a14e",
        )
        self.assertEqual(len({c["case_id"] for c in self.cases}), 64)
        self.assertEqual(sum(c["critical"] for c in self.cases), 16)
        for category in ("fact", "instruction", "code", "tool_call"):
            cases = [c for c in self.cases if c["category"] == category]
            self.assertEqual(len(cases), 16)
            for family in range(4):
                self.assertEqual(
                    {
                        c["data_position_fraction"]
                        for c in cases
                        if c["task_family"] == family
                    },
                    {0.10, 0.25, 0.45, 0.60},
                )
        for case in self.cases:
            prefix = case["checkpoint_body"]
            self.assertEqual(case["body"]["messages"][:-2], prefix["messages"])
            self.assertNotIn(
                case["body"]["messages"][-1]["content"],
                prefix["messages"][-1]["content"],
            )
            self.assertEqual(case["body"].get("tools"), prefix.get("tools"))
            self.assertNotIn("tool_calls", case["body"]["messages"][-2])

    def test_complete_and_count_gate(self):
        self.assertTrue(evaluate(self.cases, self.requests, self.requests)["passed"])
        self.assertFalse(
            evaluate(self.cases[:4], self.requests[:4], self.requests[:4])["passed"]
        )

    def test_category_drop_and_critical_are_independent(self):
        changed = copy.deepcopy(self.requests)
        # One noncritical fact failure is below the aggregate drop limit but
        # exceeds the category limit; neither gate may mask the other.
        index = next(
            i for i, c in enumerate(self.cases) if c["case_id"] == "v2-fact-04"
        )
        changed[index]["response"]["choices"][0]["message"]["content"] = "wrong"
        report = evaluate(self.cases, self.requests, changed)
        self.assertLess(report["overall"]["drop_percentage_points"], 2)
        self.assertEqual(report["categories"]["fact"]["drop_percentage_points"], 6.25)
        self.assertFalse(report["passed"])
        self.assertEqual(report["regression_case_ids"], ["v2-fact-04"])
        self.assertEqual(report["baseline_failure_case_ids"], [])
        changed = copy.deepcopy(self.requests)
        changed[0]["response"]["choices"][0]["message"]["content"] = "wrong"
        self.assertFalse(evaluate(self.cases, changed, changed)["critical_passed"])

    def test_body_and_order_cannot_change(self):
        changed = copy.deepcopy(self.requests)
        changed[0]["body"]["max_tokens"] = 999
        with self.assertRaises(ValueError):
            evaluate(self.cases, self.requests, changed)
        with self.assertRaises(ValueError):
            evaluate(self.cases, self.requests, self.requests[::-1])

    def test_legacy_tool_evidence_remains_readable_and_strict(self):
        case = {"category": "tool_call", "expected_code": "violet-729"}
        good = {
            "choices": [
                {
                    "finish_reason": "tool_calls",
                    "message": {
                        "content": None,
                        "tool_calls": [
                            {
                                "id": "call-test",
                                "type": "function",
                                "function": {
                                    "name": "lookup_access_record",
                                    "arguments": '{"access_code":"violet-729"}',
                                },
                            }
                        ],
                    },
                }
            ]
        }
        self.assertTrue(score(case, good))
        for mutation in ("duplicate", "wrong_function", "extra_key", "text"):
            bad = copy.deepcopy(good)
            message = bad["choices"][0]["message"]
            fn = message["tool_calls"][0]["function"]
            if mutation == "duplicate":
                fn["arguments"] = '{"access_code":"wrong","access_code":"violet-729"}'
            elif mutation == "wrong_function":
                fn["name"] = "wrong"
            elif mutation == "extra_key":
                fn["arguments"] = '{"access_code":"violet-729","extra":true}'
            else:
                message["content"] = "violet-729"
            self.assertFalse(score(case, bad))

    def test_oracle_responses_and_critical_failure(self):
        rows = [
            {"fixture": c["case_id"], "body": c["body"], "response": response_for(c)}
            for c in self.cases
        ]
        self.assertTrue(evaluate(self.cases, rows, rows)["passed"])
        altered = copy.deepcopy(rows)
        altered[0]["response"]["choices"][0]["finish_reason"] = "length"
        self.assertFalse(evaluate(self.cases, rows, altered)["passed"])

    def test_code_answer_keys_agree_with_python_execution(self):
        # Execute only snippets authored by this deterministic fixture generator,
        # never model output or a user-supplied suite.
        for index in range(16):
            _, data, _, expected, _ = task("code", index)
            namespace = {}
            exec(
                compile(
                    data.removeprefix("Python:\n"), f"<code-fixture-{index}>", "exec"
                ),
                namespace,
            )
            actual = namespace["result"]
            if expected["kind"] == "json":
                self.assertTrue(same_json(actual, expected["value"]), index)
            else:
                self.assertEqual(str(actual), expected["value"], index)

    def test_json_rejects_duplicate_keys_nonfinite_values_and_boolean_integer_aliases(
        self,
    ):
        case = {"expected": {"kind": "json", "value": {"count": 1}}}
        response = {
            "choices": [
                {"finish_reason": "stop", "message": {"content": '{"count": 1}'}}
            ]
        }
        self.assertTrue(score(case, response))
        response["choices"][0]["message"]["content"] = '{"count": 1.0}'
        self.assertTrue(score(case, response))
        for content in (
            '{"count":true}',
            '{"count":0,"count":1}',
            '{"count":NaN}',
            '{"count":Infinity}',
            '{"count":1e309}',
            '{"count":1,"extra":0}',
            '```json\n{"count":1}\n```',
        ):
            response["choices"][0]["message"]["content"] = content
            self.assertFalse(score(case, response), content)
        for content in ("NaN", "Infinity", "-Infinity", "1e309", "-1e309"):
            with self.assertRaises(ValueError):
                strict_json(content)

    def test_nested_tool_arguments_and_arrays_are_strict(self):
        for case in (c for c in self.cases[:16] if c["category"] == "tool_call"):
            good = response_for(case)
            self.assertTrue(score(case, good))
            bad = copy.deepcopy(good)
            fn = bad["choices"][0]["message"]["tool_calls"][0]["function"]
            args = json.loads(fn["arguments"])
            args["extra"] = True
            fn["arguments"] = json.dumps(args)
            self.assertFalse(score(case, bad))
            bad = copy.deepcopy(good)
            bad["choices"][0]["message"]["tool_calls"].append(
                copy.deepcopy(good["choices"][0]["message"]["tool_calls"][0])
            )
            self.assertFalse(score(case, bad))
        nested = self.cases[15]
        bad = response_for(nested)
        fn = bad["choices"][0]["message"]["tool_calls"][0]["function"]
        args = json.loads(fn["arguments"])
        args["options"]["dry_run"] = 1
        fn["arguments"] = json.dumps(args)
        self.assertFalse(score(nested, bad))


if __name__ == "__main__":
    unittest.main()
