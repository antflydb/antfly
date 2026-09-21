"""Exercise promotion admission after an intentionally skipped RC ancestor.

GitHub applies implicit success() to conditions without a status function,
including skips in the dependency chain. See:
https://docs.github.com/en/actions/how-tos/write-workflows/choose-what-workflows-do/use-jobs
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path


WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/antfly-release.yml"
JOBS = {
    "begin-release-channel": (
        "prepare-release-promotion", "preflight-release-channel",
        "preflight-publication", "stage-container",
    ),
    "publish-cli-npm": ("prepare-release-promotion", "begin-release-channel"),
    "publish-cli-pypi": ("prepare-release-promotion", "begin-release-channel"),
}


def condition(job: str) -> str:
    block = re.search(
        rf"^  {re.escape(job)}:\n(.*?)(?=^  [\w-]+:|\Z)",
        WORKFLOW.read_text(), re.MULTILINE | re.DOTALL,
    ).group(1)
    match = re.search(r"^    if: (.*?)(?=^    [\w-]+:)", block, re.MULTILINE | re.DOTALL)
    return re.search(r"\$\{\{(.*?)\}\}", match.group(1), re.DOTALL).group(1) if match else "success()"


def admitted(expression: str, results: dict[str, str], *, cancelled=False, publish="true", ancestor="skipped") -> bool:
    # Evaluate this workflow's boolean/equality subset, not arbitrary Actions expressions.
    # An explicit status function suppresses the default success() admission check.
    success = ancestor == "success" and all(r == "success" for r in results.values())
    if not re.search(r"\b(?:always|cancelled|success|failure)\(\)", expression):
        if not success:
            return False
    expression = re.sub(r"needs\.([\w-]+)\.result", lambda m: repr(results[m[1]]), expression)
    expression = re.sub(r"needs\.prepare-release-promotion\.outputs\.publish_(?:npm|pypi)", repr(publish), expression)
    expression = expression.replace("cancelled()", str(cancelled)).replace("success()", str(success)).replace("always()", "True")
    expression = expression.replace("&&", " and ").replace("||", " or ")
    expression = re.sub(r"!(?!=)", " not ", expression)
    return bool(eval(" ".join(expression.split()), {"__builtins__": {}}, {}))


class PromotionJobConditionsTests(unittest.TestCase):
    def test_rc_skip_and_stable_success_admit_ready_jobs(self):
        for job, dependencies in JOBS.items():
            for ancestor in ("skipped", "success"):
                with self.subTest(job=job, ancestor=ancestor):
                    self.assertTrue(admitted(condition(job), dict.fromkeys(dependencies, "success"), ancestor=ancestor))

    def test_each_required_gate_must_succeed(self):
        for job, dependencies in JOBS.items():
            for dependency in dependencies:
                for result in ("failure", "skipped", "cancelled"):
                    with self.subTest(job=job, dependency=dependency, result=result):
                        results = dict.fromkeys(dependencies, "success")
                        results[dependency] = result
                        self.assertFalse(admitted(condition(job), results))

    def test_cancelled_workflow_does_not_begin_or_publish(self):
        for job, dependencies in JOBS.items():
            with self.subTest(job=job):
                self.assertFalse(admitted(condition(job), dict.fromkeys(dependencies, "success"), cancelled=True))

    def test_package_policy_still_controls_publication(self):
        for job in ("publish-cli-npm", "publish-cli-pypi"):
            for policy in ("false", ""):
                with self.subTest(job=job, policy=policy):
                    self.assertFalse(admitted(condition(job), dict.fromkeys(JOBS[job], "success"), publish=policy))


if __name__ == "__main__":
    unittest.main()
