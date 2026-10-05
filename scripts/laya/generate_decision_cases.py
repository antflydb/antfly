#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
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

# /// script
# requires-python = ">=3.11"
# dependencies = ["mlx-lm>=0.32"]
# ///
"""Generate synthetic typed-decision cases with a local MLX model.

uv run --script scripts/laya/generate_decision_cases.py <model_dir> --cases 8000 \\
    --output synthetic-cases.jsonl

Each case is a structured JSON state from a business workflow (a support
thread, an agent log, an order, a claim, ...) plus three to five typed
questions about it (choice, score and yes/no, in typed-decisions' shape). The
cases carry no answers: a calibrated teacher labels them afterwards
(prepare_laya_longcontext_teacher.py, after build_decision_mix.py's record
conversion). typed-decisions' own four workflows (customer service, agent
trace observability, security incidents, invoice processing) are left out on
purpose, so its train split stays the only data from the benchmark's family.

Every prompt draws a workflow, a difficulty (clear-cut, borderline or
ambiguous) and a nudge toward a non-default outcome, so answers do not pile up
on the obvious option. Outputs that are not valid JSON in the expected shape
are dropped and regenerated.
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time
from pathlib import Path

WORKFLOWS = [
    (
        "ecommerce_orders",
        "an online store order with items, payment checks, shipping and customer notes",
    ),
    (
        "hr_requests",
        "an employee request to HR (leave, payroll, policy, complaint) with history",
    ),
    (
        "it_helpdesk",
        "an IT helpdesk ticket with device details, error messages and the thread",
    ),
    (
        "code_review",
        "a pull request with the diff summary, CI results and review comments",
    ),
    (
        "forum_moderation",
        "a user post on a community forum with the author's history and reports",
    ),
    (
        "insurance_claims",
        "an insurance claim with policy details, incident description and documents",
    ),
    (
        "loan_applications",
        "a small-business loan application with financials and notes",
    ),
    (
        "candidate_screening",
        "a job application with the role, resume summary and screening notes",
    ),
    (
        "shipment_exceptions",
        "a logistics shipment with tracking events and an exception",
    ),
    (
        "clinic_scheduling",
        "a patient's appointment request to a clinic front desk (administrative, not diagnostic)",
    ),
    (
        "sales_leads",
        "an inbound sales lead with company data, activity and the message",
    ),
    (
        "travel_changes",
        "a traveler's request to change or cancel a booking, with fare rules",
    ),
    (
        "subscription_cancellation",
        "a subscriber's cancellation request with usage and billing history",
    ),
    ("app_reviews", "an app store review with rating, version and device"),
    ("bug_reports", "a bug report with reproduction steps, environment and logs"),
    (
        "social_mentions",
        "a social media post mentioning a brand, with reach and context",
    ),
    (
        "contract_review",
        "a contract clause with the negotiating parties' redlines and notes",
    ),
    (
        "procurement_requests",
        "an internal purchase request with vendor quotes and budget",
    ),
    (
        "expense_reports",
        "an employee expense report with line items, receipts and policy limits",
    ),
    (
        "campaign_qa",
        "a marketing email draft with audience, links and compliance checks",
    ),
    (
        "cloud_cost_alerts",
        "a cloud cost anomaly alert with service usage and recent deploys",
    ),
    (
        "ci_failures",
        "a CI pipeline failure with job logs, flaky-test history and recent commits",
    ),
    (
        "data_quality_alerts",
        "a data pipeline quality alert with table stats and schema changes",
    ),
    (
        "warranty_claims",
        "a product warranty claim with purchase date, photos description and history",
    ),
    (
        "kyc_review",
        "a customer identity verification (KYC) check with document results and risk signals",
    ),
    (
        "restaurant_bookings",
        "a restaurant reservation request with party details and availability",
    ),
    (
        "tenant_requests",
        "a tenant maintenance request to a property manager with photos description",
    ),
    (
        "survey_feedback",
        "a free-text customer survey response with scores and account data",
    ),
    (
        "content_licensing",
        "a request to license an image or text with usage terms and rights data",
    ),
    (
        "fleet_telemetry",
        "a delivery vehicle telemetry alert with sensor readings and route",
    ),
]

DIFFICULTY = ["clear-cut", "borderline", "ambiguous, with conflicting signals"]

SCHEMA = """Return only a JSON object, no prose, in exactly this shape:
{
  "state": { ... realistic nested JSON for the case: ids, numbers, dates, short texts, threads or logs ... },
  "questions": {
    "<snake_case_name>": {"type": "choice", "instructions": "<question>", "criteria": {"<snake_case_option>": "<what this option means>", ...}},
    "<snake_case_name>": {"type": "score", "instructions": "<question>", "criteria": ["<lowest level description>", "...", "<highest level description>"]},
    "<snake_case_name>": {"type": "noul", "instructions": "<a statement that is true or false about the case>"}
  }
}
Rules:
- 3 to 5 questions, at least one of each type.
- choice: 3 to 6 mutually exclusive options. score: 3 to 5 ordered levels.
- noul: phrase the instruction as a statement to judge true or false.
- Questions must be answerable from the state alone; do not state the answers anywhere.
- The state is 80 to 350 words of JSON. Use realistic but fictional names and numbers."""


def prompt_for(rng: random.Random) -> tuple[str, str]:
    workflow, description = rng.choice(WORKFLOWS)
    difficulty = rng.choice(DIFFICULTY)
    twist = rng.choice(
        [
            "",
            "Make the right answer to the first question something other than the most common or default option.",
            "Include one misleading detail that a careless reader would over-weight.",
            "Make the situation routine and low-stakes.",
            "Make the situation urgent or high-stakes.",
        ]
    )
    text = (
        f"Write one synthetic case for an automated decision system that handles {description}.\n"
        f"The case should be {difficulty}. {twist}\n\n{SCHEMA}"
    )
    return workflow, text


def valid(case: object) -> bool:
    if (
        not isinstance(case, dict)
        or not isinstance(case.get("state"), dict)
        or not case["state"]
    ):
        return False
    questions = case.get("questions")
    if not isinstance(questions, dict) or not 3 <= len(questions) <= 5:
        return False
    kinds = set()
    for name, q in questions.items():
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,40}", name) or not isinstance(q, dict):
            return False
        kind, instructions, criteria = (
            q.get("type"),
            q.get("instructions"),
            q.get("criteria"),
        )
        if not isinstance(instructions, str) or not instructions.strip():
            return False
        if kind == "choice":
            if not isinstance(criteria, dict) or not 3 <= len(criteria) <= 6:
                return False
            if not all(
                re.fullmatch(r"[a-z][a-z0-9_]{0,40}", k) and isinstance(v, str)
                for k, v in criteria.items()
            ):
                return False
        elif kind == "score":
            if (
                not isinstance(criteria, list)
                or not 3 <= len(criteria) <= 5
                or not all(isinstance(c, str) and c for c in criteria)
            ):
                return False
        elif kind == "noul":
            if criteria is not None and not isinstance(criteria, dict):
                return False
        else:
            return False
        kinds.add(kind)
    return kinds == {"choice", "score", "noul"}


def parse(text: str) -> object:
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        return json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("model", type=Path, help="local MLX model directory")
    parser.add_argument("--cases", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--batch", type=int, default=16)
    parser.add_argument("--max-tokens", type=int, default=1100)
    parser.add_argument("--temperature", type=float, default=0.8)
    parser.add_argument("--seed", type=int, default=20261005)
    args = parser.parse_args()

    from mlx_lm import batch_generate, load
    from mlx_lm.sample_utils import make_sampler

    model, tok = load(str(args.model))
    sampler = make_sampler(temp=args.temperature, top_p=0.95)
    rng = random.Random(args.seed)
    # Resume: count what is already written.
    done = (
        sum(1 for line in args.output.open() if line.strip())
        if args.output.exists()
        else 0
    )
    for _ in range(done * 2):
        rng.random()
    began, written, attempts = time.time(), done, 0
    with args.output.open("a") as out:
        while written < args.cases:
            batch = [prompt_for(rng) for _ in range(args.batch)]
            prompts = [
                tok.apply_chat_template(
                    [{"role": "user", "content": text}],
                    add_generation_prompt=True,
                    enable_thinking=False,
                )
                for _, text in batch
            ]
            response = batch_generate(
                model, tok, prompts, max_tokens=args.max_tokens, sampler=sampler
            )
            for (workflow, _), text in zip(batch, response.texts):
                attempts += 1
                case = parse(text)
                if not valid(case) or written >= args.cases:
                    continue
                out.write(
                    json.dumps(
                        {
                            "id": f"syn_{workflow}_{written:06d}",
                            "workflow": workflow,
                            "state": case["state"],
                            "questions": case["questions"],
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
                written += 1
            out.flush()
            rate = (written - done) / max(time.time() - began, 1e-9) * 3600
            print(
                f"{written}/{args.cases} cases, {attempts} generated ({(written - done) / max(attempts, 1):.0%} valid), {rate:.0f} cases/h",
                file=sys.stderr,
                flush=True,
            )


if __name__ == "__main__":
    main()
