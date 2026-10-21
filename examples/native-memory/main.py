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

"""Exercise the shared model-free native memory policy over HTTP or embedded."""

from __future__ import annotations
import argparse
import os
import json


def main() -> None:
    parser = argparse.ArgumentParser()
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--url")
    source.add_argument("--database")
    parser.add_argument("--space", default="demo")
    args = parser.parse_args()
    config = {
        "features": {"reconciliation": "deterministic"},
        "predicates": [{"name": "deploys_with", "cardinality": "one"}],
    }
    if args.database:
        import antfly_embedded
        from pathlib import Path

        client = (
            antfly_embedded.open(args.database)
            if Path(args.database).exists()
            else antfly_embedded.create(args.database)
        )

        def call(action: str, body: dict, key: str = "", *, id: str = ""):
            return client.memory(
                args.space,
                {"action": action, "body": body, "idempotency_key": key, "id": id},
            )

        call("create", config, "create")
    else:
        from antfly import AntflyClient

        client = AntflyClient(args.url, token=os.environ.get("ANTFLY_TOKEN"))
        client.memory.create(args.space, config=config, idempotency_key="create")

        def call(action: str, body: dict, key: str = "", *, id: str = ""):
            return client.memory.execute(
                args.space, action, body=body, idempotency_key=key, id=id
            )

    try:
        # Explicit external assertions preserve source provenance; no extraction is implied.
        for name, start in (("Terraform", 300), ("Helm", 100)):
            evidence_id = "guidance-" + name.lower()
            call(
                "observe",
                {
                    "evidence": [
                        {
                            "id": evidence_id,
                            "source_id": "deployment-runbook",
                            "source_version": str(start),
                            "text": "Production uses " + name,
                        }
                    ],
                    "assertions": [
                        {
                            "id": name.lower(),
                            "subject": "production",
                            "predicate": "deploys_with",
                            "value": name,
                            "statement": "Production uses " + name,
                            "valid_from": start,
                            "evidence": [{"id": evidence_id}],
                        }
                    ],
                },
                evidence_id,
            )
        current = call("recall", {"subject": "production", "token_budget": 8192})
        historical = call(
            "recall", {"subject": "production", "valid_at": 200, "token_budget": 8192}
        )
        assert [hit["memory"]["assertion"]["value"] for hit in current["items"]] == [
            "Terraform"
        ]
        assert [hit["memory"]["assertion"]["value"] for hit in historical["items"]] == [
            "Helm"
        ]
        explanation = call("explain", {}, id="terraform")
        print(
            json.dumps(
                {
                    "current": current,
                    "historical": historical,
                    "explanation": explanation,
                },
                indent=2,
            )
        )
    finally:
        if args.database:
            client.close()


if __name__ == "__main__":
    main()
