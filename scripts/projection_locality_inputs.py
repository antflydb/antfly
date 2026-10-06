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

"""Explicit measurement dependencies for the archived locality harness."""

import json
from pathlib import Path

MEASUREMENT_HELPERS = (
    "prepare_vdbbench_vector_source.py",
    "profile_vdbbench_public_query.py",
    "profile_vdbbench_mixed_public_workload.py",
    "validate_vdbbench_result.py",
    "summarize_vdbbench_qualification.py",
    "vector_store_disk_accounting.py",
)


def input_paths(binary, runner):
    scripts = runner.parent
    return {
        binary,
        runner,
        Path(__file__).resolve(),
        scripts / "run_vdbbench_qualification_snapshot_20260906.sh",
        *(scripts / name for name in MEASUREMENT_HELPERS),
    }


def receipt_passed(root, receipt):
    if receipt.get("exit_code") != 0:
        return False
    if not receipt.get("invalid_reason"):
        return True
    # Preserve the original invalidation. Only a separately recorded manual
    # dependency audit may accept an arm stopped by an unused-input change.
    audit = root / "unused-input-change-audit.json"
    if not audit.exists() or not receipt["invalid_reason"].startswith(
        "qualification input changed:"
    ):
        return False
    record = json.loads(audit.read_text())
    arm = Path(receipt["command"][1]).name
    return arm in record.get("accepted_arms", [])
