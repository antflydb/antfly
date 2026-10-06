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

"""Guard immutable qualification evidence independently of source header checks."""

from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import qualification_provenance as provenance


class QualificationProvenanceTests(unittest.TestCase):
    def test_recorded_gliner_contracts_remain_valid(self):
        self.assertEqual(provenance.check_frozen_helpers(), [])
        here = provenance.ROOT / "zig/pkg/inference/scripts/gliner25"
        with patch.object(sys, "path", [str(here), *sys.path]):
            import check_trained_execution
            import check_training_merge
            import oracle

            oracle.verify_reference_fixtures()
            check_training_merge.load_contract()
            check_trained_execution.load_contract()

    def test_changes_to_frozen_bytes_fail_without_rewriting_pins(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "zig/pkg/inference/helper.py"
            source.parent.mkdir(parents=True)
            data = b"print('original')\n"
            source.write_bytes(data)
            pin = {"size_bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"generators": {"helper.py": pin}}))
            registry = root / "registry.json"
            registry.write_text(
                json.dumps(
                    {
                        "manifest.json": {
                            "base": "zig/pkg/inference",
                            "key": "generators",
                        }
                    }
                )
            )
            (root / "LICENSES").mkdir()
            (root / "LICENSES/Apache-2.0.txt").write_text("Apache license fixture\n")
            (root / "zig/pkg/inference/LICENSE").write_text("Apache license fixture\n")
            with patch.object(provenance, "REGISTRY", registry):
                self.assertEqual(provenance.check_frozen_helpers(root), [])
                source.write_bytes(b"# new header\n" + data)
                self.assertTrue(provenance.check_frozen_helpers(root))
                self.assertEqual(
                    json.loads(manifest.read_text())["generators"]["helper.py"], pin
                )
                source.write_bytes(data)
                (root / "zig/pkg/inference/LICENSE").write_text("different license\n")
                self.assertTrue(provenance.check_frozen_helpers(root))

    def test_manifest_cannot_escape_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry = root / "registry.json"
            registry.write_text(
                json.dumps({"../outside.json": {"base": ".", "key": "files"}})
            )
            with patch.object(provenance, "REGISTRY", registry):
                self.assertTrue(
                    any(
                        "escapes repository" in error
                        for error in provenance.check_frozen_helpers(root)
                    )
                )


if __name__ == "__main__":
    unittest.main()
