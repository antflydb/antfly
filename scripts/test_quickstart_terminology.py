#!/usr/bin/env python3
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

"""Release-readiness checks for the public quickstart."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import test_quickstart_docs as docs


class TerminologyTests(unittest.TestCase):
    def test_history_and_code_keep_original_names(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pages = root / "docs"
            history = pages / "design/history"
            history.mkdir(parents=True)
            (history / "notes.md").write_text("Termite was the old name.\n")
            page = pages / "live.md"
            page.write_text(
                "Use Antfly Inference.\n```go\ntype StandaloneTermiteSpec struct {}\n```\n"
                "The old field is `TermiteReady`.\n~~~text\nTermite\n~~~\n"
            )
            with (
                patch.object(docs, "REPO", root),
                patch.object(docs, "DOCS_DIR", pages),
            ):
                docs.check_terminology()
                page.write_text(page.read_text() + "Use Termite.\n")
                with self.assertRaisesRegex(AssertionError, "live.md:9.*Termite"):
                    docs.check_terminology()

    def test_live_history_named_page_is_still_checked(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pages = root / "docs"
            pages.mkdir()
            (pages / "history.md").write_text("Use Antfly inference.\n")
            with (
                patch.object(docs, "REPO", root),
                patch.object(docs, "DOCS_DIR", pages),
            ):
                with self.assertRaisesRegex(AssertionError, "history.md:1.*capital I"):
                    docs.check_terminology()
