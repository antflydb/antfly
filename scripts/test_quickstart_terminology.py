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


class RecordingWaitTests(unittest.TestCase):
    tape = (
        "Type@75ms `antfly index wait -t wikipedia -i title_body "
        "--until source-covered=10% --timeout 45m`\n"
        "Enter\n# Follow the cursor after scrolling output.\nWait@2700s /❯/\n"
        "Type `antfly query -t wikipedia`\nEnter\n"
    )

    def test_cursor_prompt_wait_accepts_short_and_long_flags(self):
        docs.check_recording_coverage_wait(self.tape)
        docs.check_recording_coverage_wait(
            self.tape.replace("-t wikipedia", "--table wikipedia")
            .replace("-i title_body", "--index title_body")
            .replace("Wait@", "Wait+Line@")
        )

    def test_continued_recording_command_and_presentation_directives(self):
        continued = self.tape.replace(
            "--until source-covered=10%",
            "\\`\nEnter\nType `  --until source-covered=10%",
        ).replace("Wait@2700s", "Sleep 2s\nHide\nWait@2700s")
        docs.check_recording_coverage_wait(continued)
        docs.check_recording_coverage_wait(docs.QUICKSTART_TAPE.read_text())
        with self.assertRaises(AssertionError):
            docs.check_recording_coverage_wait(
                continued.replace("--timeout 45m", "--timeout 1s")
            )
        with self.assertRaises(AssertionError):
            docs.check_recording_coverage_wait(
                continued.replace("Enter\nType `  --until", "Type `  --until")
            )

    def test_unrelated_timeout_or_prompt_cannot_satisfy_wait(self):
        for old, new in (
            ("--timeout 45m", "--timeout 1s"),
            ("source-covered=10%", "searchable-artifacts=1"),
            ("Wait@2700s /❯/", "Wait+Screen@2700s /❯/"),
            ("Wait@2700s /❯/", "Sleep 1s"),
        ):
            with self.subTest(old=old), self.assertRaises(AssertionError):
                docs.check_recording_coverage_wait(
                    self.tape.replace(old, new)
                    + "Type `echo --timeout 45m source-covered=10%`\nEnter\nWait@2700s /❯/\n"
                )


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
                self.assertRaisesRegex(AssertionError, "history.md:1.*capital I"),
            ):
                docs.check_terminology()
