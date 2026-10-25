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

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import qualify_attention_match_queries as qualification


class ProcessGroupShutdownTests(unittest.TestCase):
    @staticmethod
    def process(root, pid, group, leader_state, tasks):
        directory = root / str(pid)
        directory.mkdir()
        (directory / "stat").write_text(
            f"{pid} (worker with spaces) {leader_state} 1 {group} 0\n"
        )
        for tid, state in tasks.items():
            task = directory / "task" / str(tid)
            task.mkdir(parents=True)
            if state is not None:
                (task / "stat").write_text(f"{tid} (worker task) {state} 1 {group} 0\n")
        return directory

    def test_zombie_leader_with_live_shutdown_thread_remains_active(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.process(root, 11, 7, "Z", {11: "Z", 12: "D"})
            with patch.object(qualification, "Path", return_value=root):
                self.assertEqual(qualification.active_group_members(7), [11])

    def test_only_the_requested_group_and_live_tasks_count(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.process(root, 11, 7, "S", {11: "S"})
            self.process(root, 12, 8, "S", {12: "S"})
            self.process(root, 13, 7, "Z", {13: "Z", 14: "X"})
            with patch.object(qualification, "Path", return_value=root):
                self.assertEqual(qualification.active_group_members(7), [11])

    def test_disappearing_task_does_not_prevent_completed_shutdown(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.process(root, 11, 7, "Z", {11: "Z", 12: None})
            with patch.object(qualification, "Path", return_value=root):
                self.assertEqual(qualification.active_group_members(7), [])


if __name__ == "__main__":
    unittest.main()
