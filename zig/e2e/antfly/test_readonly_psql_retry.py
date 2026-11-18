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

"""Capacity retries may replay SELECT scripts, never uncertain outcomes."""

import subprocess

import helpers
import pytest


def test_readonly_psql_discards_partial_output_when_capacity_recovers(monkeypatch):
    results = iter(
        [
            subprocess.CompletedProcess(
                [], 3, "partial\n", 'ERROR: 53300: capacity\nDETAIL: {"retryable":true}'
            ),
            subprocess.CompletedProcess([], 0, "complete\n", ""),
        ]
    )
    calls = []

    def run(command, **kwargs):
        calls.append(kwargs)
        return next(results)

    monkeypatch.setattr(helpers.subprocess, "run", run)
    monkeypatch.setattr(helpers.time, "sleep", lambda _: None)
    result = helpers.run_readonly_psql_with_retry(["psql"], input="SELECT 1; SELECT 2;")
    assert result.stdout == "complete\n"
    assert len(calls) == 2
    assert all(call["input"] == "SELECT 1; SELECT 2;" for call in calls)


@pytest.mark.parametrize(
    "diagnostic",
    [
        'ERROR: 40003: outcome unknown\nDETAIL: {"retryable":true}',
        'ERROR: 55000: declaration committed\nDETAIL: {"retryable":true}',
        'ERROR: 53300: capacity\nDETAIL: {"retryable":false}',
        "ERROR: 53300: capacity",
        'ERROR: 42601: syntax error\nDETAIL: {"retryable":true}',
    ],
)
def test_readonly_psql_preserves_non_capacity_failures(monkeypatch, diagnostic):
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 3, "", diagnostic)

    monkeypatch.setattr(helpers.subprocess, "run", run)
    result = helpers.run_readonly_psql_with_retry(["psql"], input="SELECT 1;")
    assert result.returncode == 3
    assert len(calls) == 1


def test_readonly_psql_capacity_retries_have_one_deadline(monkeypatch):
    now = [0.0]
    calls = []

    def run(command, **kwargs):
        calls.append(kwargs["timeout"])
        return subprocess.CompletedProcess(
            command, 3, "", 'ERROR: 53300: capacity\nDETAIL: {"retryable":true}'
        )

    monkeypatch.setattr(helpers.subprocess, "run", run)
    monkeypatch.setattr(helpers.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(
        helpers.time, "sleep", lambda delay: now.__setitem__(0, now[0] + delay)
    )
    result = helpers.run_readonly_psql_with_retry(
        ["psql"], input="SELECT 1;", timeout=0.1
    )
    assert result.returncode == 3
    assert len(calls) == 2
    assert 0 < calls[1] < calls[0]
