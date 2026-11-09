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

import pytest

from vllm_quality_reference import compare_results


def row(case_id="a", tokens=(42, 151645), answer="answer", repeat=0):
    return dict(case_id=case_id, token_ids=list(tokens), answer=answer, repeat=repeat)


def test_terminal_token_convention_is_reported_separately():
    actual = [row(tokens=(42,))]
    reference = dict(results=[row(), row(repeat=1, tokens=(99,))])
    comparison = compare_results(actual, reference)
    assert comparison["raw_token_ids_equal"] == 0
    assert comparison["content_token_ids_equal"] == 1
    assert comparison["answer_equal"] == 1
    assert comparison["differences"] == []


def test_actual_content_difference_is_retained_even_when_text_is_equal():
    comparison = compare_results([row(tokens=(43,))], dict(results=[row()]))
    assert comparison["answer_equal"] == 1
    assert comparison["content_token_ids_equal"] == 0
    assert comparison["differences"][0]["vllm_token_ids"] == [43]
    assert comparison["differences"][0]["reference_token_ids"] == [42, 151645]


@pytest.mark.parametrize(
    "actual,reference",
    [([row()], []), ([row(), row()], [row()]), ([row()], [row(), row()])],
)
def test_missing_or_duplicate_cases_cannot_claim_parity(actual, reference):
    with pytest.raises(ValueError, match="case IDs differ"):
        compare_results(actual, dict(results=reference))
