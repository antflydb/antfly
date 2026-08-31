# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

from antfly.graph_identifier_policy_generated import (
    GRAPH_IDENTIFIER_CONFORMANCE_CASES,
    GRAPH_IDENTIFIER_POLICY_VERSION,
    GRAPH_IDENTIFIER_UNICODE_VERSION,
    is_valid_graph_identifier,
)


def test_graph_identifier_policy_matches_versioned_conformance_cases() -> None:
    assert GRAPH_IDENTIFIER_POLICY_VERSION == 1
    assert GRAPH_IDENTIFIER_UNICODE_VERSION == "15.0.0"
    for name, value, valid in GRAPH_IDENTIFIER_CONFORMANCE_CASES:
        assert is_valid_graph_identifier(value) is valid, name
