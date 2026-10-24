# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import json

import pytest

from antfly.provenance import resource_reference, trace_event


def test_trace_rows_keep_exact_resource_versions_and_distinct_link_meanings():
    ref = resource_reference(table="business", table_id=7, id="approved", revision=9007199254740993)
    row = trace_event(
        trace_id="trace",
        span_id="span",
        name="deployment",
        started_at=1,
        ended_at=2,
        status="succeeded",
        links=[dict(relation="retrieved", target=ref), dict(relation="used", target=ref)],
    )
    decoded = json.loads(json.dumps(row))
    assert decoded["schema_version"] == 1
    assert [link["relation"] for link in decoded["provenance"]["links"]] == ["retrieved", "used"]
    assert decoded["provenance"]["links"][0]["target"]["revision"] == 9007199254740993
    assert "memory" not in decoded


@pytest.mark.parametrize("revision", [0, -1, True, 1.5, 2**64])
def test_resource_helpers_reject_lossy_or_invalid_identity(revision):
    with pytest.raises(ValueError):
        resource_reference(table="business", table_id=7, id="approved", revision=revision)
