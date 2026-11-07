"""Constructed SDK defaults share the server cascade compiler's fixture."""

import json
from pathlib import Path

from antfly.client_generated.models import (
    DeciderCascade,
    DeciderCascadeStage,
    DeciderCascadeStageWhen,
    DeciderConfig,
    DeciderConfigDecisionMethod,
)


def test_constructed_cascade_defaults_match_server_fixture_and_round_trip():
    config = DeciderConfig(
        decision_method=DeciderConfigDecisionMethod.CASCADE,
        cascade=DeciderCascade(
            stages=[
                DeciderCascadeStage(name="fast", decider=DeciderConfig(model="fast")),
                DeciderCascadeStage(
                    name="strong",
                    decider=DeciderConfig(model="strong"),
                    when=DeciderCascadeStageWhen(probability_between=[0.3, 0.9]),
                ),
            ]
        ),
    )
    fixture = Path(__file__).resolve().parents[4] / "zig/lib/decisions/testdata/sdk_cascade_defaults.json"
    wire = config.to_dict()
    assert wire == json.loads(fixture.read_text())
    assert wire["cascade"]["stages"][0]["on_failure"] == "fail"
    assert "require_agreement" not in wire["cascade"]["stages"][0]
    assert DeciderConfig.from_dict(wire).to_dict() == wire
    wire["cascade"]["stages"][1]["require_agreement"] = False
    assert DeciderConfig.from_dict(wire).to_dict() == wire
    wire["cascade"]["stages"][1]["require_agreement"] = True
    assert DeciderConfig.from_dict(wire).to_dict() == wire


def test_cascade_agreement_trace_preserves_false_and_unverified_presence():
    from antfly.client_generated.models import DecisionCascadeTrace

    for state in ("agreed", "disagreed", "unverified"):
        wire = {
            "decider": "c",
            "definition_sha256": "a" * 64,
            "answered_by": "strong",
            "stages": [],
            "agreement_status": state,
        }
        if state != "unverified":
            wire["agreed"] = state == "agreed"
        if state == "disagreed":
            wire["disagreed_questions"] = ["same"]
        assert DecisionCascadeTrace.from_dict(wire).to_dict() == wire


def test_upstream_agreement_summary_round_trip_preserves_abstention():
    from antfly.client_generated.models import DecisionCascadeStageTrace

    for state in ("agreed", "disagreed", "unverified"):
        summary = {
            "agreement_status": state,
            "decider": "remote",
            "definition_sha256": "b" * 64,
            "answered_by": "strong",
        }
        if state != "unverified":
            summary["agreed"] = state == "agreed"
        if state == "disagreed":
            summary["disagreed_questions"] = ["same"]
        wire = {
            "name": "proxy",
            "decision_method": "typed",
            "escalated": False,
            "upstream_agreement": summary,
        }
        assert DecisionCascadeStageTrace.from_dict(wire).to_dict() == wire
