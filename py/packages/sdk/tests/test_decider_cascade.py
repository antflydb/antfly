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
    assert DeciderConfig.from_dict(wire).to_dict() == wire
