from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.extraction_decision_confidence_method import ExtractionDecisionConfidenceMethod
from ..models.extraction_decision_type import ExtractionDecisionType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.extraction_label_probability import ExtractionLabelProbability


T = TypeVar("T", bound="ExtractionDecision")


@_attrs_define
class ExtractionDecision:
    """Version 2 typed classification decision. Probabilities follow request label order; ordinal levels are zero-based.

    Attributes:
        name (str):
        type_ (ExtractionDecisionType):
        label (str): Highest-probability label. This is distinct from the expected ordinal value.
        probabilities (list[ExtractionLabelProbability]):
        confidence (float):
        confidence_method (ExtractionDecisionConfidenceMethod): Entropy confidence is not the probability that the
            selected label is correct.
        act_probability (float): Auxiliary model estimate for acting. Does not authorize or execute a tool call.
        expected_value (float | Unset): Score decisions only; sum of zero-based level index times probability.
        true_probability (float | Unset): Boolean decisions only; probability of the true label.
    """

    name: str
    type_: ExtractionDecisionType
    label: str
    probabilities: list[ExtractionLabelProbability]
    confidence: float
    confidence_method: ExtractionDecisionConfidenceMethod
    act_probability: float
    expected_value: float | Unset = UNSET
    true_probability: float | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        type_ = self.type_.value

        label = self.label

        probabilities = []
        for probabilities_item_data in self.probabilities:
            probabilities_item = probabilities_item_data.to_dict()
            probabilities.append(probabilities_item)

        confidence = self.confidence

        confidence_method = self.confidence_method.value

        act_probability = self.act_probability

        expected_value = self.expected_value

        true_probability = self.true_probability

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "type": type_,
                "label": label,
                "probabilities": probabilities,
                "confidence": confidence,
                "confidence_method": confidence_method,
                "act_probability": act_probability,
            }
        )
        if expected_value is not UNSET:
            field_dict["expected_value"] = expected_value
        if true_probability is not UNSET:
            field_dict["true_probability"] = true_probability

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extraction_label_probability import ExtractionLabelProbability

        d = dict(src_dict)
        name = d.pop("name")

        type_ = ExtractionDecisionType(d.pop("type"))

        label = d.pop("label")

        probabilities = []
        _probabilities = d.pop("probabilities")
        for probabilities_item_data in _probabilities:
            probabilities_item = ExtractionLabelProbability.from_dict(probabilities_item_data)

            probabilities.append(probabilities_item)

        confidence = d.pop("confidence")

        confidence_method = ExtractionDecisionConfidenceMethod(d.pop("confidence_method"))

        act_probability = d.pop("act_probability")

        expected_value = d.pop("expected_value", UNSET)

        true_probability = d.pop("true_probability", UNSET)

        extraction_decision = cls(
            name=name,
            type_=type_,
            label=label,
            probabilities=probabilities,
            confidence=confidence,
            confidence_method=confidence_method,
            act_probability=act_probability,
            expected_value=expected_value,
            true_probability=true_probability,
        )

        extraction_decision.additional_properties = d
        return extraction_decision

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
