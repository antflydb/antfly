from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.decision_evidence_text_type import DecisionEvidenceTextType

T = TypeVar("T", bound="DecisionEvidenceText")


@_attrs_define
class DecisionEvidenceText:
    """
    Attributes:
        type_ (DecisionEvidenceTextType):
        text (str):
    """

    type_: DecisionEvidenceTextType
    text: str

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        text = self.text

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "text": text,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = DecisionEvidenceTextType(d.pop("type"))

        text = d.pop("text")

        decision_evidence_text = cls(
            type_=type_,
            text=text,
        )

        return decision_evidence_text
