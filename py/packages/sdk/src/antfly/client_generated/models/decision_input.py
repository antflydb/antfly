from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.decision_evidence_message import DecisionEvidenceMessage


T = TypeVar("T", bound="DecisionInput")


@_attrs_define
class DecisionInput:
    """
    Attributes:
        input_ (list[DecisionEvidenceMessage] | str): Text or ordered user messages containing text and images. Named
            OpenAI and Jev deciders accept messages. Native trained, embedding, and generated deciders require text. At most
            128 images and 1 MiB of serialized evidence per call. Jev requires inline PNG/JPEG data URLs and detail auto;
            OpenAI also accepts public HTTP(S) image URLs and WebP/GIF. Audio, files, tools, and non-user roles are
            unsupported.
        id (str | Unset):
        context_index (int | Unset): Index into request contexts. Requires context_version 1. Text expands to context,
            newline, then input. Message evidence gets a leading user message containing the context. The 1 MiB per-item and
            16 MiB aggregate evidence limits apply after expansion. Native and generated models require text.
    """

    input_: list[DecisionEvidenceMessage] | str
    id: str | Unset = UNSET
    context_index: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        input_: list[dict[str, Any]] | str
        if isinstance(self.input_, list):
            input_ = []
            for componentsschemas_decision_evidence_type_1_item_data in self.input_:
                componentsschemas_decision_evidence_type_1_item = (
                    componentsschemas_decision_evidence_type_1_item_data.to_dict()
                )
                input_.append(componentsschemas_decision_evidence_type_1_item)

        else:
            input_ = self.input_

        id = self.id

        context_index = self.context_index

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "input": input_,
            }
        )
        if id is not UNSET:
            field_dict["id"] = id
        if context_index is not UNSET:
            field_dict["context_index"] = context_index

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.decision_evidence_message import DecisionEvidenceMessage

        d = dict(src_dict)

        def _parse_input_(data: object) -> list[DecisionEvidenceMessage] | str:
            try:
                if not isinstance(data, list):
                    raise TypeError()
                componentsschemas_decision_evidence_type_1 = []
                _componentsschemas_decision_evidence_type_1 = data
                for componentsschemas_decision_evidence_type_1_item_data in _componentsschemas_decision_evidence_type_1:
                    componentsschemas_decision_evidence_type_1_item = DecisionEvidenceMessage.from_dict(
                        componentsschemas_decision_evidence_type_1_item_data
                    )

                    componentsschemas_decision_evidence_type_1.append(componentsschemas_decision_evidence_type_1_item)

                return componentsschemas_decision_evidence_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[DecisionEvidenceMessage] | str, data)

        input_ = _parse_input_(d.pop("input"))

        id = d.pop("id", UNSET)

        context_index = d.pop("context_index", UNSET)

        decision_input = cls(
            input_=input_,
            id=id,
            context_index=context_index,
        )

        return decision_input
