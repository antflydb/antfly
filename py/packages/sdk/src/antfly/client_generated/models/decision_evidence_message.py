from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.decision_evidence_message_role import DecisionEvidenceMessageRole
from ..models.decision_evidence_message_type import DecisionEvidenceMessageType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.decision_evidence_image import DecisionEvidenceImage
    from ..models.decision_evidence_text import DecisionEvidenceText


T = TypeVar("T", bound="DecisionEvidenceMessage")


@_attrs_define
class DecisionEvidenceMessage:
    """
    Attributes:
        role (DecisionEvidenceMessageRole):
        content (list[DecisionEvidenceImage | DecisionEvidenceText] | str):
        type_ (DecisionEvidenceMessageType | Unset):
    """

    role: DecisionEvidenceMessageRole
    content: list[DecisionEvidenceImage | DecisionEvidenceText] | str
    type_: DecisionEvidenceMessageType | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.decision_evidence_text import DecisionEvidenceText

        role = self.role.value

        content: list[dict[str, Any]] | str
        if isinstance(self.content, list):
            content = []
            for content_type_1_item_data in self.content:
                content_type_1_item: dict[str, Any]
                if isinstance(content_type_1_item_data, DecisionEvidenceText):
                    content_type_1_item = content_type_1_item_data.to_dict()
                else:
                    content_type_1_item = content_type_1_item_data.to_dict()

                content.append(content_type_1_item)

        else:
            content = self.content

        type_: str | Unset = UNSET
        if not isinstance(self.type_, Unset):
            type_ = self.type_.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "role": role,
                "content": content,
            }
        )
        if type_ is not UNSET:
            field_dict["type"] = type_

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.decision_evidence_image import DecisionEvidenceImage
        from ..models.decision_evidence_text import DecisionEvidenceText

        d = dict(src_dict)
        role = DecisionEvidenceMessageRole(d.pop("role"))

        def _parse_content(data: object) -> list[DecisionEvidenceImage | DecisionEvidenceText] | str:
            try:
                if not isinstance(data, list):
                    raise TypeError()
                content_type_1 = []
                _content_type_1 = data
                for content_type_1_item_data in _content_type_1:

                    def _parse_content_type_1_item(data: object) -> DecisionEvidenceImage | DecisionEvidenceText:
                        try:
                            if not isinstance(data, dict):
                                raise TypeError()
                            content_type_1_item_type_0 = DecisionEvidenceText.from_dict(data)

                            return content_type_1_item_type_0
                        except (TypeError, ValueError, AttributeError, KeyError):
                            pass
                        if not isinstance(data, dict):
                            raise TypeError()
                        content_type_1_item_type_1 = DecisionEvidenceImage.from_dict(data)

                        return content_type_1_item_type_1

                    content_type_1_item = _parse_content_type_1_item(content_type_1_item_data)

                    content_type_1.append(content_type_1_item)

                return content_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[DecisionEvidenceImage | DecisionEvidenceText] | str, data)

        content = _parse_content(d.pop("content"))

        _type_ = d.pop("type", UNSET)
        type_: DecisionEvidenceMessageType | Unset
        if isinstance(_type_, Unset):
            type_ = UNSET
        else:
            type_ = DecisionEvidenceMessageType(_type_)

        decision_evidence_message = cls(
            role=role,
            content=content,
            type_=type_,
        )

        return decision_evidence_message
