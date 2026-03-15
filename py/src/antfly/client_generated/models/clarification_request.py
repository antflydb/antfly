from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ClarificationRequest")


@_attrs_define
class ClarificationRequest:
    """Request for clarification from the user

    Attributes:
        question (str): The clarifying question to ask the user Example: Did you mean OAuth 1.0 or OAuth 2.0?.
        options (Union[Unset, list[str]]): Optional list of choices for the user Example: ['OAuth 1.0', 'OAuth 2.0',
            'Both'].
        reason (Union[Unset, str]): Why clarification is needed Example: The query mentions OAuth but doesn't specify
            which version.
    """

    question: str
    options: Union[Unset, list[str]] = UNSET
    reason: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        question = self.question

        options: Union[Unset, list[str]] = UNSET
        if not isinstance(self.options, Unset):
            options = self.options

        reason = self.reason

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "question": question,
            }
        )
        if options is not UNSET:
            field_dict["options"] = options
        if reason is not UNSET:
            field_dict["reason"] = reason

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        question = d.pop("question")

        options = cast(list[str], d.pop("options", UNSET))

        reason = d.pop("reason", UNSET)

        clarification_request = cls(
            question=question,
            options=options,
            reason=reason,
        )

        clarification_request.additional_properties = d
        return clarification_request

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
