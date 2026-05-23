from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.termite_finish_reason import TermiteFinishReason
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.termite_generate_choice_logprobs_type_0 import TermiteGenerateChoiceLogprobsType0
    from ..models.termite_generate_message import TermiteGenerateMessage


T = TypeVar("T", bound="TermiteGenerateChoice")


@_attrs_define
class TermiteGenerateChoice:
    """
    Attributes:
        index (int): Index of this choice in the list
        message (TermiteGenerateMessage):
        finish_reason (TermiteFinishReason): Reason why generation stopped
        logprobs (None | TermiteGenerateChoiceLogprobsType0 | Unset): Log probability information (not supported, always
            null)
    """

    index: int
    message: TermiteGenerateMessage
    finish_reason: TermiteFinishReason
    logprobs: None | TermiteGenerateChoiceLogprobsType0 | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.termite_generate_choice_logprobs_type_0 import TermiteGenerateChoiceLogprobsType0

        index = self.index

        message = self.message.to_dict()

        finish_reason = self.finish_reason.value

        logprobs: dict[str, Any] | None | Unset
        if isinstance(self.logprobs, Unset):
            logprobs = UNSET
        elif isinstance(self.logprobs, TermiteGenerateChoiceLogprobsType0):
            logprobs = self.logprobs.to_dict()
        else:
            logprobs = self.logprobs

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "index": index,
                "message": message,
                "finish_reason": finish_reason,
            }
        )
        if logprobs is not UNSET:
            field_dict["logprobs"] = logprobs

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.termite_generate_choice_logprobs_type_0 import TermiteGenerateChoiceLogprobsType0
        from ..models.termite_generate_message import TermiteGenerateMessage

        d = dict(src_dict)
        index = d.pop("index")

        message = TermiteGenerateMessage.from_dict(d.pop("message"))

        finish_reason = TermiteFinishReason(d.pop("finish_reason"))

        def _parse_logprobs(data: object) -> None | TermiteGenerateChoiceLogprobsType0 | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                logprobs_type_0 = TermiteGenerateChoiceLogprobsType0.from_dict(data)

                return logprobs_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | TermiteGenerateChoiceLogprobsType0 | Unset, data)

        logprobs = _parse_logprobs(d.pop("logprobs", UNSET))

        termite_generate_choice = cls(
            index=index,
            message=message,
            finish_reason=finish_reason,
            logprobs=logprobs,
        )

        termite_generate_choice.additional_properties = d
        return termite_generate_choice

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
