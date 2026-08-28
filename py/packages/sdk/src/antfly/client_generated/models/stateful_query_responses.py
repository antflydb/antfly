from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.stateful_query_result import StatefulQueryResult


T = TypeVar("T", bound="StatefulQueryResponses")


@_attrs_define
class StatefulQueryResponses:
    """Responses from the stateful compatibility transport. Canonical requests still produce canonical graph result
    variants; deprecated graph_searches may produce LegacyGraphSearchResult values during the transition window.

        Attributes:
            responses (list[StatefulQueryResult] | Unset):
    """

    responses: list[StatefulQueryResult] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        responses: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.responses, Unset):
            responses = []
            for responses_item_data in self.responses:
                responses_item = responses_item_data.to_dict()
                responses.append(responses_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if responses is not UNSET:
            field_dict["responses"] = responses

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.stateful_query_result import StatefulQueryResult

        d = dict(src_dict)
        _responses = d.pop("responses", UNSET)
        responses: list[StatefulQueryResult] | Unset = UNSET
        if _responses is not UNSET:
            responses = []
            for responses_item_data in _responses:
                responses_item = StatefulQueryResult.from_dict(responses_item_data)

                responses.append(responses_item)

        stateful_query_responses = cls(
            responses=responses,
        )

        stateful_query_responses.additional_properties = d
        return stateful_query_responses

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
