from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="RerankerProfile")


@_attrs_define
class RerankerProfile:
    """Reranking execution statistics.

    Attributes:
        model (Union[Unset, str]): Reranker model that was used.
        documents_reranked (Union[Unset, int]): Number of documents that were reranked.
        duration_ms (Union[Unset, int]): Time spent reranking in milliseconds.
    """

    model: Union[Unset, str] = UNSET
    documents_reranked: Union[Unset, int] = UNSET
    duration_ms: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        documents_reranked = self.documents_reranked

        duration_ms = self.duration_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if model is not UNSET:
            field_dict["model"] = model
        if documents_reranked is not UNSET:
            field_dict["documents_reranked"] = documents_reranked
        if duration_ms is not UNSET:
            field_dict["duration_ms"] = duration_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model", UNSET)

        documents_reranked = d.pop("documents_reranked", UNSET)

        duration_ms = d.pop("duration_ms", UNSET)

        reranker_profile = cls(
            model=model,
            documents_reranked=documents_reranked,
            duration_ms=duration_ms,
        )

        reranker_profile.additional_properties = d
        return reranker_profile

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
