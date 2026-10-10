from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.composed_query_source import ComposedQuerySource


T = TypeVar("T", bound="CreateQuerySourceBody")


@_attrs_define
class CreateQuerySourceBody:
    """
    Attributes:
        source (ComposedQuerySource): Specify exactly one of saved, union or overlay. Union preserves duplicates and
            table provenance. Overlay suppresses replaced base keys and tombstones before ranking using indexed unfiltered
            change lookups. Inputs are streamed in bounded pages; result pages allow at most 4096 hits. Large overlay totals
            are lower bounds unless count is explicitly requested; exact count streams the full visible relation within the
            request deadline.
    """

    source: ComposedQuerySource
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        source = self.source.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "source": source,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.composed_query_source import ComposedQuerySource

        d = dict(src_dict)
        source = ComposedQuerySource.from_dict(d.pop("source"))

        create_query_source_body = cls(
            source=source,
        )

        create_query_source_body.additional_properties = d
        return create_query_source_body

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
