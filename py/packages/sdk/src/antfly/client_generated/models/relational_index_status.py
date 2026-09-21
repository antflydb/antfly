from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.relational_index_build_state import RelationalIndexBuildState

if TYPE_CHECKING:
    from ..models.relational_index_range_status import RelationalIndexRangeStatus


T = TypeVar("T", bound="RelationalIndexStatus")


@_attrs_define
class RelationalIndexStatus:
    """
    Attributes:
        table_id (str):
        schema_version (int):
        index_name (str):
        state (RelationalIndexBuildState):
        ranges (list[RelationalIndexRangeStatus]):
    """

    table_id: str
    schema_version: int
    index_name: str
    state: RelationalIndexBuildState
    ranges: list[RelationalIndexRangeStatus]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        table_id = self.table_id

        schema_version = self.schema_version

        index_name = self.index_name

        state = self.state.value

        ranges = []
        for ranges_item_data in self.ranges:
            ranges_item = ranges_item_data.to_dict()
            ranges.append(ranges_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "table_id": table_id,
                "schema_version": schema_version,
                "index_name": index_name,
                "state": state,
                "ranges": ranges,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_index_range_status import RelationalIndexRangeStatus

        d = dict(src_dict)
        table_id = d.pop("table_id")

        schema_version = d.pop("schema_version")

        index_name = d.pop("index_name")

        state = RelationalIndexBuildState(d.pop("state"))

        ranges = []
        _ranges = d.pop("ranges")
        for ranges_item_data in _ranges:
            ranges_item = RelationalIndexRangeStatus.from_dict(ranges_item_data)

            ranges.append(ranges_item)

        relational_index_status = cls(
            table_id=table_id,
            schema_version=schema_version,
            index_name=index_name,
            state=state,
            ranges=ranges,
        )

        relational_index_status.additional_properties = d
        return relational_index_status

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
