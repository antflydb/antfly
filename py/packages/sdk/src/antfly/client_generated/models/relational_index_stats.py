from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.relational_index_stats_index_type import RelationalIndexStatsIndexType

if TYPE_CHECKING:
    from ..models.index_milestones import IndexMilestones
    from ..models.relational_index_status import RelationalIndexStatus


T = TypeVar("T", bound="RelationalIndexStats")


@_attrs_define
class RelationalIndexStats:
    """
    Attributes:
        index_type (RelationalIndexStatsIndexType):
        milestones (IndexMilestones):
        relational_index (RelationalIndexStatus):
    """

    index_type: RelationalIndexStatsIndexType
    milestones: IndexMilestones
    relational_index: RelationalIndexStatus
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        index_type = self.index_type.value

        milestones = self.milestones.to_dict()

        relational_index = self.relational_index.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "index_type": index_type,
                "milestones": milestones,
                "relational_index": relational_index,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.index_milestones import IndexMilestones
        from ..models.relational_index_status import RelationalIndexStatus

        d = dict(src_dict)
        index_type = RelationalIndexStatsIndexType(d.pop("index_type"))

        milestones = IndexMilestones.from_dict(d.pop("milestones"))

        relational_index = RelationalIndexStatus.from_dict(d.pop("relational_index"))

        relational_index_stats = cls(
            index_type=index_type,
            milestones=milestones,
            relational_index=relational_index,
        )

        relational_index_stats.additional_properties = d
        return relational_index_stats

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
