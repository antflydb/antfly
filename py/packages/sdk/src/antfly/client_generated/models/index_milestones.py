from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.index_milestone_status import IndexMilestoneStatus


T = TypeVar("T", bound="IndexMilestones")


@_attrs_define
class IndexMilestones:
    """
    Attributes:
        queryable (IndexMilestoneStatus):
        complete (IndexMilestoneStatus):
    """

    queryable: IndexMilestoneStatus
    complete: IndexMilestoneStatus

    def to_dict(self) -> dict[str, Any]:
        queryable = self.queryable.to_dict()

        complete = self.complete.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "queryable": queryable,
                "complete": complete,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.index_milestone_status import IndexMilestoneStatus

        d = dict(src_dict)
        queryable = IndexMilestoneStatus.from_dict(d.pop("queryable"))

        complete = IndexMilestoneStatus.from_dict(d.pop("complete"))

        index_milestones = cls(
            queryable=queryable,
            complete=complete,
        )

        return index_milestones
