from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

T = TypeVar("T", bound="IndexMilestoneStatus")


@_attrs_define
class IndexMilestoneStatus:
    """
    Attributes:
        reached (bool): Whether this milestone is satisfied by the observed index incarnation.
        blockers (list[str]): Milestone-specific, machine-readable blockers. Empty whenever reached is true.
    """

    reached: bool
    blockers: list[str]

    def to_dict(self) -> dict[str, Any]:
        reached = self.reached

        blockers = self.blockers

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "reached": reached,
                "blockers": blockers,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        reached = d.pop("reached")

        blockers = cast(list[str], d.pop("blockers"))

        index_milestone_status = cls(
            reached=reached,
            blockers=blockers,
        )

        return index_milestone_status
