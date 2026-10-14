from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.lake_read_requirement_visibility import LakeReadRequirementVisibility
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.lake_read_receipt import LakeReadReceipt


T = TypeVar("T", bound="LakeReadRequirement")


@_attrs_define
class LakeReadRequirement:
    """
    Attributes:
        visibility (LakeReadRequirementVisibility | Unset): Accepted requires current WAL visibility; vector/hybrid
            reads wait for matching publication. Published explicitly permits the retained archive generation. Applies to
            native writable Iceberg reads.
        through (LakeReadReceipt | Unset):
        wait_ms (int | Unset): Bounded readiness wait, default 5000 ms when lake_read is supplied; also bounded by query
            timeout and cancellation.
    """

    visibility: LakeReadRequirementVisibility | Unset = UNSET
    through: LakeReadReceipt | Unset = UNSET
    wait_ms: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        visibility: str | Unset = UNSET
        if not isinstance(self.visibility, Unset):
            visibility = self.visibility.value

        through: dict[str, Any] | Unset = UNSET
        if not isinstance(self.through, Unset):
            through = self.through.to_dict()

        wait_ms = self.wait_ms

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if visibility is not UNSET:
            field_dict["visibility"] = visibility
        if through is not UNSET:
            field_dict["through"] = through
        if wait_ms is not UNSET:
            field_dict["wait_ms"] = wait_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.lake_read_receipt import LakeReadReceipt

        d = dict(src_dict)
        _visibility = d.pop("visibility", UNSET)
        visibility: LakeReadRequirementVisibility | Unset
        if isinstance(_visibility, Unset):
            visibility = UNSET
        else:
            visibility = LakeReadRequirementVisibility(_visibility)

        _through = d.pop("through", UNSET)
        through: LakeReadReceipt | Unset
        if isinstance(_through, Unset):
            through = UNSET
        else:
            through = LakeReadReceipt.from_dict(_through)

        wait_ms = d.pop("wait_ms", UNSET)

        lake_read_requirement = cls(
            visibility=visibility,
            through=through,
            wait_ms=wait_ms,
        )

        return lake_read_requirement
