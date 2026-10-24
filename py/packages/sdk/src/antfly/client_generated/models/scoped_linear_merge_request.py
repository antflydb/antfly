from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.scoped_linear_merge_page import ScopedLinearMergePage


T = TypeVar("T", bound="ScopedLinearMergeRequest")


@_attrs_define
class ScopedLinearMergeRequest:
    """Bounded LinearMerge v1 envelope. Legacy servers reject this envelope
    before mutation because it has no top-level records field. Never fall
    back to an unbounded merge when a server rejects this request.

        Attributes:
            scoped_merge (ScopedLinearMergePage): Merge within explicit exclusive key bounds. Records and retained IDs
                must be inside the range and after last_merged_id. Overlapping writers
                still require external serialization. Upsert pages normally set
                delete_missing=false. After a complete authoritative inventory has been
                written and verified, an empty-record page with retain_ids reconciles
                the entire bounded range without retransmitting document content.
    """

    scoped_merge: ScopedLinearMergePage

    def to_dict(self) -> dict[str, Any]:
        scoped_merge = self.scoped_merge.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "scoped_merge": scoped_merge,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.scoped_linear_merge_page import ScopedLinearMergePage

        d = dict(src_dict)
        scoped_merge = ScopedLinearMergePage.from_dict(d.pop("scoped_merge"))

        scoped_linear_merge_request = cls(
            scoped_merge=scoped_merge,
        )

        return scoped_linear_merge_request
