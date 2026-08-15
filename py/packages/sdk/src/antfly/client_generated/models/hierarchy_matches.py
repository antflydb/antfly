from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="HierarchyMatches")


@_attrs_define
class HierarchyMatches:
    """
    Attributes:
        fields (list[str]): Fields to include in each nested match. This projection is required because
            grouped and matching records commonly have different schemas. Use an empty
            array to return match identity and hierarchy metadata without stored fields.
        limit (int | Unset): Maximum matching descendant hits attached to each group, independent of
            the top-level query limit. Matches follow the effective query order, and
            the group score is the score of its best matching descendant. The maximum
            bounds nested response growth. Group selection uses an adaptive candidate
            window, then each returned group is expanded with a separately bounded query,
            so a group with fewer matches never forces a global exhaustive scan. To bound
            execution as well as response growth, grouped queries accept at most 100
            top-level groups and 1,000 requested matches across the complete result page.
             Default: 3.
    """

    fields: list[str]
    limit: int | Unset = 3

    def to_dict(self) -> dict[str, Any]:
        fields = self.fields

        limit = self.limit

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "fields": fields,
            }
        )
        if limit is not UNSET:
            field_dict["limit"] = limit

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        fields = cast(list[str], d.pop("fields"))

        limit = d.pop("limit", UNSET)

        hierarchy_matches = cls(
            fields=fields,
            limit=limit,
        )

        return hierarchy_matches
