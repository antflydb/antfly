from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.source_projection_field_mode import SourceProjectionFieldMode
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.source_value_condition import SourceValueCondition


T = TypeVar("T", bound="SourceProjectionField")


@_attrs_define
class SourceProjectionField:
    """
    Attributes:
        field (str): Exact logical object path; overlapping paths and wildcards are not supported.
        mode (SourceProjectionFieldMode | Unset): Value mode delivers the value unless if_match succeeds. Identity mode
            returns only a freshly authorized identity; existing values have state reference and are omitted from _source
            independently of if_match. Reference state is execution evidence, never a read grant. Default:
            SourceProjectionFieldMode.VALUE.
        if_match (list[SourceValueCondition] | Unset): Optional per-record expected identities. Duplicate record IDs are
            invalid.
    """

    field: str
    mode: SourceProjectionFieldMode | Unset = SourceProjectionFieldMode.VALUE
    if_match: list[SourceValueCondition] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        field = self.field

        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        if_match: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.if_match, Unset):
            if_match = []
            for if_match_item_data in self.if_match:
                if_match_item = if_match_item_data.to_dict()
                if_match.append(if_match_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "field": field,
            }
        )
        if mode is not UNSET:
            field_dict["mode"] = mode
        if if_match is not UNSET:
            field_dict["if_match"] = if_match

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.source_value_condition import SourceValueCondition

        d = dict(src_dict)
        field = d.pop("field")

        _mode = d.pop("mode", UNSET)
        mode: SourceProjectionFieldMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = SourceProjectionFieldMode(_mode)

        _if_match = d.pop("if_match", UNSET)
        if_match: list[SourceValueCondition] | Unset = UNSET
        if _if_match is not UNSET:
            if_match = []
            for if_match_item_data in _if_match:
                if_match_item = SourceValueCondition.from_dict(if_match_item_data)

                if_match.append(if_match_item)

        source_projection_field = cls(
            field=field,
            mode=mode,
            if_match=if_match,
        )

        return source_projection_field
