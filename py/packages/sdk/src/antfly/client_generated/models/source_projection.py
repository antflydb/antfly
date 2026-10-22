from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.source_projection_field import SourceProjectionField


T = TypeVar("T", bound="SourceProjection")


@_attrs_define
class SourceProjection:
    """Conditional delivery of selected logical source fields on authorized hits.
    Every field must also be selected by `fields` (or the default all-fields
    projection). Conditions only suppress unchanged source values; they never
    authorize a read, skip filters, or establish the existence of a record.
    The response reports value, unchanged, or missing separately for each path.
    Older servers may omit this extension; callers must retain full-value fallback.

        Attributes:
            fields (list[SourceProjectionField]):
    """

    fields: list[SourceProjectionField]

    def to_dict(self) -> dict[str, Any]:
        fields = []
        for fields_item_data in self.fields:
            fields_item = fields_item_data.to_dict()
            fields.append(fields_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "fields": fields,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.source_projection_field import SourceProjectionField

        d = dict(src_dict)
        fields = []
        _fields = d.pop("fields")
        for fields_item_data in _fields:
            fields_item = SourceProjectionField.from_dict(fields_item_data)

            fields.append(fields_item)

        source_projection = cls(
            fields=fields,
        )

        return source_projection
