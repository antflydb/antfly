from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="RestoreRoutineMapping")


@_attrs_define
class RestoreRoutineMapping:
    """
    Attributes:
        source_metadata_incarnation (str): Metadata catalog incarnation recorded in the source attachment.
        source_routine_id (str): Immutable source function identifier, expressed losslessly as decimal.
        destination_routine_id (str): Existing immutable destination function identifier, expressed losslessly as
            decimal.
    """

    source_metadata_incarnation: str
    source_routine_id: str
    destination_routine_id: str

    def to_dict(self) -> dict[str, Any]:
        source_metadata_incarnation = self.source_metadata_incarnation

        source_routine_id = self.source_routine_id

        destination_routine_id = self.destination_routine_id

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "source_metadata_incarnation": source_metadata_incarnation,
                "source_routine_id": source_routine_id,
                "destination_routine_id": destination_routine_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        source_metadata_incarnation = d.pop("source_metadata_incarnation")

        source_routine_id = d.pop("source_routine_id")

        destination_routine_id = d.pop("destination_routine_id")

        restore_routine_mapping = cls(
            source_metadata_incarnation=source_metadata_incarnation,
            source_routine_id=source_routine_id,
            destination_routine_id=destination_routine_id,
        )

        return restore_routine_mapping
