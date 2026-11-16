from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="RepairRemainingFailure")


@_attrs_define
class RepairRemainingFailure:
    """A group of documents whose repair still fails with the same error.

    Attributes:
        error_code (str): Error recorded for the documents, for example `ModelNotFound`.
        count (int): Number of documents in this group.
        attempts (int): Highest repair attempt count among the group's documents.
        transient (bool): Whether the error is temporary (admission refusal, rate limit). Repair retries these within a
            job; running repair again usually succeeds.
        hint (str): What to do next.
    """

    error_code: str
    count: int
    attempts: int
    transient: bool
    hint: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        error_code = self.error_code

        count = self.count

        attempts = self.attempts

        transient = self.transient

        hint = self.hint

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "error_code": error_code,
                "count": count,
                "attempts": attempts,
                "transient": transient,
                "hint": hint,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        error_code = d.pop("error_code")

        count = d.pop("count")

        attempts = d.pop("attempts")

        transient = d.pop("transient")

        hint = d.pop("hint")

        repair_remaining_failure = cls(
            error_code=error_code,
            count=count,
            attempts=attempts,
            transient=transient,
            hint=hint,
        )

        repair_remaining_failure.additional_properties = d
        return repair_remaining_failure

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
