from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="SQLNotice")


@_attrs_define
class SQLNotice:
    """
    Attributes:
        code (str):
        message (str): Nonfatal statement notice; prior notices remain visible if a later instruction fails.
    """

    code: str
    message: str

    def to_dict(self) -> dict[str, Any]:
        code = self.code

        message = self.message

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "code": code,
                "message": message,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        code = d.pop("code")

        message = d.pop("message")

        sql_notice = cls(
            code=code,
            message=message,
        )

        return sql_notice
