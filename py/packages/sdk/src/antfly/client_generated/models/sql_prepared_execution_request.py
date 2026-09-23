from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="SQLPreparedExecutionRequest")


@_attrs_define
class SQLPreparedExecutionRequest:
    """
    Attributes:
        parameters (list[Any] | Unset):
        limit (int | Unset):  Default: 128.
        session_id (str | Unset): Optional durable transaction session, independent of the prepared resource lifetime.
    """

    parameters: list[Any] | Unset = UNSET
    limit: int | Unset = 128
    session_id: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        parameters: list[Any] | Unset = UNSET
        if not isinstance(self.parameters, Unset):
            parameters = self.parameters

        limit = self.limit

        session_id = self.session_id

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if parameters is not UNSET:
            field_dict["parameters"] = parameters
        if limit is not UNSET:
            field_dict["limit"] = limit
        if session_id is not UNSET:
            field_dict["session_id"] = session_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        parameters = cast(list[Any], d.pop("parameters", UNSET))

        limit = d.pop("limit", UNSET)

        session_id = d.pop("session_id", UNSET)

        sql_prepared_execution_request = cls(
            parameters=parameters,
            limit=limit,
            session_id=session_id,
        )

        return sql_prepared_execution_request
