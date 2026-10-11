from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.read_requirements import ReadRequirements


T = TypeVar("T", bound="SQLPreparedExecutionRequest")


@_attrs_define
class SQLPreparedExecutionRequest:
    """
    Attributes:
        read_requirements (ReadRequirements | Unset): Freshness requirements for writable Iceberg reads. Search supports
            accepted and published (default accepted); SQL supports accepted and committed (default committed). Unsupported
            visibility modes fail explicitly. Cannot be combined with legacy lake_read or lake_visibility. A pinned
            transaction snapshot never advances to satisfy a newer receipt.
        parameters (list[Any] | Unset):
        limit (int | Unset):  Default: 128.
        session_id (str | Unset): Optional durable transaction session. Required when the resource was prepared against
            a session; otherwise independent of the prepared resource lifetime.
        connection_id (str | Unset): Required for a connection-bound resource; DISCARD ALL closes it without affecting
            other clients' resources.
    """

    read_requirements: ReadRequirements | Unset = UNSET
    parameters: list[Any] | Unset = UNSET
    limit: int | Unset = 128
    session_id: str | Unset = UNSET
    connection_id: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        read_requirements: dict[str, Any] | Unset = UNSET
        if not isinstance(self.read_requirements, Unset):
            read_requirements = self.read_requirements.to_dict()

        parameters: list[Any] | Unset = UNSET
        if not isinstance(self.parameters, Unset):
            parameters = self.parameters

        limit = self.limit

        session_id = self.session_id

        connection_id = self.connection_id

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if read_requirements is not UNSET:
            field_dict["read_requirements"] = read_requirements
        if parameters is not UNSET:
            field_dict["parameters"] = parameters
        if limit is not UNSET:
            field_dict["limit"] = limit
        if session_id is not UNSET:
            field_dict["session_id"] = session_id
        if connection_id is not UNSET:
            field_dict["connection_id"] = connection_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.read_requirements import ReadRequirements

        d = dict(src_dict)
        _read_requirements = d.pop("read_requirements", UNSET)
        read_requirements: ReadRequirements | Unset
        if isinstance(_read_requirements, Unset):
            read_requirements = UNSET
        else:
            read_requirements = ReadRequirements.from_dict(_read_requirements)

        parameters = cast(list[Any], d.pop("parameters", UNSET))

        limit = d.pop("limit", UNSET)

        session_id = d.pop("session_id", UNSET)

        connection_id = d.pop("connection_id", UNSET)

        sql_prepared_execution_request = cls(
            read_requirements=read_requirements,
            parameters=parameters,
            limit=limit,
            session_id=session_id,
            connection_id=connection_id,
        )

        return sql_prepared_execution_request
