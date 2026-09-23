from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.sql_column_type import SQLColumnType

if TYPE_CHECKING:
    from ..models.sql_column import SQLColumn


T = TypeVar("T", bound="SQLPreparedResponse")


@_attrs_define
class SQLPreparedResponse:
    """
    Attributes:
        prepared_id (str):
        expires_at_ms (int):
        owner_node_id (str): Exact decimal API owner identifier, preserved by JavaScript clients.
        parameter_types (list[SQLColumnType]):
        columns (list[SQLColumn]):
    """

    prepared_id: str
    expires_at_ms: int
    owner_node_id: str
    parameter_types: list[SQLColumnType]
    columns: list[SQLColumn]

    def to_dict(self) -> dict[str, Any]:
        prepared_id = self.prepared_id

        expires_at_ms = self.expires_at_ms

        owner_node_id = self.owner_node_id

        parameter_types = []
        for parameter_types_item_data in self.parameter_types:
            parameter_types_item = parameter_types_item_data.value
            parameter_types.append(parameter_types_item)

        columns = []
        for columns_item_data in self.columns:
            columns_item = columns_item_data.to_dict()
            columns.append(columns_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "prepared_id": prepared_id,
                "expires_at_ms": expires_at_ms,
                "owner_node_id": owner_node_id,
                "parameter_types": parameter_types,
                "columns": columns,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.sql_column import SQLColumn

        d = dict(src_dict)
        prepared_id = d.pop("prepared_id")

        expires_at_ms = d.pop("expires_at_ms")

        owner_node_id = d.pop("owner_node_id")

        parameter_types = []
        _parameter_types = d.pop("parameter_types")
        for parameter_types_item_data in _parameter_types:
            parameter_types_item = SQLColumnType(parameter_types_item_data)

            parameter_types.append(parameter_types_item)

        columns = []
        _columns = d.pop("columns")
        for columns_item_data in _columns:
            columns_item = SQLColumn.from_dict(columns_item_data)

            columns.append(columns_item)

        sql_prepared_response = cls(
            prepared_id=prepared_id,
            expires_at_ms=expires_at_ms,
            owner_node_id=owner_node_id,
            parameter_types=parameter_types,
            columns=columns,
        )

        return sql_prepared_response
