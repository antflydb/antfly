from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="SQLConnectionResponse")


@_attrs_define
class SQLConnectionResponse:
    """
    Attributes:
        connection_id (str):
        owner_node_id (str): The owning API node. Zero denotes a standalone-local endpoint; send subsequent connection
            requests to that same endpoint. Otherwise route requests to the returned owning node.
        expires_at_ms (int): Idle-use deadline in Unix milliseconds. An attached active or uncertain transaction remains
            accessible for completion and reconciliation after this deadline; expiry never implies abort.
        database (str):
        namespace (str):
    """

    connection_id: str
    owner_node_id: str
    expires_at_ms: int
    database: str
    namespace: str

    def to_dict(self) -> dict[str, Any]:
        connection_id = self.connection_id

        owner_node_id = self.owner_node_id

        expires_at_ms = self.expires_at_ms

        database = self.database

        namespace = self.namespace

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "connection_id": connection_id,
                "owner_node_id": owner_node_id,
                "expires_at_ms": expires_at_ms,
                "database": database,
                "namespace": namespace,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        connection_id = d.pop("connection_id")

        owner_node_id = d.pop("owner_node_id")

        expires_at_ms = d.pop("expires_at_ms")

        database = d.pop("database")

        namespace = d.pop("namespace")

        sql_connection_response = cls(
            connection_id=connection_id,
            owner_node_id=owner_node_id,
            expires_at_ms=expires_at_ms,
            database=database,
            namespace=namespace,
        )

        return sql_connection_response
