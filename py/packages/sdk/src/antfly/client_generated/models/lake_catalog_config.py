from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.lake_catalog_config_type import LakeCatalogConfigType
from ..types import UNSET, Unset

T = TypeVar("T", bound="LakeCatalogConfig")


@_attrs_define
class LakeCatalogConfig:
    """Catalog authority is independent of S3/GCS storage and deployment. managed uses a conditional durable head under the
    table root. rest uses a named HTTP connection; raw secrets are forbidden. Omit to retain explicit metadata
    URI/version-hint discovery.

        Attributes:
            type_ (LakeCatalogConfigType):
            connection (str | Unset): Required for rest. Named external_io/http connection with lake_catalog_read and, for
                commits, lake_catalog_write capabilities.
            uri (str | Unset): Required for rest; catalog base URI whose origin must be allowed by the named connection.
            namespace (list[str] | Unset): Required nonempty namespace components for rest.
            name (str | Unset): Required table name for rest, distinct from Antfly's logical table name.
            warehouse (str | Unset): Optional REST config warehouse selector. Other fields must be omitted for managed.
    """

    type_: LakeCatalogConfigType
    connection: str | Unset = UNSET
    uri: str | Unset = UNSET
    namespace: list[str] | Unset = UNSET
    name: str | Unset = UNSET
    warehouse: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        connection = self.connection

        uri = self.uri

        namespace: list[str] | Unset = UNSET
        if not isinstance(self.namespace, Unset):
            namespace = self.namespace

        name = self.name

        warehouse = self.warehouse

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
            }
        )
        if connection is not UNSET:
            field_dict["connection"] = connection
        if uri is not UNSET:
            field_dict["uri"] = uri
        if namespace is not UNSET:
            field_dict["namespace"] = namespace
        if name is not UNSET:
            field_dict["name"] = name
        if warehouse is not UNSET:
            field_dict["warehouse"] = warehouse

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = LakeCatalogConfigType(d.pop("type"))

        connection = d.pop("connection", UNSET)

        uri = d.pop("uri", UNSET)

        namespace = cast(list[str], d.pop("namespace", UNSET))

        name = d.pop("name", UNSET)

        warehouse = d.pop("warehouse", UNSET)

        lake_catalog_config = cls(
            type_=type_,
            connection=connection,
            uri=uri,
            namespace=namespace,
            name=name,
            warehouse=warehouse,
        )

        return lake_catalog_config
