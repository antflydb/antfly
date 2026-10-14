from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.lake_catalog_create_request_partition_spec import LakeCatalogCreateRequestPartitionSpec
    from ..models.lake_catalog_create_request_properties import LakeCatalogCreateRequestProperties
    from ..models.lake_catalog_create_request_schema import LakeCatalogCreateRequestSchema
    from ..models.lake_catalog_create_request_write_order import LakeCatalogCreateRequestWriteOrder


T = TypeVar("T", bound="LakeCatalogCreateRequest")


@_attrs_define
class LakeCatalogCreateRequest:
    """
    Attributes:
        commit_id (str): Stable identifier reused with exactly the same request after timeout or restart.
        schema (LakeCatalogCreateRequestSchema): Iceberg schema including schema-id and persistent field IDs.
        partition_spec (LakeCatalogCreateRequestPartitionSpec | Unset):
        write_order (LakeCatalogCreateRequestWriteOrder | Unset):
        properties (LakeCatalogCreateRequestProperties | Unset):
    """

    commit_id: str
    schema: LakeCatalogCreateRequestSchema
    partition_spec: LakeCatalogCreateRequestPartitionSpec | Unset = UNSET
    write_order: LakeCatalogCreateRequestWriteOrder | Unset = UNSET
    properties: LakeCatalogCreateRequestProperties | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        commit_id = self.commit_id

        schema = self.schema.to_dict()

        partition_spec: dict[str, Any] | Unset = UNSET
        if not isinstance(self.partition_spec, Unset):
            partition_spec = self.partition_spec.to_dict()

        write_order: dict[str, Any] | Unset = UNSET
        if not isinstance(self.write_order, Unset):
            write_order = self.write_order.to_dict()

        properties: dict[str, Any] | Unset = UNSET
        if not isinstance(self.properties, Unset):
            properties = self.properties.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "commit_id": commit_id,
                "schema": schema,
            }
        )
        if partition_spec is not UNSET:
            field_dict["partition-spec"] = partition_spec
        if write_order is not UNSET:
            field_dict["write-order"] = write_order
        if properties is not UNSET:
            field_dict["properties"] = properties

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.lake_catalog_create_request_partition_spec import LakeCatalogCreateRequestPartitionSpec
        from ..models.lake_catalog_create_request_properties import LakeCatalogCreateRequestProperties
        from ..models.lake_catalog_create_request_schema import LakeCatalogCreateRequestSchema
        from ..models.lake_catalog_create_request_write_order import LakeCatalogCreateRequestWriteOrder

        d = dict(src_dict)
        commit_id = d.pop("commit_id")

        schema = LakeCatalogCreateRequestSchema.from_dict(d.pop("schema"))

        _partition_spec = d.pop("partition-spec", UNSET)
        partition_spec: LakeCatalogCreateRequestPartitionSpec | Unset
        if isinstance(_partition_spec, Unset):
            partition_spec = UNSET
        else:
            partition_spec = LakeCatalogCreateRequestPartitionSpec.from_dict(_partition_spec)

        _write_order = d.pop("write-order", UNSET)
        write_order: LakeCatalogCreateRequestWriteOrder | Unset
        if isinstance(_write_order, Unset):
            write_order = UNSET
        else:
            write_order = LakeCatalogCreateRequestWriteOrder.from_dict(_write_order)

        _properties = d.pop("properties", UNSET)
        properties: LakeCatalogCreateRequestProperties | Unset
        if isinstance(_properties, Unset):
            properties = UNSET
        else:
            properties = LakeCatalogCreateRequestProperties.from_dict(_properties)

        lake_catalog_create_request = cls(
            commit_id=commit_id,
            schema=schema,
            partition_spec=partition_spec,
            write_order=write_order,
            properties=properties,
        )

        return lake_catalog_create_request
