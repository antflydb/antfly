from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.table_storage_settings_dense_embeddings import TableStorageSettingsDenseEmbeddings
from ..types import UNSET, Unset

T = TypeVar("T", bound="TableStorageSettings")


@_attrs_define
class TableStorageSettings:
    """Immutable source embedding ownership. Omit storage when creating a table to select vector_store for a local single-
    shard standalone table without HA or replication, and primary_lsm for other deployments. Existing tables retain
    their recorded ownership; changing the creation default does not migrate data. Snapshot/backup and split operations
    currently reject vector_store tables; explicitly select primary_lsm when these operations are required.

        Attributes:
            dense_embeddings (TableStorageSettingsDenseEmbeddings | Unset): Explicit ownership choice. vector_store requires
                a fresh local single-shard standalone table without HA or replication. An explicit empty storage object keeps
                primary_lsm; omit the storage object to use the deployment default. Default:
                TableStorageSettingsDenseEmbeddings.PRIMARY_LSM.
    """

    dense_embeddings: TableStorageSettingsDenseEmbeddings | Unset = TableStorageSettingsDenseEmbeddings.PRIMARY_LSM

    def to_dict(self) -> dict[str, Any]:
        dense_embeddings: str | Unset = UNSET
        if not isinstance(self.dense_embeddings, Unset):
            dense_embeddings = self.dense_embeddings.value

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if dense_embeddings is not UNSET:
            field_dict["dense_embeddings"] = dense_embeddings

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _dense_embeddings = d.pop("dense_embeddings", UNSET)
        dense_embeddings: TableStorageSettingsDenseEmbeddings | Unset
        if isinstance(_dense_embeddings, Unset):
            dense_embeddings = UNSET
        else:
            dense_embeddings = TableStorageSettingsDenseEmbeddings(_dense_embeddings)

        table_storage_settings = cls(
            dense_embeddings=dense_embeddings,
        )

        return table_storage_settings
