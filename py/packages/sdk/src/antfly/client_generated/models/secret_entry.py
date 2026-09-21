from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..models.secret_status import SecretStatus
from ..types import UNSET, Unset

T = TypeVar("T", bound="SecretEntry")


@_attrs_define
class SecretEntry:
    """
    Attributes:
        key (str): Secret name (e.g., openai.api_key)
        status (SecretStatus): Source of the secret configuration
        source (str | Unset): Name of the winning source, or environment.
        managed (bool | Unset): Whether this key has an Antfly-managed override that can be deleted.
        revision (int | Unset): Committed native entry revision, when supported by the configured backend.
        env_var (str | Unset): Corresponding environment variable name (e.g., OPENAI_API_KEY)
        created_at (datetime.datetime | Unset):
        updated_at (datetime.datetime | Unset):
    """

    key: str
    status: SecretStatus
    source: str | Unset = UNSET
    managed: bool | Unset = UNSET
    revision: int | Unset = UNSET
    env_var: str | Unset = UNSET
    created_at: datetime.datetime | Unset = UNSET
    updated_at: datetime.datetime | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        key = self.key

        status = self.status.value

        source = self.source

        managed = self.managed

        revision = self.revision

        env_var = self.env_var

        created_at: str | Unset = UNSET
        if not isinstance(self.created_at, Unset):
            created_at = self.created_at.isoformat()

        updated_at: str | Unset = UNSET
        if not isinstance(self.updated_at, Unset):
            updated_at = self.updated_at.isoformat()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "key": key,
                "status": status,
            }
        )
        if source is not UNSET:
            field_dict["source"] = source
        if managed is not UNSET:
            field_dict["managed"] = managed
        if revision is not UNSET:
            field_dict["revision"] = revision
        if env_var is not UNSET:
            field_dict["env_var"] = env_var
        if created_at is not UNSET:
            field_dict["created_at"] = created_at
        if updated_at is not UNSET:
            field_dict["updated_at"] = updated_at

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        key = d.pop("key")

        status = SecretStatus(d.pop("status"))

        source = d.pop("source", UNSET)

        managed = d.pop("managed", UNSET)

        revision = d.pop("revision", UNSET)

        env_var = d.pop("env_var", UNSET)

        _created_at = d.pop("created_at", UNSET)
        created_at: datetime.datetime | Unset
        if isinstance(_created_at, Unset):
            created_at = UNSET
        else:
            created_at = isoparse(_created_at)

        _updated_at = d.pop("updated_at", UNSET)
        updated_at: datetime.datetime | Unset
        if isinstance(_updated_at, Unset):
            updated_at = UNSET
        else:
            updated_at = isoparse(_updated_at)

        secret_entry = cls(
            key=key,
            status=status,
            source=source,
            managed=managed,
            revision=revision,
            env_var=env_var,
            created_at=created_at,
            updated_at=updated_at,
        )

        secret_entry.additional_properties = d
        return secret_entry

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
