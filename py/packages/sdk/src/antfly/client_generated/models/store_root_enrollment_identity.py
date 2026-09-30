from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="StoreRootEnrollmentIdentity")


@_attrs_define
class StoreRootEnrollmentIdentity:
    """
    Attributes:
        metadata_incarnation (str):
        node_id (int):
        store_id (int):
        root_incarnation (str): Decimal u128 string; never pass through a floating-point JSON number.
        public_key (str): Ed25519 public key in lowercase hex.
    """

    metadata_incarnation: str
    node_id: int
    store_id: int
    root_incarnation: str
    public_key: str

    def to_dict(self) -> dict[str, Any]:
        metadata_incarnation = self.metadata_incarnation

        node_id = self.node_id

        store_id = self.store_id

        root_incarnation = self.root_incarnation

        public_key = self.public_key

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "metadata_incarnation": metadata_incarnation,
                "node_id": node_id,
                "store_id": store_id,
                "root_incarnation": root_incarnation,
                "public_key": public_key,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        metadata_incarnation = d.pop("metadata_incarnation")

        node_id = d.pop("node_id")

        store_id = d.pop("store_id")

        root_incarnation = d.pop("root_incarnation")

        public_key = d.pop("public_key")

        store_root_enrollment_identity = cls(
            metadata_incarnation=metadata_incarnation,
            node_id=node_id,
            store_id=store_id,
            root_incarnation=root_incarnation,
            public_key=public_key,
        )

        return store_root_enrollment_identity
