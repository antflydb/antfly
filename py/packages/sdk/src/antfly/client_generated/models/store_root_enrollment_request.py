from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.store_root_enrollment_identity import StoreRootEnrollmentIdentity


T = TypeVar("T", bound="StoreRootEnrollmentRequest")


@_attrs_define
class StoreRootEnrollmentRequest:
    """
    Attributes:
        identity (StoreRootEnrollmentIdentity):
        signature (str): Ed25519 proof-of-possession signature in lowercase hex.
    """

    identity: StoreRootEnrollmentIdentity
    signature: str

    def to_dict(self) -> dict[str, Any]:
        identity = self.identity.to_dict()

        signature = self.signature

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "identity": identity,
                "signature": signature,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.store_root_enrollment_identity import StoreRootEnrollmentIdentity

        d = dict(src_dict)
        identity = StoreRootEnrollmentIdentity.from_dict(d.pop("identity"))

        signature = d.pop("signature")

        store_root_enrollment_request = cls(
            identity=identity,
            signature=signature,
        )

        return store_root_enrollment_request
