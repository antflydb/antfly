from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

T = TypeVar("T", bound="SQLPackageDefaultControl")


@_attrs_define
class SQLPackageDefaultControl:
    """Current global SQL default selection. Retained promotion receipts remain private to durable metadata and
    checkpoints.

        Attributes:
            name (str): Path-safe extension or package identifier.
            identity (list[int]):
            generation (int):
            default_version (str):
            default_digest (list[int] | None): Null while the initial declared target has not been registered. Explicit
                promotions always bind an immutable SHA256 target digest.
    """

    name: str
    identity: list[int]
    generation: int
    default_version: str
    default_digest: list[int] | None

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        identity = self.identity

        generation = self.generation

        default_version = self.default_version

        default_digest: list[int] | None
        if isinstance(self.default_digest, list):
            default_digest = self.default_digest

        else:
            default_digest = self.default_digest

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "identity": identity,
                "generation": generation,
                "default_version": default_version,
                "default_digest": default_digest,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        identity = cast(list[int], d.pop("identity"))

        generation = d.pop("generation")

        default_version = d.pop("default_version")

        def _parse_default_digest(data: object) -> list[int] | None:
            if data is None:
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                default_digest_type_0 = cast(list[int], data)

                return default_digest_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[int] | None, data)

        default_digest = _parse_default_digest(d.pop("default_digest"))

        sql_package_default_control = cls(
            name=name,
            identity=identity,
            generation=generation,
            default_version=default_version,
            default_digest=default_digest,
        )

        return sql_package_default_control
