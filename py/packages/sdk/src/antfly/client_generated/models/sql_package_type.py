from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.sql_package_scalar_type import SQLPackageScalarType
from ..types import UNSET, Unset

T = TypeVar("T", bound="SQLPackageType")


@_attrs_define
class SQLPackageType:
    """
    Attributes:
        scalar (SQLPackageScalarType): Exact native SQL callable scalar domains, preserving integer widths and
            temporal/range identities.
        array (bool | Unset): Arrays support text, uuid, int16, int32, int64, float32, float64, boolean, jsonb and
            numeric. Other scalar domains require false. Default: False.
    """

    scalar: SQLPackageScalarType
    array: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        scalar = self.scalar.value

        array = self.array

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "scalar": scalar,
            }
        )
        if array is not UNSET:
            field_dict["array"] = array

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        scalar = SQLPackageScalarType(d.pop("scalar"))

        array = d.pop("array", UNSET)

        sql_package_type = cls(
            scalar=scalar,
            array=array,
        )

        return sql_package_type
