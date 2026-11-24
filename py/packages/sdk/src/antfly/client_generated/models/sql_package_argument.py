from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.sql_package_type import SQLPackageType


T = TypeVar("T", bound="SQLPackageArgument")


@_attrs_define
class SQLPackageArgument:
    """
    Attributes:
        type_ (SQLPackageType):
        name (None | str | Unset):
    """

    type_: SQLPackageType
    name: None | str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.to_dict()

        name: None | str | Unset
        if isinstance(self.name, Unset):
            name = UNSET
        else:
            name = self.name

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
            }
        )
        if name is not UNSET:
            field_dict["name"] = name

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.sql_package_type import SQLPackageType

        d = dict(src_dict)
        type_ = SQLPackageType.from_dict(d.pop("type"))

        def _parse_name(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        name = _parse_name(d.pop("name", UNSET))

        sql_package_argument = cls(
            type_=type_,
            name=name,
        )

        return sql_package_argument
