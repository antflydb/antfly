from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.sql_package_volatility import SQLPackageVolatility
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.sql_package_argument import SQLPackageArgument
    from ..models.sql_package_type import SQLPackageType


T = TypeVar("T", bound="SQLPackageExport")


@_attrs_define
class SQLPackageExport:
    """A pure scalar SQL function implemented by the declared immutable core WASM runtime; no host capabilities or native-
    library fallback.

        Attributes:
            name (str):
            runtime (str): Path-safe extension or package identifier.
            export_name (str): Tool selector passed to antfly_extension_call_tool.
            result (SQLPackageType):
            arguments (list[SQLPackageArgument] | Unset):
            strict (bool | Unset):  Default: False.
            volatility (SQLPackageVolatility | Unset):
    """

    name: str
    runtime: str
    export_name: str
    result: SQLPackageType
    arguments: list[SQLPackageArgument] | Unset = UNSET
    strict: bool | Unset = False
    volatility: SQLPackageVolatility | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        runtime = self.runtime

        export_name = self.export_name

        result = self.result.to_dict()

        arguments: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.arguments, Unset):
            arguments = []
            for arguments_item_data in self.arguments:
                arguments_item = arguments_item_data.to_dict()
                arguments.append(arguments_item)

        strict = self.strict

        volatility: str | Unset = UNSET
        if not isinstance(self.volatility, Unset):
            volatility = self.volatility.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "runtime": runtime,
                "export_name": export_name,
                "result": result,
            }
        )
        if arguments is not UNSET:
            field_dict["arguments"] = arguments
        if strict is not UNSET:
            field_dict["strict"] = strict
        if volatility is not UNSET:
            field_dict["volatility"] = volatility

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.sql_package_argument import SQLPackageArgument
        from ..models.sql_package_type import SQLPackageType

        d = dict(src_dict)
        name = d.pop("name")

        runtime = d.pop("runtime")

        export_name = d.pop("export_name")

        result = SQLPackageType.from_dict(d.pop("result"))

        _arguments = d.pop("arguments", UNSET)
        arguments: list[SQLPackageArgument] | Unset = UNSET
        if _arguments is not UNSET:
            arguments = []
            for arguments_item_data in _arguments:
                arguments_item = SQLPackageArgument.from_dict(arguments_item_data)

                arguments.append(arguments_item)

        strict = d.pop("strict", UNSET)

        _volatility = d.pop("volatility", UNSET)
        volatility: SQLPackageVolatility | Unset
        if isinstance(_volatility, Unset):
            volatility = UNSET
        else:
            volatility = SQLPackageVolatility(_volatility)

        sql_package_export = cls(
            name=name,
            runtime=runtime,
            export_name=export_name,
            result=result,
            arguments=arguments,
            strict=strict,
            volatility=volatility,
        )

        return sql_package_export
