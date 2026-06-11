from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="RowsGetRequest")


@_attrs_define
class RowsGetRequest:
    """
    Attributes:
        keys (list[Any]):
        include_physical_key (bool | Unset): Include the diagnostic storage-owned physical key in each result. Default:
            False.
    """

    keys: list[Any]
    include_physical_key: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        keys = []
        for keys_item_data in self.keys:
            keys_item: Any
            keys_item = keys_item_data
            keys.append(keys_item)

        include_physical_key = self.include_physical_key

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "keys": keys,
            }
        )
        if include_physical_key is not UNSET:
            field_dict["include_physical_key"] = include_physical_key

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        keys = []
        _keys = d.pop("keys")
        for keys_item_data in _keys:

            def _parse_keys_item(data: object) -> Any:
                return cast(Any, data)

            keys_item = _parse_keys_item(keys_item_data)

            keys.append(keys_item)

        include_physical_key = d.pop("include_physical_key", UNSET)

        rows_get_request = cls(
            keys=keys,
            include_physical_key=include_physical_key,
        )

        return rows_get_request
