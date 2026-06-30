from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_get_result_row import RowsGetResultRow


T = TypeVar("T", bound="RowsGetResult")


@_attrs_define
class RowsGetResult:
    """
    Attributes:
        identity (Any | Unset): Structured row selector. `primary` addresses declared primary-key
            tables directly. `unique` addresses a declared unique constraint through
            durable unique-owner rows. The selector is exact and accepts exactly one
            of `primary` or `unique`.
        found (bool | Unset):
        row (RowsGetResultRow | Unset):
        version (int | Unset):
        physical_key (None | str | Unset): Diagnostic storage-owned physical key. Null when a unique selector did not
            resolve. Do not persist as public row identity.
    """

    identity: Any | Unset = UNSET
    found: bool | Unset = UNSET
    row: RowsGetResultRow | Unset = UNSET
    version: int | Unset = UNSET
    physical_key: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        identity: Any | Unset
        if isinstance(self.identity, Unset):
            identity = UNSET
        else:
            identity = self.identity

        found = self.found

        row: dict[str, Any] | Unset = UNSET
        if not isinstance(self.row, Unset):
            row = self.row.to_dict()

        version = self.version

        physical_key: None | str | Unset
        if isinstance(self.physical_key, Unset):
            physical_key = UNSET
        else:
            physical_key = self.physical_key

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if identity is not UNSET:
            field_dict["identity"] = identity
        if found is not UNSET:
            field_dict["found"] = found
        if row is not UNSET:
            field_dict["row"] = row
        if version is not UNSET:
            field_dict["version"] = version
        if physical_key is not UNSET:
            field_dict["physical_key"] = physical_key

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_get_result_row import RowsGetResultRow

        d = dict(src_dict)

        def _parse_identity(data: object) -> Any | Unset:
            if isinstance(data, Unset):
                return data
            return cast(Any | Unset, data)

        identity = _parse_identity(d.pop("identity", UNSET))

        found = d.pop("found", UNSET)

        _row = d.pop("row", UNSET)
        row: RowsGetResultRow | Unset
        if isinstance(_row, Unset):
            row = UNSET
        else:
            row = RowsGetResultRow.from_dict(_row)

        version = d.pop("version", UNSET)

        def _parse_physical_key(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        physical_key = _parse_physical_key(d.pop("physical_key", UNSET))

        rows_get_result = cls(
            identity=identity,
            found=found,
            row=row,
            version=version,
            physical_key=physical_key,
        )

        rows_get_result.additional_properties = d
        return rows_get_result

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
