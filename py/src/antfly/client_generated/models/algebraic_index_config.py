from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="AlgebraicIndexConfig")


@_attrs_define
class AlgebraicIndexConfig:
    """Schema-derived algebraic sidecar configuration. Public requests may opt into schema derivation, while
    materializations remain engine-owned.

        Attributes:
            derive_from_schema (Union[Unset, bool]): When true, derive the algebraic capability sidecar from the table
                schema. Internal fields and materialization definitions are not public API.
    """

    derive_from_schema: Union[Unset, bool] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        derive_from_schema = self.derive_from_schema

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if derive_from_schema is not UNSET:
            field_dict["derive_from_schema"] = derive_from_schema

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        derive_from_schema = d.pop("derive_from_schema", UNSET)

        algebraic_index_config = cls(
            derive_from_schema=derive_from_schema,
        )

        algebraic_index_config.additional_properties = d
        return algebraic_index_config

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
