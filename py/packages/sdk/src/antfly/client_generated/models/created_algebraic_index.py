from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.created_algebraic_index_type import CreatedAlgebraicIndexType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.enrichment_config import EnrichmentConfig


T = TypeVar("T", bound="CreatedAlgebraicIndex")


@_attrs_define
class CreatedAlgebraicIndex:
    """Normalized effective schema-derived algebraic index configuration returned after creation.

    Attributes:
        name (str): Name of the created index
        type_ (CreatedAlgebraicIndexType):
        description (str | Unset): Optional description of the index and its purpose
        version (int | Unset): Version of the index implementation. Defaults to 0. Default: 0.
        enrichments (list[EnrichmentConfig] | Unset): Normalized inline managed enrichment definitions required by this
            index.
        derive_from_schema (bool | Unset): When true, derive the algebraic capability sidecar from the table schema.
            Internal fields and materialization definitions are not public API.
    """

    name: str
    type_: CreatedAlgebraicIndexType
    description: str | Unset = UNSET
    version: int | Unset = 0
    enrichments: list[EnrichmentConfig] | Unset = UNSET
    derive_from_schema: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        type_ = self.type_.value

        description = self.description

        version = self.version

        enrichments: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.enrichments, Unset):
            enrichments = []
            for enrichments_item_data in self.enrichments:
                enrichments_item = enrichments_item_data.to_dict()
                enrichments.append(enrichments_item)

        derive_from_schema = self.derive_from_schema

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "type": type_,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description
        if version is not UNSET:
            field_dict["version"] = version
        if enrichments is not UNSET:
            field_dict["enrichments"] = enrichments
        if derive_from_schema is not UNSET:
            field_dict["derive_from_schema"] = derive_from_schema

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.enrichment_config import EnrichmentConfig

        d = dict(src_dict)
        name = d.pop("name")

        type_ = CreatedAlgebraicIndexType(d.pop("type"))

        description = d.pop("description", UNSET)

        version = d.pop("version", UNSET)

        _enrichments = d.pop("enrichments", UNSET)
        enrichments: list[EnrichmentConfig] | Unset = UNSET
        if _enrichments is not UNSET:
            enrichments = []
            for enrichments_item_data in _enrichments:
                enrichments_item = EnrichmentConfig.from_dict(enrichments_item_data)

                enrichments.append(enrichments_item)

        derive_from_schema = d.pop("derive_from_schema", UNSET)

        created_algebraic_index = cls(
            name=name,
            type_=type_,
            description=description,
            version=version,
            enrichments=enrichments,
            derive_from_schema=derive_from_schema,
        )

        created_algebraic_index.additional_properties = d
        return created_algebraic_index

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
