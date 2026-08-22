from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.create_algebraic_index_request_type import CreateAlgebraicIndexRequestType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.enrichment_config import EnrichmentConfig


T = TypeVar("T", bound="CreateAlgebraicIndexRequest")


@_attrs_define
class CreateAlgebraicIndexRequest:
    """Create a schema-derived algebraic index.

    Attributes:
        type_ (CreateAlgebraicIndexRequestType):
        description (str | Unset): Optional description of the index and its purpose
        version (int | Unset): Version of the index implementation. Defaults to 0. Default: 0.
        enrichments (list[EnrichmentConfig] | Unset): Inline managed enrichment definitions required by this index.
        derive_from_schema (bool | Unset): When true, derive the algebraic capability sidecar from the table schema.
            Internal fields and materialization definitions are not public API.
    """

    type_: CreateAlgebraicIndexRequestType
    description: str | Unset = UNSET
    version: int | Unset = 0
    enrichments: list[EnrichmentConfig] | Unset = UNSET
    derive_from_schema: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
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
        type_ = CreateAlgebraicIndexRequestType(d.pop("type"))

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

        create_algebraic_index_request = cls(
            type_=type_,
            description=description,
            version=version,
            enrichments=enrichments,
            derive_from_schema=derive_from_schema,
        )

        create_algebraic_index_request.additional_properties = d
        return create_algebraic_index_request

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
