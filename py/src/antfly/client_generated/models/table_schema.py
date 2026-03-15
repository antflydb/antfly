from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.dynamic_template import DynamicTemplate
    from ..models.table_schema_document_schemas import TableSchemaDocumentSchemas


T = TypeVar("T", bound="TableSchema")


@_attrs_define
class TableSchema:
    """Schema definition for a table with multiple document types

    Attributes:
        version (Union[Unset, int]): Version of the schema. Used for migrations.
        default_type (Union[Unset, str]): Default type to use from the document_types.
        enforce_types (Union[Unset, bool]): Whether to enforce that documents must match one of the provided document
            types.
            If false, documents not matching any type will be accepted but not indexed.
        document_schemas (Union[Unset, TableSchemaDocumentSchemas]): A map of type names to their document json schemas.
        ttl_field (Union[Unset, str]): The field containing the timestamp for TTL expiration (optional).
            Defaults to "_timestamp" if ttl_duration is specified but ttl_field is not.
        ttl_duration (Union[Unset, str]): The duration after which documents should expire, based on the ttl_field
            timestamp (optional).
            Uses Go duration format (e.g., '24h', '7d', '168h').
        dynamic_templates (Union[Unset, list['DynamicTemplate']]): Rules for mapping dynamically detected fields. When a
            document contains fields
            that don't have explicit mappings and dynamic mapping is enabled, templates are
            evaluated in order to determine how those fields should be indexed.
    """

    version: Union[Unset, int] = UNSET
    default_type: Union[Unset, str] = UNSET
    enforce_types: Union[Unset, bool] = UNSET
    document_schemas: Union[Unset, "TableSchemaDocumentSchemas"] = UNSET
    ttl_field: Union[Unset, str] = UNSET
    ttl_duration: Union[Unset, str] = UNSET
    dynamic_templates: Union[Unset, list["DynamicTemplate"]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        version = self.version

        default_type = self.default_type

        enforce_types = self.enforce_types

        document_schemas: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.document_schemas, Unset):
            document_schemas = self.document_schemas.to_dict()

        ttl_field = self.ttl_field

        ttl_duration = self.ttl_duration

        dynamic_templates: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.dynamic_templates, Unset):
            dynamic_templates = []
            for dynamic_templates_item_data in self.dynamic_templates:
                dynamic_templates_item = dynamic_templates_item_data.to_dict()
                dynamic_templates.append(dynamic_templates_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if version is not UNSET:
            field_dict["version"] = version
        if default_type is not UNSET:
            field_dict["default_type"] = default_type
        if enforce_types is not UNSET:
            field_dict["enforce_types"] = enforce_types
        if document_schemas is not UNSET:
            field_dict["document_schemas"] = document_schemas
        if ttl_field is not UNSET:
            field_dict["ttl_field"] = ttl_field
        if ttl_duration is not UNSET:
            field_dict["ttl_duration"] = ttl_duration
        if dynamic_templates is not UNSET:
            field_dict["dynamic_templates"] = dynamic_templates

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.dynamic_template import DynamicTemplate
        from ..models.table_schema_document_schemas import TableSchemaDocumentSchemas

        d = dict(src_dict)
        version = d.pop("version", UNSET)

        default_type = d.pop("default_type", UNSET)

        enforce_types = d.pop("enforce_types", UNSET)

        _document_schemas = d.pop("document_schemas", UNSET)
        document_schemas: Union[Unset, TableSchemaDocumentSchemas]
        if isinstance(_document_schemas, Unset):
            document_schemas = UNSET
        else:
            document_schemas = TableSchemaDocumentSchemas.from_dict(_document_schemas)

        ttl_field = d.pop("ttl_field", UNSET)

        ttl_duration = d.pop("ttl_duration", UNSET)

        dynamic_templates = []
        _dynamic_templates = d.pop("dynamic_templates", UNSET)
        for dynamic_templates_item_data in _dynamic_templates or []:
            dynamic_templates_item = DynamicTemplate.from_dict(dynamic_templates_item_data)

            dynamic_templates.append(dynamic_templates_item)

        table_schema = cls(
            version=version,
            default_type=default_type,
            enforce_types=enforce_types,
            document_schemas=document_schemas,
            ttl_field=ttl_field,
            ttl_duration=ttl_duration,
            dynamic_templates=dynamic_templates,
        )

        table_schema.additional_properties = d
        return table_schema

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
