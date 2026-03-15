from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.document_schema_schema import DocumentSchemaSchema


T = TypeVar("T", bound="DocumentSchema")


@_attrs_define
class DocumentSchema:
    """Defines the structure of a document type

    Attributes:
        description (Union[Unset, str]): A description of the document type.
        schema (Union[Unset, DocumentSchemaSchema]): A valid JSON Schema defining the document's structure.
            This is used to infer indexing rules and field types.
    """

    description: Union[Unset, str] = UNSET
    schema: Union[Unset, "DocumentSchemaSchema"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        description = self.description

        schema: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.schema, Unset):
            schema = self.schema.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if description is not UNSET:
            field_dict["description"] = description
        if schema is not UNSET:
            field_dict["schema"] = schema

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.document_schema_schema import DocumentSchemaSchema

        d = dict(src_dict)
        description = d.pop("description", UNSET)

        _schema = d.pop("schema", UNSET)
        schema: Union[Unset, DocumentSchemaSchema]
        if isinstance(_schema, Unset):
            schema = UNSET
        else:
            schema = DocumentSchemaSchema.from_dict(_schema)

        document_schema = cls(
            description=description,
            schema=schema,
        )

        document_schema.additional_properties = d
        return document_schema

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
