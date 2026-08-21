from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.document_field_mapping_missing_null_policy import DocumentFieldMappingMissingNullPolicy
from ..models.field_mapping_type import FieldMappingType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.document_field_mapping_fields import DocumentFieldMappingFields


T = TypeVar("T", bound="DocumentFieldMapping")


@_attrs_define
class DocumentFieldMapping:
    """Executable physical mapping used by a document property's `x-antfly-field` annotation. The mapping must accept the
    JSON Schema value type. Declarations for the same dotted path across document types must normalize to an identical
    physical mapping, and present values that cannot be encoded are rejected at write admission. Mappings contributed by
    `anyOf` or `oneOf` must normalize to the same mapping in every alternative; conditional and dynamically named
    mappings are rejected.

        Attributes:
            type_ (FieldMappingType | Unset): Field types accepted by detailed `x-antfly-field` and dynamic-template
                mappings. JSON-schema-oriented aliases are normalized to Antfly's
                corresponding runtime type: number/integer to numeric, bool to boolean,
                date/timestamp to datetime, geo_point to geopoint, and geo_shape to
                geoshape.
            analyzer (str | Unset): Analyzer name for text-oriented mappings.
            index (bool | Unset): Whether to index the field (default true) Default: True.
            store (bool | Unset): Whether to store the field value (default false) Default: False.
            include_in_all (bool | Unset): Whether to include in the _all field for cross-field search Default: False.
            sortable (bool | Unset): Whether this exact scalar field can be used in order_by. Supported
                sortable mapping types are keyword, numeric/number/integer,
                boolean/bool, datetime/date/timestamp, and link. Analyzed text,
                search_as_you_type, geo, embedding, blob, html, object, and array
                fields are not directly sortable; use an exact scalar subfield such
                as title.keyword for sorted string pagination. Antfly derives the
                required typed doc values when enabled.
                 Default: False.
            missing_null_policy (DocumentFieldMappingMissingNullPolicy | Unset): Missing/null sort policy. The current
                production policy rejects missing or null native sort values. Default:
                DocumentFieldMappingMissingNullPolicy.MISSING_REJECTED.
            fields (DocumentFieldMappingFields | Unset): Named one-level multifields emitted from this property's value. For
                example, a text title can expose a sortable keyword subfield.
    """

    type_: FieldMappingType | Unset = UNSET
    analyzer: str | Unset = UNSET
    index: bool | Unset = True
    store: bool | Unset = False
    include_in_all: bool | Unset = False
    sortable: bool | Unset = False
    missing_null_policy: DocumentFieldMappingMissingNullPolicy | Unset = (
        DocumentFieldMappingMissingNullPolicy.MISSING_REJECTED
    )
    fields: DocumentFieldMappingFields | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        type_: str | Unset = UNSET
        if not isinstance(self.type_, Unset):
            type_ = self.type_.value

        analyzer = self.analyzer

        index = self.index

        store = self.store

        include_in_all = self.include_in_all

        sortable = self.sortable

        missing_null_policy: str | Unset = UNSET
        if not isinstance(self.missing_null_policy, Unset):
            missing_null_policy = self.missing_null_policy.value

        fields: dict[str, Any] | Unset = UNSET
        if not isinstance(self.fields, Unset):
            fields = self.fields.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if type_ is not UNSET:
            field_dict["type"] = type_
        if analyzer is not UNSET:
            field_dict["analyzer"] = analyzer
        if index is not UNSET:
            field_dict["index"] = index
        if store is not UNSET:
            field_dict["store"] = store
        if include_in_all is not UNSET:
            field_dict["include_in_all"] = include_in_all
        if sortable is not UNSET:
            field_dict["sortable"] = sortable
        if missing_null_policy is not UNSET:
            field_dict["missing_null_policy"] = missing_null_policy
        if fields is not UNSET:
            field_dict["fields"] = fields

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.document_field_mapping_fields import DocumentFieldMappingFields

        d = dict(src_dict)
        _type_ = d.pop("type", UNSET)
        type_: FieldMappingType | Unset
        if isinstance(_type_, Unset):
            type_ = UNSET
        else:
            type_ = FieldMappingType(_type_)

        analyzer = d.pop("analyzer", UNSET)

        index = d.pop("index", UNSET)

        store = d.pop("store", UNSET)

        include_in_all = d.pop("include_in_all", UNSET)

        sortable = d.pop("sortable", UNSET)

        _missing_null_policy = d.pop("missing_null_policy", UNSET)
        missing_null_policy: DocumentFieldMappingMissingNullPolicy | Unset
        if isinstance(_missing_null_policy, Unset):
            missing_null_policy = UNSET
        else:
            missing_null_policy = DocumentFieldMappingMissingNullPolicy(_missing_null_policy)

        _fields = d.pop("fields", UNSET)
        fields: DocumentFieldMappingFields | Unset
        if isinstance(_fields, Unset):
            fields = UNSET
        else:
            fields = DocumentFieldMappingFields.from_dict(_fields)

        document_field_mapping = cls(
            type_=type_,
            analyzer=analyzer,
            index=index,
            store=store,
            include_in_all=include_in_all,
            sortable=sortable,
            missing_null_policy=missing_null_policy,
            fields=fields,
        )

        return document_field_mapping
