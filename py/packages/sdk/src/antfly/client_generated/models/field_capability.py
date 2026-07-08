from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.antfly_type import AntflyType
from ..models.field_capability_index_sort_order import FieldCapabilityIndexSortOrder
from ..models.field_capability_query_modes_item import FieldCapabilityQueryModesItem
from ..models.field_capability_sort_lifecycle_state import FieldCapabilitySortLifecycleState
from ..types import UNSET, Unset

T = TypeVar("T", bound="FieldCapability")


@_attrs_define
class FieldCapability:
    """Public field capability derived from Antfly schema mappings and observed dynamic field metadata.

    Attributes:
        type_ (AntflyType):
        query_modes (list[FieldCapabilityQueryModesItem]): Query modes supported by this concrete field variant. These
            modes are derived from
            Antfly field types such as `text`, `keyword`, `datetime`, `geopoint`, and
            `search_as_you_type`; they are not separate schema toggles.
        sortable (bool): Whether this concrete field is declared sortable in the effective
            capability model. Public exact order_by accepts it only when
            sort_lifecycle_state is queryable or accelerated.
        provenance (str): Capability source, such as reserved, document_schema, dynamic_template, or observed_dynamic.
        missing_null_policy (str): Current missing/null handling policy for this field.
        sort_lifecycle_state (FieldCapabilitySortLifecycleState): Operational lifecycle state for exact sort use.
            Queryable fields are accepted by public exact sort; accelerated fields are queryable and participate in the
            configured index_sort tuple.
        name (str | Unset): Mapping or dynamic-template name, when applicable.
        field (str | Unset): Concrete query field path when known. Pattern-only dynamic templates may omit this.
        path_pattern (str | Unset): Dynamic-template path_match pattern, when applicable.
        field_pattern (str | Unset): Dynamic-template match pattern, when applicable.
        match_mapping_type (str | Unset): Dynamic-template match_mapping_type, when applicable.
        emitted_name (str | Unset): Physical emitted field name for schema-derived text fields, when different from the
            logical path.
        document_schema (str | Unset): Document schema that produced this capability, when applicable.
        analyzer (str | Unset): Analyzer name for text/searchable fields, when applicable.
        index_sort_position (int | Unset): Zero-based position in the table index_sort tuple when this field
            participates.
        index_sort_order (FieldCapabilityIndexSortOrder | Unset): Sort direction in the table index_sort tuple when this
            field participates.
    """

    type_: AntflyType
    query_modes: list[FieldCapabilityQueryModesItem]
    sortable: bool
    provenance: str
    missing_null_policy: str
    sort_lifecycle_state: FieldCapabilitySortLifecycleState
    name: str | Unset = UNSET
    field: str | Unset = UNSET
    path_pattern: str | Unset = UNSET
    field_pattern: str | Unset = UNSET
    match_mapping_type: str | Unset = UNSET
    emitted_name: str | Unset = UNSET
    document_schema: str | Unset = UNSET
    analyzer: str | Unset = UNSET
    index_sort_position: int | Unset = UNSET
    index_sort_order: FieldCapabilityIndexSortOrder | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        query_modes = []
        for query_modes_item_data in self.query_modes:
            query_modes_item = query_modes_item_data.value
            query_modes.append(query_modes_item)

        sortable = self.sortable

        provenance = self.provenance

        missing_null_policy = self.missing_null_policy

        sort_lifecycle_state = self.sort_lifecycle_state.value

        name = self.name

        field = self.field

        path_pattern = self.path_pattern

        field_pattern = self.field_pattern

        match_mapping_type = self.match_mapping_type

        emitted_name = self.emitted_name

        document_schema = self.document_schema

        analyzer = self.analyzer

        index_sort_position = self.index_sort_position

        index_sort_order: str | Unset = UNSET
        if not isinstance(self.index_sort_order, Unset):
            index_sort_order = self.index_sort_order.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
                "query_modes": query_modes,
                "sortable": sortable,
                "provenance": provenance,
                "missing_null_policy": missing_null_policy,
                "sort_lifecycle_state": sort_lifecycle_state,
            }
        )
        if name is not UNSET:
            field_dict["name"] = name
        if field is not UNSET:
            field_dict["field"] = field
        if path_pattern is not UNSET:
            field_dict["path_pattern"] = path_pattern
        if field_pattern is not UNSET:
            field_dict["field_pattern"] = field_pattern
        if match_mapping_type is not UNSET:
            field_dict["match_mapping_type"] = match_mapping_type
        if emitted_name is not UNSET:
            field_dict["emitted_name"] = emitted_name
        if document_schema is not UNSET:
            field_dict["document_schema"] = document_schema
        if analyzer is not UNSET:
            field_dict["analyzer"] = analyzer
        if index_sort_position is not UNSET:
            field_dict["index_sort_position"] = index_sort_position
        if index_sort_order is not UNSET:
            field_dict["index_sort_order"] = index_sort_order

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = AntflyType(d.pop("type"))

        query_modes = []
        _query_modes = d.pop("query_modes")
        for query_modes_item_data in _query_modes:
            query_modes_item = FieldCapabilityQueryModesItem(query_modes_item_data)

            query_modes.append(query_modes_item)

        sortable = d.pop("sortable")

        provenance = d.pop("provenance")

        missing_null_policy = d.pop("missing_null_policy")

        sort_lifecycle_state = FieldCapabilitySortLifecycleState(d.pop("sort_lifecycle_state"))

        name = d.pop("name", UNSET)

        field = d.pop("field", UNSET)

        path_pattern = d.pop("path_pattern", UNSET)

        field_pattern = d.pop("field_pattern", UNSET)

        match_mapping_type = d.pop("match_mapping_type", UNSET)

        emitted_name = d.pop("emitted_name", UNSET)

        document_schema = d.pop("document_schema", UNSET)

        analyzer = d.pop("analyzer", UNSET)

        index_sort_position = d.pop("index_sort_position", UNSET)

        _index_sort_order = d.pop("index_sort_order", UNSET)
        index_sort_order: FieldCapabilityIndexSortOrder | Unset
        if isinstance(_index_sort_order, Unset):
            index_sort_order = UNSET
        else:
            index_sort_order = FieldCapabilityIndexSortOrder(_index_sort_order)

        field_capability = cls(
            type_=type_,
            query_modes=query_modes,
            sortable=sortable,
            provenance=provenance,
            missing_null_policy=missing_null_policy,
            sort_lifecycle_state=sort_lifecycle_state,
            name=name,
            field=field,
            path_pattern=path_pattern,
            field_pattern=field_pattern,
            match_mapping_type=match_mapping_type,
            emitted_name=emitted_name,
            document_schema=document_schema,
            analyzer=analyzer,
            index_sort_position=index_sort_position,
            index_sort_order=index_sort_order,
        )

        field_capability.additional_properties = d
        return field_capability

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
