from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.dynamic_template_match_mapping_type import DynamicTemplateMatchMappingType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.template_field_mapping import TemplateFieldMapping


T = TypeVar("T", bound="DynamicTemplate")


@_attrs_define
class DynamicTemplate:
    """A rule for mapping dynamically detected fields. Templates are checked in order
    and the first matching template's mapping is used.

        Attributes:
            name (Union[Unset, str]): Optional identifier for the template (useful for debugging)
            match (Union[Unset, str]): Glob pattern for field name (last path element).
                Supports * and ** wildcards. Example: "*_text" matches "title_text", "body_text"
            unmatch (Union[Unset, str]): Exclusion pattern for field name. If it matches, the template is skipped.
                Example: "skip_*" would exclude fields like "skip_this"
            path_match (Union[Unset, str]): Glob pattern for the full dotted path. Supports ** for matching multiple
                segments.
                Example: "metadata.**" matches "metadata.author", "metadata.tags.primary"
            path_unmatch (Union[Unset, str]): Path exclusion pattern. If it matches the full path, the template is skipped.
            match_mapping_type (Union[Unset, DynamicTemplateMatchMappingType]): Filter by detected JSON type
            mapping (Union[Unset, TemplateFieldMapping]): Field mapping to apply when a dynamic template matches
    """

    name: Union[Unset, str] = UNSET
    match: Union[Unset, str] = UNSET
    unmatch: Union[Unset, str] = UNSET
    path_match: Union[Unset, str] = UNSET
    path_unmatch: Union[Unset, str] = UNSET
    match_mapping_type: Union[Unset, DynamicTemplateMatchMappingType] = UNSET
    mapping: Union[Unset, "TemplateFieldMapping"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        match = self.match

        unmatch = self.unmatch

        path_match = self.path_match

        path_unmatch = self.path_unmatch

        match_mapping_type: Union[Unset, str] = UNSET
        if not isinstance(self.match_mapping_type, Unset):
            match_mapping_type = self.match_mapping_type.value

        mapping: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.mapping, Unset):
            mapping = self.mapping.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if name is not UNSET:
            field_dict["name"] = name
        if match is not UNSET:
            field_dict["match"] = match
        if unmatch is not UNSET:
            field_dict["unmatch"] = unmatch
        if path_match is not UNSET:
            field_dict["path_match"] = path_match
        if path_unmatch is not UNSET:
            field_dict["path_unmatch"] = path_unmatch
        if match_mapping_type is not UNSET:
            field_dict["match_mapping_type"] = match_mapping_type
        if mapping is not UNSET:
            field_dict["mapping"] = mapping

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.template_field_mapping import TemplateFieldMapping

        d = dict(src_dict)
        name = d.pop("name", UNSET)

        match = d.pop("match", UNSET)

        unmatch = d.pop("unmatch", UNSET)

        path_match = d.pop("path_match", UNSET)

        path_unmatch = d.pop("path_unmatch", UNSET)

        _match_mapping_type = d.pop("match_mapping_type", UNSET)
        match_mapping_type: Union[Unset, DynamicTemplateMatchMappingType]
        if isinstance(_match_mapping_type, Unset):
            match_mapping_type = UNSET
        else:
            match_mapping_type = DynamicTemplateMatchMappingType(_match_mapping_type)

        _mapping = d.pop("mapping", UNSET)
        mapping: Union[Unset, TemplateFieldMapping]
        if isinstance(_mapping, Unset):
            mapping = UNSET
        else:
            mapping = TemplateFieldMapping.from_dict(_mapping)

        dynamic_template = cls(
            name=name,
            match=match,
            unmatch=unmatch,
            path_match=path_match,
            path_unmatch=path_unmatch,
            match_mapping_type=match_mapping_type,
            mapping=mapping,
        )

        dynamic_template.additional_properties = d
        return dynamic_template

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
