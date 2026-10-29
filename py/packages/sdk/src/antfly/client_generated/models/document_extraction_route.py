from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.document_extraction_route_extractor import DocumentExtractionRouteExtractor
    from ..models.document_extraction_route_match import DocumentExtractionRouteMatch


T = TypeVar("T", bound="DocumentExtractionRoute")


@_attrs_define
class DocumentExtractionRoute:
    """
    Attributes:
        extractor (DocumentExtractionRouteExtractor):
        match (DocumentExtractionRouteMatch | Unset): Conditions a source must meet. Every condition given must match.
    """

    extractor: DocumentExtractionRouteExtractor
    match: DocumentExtractionRouteMatch | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        extractor = self.extractor.to_dict()

        match: dict[str, Any] | Unset = UNSET
        if not isinstance(self.match, Unset):
            match = self.match.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "extractor": extractor,
            }
        )
        if match is not UNSET:
            field_dict["match"] = match

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.document_extraction_route_extractor import DocumentExtractionRouteExtractor
        from ..models.document_extraction_route_match import DocumentExtractionRouteMatch

        d = dict(src_dict)
        extractor = DocumentExtractionRouteExtractor.from_dict(d.pop("extractor"))

        _match = d.pop("match", UNSET)
        match: DocumentExtractionRouteMatch | Unset
        if isinstance(_match, Unset):
            match = UNSET
        else:
            match = DocumentExtractionRouteMatch.from_dict(_match)

        document_extraction_route = cls(
            extractor=extractor,
            match=match,
        )

        document_extraction_route.additional_properties = d
        return document_extraction_route

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
