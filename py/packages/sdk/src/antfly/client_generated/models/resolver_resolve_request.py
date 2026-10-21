from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.resolver_resolve_request_extraction import ResolverResolveRequestExtraction


T = TypeVar("T", bound="ResolverResolveRequest")


@_attrs_define
class ResolverResolveRequest:
    """Supply exactly one of text or extraction. Text uses this resolver's configured extractor. Resolution is ephemeral
    and uses the serving policy, candidate namespace and typed adjudicator without publishing artifacts or entities.

        Attributes:
            text (str | Unset):
            extraction (ResolverResolveRequestExtraction | Unset):
    """

    text: str | Unset = UNSET
    extraction: ResolverResolveRequestExtraction | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        text = self.text

        extraction: dict[str, Any] | Unset = UNSET
        if not isinstance(self.extraction, Unset):
            extraction = self.extraction.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if text is not UNSET:
            field_dict["text"] = text
        if extraction is not UNSET:
            field_dict["extraction"] = extraction

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.resolver_resolve_request_extraction import ResolverResolveRequestExtraction

        d = dict(src_dict)
        text = d.pop("text", UNSET)

        _extraction = d.pop("extraction", UNSET)
        extraction: ResolverResolveRequestExtraction | Unset
        if isinstance(_extraction, Unset):
            extraction = UNSET
        else:
            extraction = ResolverResolveRequestExtraction.from_dict(_extraction)

        resolver_resolve_request = cls(
            text=text,
            extraction=extraction,
        )

        return resolver_resolve_request
