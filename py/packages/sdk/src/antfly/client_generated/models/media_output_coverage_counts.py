from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.media_output_coverage_values import MediaOutputCoverageValues


T = TypeVar("T", bound="MediaOutputCoverageCounts")


@_attrs_define
class MediaOutputCoverageCounts:
    """Cumulative durably accepted output coverage by media-output kind. Per-output names, source clocks, and current-
    document status remain on the named output assets.

        Attributes:
            embedding (MediaOutputCoverageValues | Unset):
            transcribe (MediaOutputCoverageValues | Unset):
            classify (MediaOutputCoverageValues | Unset):
    """

    embedding: MediaOutputCoverageValues | Unset = UNSET
    transcribe: MediaOutputCoverageValues | Unset = UNSET
    classify: MediaOutputCoverageValues | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        embedding: dict[str, Any] | Unset = UNSET
        if not isinstance(self.embedding, Unset):
            embedding = self.embedding.to_dict()

        transcribe: dict[str, Any] | Unset = UNSET
        if not isinstance(self.transcribe, Unset):
            transcribe = self.transcribe.to_dict()

        classify: dict[str, Any] | Unset = UNSET
        if not isinstance(self.classify, Unset):
            classify = self.classify.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if embedding is not UNSET:
            field_dict["embedding"] = embedding
        if transcribe is not UNSET:
            field_dict["transcribe"] = transcribe
        if classify is not UNSET:
            field_dict["classify"] = classify

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.media_output_coverage_values import MediaOutputCoverageValues

        d = dict(src_dict)
        _embedding = d.pop("embedding", UNSET)
        embedding: MediaOutputCoverageValues | Unset
        if isinstance(_embedding, Unset):
            embedding = UNSET
        else:
            embedding = MediaOutputCoverageValues.from_dict(_embedding)

        _transcribe = d.pop("transcribe", UNSET)
        transcribe: MediaOutputCoverageValues | Unset
        if isinstance(_transcribe, Unset):
            transcribe = UNSET
        else:
            transcribe = MediaOutputCoverageValues.from_dict(_transcribe)

        _classify = d.pop("classify", UNSET)
        classify: MediaOutputCoverageValues | Unset
        if isinstance(_classify, Unset):
            classify = UNSET
        else:
            classify = MediaOutputCoverageValues.from_dict(_classify)

        media_output_coverage_counts = cls(
            embedding=embedding,
            transcribe=transcribe,
            classify=classify,
        )

        media_output_coverage_counts.additional_properties = d
        return media_output_coverage_counts

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
