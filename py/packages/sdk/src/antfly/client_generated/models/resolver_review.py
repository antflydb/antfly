from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.resolver_adjudication_evidence import ResolverAdjudicationEvidence


T = TypeVar("T", bound="ResolverReview")


@_attrs_define
class ResolverReview:
    """
    Attributes:
        id (str): Opaque revision-bound review identity; treat it as a single URL-encoded path component.
        source_key (str):
        local_id (str):
        label (str):
        surface_form (str):
        key (str):
        confidence (float):
        table (str | Unset):
        config_generation (int | Unset):
        adjudication (ResolverAdjudicationEvidence | Unset):
    """

    id: str
    source_key: str
    local_id: str
    label: str
    surface_form: str
    key: str
    confidence: float
    table: str | Unset = UNSET
    config_generation: int | Unset = UNSET
    adjudication: ResolverAdjudicationEvidence | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        source_key = self.source_key

        local_id = self.local_id

        label = self.label

        surface_form = self.surface_form

        key = self.key

        confidence = self.confidence

        table = self.table

        config_generation = self.config_generation

        adjudication: dict[str, Any] | Unset = UNSET
        if not isinstance(self.adjudication, Unset):
            adjudication = self.adjudication.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "source_key": source_key,
                "local_id": local_id,
                "label": label,
                "surface_form": surface_form,
                "key": key,
                "confidence": confidence,
            }
        )
        if table is not UNSET:
            field_dict["table"] = table
        if config_generation is not UNSET:
            field_dict["config_generation"] = config_generation
        if adjudication is not UNSET:
            field_dict["adjudication"] = adjudication

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.resolver_adjudication_evidence import ResolverAdjudicationEvidence

        d = dict(src_dict)
        id = d.pop("id")

        source_key = d.pop("source_key")

        local_id = d.pop("local_id")

        label = d.pop("label")

        surface_form = d.pop("surface_form")

        key = d.pop("key")

        confidence = d.pop("confidence")

        table = d.pop("table", UNSET)

        config_generation = d.pop("config_generation", UNSET)

        _adjudication = d.pop("adjudication", UNSET)
        adjudication: ResolverAdjudicationEvidence | Unset
        if isinstance(_adjudication, Unset):
            adjudication = UNSET
        else:
            adjudication = ResolverAdjudicationEvidence.from_dict(_adjudication)

        resolver_review = cls(
            id=id,
            source_key=source_key,
            local_id=local_id,
            label=label,
            surface_form=surface_form,
            key=key,
            confidence=confidence,
            table=table,
            config_generation=config_generation,
            adjudication=adjudication,
        )

        resolver_review.additional_properties = d
        return resolver_review

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
