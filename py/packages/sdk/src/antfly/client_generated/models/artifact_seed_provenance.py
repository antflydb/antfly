from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="ArtifactSeedProvenance")


@_attrs_define
class ArtifactSeedProvenance:
    """Optional caller assertion about the producer; never proof that Antfly ran it.

    Attributes:
        producer (str | Unset):
        model (str | Unset):
        revision (str | Unset):
    """

    producer: str | Unset = UNSET
    model: str | Unset = UNSET
    revision: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        producer = self.producer

        model = self.model

        revision = self.revision

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if producer is not UNSET:
            field_dict["producer"] = producer
        if model is not UNSET:
            field_dict["model"] = model
        if revision is not UNSET:
            field_dict["revision"] = revision

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        producer = d.pop("producer", UNSET)

        model = d.pop("model", UNSET)

        revision = d.pop("revision", UNSET)

        artifact_seed_provenance = cls(
            producer=producer,
            model=model,
            revision=revision,
        )

        return artifact_seed_provenance
