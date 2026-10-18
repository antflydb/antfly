from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphResolverAdjudicationConfigDescriptionInputs")


@_attrs_define
class GraphResolverAdjudicationConfigDescriptionInputs:
    """Optional asset bindings. Resolution reads available data without waiting for promotion or producers.

    Attributes:
        mention_artifact (str | Unset): Source-document asset containing a JSON object mapping local mention ids to
            description strings. Publication re-drives asset-scoped resolution.
        candidate_artifact (str | Unset): Candidate-document text asset, such as a generated summary. Changes affect
            future decisions; explicitly re-resolve to revisit committed evidence.
    """

    mention_artifact: str | Unset = UNSET
    candidate_artifact: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        mention_artifact = self.mention_artifact

        candidate_artifact = self.candidate_artifact

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if mention_artifact is not UNSET:
            field_dict["mention_artifact"] = mention_artifact
        if candidate_artifact is not UNSET:
            field_dict["candidate_artifact"] = candidate_artifact

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        mention_artifact = d.pop("mention_artifact", UNSET)

        candidate_artifact = d.pop("candidate_artifact", UNSET)

        graph_resolver_adjudication_config_description_inputs = cls(
            mention_artifact=mention_artifact,
            candidate_artifact=candidate_artifact,
        )

        return graph_resolver_adjudication_config_description_inputs
