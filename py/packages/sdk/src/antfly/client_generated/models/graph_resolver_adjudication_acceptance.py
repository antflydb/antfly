from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="GraphResolverAdjudicationAcceptance")


@_attrs_define
class GraphResolverAdjudicationAcceptance:
    """
    Attributes:
        choice (str): A choice identifier in the configured question. Accept and reject must differ.
        min_probability (float): Require this typed probability and a strict majority. Raw cosine is not a probability.
    """

    choice: str
    min_probability: float

    def to_dict(self) -> dict[str, Any]:
        choice = self.choice

        min_probability = self.min_probability

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "choice": choice,
                "min_probability": min_probability,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        choice = d.pop("choice")

        min_probability = d.pop("min_probability")

        graph_resolver_adjudication_acceptance = cls(
            choice=choice,
            min_probability=min_probability,
        )

        return graph_resolver_adjudication_acceptance
