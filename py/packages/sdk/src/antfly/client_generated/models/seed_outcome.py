from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.seed_outcome_existing_origin import SeedOutcomeExistingOrigin
from ..models.seed_outcome_outcome import SeedOutcomeOutcome
from ..models.seed_outcome_via import SeedOutcomeVia
from ..types import UNSET, Unset

T = TypeVar("T", bound="SeedOutcome")


@_attrs_define
class SeedOutcome:
    """
    Attributes:
        key (str):
        outcome (SeedOutcomeOutcome): - `accepted`: the seed becomes the output.
            - `unchanged`: identical output is already accepted for this input.
            - `conflict`: different output is already accepted (kept).
            - `replaced`: different output is replaced by the seed.
            - `deleted`: a `null` seed drops the accepted output.
            - `missing`: no seed was supplied; the producer will run.
            - `pending`: no seed was supplied and the field mapping waits for
              one (`on_missing: pending`); nothing generates it.
            - `rejected`: the write would fail with `error`.
        name (str | Unset): Enrichment name. Absent for items rejected as a whole.
        existing_origin (SeedOutcomeExistingOrigin | Unset):
        via (SeedOutcomeVia | Unset): Where the seed came from: an explicit `_artifacts`/`_chunks` seed
            in the write, which wins, or the enrichment's field mapping.
            Absent for `missing` and `rejected`.
        error (str | Unset):
    """

    key: str
    outcome: SeedOutcomeOutcome
    name: str | Unset = UNSET
    existing_origin: SeedOutcomeExistingOrigin | Unset = UNSET
    via: SeedOutcomeVia | Unset = UNSET
    error: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        key = self.key

        outcome = self.outcome.value

        name = self.name

        existing_origin: str | Unset = UNSET
        if not isinstance(self.existing_origin, Unset):
            existing_origin = self.existing_origin.value

        via: str | Unset = UNSET
        if not isinstance(self.via, Unset):
            via = self.via.value

        error = self.error

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "key": key,
                "outcome": outcome,
            }
        )
        if name is not UNSET:
            field_dict["name"] = name
        if existing_origin is not UNSET:
            field_dict["existing_origin"] = existing_origin
        if via is not UNSET:
            field_dict["via"] = via
        if error is not UNSET:
            field_dict["error"] = error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        key = d.pop("key")

        outcome = SeedOutcomeOutcome(d.pop("outcome"))

        name = d.pop("name", UNSET)

        _existing_origin = d.pop("existing_origin", UNSET)
        existing_origin: SeedOutcomeExistingOrigin | Unset
        if isinstance(_existing_origin, Unset):
            existing_origin = UNSET
        else:
            existing_origin = SeedOutcomeExistingOrigin(_existing_origin)

        _via = d.pop("via", UNSET)
        via: SeedOutcomeVia | Unset
        if isinstance(_via, Unset):
            via = UNSET
        else:
            via = SeedOutcomeVia(_via)

        error = d.pop("error", UNSET)

        seed_outcome = cls(
            key=key,
            outcome=outcome,
            name=name,
            existing_origin=existing_origin,
            via=via,
            error=error,
        )

        seed_outcome.additional_properties = d
        return seed_outcome

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
