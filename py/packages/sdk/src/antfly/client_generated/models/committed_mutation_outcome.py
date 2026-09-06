from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.committed_mutation_outcome_status import CommittedMutationOutcomeStatus

T = TypeVar("T", bound="CommittedMutationOutcome")


@_attrs_define
class CommittedMutationOutcome:
    """The metadata mutation committed, but requested visibility or local
    materialization is not yet fully healthy. Clients must observe status
    instead of automatically replaying the mutation. `committed_superseded`
    is terminal: a newer schema version became visible first.

        Attributes:
            status (CommittedMutationOutcomeStatus):
    """

    status: CommittedMutationOutcomeStatus
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "status": status,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        status = CommittedMutationOutcomeStatus(d.pop("status"))

        committed_mutation_outcome = cls(
            status=status,
        )

        committed_mutation_outcome.additional_properties = d
        return committed_mutation_outcome

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
