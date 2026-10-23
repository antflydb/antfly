from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.field_capability_kind import FieldCapabilityKind
from ..models.field_capability_sort_lifecycle_state import FieldCapabilitySortLifecycleState
from ..models.field_capability_value_operations_item import FieldCapabilityValueOperationsItem
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.field_representation import FieldRepresentation


T = TypeVar("T", bound="FieldCapability")


@_attrs_define
class FieldCapability:
    """One public logical field with its supported value operations and nested
    concrete representations. Queries name field; Antfly resolves compatible
    representations. Unknown source associations remain explicit diagnostics.

        Attributes:
            kind (FieldCapabilityKind): Field origin. Unassociated entries do not assert a document source path.
            value_operations (list[FieldCapabilityValueOperationsItem]): Supported logical query operations on field.
                Equals, contains and matches
                verify original values; autocomplete performs analyzed bool-prefix matching. Availability does not
                attest index coverage or authorize scans. Case sensitivity, execution
                policy and work budgets apply. Reserved/computed/template/unassociated
                entries do not advertise original-document value operations.
            sortable (bool): Whether field resolves to one compatible sortable representation. Cold declared fields validate
                coverage on first exact-sort use; callers do not route through representation names.
            sort_lifecycle_state (FieldCapabilitySortLifecycleState): Conservative cached lifecycle for the representation
                selected by the query resolver. Ambiguous or unavailable routing is unsupported.
            sort_requires_scoring (bool): True for query-computed score ordering, which requires a score-bearing query. This
                does not authorize ordering a non-scoring query by _score.
            representations (list[FieldRepresentation]): Concrete representations with analyzer, type, legacy query modes
                and readiness diagnostics. Multiple representations never require clients to reproduce Antfly routing.
            field (str | Unset): Public source or reserved/computed field accepted by queries. Absent for pattern-only
                templates and unassociated index representations.
    """

    kind: FieldCapabilityKind
    value_operations: list[FieldCapabilityValueOperationsItem]
    sortable: bool
    sort_lifecycle_state: FieldCapabilitySortLifecycleState
    sort_requires_scoring: bool
    representations: list[FieldRepresentation]
    field: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        value_operations = []
        for value_operations_item_data in self.value_operations:
            value_operations_item = value_operations_item_data.value
            value_operations.append(value_operations_item)

        sortable = self.sortable

        sort_lifecycle_state = self.sort_lifecycle_state.value

        sort_requires_scoring = self.sort_requires_scoring

        representations = []
        for representations_item_data in self.representations:
            representations_item = representations_item_data.to_dict()
            representations.append(representations_item)

        field = self.field

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "kind": kind,
                "value_operations": value_operations,
                "sortable": sortable,
                "sort_lifecycle_state": sort_lifecycle_state,
                "sort_requires_scoring": sort_requires_scoring,
                "representations": representations,
            }
        )
        if field is not UNSET:
            field_dict["field"] = field

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.field_representation import FieldRepresentation

        d = dict(src_dict)
        kind = FieldCapabilityKind(d.pop("kind"))

        value_operations = []
        _value_operations = d.pop("value_operations")
        for value_operations_item_data in _value_operations:
            value_operations_item = FieldCapabilityValueOperationsItem(value_operations_item_data)

            value_operations.append(value_operations_item)

        sortable = d.pop("sortable")

        sort_lifecycle_state = FieldCapabilitySortLifecycleState(d.pop("sort_lifecycle_state"))

        sort_requires_scoring = d.pop("sort_requires_scoring")

        representations = []
        _representations = d.pop("representations")
        for representations_item_data in _representations:
            representations_item = FieldRepresentation.from_dict(representations_item_data)

            representations.append(representations_item)

        field = d.pop("field", UNSET)

        field_capability = cls(
            kind=kind,
            value_operations=value_operations,
            sortable=sortable,
            sort_lifecycle_state=sort_lifecycle_state,
            sort_requires_scoring=sort_requires_scoring,
            representations=representations,
            field=field,
        )

        field_capability.additional_properties = d
        return field_capability

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
