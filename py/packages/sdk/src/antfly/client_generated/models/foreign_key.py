from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.foreign_key_match import ForeignKeyMatch
from ..models.foreign_key_on_delete import ForeignKeyOnDelete
from ..models.foreign_key_on_update import ForeignKeyOnUpdate
from ..models.foreign_key_validation_state import ForeignKeyValidationState
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.foreign_key_reference import ForeignKeyReference


T = TypeVar("T", bound="ForeignKey")


@_attrs_define
class ForeignKey:
    """Relational foreign-key constraint.

    Attributes:
        name (str | Unset): Constraint name, unique within the table schema.
        columns (list[str] | Unset): Child columns. A single scalar column is supported for ["_id"] references; ordered
            scalar tuples are supported when references.columns names a unique constraint column tuple.
        period (str | Unset): Child application-time period name for temporal `FOREIGN KEY (..., PERIOD period)`
            constraints.
        references (ForeignKeyReference | Unset): Parent side of a relational foreign-key constraint.
        on_delete (ForeignKeyOnDelete | Unset): Delete action. "no_action" is normalized to immediate restrictive
            behavior; "set_null" requires nullable child columns; "set_null" and "cascade" are bounded in local execution.
        on_update (ForeignKeyOnUpdate | Unset): Update action. "restrict" and "no_action" are enforced as parent-key
            update checks; "set_null" and "cascade" are supported for bounded local/scheduled mutating FK action execution
            where owner topology is configured.
        timing (str | Unset): Constraint timing. Canonical values are "immediate" and "deferred"; SQL-shaped aliases
            such as "INITIALLY DEFERRED" and combined deferrability clauses are accepted and normalized by schema parsing.
        deferrable (bool | str | Unset): Whether transaction-level timing overrides may change this constraint's
            effective timing. Accepts JSON booleans and SQL-shaped strings such as "DEFERRABLE", "NOT DEFERRABLE", and
            "DEFERRABLE INITIALLY DEFERRED". Omitted defaults to false unless timing is "deferred" for compatibility.
        match (ForeignKeyMatch | Unset): Match mode. "simple" is the default and means any null or absent child
            component creates no reference. "full" requires all child components to be present or all absent. MATCH PARTIAL
            is reserved until row-subset parent matching is implemented.
        validation_state (ForeignKeyValidationState | Unset): Constraint validation state. Public schema validation
            accepts enforced constraints and local unvalidated adoption entries; online job-owned states are reserved for
            hosted migration jobs.
    """

    name: str | Unset = UNSET
    columns: list[str] | Unset = UNSET
    period: str | Unset = UNSET
    references: ForeignKeyReference | Unset = UNSET
    on_delete: ForeignKeyOnDelete | Unset = UNSET
    on_update: ForeignKeyOnUpdate | Unset = UNSET
    timing: str | Unset = UNSET
    deferrable: bool | str | Unset = UNSET
    match: ForeignKeyMatch | Unset = UNSET
    validation_state: ForeignKeyValidationState | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        columns: list[str] | Unset = UNSET
        if not isinstance(self.columns, Unset):
            columns = self.columns

        period = self.period

        references: dict[str, Any] | Unset = UNSET
        if not isinstance(self.references, Unset):
            references = self.references.to_dict()

        on_delete: str | Unset = UNSET
        if not isinstance(self.on_delete, Unset):
            on_delete = self.on_delete.value

        on_update: str | Unset = UNSET
        if not isinstance(self.on_update, Unset):
            on_update = self.on_update.value

        timing = self.timing

        deferrable: bool | str | Unset
        if isinstance(self.deferrable, Unset):
            deferrable = UNSET
        else:
            deferrable = self.deferrable

        match: str | Unset = UNSET
        if not isinstance(self.match, Unset):
            match = self.match.value

        validation_state: str | Unset = UNSET
        if not isinstance(self.validation_state, Unset):
            validation_state = self.validation_state.value

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if name is not UNSET:
            field_dict["name"] = name
        if columns is not UNSET:
            field_dict["columns"] = columns
        if period is not UNSET:
            field_dict["period"] = period
        if references is not UNSET:
            field_dict["references"] = references
        if on_delete is not UNSET:
            field_dict["on_delete"] = on_delete
        if on_update is not UNSET:
            field_dict["on_update"] = on_update
        if timing is not UNSET:
            field_dict["timing"] = timing
        if deferrable is not UNSET:
            field_dict["deferrable"] = deferrable
        if match is not UNSET:
            field_dict["match"] = match
        if validation_state is not UNSET:
            field_dict["validation_state"] = validation_state

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.foreign_key_reference import ForeignKeyReference

        d = dict(src_dict)
        name = d.pop("name", UNSET)

        columns = cast(list[str], d.pop("columns", UNSET))

        period = d.pop("period", UNSET)

        _references = d.pop("references", UNSET)
        references: ForeignKeyReference | Unset
        if isinstance(_references, Unset):
            references = UNSET
        else:
            references = ForeignKeyReference.from_dict(_references)

        _on_delete = d.pop("on_delete", UNSET)
        on_delete: ForeignKeyOnDelete | Unset
        if isinstance(_on_delete, Unset):
            on_delete = UNSET
        else:
            on_delete = ForeignKeyOnDelete(_on_delete)

        _on_update = d.pop("on_update", UNSET)
        on_update: ForeignKeyOnUpdate | Unset
        if isinstance(_on_update, Unset):
            on_update = UNSET
        else:
            on_update = ForeignKeyOnUpdate(_on_update)

        timing = d.pop("timing", UNSET)

        def _parse_deferrable(data: object) -> bool | str | Unset:
            if isinstance(data, Unset):
                return data
            return cast(bool | str | Unset, data)

        deferrable = _parse_deferrable(d.pop("deferrable", UNSET))

        _match = d.pop("match", UNSET)
        match: ForeignKeyMatch | Unset
        if isinstance(_match, Unset):
            match = UNSET
        else:
            match = ForeignKeyMatch(_match)

        _validation_state = d.pop("validation_state", UNSET)
        validation_state: ForeignKeyValidationState | Unset
        if isinstance(_validation_state, Unset):
            validation_state = UNSET
        else:
            validation_state = ForeignKeyValidationState(_validation_state)

        foreign_key = cls(
            name=name,
            columns=columns,
            period=period,
            references=references,
            on_delete=on_delete,
            on_update=on_update,
            timing=timing,
            deferrable=deferrable,
            match=match,
            validation_state=validation_state,
        )

        return foreign_key
