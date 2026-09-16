from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.create_table_storage_migration_body_target import CreateTableStorageMigrationBodyTarget
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.create_table_storage_migration_body_budget import CreateTableStorageMigrationBodyBudget


T = TypeVar("T", bound="CreateTableStorageMigrationBody")


@_attrs_define
class CreateTableStorageMigrationBody:
    """
    Attributes:
        job_id (str):
        target (CreateTableStorageMigrationBodyTarget):
        budget (CreateTableStorageMigrationBodyBudget | Unset):
    """

    job_id: str
    target: CreateTableStorageMigrationBodyTarget
    budget: CreateTableStorageMigrationBodyBudget | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        job_id = self.job_id

        target = self.target.value

        budget: dict[str, Any] | Unset = UNSET
        if not isinstance(self.budget, Unset):
            budget = self.budget.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "job_id": job_id,
                "target": target,
            }
        )
        if budget is not UNSET:
            field_dict["budget"] = budget

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.create_table_storage_migration_body_budget import CreateTableStorageMigrationBodyBudget

        d = dict(src_dict)
        job_id = d.pop("job_id")

        target = CreateTableStorageMigrationBodyTarget(d.pop("target"))

        _budget = d.pop("budget", UNSET)
        budget: CreateTableStorageMigrationBodyBudget | Unset
        if isinstance(_budget, Unset):
            budget = UNSET
        else:
            budget = CreateTableStorageMigrationBodyBudget.from_dict(_budget)

        create_table_storage_migration_body = cls(
            job_id=job_id,
            target=target,
            budget=budget,
        )

        return create_table_storage_migration_body
