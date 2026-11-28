from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.batch_response_status import BatchResponseStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.batch_committed_failure import BatchCommittedFailure
    from ..models.seed_outcome import SeedOutcome


T = TypeVar("T", bound="BatchResponse")


@_attrs_define
class BatchResponse:
    """
    Attributes:
        status (BatchResponseStatus | Unset): Durable commit outcome. `committed_pending` means requested visibility or
            participant propagation is still completing. `committed_repair_required`
            means the primary write committed, but a terminal background materialization
            failure needs operator repair and will not be retried indefinitely. Inspect
            `failure` when present; retrying the document write is unnecessary.
        inserted (int | Unset): Number of documents successfully inserted
        deleted (int | Unset): Number of documents successfully deleted
        transformed (int | Unset): Number of documents successfully transformed
        failure (BatchCommittedFailure | Unset): Additive details for a committed batch that needs operator action. The
            open string code is forward-compatible with older SDKs; clients should
            treat unknown codes as non-retryable when `retryable` is false.
        seeds (list[SeedOutcome] | Unset): Per-item outcomes of the artifact seeds the batch carried, when any
            did. Serverless tables and embedded databases report them. A
            distributed table reports them for a single-shard write whose
            seeds the Raft leader prepared (all replicas decode seed
            preparation), at any sync level; multi-shard transactions report
            no outcomes, and writes prepared before every replica decodes seed
            preparation accept seeds at apply and omit `seeds` (plan those
            with `validate_only`).
    """

    status: BatchResponseStatus | Unset = UNSET
    inserted: int | Unset = UNSET
    deleted: int | Unset = UNSET
    transformed: int | Unset = UNSET
    failure: BatchCommittedFailure | Unset = UNSET
    seeds: list[SeedOutcome] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        status: str | Unset = UNSET
        if not isinstance(self.status, Unset):
            status = self.status.value

        inserted = self.inserted

        deleted = self.deleted

        transformed = self.transformed

        failure: dict[str, Any] | Unset = UNSET
        if not isinstance(self.failure, Unset):
            failure = self.failure.to_dict()

        seeds: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.seeds, Unset):
            seeds = []
            for seeds_item_data in self.seeds:
                seeds_item = seeds_item_data.to_dict()
                seeds.append(seeds_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if status is not UNSET:
            field_dict["status"] = status
        if inserted is not UNSET:
            field_dict["inserted"] = inserted
        if deleted is not UNSET:
            field_dict["deleted"] = deleted
        if transformed is not UNSET:
            field_dict["transformed"] = transformed
        if failure is not UNSET:
            field_dict["failure"] = failure
        if seeds is not UNSET:
            field_dict["seeds"] = seeds

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.batch_committed_failure import BatchCommittedFailure
        from ..models.seed_outcome import SeedOutcome

        d = dict(src_dict)
        _status = d.pop("status", UNSET)
        status: BatchResponseStatus | Unset
        if isinstance(_status, Unset):
            status = UNSET
        else:
            status = BatchResponseStatus(_status)

        inserted = d.pop("inserted", UNSET)

        deleted = d.pop("deleted", UNSET)

        transformed = d.pop("transformed", UNSET)

        _failure = d.pop("failure", UNSET)
        failure: BatchCommittedFailure | Unset
        if isinstance(_failure, Unset):
            failure = UNSET
        else:
            failure = BatchCommittedFailure.from_dict(_failure)

        _seeds = d.pop("seeds", UNSET)
        seeds: list[SeedOutcome] | Unset = UNSET
        if _seeds is not UNSET:
            seeds = []
            for seeds_item_data in _seeds:
                seeds_item = SeedOutcome.from_dict(seeds_item_data)

                seeds.append(seeds_item)

        batch_response = cls(
            status=status,
            inserted=inserted,
            deleted=deleted,
            transformed=transformed,
            failure=failure,
            seeds=seeds,
        )

        batch_response.additional_properties = d
        return batch_response

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
