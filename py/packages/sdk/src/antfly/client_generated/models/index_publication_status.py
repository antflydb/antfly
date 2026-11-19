from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.index_publication_failure import IndexPublicationFailure
from ..models.index_publication_phase import IndexPublicationPhase
from ..models.index_publication_state import IndexPublicationState
from ..models.index_publication_status_index_type import IndexPublicationStatusIndexType
from ..types import UNSET, Unset

T = TypeVar("T", bound="IndexPublicationStatus")


@_attrs_define
class IndexPublicationStatus:
    """Exact observation of an online relational-index publication job, not a query-readiness certificate. Published means
    this retained job completed; later schema/index changes may supersede its artifacts. An absent or unmirrored job is
    admission_unknown, never permission to replay DDL.

        Attributes:
            index_type (IndexPublicationStatusIndexType):
            index_name (str):
            publication_token (str): Opaque versioned recovery handle binding a metadata incarnation, immutable table
                identity, publication ID and plan commitment. This is not authentication or permission to execute, cancel, adopt
                a root or replay DDL. Retain it before admission and use current table authorization when observing the
                publication.
            table_id (str):
            publication_id (str):
            plan_digest (str):
            metadata_incarnation (str):
            state (IndexPublicationState):
            replication_pending (bool):
            phase (IndexPublicationPhase | Unset):
            failure (IndexPublicationFailure | Unset):
            owner_count (int | Unset):
    """

    index_type: IndexPublicationStatusIndexType
    index_name: str
    publication_token: str
    table_id: str
    publication_id: str
    plan_digest: str
    metadata_incarnation: str
    state: IndexPublicationState
    replication_pending: bool
    phase: IndexPublicationPhase | Unset = UNSET
    failure: IndexPublicationFailure | Unset = UNSET
    owner_count: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        index_type = self.index_type.value

        index_name = self.index_name

        publication_token = self.publication_token

        table_id = self.table_id

        publication_id = self.publication_id

        plan_digest = self.plan_digest

        metadata_incarnation = self.metadata_incarnation

        state = self.state.value

        replication_pending = self.replication_pending

        phase: str | Unset = UNSET
        if not isinstance(self.phase, Unset):
            phase = self.phase.value

        failure: str | Unset = UNSET
        if not isinstance(self.failure, Unset):
            failure = self.failure.value

        owner_count = self.owner_count

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "index_type": index_type,
                "index_name": index_name,
                "publication_token": publication_token,
                "table_id": table_id,
                "publication_id": publication_id,
                "plan_digest": plan_digest,
                "metadata_incarnation": metadata_incarnation,
                "state": state,
                "replication_pending": replication_pending,
            }
        )
        if phase is not UNSET:
            field_dict["phase"] = phase
        if failure is not UNSET:
            field_dict["failure"] = failure
        if owner_count is not UNSET:
            field_dict["owner_count"] = owner_count

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        index_type = IndexPublicationStatusIndexType(d.pop("index_type"))

        index_name = d.pop("index_name")

        publication_token = d.pop("publication_token")

        table_id = d.pop("table_id")

        publication_id = d.pop("publication_id")

        plan_digest = d.pop("plan_digest")

        metadata_incarnation = d.pop("metadata_incarnation")

        state = IndexPublicationState(d.pop("state"))

        replication_pending = d.pop("replication_pending")

        _phase = d.pop("phase", UNSET)
        phase: IndexPublicationPhase | Unset
        if isinstance(_phase, Unset):
            phase = UNSET
        else:
            phase = IndexPublicationPhase(_phase)

        _failure = d.pop("failure", UNSET)
        failure: IndexPublicationFailure | Unset
        if isinstance(_failure, Unset):
            failure = UNSET
        else:
            failure = IndexPublicationFailure(_failure)

        owner_count = d.pop("owner_count", UNSET)

        index_publication_status = cls(
            index_type=index_type,
            index_name=index_name,
            publication_token=publication_token,
            table_id=table_id,
            publication_id=publication_id,
            plan_digest=plan_digest,
            metadata_incarnation=metadata_incarnation,
            state=state,
            replication_pending=replication_pending,
            phase=phase,
            failure=failure,
            owner_count=owner_count,
        )

        return index_publication_status
