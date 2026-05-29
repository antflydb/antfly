from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ClusterDataGroupStatus")


@_attrs_define
class ClusterDataGroupStatus:
    """
    Attributes:
        group_id (int):
        leader_known (bool | Unset):
        leader_data_id (int | None | Unset):
        voter_count_known (bool | Unset):
        voter_count (int | Unset):
        healthy_voter_reports (int | Unset):
        joint_consensus (bool | Unset):
        transition_pending (bool | Unset):
        replay_required (bool | Unset):
        replay_caught_up (bool | Unset):
        cutover_ready (bool | Unset):
        reads_ready_after_cutover (bool | Unset):
        doc_identity_lifecycle (str | Unset):
        doc_count (int | Unset):
        disk_bytes (int | Unset):
        empty (bool | Unset):
    """

    group_id: int
    leader_known: bool | Unset = UNSET
    leader_data_id: int | None | Unset = UNSET
    voter_count_known: bool | Unset = UNSET
    voter_count: int | Unset = UNSET
    healthy_voter_reports: int | Unset = UNSET
    joint_consensus: bool | Unset = UNSET
    transition_pending: bool | Unset = UNSET
    replay_required: bool | Unset = UNSET
    replay_caught_up: bool | Unset = UNSET
    cutover_ready: bool | Unset = UNSET
    reads_ready_after_cutover: bool | Unset = UNSET
    doc_identity_lifecycle: str | Unset = UNSET
    doc_count: int | Unset = UNSET
    disk_bytes: int | Unset = UNSET
    empty: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        group_id = self.group_id

        leader_known = self.leader_known

        leader_data_id: int | None | Unset
        if isinstance(self.leader_data_id, Unset):
            leader_data_id = UNSET
        else:
            leader_data_id = self.leader_data_id

        voter_count_known = self.voter_count_known

        voter_count = self.voter_count

        healthy_voter_reports = self.healthy_voter_reports

        joint_consensus = self.joint_consensus

        transition_pending = self.transition_pending

        replay_required = self.replay_required

        replay_caught_up = self.replay_caught_up

        cutover_ready = self.cutover_ready

        reads_ready_after_cutover = self.reads_ready_after_cutover

        doc_identity_lifecycle = self.doc_identity_lifecycle

        doc_count = self.doc_count

        disk_bytes = self.disk_bytes

        empty = self.empty

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "group_id": group_id,
            }
        )
        if leader_known is not UNSET:
            field_dict["leader_known"] = leader_known
        if leader_data_id is not UNSET:
            field_dict["leader_data_id"] = leader_data_id
        if voter_count_known is not UNSET:
            field_dict["voter_count_known"] = voter_count_known
        if voter_count is not UNSET:
            field_dict["voter_count"] = voter_count
        if healthy_voter_reports is not UNSET:
            field_dict["healthy_voter_reports"] = healthy_voter_reports
        if joint_consensus is not UNSET:
            field_dict["joint_consensus"] = joint_consensus
        if transition_pending is not UNSET:
            field_dict["transition_pending"] = transition_pending
        if replay_required is not UNSET:
            field_dict["replay_required"] = replay_required
        if replay_caught_up is not UNSET:
            field_dict["replay_caught_up"] = replay_caught_up
        if cutover_ready is not UNSET:
            field_dict["cutover_ready"] = cutover_ready
        if reads_ready_after_cutover is not UNSET:
            field_dict["reads_ready_after_cutover"] = reads_ready_after_cutover
        if doc_identity_lifecycle is not UNSET:
            field_dict["doc_identity_lifecycle"] = doc_identity_lifecycle
        if doc_count is not UNSET:
            field_dict["doc_count"] = doc_count
        if disk_bytes is not UNSET:
            field_dict["disk_bytes"] = disk_bytes
        if empty is not UNSET:
            field_dict["empty"] = empty

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        group_id = d.pop("group_id")

        leader_known = d.pop("leader_known", UNSET)

        def _parse_leader_data_id(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        leader_data_id = _parse_leader_data_id(d.pop("leader_data_id", UNSET))

        voter_count_known = d.pop("voter_count_known", UNSET)

        voter_count = d.pop("voter_count", UNSET)

        healthy_voter_reports = d.pop("healthy_voter_reports", UNSET)

        joint_consensus = d.pop("joint_consensus", UNSET)

        transition_pending = d.pop("transition_pending", UNSET)

        replay_required = d.pop("replay_required", UNSET)

        replay_caught_up = d.pop("replay_caught_up", UNSET)

        cutover_ready = d.pop("cutover_ready", UNSET)

        reads_ready_after_cutover = d.pop("reads_ready_after_cutover", UNSET)

        doc_identity_lifecycle = d.pop("doc_identity_lifecycle", UNSET)

        doc_count = d.pop("doc_count", UNSET)

        disk_bytes = d.pop("disk_bytes", UNSET)

        empty = d.pop("empty", UNSET)

        cluster_data_group_status = cls(
            group_id=group_id,
            leader_known=leader_known,
            leader_data_id=leader_data_id,
            voter_count_known=voter_count_known,
            voter_count=voter_count,
            healthy_voter_reports=healthy_voter_reports,
            joint_consensus=joint_consensus,
            transition_pending=transition_pending,
            replay_required=replay_required,
            replay_caught_up=replay_caught_up,
            cutover_ready=cutover_ready,
            reads_ready_after_cutover=reads_ready_after_cutover,
            doc_identity_lifecycle=doc_identity_lifecycle,
            doc_count=doc_count,
            disk_bytes=disk_bytes,
            empty=empty,
        )

        cluster_data_group_status.additional_properties = d
        return cluster_data_group_status

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
