from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.resolver_mutation_result_replay_sequence_kind import ResolverMutationResultReplaySequenceKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.resolver_mutation_result_metadata_commit_type_0 import ResolverMutationResultMetadataCommitType0
    from ..models.resolver_replay_progress import ResolverReplayProgress


T = TypeVar("T", bound="ResolverMutationResult")


@_attrs_define
class ResolverMutationResult:
    """
    Attributes:
        resolver (str):
        changed (bool):
        revision (int):
        replay_sequence (int):
        replay_sequence_kind (ResolverMutationResultReplaySequenceKind):
        applied (bool):
        metadata_commit (None | ResolverMutationResultMetadataCommitType0 | Unset):
        progress (ResolverReplayProgress | Unset):
    """

    resolver: str
    changed: bool
    revision: int
    replay_sequence: int
    replay_sequence_kind: ResolverMutationResultReplaySequenceKind
    applied: bool
    metadata_commit: None | ResolverMutationResultMetadataCommitType0 | Unset = UNSET
    progress: ResolverReplayProgress | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.resolver_mutation_result_metadata_commit_type_0 import ResolverMutationResultMetadataCommitType0

        resolver = self.resolver

        changed = self.changed

        revision = self.revision

        replay_sequence = self.replay_sequence

        replay_sequence_kind = self.replay_sequence_kind.value

        applied = self.applied

        metadata_commit: dict[str, Any] | None | Unset
        if isinstance(self.metadata_commit, Unset):
            metadata_commit = UNSET
        elif isinstance(self.metadata_commit, ResolverMutationResultMetadataCommitType0):
            metadata_commit = self.metadata_commit.to_dict()
        else:
            metadata_commit = self.metadata_commit

        progress: dict[str, Any] | Unset = UNSET
        if not isinstance(self.progress, Unset):
            progress = self.progress.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "resolver": resolver,
                "changed": changed,
                "revision": revision,
                "replay_sequence": replay_sequence,
                "replay_sequence_kind": replay_sequence_kind,
                "applied": applied,
            }
        )
        if metadata_commit is not UNSET:
            field_dict["metadata_commit"] = metadata_commit
        if progress is not UNSET:
            field_dict["progress"] = progress

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.resolver_mutation_result_metadata_commit_type_0 import ResolverMutationResultMetadataCommitType0
        from ..models.resolver_replay_progress import ResolverReplayProgress

        d = dict(src_dict)
        resolver = d.pop("resolver")

        changed = d.pop("changed")

        revision = d.pop("revision")

        replay_sequence = d.pop("replay_sequence")

        replay_sequence_kind = ResolverMutationResultReplaySequenceKind(d.pop("replay_sequence_kind"))

        applied = d.pop("applied")

        def _parse_metadata_commit(data: object) -> None | ResolverMutationResultMetadataCommitType0 | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                metadata_commit_type_0 = ResolverMutationResultMetadataCommitType0.from_dict(data)

                return metadata_commit_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | ResolverMutationResultMetadataCommitType0 | Unset, data)

        metadata_commit = _parse_metadata_commit(d.pop("metadata_commit", UNSET))

        _progress = d.pop("progress", UNSET)
        progress: ResolverReplayProgress | Unset
        if isinstance(_progress, Unset):
            progress = UNSET
        else:
            progress = ResolverReplayProgress.from_dict(_progress)

        resolver_mutation_result = cls(
            resolver=resolver,
            changed=changed,
            revision=revision,
            replay_sequence=replay_sequence,
            replay_sequence_kind=replay_sequence_kind,
            applied=applied,
            metadata_commit=metadata_commit,
            progress=progress,
        )

        resolver_mutation_result.additional_properties = d
        return resolver_mutation_result

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
