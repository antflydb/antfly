from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.sync_level import SyncLevel
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.relational_row_mutation import RelationalRowMutation


T = TypeVar("T", bound="RelationalRowMutationRequest")


@_attrs_define
class RelationalRowMutationRequest:
    """Atomic version-conditional typed-row replacements and deletions using the durable distributed transaction
    coordinator.

        Attributes:
            schema_version (int): Required active relational schema epoch, fenced during every participant prepare.
            mutations (list[RelationalRowMutation]):
            sync_level (SyncLevel | Unset): Response acknowledgment/publication barrier, independent of commit_requires:
                - "propose": Wait for Raft proposal acceptance (fastest, default)
                - "write": Wait for durable application to the local key-value store
                - "full_text": Wait for full-text index WAL write
                - "enrichments": Wait for this write's enrichment publication
                - "full_index": Wait for full-text, enrichment and vector-index publication
                A post-commit wait failure never rolls back the source. Terminal background
                failures may return committed_repair_required. During the embedded migration
                window, omitting commit_requires retains legacy precomputation for enrichments
                and full_index; explicit none disables that implicit admission prerequisite.
    """

    schema_version: int
    mutations: list[RelationalRowMutation]
    sync_level: SyncLevel | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        schema_version = self.schema_version

        mutations = []
        for mutations_item_data in self.mutations:
            mutations_item = mutations_item_data.to_dict()
            mutations.append(mutations_item)

        sync_level: str | Unset = UNSET
        if not isinstance(self.sync_level, Unset):
            sync_level = self.sync_level.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "schema_version": schema_version,
                "mutations": mutations,
            }
        )
        if sync_level is not UNSET:
            field_dict["sync_level"] = sync_level

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_row_mutation import RelationalRowMutation

        d = dict(src_dict)
        schema_version = d.pop("schema_version")

        mutations = []
        _mutations = d.pop("mutations")
        for mutations_item_data in _mutations:
            mutations_item = RelationalRowMutation.from_dict(mutations_item_data)

            mutations.append(mutations_item)

        _sync_level = d.pop("sync_level", UNSET)
        sync_level: SyncLevel | Unset
        if isinstance(_sync_level, Unset):
            sync_level = UNSET
        else:
            sync_level = SyncLevel(_sync_level)

        relational_row_mutation_request = cls(
            schema_version=schema_version,
            mutations=mutations,
            sync_level=sync_level,
        )

        return relational_row_mutation_request
