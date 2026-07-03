from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.full_text_index_stats_index_type import FullTextIndexStatsIndexType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.full_text_index_stats_async_indexing import FullTextIndexStatsAsyncIndexing
    from ..models.full_text_index_stats_text_merge import FullTextIndexStatsTextMerge


T = TypeVar("T", bound="FullTextIndexStats")


@_attrs_define
class FullTextIndexStats:
    """
    Attributes:
        index_type (FullTextIndexStatsIndexType): Discriminator for the index stats variant.
        error (str | Unset): Error message if stats could not be retrieved
        total_indexed (int | Unset): Number of documents in the index
        rebuilding (bool | Unset): Whether the index is currently rebuilding
        backfill_active (bool | Unset): Whether the index is actively rebuilding, replaying, or catching up.
        backfill_progress (float | Unset): Progress of ongoing rebuild as fraction [0.0, 1.0]
        backfill_state (str | Unset): Operational readiness state such as ready, running, retrying, or failed.
        doc_count (int | Unset): Number of documents visible to the index.
        term_count (int | Unset): Number of indexed terms when available.
        replay_applied_sequence (int | Unset): Highest replay sequence applied to the index runtime.
        replay_target_sequence (int | Unset): Replay sequence the index runtime must reach to be current.
        replay_catch_up_required (bool | Unset): Whether replay must catch up before the index is fully current.
        runtime_present (bool | Unset):
        runtime_fresh (bool | Unset):
        runtime_source (str | Unset):
        runtime_freshness (str | Unset):
        catch_up_active (bool | Unset):
        catch_up_phase (str | Unset):
        catch_up_applied_sequence (int | Unset):
        catch_up_target_sequence (int | Unset):
        text_merge (FullTextIndexStatsTextMerge | Unset): Full-text merge runtime diagnostics.
        async_indexing (FullTextIndexStatsAsyncIndexing | Unset): Asynchronous indexer runtime diagnostics.
    """

    index_type: FullTextIndexStatsIndexType
    error: str | Unset = UNSET
    total_indexed: int | Unset = UNSET
    rebuilding: bool | Unset = UNSET
    backfill_active: bool | Unset = UNSET
    backfill_progress: float | Unset = UNSET
    backfill_state: str | Unset = UNSET
    doc_count: int | Unset = UNSET
    term_count: int | Unset = UNSET
    replay_applied_sequence: int | Unset = UNSET
    replay_target_sequence: int | Unset = UNSET
    replay_catch_up_required: bool | Unset = UNSET
    runtime_present: bool | Unset = UNSET
    runtime_fresh: bool | Unset = UNSET
    runtime_source: str | Unset = UNSET
    runtime_freshness: str | Unset = UNSET
    catch_up_active: bool | Unset = UNSET
    catch_up_phase: str | Unset = UNSET
    catch_up_applied_sequence: int | Unset = UNSET
    catch_up_target_sequence: int | Unset = UNSET
    text_merge: FullTextIndexStatsTextMerge | Unset = UNSET
    async_indexing: FullTextIndexStatsAsyncIndexing | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        index_type = self.index_type.value

        error = self.error

        total_indexed = self.total_indexed

        rebuilding = self.rebuilding

        backfill_active = self.backfill_active

        backfill_progress = self.backfill_progress

        backfill_state = self.backfill_state

        doc_count = self.doc_count

        term_count = self.term_count

        replay_applied_sequence = self.replay_applied_sequence

        replay_target_sequence = self.replay_target_sequence

        replay_catch_up_required = self.replay_catch_up_required

        runtime_present = self.runtime_present

        runtime_fresh = self.runtime_fresh

        runtime_source = self.runtime_source

        runtime_freshness = self.runtime_freshness

        catch_up_active = self.catch_up_active

        catch_up_phase = self.catch_up_phase

        catch_up_applied_sequence = self.catch_up_applied_sequence

        catch_up_target_sequence = self.catch_up_target_sequence

        text_merge: dict[str, Any] | Unset = UNSET
        if not isinstance(self.text_merge, Unset):
            text_merge = self.text_merge.to_dict()

        async_indexing: dict[str, Any] | Unset = UNSET
        if not isinstance(self.async_indexing, Unset):
            async_indexing = self.async_indexing.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "index_type": index_type,
            }
        )
        if error is not UNSET:
            field_dict["error"] = error
        if total_indexed is not UNSET:
            field_dict["total_indexed"] = total_indexed
        if rebuilding is not UNSET:
            field_dict["rebuilding"] = rebuilding
        if backfill_active is not UNSET:
            field_dict["backfill_active"] = backfill_active
        if backfill_progress is not UNSET:
            field_dict["backfill_progress"] = backfill_progress
        if backfill_state is not UNSET:
            field_dict["backfill_state"] = backfill_state
        if doc_count is not UNSET:
            field_dict["doc_count"] = doc_count
        if term_count is not UNSET:
            field_dict["term_count"] = term_count
        if replay_applied_sequence is not UNSET:
            field_dict["replay_applied_sequence"] = replay_applied_sequence
        if replay_target_sequence is not UNSET:
            field_dict["replay_target_sequence"] = replay_target_sequence
        if replay_catch_up_required is not UNSET:
            field_dict["replay_catch_up_required"] = replay_catch_up_required
        if runtime_present is not UNSET:
            field_dict["runtime_present"] = runtime_present
        if runtime_fresh is not UNSET:
            field_dict["runtime_fresh"] = runtime_fresh
        if runtime_source is not UNSET:
            field_dict["runtime_source"] = runtime_source
        if runtime_freshness is not UNSET:
            field_dict["runtime_freshness"] = runtime_freshness
        if catch_up_active is not UNSET:
            field_dict["catch_up_active"] = catch_up_active
        if catch_up_phase is not UNSET:
            field_dict["catch_up_phase"] = catch_up_phase
        if catch_up_applied_sequence is not UNSET:
            field_dict["catch_up_applied_sequence"] = catch_up_applied_sequence
        if catch_up_target_sequence is not UNSET:
            field_dict["catch_up_target_sequence"] = catch_up_target_sequence
        if text_merge is not UNSET:
            field_dict["text_merge"] = text_merge
        if async_indexing is not UNSET:
            field_dict["async_indexing"] = async_indexing

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.full_text_index_stats_async_indexing import FullTextIndexStatsAsyncIndexing
        from ..models.full_text_index_stats_text_merge import FullTextIndexStatsTextMerge

        d = dict(src_dict)
        index_type = FullTextIndexStatsIndexType(d.pop("index_type"))

        error = d.pop("error", UNSET)

        total_indexed = d.pop("total_indexed", UNSET)

        rebuilding = d.pop("rebuilding", UNSET)

        backfill_active = d.pop("backfill_active", UNSET)

        backfill_progress = d.pop("backfill_progress", UNSET)

        backfill_state = d.pop("backfill_state", UNSET)

        doc_count = d.pop("doc_count", UNSET)

        term_count = d.pop("term_count", UNSET)

        replay_applied_sequence = d.pop("replay_applied_sequence", UNSET)

        replay_target_sequence = d.pop("replay_target_sequence", UNSET)

        replay_catch_up_required = d.pop("replay_catch_up_required", UNSET)

        runtime_present = d.pop("runtime_present", UNSET)

        runtime_fresh = d.pop("runtime_fresh", UNSET)

        runtime_source = d.pop("runtime_source", UNSET)

        runtime_freshness = d.pop("runtime_freshness", UNSET)

        catch_up_active = d.pop("catch_up_active", UNSET)

        catch_up_phase = d.pop("catch_up_phase", UNSET)

        catch_up_applied_sequence = d.pop("catch_up_applied_sequence", UNSET)

        catch_up_target_sequence = d.pop("catch_up_target_sequence", UNSET)

        _text_merge = d.pop("text_merge", UNSET)
        text_merge: FullTextIndexStatsTextMerge | Unset
        if isinstance(_text_merge, Unset):
            text_merge = UNSET
        else:
            text_merge = FullTextIndexStatsTextMerge.from_dict(_text_merge)

        _async_indexing = d.pop("async_indexing", UNSET)
        async_indexing: FullTextIndexStatsAsyncIndexing | Unset
        if isinstance(_async_indexing, Unset):
            async_indexing = UNSET
        else:
            async_indexing = FullTextIndexStatsAsyncIndexing.from_dict(_async_indexing)

        full_text_index_stats = cls(
            index_type=index_type,
            error=error,
            total_indexed=total_indexed,
            rebuilding=rebuilding,
            backfill_active=backfill_active,
            backfill_progress=backfill_progress,
            backfill_state=backfill_state,
            doc_count=doc_count,
            term_count=term_count,
            replay_applied_sequence=replay_applied_sequence,
            replay_target_sequence=replay_target_sequence,
            replay_catch_up_required=replay_catch_up_required,
            runtime_present=runtime_present,
            runtime_fresh=runtime_fresh,
            runtime_source=runtime_source,
            runtime_freshness=runtime_freshness,
            catch_up_active=catch_up_active,
            catch_up_phase=catch_up_phase,
            catch_up_applied_sequence=catch_up_applied_sequence,
            catch_up_target_sequence=catch_up_target_sequence,
            text_merge=text_merge,
            async_indexing=async_indexing,
        )

        full_text_index_stats.additional_properties = d
        return full_text_index_stats

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
