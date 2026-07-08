from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.sort_profile_candidate_source import SortProfileCandidateSource
from ..models.sort_profile_sort_lifecycle_state import SortProfileSortLifecycleState
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.sort_field import SortField


T = TypeVar("T", bound="SortProfile")


@_attrs_define
class SortProfile:
    """Sort execution profile. These fields are the stable public diagnostic
    surface. Low-level implementation counters such as doc-value load
    timings, stored-source loads, collector/window internals, cost-model
    inputs, native-filter modes, and index-sort availability flags are kept
    out of normal SDK-facing query responses and may appear only in
    internal debug logs or explicit debug surfaces.

        Attributes:
            plan (str | Unset): Stable physical sort plan name. Known values include `none`,
                `id_only`, `id_seek`, `sorted_segment_seek`,
                `native_doc_values_top_n`, `score_top_k`,
                `distributed_k_way_merge`, `stored_json_debug`, and
                `unsupported_exact_sort`. Public exact sort requests must not
                silently move from native plans to `stored_json_debug`; missing
                native coverage is reported through the rejection fields instead.
            order_by (list[SortField] | Unset): Requested order fields, including the implicit _id tie-breaker when
                applicable.
            cursor (str | Unset): Cursor mode for this request.
            exactness (str | Unset): Exactness class for the selected plan. Known values include
                `none`, `exact`, `bounded_exact`, `approximate`, and
                `unsupported`.
            source (str | Unset): Sort execution primitive used by the selected plan. Known values
                include `none`, `candidate_collector`, `primary_key_scan`,
                `sorted_segment_scan`, `score_top_k`, `doc_values_collector`,
                `distributed_merge`, `stored_json_debug`, and `unsupported`.
            candidate_source (SortProfileCandidateSource | Unset): Exact candidate source consumed by the selected sort
                primitive.
            cursor_support (str | Unset): Cursor support level for the selected plan. Known values include
                `none`, `comparator`, `segment_seek`, `distributed_seek`, and
                `unsupported`.
            source_load (str | Unset): Stored source load strategy. Known values include `none`,
                `source_free`, `projected_source_after_page`,
                `stored_source_required`, and `unsupported`.
            distributed_behavior (str | Unset): Distributed sort behavior. Known values include `none`,
                `shard_local_only`, `coordinator_merge`, and `unsupported`.
            selection_reason (str | Unset): Stable reason the planner selected this sort plan. Known values
                include `none`, `unsupported_exact_sort`,
                `distributed_k_way_merge`, `stored_json_debug`,
                `id_candidate_order`, `id_primary_key_seek`, `score_top_k`,
                `index_sort_sorted_segment_seek`, `sorted_segment_seek`,
                `doc_values_collector`, `index_sort_unavailable_doc_values_collector`,
                `caller_selected_doc_values_collector`, and
                `selective_filter_doc_values_collector`.
            require_native (bool | Unset): Whether exact execution required native typed sort values.
            sort_lifecycle_state (SortProfileSortLifecycleState | Unset): Conservative lifecycle state for the requested
                sort path. Queryable fields are accepted by public exact sort; accelerated fields are queryable and have an
                index_sort-compatible physical path.
            index_sort_coverage (str | Unset): Physical index_sort coverage status for the requested order. Known
                values include `request_mismatch`, `no_live_segments`,
                `missing_segment_index_sort`, `covered_without_bounds`, and
                `covered_with_bounds`.
            candidate_count (int | Unset): Candidate documents considered by sort execution.
            cursor_rejected_count (int | Unset): Candidates rejected by cursor comparison.
            selected_count (int | Unset): Hits selected for the returned page.
            total_us (int | Unset): Total sort execution time in microseconds.
            distributed_shard_count (int | Unset): Shards participating in distributed sort execution.
            budget_rejection_reason (str | Unset): Stable budget rejection reason. Known values include
                `text_exact_late_visibility_totals`,
                `text_field_sort_candidate_window`,
                `match_all_candidate_collect_limit`,
                `match_all_exact_candidate_window`,
                `sorted_segment_scan_window`, and
                `distributed_merge_shard_window`.
            sort_rejection_reason (str | Unset): Stable public exact-sort rejection reason. Known values include
                `unmapped_field`, `non_sortable_field`,
                `unsupported_sort_field`, `mixed_field_type`,
                `field_not_sort_ready`, `filter_not_queryable`,
                `invalid_cursor_arity`, `invalid_cursor_type`,
                `invalid_sort_tuple`, `approximate_candidate_source`,
                `candidate_budget_exceeded`, `missing_null_policy`,
                `non_score_bearing_source`, `invalid_score_value`,
                `count_only_ordered_page`, `stored_json_sort_disabled`,
                `unsupported_exact_sort`, and
                `distributed_merge_unsupported`.
            sort_rejection_detail (str | Unset): Stable rejection detail. Known exact-sort details include
                `unmapped_sort_field`, `unmapped_field`,
                `non_sortable_sort_field`, `non_scalar_field`,
                `non_sortable_field`, `mixed_field_type`,
                `missing_doc_values_section`, `malformed_doc_values_section`,
                `doc_values_kind_mismatch`, `sparse_live_doc_values`,
                `invalid_doc_value_doc_id`, `duplicate_doc_value_doc_id`,
                `unsupported_doc_values_type`, `missing_doc_values_coverage`,
                `missing_doc_values_capability`, `schema_declared`,
                `observed_declared`, `not_declared`, `missing_doc_values`,
                `non_sortable`, `declared`, `text_search_only`, `mixed`,
                `missing_native_filter_coverage`, `invalid_cursor_arity`,
                `invalid_cursor_type`, `invalid_sort_tuple`,
                `sort_tuple_arity`, `invalid_doc_value_type`,
                `missing_runtime_mapping`, `incomplete_sort_tuple`,
                `mixed_sort_value_domain`, `unsorted_shard_window`,
                `unsorted_component_window`,
                `non_numeric_score`, `missing_score`, `non_finite_score`,
                `score_sort_tuple_mismatch`, `non_score_bearing_source`,
                `id_tiebreaker_mismatch`, `approximate_candidate_source`,
                `count_only_ordered_page`, `native_sort_loader_unavailable`,
                `sorted_segment_executor_unavailable`,
                `primary_key_stream_unavailable`,
                `native_candidate_stream_unavailable`,
                `candidate_stream_unavailable`, `incompatible_sort_plan`,
                `sorted_segment_bounds_unavailable`,
                `filter_query_json_unresolved`,
                `exclusion_query_json_unresolved`,
                `text_index_entry_unavailable`,
                `doc_ordinal_projection_unavailable`,
                `component_sort_profile_missing`,
                `unsupported_composed_sort_source`,
                `stored_json_sort_disabled`, `distributed_merge_unsupported`,
                `distributed_merge_plan_required`,
                `distributed_shard_window_incomplete`,
                `distributed_shard_cursor_window_invalid`, and
                `distributed_merge_shard_window`.
            sort_rejection_field (str | Unset): Sort field associated with the rejection when safe to expose.
    """

    plan: str | Unset = UNSET
    order_by: list[SortField] | Unset = UNSET
    cursor: str | Unset = UNSET
    exactness: str | Unset = UNSET
    source: str | Unset = UNSET
    candidate_source: SortProfileCandidateSource | Unset = UNSET
    cursor_support: str | Unset = UNSET
    source_load: str | Unset = UNSET
    distributed_behavior: str | Unset = UNSET
    selection_reason: str | Unset = UNSET
    require_native: bool | Unset = UNSET
    sort_lifecycle_state: SortProfileSortLifecycleState | Unset = UNSET
    index_sort_coverage: str | Unset = UNSET
    candidate_count: int | Unset = UNSET
    cursor_rejected_count: int | Unset = UNSET
    selected_count: int | Unset = UNSET
    total_us: int | Unset = UNSET
    distributed_shard_count: int | Unset = UNSET
    budget_rejection_reason: str | Unset = UNSET
    sort_rejection_reason: str | Unset = UNSET
    sort_rejection_detail: str | Unset = UNSET
    sort_rejection_field: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        plan = self.plan

        order_by: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.order_by, Unset):
            order_by = []
            for order_by_item_data in self.order_by:
                order_by_item = order_by_item_data.to_dict()
                order_by.append(order_by_item)

        cursor = self.cursor

        exactness = self.exactness

        source = self.source

        candidate_source: str | Unset = UNSET
        if not isinstance(self.candidate_source, Unset):
            candidate_source = self.candidate_source.value

        cursor_support = self.cursor_support

        source_load = self.source_load

        distributed_behavior = self.distributed_behavior

        selection_reason = self.selection_reason

        require_native = self.require_native

        sort_lifecycle_state: str | Unset = UNSET
        if not isinstance(self.sort_lifecycle_state, Unset):
            sort_lifecycle_state = self.sort_lifecycle_state.value

        index_sort_coverage = self.index_sort_coverage

        candidate_count = self.candidate_count

        cursor_rejected_count = self.cursor_rejected_count

        selected_count = self.selected_count

        total_us = self.total_us

        distributed_shard_count = self.distributed_shard_count

        budget_rejection_reason = self.budget_rejection_reason

        sort_rejection_reason = self.sort_rejection_reason

        sort_rejection_detail = self.sort_rejection_detail

        sort_rejection_field = self.sort_rejection_field

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if plan is not UNSET:
            field_dict["plan"] = plan
        if order_by is not UNSET:
            field_dict["order_by"] = order_by
        if cursor is not UNSET:
            field_dict["cursor"] = cursor
        if exactness is not UNSET:
            field_dict["exactness"] = exactness
        if source is not UNSET:
            field_dict["source"] = source
        if candidate_source is not UNSET:
            field_dict["candidate_source"] = candidate_source
        if cursor_support is not UNSET:
            field_dict["cursor_support"] = cursor_support
        if source_load is not UNSET:
            field_dict["source_load"] = source_load
        if distributed_behavior is not UNSET:
            field_dict["distributed_behavior"] = distributed_behavior
        if selection_reason is not UNSET:
            field_dict["selection_reason"] = selection_reason
        if require_native is not UNSET:
            field_dict["require_native"] = require_native
        if sort_lifecycle_state is not UNSET:
            field_dict["sort_lifecycle_state"] = sort_lifecycle_state
        if index_sort_coverage is not UNSET:
            field_dict["index_sort_coverage"] = index_sort_coverage
        if candidate_count is not UNSET:
            field_dict["candidate_count"] = candidate_count
        if cursor_rejected_count is not UNSET:
            field_dict["cursor_rejected_count"] = cursor_rejected_count
        if selected_count is not UNSET:
            field_dict["selected_count"] = selected_count
        if total_us is not UNSET:
            field_dict["total_us"] = total_us
        if distributed_shard_count is not UNSET:
            field_dict["distributed_shard_count"] = distributed_shard_count
        if budget_rejection_reason is not UNSET:
            field_dict["budget_rejection_reason"] = budget_rejection_reason
        if sort_rejection_reason is not UNSET:
            field_dict["sort_rejection_reason"] = sort_rejection_reason
        if sort_rejection_detail is not UNSET:
            field_dict["sort_rejection_detail"] = sort_rejection_detail
        if sort_rejection_field is not UNSET:
            field_dict["sort_rejection_field"] = sort_rejection_field

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.sort_field import SortField

        d = dict(src_dict)
        plan = d.pop("plan", UNSET)

        _order_by = d.pop("order_by", UNSET)
        order_by: list[SortField] | Unset = UNSET
        if _order_by is not UNSET:
            order_by = []
            for order_by_item_data in _order_by:
                order_by_item = SortField.from_dict(order_by_item_data)

                order_by.append(order_by_item)

        cursor = d.pop("cursor", UNSET)

        exactness = d.pop("exactness", UNSET)

        source = d.pop("source", UNSET)

        _candidate_source = d.pop("candidate_source", UNSET)
        candidate_source: SortProfileCandidateSource | Unset
        if isinstance(_candidate_source, Unset):
            candidate_source = UNSET
        else:
            candidate_source = SortProfileCandidateSource(_candidate_source)

        cursor_support = d.pop("cursor_support", UNSET)

        source_load = d.pop("source_load", UNSET)

        distributed_behavior = d.pop("distributed_behavior", UNSET)

        selection_reason = d.pop("selection_reason", UNSET)

        require_native = d.pop("require_native", UNSET)

        _sort_lifecycle_state = d.pop("sort_lifecycle_state", UNSET)
        sort_lifecycle_state: SortProfileSortLifecycleState | Unset
        if isinstance(_sort_lifecycle_state, Unset):
            sort_lifecycle_state = UNSET
        else:
            sort_lifecycle_state = SortProfileSortLifecycleState(_sort_lifecycle_state)

        index_sort_coverage = d.pop("index_sort_coverage", UNSET)

        candidate_count = d.pop("candidate_count", UNSET)

        cursor_rejected_count = d.pop("cursor_rejected_count", UNSET)

        selected_count = d.pop("selected_count", UNSET)

        total_us = d.pop("total_us", UNSET)

        distributed_shard_count = d.pop("distributed_shard_count", UNSET)

        budget_rejection_reason = d.pop("budget_rejection_reason", UNSET)

        sort_rejection_reason = d.pop("sort_rejection_reason", UNSET)

        sort_rejection_detail = d.pop("sort_rejection_detail", UNSET)

        sort_rejection_field = d.pop("sort_rejection_field", UNSET)

        sort_profile = cls(
            plan=plan,
            order_by=order_by,
            cursor=cursor,
            exactness=exactness,
            source=source,
            candidate_source=candidate_source,
            cursor_support=cursor_support,
            source_load=source_load,
            distributed_behavior=distributed_behavior,
            selection_reason=selection_reason,
            require_native=require_native,
            sort_lifecycle_state=sort_lifecycle_state,
            index_sort_coverage=index_sort_coverage,
            candidate_count=candidate_count,
            cursor_rejected_count=cursor_rejected_count,
            selected_count=selected_count,
            total_us=total_us,
            distributed_shard_count=distributed_shard_count,
            budget_rejection_reason=budget_rejection_reason,
            sort_rejection_reason=sort_rejection_reason,
            sort_rejection_detail=sort_rejection_detail,
            sort_rejection_field=sort_rejection_field,
        )

        return sort_profile
