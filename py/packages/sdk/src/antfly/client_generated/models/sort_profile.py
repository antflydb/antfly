from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.sort_profile_native_filter_mode import SortProfileNativeFilterMode
from ..models.sort_profile_sort_lifecycle_state import SortProfileSortLifecycleState
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.sort_field import SortField


T = TypeVar("T", bound="SortProfile")


@_attrs_define
class SortProfile:
    """Sort execution profile. The fields below are the stable public
    diagnostic surface; profiling responses may include additional
    implementation counters.

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
            exactness (str | Unset): Exactness class for the selected plan.
            source (str | Unset): Candidate source used by the selected plan.
            cursor_support (str | Unset): Cursor support level for the selected plan.
            source_load (str | Unset): Stored source load strategy.
            distributed_behavior (str | Unset): Distributed sort behavior.
            selection_reason (str | Unset): Stable reason the planner selected this sort plan.
            require_native (bool | Unset): Whether exact execution required native typed sort values.
            native_loader (bool | Unset): Whether a native typed sort value loader was active.
            sort_lifecycle_state (SortProfileSortLifecycleState | Unset): Conservative lifecycle state for the requested
                sort path. Queryable fields are accepted by public exact sort; accelerated fields are queryable and have an
                index_sort-compatible physical path.
            native_filter_mode (SortProfileNativeFilterMode | Unset): Native filter constraint shape available to sort
                planning for this request.
            native_filter_candidate_count (int | Unset): Number of resolved native positive-filter candidates available to
                the sort executor.
            native_filter_exclusion_count (int | Unset): Number of resolved native exclusion candidates available to the
                sort executor.
            selective_filter_doc_values_preferred (bool | Unset): Whether the planner preferred candidate-first doc-values
                collection over sorted-segment scanning because a native filter was selective.
            native_doc_values_coverage (str | Unset): Native typed doc-values coverage status for mapped sort fields.
            index_sort_coverage (str | Unset): Physical index_sort coverage status for the requested order.
            index_sort_match (bool | Unset): Whether the requested order matched the configured physical index_sort prefix.
            sorted_segment_executor_available (bool | Unset): Whether sorted-segment execution was available for this
                request.
            sorted_segment_bounds_available (bool | Unset): Whether sorted-segment bounds were available for cursor seeks.
            sorted_segment_scanned_count (int | Unset): Physical sorted-segment documents scanned before deleted-doc,
                cursor, membership, and filter checks.
            sorted_segment_scan_budget (int | Unset): Maximum physical sorted-segment documents allowed before the
                sorted_segment_scan_window budget rejection.
            candidate_count (int | Unset): Candidate documents considered by sort execution.
            cursor_rejected_count (int | Unset): Candidates rejected by cursor comparison.
            admitted_count (int | Unset): Candidate hits admitted to the sort window.
            replaced_count (int | Unset): Candidate hits replaced in the bounded sort window.
            discarded_count (int | Unset): Candidate hits discarded by the bounded sort window.
            selected_count (int | Unset): Hits selected for the returned page.
            decorate_us (int | Unset): Time spent decorating hits with sort values, in microseconds.
            native_doc_value_load_us (int | Unset): Time spent loading native typed doc values, in microseconds.
            native_doc_value_hit_count (int | Unset): Native typed doc-value loads that returned a value.
            native_doc_value_miss_count (int | Unset): Native typed doc-value loads that missed and had to fail or fall
                back.
            stored_json_load_us (int | Unset): Time spent loading stored JSON for debug sort paths, in microseconds.
            stored_json_load_count (int | Unset): Stored JSON loads performed by sort execution.
            projected_source_load_us (int | Unset): Time spent loading projected source after page selection, in
                microseconds.
            projected_source_load_count (int | Unset): Projected source documents loaded after page selection.
            final_sort_us (int | Unset): Time spent in the final in-memory page/window sort, in microseconds.
            total_us (int | Unset): Total sort execution time in microseconds.
            window_capacity (int | Unset): Capacity of the bounded sort window.
            window_len (int | Unset): Number of hits retained in the bounded sort window.
            collector_heap_peak (int | Unset): Peak collector heap size observed during sort execution.
            distributed_shard_count (int | Unset): Shards participating in distributed sort execution.
            distributed_shard_window (int | Unset): Largest shard-local sorted window merged by the coordinator.
            budget_rejection_reason (str | Unset): Stable budget rejection reason. Known values include
                `text_exact_late_visibility_totals`,
                `text_field_sort_candidate_window`,
                `match_all_candidate_collect_limit`,
                `match_all_exact_candidate_window`,
                `sorted_segment_scan_window`, and
                `distributed_merge_shard_window`.
            sort_rejection_reason (str | Unset): Stable exact-sort rejection reason. Known values include
                `unmapped_sort_field`, `non_sortable_sort_field`,
                `missing_doc_values_coverage`,
                `missing_native_filter_coverage`, `invalid_cursor_arity`,
                `invalid_cursor_type`, `invalid_sort_tuple`,
                `approximate_candidate_source`, `candidate_budget_exceeded`,
                `missing_runtime_mapping`, `invalid_doc_value_type`,
                `missing_null_policy`, `non_score_bearing_source`,
                `invalid_score_value`, `count_only_ordered_page`,
                `stored_json_sort_disabled`, `unsupported_exact_sort`, and
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
                `incomplete_sort_tuple`, `mixed_sort_value_domain`,
                `unsorted_shard_window`, `unsorted_component_window`,
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
    cursor_support: str | Unset = UNSET
    source_load: str | Unset = UNSET
    distributed_behavior: str | Unset = UNSET
    selection_reason: str | Unset = UNSET
    require_native: bool | Unset = UNSET
    native_loader: bool | Unset = UNSET
    sort_lifecycle_state: SortProfileSortLifecycleState | Unset = UNSET
    native_filter_mode: SortProfileNativeFilterMode | Unset = UNSET
    native_filter_candidate_count: int | Unset = UNSET
    native_filter_exclusion_count: int | Unset = UNSET
    selective_filter_doc_values_preferred: bool | Unset = UNSET
    native_doc_values_coverage: str | Unset = UNSET
    index_sort_coverage: str | Unset = UNSET
    index_sort_match: bool | Unset = UNSET
    sorted_segment_executor_available: bool | Unset = UNSET
    sorted_segment_bounds_available: bool | Unset = UNSET
    sorted_segment_scanned_count: int | Unset = UNSET
    sorted_segment_scan_budget: int | Unset = UNSET
    candidate_count: int | Unset = UNSET
    cursor_rejected_count: int | Unset = UNSET
    admitted_count: int | Unset = UNSET
    replaced_count: int | Unset = UNSET
    discarded_count: int | Unset = UNSET
    selected_count: int | Unset = UNSET
    decorate_us: int | Unset = UNSET
    native_doc_value_load_us: int | Unset = UNSET
    native_doc_value_hit_count: int | Unset = UNSET
    native_doc_value_miss_count: int | Unset = UNSET
    stored_json_load_us: int | Unset = UNSET
    stored_json_load_count: int | Unset = UNSET
    projected_source_load_us: int | Unset = UNSET
    projected_source_load_count: int | Unset = UNSET
    final_sort_us: int | Unset = UNSET
    total_us: int | Unset = UNSET
    window_capacity: int | Unset = UNSET
    window_len: int | Unset = UNSET
    collector_heap_peak: int | Unset = UNSET
    distributed_shard_count: int | Unset = UNSET
    distributed_shard_window: int | Unset = UNSET
    budget_rejection_reason: str | Unset = UNSET
    sort_rejection_reason: str | Unset = UNSET
    sort_rejection_detail: str | Unset = UNSET
    sort_rejection_field: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

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

        cursor_support = self.cursor_support

        source_load = self.source_load

        distributed_behavior = self.distributed_behavior

        selection_reason = self.selection_reason

        require_native = self.require_native

        native_loader = self.native_loader

        sort_lifecycle_state: str | Unset = UNSET
        if not isinstance(self.sort_lifecycle_state, Unset):
            sort_lifecycle_state = self.sort_lifecycle_state.value

        native_filter_mode: str | Unset = UNSET
        if not isinstance(self.native_filter_mode, Unset):
            native_filter_mode = self.native_filter_mode.value

        native_filter_candidate_count = self.native_filter_candidate_count

        native_filter_exclusion_count = self.native_filter_exclusion_count

        selective_filter_doc_values_preferred = self.selective_filter_doc_values_preferred

        native_doc_values_coverage = self.native_doc_values_coverage

        index_sort_coverage = self.index_sort_coverage

        index_sort_match = self.index_sort_match

        sorted_segment_executor_available = self.sorted_segment_executor_available

        sorted_segment_bounds_available = self.sorted_segment_bounds_available

        sorted_segment_scanned_count = self.sorted_segment_scanned_count

        sorted_segment_scan_budget = self.sorted_segment_scan_budget

        candidate_count = self.candidate_count

        cursor_rejected_count = self.cursor_rejected_count

        admitted_count = self.admitted_count

        replaced_count = self.replaced_count

        discarded_count = self.discarded_count

        selected_count = self.selected_count

        decorate_us = self.decorate_us

        native_doc_value_load_us = self.native_doc_value_load_us

        native_doc_value_hit_count = self.native_doc_value_hit_count

        native_doc_value_miss_count = self.native_doc_value_miss_count

        stored_json_load_us = self.stored_json_load_us

        stored_json_load_count = self.stored_json_load_count

        projected_source_load_us = self.projected_source_load_us

        projected_source_load_count = self.projected_source_load_count

        final_sort_us = self.final_sort_us

        total_us = self.total_us

        window_capacity = self.window_capacity

        window_len = self.window_len

        collector_heap_peak = self.collector_heap_peak

        distributed_shard_count = self.distributed_shard_count

        distributed_shard_window = self.distributed_shard_window

        budget_rejection_reason = self.budget_rejection_reason

        sort_rejection_reason = self.sort_rejection_reason

        sort_rejection_detail = self.sort_rejection_detail

        sort_rejection_field = self.sort_rejection_field

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
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
        if native_loader is not UNSET:
            field_dict["native_loader"] = native_loader
        if sort_lifecycle_state is not UNSET:
            field_dict["sort_lifecycle_state"] = sort_lifecycle_state
        if native_filter_mode is not UNSET:
            field_dict["native_filter_mode"] = native_filter_mode
        if native_filter_candidate_count is not UNSET:
            field_dict["native_filter_candidate_count"] = native_filter_candidate_count
        if native_filter_exclusion_count is not UNSET:
            field_dict["native_filter_exclusion_count"] = native_filter_exclusion_count
        if selective_filter_doc_values_preferred is not UNSET:
            field_dict["selective_filter_doc_values_preferred"] = selective_filter_doc_values_preferred
        if native_doc_values_coverage is not UNSET:
            field_dict["native_doc_values_coverage"] = native_doc_values_coverage
        if index_sort_coverage is not UNSET:
            field_dict["index_sort_coverage"] = index_sort_coverage
        if index_sort_match is not UNSET:
            field_dict["index_sort_match"] = index_sort_match
        if sorted_segment_executor_available is not UNSET:
            field_dict["sorted_segment_executor_available"] = sorted_segment_executor_available
        if sorted_segment_bounds_available is not UNSET:
            field_dict["sorted_segment_bounds_available"] = sorted_segment_bounds_available
        if sorted_segment_scanned_count is not UNSET:
            field_dict["sorted_segment_scanned_count"] = sorted_segment_scanned_count
        if sorted_segment_scan_budget is not UNSET:
            field_dict["sorted_segment_scan_budget"] = sorted_segment_scan_budget
        if candidate_count is not UNSET:
            field_dict["candidate_count"] = candidate_count
        if cursor_rejected_count is not UNSET:
            field_dict["cursor_rejected_count"] = cursor_rejected_count
        if admitted_count is not UNSET:
            field_dict["admitted_count"] = admitted_count
        if replaced_count is not UNSET:
            field_dict["replaced_count"] = replaced_count
        if discarded_count is not UNSET:
            field_dict["discarded_count"] = discarded_count
        if selected_count is not UNSET:
            field_dict["selected_count"] = selected_count
        if decorate_us is not UNSET:
            field_dict["decorate_us"] = decorate_us
        if native_doc_value_load_us is not UNSET:
            field_dict["native_doc_value_load_us"] = native_doc_value_load_us
        if native_doc_value_hit_count is not UNSET:
            field_dict["native_doc_value_hit_count"] = native_doc_value_hit_count
        if native_doc_value_miss_count is not UNSET:
            field_dict["native_doc_value_miss_count"] = native_doc_value_miss_count
        if stored_json_load_us is not UNSET:
            field_dict["stored_json_load_us"] = stored_json_load_us
        if stored_json_load_count is not UNSET:
            field_dict["stored_json_load_count"] = stored_json_load_count
        if projected_source_load_us is not UNSET:
            field_dict["projected_source_load_us"] = projected_source_load_us
        if projected_source_load_count is not UNSET:
            field_dict["projected_source_load_count"] = projected_source_load_count
        if final_sort_us is not UNSET:
            field_dict["final_sort_us"] = final_sort_us
        if total_us is not UNSET:
            field_dict["total_us"] = total_us
        if window_capacity is not UNSET:
            field_dict["window_capacity"] = window_capacity
        if window_len is not UNSET:
            field_dict["window_len"] = window_len
        if collector_heap_peak is not UNSET:
            field_dict["collector_heap_peak"] = collector_heap_peak
        if distributed_shard_count is not UNSET:
            field_dict["distributed_shard_count"] = distributed_shard_count
        if distributed_shard_window is not UNSET:
            field_dict["distributed_shard_window"] = distributed_shard_window
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

        cursor_support = d.pop("cursor_support", UNSET)

        source_load = d.pop("source_load", UNSET)

        distributed_behavior = d.pop("distributed_behavior", UNSET)

        selection_reason = d.pop("selection_reason", UNSET)

        require_native = d.pop("require_native", UNSET)

        native_loader = d.pop("native_loader", UNSET)

        _sort_lifecycle_state = d.pop("sort_lifecycle_state", UNSET)
        sort_lifecycle_state: SortProfileSortLifecycleState | Unset
        if isinstance(_sort_lifecycle_state, Unset):
            sort_lifecycle_state = UNSET
        else:
            sort_lifecycle_state = SortProfileSortLifecycleState(_sort_lifecycle_state)

        _native_filter_mode = d.pop("native_filter_mode", UNSET)
        native_filter_mode: SortProfileNativeFilterMode | Unset
        if isinstance(_native_filter_mode, Unset):
            native_filter_mode = UNSET
        else:
            native_filter_mode = SortProfileNativeFilterMode(_native_filter_mode)

        native_filter_candidate_count = d.pop("native_filter_candidate_count", UNSET)

        native_filter_exclusion_count = d.pop("native_filter_exclusion_count", UNSET)

        selective_filter_doc_values_preferred = d.pop("selective_filter_doc_values_preferred", UNSET)

        native_doc_values_coverage = d.pop("native_doc_values_coverage", UNSET)

        index_sort_coverage = d.pop("index_sort_coverage", UNSET)

        index_sort_match = d.pop("index_sort_match", UNSET)

        sorted_segment_executor_available = d.pop("sorted_segment_executor_available", UNSET)

        sorted_segment_bounds_available = d.pop("sorted_segment_bounds_available", UNSET)

        sorted_segment_scanned_count = d.pop("sorted_segment_scanned_count", UNSET)

        sorted_segment_scan_budget = d.pop("sorted_segment_scan_budget", UNSET)

        candidate_count = d.pop("candidate_count", UNSET)

        cursor_rejected_count = d.pop("cursor_rejected_count", UNSET)

        admitted_count = d.pop("admitted_count", UNSET)

        replaced_count = d.pop("replaced_count", UNSET)

        discarded_count = d.pop("discarded_count", UNSET)

        selected_count = d.pop("selected_count", UNSET)

        decorate_us = d.pop("decorate_us", UNSET)

        native_doc_value_load_us = d.pop("native_doc_value_load_us", UNSET)

        native_doc_value_hit_count = d.pop("native_doc_value_hit_count", UNSET)

        native_doc_value_miss_count = d.pop("native_doc_value_miss_count", UNSET)

        stored_json_load_us = d.pop("stored_json_load_us", UNSET)

        stored_json_load_count = d.pop("stored_json_load_count", UNSET)

        projected_source_load_us = d.pop("projected_source_load_us", UNSET)

        projected_source_load_count = d.pop("projected_source_load_count", UNSET)

        final_sort_us = d.pop("final_sort_us", UNSET)

        total_us = d.pop("total_us", UNSET)

        window_capacity = d.pop("window_capacity", UNSET)

        window_len = d.pop("window_len", UNSET)

        collector_heap_peak = d.pop("collector_heap_peak", UNSET)

        distributed_shard_count = d.pop("distributed_shard_count", UNSET)

        distributed_shard_window = d.pop("distributed_shard_window", UNSET)

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
            cursor_support=cursor_support,
            source_load=source_load,
            distributed_behavior=distributed_behavior,
            selection_reason=selection_reason,
            require_native=require_native,
            native_loader=native_loader,
            sort_lifecycle_state=sort_lifecycle_state,
            native_filter_mode=native_filter_mode,
            native_filter_candidate_count=native_filter_candidate_count,
            native_filter_exclusion_count=native_filter_exclusion_count,
            selective_filter_doc_values_preferred=selective_filter_doc_values_preferred,
            native_doc_values_coverage=native_doc_values_coverage,
            index_sort_coverage=index_sort_coverage,
            index_sort_match=index_sort_match,
            sorted_segment_executor_available=sorted_segment_executor_available,
            sorted_segment_bounds_available=sorted_segment_bounds_available,
            sorted_segment_scanned_count=sorted_segment_scanned_count,
            sorted_segment_scan_budget=sorted_segment_scan_budget,
            candidate_count=candidate_count,
            cursor_rejected_count=cursor_rejected_count,
            admitted_count=admitted_count,
            replaced_count=replaced_count,
            discarded_count=discarded_count,
            selected_count=selected_count,
            decorate_us=decorate_us,
            native_doc_value_load_us=native_doc_value_load_us,
            native_doc_value_hit_count=native_doc_value_hit_count,
            native_doc_value_miss_count=native_doc_value_miss_count,
            stored_json_load_us=stored_json_load_us,
            stored_json_load_count=stored_json_load_count,
            projected_source_load_us=projected_source_load_us,
            projected_source_load_count=projected_source_load_count,
            final_sort_us=final_sort_us,
            total_us=total_us,
            window_capacity=window_capacity,
            window_len=window_len,
            collector_heap_peak=collector_heap_peak,
            distributed_shard_count=distributed_shard_count,
            distributed_shard_window=distributed_shard_window,
            budget_rejection_reason=budget_rejection_reason,
            sort_rejection_reason=sort_rejection_reason,
            sort_rejection_detail=sort_rejection_detail,
            sort_rejection_field=sort_rejection_field,
        )

        sort_profile.additional_properties = d
        return sort_profile

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
