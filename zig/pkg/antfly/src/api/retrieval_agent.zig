// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const ai_openapi = @import("antfly_ai_openapi");
const eval_openapi = @import("antfly_eval_openapi");
const generating_openapi = @import("antfly_generating_openapi");
const indexes_openapi = @import("antfly_indexes_openapi");
const metadata_openapi = @import("antfly_metadata_openapi");
const generating = @import("antfly_generating");
const platform_time = @import("../platform/time.zig");
const query_api = @import("query.zig");
const query_builder_agent = @import("query_builder_agent.zig");
const json_helpers = @import("json_helpers.zig");

const AgentDecision = metadata_openapi.AgentDecision;
const AgentQuestion = metadata_openapi.AgentQuestion;
const AgentStatus = metadata_openapi.AgentStatus;
const AgentStep = metadata_openapi.AgentStep;
const QueryHit = metadata_openapi.QueryHit;
const QueryRequest = metadata_openapi.QueryRequest;
const QueryResponses = metadata_openapi.QueryResponses;
const GraphPath = indexes_openapi.Path;
const RetrievalAgentRequest = metadata_openapi.RetrievalAgentRequest;
const RetrievalAgentResult = metadata_openapi.RetrievalAgentResult;
const RetrievalQueryRequest = metadata_openapi.RetrievalQueryRequest;
const RetrievalStrategy = metadata_openapi.RetrievalStrategy;
const TreeSearchConfig = metadata_openapi.TreeSearchConfig;

pub const EncodedResponse = struct {
    content_type: []const u8,
    body: []u8,
};

pub const EventSink = struct {
    ptr: *anyopaque,
    emit_json_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8) anyerror!void,

    pub fn emitJson(self: EventSink, alloc: std.mem.Allocator, event_name: []const u8, json: []const u8) !void {
        try self.emit_json_fn(self.ptr, alloc, event_name, json);
    }

    pub fn emitValue(self: EventSink, alloc: std.mem.Allocator, event_name: []const u8, value: anytype) !void {
        const encoded = try std.json.Stringify.valueAlloc(alloc, value, .{});
        defer alloc.free(encoded);
        try self.emitJson(alloc, event_name, encoded);
    }
};

fn parseJsonBody(comptime T: type, alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(T) {
    return try std.json.parseFromSlice(T, alloc, body, .{});
}

fn parseQueryRequestBody(alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(QueryRequest) {
    return try std.json.parseFromSlice(QueryRequest, alloc, body, .{ .ignore_unknown_fields = true });
}

fn expectFullTextQueryValue(value: std.json.Value, expected: []const u8) !void {
    try std.testing.expect(value == .object);
    const query = value.object.get("query") orelse return error.TestExpectedEqual;
    try std.testing.expect(query == .string);
    try std.testing.expectEqualStrings(expected, query.string);
}

const TestSseEvent = struct {
    event: []const u8,
    data: []const u8,
};

fn parseSseEventsAlloc(alloc: std.mem.Allocator, body: []const u8) ![]TestSseEvent {
    var events = std.ArrayListUnmanaged(TestSseEvent).empty;
    errdefer events.deinit(alloc);

    var frames = std.mem.splitSequence(u8, body, "\n\n");
    while (frames.next()) |frame| {
        if (frame.len == 0) continue;
        var event_name: ?[]const u8 = null;
        var data: ?[]const u8 = null;
        var lines = std.mem.splitScalar(u8, frame, '\n');
        while (lines.next()) |line| {
            if (std.mem.startsWith(u8, line, "event: ")) {
                event_name = line["event: ".len..];
            } else if (std.mem.startsWith(u8, line, "data: ")) {
                data = line["data: ".len..];
            }
        }
        if (event_name != null and data != null) {
            try events.append(alloc, .{
                .event = event_name.?,
                .data = data.?,
            });
        }
    }

    return try events.toOwnedSlice(alloc);
}

fn countSseEvents(events: []const TestSseEvent, name: []const u8) usize {
    var count: usize = 0;
    for (events) |event| {
        if (std.mem.eql(u8, event.event, name)) count += 1;
    }
    return count;
}

fn firstSseEventData(events: []const TestSseEvent, name: []const u8) ?[]const u8 {
    for (events) |event| {
        if (std.mem.eql(u8, event.event, name)) return event.data;
    }
    return null;
}

const TestToolModeEvent = struct {
    mode: []const u8,
    tools_count: ?usize = null,
};

const TestStepProgressEvent = struct {
    id: ?[]const u8 = null,
    kind: ?[]const u8 = null,
    phase: []const u8,
    num_nodes: ?i64 = null,
    collected: ?i64 = null,
    complete: ?bool = null,
    questions: ?[]AgentQuestion = null,
    selection_source: ?[]const u8 = null,
    next_selection_source: ?[]const u8 = null,
    probe_relevance: ?f64 = null,
    probe_hits: ?i64 = null,
    sub_question: ?[]const u8 = null,
    planner_decision: ?[]const u8 = null,
    fallback_consensus_ambiguous: ?bool = null,
    generation: ?[]const u8 = null,
};

pub const QueryRunner = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        run_query: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            query_json: []const u8,
        ) anyerror!query_api.QueryResponse,
        scan_keys: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
        ) anyerror![]const []const u8 = null,
    };

    pub fn runQuery(
        self: QueryRunner,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        query_json: []const u8,
    ) !query_api.QueryResponse {
        return try self.vtable.run_query(self.ptr, alloc, table_name, query_json);
    }

    pub fn scanKeys(
        self: QueryRunner,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) ![]const []const u8 {
        const fn_ptr = self.vtable.scan_keys orelse return error.UnsupportedRetrievalAgentRequest;
        return try fn_ptr(self.ptr, alloc, table_name);
    }
};

pub const GenerationRunner = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        execute_chain: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            chain: []const generating.ChainLink,
            messages: []const generating.ChatMessage,
        ) anyerror!generating.GenerateResult,
    };

    pub fn executeChain(
        self: GenerationRunner,
        alloc: std.mem.Allocator,
        chain: []const generating.ChainLink,
        messages: []const generating.ChatMessage,
    ) !generating.GenerateResult {
        return try self.vtable.execute_chain(self.ptr, alloc, chain, messages);
    }
};

const ResponseFormat = enum {
    json,
    sse,
};

const LiveEmitter = struct {
    sink: ?EventSink = null,
    alloc: std.mem.Allocator,
    next_step_index: usize = 0,

    fn emitValue(self: *LiveEmitter, event_name: []const u8, value: anytype) !void {
        if (self.sink) |sink| try sink.emitValue(self.alloc, event_name, value);
    }

    fn emitTextChunks(self: *LiveEmitter, event_name: []const u8, text: []const u8) !void {
        if (self.sink == null) return;
        if (text.len == 0) {
            try self.emitValue(event_name, text);
            return;
        }
        const chunk_len: usize = 80;
        var start: usize = 0;
        while (start < text.len) {
            const end = @min(start + chunk_len, text.len);
            try self.emitValue(event_name, text[start..end]);
            start = end;
        }
    }

    fn emitClassification(self: *LiveEmitter, classification: ai_openapi.ClassificationTransformationResult) !void {
        try self.emitValue("classification", classification);
        if (classification.reasoning) |reasoning| try self.emitTextChunks("reasoning", reasoning);
        if (classification.sub_questions) |sub_questions| {
            for (sub_questions, 0..) |sub_question, i| {
                try self.emitValue("step_progress", .{
                    .name = "classification",
                    .phase = "decompose",
                    .index = i,
                    .sub_question = sub_question,
                });
            }
        }
    }

    fn emitStep(self: *LiveEmitter, step: AgentStep) !void {
        if (self.sink == null) return;
        const step_id = try std.fmt.allocPrint(self.alloc, "live_step_{d}", .{self.next_step_index});
        defer self.alloc.free(step_id);
        self.next_step_index += 1;

        try self.emitValue("step_started", .{
            .id = step_id,
            .kind = step.kind,
            .name = step.name,
            .action = step.action,
        });

        if (std.mem.eql(u8, step.name, "select_strategy")) {
            try self.emitTextChunks("reasoning", step.action);
            try self.emitValue("step_progress", .{
                .id = step_id,
                .kind = step.kind,
                .name = step.name,
                .phase = "select_strategy",
                .action = step.action,
                .details = step.details,
            });
        } else if (std.mem.eql(u8, step.name, "refine_query")) {
            try self.emitTextChunks("reasoning", step.action);
            try self.emitValue("step_progress", .{
                .id = step_id,
                .kind = step.kind,
                .name = step.name,
                .phase = stepProgressPhase(step.details, "refine_query"),
                .action = step.action,
                .details = step.details,
            });
        } else if (std.mem.eql(u8, step.name, "evaluate")) {
            try self.emitTextChunks("reasoning", step.action);
            try self.emitValue("step_progress", .{
                .id = step_id,
                .kind = step.kind,
                .name = step.name,
                .phase = "evaluate",
                .action = step.action,
                .details = step.details,
            });
        } else if (std.mem.eql(u8, step.name, "agentic")) {
            try self.emitValue("tool_mode", .{ .mode = "structured_output" });
            try self.emitTextChunks("reasoning", step.action);
        } else if (step.kind == .tool_call) {
            try self.emitValue("step_progress", .{
                .id = step_id,
                .kind = step.kind,
                .name = step.name,
                .phase = "tool_call",
                .action = step.action,
                .details = step.details,
            });
        } else if (std.mem.eql(u8, step.name, "clarification")) {
            try self.emitTextChunks("reasoning", step.action);
            try self.emitValue("step_progress", .{
                .id = step_id,
                .kind = step.kind,
                .name = step.name,
                .phase = "clarification",
                .action = step.action,
                .details = step.details,
            });
        }

        try self.emitValue("step_completed", .{
            .id = step_id,
            .kind = step.kind,
            .name = step.name,
            .action = step.action,
            .status = step.status,
            .details = step.details,
        });
    }

    fn emitHits(self: *LiveEmitter, hits: []const QueryHit, tree_search: bool) !void {
        if (self.sink == null) return;
        if (tree_search) {
            try self.emitValue("step_progress", .{
                .name = "pipeline",
                .phase = "tree_search",
                .depth = maxTreeHitDepth(hits),
                .num_nodes = hits.len,
                .collected = hits.len,
                .complete = true,
                .sufficient = hits.len > 0,
            });
        }
        for (hits) |hit| try self.emitValue("hit", hit);
    }

    fn emitDone(self: *LiveEmitter, result: RetrievalAgentResult) !void {
        try self.emitValue("done", result);
    }
};

fn appendStep(alloc: std.mem.Allocator, steps: *std.ArrayListUnmanaged(AgentStep), live: *LiveEmitter, step: AgentStep) !void {
    try steps.append(alloc, step);
    try live.emitStep(step);
}

fn finishAgentResult(
    alloc: std.mem.Allocator,
    format: ResponseFormat,
    result: RetrievalAgentResult,
    live: *LiveEmitter,
) !EncodedResponse {
    try live.emitDone(result);
    return try encodeAgentResult(alloc, format, result);
}

const QueryRefinementPass = enum {
    initial,
    followup,
    evaluation,
};

const AgenticFallbackPlan = struct {
    indices: []const usize,
    source: AgenticSelectionSource,
    candidate_scores: []const AgenticCandidateScore,
};

const AgenticEvaluationTrigger = enum {
    none,
    empty_result,
    weak_result,
    partial_result,
};

const AgenticPlannerDecision = enum {
    accept_result,
    expand_branch,
    refine_query,
    switch_strategy,
    clarify,
};

const AttemptEvaluationSummary = struct {
    hit_count: i64,
    top_score: ?f32 = null,
    context_relevance: ?f32 = null,
    context_length: ?i64 = null,
    top_tree_branch_relevance: ?f32 = null,
    top_tree_branch_nodes: ?i64 = null,
    top_tree_branch_leaf_hits: ?i64 = null,
};

pub fn execute(
    alloc: std.mem.Allocator,
    runner: QueryRunner,
    generation_runner: ?GenerationRunner,
    body: []const u8,
) !EncodedResponse {
    return try executeInternal(alloc, runner, generation_runner, body, null);
}

pub fn executeWithEventSink(
    alloc: std.mem.Allocator,
    runner: QueryRunner,
    generation_runner: ?GenerationRunner,
    body: []const u8,
    event_sink: EventSink,
) !EncodedResponse {
    return try executeInternal(alloc, runner, generation_runner, body, event_sink);
}

fn executeInternal(
    alloc: std.mem.Allocator,
    runner: QueryRunner,
    generation_runner: ?GenerationRunner,
    body: []const u8,
    event_sink: ?EventSink,
) !EncodedResponse {
    if (body.len == 0) return error.InvalidRetrievalAgentRequest;

    var parsed = std.json.parseFromSlice(RetrievalAgentRequest, alloc, body, .{}) catch {
        return error.InvalidRetrievalAgentRequest;
    };
    defer parsed.deinit();
    const request = parsed.value;

    var parsed_raw = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch {
        return error.InvalidRetrievalAgentRequest;
    };
    defer parsed_raw.deinit();
    const raw_queries_value = parsed_raw.value.object.get("queries") orelse return error.InvalidRetrievalAgentRequest;
    if (raw_queries_value != .array) return error.InvalidRetrievalAgentRequest;
    const raw_queries = raw_queries_value.array.items;

    const format: ResponseFormat = if (request.stream orelse false) .sse else .json;
    var live = LiveEmitter{ .sink = event_sink, .alloc = alloc };
    const max_internal_iterations: i64 = request.max_internal_iterations orelse 0;
    if (max_internal_iterations < 0) return error.InvalidRetrievalAgentRequest;
    const agentic_mode = max_internal_iterations > 0;
    if (request.accumulated_filters != null) return error.UnsupportedRetrievalAgentRequest;

    const retrieval_queries = request.queries;
    if (retrieval_queries.len == 0) return error.InvalidRetrievalAgentRequest;
    if (request.query.len == 0) return error.InvalidRetrievalAgentRequest;
    if (raw_queries.len != retrieval_queries.len) return error.InvalidRetrievalAgentRequest;

    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var hit_list = std.ArrayListUnmanaged(QueryHit).empty;
    var seen_ids = std.StringHashMapUnmanaged(void).empty;
    defer {
        hit_list.deinit(arena);
        seen_ids.deinit(arena);
    }

    var strategies = std.ArrayListUnmanaged(RetrievalStrategy).empty;
    defer strategies.deinit(arena);

    const classification_cfg = try parseClassificationConfig(request);
    const generation_cfg = try parseGenerationConfig(arena, request);
    const followup_cfg = try parseFollowupConfig(request, generation_cfg != null);
    const eval_cfg = try parseEvalConfig(arena, request, generation_cfg != null);
    const confidence_enabled = try parseConfidenceEnabled(request, generation_cfg != null);
    const clarification_state = try parseClarificationState(request);
    var steps_list = std.ArrayListUnmanaged(AgentStep).empty;
    defer steps_list.deinit(arena);
    const classification_result: ?ai_openapi.ClassificationTransformationResult = if (classification_cfg) |cfg|
        try buildClassificationResult(arena, request.query, cfg)
    else if (agentic_mode)
        try buildClassificationResult(arena, request.query, .{
            .force_strategy = preferredAgenticQueryStrategy(request),
            .force_semantic_mode = null,
            .with_reasoning = true,
        })
    else
        null;
    if (classification_result) |classification| try live.emitClassification(classification);
    var selection = if (agentic_mode)
        try selectAgenticQueries(arena, request, retrieval_queries, clarification_state)
    else
        null;
    const allow_probe_selection = if (request.require_decision_after) |limit|
        limit > 0
    else
        true;
    if (selection) |value| {
        if (allow_probe_selection and value.indices == null and value.candidate_scores != null and (value.question != null or value.incomplete_reason != null)) {
            selection = try maybeProbeAgenticSelection(
                alloc,
                arena,
                runner,
                raw_queries,
                retrieval_queries,
                classification_result,
                value,
            );
        }
    }
    const selected_query_indices = if (selection) |value| value.indices else null;
    const broadened_from_decision = decisionApproved(clarification_state.decisions, "broaden_search");

    if (classification_cfg) |cfg| {
        try appendStep(arena, &steps_list, &live, .{
            .kind = .classification,
            .name = "classification",
            .action = "classified query and selected retrieval strategy",
            .status = .success,
            .details = try buildClassificationStepDetails(arena, request, cfg, selected_query_indices),
        });
    } else if (agentic_mode) {
        try appendStep(arena, &steps_list, &live, .{
            .kind = .classification,
            .name = "classification",
            .action = "selected retrieval tools for agentic execution",
            .status = .success,
            .details = try buildAgenticSelectionDetails(arena, request, selected_query_indices),
        });
    }

    if (selection) |value| {
        if (value.question) |question| {
            try appendStep(arena, &steps_list, &live, .{
                .kind = .clarification,
                .name = "clarification",
                .action = question.question,
                .status = .success,
                .details = try buildClarificationSelectionDetails(arena, value.candidate_scores orelse &.{}),
            });

            const steps = try steps_list.toOwnedSlice(arena);
            const questions = try arena.dupe(AgentQuestion, &[_]AgentQuestion{question});
            const result = RetrievalAgentResult{
                .created_at = 0,
                .status = .clarification_required,
                .hits = &.{},
                .steps = steps,
                .strategy_used = null,
                .session_id = request.session_id,
                .iteration = 0,
                .clarification_count = clarification_state.count,
                .remaining_internal_iterations = max_internal_iterations,
                .remaining_user_clarifications = clarification_state.remaining,
                .questions = questions,
                .tool_calls_made = 0,
            };
            return try finishAgentResult(alloc, format, result, &live);
        }
        if (value.incomplete_reason) |reason| {
            try appendStep(arena, &steps_list, &live, .{
                .kind = .clarification,
                .name = "clarification",
                .action = "bounded agent stopped because a user decision was required",
                .status = .skipped,
                .details = try buildClarificationSelectionDetails(arena, value.candidate_scores orelse &.{}),
            });

            const steps = try steps_list.toOwnedSlice(arena);
            const result = RetrievalAgentResult{
                .created_at = 0,
                .status = .incomplete,
                .incomplete_details = .{ .reason = reason },
                .hits = &.{},
                .steps = steps,
                .strategy_used = null,
                .session_id = request.session_id,
                .iteration = 0,
                .clarification_count = clarification_state.count,
                .remaining_internal_iterations = max_internal_iterations,
                .remaining_user_clarifications = clarification_state.remaining,
                .tool_calls_made = 0,
            };
            return try finishAgentResult(alloc, format, result, &live);
        }
    }

    const action = if (retrieval_queries.len == 1)
        try std.fmt.allocPrint(arena, "executed 1 retrieval query", .{})
    else
        try std.fmt.allocPrint(arena, "executed {d} retrieval queries", .{retrieval_queries.len});
    try appendStep(arena, &steps_list, &live, .{
        .kind = .planning,
        .name = "pipeline",
        .action = action,
        .status = .success,
        .details = try buildPipelineStepDetails(arena, retrieval_queries, selected_query_indices, broadened_from_decision),
    });
    if (agentic_mode) {
        try appendStep(arena, &steps_list, &live, .{
            .kind = .tool_call,
            .name = "agentic",
            .action = "executed retrieval tools in bounded agentic mode",
            .status = .success,
        });
    }

    var generated_content: ?[]const u8 = null;
    var model_used: ?[]const u8 = null;
    if (agentic_mode) {
        if (selection) |value| if (value.indices) |selected| {
            try appendStep(arena, &steps_list, &live, .{
                .kind = .planning,
                .name = "select_strategy",
                .action = try buildSelectStrategyAction(arena, classification_result, retrieval_queries, selected),
                .status = .success,
                .details = try buildSelectStrategyStepDetails(
                    arena,
                    retrieval_queries,
                    selected,
                    classification_result,
                    value.source,
                    value.candidate_scores orelse &.{},
                    broadened_from_decision,
                ),
            });
        };
    }
    var generation_confidence: ?f32 = null;
    var context_relevance: ?f32 = null;
    var followup_questions: ?[]const []const u8 = null;
    var eval_result: ?eval_openapi.EvalResult = null;
    var tool_calls_made: i64 = 0;
    var iteration_count: i64 = 0;
    const selection_source = if (selection) |value| value.source else AgenticSelectionSource.heuristic;
    const candidate_scores = if (selection) |value| value.candidate_scores orelse &.{} else &.{};
    var attempted_query_indices = try arena.alloc(bool, retrieval_queries.len);
    @memset(attempted_query_indices, false);
    var planned_query_indices = std.ArrayListUnmanaged(usize).empty;
    defer planned_query_indices.deinit(arena);
    if (selected_query_indices) |selected| {
        try planned_query_indices.appendSlice(arena, selected);
    } else {
        for (retrieval_queries, 0..) |_, retrieval_query_index| {
            try planned_query_indices.append(arena, retrieval_query_index);
        }
    }

    var previous_query_hits: []const QueryHit = &.{};

    var query_cursor: usize = 0;
    while (query_cursor < planned_query_indices.items.len) : (query_cursor += 1) {
        const retrieval_query_index = planned_query_indices.items[query_cursor];
        if (attempted_query_indices[retrieval_query_index]) continue;
        attempted_query_indices[retrieval_query_index] = true;
        const retrieval_query = retrieval_queries[retrieval_query_index];
        const raw_query = raw_queries[retrieval_query_index];
        const table_name = retrieval_query.table orelse return error.InvalidRetrievalAgentRequest;
        const strategy = detectStrategy(retrieval_query);
        try strategies.append(arena, strategy);
        var refinement_queries = std.ArrayListUnmanaged([]const u8).empty;
        if (currentRetrievalQueryText(retrieval_query)) |current_query| {
            try refinement_queries.append(arena, current_query);
        }

        if (agentic_mode) {
            const tool_action = try std.fmt.allocPrint(arena, "executed retrieval tool {d} using {s} strategy", .{
                tool_calls_made + 1,
                @tagName(strategy),
            });
            try appendStep(arena, &steps_list, &live, .{
                .kind = .tool_call,
                .name = try std.fmt.allocPrint(arena, "tool_{d}", .{tool_calls_made + 1}),
                .action = tool_action,
                .status = .success,
                .details = try buildToolStepDetails(arena, retrieval_query, retrieval_query_index, strategy),
            });
        }

        if (initialRefinedQueryText(classification_result, retrieval_query, retrieval_query_index)) |refined_query| {
            try refinement_queries.append(arena, refined_query);
            try appendStep(arena, &steps_list, &live, .{
                .kind = .planning,
                .name = "refine_query",
                .action = "refined retrieval query before bounded execution",
                .status = .success,
                .details = try buildInitialRefineQueryStepDetails(arena, retrieval_query, retrieval_query_index, classification_result.?, refined_query),
            });
        }

        const query_json = try encodeQueryValueForRetrievalQuery(alloc, runner, raw_query, retrieval_query, previous_query_hits, classification_result, retrieval_query_index, .initial);
        defer alloc.free(query_json);

        const query_hits = runQueryAndExtractHits(alloc, arena, runner, table_name, query_json, request.query, retrieval_query.tree_search != null, true) catch |err| switch (format) {
            .sse => return .{
                .content_type = "text/event-stream",
                .body = try encodeSseError(alloc, @errorName(err)),
            },
            .json => return err,
        };
        var evaluation_hits = query_hits;
        previous_query_hits = query_hits;
        tool_calls_made += 1;
        iteration_count += 1;
        try accumulateHits(arena, &hit_list, &seen_ids, query_hits);
        try live.emitHits(query_hits, retrieval_query.tree_search != null);

        if (shouldRunStepBackFollowup(agentic_mode, max_internal_iterations, tool_calls_made, classification_result, retrieval_query)) {
            try appendStep(arena, &steps_list, &live, .{
                .kind = .planning,
                .name = "refine_query",
                .action = "refined retrieval query after bounded step-back context gathering",
                .status = .success,
                .details = try buildRefineQueryStepDetails(arena, retrieval_query, retrieval_query_index),
            });

            const followup_query_json = try encodeQueryValueForRetrievalQuery(
                alloc,
                runner,
                raw_query,
                retrieval_query,
                previous_query_hits,
                classification_result,
                retrieval_query_index,
                .followup,
            );
            defer alloc.free(followup_query_json);

            const followup_hits = runQueryAndExtractHits(alloc, arena, runner, table_name, followup_query_json, request.query, retrieval_query.tree_search != null, false) catch |err| switch (format) {
                .sse => return .{
                    .content_type = "text/event-stream",
                    .body = try encodeSseError(alloc, @errorName(err)),
                },
                .json => return err,
            };
            evaluation_hits = followup_hits;
            previous_query_hits = followup_hits;
            tool_calls_made += 1;
            iteration_count += 1;

            const followup_strategy = detectStrategy(retrieval_query);
            const tool_action = try std.fmt.allocPrint(arena, "executed retrieval tool {d} using {s} strategy after refinement", .{
                tool_calls_made,
                @tagName(followup_strategy),
            });
            try appendStep(arena, &steps_list, &live, .{
                .kind = .tool_call,
                .name = try std.fmt.allocPrint(arena, "tool_{d}", .{tool_calls_made}),
                .action = tool_action,
                .status = .success,
                .details = try buildToolStepDetails(arena, retrieval_query, retrieval_query_index, followup_strategy),
            });

            try accumulateHits(arena, &hit_list, &seen_ids, followup_hits);
            try live.emitHits(followup_hits, retrieval_query.tree_search != null);
        }

        const plan_exhausted = query_cursor + 1 >= planned_query_indices.items.len;
        var attempt_summary = summarizeAttemptEvaluation(arena, request.query, evaluation_hits);
        var pending_tree_expansion_plan: ?TreeBranchExpansionPlan = if (retrieval_query.tree_search) |tree_search|
            try selectTreeBranchExpansionPlan(arena, request.query, tree_search, evaluation_hits)
        else
            null;
        var evaluation_trigger = if (agentic_mode and plan_exhausted and tool_calls_made < max_internal_iterations)
            detectAgenticEvaluationTrigger(
                request.query,
                classification_result,
                strategy,
                attempt_summary,
                candidate_scores,
                attempted_query_indices,
            )
        else
            .none;
        var previous_attempt_summary: ?AttemptEvaluationSummary = null;

        const planner_can_clarify = clarification_state.interactive and clarification_state.remaining > 0;
        while (agentic_mode and tool_calls_made < max_internal_iterations) {
            const can_attempt_refinement = switch (strategy) {
                .bm25, .metadata => true,
                .semantic, .hybrid, .tree => evaluation_trigger == .partial_result,
                .graph => false,
            };
            const planner_decision = decideAgenticPlannerAction(
                evaluation_trigger,
                strategy,
                attempt_summary,
                previous_attempt_summary,
                candidate_scores,
                attempted_query_indices,
                can_attempt_refinement and nextEvaluationRefinedQueryText(classification_result, retrieval_query, retrieval_query_index, refinement_queries.items) != null,
                strategy == .tree and pending_tree_expansion_plan != null,
                planner_can_clarify,
            );
            if (planner_decision == .expand_branch) {
                const tree_plan = pending_tree_expansion_plan orelse break;
                const tree_search = retrieval_query.tree_search orelse break;
                var expanded_query = retrieval_query;
                var expanded_tree = tree_search;
                expanded_tree.start_nodes = tree_plan.seed_key;
                expanded_tree.max_depth = tree_plan.max_depth;
                expanded_query.tree_search = expanded_tree;

                try appendStep(arena, &steps_list, &live, .{
                    .kind = .planning,
                    .name = "evaluate",
                    .action = try buildEvaluationTreeExpansionActionText(arena, evaluation_trigger),
                    .status = .success,
                    .details = try buildEvaluationTreeExpansionStepDetails(
                        arena,
                        retrieval_query,
                        retrieval_query_index,
                        @max(@as(i64, 0), max_internal_iterations - tool_calls_made),
                        evaluation_trigger,
                        attempt_summary,
                        bestRemainingCandidateScore(candidate_scores, attempted_query_indices),
                        tree_plan,
                    ),
                });
                try appendStep(arena, &steps_list, &live, .{
                    .kind = .planning,
                    .name = "tree_search",
                    .action = "continued tree search on the strongest branch after query-aware evaluation",
                    .status = .success,
                    .details = try buildTreeExpansionStepDetails(arena, retrieval_query, retrieval_query_index, tree_plan),
                });

                const expanded_query_json = try encodeQueryValueForRetrievalQuery(
                    alloc,
                    runner,
                    raw_query,
                    expanded_query,
                    evaluation_hits,
                    classification_result,
                    retrieval_query_index,
                    .followup,
                );
                defer alloc.free(expanded_query_json);

                const expanded_hits = runQueryAndExtractHits(alloc, arena, runner, table_name, expanded_query_json, request.query, true, false) catch |err| switch (format) {
                    .sse => return .{
                        .content_type = "text/event-stream",
                        .body = try encodeSseError(alloc, @errorName(err)),
                    },
                    .json => return err,
                };
                const merged_hits = try mergeTreeHits(arena, request.query, evaluation_hits, expanded_hits);
                evaluation_hits = merged_hits;
                previous_query_hits = merged_hits;
                tool_calls_made += 1;
                iteration_count += 1;

                const expanded_tool_action = try std.fmt.allocPrint(arena, "executed retrieval tool {d} using tree strategy after branch expansion", .{
                    tool_calls_made,
                });
                try appendStep(arena, &steps_list, &live, .{
                    .kind = .tool_call,
                    .name = try std.fmt.allocPrint(arena, "tool_{d}", .{tool_calls_made}),
                    .action = expanded_tool_action,
                    .status = .success,
                    .details = try buildToolStepDetails(arena, expanded_query, retrieval_query_index, .tree),
                });

                try accumulateHits(arena, &hit_list, &seen_ids, expanded_hits);
                try live.emitHits(expanded_hits, true);

                previous_attempt_summary = attempt_summary;
                attempt_summary = summarizeAttemptEvaluation(arena, request.query, evaluation_hits);
                pending_tree_expansion_plan = null;
                evaluation_trigger = if (agentic_mode and plan_exhausted and tool_calls_made < max_internal_iterations)
                    detectAgenticEvaluationTrigger(
                        request.query,
                        classification_result,
                        strategy,
                        attempt_summary,
                        candidate_scores,
                        attempted_query_indices,
                    )
                else
                    .none;
                continue;
            }
            if (planner_decision != .refine_query) break;
            if (nextEvaluationRefinedQueryText(classification_result, retrieval_query, retrieval_query_index, refinement_queries.items)) |refined_query| {
                try refinement_queries.append(arena, refined_query);
                try appendStep(arena, &steps_list, &live, .{
                    .kind = .planning,
                    .name = "evaluate",
                    .action = try buildEvaluationRefinementActionText(arena, evaluation_trigger),
                    .status = .success,
                    .details = try buildEvaluationRefinementStepDetails(
                        arena,
                        retrieval_query,
                        retrieval_query_index,
                        @max(@as(i64, 0), max_internal_iterations - tool_calls_made),
                        evaluation_trigger,
                        attempt_summary,
                        bestRemainingCandidateScore(candidate_scores, attempted_query_indices),
                    ),
                });
                try appendStep(arena, &steps_list, &live, .{
                    .kind = .planning,
                    .name = "refine_query",
                    .action = try buildEvaluationRefineQueryActionText(arena, evaluation_trigger),
                    .status = .success,
                    .details = try buildEvaluationRefineQueryStepDetails(
                        arena,
                        retrieval_query,
                        retrieval_query_index,
                        classification_result.?,
                        refined_query,
                    ),
                });

                const refined_query_json = try encodeQueryValueForRetrievalQuery(
                    alloc,
                    runner,
                    raw_query,
                    retrieval_query,
                    previous_query_hits,
                    classification_result,
                    retrieval_query_index,
                    .evaluation,
                );
                defer alloc.free(refined_query_json);

                const refined_hits = runQueryAndExtractHits(alloc, arena, runner, table_name, refined_query_json, request.query, retrieval_query.tree_search != null, false) catch |err| switch (format) {
                    .sse => return .{
                        .content_type = "text/event-stream",
                        .body = try encodeSseError(alloc, @errorName(err)),
                    },
                    .json => return err,
                };
                evaluation_hits = refined_hits;
                previous_query_hits = refined_hits;
                tool_calls_made += 1;
                iteration_count += 1;

                const refined_tool_action = try std.fmt.allocPrint(arena, "executed retrieval tool {d} using {s} strategy after evaluation-driven refinement", .{
                    tool_calls_made,
                    @tagName(strategy),
                });
                try appendStep(arena, &steps_list, &live, .{
                    .kind = .tool_call,
                    .name = try std.fmt.allocPrint(arena, "tool_{d}", .{tool_calls_made}),
                    .action = refined_tool_action,
                    .status = .success,
                    .details = try buildToolStepDetails(arena, retrieval_query, retrieval_query_index, strategy),
                });

                try accumulateHits(arena, &hit_list, &seen_ids, refined_hits);
                try live.emitHits(refined_hits, retrieval_query.tree_search != null);

                previous_attempt_summary = attempt_summary;
                attempt_summary = summarizeAttemptEvaluation(arena, request.query, evaluation_hits);
                pending_tree_expansion_plan = if (retrieval_query.tree_search) |tree_search|
                    try selectTreeBranchExpansionPlan(arena, request.query, tree_search, evaluation_hits)
                else
                    null;
                evaluation_trigger = if (agentic_mode and plan_exhausted and tool_calls_made < max_internal_iterations)
                    detectAgenticEvaluationTrigger(
                        request.query,
                        classification_result,
                        strategy,
                        attempt_summary,
                        candidate_scores,
                        attempted_query_indices,
                    )
                else
                    .none;
                continue;
            }
            break;
        }

        if (evaluation_trigger != .none) {
            const allow_agentic_fallback = switch (selection_source) {
                .broaden_decision, .decompose => false,
                .user_decision => evaluation_trigger == .weak_result,
                else => true,
            };
            if (allow_agentic_fallback) {
                if (try planNextAgenticFallback(
                    alloc,
                    arena,
                    runner,
                    raw_queries,
                    retrieval_queries,
                    classification_result,
                    candidate_scores,
                    attempted_query_indices,
                )) |fallback_plan| {
                    const planner_decision = decideAgenticPlannerAction(
                        evaluation_trigger,
                        strategy,
                        attempt_summary,
                        previous_attempt_summary,
                        fallback_plan.candidate_scores,
                        attempted_query_indices,
                        false,
                        false,
                        planner_can_clarify,
                    );
                    if (planner_decision == .accept_result) {
                        try appendStep(arena, &steps_list, &live, .{
                            .kind = .planning,
                            .name = "evaluate",
                            .action = "evaluated retrieval result and kept the current bounded strategy",
                            .status = .success,
                            .details = try buildEvaluationAcceptStepDetails(
                                arena,
                                retrieval_query,
                                retrieval_query_index,
                                evaluation_trigger,
                                attempt_summary,
                                attemptPlannerScore(attempt_summary, strategy),
                                if (previous_attempt_summary) |previous| attemptPlannerScore(previous, strategy) else null,
                                bestRemainingCandidateScore(fallback_plan.candidate_scores, attempted_query_indices),
                            ),
                        });
                    } else if (planner_decision == .clarify) {
                        const clarification_indices = try collectRemainingCandidateIndices(arena, fallback_plan.candidate_scores, attempted_query_indices);
                        var clarification_details = try buildEvaluationStepDetails(
                            arena,
                            attempted_query_indices,
                            retrieval_queries,
                            @max(@as(i64, 0), max_internal_iterations - tool_calls_made),
                            clarification_indices,
                            fallback_plan.source,
                            evaluation_trigger,
                            strategy,
                            attempt_summary,
                            previous_attempt_summary,
                            fallback_plan.candidate_scores,
                        );
                        try clarification_details.object.put(alloc, "planner_decision", .{ .string = "clarify" });
                        try appendStep(arena, &steps_list, &live, .{
                            .kind = .planning,
                            .name = "evaluate",
                            .action = try buildEvaluationClarificationActionText(arena, evaluation_trigger),
                            .status = .success,
                            .details = clarification_details,
                        });
                        try appendStep(arena, &steps_list, &live, .{
                            .kind = .clarification,
                            .name = "clarification",
                            .action = "Multiple fallback strategies still look plausible after evaluation; asking for a user choice.",
                            .status = .success,
                            .details = try buildClarificationSelectionDetails(arena, fallback_plan.candidate_scores),
                        });

                        const steps = try steps_list.toOwnedSlice(arena);
                        const questions = try arena.dupe(AgentQuestion, &[_]AgentQuestion{
                            try buildAgenticSelectionQuestionForIndices(arena, request.query, retrieval_queries, clarification_indices),
                        });
                        const result = RetrievalAgentResult{
                            .created_at = 0,
                            .status = .clarification_required,
                            .hits = try hit_list.toOwnedSlice(arena),
                            .steps = steps,
                            .strategy_used = detectAggregateStrategy(strategies.items),
                            .session_id = request.session_id,
                            .iteration = iteration_count,
                            .clarification_count = clarification_state.count,
                            .remaining_internal_iterations = @max(@as(i64, 0), max_internal_iterations - iteration_count),
                            .remaining_user_clarifications = clarification_state.remaining,
                            .questions = questions,
                            .tool_calls_made = tool_calls_made,
                            .classification = classification_result,
                        };
                        return try finishAgentResult(alloc, format, result, &live);
                    } else {
                        const evaluation_action = try buildEvaluationActionText(arena, evaluation_trigger);
                        try appendStep(arena, &steps_list, &live, .{
                            .kind = .planning,
                            .name = "evaluate",
                            .action = evaluation_action,
                            .status = .success,
                            .details = try buildEvaluationStepDetails(
                                arena,
                                attempted_query_indices,
                                retrieval_queries,
                                @max(@as(i64, 0), max_internal_iterations - tool_calls_made),
                                fallback_plan.indices,
                                fallback_plan.source,
                                evaluation_trigger,
                                strategy,
                                attempt_summary,
                                previous_attempt_summary,
                                fallback_plan.candidate_scores,
                            ),
                        });
                        try appendStep(arena, &steps_list, &live, .{
                            .kind = .planning,
                            .name = "select_strategy",
                            .action = try buildSelectStrategyAction(arena, classification_result, retrieval_queries, fallback_plan.indices),
                            .status = .success,
                            .details = try buildSelectStrategyStepDetails(
                                arena,
                                retrieval_queries,
                                fallback_plan.indices,
                                classification_result,
                                fallback_plan.source,
                                fallback_plan.candidate_scores,
                                false,
                            ),
                        });
                        try planned_query_indices.appendSlice(arena, fallback_plan.indices);
                    }
                }
            }
        }
    }

    if (agentic_mode and hit_list.items.len == 0 and retrieval_queries.len > 1 and !broadened_from_decision and clarification_state.interactive and clarification_state.remaining > 0 and hasUnattemptedAgenticCandidate(candidate_scores, attempted_query_indices)) {
        try appendStep(arena, &steps_list, &live, .{
            .kind = .clarification,
            .name = "clarification",
            .action = "No relevant hits were found; asking whether to broaden retrieval to the other available strategies.",
            .status = .success,
        });

        const steps = try steps_list.toOwnedSlice(arena);
        const questions = try arena.dupe(AgentQuestion, &[_]AgentQuestion{
            try buildBroadenSearchQuestion(arena, request.query),
        });
        const result = RetrievalAgentResult{
            .created_at = 0,
            .status = .clarification_required,
            .hits = &.{},
            .steps = steps,
            .strategy_used = null,
            .session_id = request.session_id,
            .iteration = iteration_count,
            .clarification_count = clarification_state.count,
            .remaining_internal_iterations = @max(@as(i64, 0), max_internal_iterations - iteration_count),
            .remaining_user_clarifications = clarification_state.remaining,
            .questions = questions,
            .tool_calls_made = tool_calls_made,
        };
        return try finishAgentResult(alloc, format, result, &live);
    }

    if (generation_cfg) |cfg| {
        const exec = generation_runner orelse return error.UnsupportedRetrievalAgentRequest;
        const messages = try buildGenerationMessages(arena, request.query, hit_list.items, cfg);
        var result = exec.executeChain(alloc, cfg.chain, messages) catch |err| switch (format) {
            .sse => return .{
                .content_type = "text/event-stream",
                .body = try encodeSseError(alloc, @errorName(err)),
            },
            .json => return err,
        };
        defer result.deinit();
        generated_content = try arena.dupe(u8, result.content);
        try live.emitTextChunks("generation", generated_content.?);
        if (cfg.chain.len > 0) model_used = try arena.dupe(u8, cfg.chain[0].generator.model);
        try appendStep(arena, &steps_list, &live, .{
            .kind = .generation,
            .name = "generation",
            .action = "generated response from retrieved context",
            .status = .success,
        });
    }

    if (confidence_enabled) {
        const scores = scoreConfidence(hit_list.items, generated_content);
        generation_confidence = scores.generation_confidence;
        context_relevance = scores.context_relevance;
    }

    if (followup_cfg) |cfg| {
        followup_questions = try buildFollowupQuestions(arena, request.query, generated_content, cfg);
        for (followup_questions.?) |followup| try live.emitValue("followup", followup);
    }

    if (eval_cfg) |cfg| {
        const generated_eval_result = try buildEvalResult(
            arena,
            request.query,
            hit_list.items,
            generated_content,
            generation_confidence,
            context_relevance,
            cfg,
        );
        eval_result = generated_eval_result;
        try live.emitValue("eval", generated_eval_result);
        try appendStep(arena, &steps_list, &live, .{
            .kind = .validation,
            .name = "eval",
            .action = "evaluated retrieval and generation quality",
            .status = .success,
        });
    }

    const steps = try steps_list.toOwnedSlice(arena);
    const result = RetrievalAgentResult{
        .model = model_used,
        .created_at = 0,
        .status = .completed,
        .hits = try hit_list.toOwnedSlice(arena),
        .steps = steps,
        .strategy_used = detectAggregateStrategy(strategies.items),
        .session_id = request.session_id,
        .iteration = if (agentic_mode) iteration_count else 0,
        .clarification_count = clarification_state.count,
        .remaining_internal_iterations = if (agentic_mode) @max(@as(i64, 0), max_internal_iterations - iteration_count) else 0,
        .remaining_user_clarifications = clarification_state.remaining,
        .tool_calls_made = tool_calls_made,
        .classification = classification_result,
        .generation = generated_content,
        .generation_confidence = generation_confidence,
        .context_relevance = context_relevance,
        .eval_result = eval_result,
        .followup_questions = followup_questions,
    };
    return try finishAgentResult(alloc, format, result, &live);
}

fn encodeAgentResult(
    alloc: std.mem.Allocator,
    format: ResponseFormat,
    result: RetrievalAgentResult,
) !EncodedResponse {
    return switch (format) {
        .json => .{
            .content_type = "application/json",
            .body = try std.json.Stringify.valueAlloc(alloc, result, .{}),
        },
        .sse => .{
            .content_type = "text/event-stream",
            .body = try encodeSse(alloc, result),
        },
    };
}

fn runQueryAndExtractHits(
    alloc: std.mem.Allocator,
    arena: std.mem.Allocator,
    runner: QueryRunner,
    table_name: []const u8,
    query_json: []const u8,
    query_text: []const u8,
    has_tree_search: bool,
    normalize: bool,
) ![]const QueryHit {
    var query_response = try runner.runQuery(alloc, table_name, query_json);
    defer query_response.deinit(alloc);

    const response_json = if (normalize)
        try normalizeRetrievalQueryResponsesJson(arena, table_name, query_response.json)
    else
        query_response.json;

    const parsed = std.json.parseFromSlice(QueryResponses, arena, response_json, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    }) catch {
        return error.InvalidRetrievalAgentRequest;
    };
    // Do not deinit parsed — the returned hits reference its memory.
    // The arena owns the allocation and will free it.

    const tree_root = if (has_tree_search)
        try extractTreeFallbackRootKey(arena, query_json)
    else
        null;

    return if (has_tree_search)
        try extractTreeHits(arena, parsed.value, query_text, tree_root)
    else
        extractHits(parsed.value);
}

fn accumulateHits(
    arena: std.mem.Allocator,
    hit_list: *std.ArrayListUnmanaged(QueryHit),
    seen_ids: *std.StringHashMapUnmanaged(void),
    hits: []const QueryHit,
) !void {
    for (hits) |hit| {
        if (seen_ids.contains(hit._id)) continue;
        try seen_ids.put(arena, hit._id, {});
        try hit_list.append(arena, hit);
    }
}

pub fn executeJson(
    alloc: std.mem.Allocator,
    runner: QueryRunner,
    generation_runner: ?GenerationRunner,
    body: []const u8,
) ![]u8 {
    const encoded = try execute(alloc, runner, generation_runner, body);
    errdefer if (encoded.body.len > 0) alloc.free(encoded.body);
    if (!std.mem.eql(u8, encoded.content_type, "application/json")) return error.UnsupportedRetrievalAgentRequest;
    return encoded.body;
}

pub fn executeEval(
    alloc: std.mem.Allocator,
    body: []const u8,
) ![]u8 {
    if (body.len == 0) return error.InvalidEvalRequest;

    var parsed = std.json.parseFromSlice(eval_openapi.EvalRequest, alloc, body, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    }) catch return error.InvalidEvalRequest;
    defer parsed.deinit();
    return try executeEvalRequest(alloc, parsed.value);
}

pub fn buildEvalResponse(
    alloc: std.mem.Allocator,
    request: eval_openapi.EvalRequest,
) !eval_openapi.EvalResult {
    if (request.evaluators.len == 0) return error.InvalidEvalRequest;

    const has_generation = if (request.output) |value| value.len > 0 else false;
    const cfg = try parseStandaloneEvalConfig(request, has_generation);
    const hits = try buildEvalHitsFromRequest(alloc, request);
    return try buildEvalResult(
        alloc,
        request.query orelse "",
        hits,
        if (request.output) |value| if (value.len > 0) value else null else null,
        null,
        null,
        cfg,
    );
}

pub fn executeEvalRequest(
    alloc: std.mem.Allocator,
    request: eval_openapi.EvalRequest,
) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const result = try buildEvalResponse(arena_impl.allocator(), request);
    return try std.json.Stringify.valueAlloc(alloc, result, .{});
}

pub const QueryBuilderTableContext = query_builder_agent.QueryBuilderTableContext;

pub fn buildQueryBuilderResponse(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    table_schema_fields: ?[]const []const u8,
) !metadata_openapi.QueryBuilderResult {
    return try query_builder_agent.buildQueryBuilderResponse(alloc, request, table_schema_fields);
}

pub fn executeQueryBuilder(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryBuilderRequest,
    table_schema_fields: ?[]const []const u8,
) ![]u8 {
    return try query_builder_agent.executeQueryBuilder(alloc, request, table_schema_fields);
}

const ParsedGenerationConfig = struct {
    chain: []const generating.ChainLink,
    system_prompt: ?[]const u8,
    generation_context: ?[]const u8,
};

const ParsedClassificationConfig = struct {
    force_strategy: ?ai_openapi.QueryStrategy,
    force_semantic_mode: ?ai_openapi.SemanticQueryMode,
    with_reasoning: bool,
};

const ParsedFollowupConfig = struct {
    count: usize,
};

const ParsedEvalConfig = struct {
    evaluators: []const eval_openapi.EvaluatorName,
    k: usize,
    pass_threshold: f32,
    relevant_ids: []const []const u8,
    expectations: ?[]const u8,
};

fn parseStandaloneEvalConfig(
    request: eval_openapi.EvalRequest,
    has_generation: bool,
) !ParsedEvalConfig {
    const relevant_ids = if (request.ground_truth) |ground_truth|
        ground_truth.relevant_ids orelse &.{}
    else
        &.{};
    const expectations = if (request.ground_truth) |ground_truth| ground_truth.expectations else null;
    const k_value = if (request.options) |options|
        @as(usize, @intCast(@max(@as(i64, 1), options.k orelse 5)))
    else
        5;
    const pass_threshold = if (request.options) |options|
        @max(@as(f32, 0.0), @min(@as(f32, 1.0), options.pass_threshold orelse 0.5))
    else
        0.5;

    for (request.evaluators) |evaluator| {
        switch (evaluator) {
            .recall, .precision, .ndcg, .mrr, .map => {
                if (relevant_ids.len == 0) return error.InvalidEvalRequest;
                if (request.retrieved_ids == null or request.retrieved_ids.?.len == 0) return error.InvalidEvalRequest;
            },
            .faithfulness, .completeness, .coherence, .helpfulness, .correctness, .citation_quality => {
                if (!has_generation) return error.InvalidEvalRequest;
            },
            .relevance, .safety => {},
        }
    }

    return .{
        .evaluators = request.evaluators,
        .k = k_value,
        .pass_threshold = pass_threshold,
        .relevant_ids = relevant_ids,
        .expectations = expectations,
    };
}

fn buildEvalHitsFromRequest(
    alloc: std.mem.Allocator,
    request: eval_openapi.EvalRequest,
) ![]const QueryHit {
    const context = request.context orelse &.{};
    const retrieved_ids = request.retrieved_ids orelse &.{};
    const hit_count = if (context.len > retrieved_ids.len) context.len else retrieved_ids.len;
    const hits = try alloc.alloc(QueryHit, hit_count);
    for (0..hit_count) |i| {
        hits[i] = .{
            ._id = if (i < retrieved_ids.len)
                retrieved_ids[i]
            else
                try std.fmt.allocPrint(alloc, "doc:{d}", .{i + 1}),
            ._score = 1.0,
            ._index_scores = null,
            ._source = if (i < context.len) context[i] else null,
            ._sort = null,
        };
    }
    return hits;
}

// Identity conversion functions removed — these were no-ops (same type in and out).

fn buildClassificationStepDetails(
    alloc: std.mem.Allocator,
    request: RetrievalAgentRequest,
    cfg: ParsedClassificationConfig,
    selected_query_indices: ?[]const usize,
) !std.json.Value {
    var obj = std.json.ObjectMap.empty;
    try obj.put(alloc, "agentic_mode", .{ .bool = (request.max_internal_iterations orelse 0) > 0 });
    if (cfg.force_strategy) |strategy| try obj.put(alloc, "force_strategy", .{ .string = @tagName(strategy) });
    if (cfg.force_semantic_mode) |mode| try obj.put(alloc, "force_semantic_mode", .{ .string = @tagName(mode) });
    try obj.put(alloc, "with_reasoning", .{ .bool = cfg.with_reasoning });
    if (selected_query_indices) |selected| {
        var values = std.json.Array.init(alloc);
        for (selected) |index| try values.append(.{ .integer = @intCast(index) });
        try obj.put(alloc, "selected_query_indices", .{ .array = values });
    }
    return .{ .object = obj };
}

fn buildAgenticSelectionDetails(
    alloc: std.mem.Allocator,
    request: RetrievalAgentRequest,
    selected_query_indices: ?[]const usize,
) !std.json.Value {
    var obj = std.json.ObjectMap.empty;
    try obj.put(alloc, "agentic_mode", .{ .bool = true });
    try obj.put(alloc, "query_count", .{ .integer = @intCast(request.queries.len) });
    if (selected_query_indices) |selected| {
        var values = std.json.Array.init(alloc);
        for (selected) |index| try values.append(.{ .integer = @intCast(index) });
        try obj.put(alloc, "selected_query_indices", .{ .array = values });
    }
    return .{ .object = obj };
}

fn buildPipelineStepDetails(
    alloc: std.mem.Allocator,
    retrieval_queries: []const RetrievalQueryRequest,
    selected_query_indices: ?[]const usize,
    broadened_from_decision: bool,
) !std.json.Value {
    var obj = std.json.ObjectMap.empty;
    try obj.put(alloc, "query_count", .{ .integer = @intCast(retrieval_queries.len) });
    try obj.put(alloc, "broadened_from_decision", .{ .bool = broadened_from_decision });

    var strategies = std.json.Array.init(alloc);
    for (retrieval_queries) |retrieval_query| {
        try strategies.append(.{ .string = @tagName(detectStrategy(retrieval_query)) });
    }
    try obj.put(alloc, "strategies", .{ .array = strategies });

    if (selected_query_indices) |selected| {
        var values = std.json.Array.init(alloc);
        for (selected) |index| try values.append(.{ .integer = @intCast(index) });
        try obj.put(alloc, "selected_query_indices", .{ .array = values });
    }
    return .{ .object = obj };
}

fn buildToolStepDetails(
    alloc: std.mem.Allocator,
    retrieval_query: RetrievalQueryRequest,
    retrieval_query_index: usize,
    strategy: RetrievalStrategy,
) !std.json.Value {
    var obj = std.json.ObjectMap.empty;
    try obj.put(alloc, "query_index", .{ .integer = @intCast(retrieval_query_index) });
    try obj.put(alloc, "strategy", .{ .string = @tagName(strategy) });
    if (retrieval_query.table) |table_name| {
        try obj.put(alloc, "table", .{ .string = table_name });
    }
    if (retrieval_query.indexes) |indexes| {
        var values = std.json.Array.init(alloc);
        for (indexes) |index_name| try values.append(.{ .string = index_name });
        try obj.put(alloc, "indexes", .{ .array = values });
    }
    if (retrieval_query.tree_search) |tree_search| {
        var tree_obj = std.json.ObjectMap.empty;
        try tree_obj.put(alloc, "index", .{ .string = tree_search.index });
        if (tree_search.start_nodes) |start_nodes| try tree_obj.put(alloc, "start_nodes", .{ .string = start_nodes });
        if (tree_search.max_depth) |max_depth| try tree_obj.put(alloc, "max_depth", .{ .integer = max_depth });
        if (tree_search.beam_width) |beam_width| try tree_obj.put(alloc, "beam_width", .{ .integer = beam_width });
        try obj.put(alloc, "tree_search", .{ .object = tree_obj });
    }
    return .{ .object = obj };
}

fn buildTreeExpansionStepDetails(
    alloc: std.mem.Allocator,
    retrieval_query: RetrievalQueryRequest,
    retrieval_query_index: usize,
    plan: TreeBranchExpansionPlan,
) !std.json.Value {
    var obj = std.json.ObjectMap.empty;
    try obj.put(alloc, "query_index", .{ .integer = @intCast(retrieval_query_index) });
    try obj.put(alloc, "strategy", .{ .string = @tagName(detectStrategy(retrieval_query)) });
    try obj.put(alloc, "phase", .{ .string = "tree_search" });
    try obj.put(alloc, "branch_root", .{ .string = plan.branch.root });
    try obj.put(alloc, "branch_path", .{ .string = plan.branch.path });
    try obj.put(alloc, "seed_key", .{ .string = plan.seed_key });
    try obj.put(alloc, "seed_depth", .{ .integer = @intCast(plan.seed_depth) });
    try obj.put(alloc, "max_depth", .{ .integer = plan.max_depth });
    try obj.put(alloc, "query_relevance", .{ .float = plan.branch.query_relevance });
    try obj.put(alloc, "node_count", .{ .integer = @intCast(plan.branch.node_count) });
    return .{ .object = obj };
}

fn buildSelectStrategyAction(
    alloc: std.mem.Allocator,
    classification_result: ?ai_openapi.ClassificationTransformationResult,
    retrieval_queries: []const RetrievalQueryRequest,
    selected_query_indices: []const usize,
) ![]const u8 {
    const selected_strategy = detectSelectedAgenticStrategy(retrieval_queries, selected_query_indices);
    if (classification_result) |classification| {
        return try std.fmt.allocPrint(alloc, "selected {s} retrieval strategy after {s} classification", .{
            @tagName(selected_strategy),
            @tagName(classification.strategy),
        });
    }
    return try std.fmt.allocPrint(alloc, "selected {s} retrieval strategy for bounded agentic execution", .{
        @tagName(selected_strategy),
    });
}

fn buildSelectStrategyStepDetails(
    alloc: std.mem.Allocator,
    retrieval_queries: []const RetrievalQueryRequest,
    selected_query_indices: []const usize,
    classification_result: ?ai_openapi.ClassificationTransformationResult,
    source: AgenticSelectionSource,
    candidate_scores: []const AgenticCandidateScore,
    broadened_from_decision: bool,
) !std.json.Value {
    var obj = std.json.ObjectMap.empty;
    try obj.put(alloc, "selected_strategy", .{ .string = @tagName(detectSelectedAgenticStrategy(retrieval_queries, selected_query_indices)) });
    try obj.put(alloc, "selected_query_count", .{ .integer = @intCast(selected_query_indices.len) });
    try obj.put(alloc, "selection_source", .{ .string = @tagName(source) });
    try obj.put(alloc, "broadened_from_decision", .{ .bool = broadened_from_decision });
    if (classification_result) |classification| {
        try obj.put(alloc, "classification_strategy", .{ .string = @tagName(classification.strategy) });
    }

    var indices = std.json.Array.init(alloc);
    for (selected_query_indices) |index| try indices.append(.{ .integer = @intCast(index) });
    try obj.put(alloc, "selected_query_indices", .{ .array = indices });

    var strategies = std.json.Array.init(alloc);
    for (selected_query_indices) |index| {
        try strategies.append(.{ .string = @tagName(detectStrategy(retrieval_queries[index])) });
    }
    try obj.put(alloc, "selected_query_strategies", .{ .array = strategies });

    var scores = std.json.Array.init(alloc);
    for (candidate_scores) |candidate| {
        var score_obj = std.json.ObjectMap.empty;
        try score_obj.put(alloc, "index", .{ .integer = @intCast(candidate.index) });
        try score_obj.put(alloc, "strategy", .{ .string = @tagName(candidate.strategy) });
        try score_obj.put(alloc, "score", .{ .integer = candidate.score });
        if (candidate.probe_hits) |probe_hits| try score_obj.put(alloc, "probe_hits", .{ .integer = probe_hits });
        if (candidate.probe_relevance) |probe_relevance| try score_obj.put(alloc, "probe_relevance", .{ .float = probe_relevance });
        if (candidate.probe_top_score) |probe_top_score| try score_obj.put(alloc, "probe_top_score", .{ .float = probe_top_score });
        try scores.append(.{ .object = score_obj });
    }
    try obj.put(alloc, "candidate_scores", .{ .array = scores });
    return .{ .object = obj };
}

fn buildClarificationSelectionDetails(
    alloc: std.mem.Allocator,
    candidate_scores: []const AgenticCandidateScore,
) !std.json.Value {
    var obj = std.json.ObjectMap.empty;
    var scores = std.json.Array.init(alloc);
    for (candidate_scores) |candidate| {
        var score_obj = std.json.ObjectMap.empty;
        try score_obj.put(alloc, "index", .{ .integer = @intCast(candidate.index) });
        try score_obj.put(alloc, "strategy", .{ .string = @tagName(candidate.strategy) });
        try score_obj.put(alloc, "score", .{ .integer = candidate.score });
        if (candidate.probe_hits) |probe_hits| try score_obj.put(alloc, "probe_hits", .{ .integer = probe_hits });
        if (candidate.probe_relevance) |probe_relevance| try score_obj.put(alloc, "probe_relevance", .{ .float = probe_relevance });
        if (candidate.probe_top_score) |probe_top_score| try score_obj.put(alloc, "probe_top_score", .{ .float = probe_top_score });
        try scores.append(.{ .object = score_obj });
    }
    try obj.put(alloc, "candidate_scores", .{ .array = scores });
    return .{ .object = obj };
}

fn buildRefineQueryStepDetails(
    alloc: std.mem.Allocator,
    retrieval_query: RetrievalQueryRequest,
    retrieval_query_index: usize,
) !std.json.Value {
    var obj = std.json.ObjectMap.empty;
    try obj.put(alloc, "query_index", .{ .integer = @intCast(retrieval_query_index) });
    try obj.put(alloc, "strategy", .{ .string = @tagName(detectStrategy(retrieval_query)) });
    try obj.put(alloc, "phase", .{ .string = "step_back_followup" });
    if (retrieval_query.table) |table_name| {
        try obj.put(alloc, "table", .{ .string = table_name });
    }
    return .{ .object = obj };
}

fn buildEvaluationRefineQueryStepDetails(
    alloc: std.mem.Allocator,
    retrieval_query: RetrievalQueryRequest,
    retrieval_query_index: usize,
    classification: ai_openapi.ClassificationTransformationResult,
    refined_query: []const u8,
) !std.json.Value {
    var obj = std.json.ObjectMap.empty;
    try obj.put(alloc, "query_index", .{ .integer = @intCast(retrieval_query_index) });
    try obj.put(alloc, "strategy", .{ .string = @tagName(detectStrategy(retrieval_query)) });
    try obj.put(alloc, "classification_strategy", .{ .string = @tagName(classification.strategy) });
    try obj.put(alloc, "phase", .{ .string = "evaluation_refine" });
    try obj.put(alloc, "refined_query", .{ .string = refined_query });
    if (retrieval_query.semantic_search) |original_query| {
        try obj.put(alloc, "original_query", .{ .string = original_query });
    } else if (retrieval_query.full_text_search) |full_text| {
        if (full_text == .object) {
            if (full_text.object.get("query")) |query_value| {
                if (query_value == .string) try obj.put(alloc, "original_query", .{ .string = query_value.string });
            }
        }
    }
    if (retrieval_query.table) |table_name| {
        try obj.put(alloc, "table", .{ .string = table_name });
    }
    return .{ .object = obj };
}

fn buildEvaluationStepDetails(
    alloc: std.mem.Allocator,
    attempted_query_indices: []const bool,
    retrieval_queries: []const RetrievalQueryRequest,
    remaining_internal_iterations: i64,
    next_query_indices: []const usize,
    next_source: AgenticSelectionSource,
    trigger: AgenticEvaluationTrigger,
    strategy: RetrievalStrategy,
    attempt_summary: AttemptEvaluationSummary,
    previous_attempt_summary: ?AttemptEvaluationSummary,
    candidate_scores: []const AgenticCandidateScore,
) !std.json.Value {
    var obj = std.json.ObjectMap.empty;
    const current_planner_score = attemptPlannerScore(attempt_summary, strategy);
    const best_fallback_score = bestRemainingCandidateScore(candidate_scores, attempted_query_indices);
    const best_fallback = bestRemainingCandidate(candidate_scores, attempted_query_indices);
    const second_fallback = secondRemainingCandidate(candidate_scores, attempted_query_indices);
    try obj.put(alloc, "remaining_internal_iterations", .{ .integer = remaining_internal_iterations });
    try obj.put(alloc, "next_selection_source", .{ .string = @tagName(next_source) });
    try obj.put(alloc, "trigger", .{ .string = @tagName(trigger) });
    try obj.put(alloc, "planner_decision", .{ .string = "switch_strategy" });
    try obj.put(alloc, "current_hit_count", .{ .integer = attempt_summary.hit_count });
    try obj.put(alloc, "current_planner_score", .{ .float = current_planner_score });
    if (previous_attempt_summary) |previous| {
        const previous_planner_score = attemptPlannerScore(previous, strategy);
        try obj.put(alloc, "previous_planner_score", .{ .float = previous_planner_score });
        try obj.put(alloc, "planner_progress_delta", .{ .float = current_planner_score - previous_planner_score });
    }
    if (best_fallback_score) |value| {
        try obj.put(alloc, "best_fallback_score", .{ .float = value });
        try obj.put(alloc, "planner_score_delta", .{ .float = value - current_planner_score });
    }
    if (best_fallback) |candidate| {
        try obj.put(alloc, "best_fallback_index", .{ .integer = @intCast(candidate.index) });
        try obj.put(alloc, "best_fallback_strategy", .{ .string = @tagName(candidate.strategy) });
        try obj.put(alloc, "current_vs_fallback_ambiguous", .{
            .bool = shouldClarifyBetweenCurrentAndFallback(strategy, attempt_summary, previous_attempt_summary, candidate),
        });
        if (second_fallback) |second_candidate| {
            try obj.put(alloc, "second_fallback_index", .{ .integer = @intCast(second_candidate.index) });
            try obj.put(alloc, "second_fallback_strategy", .{ .string = @tagName(second_candidate.strategy) });
            try obj.put(alloc, "fallback_consensus_ambiguous", .{
                .bool = shouldClarifyBetweenFallbackCandidates(strategy, attempt_summary, previous_attempt_summary, candidate, second_candidate),
            });
        }
    }
    if (attempt_summary.top_score) |top_score| try obj.put(alloc, "current_top_score", .{ .float = top_score });
    if (attempt_summary.context_relevance) |context_relevance| try obj.put(alloc, "current_context_relevance", .{ .float = context_relevance });
    if (attempt_summary.context_length) |context_length| try obj.put(alloc, "current_context_length", .{ .integer = context_length });
    if (attempt_summary.top_tree_branch_relevance) |value| try obj.put(alloc, "current_top_tree_branch_relevance", .{ .float = value });
    if (attempt_summary.top_tree_branch_nodes) |value| try obj.put(alloc, "current_top_tree_branch_nodes", .{ .integer = value });
    if (attempt_summary.top_tree_branch_leaf_hits) |value| {
        try obj.put(alloc, "current_top_tree_branch_leaf_hits", .{ .integer = value });
        try obj.put(alloc, "current_tree_branch_thin", .{ .bool = value == 0 });
    }

    var attempted = std.json.Array.init(alloc);
    for (attempted_query_indices, 0..) |attempted_query, i| {
        if (!attempted_query) continue;
        try attempted.append(.{ .integer = @intCast(i) });
    }
    try obj.put(alloc, "attempted_query_indices", .{ .array = attempted });

    var next_indices = std.json.Array.init(alloc);
    for (next_query_indices) |index| try next_indices.append(.{ .integer = @intCast(index) });
    try obj.put(alloc, "next_query_indices", .{ .array = next_indices });

    var next_strategies = std.json.Array.init(alloc);
    for (next_query_indices) |index| {
        try next_strategies.append(.{ .string = @tagName(detectStrategy(retrieval_queries[index])) });
    }
    try obj.put(alloc, "next_query_strategies", .{ .array = next_strategies });

    var scores = std.json.Array.init(alloc);
    for (candidate_scores) |candidate| {
        var score_obj = std.json.ObjectMap.empty;
        try score_obj.put(alloc, "index", .{ .integer = @intCast(candidate.index) });
        try score_obj.put(alloc, "strategy", .{ .string = @tagName(candidate.strategy) });
        try score_obj.put(alloc, "score", .{ .integer = candidate.score });
        if (candidate.probe_hits) |probe_hits| try score_obj.put(alloc, "probe_hits", .{ .integer = probe_hits });
        if (candidate.probe_relevance) |probe_relevance| try score_obj.put(alloc, "probe_relevance", .{ .float = probe_relevance });
        if (candidate.probe_top_score) |probe_top_score| try score_obj.put(alloc, "probe_top_score", .{ .float = probe_top_score });
        try scores.append(.{ .object = score_obj });
    }
    try obj.put(alloc, "candidate_scores", .{ .array = scores });
    return .{ .object = obj };
}

fn buildEvaluationRefinementStepDetails(
    alloc: std.mem.Allocator,
    retrieval_query: RetrievalQueryRequest,
    retrieval_query_index: usize,
    remaining_internal_iterations: i64,
    trigger: AgenticEvaluationTrigger,
    attempt_summary: AttemptEvaluationSummary,
    best_fallback_score: ?f32,
) !std.json.Value {
    var obj = std.json.ObjectMap.empty;
    const current_planner_score = attemptPlannerScore(attempt_summary, detectStrategy(retrieval_query));
    try obj.put(alloc, "query_index", .{ .integer = @intCast(retrieval_query_index) });
    try obj.put(alloc, "strategy", .{ .string = @tagName(detectStrategy(retrieval_query)) });
    try obj.put(alloc, "trigger", .{ .string = @tagName(trigger) });
    try obj.put(alloc, "planner_decision", .{ .string = "refine_query" });
    try obj.put(alloc, "remaining_internal_iterations", .{ .integer = remaining_internal_iterations });
    try obj.put(alloc, "current_hit_count", .{ .integer = attempt_summary.hit_count });
    try obj.put(alloc, "current_planner_score", .{ .float = current_planner_score });
    if (best_fallback_score) |value| {
        try obj.put(alloc, "best_fallback_score", .{ .float = value });
        try obj.put(alloc, "planner_score_delta", .{ .float = value - current_planner_score });
    }
    if (attempt_summary.top_score) |top_score| try obj.put(alloc, "current_top_score", .{ .float = top_score });
    if (attempt_summary.context_relevance) |context_relevance| try obj.put(alloc, "current_context_relevance", .{ .float = context_relevance });
    if (attempt_summary.context_length) |context_length| try obj.put(alloc, "current_context_length", .{ .integer = context_length });
    if (attempt_summary.top_tree_branch_relevance) |value| try obj.put(alloc, "current_top_tree_branch_relevance", .{ .float = value });
    if (attempt_summary.top_tree_branch_nodes) |value| try obj.put(alloc, "current_top_tree_branch_nodes", .{ .integer = value });
    if (attempt_summary.top_tree_branch_leaf_hits) |value| {
        try obj.put(alloc, "current_top_tree_branch_leaf_hits", .{ .integer = value });
        try obj.put(alloc, "current_tree_branch_thin", .{ .bool = value == 0 });
    }
    return .{ .object = obj };
}

fn buildEvaluationAcceptStepDetails(
    alloc: std.mem.Allocator,
    retrieval_query: RetrievalQueryRequest,
    retrieval_query_index: usize,
    trigger: AgenticEvaluationTrigger,
    attempt_summary: AttemptEvaluationSummary,
    current_score: f32,
    previous_score: ?f32,
    best_fallback_score: ?f32,
) !std.json.Value {
    var obj = std.json.ObjectMap.empty;
    try obj.put(alloc, "query_index", .{ .integer = @intCast(retrieval_query_index) });
    try obj.put(alloc, "strategy", .{ .string = @tagName(detectStrategy(retrieval_query)) });
    try obj.put(alloc, "trigger", .{ .string = @tagName(trigger) });
    try obj.put(alloc, "planner_decision", .{ .string = "accept_result" });
    try obj.put(alloc, "current_hit_count", .{ .integer = attempt_summary.hit_count });
    try obj.put(alloc, "current_planner_score", .{ .float = current_score });
    if (previous_score) |value| {
        try obj.put(alloc, "previous_planner_score", .{ .float = value });
        try obj.put(alloc, "planner_progress_delta", .{ .float = current_score - value });
    }
    if (best_fallback_score) |value| {
        try obj.put(alloc, "best_fallback_score", .{ .float = value });
        try obj.put(alloc, "planner_score_delta", .{ .float = value - current_score });
    }
    if (attempt_summary.top_score) |top_score| try obj.put(alloc, "current_top_score", .{ .float = top_score });
    if (attempt_summary.context_relevance) |context_relevance| try obj.put(alloc, "current_context_relevance", .{ .float = context_relevance });
    if (attempt_summary.context_length) |context_length| try obj.put(alloc, "current_context_length", .{ .integer = context_length });
    if (attempt_summary.top_tree_branch_relevance) |value| try obj.put(alloc, "current_top_tree_branch_relevance", .{ .float = value });
    if (attempt_summary.top_tree_branch_nodes) |value| try obj.put(alloc, "current_top_tree_branch_nodes", .{ .integer = value });
    if (attempt_summary.top_tree_branch_leaf_hits) |value| {
        try obj.put(alloc, "current_top_tree_branch_leaf_hits", .{ .integer = value });
        try obj.put(alloc, "current_tree_branch_thin", .{ .bool = value == 0 });
    }
    return .{ .object = obj };
}

fn buildEvaluationTreeExpansionStepDetails(
    alloc: std.mem.Allocator,
    retrieval_query: RetrievalQueryRequest,
    retrieval_query_index: usize,
    remaining_internal_iterations: i64,
    trigger: AgenticEvaluationTrigger,
    attempt_summary: AttemptEvaluationSummary,
    best_fallback_score: ?f32,
    plan: TreeBranchExpansionPlan,
) !std.json.Value {
    var obj = std.json.ObjectMap.empty;
    const current_planner_score = attemptPlannerScore(attempt_summary, detectStrategy(retrieval_query));
    try obj.put(alloc, "query_index", .{ .integer = @intCast(retrieval_query_index) });
    try obj.put(alloc, "strategy", .{ .string = @tagName(detectStrategy(retrieval_query)) });
    try obj.put(alloc, "trigger", .{ .string = @tagName(trigger) });
    try obj.put(alloc, "phase", .{ .string = "tree_search" });
    try obj.put(alloc, "tree_search_decision", .{ .string = "continue_branch" });
    try obj.put(alloc, "remaining_internal_iterations", .{ .integer = remaining_internal_iterations });
    try obj.put(alloc, "current_hit_count", .{ .integer = attempt_summary.hit_count });
    try obj.put(alloc, "current_planner_score", .{ .float = current_planner_score });
    if (best_fallback_score) |value| {
        try obj.put(alloc, "best_fallback_score", .{ .float = value });
        try obj.put(alloc, "planner_score_delta", .{ .float = value - current_planner_score });
    }
    if (attempt_summary.context_relevance) |context_relevance| try obj.put(alloc, "current_context_relevance", .{ .float = context_relevance });
    if (attempt_summary.top_tree_branch_relevance) |value| try obj.put(alloc, "current_top_tree_branch_relevance", .{ .float = value });
    if (attempt_summary.top_tree_branch_nodes) |value| try obj.put(alloc, "current_top_tree_branch_nodes", .{ .integer = value });
    if (attempt_summary.top_tree_branch_leaf_hits) |value| {
        try obj.put(alloc, "current_top_tree_branch_leaf_hits", .{ .integer = value });
        try obj.put(alloc, "current_tree_branch_thin", .{ .bool = value == 0 });
    }
    try obj.put(alloc, "branch_root", .{ .string = plan.branch.root });
    try obj.put(alloc, "branch_path", .{ .string = plan.branch.path });
    try obj.put(alloc, "seed_key", .{ .string = plan.seed_key });
    try obj.put(alloc, "seed_depth", .{ .integer = @intCast(plan.seed_depth) });
    try obj.put(alloc, "max_depth", .{ .integer = plan.max_depth });
    return .{ .object = obj };
}

fn summarizeAttemptEvaluation(
    alloc: std.mem.Allocator,
    query: []const u8,
    attempt_hits: []const QueryHit,
) AttemptEvaluationSummary {
    var summary: AttemptEvaluationSummary = .{
        .hit_count = @intCast(attempt_hits.len),
        .top_score = if (attempt_hits.len > 0) attempt_hits[0]._score else null,
    };
    if (attempt_hits.len == 0) return summary;

    const context_text = buildContextText(alloc, attempt_hits[0..@min(attempt_hits.len, 3)]) catch return summary;
    defer alloc.free(context_text);
    summary.context_relevance = queryCoverageScore(query, context_text);
    summary.context_length = @intCast(context_text.len);
    const maybe_branches = rankedTreeBranchesForQuery(alloc, query, attempt_hits) catch null;
    defer if (maybe_branches) |branches| alloc.free(branches);
    if (maybe_branches) |branches| {
        if (branches.len > 0) {
            summary.top_tree_branch_relevance = branches[0].query_relevance;
            summary.top_tree_branch_nodes = @intCast(branches[0].node_count);
            summary.top_tree_branch_leaf_hits = @intCast(branches[0].leaf_hits);
        }
    }
    return summary;
}

fn buildEvaluationActionText(
    alloc: std.mem.Allocator,
    trigger: AgenticEvaluationTrigger,
) ![]const u8 {
    return switch (trigger) {
        .empty_result => try alloc.dupe(u8, "evaluated retrieval attempt after no hits and continued with the next bounded strategy"),
        .weak_result => try alloc.dupe(u8, "evaluated weak retrieval result and continued with the next bounded strategy"),
        .partial_result => try alloc.dupe(u8, "evaluated partial retrieval result and continued with the next bounded strategy"),
        .none => try alloc.dupe(u8, "evaluated retrieval attempt and continued with the next bounded strategy"),
    };
}

fn buildEvaluationClarificationActionText(
    alloc: std.mem.Allocator,
    trigger: AgenticEvaluationTrigger,
) ![]const u8 {
    return switch (trigger) {
        .empty_result => try alloc.dupe(u8, "evaluated empty retrieval result and asked for a user choice between the remaining bounded strategies"),
        .weak_result => try alloc.dupe(u8, "evaluated weak retrieval result and asked for a user choice between the remaining bounded strategies"),
        .partial_result => try alloc.dupe(u8, "evaluated partial retrieval result and asked for a user choice between the remaining bounded strategies"),
        .none => try alloc.dupe(u8, "evaluated retrieval result and asked for a user choice between the remaining bounded strategies"),
    };
}

fn buildEvaluationRefinementActionText(
    alloc: std.mem.Allocator,
    trigger: AgenticEvaluationTrigger,
) ![]const u8 {
    return switch (trigger) {
        .partial_result => try alloc.dupe(u8, "evaluated partial retrieval result and refined the current query before switching strategy"),
        .weak_result => try alloc.dupe(u8, "evaluated weak retrieval result and refined the current query before switching strategy"),
        .empty_result => try alloc.dupe(u8, "evaluated empty retrieval result and refined the current query before switching strategy"),
        .none => try alloc.dupe(u8, "evaluated retrieval result and refined the current query before switching strategy"),
    };
}

fn buildEvaluationTreeExpansionActionText(
    alloc: std.mem.Allocator,
    trigger: AgenticEvaluationTrigger,
) ![]const u8 {
    return switch (trigger) {
        .partial_result => try alloc.dupe(u8, "evaluated partial tree retrieval result and continued tree search on the strongest branch"),
        .weak_result => try alloc.dupe(u8, "evaluated weak tree retrieval result and continued tree search on the strongest branch"),
        .empty_result => try alloc.dupe(u8, "evaluated tree retrieval result and continued tree search on the strongest branch"),
        .none => try alloc.dupe(u8, "evaluated tree retrieval result and continued tree search on the strongest branch"),
    };
}

fn buildEvaluationRefineQueryActionText(
    alloc: std.mem.Allocator,
    trigger: AgenticEvaluationTrigger,
) ![]const u8 {
    return switch (trigger) {
        .partial_result => try alloc.dupe(u8, "refined retrieval query after evaluating a partial result"),
        .weak_result => try alloc.dupe(u8, "refined retrieval query after evaluating a weak result"),
        .empty_result => try alloc.dupe(u8, "refined retrieval query after evaluating an empty result"),
        .none => try alloc.dupe(u8, "refined retrieval query after evaluation"),
    };
}

fn buildInitialRefineQueryStepDetails(
    alloc: std.mem.Allocator,
    retrieval_query: RetrievalQueryRequest,
    retrieval_query_index: usize,
    classification: ai_openapi.ClassificationTransformationResult,
    refined_query: []const u8,
) !std.json.Value {
    var obj = std.json.ObjectMap.empty;
    try obj.put(alloc, "query_index", .{ .integer = @intCast(retrieval_query_index) });
    try obj.put(alloc, "strategy", .{ .string = @tagName(detectStrategy(retrieval_query)) });
    try obj.put(alloc, "classification_strategy", .{ .string = @tagName(classification.strategy) });
    try obj.put(alloc, "refined_query", .{ .string = refined_query });
    if (retrieval_query.semantic_search) |original_query| {
        try obj.put(alloc, "original_query", .{ .string = original_query });
    }
    const phase = switch (classification.strategy) {
        .decompose => "decompose",
        .step_back => "step_back_initial",
        .hyde => "hyde_rewrite",
        .simple => "rewrite",
    };
    try obj.put(alloc, "phase", .{ .string = phase });
    if (retrieval_query.table) |table_name| {
        try obj.put(alloc, "table", .{ .string = table_name });
    }
    return .{ .object = obj };
}

fn parseGenerationConfig(
    alloc: std.mem.Allocator,
    request: RetrievalAgentRequest,
) !?ParsedGenerationConfig {
    if (request.document_renderer != null) return error.UnsupportedRetrievalAgentRequest;
    const steps = request.steps orelse {
        if (request.chain != null) return error.UnsupportedRetrievalAgentRequest;
        return null;
    };
    if (steps.tools != null) {
        return error.UnsupportedRetrievalAgentRequest;
    }

    const public_generation = steps.generation orelse {
        if (request.chain != null) return error.UnsupportedRetrievalAgentRequest;
        return null;
    };
    if (public_generation.enabled != null and public_generation.enabled.? == false) {
        if (request.chain != null) return error.UnsupportedRetrievalAgentRequest;
        return null;
    }

    const generation = generationStepFromPublic(public_generation);
    const chain = try buildGenerationChain(alloc, request, generation);
    return .{
        .chain = chain,
        .system_prompt = generation.system_prompt,
        .generation_context = generation.generation_context,
    };
}

fn normalizeRetrievalQueryResponsesJson(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    query_json: []const u8,
) ![]const u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, query_json, .{
        .allocate = .alloc_always,
    }) catch return error.InvalidRetrievalAgentRequest;
    const root = &parsed.value;
    if (root.* != .object) return error.InvalidRetrievalAgentRequest;
    const responses_value = root.object.getPtr("responses") orelse return error.InvalidRetrievalAgentRequest;
    if (responses_value.* != .array) return error.InvalidRetrievalAgentRequest;

    for (responses_value.array.items) |*response_value| {
        if (response_value.* != .object) return error.InvalidRetrievalAgentRequest;
        if (response_value.object.get("status") == null) {
            try response_value.object.put(alloc, "status", .{ .integer = 200 });
        }
        if (response_value.object.get("table") == null) {
            try response_value.object.put(alloc, "table", .{ .string = try alloc.dupe(u8, table_name) });
        }
        if (response_value.object.get("took") == null) {
            try response_value.object.put(alloc, "took", .{ .integer = 0 });
        }
        if (response_value.object.getPtr("hits")) |hits_value| {
            if (hits_value.* != .object) return error.InvalidRetrievalAgentRequest;
            const hit_items = if (hits_value.object.getPtr("hits")) |items_value|
                switch (items_value.*) {
                    .array => items_value.array.items,
                    else => return error.InvalidRetrievalAgentRequest,
                }
            else
                &.{};
            if (hits_value.object.get("total") == null) {
                try hits_value.object.put(alloc, "total", .{ .integer = @intCast(hit_items.len) });
            }
            if (hits_value.object.get("max_score") == null) {
                try hits_value.object.put(alloc, "max_score", .{ .float = computeNormalizedMaxScore(hit_items) });
            }
        }
    }

    return try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
}

fn computeNormalizedMaxScore(hit_items: []const std.json.Value) f64 {
    var max_score: f64 = 0;
    for (hit_items) |item| {
        if (item != .object) continue;
        const score_value = item.object.get("_score") orelse continue;
        const score = switch (score_value) {
            .float => |value| value,
            .integer => |value| @as(f64, @floatFromInt(value)),
            else => continue,
        };
        if (score > max_score) max_score = score;
    }
    return max_score;
}

fn parseClassificationConfig(request: RetrievalAgentRequest) !?ParsedClassificationConfig {
    const steps = request.steps orelse return null;
    const classification = steps.classification orelse return null;
    if (classification.enabled != null and classification.enabled.? == false) return null;
    if (classification.generator != null or classification.chain != null) return error.UnsupportedRetrievalAgentRequest;
    return .{
        .force_strategy = if (classification.force_strategy) |value| value else null,
        .force_semantic_mode = if (classification.force_semantic_mode) |value| value else null,
        .with_reasoning = classification.with_reasoning orelse false,
    };
}

fn parseFollowupConfig(request: RetrievalAgentRequest, has_generation: bool) !?ParsedFollowupConfig {
    const steps = request.steps orelse return null;
    const followup = steps.followup orelse return null;
    if (followup.enabled != null and followup.enabled.? == false) return null;
    if (!has_generation) return error.UnsupportedRetrievalAgentRequest;
    if (followup.generator != null or followup.chain != null) return error.UnsupportedRetrievalAgentRequest;
    const count = @as(usize, @intCast(@max(@as(i64, 1), followup.count orelse 3)));
    return .{ .count = count };
}

fn parseEvalConfig(
    alloc: std.mem.Allocator,
    request: RetrievalAgentRequest,
    has_generation: bool,
) !?ParsedEvalConfig {
    const steps = request.steps orelse return null;
    const eval = steps.eval orelse return null;
    const public_evaluators = eval.evaluators orelse return null;
    if (public_evaluators.len == 0) return error.InvalidRetrievalAgentRequest;
    if (eval.judge) |judge| {
        _ = try generatorConfigFromPublic(judge);
    }

    const relevant_ids = if (eval.ground_truth) |ground_truth|
        ground_truth.relevant_ids orelse &.{}
    else
        &.{};
    const expectations = if (eval.ground_truth) |ground_truth| ground_truth.expectations else null;
    const k_value = if (eval.options) |options|
        @as(usize, @intCast(@max(@as(i64, 1), options.k orelse 5)))
    else
        5;
    const pass_threshold = if (eval.options) |options|
        @max(@as(f32, 0.0), @min(@as(f32, 1.0), options.pass_threshold orelse 0.5))
    else
        0.5;

    const evaluators = try alloc.alloc(eval_openapi.EvaluatorName, public_evaluators.len);
    for (public_evaluators, 0..) |evaluator, i| {
        evaluators[i] = evaluator;
    }

    for (evaluators) |evaluator| {
        switch (evaluator) {
            .recall, .precision, .ndcg, .mrr, .map => {
                if (relevant_ids.len == 0) return error.InvalidRetrievalAgentRequest;
            },
            .faithfulness, .completeness, .coherence, .helpfulness, .correctness, .citation_quality => {
                if (!has_generation) return error.UnsupportedRetrievalAgentRequest;
            },
            .relevance, .safety => {},
        }
    }

    return .{
        .evaluators = evaluators,
        .k = k_value,
        .pass_threshold = pass_threshold,
        .relevant_ids = relevant_ids,
        .expectations = expectations,
    };
}

fn parseConfidenceEnabled(request: RetrievalAgentRequest, has_generation: bool) !bool {
    const steps = request.steps orelse return false;
    const confidence = steps.confidence orelse return false;
    if (confidence.enabled != null and confidence.enabled.? == false) return false;
    if (!has_generation) return error.UnsupportedRetrievalAgentRequest;
    if (confidence.generator != null or confidence.chain != null) return error.UnsupportedRetrievalAgentRequest;
    return true;
}

fn buildGenerationChain(
    alloc: std.mem.Allocator,
    request: RetrievalAgentRequest,
    generation: ai_openapi.GenerationStepConfig,
) ![]const generating.ChainLink {
    var links = std.ArrayListUnmanaged(generating.ChainLink).empty;
    errdefer links.deinit(alloc);

    if (generation.chain != null or request.chain != null) return error.UnsupportedRetrievalAgentRequest;

    if (generation.generator) |generator_cfg| {
        try links.append(alloc, .{ .generator = try generatorConfigFromGenerated(generator_cfg) });
    } else if (request.generator) |generator_cfg| {
        try links.append(alloc, .{ .generator = try generatorConfigFromPublic(generator_cfg) });
    } else {
        return error.UnsupportedRetrievalAgentRequest;
    }

    return try links.toOwnedSlice(alloc);
}

fn generationStepFromPublic(generation: ai_openapi.GenerationStepConfig) ai_openapi.GenerationStepConfig {
    return .{
        .enabled = generation.enabled,
        .generator = if (generation.generator) |cfg| publicGeneratorConfigToGenerated(cfg) else null,
        // Chain fallback is not implemented yet, but non-null still needs to trip validation.
        .chain = if (generation.chain != null) &[_]generating_openapi.ChainLink{} else null,
        .system_prompt = generation.system_prompt,
        .generation_context = generation.generation_context,
    };
}

fn publicGeneratorConfigToGenerated(cfg: generating_openapi.GeneratorConfig) generating_openapi.GeneratorConfig {
    return .{
        .provider = switch (cfg.provider) {
            .gemini => .gemini,
            .vertex => .vertex,
            .ollama => .ollama,
            .openai => .openai,
            .openrouter => .openrouter,
            .bedrock => .bedrock,
            .anthropic => .anthropic,
            .cohere => .cohere,
            .termite => .termite,
            .antfly => .antfly,
            .mock => .mock,
        },
        .model = cfg.model,
        .temperature = cfg.temperature,
        .max_tokens = cfg.max_tokens,
        .top_p = cfg.top_p,
        .top_k = cfg.top_k,
        .api_key = cfg.api_key,
        .url = cfg.url,
        .api_url = cfg.api_url,
        .project_id = cfg.project_id,
        .location = cfg.location,
        .credentials_path = cfg.credentials_path,
        .region = cfg.region,
    };
}

fn generatorConfigFromGenerated(cfg: generating_openapi.GeneratorConfig) !generating.GeneratorConfig {
    const provider: generating.Provider = switch (cfg.provider) {
        .openai => .openai,
        .ollama => .ollama,
        .termite => .termite,
        .antfly => .antfly,
        else => return error.UnsupportedRetrievalAgentRequest,
    };
    const model = cfg.model orelse return error.InvalidRetrievalAgentRequest;
    const url = switch (provider) {
        .termite => cfg.api_url orelse "",
        .antfly => "",
        .openai, .ollama => cfg.url orelse return error.InvalidRetrievalAgentRequest,
        else => return error.UnsupportedRetrievalAgentRequest,
    };
    return .{
        .provider = provider,
        .model = model,
        .url = url,
        .api_key = cfg.api_key,
    };
}

fn generatorConfigFromPublic(cfg: generating_openapi.GeneratorConfig) !generating.GeneratorConfig {
    return try generatorConfigFromGenerated(publicGeneratorConfigToGenerated(cfg));
}

fn buildGenerationMessages(
    alloc: std.mem.Allocator,
    query: []const u8,
    hits: []const QueryHit,
    cfg: ParsedGenerationConfig,
) ![]const generating.ChatMessage {
    const system_prompt = if (cfg.system_prompt) |prompt|
        prompt
    else if (cfg.generation_context) |context|
        try std.fmt.allocPrint(alloc, "Answer the user query using only the retrieved documents. {s}", .{context})
    else
        "Answer the user query using only the retrieved documents and cite document ids inline.";

    const ordered_hits = try orderHitsForGeneration(alloc, hits);
    defer alloc.free(ordered_hits);
    const selected_hits = try selectHitsForGenerationContext(alloc, query, ordered_hits);
    defer alloc.free(selected_hits);
    const documents_context = try buildGenerationDocumentsContext(alloc, query, selected_hits);
    defer alloc.free(documents_context);

    const tree_context = try buildTreeGenerationContext(alloc, selected_hits);
    defer if (tree_context) |value| alloc.free(value);
    const tree_branches = try buildTreeBranchSelectionContextForQuery(alloc, query, selected_hits);
    defer if (tree_branches) |value| alloc.free(value);
    const tree_branch_prefix = if (tree_branches) |branch_summary|
        try std.fmt.allocPrint(alloc, "Selected tree branches:\n{s}\n", .{branch_summary})
    else
        null;
    defer if (tree_branch_prefix) |value| alloc.free(value);

    const user_prompt = if (tree_context) |tree_summary|
        try std.fmt.allocPrint(
            alloc,
            "User query: {s}\n\nRetrieved documents:\n{s}\n{s}Tree hierarchy context:\n{s}\nProvide a concise answer grounded in the retrieved documents.",
            .{
                query,
                documents_context,
                tree_branch_prefix orelse "",
                tree_summary,
            },
        )
    else
        try std.fmt.allocPrint(
            alloc,
            "User query: {s}\n\nRetrieved documents:\n{s}\nProvide a concise answer grounded in the retrieved documents.",
            .{ query, documents_context },
        );

    const messages = try alloc.alloc(generating.ChatMessage, 2);
    messages[0] = .{ .role = .system, .content = system_prompt };
    messages[1] = .{ .role = .user, .content = user_prompt };
    return messages;
}

fn buildGenerationDocumentsContext(
    alloc: std.mem.Allocator,
    query: []const u8,
    hits: []const QueryHit,
) ![]u8 {
    const maybe_branches = try rankedTreeBranchesForQuery(alloc, query, hits);
    defer if (maybe_branches) |branches| alloc.free(branches);

    if (maybe_branches) |branches| {
        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(alloc);
        var document_index: usize = 0;

        for (branches, 0..) |branch, branch_index| {
            const branch_number = try std.fmt.allocPrint(alloc, "{d}", .{branch_index + 1});
            defer alloc.free(branch_number);
            try out.appendSlice(alloc, "Branch ");
            try out.appendSlice(alloc, branch_number);
            try out.appendSlice(alloc, " (root=");
            try out.appendSlice(alloc, branch.root);
            try out.appendSlice(alloc, ", path=");
            try out.appendSlice(alloc, branch.path);
            try out.appendSlice(alloc, ")\n");
            if (try buildBranchGenerationSummary(alloc, branch, hits)) |summary| {
                defer alloc.free(summary);
                try out.appendSlice(alloc, "  Summary: ");
                try out.appendSlice(alloc, summary);
                try out.append(alloc, '\n');
            }

            for (hits) |hit| {
                const branch_path = treeMetaString(hit, "branch_path_text") orelse treeMetaString(hit, "path_text") orelse continue;
                if (!std.mem.eql(u8, branch.path, branch_path)) continue;

                document_index += 1;
                const number = try std.fmt.allocPrint(alloc, "{d}", .{document_index});
                defer alloc.free(number);
                try out.appendSlice(alloc, "  Document ");
                try out.appendSlice(alloc, number);
                try out.appendSlice(alloc, " (id=");
                try out.appendSlice(alloc, hit._id);
                try out.appendSlice(alloc, "): ");
                const description = try describeHitForGeneration(alloc, hit);
                defer alloc.free(description);
                try out.appendSlice(alloc, description);
                try out.append(alloc, '\n');
            }
        }

        if (out.items.len > 0) return try out.toOwnedSlice(alloc);
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    for (hits, 0..) |hit, i| {
        try out.appendSlice(alloc, "Document ");
        const index = try std.fmt.allocPrint(alloc, "{d}", .{i + 1});
        defer alloc.free(index);
        try out.appendSlice(alloc, index);
        try out.appendSlice(alloc, " (id=");
        try out.appendSlice(alloc, hit._id);
        try out.appendSlice(alloc, "): ");
        const description = try describeHitForGeneration(alloc, hit);
        defer alloc.free(description);
        try out.appendSlice(alloc, description);
        try out.append(alloc, '\n');
    }
    return try out.toOwnedSlice(alloc);
}

fn buildBranchGenerationSummary(
    alloc: std.mem.Allocator,
    branch: TreeBranchSummary,
    hits: []const QueryHit,
) !?[]u8 {
    return try buildBranchGenerationSummaryWithLimit(alloc, branch, hits, null);
}

fn buildBranchGenerationSummaryWithLimit(
    alloc: std.mem.Allocator,
    branch: TreeBranchSummary,
    hits: []const QueryHit,
    node_limit: ?usize,
) !?[]u8 {
    var parts = std.ArrayListUnmanaged([]const u8).empty;
    defer parts.deinit(alloc);
    var seen_nodes: usize = 0;

    for (hits) |hit| {
        const branch_path = treeMetaString(hit, "branch_path_text") orelse treeMetaString(hit, "path_text") orelse continue;
        if (!std.mem.eql(u8, branch.path, branch_path)) continue;
        if (node_limit) |limit| {
            if (seen_nodes >= limit) break;
        }
        const source = hit._source orelse continue;
        if (source != .object) continue;
        const title = switch (source.object.get("title") orelse continue) {
            .string => |value| value,
            else => continue,
        };
        seen_nodes += 1;
        var seen = false;
        for (parts.items) |existing| {
            if (std.mem.eql(u8, existing, title)) {
                seen = true;
                break;
            }
        }
        if (!seen) try parts.append(alloc, title);
    }

    if (parts.items.len == 0) return null;

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    for (parts.items, 0..) |part, idx| {
        if (idx != 0) try out.appendSlice(alloc, " -> ");
        try out.appendSlice(alloc, part);
    }
    return try out.toOwnedSlice(alloc);
}

fn selectTreeBranchExpansionPlan(
    alloc: std.mem.Allocator,
    query: []const u8,
    tree_search: TreeSearchConfig,
    hits: []const QueryHit,
) !?TreeBranchExpansionPlan {
    const maybe_branches = try rankedTreeBranchesForQuery(alloc, query, hits);
    defer if (maybe_branches) |branches| alloc.free(branches);
    const branches = maybe_branches orelse return null;
    if (branches.len == 0) return null;

    const best = branches[0];
    if (best.query_relevance <= 0.05) return null;
    if (best.leaf_hits > 0 and best.node_count > max_tree_generation_nodes_per_branch) return null;

    const maybe_seed = try selectTreeBranchExpansionSeed(alloc, query, best, hits);
    const seed = maybe_seed orelse return null;

    const second_relevance = if (branches.len > 1) branches[1].query_relevance else 0.0;
    if (best.query_relevance <= second_relevance + 0.05 and best.node_count >= max_tree_generation_nodes_per_branch) {
        return null;
    }

    return .{
        .branch = best,
        .seed_key = seed.key,
        .seed_depth = seed.depth,
        .max_depth = @max(@as(i64, 1), @min(tree_search.max_depth orelse 5, 2)),
    };
}

fn selectTreeBranchExpansionSeed(
    alloc: std.mem.Allocator,
    query: []const u8,
    branch: TreeBranchSummary,
    hits: []const QueryHit,
) !?struct { key: []const u8, depth: usize } {
    var best_key: ?[]const u8 = null;
    var best_depth: usize = 0;
    var best_relevance: f32 = 0.0;

    for (hits) |hit| {
        const branch_path = treeMetaString(hit, "branch_path_text") orelse treeMetaString(hit, "path_text") orelse continue;
        if (!std.mem.eql(u8, branch.path, branch_path)) continue;
        if (treeMetaBool(hit, "leaf") orelse false) continue;

        const description = try describeHitForGeneration(alloc, hit);
        defer alloc.free(description);
        const relevance = queryCoverageScore(query, description);
        const depth = @as(usize, @intCast(treeMetaInteger(hit, "depth") orelse 0));

        if (best_key == null or
            relevance > best_relevance + 0.0001 or
            (std.math.approxEqAbs(f32, relevance, best_relevance, 0.0001) and depth > best_depth))
        {
            best_key = hit._id;
            best_depth = depth;
            best_relevance = relevance;
        }
    }

    if (best_key) |key| return .{ .key = key, .depth = best_depth };
    return null;
}

fn mergeTreeHits(
    alloc: std.mem.Allocator,
    query: []const u8,
    base_hits: []const QueryHit,
    extra_hits: []const QueryHit,
) ![]const QueryHit {
    var merged = std.ArrayListUnmanaged(QueryHit).empty;
    errdefer merged.deinit(alloc);
    var seen = std.StringHashMapUnmanaged(void){};
    defer seen.deinit(alloc);

    for (base_hits) |hit| {
        if (!seen.contains(hit._id)) {
            try seen.put(alloc, hit._id, {});
            try merged.append(alloc, hit);
        }
    }
    for (extra_hits) |hit| {
        if (!seen.contains(hit._id)) {
            try seen.put(alloc, hit._id, {});
            try merged.append(alloc, hit);
        }
    }

    if (merged.items.len > 1) {
        const maybe_branches = try rankedTreeBranchesForQuery(alloc, query, merged.items);
        defer if (maybe_branches) |branches| alloc.free(branches);
        if (maybe_branches) |branches| sortTreeHitsByBranchRank(merged.items, branches);
    }
    return try merged.toOwnedSlice(alloc);
}

fn branchBestMatchingNodeDepth(
    alloc: std.mem.Allocator,
    query: []const u8,
    branch: TreeBranchSummary,
    hits: []const QueryHit,
) !?usize {
    var best_depth: ?usize = null;
    var best_relevance: f32 = 0.0;

    for (hits) |hit| {
        const branch_path = treeMetaString(hit, "branch_path_text") orelse treeMetaString(hit, "path_text") orelse continue;
        if (!std.mem.eql(u8, branch.path, branch_path)) continue;

        const description = try describeHitForGeneration(alloc, hit);
        defer alloc.free(description);
        const relevance = queryCoverageScore(query, description);
        const depth = @as(usize, @intCast(treeMetaInteger(hit, "depth") orelse 0));

        if (best_depth == null or relevance > best_relevance + 0.0001 or
            (std.math.approxEqAbs(f32, relevance, best_relevance, 0.0001) and depth > best_depth.?))
        {
            best_depth = depth;
            best_relevance = relevance;
        }
    }

    return best_depth;
}

fn branchGenerationNodeBudget(
    alloc: std.mem.Allocator,
    query: []const u8,
    branch: TreeBranchSummary,
    hits: []const QueryHit,
) !usize {
    if (branch.node_count <= max_tree_generation_nodes_per_branch) return branch.node_count;

    var budget = max_tree_generation_nodes_per_branch;

    const prefix_summary = try buildBranchGenerationSummaryWithLimit(
        alloc,
        branch,
        hits,
        max_tree_generation_nodes_per_branch,
    );
    defer if (prefix_summary) |value| alloc.free(value);

    const prefix_relevance = queryCoverageScore(query, prefix_summary orelse branch.path);
    if (try branchBestMatchingNodeDepth(alloc, query, branch, hits)) |depth| {
        if (depth + 1 > budget and branch.query_relevance - prefix_relevance > 0.15) {
            budget = @min(branch.node_count, depth + 1);
        }
    }
    if (branch.query_relevance - prefix_relevance > 0.25) {
        budget = @max(budget, @min(branch.node_count, max_tree_generation_nodes_per_branch + 1));
    }
    return budget;
}

fn selectHitsForGenerationContext(
    alloc: std.mem.Allocator,
    query: []const u8,
    hits: []const QueryHit,
) ![]QueryHit {
    const ranked_branches = try rankedTreeBranchesForQuery(alloc, query, hits);
    defer if (ranked_branches) |branches| alloc.free(branches);

    if (ranked_branches == null) {
        const out = try alloc.alloc(QueryHit, hits.len);
        @memcpy(out, hits);
        return out;
    }

    const selected_branch_count = @min(ranked_branches.?.len, max_tree_generation_branches);
    var out = std.ArrayListUnmanaged(QueryHit).empty;
    errdefer out.deinit(alloc);
    var branch_node_counts = std.StringHashMapUnmanaged(usize){};
    defer branch_node_counts.deinit(alloc);

    for (hits) |hit| {
        const branch_path = treeMetaString(hit, "branch_path_text") orelse treeMetaString(hit, "path_text");
        if (branch_path == null) {
            try out.append(alloc, hit);
            continue;
        }

        var keep = false;
        var branch_budget = max_tree_generation_nodes_per_branch;
        for (ranked_branches.?[0..selected_branch_count]) |branch| {
            if (std.mem.eql(u8, branch.path, branch_path.?)) {
                keep = true;
                branch_budget = try branchGenerationNodeBudget(alloc, query, branch, hits);
                break;
            }
        }
        if (!keep) continue;

        const entry = try branch_node_counts.getOrPut(alloc, branch_path.?);
        if (!entry.found_existing) entry.value_ptr.* = 0;
        if (entry.value_ptr.* >= branch_budget) continue;
        entry.value_ptr.* += 1;
        try out.append(alloc, hit);
    }

    return try out.toOwnedSlice(alloc);
}

fn orderHitsForGeneration(
    alloc: std.mem.Allocator,
    hits: []const QueryHit,
) ![]QueryHit {
    const out = try alloc.alloc(QueryHit, hits.len);
    @memcpy(out, hits);
    if (hits.len <= 1) return out;

    var i: usize = 1;
    while (i < out.len) : (i += 1) {
        var j = i;
        while (j > 0) : (j -= 1) {
            if (compareGenerationHitOrder(out[j - 1], out[j]) <= 0) break;
            const tmp = out[j - 1];
            out[j - 1] = out[j];
            out[j] = tmp;
        }
    }
    return out;
}

fn compareGenerationHitOrder(lhs: QueryHit, rhs: QueryHit) i32 {
    const lhs_root = treeMetaString(lhs, "root");
    const rhs_root = treeMetaString(rhs, "root");
    if (lhs_root != null or rhs_root != null) {
        if (lhs_root == null) return 1;
        if (rhs_root == null) return -1;
        const root_cmp = std.mem.order(u8, lhs_root.?, rhs_root.?);
        if (root_cmp != .eq) return switch (root_cmp) {
            .lt => -1,
            .gt => 1,
            .eq => 0,
        };

        const lhs_branch = treeMetaString(lhs, "branch_path_text") orelse treeMetaString(lhs, "path_text") orelse lhs._id;
        const rhs_branch = treeMetaString(rhs, "branch_path_text") orelse treeMetaString(rhs, "path_text") orelse rhs._id;
        const branch_cmp = std.mem.order(u8, lhs_branch, rhs_branch);
        if (branch_cmp != .eq) return switch (branch_cmp) {
            .lt => -1,
            .gt => 1,
            .eq => 0,
        };

        const lhs_depth = treeMetaInteger(lhs, "depth") orelse 0;
        const rhs_depth = treeMetaInteger(rhs, "depth") orelse 0;
        if (lhs_depth != rhs_depth) return if (lhs_depth < rhs_depth) -1 else 1;

        const lhs_leaf = treeMetaBool(lhs, "leaf") orelse false;
        const rhs_leaf = treeMetaBool(rhs, "leaf") orelse false;
        if (lhs_leaf != rhs_leaf) return if (!lhs_leaf) -1 else 1;
    }

    const lhs_score = lhs._score;
    const rhs_score = rhs._score;
    if (!std.math.approxEqAbs(f32, lhs_score, rhs_score, 0.0001)) return if (lhs_score > rhs_score) -1 else 1;
    return switch (std.mem.order(u8, lhs._id, rhs._id)) {
        .lt => -1,
        .gt => 1,
        .eq => 0,
    };
}

fn treeMetaString(hit: QueryHit, key: []const u8) ?[]const u8 {
    const source = hit._source orelse return null;
    if (source != .object) return null;
    const meta_value = source.object.get("_tree") orelse return null;
    if (meta_value != .object) return null;
    const value = meta_value.object.get(key) orelse return null;
    return switch (value) {
        .string => |text| text,
        else => null,
    };
}

fn treeMetaInteger(hit: QueryHit, key: []const u8) ?i64 {
    const source = hit._source orelse return null;
    if (source != .object) return null;
    const meta_value = source.object.get("_tree") orelse return null;
    if (meta_value != .object) return null;
    const value = meta_value.object.get(key) orelse return null;
    return switch (value) {
        .integer => |int| int,
        .float => |float| @as(i64, @intFromFloat(float)),
        else => null,
    };
}

fn treeMetaBool(hit: QueryHit, key: []const u8) ?bool {
    const source = hit._source orelse return null;
    if (source != .object) return null;
    const meta_value = source.object.get("_tree") orelse return null;
    if (meta_value != .object) return null;
    const value = meta_value.object.get(key) orelse return null;
    return switch (value) {
        .bool => |flag| flag,
        else => null,
    };
}

fn buildTreeGenerationContext(
    alloc: std.mem.Allocator,
    hits: []const QueryHit,
) !?[]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    var seen_roots = std.StringArrayHashMapUnmanaged(void){};
    defer seen_roots.deinit(alloc);

    var saw_tree_hit = false;
    var tree_hit_count: usize = 0;
    for (hits) |hit| {
        const source = hit._source orelse continue;
        if (source != .object) continue;
        const meta_value = source.object.get("_tree") orelse continue;
        if (meta_value != .object) continue;
        const meta = meta_value.object;
        saw_tree_hit = true;
        tree_hit_count += 1;

        const root = blk: {
            if (meta.get("root")) |value| {
                if (value == .string) break :blk value.string;
            }
            break :blk hit._id;
        };
        if (!seen_roots.contains(root)) {
            try seen_roots.put(alloc, try alloc.dupe(u8, root), {});
        }
    }

    if (!saw_tree_hit) return null;

    try out.appendSlice(alloc, "Tree roots=");
    const root_count_text = try std.fmt.allocPrint(alloc, "{d}", .{seen_roots.count()});
    defer alloc.free(root_count_text);
    try out.appendSlice(alloc, root_count_text);
    try out.appendSlice(alloc, ", tree_hits=");
    const hit_count_text = try std.fmt.allocPrint(alloc, "{d}\n", .{tree_hit_count});
    defer alloc.free(hit_count_text);
    try out.appendSlice(alloc, hit_count_text);

    for (hits) |hit| {
        const source = hit._source orelse continue;
        if (source != .object) continue;
        const meta_value = source.object.get("_tree") orelse continue;
        if (meta_value != .object) continue;
        const meta = meta_value.object;

        const root = blk: {
            if (meta.get("root")) |value| {
                if (value == .string) break :blk value.string;
            }
            break :blk hit._id;
        };

        try out.appendSlice(alloc, "\nRoot ");
        try out.appendSlice(alloc, root);
        try out.appendSlice(alloc, "\n");

        try out.appendSlice(alloc, "  Node ");
        try out.appendSlice(alloc, hit._id);

        if (meta.get("depth")) |depth| {
            switch (depth) {
                .integer => |value| {
                    const text = try std.fmt.allocPrint(alloc, "{d}", .{value});
                    defer alloc.free(text);
                    try out.appendSlice(alloc, " depth=");
                    try out.appendSlice(alloc, text);
                },
                .float => |value| {
                    const text = try std.fmt.allocPrint(alloc, "{d}", .{@as(i64, @intFromFloat(value))});
                    defer alloc.free(text);
                    try out.appendSlice(alloc, " depth=");
                    try out.appendSlice(alloc, text);
                },
                else => {},
            }
        }
        if (meta.get("parent")) |parent| {
            if (parent == .string) {
                try out.appendSlice(alloc, " parent=");
                try out.appendSlice(alloc, parent.string);
            }
        }
        try out.append(alloc, '\n');

        if (meta.get("path_text")) |path_text| {
            if (path_text == .string) {
                try out.appendSlice(alloc, "  Path ");
                try out.appendSlice(alloc, path_text.string);
                try out.append(alloc, '\n');
            }
        }
        if (meta.get("branch_path_text")) |branch_path_text| {
            if (branch_path_text == .string) {
                try out.appendSlice(alloc, "  Branch ");
                try out.appendSlice(alloc, branch_path_text.string);
                try out.append(alloc, '\n');
            }
        }
        if (meta.get("leaf")) |leaf| {
            if (leaf == .bool and leaf.bool) {
                try out.appendSlice(alloc, "  Leaf true\n");
            }
        }

        if (source.object.get("title")) |title| {
            if (title == .string) {
                try out.appendSlice(alloc, "  Title ");
                try out.appendSlice(alloc, title.string);
                try out.append(alloc, '\n');
            }
        }
        if (source.object.get("body")) |body| {
            if (body == .string and body.string.len > 0) {
                try out.appendSlice(alloc, "  Body ");
                try out.appendSlice(alloc, body.string);
                try out.append(alloc, '\n');
            }
        } else if (source.object.get("content")) |content| {
            if (content == .string and content.string.len > 0) {
                try out.appendSlice(alloc, "  Content ");
                try out.appendSlice(alloc, content.string);
                try out.append(alloc, '\n');
            }
        }
    }
    return try out.toOwnedSlice(alloc);
}

const TreeBranchSummary = struct {
    root: []const u8,
    path: []const u8,
    best_hit_id: []const u8,
    best_score: f32,
    node_count: usize,
    leaf_hits: usize,
    query_relevance: f32 = 0.0,
};

const TreeBranchExpansionPlan = struct {
    branch: TreeBranchSummary,
    seed_key: []const u8,
    seed_depth: usize,
    max_depth: i64,
};

const max_tree_generation_branches: usize = 2;
const max_tree_generation_nodes_per_branch: usize = 3;

fn rankedTreeBranches(
    alloc: std.mem.Allocator,
    hits: []const QueryHit,
) !?[]TreeBranchSummary {
    var branches = std.ArrayListUnmanaged(TreeBranchSummary).empty;
    errdefer branches.deinit(alloc);

    for (hits) |hit| {
        const path = treeMetaString(hit, "branch_path_text") orelse treeMetaString(hit, "path_text") orelse continue;
        const root = treeMetaString(hit, "root") orelse hit._id;
        const is_leaf = treeMetaBool(hit, "leaf") orelse false;
        const score = hit._score;

        var found = false;
        for (branches.items) |*branch| {
            if (!std.mem.eql(u8, branch.path, path)) continue;
            found = true;
            branch.node_count += 1;
            if (is_leaf) branch.leaf_hits += 1;
            if (score > branch.best_score) {
                branch.best_score = score;
                branch.best_hit_id = hit._id;
            }
            break;
        }
        if (!found) {
            try branches.append(alloc, .{
                .root = root,
                .path = path,
                .best_hit_id = hit._id,
                .best_score = score,
                .node_count = 1,
                .leaf_hits = if (is_leaf) 1 else 0,
            });
        }
    }

    if (branches.items.len == 0) {
        branches.deinit(alloc);
        return null;
    }

    var i: usize = 1;
    while (i < branches.items.len) : (i += 1) {
        var j = i;
        while (j > 0) : (j -= 1) {
            if (compareTreeBranchSummary(branches.items[j - 1], branches.items[j]) <= 0) break;
            const tmp = branches.items[j - 1];
            branches.items[j - 1] = branches.items[j];
            branches.items[j] = tmp;
        }
    }

    return try branches.toOwnedSlice(alloc);
}

fn rankedTreeBranchesForQuery(
    alloc: std.mem.Allocator,
    query: []const u8,
    hits: []const QueryHit,
) !?[]TreeBranchSummary {
    const maybe_branches = try rankedTreeBranches(alloc, hits);
    const branches = maybe_branches orelse return null;
    errdefer alloc.free(branches);

    for (branches) |*branch| {
        const summary = try buildBranchGenerationSummary(alloc, branch.*, hits);
        defer if (summary) |value| alloc.free(value);
        branch.query_relevance = queryCoverageScore(query, summary orelse branch.path);
    }

    var i: usize = 1;
    while (i < branches.len) : (i += 1) {
        var j = i;
        while (j > 0) : (j -= 1) {
            if (compareTreeBranchSummaryForQuery(branches[j - 1], branches[j]) <= 0) break;
            const tmp = branches[j - 1];
            branches[j - 1] = branches[j];
            branches[j] = tmp;
        }
    }

    return branches;
}

fn buildTreeBranchSelectionContext(
    alloc: std.mem.Allocator,
    hits: []const QueryHit,
) !?[]u8 {
    const maybe_branches = try rankedTreeBranches(alloc, hits);
    const branches = maybe_branches orelse return null;
    defer alloc.free(branches);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    for (branches, 0..) |branch, idx| {
        const number = try std.fmt.allocPrint(alloc, "{d}", .{idx + 1});
        defer alloc.free(number);
        const score = try std.fmt.allocPrint(alloc, "{d:.2}", .{branch.best_score});
        defer alloc.free(score);
        try out.appendSlice(alloc, number);
        try out.appendSlice(alloc, ". root=");
        try out.appendSlice(alloc, branch.root);
        try out.appendSlice(alloc, " path=");
        try out.appendSlice(alloc, branch.path);
        try out.appendSlice(alloc, " best_hit=");
        try out.appendSlice(alloc, branch.best_hit_id);
        try out.appendSlice(alloc, " best_score=");
        try out.appendSlice(alloc, score);
        try out.appendSlice(alloc, " nodes=");
        const nodes = try std.fmt.allocPrint(alloc, "{d}", .{branch.node_count});
        defer alloc.free(nodes);
        try out.appendSlice(alloc, nodes);
        try out.appendSlice(alloc, " leaf_hits=");
        const leaf_hits = try std.fmt.allocPrint(alloc, "{d}", .{branch.leaf_hits});
        defer alloc.free(leaf_hits);
        try out.appendSlice(alloc, leaf_hits);
        try out.append(alloc, '\n');
    }
    return try out.toOwnedSlice(alloc);
}

fn buildTreeBranchSelectionContextForQuery(
    alloc: std.mem.Allocator,
    query: []const u8,
    hits: []const QueryHit,
) !?[]u8 {
    const maybe_branches = try rankedTreeBranchesForQuery(alloc, query, hits);
    const branches = maybe_branches orelse return null;
    defer alloc.free(branches);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    for (branches, 0..) |branch, idx| {
        const number = try std.fmt.allocPrint(alloc, "{d}", .{idx + 1});
        defer alloc.free(number);
        const score = try std.fmt.allocPrint(alloc, "{d:.2}", .{branch.best_score});
        defer alloc.free(score);
        const relevance = try std.fmt.allocPrint(alloc, "{d:.2}", .{branch.query_relevance});
        defer alloc.free(relevance);
        try out.appendSlice(alloc, number);
        try out.appendSlice(alloc, ". root=");
        try out.appendSlice(alloc, branch.root);
        try out.appendSlice(alloc, " path=");
        try out.appendSlice(alloc, branch.path);
        try out.appendSlice(alloc, " best_hit=");
        try out.appendSlice(alloc, branch.best_hit_id);
        try out.appendSlice(alloc, " best_score=");
        try out.appendSlice(alloc, score);
        try out.appendSlice(alloc, " query_relevance=");
        try out.appendSlice(alloc, relevance);
        try out.appendSlice(alloc, " nodes=");
        const nodes = try std.fmt.allocPrint(alloc, "{d}", .{branch.node_count});
        defer alloc.free(nodes);
        try out.appendSlice(alloc, nodes);
        try out.appendSlice(alloc, " leaf_hits=");
        const leaf_hits = try std.fmt.allocPrint(alloc, "{d}", .{branch.leaf_hits});
        defer alloc.free(leaf_hits);
        try out.appendSlice(alloc, leaf_hits);
        try out.append(alloc, '\n');
    }
    return try out.toOwnedSlice(alloc);
}

fn compareTreeBranchSummary(lhs: TreeBranchSummary, rhs: TreeBranchSummary) i32 {
    if (!std.math.approxEqAbs(f32, lhs.best_score, rhs.best_score, 0.0001)) {
        return if (lhs.best_score > rhs.best_score) -1 else 1;
    }
    if (lhs.node_count != rhs.node_count) return if (lhs.node_count > rhs.node_count) -1 else 1;
    return switch (std.mem.order(u8, lhs.path, rhs.path)) {
        .lt => -1,
        .gt => 1,
        .eq => 0,
    };
}

fn compareTreeBranchSummaryForQuery(lhs: TreeBranchSummary, rhs: TreeBranchSummary) i32 {
    if (!std.math.approxEqAbs(f32, lhs.query_relevance, rhs.query_relevance, 0.0001)) {
        return if (lhs.query_relevance > rhs.query_relevance) -1 else 1;
    }
    return compareTreeBranchSummary(lhs, rhs);
}

fn describeHitForGeneration(
    alloc: std.mem.Allocator,
    hit: QueryHit,
) ![]const u8 {
    const source = hit._source orelse return try alloc.dupe(u8, "null");
    if (source != .object) {
        // Use page_allocator to avoid @memcpy aliasing with arena-backed json strings.
        var tmp: std.Io.Writer.Allocating = .init(std.heap.page_allocator);
        defer tmp.deinit();
        try std.json.Stringify.value(source, .{}, &tmp.writer);
        return try alloc.dupe(u8, tmp.written());
    }

    const object = source.object;
    const tree_meta = object.get("_tree");
    // Use page_allocator to avoid @memcpy aliasing with arena-backed json strings.
    const encoded_source = blk: {
        var tmp: std.Io.Writer.Allocating = .init(std.heap.page_allocator);
        defer tmp.deinit();
        try std.json.Stringify.value(source, .{}, &tmp.writer);
        break :blk try alloc.dupe(u8, tmp.written());
    };
    if (tree_meta == null or tree_meta.? != .object) return encoded_source;

    const meta = tree_meta.?.object;
    const depth = switch (meta.get("depth") orelse .null) {
        .integer => |value| value,
        .float => |value| @as(i64, @intFromFloat(value)),
        else => 0,
    };
    const relation = switch (meta.get("search") orelse .null) {
        .string => |value| value,
        else => "tree",
    };
    const parent = switch (meta.get("parent") orelse .null) {
        .string => |value| value,
        else => null,
    };
    const root = switch (meta.get("root") orelse .null) {
        .string => |value| value,
        else => null,
    };
    const path_text = switch (meta.get("path_text") orelse .null) {
        .string => |value| value,
        else => null,
    };
    const branch_path_text = switch (meta.get("branch_path_text") orelse .null) {
        .string => |value| value,
        else => null,
    };
    const leaf = switch (meta.get("leaf") orelse .null) {
        .bool => |value| value,
        else => false,
    };

    if (path_text) |path| {
        const path_value = branch_path_text orelse path;
        if (parent) |parent_id| {
            if (root) |root_id| {
                return try std.fmt.allocPrint(alloc, "tree_result(search={s}, depth={d}, root={s}, parent={s}, path={s}, leaf={}) {s}", .{
                    relation,
                    depth,
                    root_id,
                    parent_id,
                    path_value,
                    leaf,
                    encoded_source,
                });
            }
            return try std.fmt.allocPrint(alloc, "tree_result(search={s}, depth={d}, parent={s}, path={s}, leaf={}) {s}", .{
                relation,
                depth,
                parent_id,
                path_value,
                leaf,
                encoded_source,
            });
        }
        if (root) |root_id| {
            return try std.fmt.allocPrint(alloc, "tree_result(search={s}, depth={d}, root={s}, path={s}, leaf={}) {s}", .{
                relation,
                depth,
                root_id,
                path_value,
                leaf,
                encoded_source,
            });
        }
        return try std.fmt.allocPrint(alloc, "tree_result(search={s}, depth={d}, path={s}, leaf={}) {s}", .{
            relation,
            depth,
            path_value,
            leaf,
            encoded_source,
        });
    }

    if (parent) |parent_id| {
        if (root) |root_id| {
            return try std.fmt.allocPrint(alloc, "tree_result(search={s}, depth={d}, root={s}, parent={s}) {s}", .{
                relation,
                depth,
                root_id,
                parent_id,
                encoded_source,
            });
        }
        return try std.fmt.allocPrint(alloc, "tree_result(search={s}, depth={d}, parent={s}) {s}", .{
            relation,
            depth,
            parent_id,
            encoded_source,
        });
    }
    if (root) |root_id| {
        return try std.fmt.allocPrint(alloc, "tree_result(search={s}, depth={d}, root={s}) {s}", .{
            relation,
            depth,
            root_id,
            encoded_source,
        });
    }
    return try std.fmt.allocPrint(alloc, "tree_result(search={s}, depth={d}) {s}", .{
        relation,
        depth,
        encoded_source,
    });
}

fn buildClassificationResult(
    alloc: std.mem.Allocator,
    query: []const u8,
    cfg: ParsedClassificationConfig,
) !ai_openapi.ClassificationTransformationResult {
    const route_type: ai_openapi.RouteType = if (isQuestionLike(query)) .question else .search;
    const strategy = cfg.force_strategy orelse inferClassificationStrategy(query);
    const semantic_mode = cfg.force_semantic_mode orelse inferSemanticMode(strategy);
    const improved_query = try buildImprovedQuery(alloc, query, route_type, strategy);
    const semantic_query = try buildSemanticQuery(alloc, query, strategy, semantic_mode);
    const step_back_query = if (strategy == .step_back) try buildStepBackQuery(alloc, query) else null;
    const sub_questions = if (strategy == .decompose) try buildSubQuestions(alloc, query) else null;
    const multi_phrases = try buildMultiPhrases(alloc, query, route_type);
    const confidence = classificationConfidence(strategy, route_type);
    const reasoning = if (cfg.with_reasoning)
        try std.fmt.allocPrint(alloc, "Selected {s} retrieval in {s} mode for the current query.", .{
            @tagName(strategy),
            @tagName(semantic_mode),
        })
    else
        null;
    return .{
        .route_type = route_type,
        .strategy = strategy,
        .semantic_mode = semantic_mode,
        .improved_query = improved_query,
        .semantic_query = semantic_query,
        .step_back_query = step_back_query,
        .sub_questions = sub_questions,
        .multi_phrases = multi_phrases,
        .reasoning = reasoning,
        .confidence = confidence,
    };
}

fn inferClassificationStrategy(query: []const u8) ai_openapi.QueryStrategy {
    if (containsAnyIgnoreCase(query, &.{ " and ", "compare", "versus", " vs ", "difference between" })) return .decompose;
    if (containsAnyIgnoreCase(query, &.{ "how does", "why does", "architecture", "workflow", "background" })) return .step_back;
    if (containsAnyIgnoreCase(query, &.{ "overview", "benefits", "tradeoffs", "concept", "summarize" })) return .hyde;
    return .simple;
}

fn inferSemanticMode(strategy: ai_openapi.QueryStrategy) ai_openapi.SemanticQueryMode {
    return switch (strategy) {
        .hyde => .hypothetical,
        .simple, .decompose, .step_back => .rewrite,
    };
}

fn buildImprovedQuery(
    alloc: std.mem.Allocator,
    query: []const u8,
    route_type: ai_openapi.RouteType,
    strategy: ai_openapi.QueryStrategy,
) ![]const u8 {
    const route_hint = switch (route_type) {
        .question => "Question",
        .search => "Search",
    };
    return try std.fmt.allocPrint(alloc, "{s} for Antfly docs using {s} strategy: {s}", .{
        route_hint,
        @tagName(strategy),
        query,
    });
}

fn buildSemanticQuery(
    alloc: std.mem.Allocator,
    query: []const u8,
    strategy: ai_openapi.QueryStrategy,
    semantic_mode: ai_openapi.SemanticQueryMode,
) ![]const u8 {
    return switch (semantic_mode) {
        .rewrite => switch (strategy) {
            .decompose => try std.fmt.allocPrint(alloc, "antfly {s} split into focused retrieval sub-questions", .{query}),
            .step_back => try std.fmt.allocPrint(alloc, "antfly background concepts and context for {s}", .{query}),
            .simple, .hyde => try std.fmt.allocPrint(alloc, "antfly {s}", .{query}),
        },
        .hypothetical => try std.fmt.allocPrint(alloc, "A relevant Antfly document would explain: {s}", .{query}),
    };
}

fn buildStepBackQuery(alloc: std.mem.Allocator, query: []const u8) ![]const u8 {
    return try std.fmt.allocPrint(alloc, "Background context and core Antfly concepts needed for: {s}", .{query});
}

fn buildSubQuestions(alloc: std.mem.Allocator, query: []const u8) ![]const []const u8 {
    var items = std.ArrayListUnmanaged([]const u8).empty;
    errdefer items.deinit(alloc);

    if (std.mem.indexOf(u8, query, " and ")) |idx| {
        const left = std.mem.trim(u8, query[0..idx], " \t\r\n?,.");
        const right = std.mem.trim(u8, query[idx + 5 ..], " \t\r\n?,.");
        if (left.len > 0) try items.append(alloc, try std.fmt.allocPrint(alloc, "{s}?", .{left}));
        if (right.len > 0) try items.append(alloc, try std.fmt.allocPrint(alloc, "{s}?", .{right}));
    } else if (std.mem.indexOf(u8, query, " vs ")) |idx| {
        const left = std.mem.trim(u8, query[0..idx], " \t\r\n?,.");
        const right = std.mem.trim(u8, query[idx + 4 ..], " \t\r\n?,.");
        if (left.len > 0) try items.append(alloc, try std.fmt.allocPrint(alloc, "What should I know about {s}?", .{left}));
        if (right.len > 0) try items.append(alloc, try std.fmt.allocPrint(alloc, "How does {s} compare here?", .{right}));
    }

    if (items.items.len == 0) {
        try items.append(alloc, try std.fmt.allocPrint(alloc, "What is the main concept behind {s}?", .{query}));
        try items.append(alloc, try std.fmt.allocPrint(alloc, "What implementation details matter for {s}?", .{query}));
    }

    return try items.toOwnedSlice(alloc);
}

fn buildMultiPhrases(
    alloc: std.mem.Allocator,
    query: []const u8,
    route_type: ai_openapi.RouteType,
) ![]const []const u8 {
    const phrases = try alloc.alloc([]const u8, 3);
    phrases[0] = try alloc.dupe(u8, query);
    phrases[1] = try std.fmt.allocPrint(alloc, "antfly {s}", .{query});
    phrases[2] = switch (route_type) {
        .question => try std.fmt.allocPrint(alloc, "answer for {s}", .{query}),
        .search => try std.fmt.allocPrint(alloc, "documents about {s}", .{query}),
    };
    return phrases;
}

fn classificationConfidence(strategy: ai_openapi.QueryStrategy, route_type: ai_openapi.RouteType) f32 {
    const base: f32 = switch (strategy) {
        .simple => 0.86,
        .decompose => 0.74,
        .step_back => 0.78,
        .hyde => 0.72,
    };
    return if (route_type == .question) @min(1.0, base + 0.04) else base;
}

const ConfidenceScores = struct {
    generation_confidence: f32,
    context_relevance: f32,
};

fn buildEvalResult(
    alloc: std.mem.Allocator,
    query: []const u8,
    hits: []const QueryHit,
    generated_content: ?[]const u8,
    generation_confidence: ?f32,
    context_relevance: ?f32,
    cfg: ParsedEvalConfig,
) !eval_openapi.EvalResult {
    const context_text = try buildContextText(alloc, hits);
    const scores = try buildEvalScores(
        alloc,
        query,
        hits,
        generated_content,
        context_text,
        generation_confidence,
        context_relevance,
        cfg,
    );
    const summary = summarizeEvalScores(scores, cfg.pass_threshold);
    return .{
        .scores = scores,
        .summary = summary,
        .duration_ms = 0,
    };
}

fn buildContextText(
    alloc: std.mem.Allocator,
    hits: []const QueryHit,
) ![]const u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    for (hits) |hit| {
        try out.appendSlice(alloc, hit._id);
        try out.appendSlice(alloc, " ");
        if (hit._source) |source| {
            // Use page_allocator for the serialization buffer to avoid
            // @memcpy aliasing when the arena backs both the source
            // json strings and the writer's internal buffer.
            var tmp: std.Io.Writer.Allocating = .init(std.heap.page_allocator);
            defer tmp.deinit();
            try std.json.Stringify.value(source, .{}, &tmp.writer);
            try out.appendSlice(alloc, tmp.written());
        }
        try out.append(alloc, '\n');
    }

    return try out.toOwnedSlice(alloc);
}

fn buildEvalScores(
    alloc: std.mem.Allocator,
    query: []const u8,
    hits: []const QueryHit,
    generated_content: ?[]const u8,
    context_text: []const u8,
    generation_confidence: ?f32,
    context_relevance: ?f32,
    cfg: ParsedEvalConfig,
) !eval_openapi.EvalScores {
    var retrieval_scores = std.json.ArrayHashMap(eval_openapi.EvaluatorScore){};
    var generation_scores = std.json.ArrayHashMap(eval_openapi.EvaluatorScore){};
    errdefer {
        retrieval_scores.deinit(alloc);
        generation_scores.deinit(alloc);
    }

    for (cfg.evaluators) |evaluator| {
        switch (evaluator) {
            .recall, .precision, .ndcg, .mrr, .map => {
                try retrieval_scores.map.put(alloc, @tagName(evaluator), evaluateRetrievalMetric(evaluator, hits, cfg));
            },
            .relevance, .faithfulness, .completeness, .coherence, .safety, .helpfulness, .correctness, .citation_quality => {
                try generation_scores.map.put(alloc, @tagName(evaluator), evaluateJudgeMetric(
                    evaluator,
                    query,
                    hits,
                    generated_content,
                    context_text,
                    generation_confidence,
                    context_relevance,
                    cfg,
                ));
            },
        }
    }

    return .{
        .retrieval = if (retrieval_scores.map.count() == 0) null else retrieval_scores,
        .generation = if (generation_scores.map.count() == 0) null else generation_scores,
    };
}

fn evaluateRetrievalMetric(
    evaluator: eval_openapi.EvaluatorName,
    hits: []const QueryHit,
    cfg: ParsedEvalConfig,
) eval_openapi.EvaluatorScore {
    const k = @min(cfg.k, hits.len);
    const relevant_total = cfg.relevant_ids.len;
    var relevant_hits: usize = 0;
    var first_rank: ?usize = null;
    var precision_sum: f32 = 0.0;
    var dcg: f32 = 0.0;

    for (hits[0..k], 0..) |hit, index| {
        if (!containsString(cfg.relevant_ids, hit._id)) continue;
        relevant_hits += 1;
        const rank = index + 1;
        if (first_rank == null) first_rank = rank;
        precision_sum += @as(f32, @floatFromInt(relevant_hits)) / @as(f32, @floatFromInt(rank));
        dcg += 1.0 / @as(f32, @floatCast(std.math.log2(@as(f64, @floatFromInt(rank + 1)))));
    }

    const score: f32 = switch (evaluator) {
        .recall => if (relevant_total == 0) 0.0 else @as(f32, @floatFromInt(relevant_hits)) / @as(f32, @floatFromInt(relevant_total)),
        .precision => if (k == 0) 0.0 else @as(f32, @floatFromInt(relevant_hits)) / @as(f32, @floatFromInt(k)),
        .mrr => if (first_rank) |rank| 1.0 / @as(f32, @floatFromInt(rank)) else 0.0,
        .map => if (relevant_total == 0) 0.0 else precision_sum / @as(f32, @floatFromInt(relevant_total)),
        .ndcg => blk: {
            var idcg: f32 = 0.0;
            const ideal = @min(relevant_total, k);
            for (0..ideal) |index| {
                idcg += 1.0 / @as(f32, @floatCast(std.math.log2(@as(f64, @floatFromInt(index + 2)))));
            }
            break :blk if (idcg == 0.0) 0.0 else dcg / idcg;
        },
        else => 0.0,
    };

    return .{
        .score = score,
        .pass = score >= cfg.pass_threshold,
        .reason = switch (evaluator) {
            .recall => "Fraction of known relevant documents retrieved.",
            .precision => "Fraction of returned hits that were relevant.",
            .ndcg => "Ranking quality against the provided relevant ids.",
            .mrr => "Rank position of the first relevant result.",
            .map => "Average precision across relevant retrieval positions.",
            else => null,
        },
    };
}

fn evaluateJudgeMetric(
    evaluator: eval_openapi.EvaluatorName,
    query: []const u8,
    hits: []const QueryHit,
    generated_content: ?[]const u8,
    context_text: []const u8,
    generation_confidence: ?f32,
    context_relevance: ?f32,
    cfg: ParsedEvalConfig,
) eval_openapi.EvaluatorScore {
    const response_text = generated_content orelse "";
    const relevance_base = context_relevance orelse queryCoverageScore(query, context_text);
    const generation_base = generation_confidence orelse queryCoverageScore(query, response_text);
    const expectation_text = cfg.expectations orelse query;

    const score: f32 = switch (evaluator) {
        .relevance => @min(1.0, 0.55 * relevance_base + 0.45 * queryCoverageScore(query, if (generated_content != null) response_text else context_text)),
        .faithfulness => @min(1.0, 0.55 * overlapScore(response_text, context_text) + 0.45 * (context_relevance orelse 0.0)),
        .completeness => @min(1.0, 0.45 * queryCoverageScore(expectation_text, response_text) + 0.35 * generation_base + 0.20 * lengthScore(response_text)),
        .coherence => @min(1.0, 0.45 + 0.55 * lengthScore(response_text)),
        .safety => 0.95,
        .helpfulness => @min(1.0, 0.5 * generation_base + 0.5 * queryCoverageScore(expectation_text, response_text)),
        .correctness => @min(1.0, 0.4 * queryCoverageScore(expectation_text, response_text) + 0.6 * overlapScore(response_text, context_text)),
        .citation_quality => if (std.mem.indexOf(u8, response_text, "doc:") != null or std.mem.indexOf(u8, response_text, "[") != null) 0.9 else if (hits.len > 0) 0.55 else 0.0,
        else => 0.0,
    };

    return .{
        .score = score,
        .pass = score >= cfg.pass_threshold,
        .reason = switch (evaluator) {
            .relevance => "Heuristic relevance of the retrieved or generated response to the user query.",
            .faithfulness => "Heuristic grounding of the generated response in retrieved context.",
            .completeness => "Heuristic coverage of the requested concepts in the response.",
            .coherence => "Heuristic fluency score based on response structure and length.",
            .safety => "Bounded retrieval-agent eval currently treats safe internal docs responses as high safety.",
            .helpfulness => "Heuristic usefulness based on answer coverage and answer presence.",
            .correctness => "Heuristic correctness based on expectations and retrieved context overlap.",
            .citation_quality => "Heuristic citation presence and retrieved-context availability.",
            else => null,
        },
    };
}

fn summarizeEvalScores(
    scores: eval_openapi.EvalScores,
    pass_threshold: f32,
) eval_openapi.EvalSummary {
    var total: i64 = 0;
    var passed: i64 = 0;
    var sum: f32 = 0.0;

    if (scores.retrieval) |retrieval| {
        var it = retrieval.map.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.score) |score| {
                sum += score;
                total += 1;
                if ((entry.value_ptr.pass orelse (score >= pass_threshold))) passed += 1;
            }
        }
    }
    if (scores.generation) |generation| {
        var it = generation.map.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.score) |score| {
                sum += score;
                total += 1;
                if ((entry.value_ptr.pass orelse (score >= pass_threshold))) passed += 1;
            }
        }
    }

    return .{
        .average_score = if (total == 0) 0.0 else sum / @as(f32, @floatFromInt(total)),
        .passed = passed,
        .failed = total - passed,
        .total = total,
    };
}

fn containsString(items: []const []const u8, needle: []const u8) bool {
    for (items) |item| {
        if (std.mem.eql(u8, item, needle)) return true;
    }
    return false;
}

fn queryCoverageScore(query: []const u8, text: []const u8) f32 {
    var total: usize = 0;
    var matched: usize = 0;
    var it = std.mem.tokenizeAny(u8, query, " \t\r\n,.;:!?()[]{}<>/\\|+-=_\"'");
    while (it.next()) |token| {
        if (token.len < 4) continue;
        total += 1;
        if (std.ascii.indexOfIgnoreCase(text, token) != null) matched += 1;
    }
    return if (total == 0) 0.0 else @as(f32, @floatFromInt(matched)) / @as(f32, @floatFromInt(total));
}

fn overlapScore(lhs: []const u8, rhs: []const u8) f32 {
    if (lhs.len == 0 or rhs.len == 0) return 0.0;
    const left = queryCoverageScore(lhs, rhs);
    const right = queryCoverageScore(rhs, lhs);
    return @min(1.0, 0.5 * left + 0.5 * right);
}

fn lengthScore(text: []const u8) f32 {
    if (text.len == 0) return 0.0;
    return @min(1.0, @as(f32, @floatFromInt(text.len)) / 160.0);
}

const ClarificationState = struct {
    interactive: bool,
    count: i64,
    remaining: i64,
    require_decision_after: ?i64,
    decisions: []const AgentDecision,
};

const AgenticSelectionSource = enum {
    single_query,
    decompose,
    broaden_decision,
    user_decision,
    probe,
    evaluation,
    heuristic,
};

const AgenticCandidateScore = struct {
    index: usize,
    strategy: RetrievalStrategy,
    score: i32,
    probe_hits: ?i64 = null,
    probe_relevance: ?f32 = null,
    probe_top_score: ?f32 = null,
};

const AgenticSelection = struct {
    indices: ?[]const usize = null,
    question: ?AgentQuestion = null,
    incomplete_reason: ?[]const u8 = null,
    source: AgenticSelectionSource = .heuristic,
    candidate_scores: ?[]const AgenticCandidateScore = null,
};

fn scoreConfidence(
    hits: []const QueryHit,
    generated_content: ?[]const u8,
) ConfidenceScores {
    const hit_factor: f32 = if (hits.len == 0) 0.0 else @min(1.0, 0.35 + @as(f32, @floatFromInt(hits.len)) * 0.15);
    const generation_factor: f32 = if (generated_content != null and generated_content.?.len > 0) 0.85 else 0.0;
    return .{
        .generation_confidence = if (generation_factor == 0.0) 0.0 else @min(1.0, 0.45 * hit_factor + 0.55 * generation_factor),
        .context_relevance = hit_factor,
    };
}

fn parseClarificationState(request: RetrievalAgentRequest) !ClarificationState {
    const interactive = request.interactive orelse true;
    const count = if (request.decisions) |decisions| @as(i64, @intCast(decisions.len)) else 0;
    const default_max: i64 = if (interactive) 1 else 0;
    const max_clarifications = @max(@as(i64, 0), request.max_user_clarifications orelse default_max);
    return .{
        .interactive = interactive,
        .count = count,
        .remaining = @max(@as(i64, 0), max_clarifications - count),
        .require_decision_after = request.require_decision_after,
        .decisions = request.decisions orelse &.{},
    };
}

fn selectAgenticQueries(
    alloc: std.mem.Allocator,
    request: RetrievalAgentRequest,
    retrieval_queries: []const RetrievalQueryRequest,
    clarification_state: ClarificationState,
) !?AgenticSelection {
    if (retrieval_queries.len == 0) return null;
    if (retrieval_queries.len == 1) {
        return .{
            .indices = try alloc.dupe(usize, &[_]usize{0}),
            .source = .single_query,
            .candidate_scores = try buildAgenticCandidateScores(alloc, request.query, retrieval_queries, preferredAgenticQueryStrategy(request)),
        };
    }

    const preferred_strategy = preferredAgenticQueryStrategy(request);
    const candidate_scores = try buildAgenticCandidateScores(alloc, request.query, retrieval_queries, preferred_strategy);
    if (preferred_strategy == .decompose) {
        return .{
            .indices = try allQueryIndices(alloc, retrieval_queries.len),
            .source = .decompose,
            .candidate_scores = candidate_scores,
        };
    }

    if (decisionApproved(clarification_state.decisions, "broaden_search")) {
        return .{
            .indices = try allQueryIndices(alloc, retrieval_queries.len),
            .source = .broaden_decision,
            .candidate_scores = candidate_scores,
        };
    }

    const decision_index = try resolveAgenticDecisionSelection(request.decisions orelse &.{}, retrieval_queries.len);
    if (decision_index) |value| {
        return .{
            .indices = try alloc.dupe(usize, &[_]usize{value}),
            .source = .user_decision,
            .candidate_scores = candidate_scores,
        };
    }

    var best_index: usize = 0;
    var best_score: i32 = std.math.minInt(i32);
    var second_best_score: i32 = std.math.minInt(i32);
    for (retrieval_queries, 0..) |retrieval_query, i| {
        const score = scoreAgenticQueryCandidate(request.query, retrieval_query, preferred_strategy);
        if (score > best_score) {
            second_best_score = best_score;
            best_score = score;
            best_index = i;
        } else if (score > second_best_score) {
            second_best_score = score;
        }
    }

    const ambiguous = retrieval_queries.len > 1 and (best_score - second_best_score) <= 10;
    const must_decide_now = if (request.require_decision_after) |limit|
        limit <= 0
    else
        false;
    const decision_count: i64 = if (request.decisions) |decisions| @intCast(decisions.len) else 0;
    const can_clarify = (request.interactive orelse true) and ((request.max_user_clarifications orelse 1) - decision_count > 0);
    if (ambiguous or must_decide_now) {
        if (can_clarify) {
            return .{
                .question = try buildAgenticSelectionQuestion(alloc, request.query, retrieval_queries),
                .candidate_scores = candidate_scores,
            };
        }
        return .{
            .incomplete_reason = "clarification_required",
            .candidate_scores = candidate_scores,
        };
    }

    return .{
        .indices = try alloc.dupe(usize, &[_]usize{best_index}),
        .source = .heuristic,
        .candidate_scores = candidate_scores,
    };
}

fn maybeProbeAgenticSelection(
    alloc: std.mem.Allocator,
    arena: std.mem.Allocator,
    runner: QueryRunner,
    raw_queries: []const std.json.Value,
    retrieval_queries: []const RetrievalQueryRequest,
    classification_result: ?ai_openapi.ClassificationTransformationResult,
    selection: AgenticSelection,
) !?AgenticSelection {
    const existing_scores = selection.candidate_scores orelse return selection;
    if (retrieval_queries.len < 2) return selection;

    var scores = try arena.dupe(AgenticCandidateScore, existing_scores);
    const probe_indices = try topProbeCandidateIndices(arena, scores, retrieval_queries);
    if (probe_indices.len < 2) {
        return .{
            .indices = selection.indices,
            .question = selection.question,
            .incomplete_reason = selection.incomplete_reason,
            .source = selection.source,
            .candidate_scores = scores,
        };
    }

    for (probe_indices) |candidate_index| {
        const retrieval_query = retrieval_queries[candidate_index];
        if (!isProbeableRetrievalQuery(retrieval_query)) continue;
        const query_json = try encodeQueryValueForRetrievalQuery(
            alloc,
            runner,
            raw_queries[candidate_index],
            retrieval_query,
            &.{},
            classification_result,
            candidate_index,
            .initial,
        );
        defer alloc.free(query_json);

        var query_response = runner.runQuery(
            alloc,
            retrieval_query.table orelse return error.InvalidRetrievalAgentRequest,
            query_json,
        ) catch continue;
        defer query_response.deinit(alloc);

        var parsed_query = std.json.parseFromSlice(QueryResponses, arena, query_response.json, .{
            .ignore_unknown_fields = true,
            .allocate = .alloc_always,
        }) catch continue;
        defer parsed_query.deinit();

        const fallback_tree_root = if (retrieval_query.tree_search != null)
            try extractTreeFallbackRootKey(arena, query_json)
        else
            null;
        const query_hits = if (retrieval_query.tree_search != null)
            try extractTreeHits(arena, parsed_query.value, queryTextForProbe(classification_result, retrieval_query), fallback_tree_root)
        else
            extractHits(parsed_query.value);

        scores[candidate_index].probe_hits = @intCast(query_hits.len);
        if (query_hits.len > 0) {
            const probe_context = buildContextText(arena, query_hits[0..@min(query_hits.len, 3)]) catch "";
            scores[candidate_index].probe_relevance = queryCoverageScore(queryTextForProbe(classification_result, retrieval_query), probe_context);
        }
        scores[candidate_index].probe_top_score = if (query_hits.len > 0) query_hits[0]._score else 0.0;
    }

    const winner = selectProbeWinner(scores, probe_indices) orelse return .{
        .indices = selection.indices,
        .question = selection.question,
        .incomplete_reason = selection.incomplete_reason,
        .source = selection.source,
        .candidate_scores = scores,
    };

    return .{
        .indices = try arena.dupe(usize, &[_]usize{winner}),
        .source = .probe,
        .candidate_scores = scores,
    };
}

fn detectAgenticEvaluationTrigger(
    query: []const u8,
    classification_result: ?ai_openapi.ClassificationTransformationResult,
    attempted_strategy: RetrievalStrategy,
    attempt_summary: AttemptEvaluationSummary,
    candidate_scores: []const AgenticCandidateScore,
    attempted_query_indices: []const bool,
) AgenticEvaluationTrigger {
    if (attempt_summary.hit_count == 0) return .empty_result;
    if (!hasUnattemptedAgenticCandidate(candidate_scores, attempted_query_indices)) return .none;

    const classification = classification_result orelse return .none;
    if (query.len < 24) return .none;
    const relevance = attempt_summary.context_relevance orelse return .none;
    const context_length = attempt_summary.context_length orelse 0;

    if (attempted_strategy == .bm25 or attempted_strategy == .metadata) {
        if (!hasUnattemptedSemanticFallback(candidate_scores, attempted_query_indices)) return .none;
        if (classification.strategy != .simple) return .none;
        if (attempt_summary.hit_count == 1) return .weak_result;
        if (relevance < 0.25) return .weak_result;
        if (attempt_summary.hit_count <= 2 and relevance < 0.35 and lengthScoreFromLength(context_length) < 0.7) return .weak_result;
        return .none;
    }

    if (classification.strategy != .simple and classification.strategy != .step_back and classification.strategy != .hyde) return .none;
    switch (attempted_strategy) {
        .semantic, .hybrid => {
            if (attempt_summary.hit_count <= 2 and relevance < 0.22) return .partial_result;
            if (attempt_summary.hit_count <= 3 and relevance < 0.16 and lengthScoreFromLength(context_length) < 0.75) return .partial_result;
        },
        .tree => {
            const branch_relevance = attempt_summary.top_tree_branch_relevance orelse 0.0;
            const branch_nodes = attempt_summary.top_tree_branch_nodes orelse 0;
            const branch_leaf_hits = attempt_summary.top_tree_branch_leaf_hits orelse 0;
            if (branch_leaf_hits == 0 and branch_nodes > 0 and branch_nodes <= 3 and branch_relevance >= 0.12) return .partial_result;
            if (attempt_summary.hit_count <= 2 and relevance < 0.22) return .partial_result;
        },
        else => {},
    }
    return .none;
}

fn lengthScoreFromLength(text_len: i64) f32 {
    const len_f: f32 = @floatFromInt(@max(@as(i64, 0), text_len));
    return @min(1.0, len_f / 120.0);
}

fn planNextAgenticFallback(
    alloc: std.mem.Allocator,
    arena: std.mem.Allocator,
    runner: QueryRunner,
    raw_queries: []const std.json.Value,
    retrieval_queries: []const RetrievalQueryRequest,
    classification_result: ?ai_openapi.ClassificationTransformationResult,
    candidate_scores: []const AgenticCandidateScore,
    attempted_query_indices: []const bool,
) !?AgenticFallbackPlan {
    const refined_scores = try probeAgenticFallbackCandidates(
        alloc,
        arena,
        runner,
        raw_queries,
        retrieval_queries,
        classification_result,
        candidate_scores,
        attempted_query_indices,
    );
    const next_index = selectNextAgenticFallbackIndex(refined_scores, attempted_query_indices) orelse return null;
    return .{
        .indices = try alloc.dupe(usize, &[_]usize{next_index}),
        .source = .evaluation,
        .candidate_scores = refined_scores,
    };
}

fn probeAgenticFallbackCandidates(
    alloc: std.mem.Allocator,
    arena: std.mem.Allocator,
    runner: QueryRunner,
    raw_queries: []const std.json.Value,
    retrieval_queries: []const RetrievalQueryRequest,
    classification_result: ?ai_openapi.ClassificationTransformationResult,
    candidate_scores: []const AgenticCandidateScore,
    attempted_query_indices: []const bool,
) ![]const AgenticCandidateScore {
    var scores = try arena.dupe(AgenticCandidateScore, candidate_scores);
    const probe_indices = try topRemainingProbeCandidateIndices(arena, scores, retrieval_queries, attempted_query_indices);
    if (probe_indices.len == 0) return scores;

    for (probe_indices) |candidate_index| {
        const retrieval_query = retrieval_queries[candidate_index];
        if (!isProbeableRetrievalQuery(retrieval_query)) continue;
        const query_json = try encodeQueryValueForRetrievalQuery(
            alloc,
            runner,
            raw_queries[candidate_index],
            retrieval_query,
            &.{},
            classification_result,
            candidate_index,
            .initial,
        );
        defer alloc.free(query_json);

        var query_response = runner.runQuery(
            alloc,
            retrieval_query.table orelse return error.InvalidRetrievalAgentRequest,
            query_json,
        ) catch continue;
        defer query_response.deinit(alloc);

        var parsed_query = std.json.parseFromSlice(QueryResponses, arena, query_response.json, .{
            .ignore_unknown_fields = true,
            .allocate = .alloc_always,
        }) catch continue;
        defer parsed_query.deinit();

        const fallback_tree_root = if (retrieval_query.tree_search != null)
            try extractTreeFallbackRootKey(arena, query_json)
        else
            null;
        const query_hits = if (retrieval_query.tree_search != null)
            try extractTreeHits(arena, parsed_query.value, queryTextForProbe(classification_result, retrieval_query), fallback_tree_root)
        else
            extractHits(parsed_query.value);

        scores[candidate_index].probe_hits = @intCast(query_hits.len);
        if (query_hits.len > 0) {
            const probe_context = buildContextText(arena, query_hits[0..@min(query_hits.len, 3)]) catch "";
            scores[candidate_index].probe_relevance = queryCoverageScore(queryTextForProbe(classification_result, retrieval_query), probe_context);
        }
        scores[candidate_index].probe_top_score = if (query_hits.len > 0) query_hits[0]._score else 0.0;
    }

    return scores;
}

fn selectNextAgenticFallbackIndex(
    candidate_scores: []const AgenticCandidateScore,
    attempted_query_indices: []const bool,
) ?usize {
    var best_index: ?usize = null;
    for (candidate_scores) |candidate| {
        if (candidate.index >= attempted_query_indices.len) continue;
        if (attempted_query_indices[candidate.index]) continue;
        if (best_index == null or compareAgenticCandidatePriority(candidate, candidate_scores[best_index.?]) > 0) {
            best_index = candidate.index;
        }
    }
    return best_index;
}

fn hasUnattemptedAgenticCandidate(
    candidate_scores: []const AgenticCandidateScore,
    attempted_query_indices: []const bool,
) bool {
    return selectNextAgenticFallbackIndex(candidate_scores, attempted_query_indices) != null;
}

fn hasUnattemptedSemanticFallback(
    candidate_scores: []const AgenticCandidateScore,
    attempted_query_indices: []const bool,
) bool {
    for (candidate_scores) |candidate| {
        if (candidate.index >= attempted_query_indices.len) continue;
        if (attempted_query_indices[candidate.index]) continue;
        switch (candidate.strategy) {
            .semantic, .hybrid, .tree => return true,
            else => {},
        }
    }
    return false;
}

fn compareAgenticCandidatePriority(lhs: AgenticCandidateScore, rhs: AgenticCandidateScore) i32 {
    if ((lhs.probe_hits != null or rhs.probe_hits != null) and compareProbeCandidate(lhs, rhs) != 0) {
        return compareProbeCandidate(lhs, rhs);
    }
    if (lhs.score == rhs.score) return 0;
    return if (lhs.score > rhs.score) 1 else -1;
}

fn attemptPlannerScore(
    summary: AttemptEvaluationSummary,
    strategy: RetrievalStrategy,
) f32 {
    const hits_score = @min(0.45, @as(f32, @floatFromInt(@max(@as(i64, 0), summary.hit_count))) * 0.12);
    const relevance_score = (summary.context_relevance orelse 0.0) * 0.9;
    const top_score = @min(1.0, summary.top_score orelse 0.0) * 0.25;
    const length_score = lengthScoreFromLength(summary.context_length orelse 0) * 0.15;
    const tree_branch_score: f32 = switch (strategy) {
        .tree => blk: {
            const branch_relevance = summary.top_tree_branch_relevance orelse 0.0;
            const branch_nodes = @as(f32, @floatFromInt(summary.top_tree_branch_nodes orelse 0));
            const leaf_hits = @as(f32, @floatFromInt(summary.top_tree_branch_leaf_hits orelse 0));
            break :blk branch_relevance * 0.45 + @min(0.12, branch_nodes * 0.04) + @min(0.08, leaf_hits * 0.04);
        },
        else => 0.0,
    };
    const strategy_bonus: f32 = switch (strategy) {
        .semantic, .hybrid, .tree => 0.08,
        .bm25, .metadata, .graph => 0.0,
    };
    return hits_score + relevance_score + top_score + length_score + tree_branch_score + strategy_bonus;
}

fn candidatePlannerScore(candidate: AgenticCandidateScore) f32 {
    var score = @as(f32, @floatFromInt(candidate.score)) / 100.0;
    if (candidate.probe_hits) |probe_hits| score += @min(0.40, @as(f32, @floatFromInt(@max(@as(i64, 0), probe_hits))) * 0.12);
    if (candidate.probe_relevance) |probe_relevance| score += probe_relevance * 0.9;
    if (candidate.probe_top_score) |probe_top_score| score += @min(1.0, probe_top_score) * 0.2;
    return score;
}

fn bestRemainingCandidateScore(
    candidate_scores: []const AgenticCandidateScore,
    attempted_query_indices: []const bool,
) ?f32 {
    var best: ?f32 = null;
    for (candidate_scores) |candidate| {
        if (candidate.index >= attempted_query_indices.len) continue;
        if (attempted_query_indices[candidate.index]) continue;
        const score = candidatePlannerScore(candidate);
        if (best == null or score > best.?) best = score;
    }
    return best;
}

fn bestRemainingCandidate(
    candidate_scores: []const AgenticCandidateScore,
    attempted_query_indices: []const bool,
) ?AgenticCandidateScore {
    var best: ?AgenticCandidateScore = null;
    for (candidate_scores) |candidate| {
        if (candidate.index >= attempted_query_indices.len) continue;
        if (attempted_query_indices[candidate.index]) continue;
        if (best == null or compareAgenticCandidatePriority(candidate, best.?) > 0) {
            best = candidate;
        }
    }
    return best;
}

fn secondRemainingCandidate(
    candidate_scores: []const AgenticCandidateScore,
    attempted_query_indices: []const bool,
) ?AgenticCandidateScore {
    var best: ?AgenticCandidateScore = null;
    var second: ?AgenticCandidateScore = null;
    for (candidate_scores) |candidate| {
        if (candidate.index >= attempted_query_indices.len) continue;
        if (attempted_query_indices[candidate.index]) continue;
        if (best == null or compareAgenticCandidatePriority(candidate, best.?) > 0) {
            second = best;
            best = candidate;
        } else if (second == null or compareAgenticCandidatePriority(candidate, second.?) > 0) {
            second = candidate;
        }
    }
    return second;
}

fn plannerProgressScore(
    previous_summary: ?AttemptEvaluationSummary,
    current_summary: AttemptEvaluationSummary,
    strategy: RetrievalStrategy,
) ?f32 {
    const previous = previous_summary orelse return null;
    return attemptPlannerScore(current_summary, strategy) - attemptPlannerScore(previous, strategy);
}

fn shouldAcceptCurrentAttemptOverFallback(
    strategy: RetrievalStrategy,
    attempt_summary: AttemptEvaluationSummary,
    previous_attempt_summary: ?AttemptEvaluationSummary,
    best_fallback: AgenticCandidateScore,
) bool {
    const current_score = attemptPlannerScore(attempt_summary, strategy);
    const fallback_score = candidatePlannerScore(best_fallback);
    const progress = plannerProgressScore(previous_attempt_summary, attempt_summary, strategy) orelse 0.0;

    const current_hits = attempt_summary.hit_count;
    const current_relevance = attempt_summary.context_relevance orelse 0.0;
    const current_top = attempt_summary.top_score orelse 0.0;

    const fallback_hits = best_fallback.probe_hits orelse 0;
    const fallback_relevance = best_fallback.probe_relevance orelse 0.0;
    const fallback_top = best_fallback.probe_top_score orelse 0.0;
    const current_tree_relevance = attempt_summary.top_tree_branch_relevance orelse 0.0;

    if (progress >= 0.06 and current_score + 0.06 >= fallback_score) return true;

    if (current_hits >= fallback_hits - 1 and
        current_relevance >= fallback_relevance - 0.05 and
        current_top >= fallback_top - 0.08 and
        current_score + 0.15 >= fallback_score)
    {
        return true;
    }

    if (strategy == .semantic or strategy == .hybrid or strategy == .tree) {
        if (current_relevance >= fallback_relevance and current_score + 0.03 >= fallback_score) return true;
    }
    if (strategy == .tree and current_tree_relevance >= fallback_relevance - 0.03 and current_score + 0.05 >= fallback_score) return true;

    return false;
}

fn shouldExpandTreeBranch(
    attempt_summary: AttemptEvaluationSummary,
    previous_attempt_summary: ?AttemptEvaluationSummary,
    best_fallback: AgenticCandidateScore,
) bool {
    const branch_relevance = attempt_summary.top_tree_branch_relevance orelse return false;
    const branch_nodes = attempt_summary.top_tree_branch_nodes orelse return false;
    const leaf_hits = attempt_summary.top_tree_branch_leaf_hits orelse return false;
    if (leaf_hits > 0) return false;
    if (branch_nodes > 3) return false;

    const current_score = attemptPlannerScore(attempt_summary, .tree);
    const fallback_score = candidatePlannerScore(best_fallback);
    const progress = plannerProgressScore(previous_attempt_summary, attempt_summary, .tree) orelse 0.0;
    const fallback_relevance = best_fallback.probe_relevance orelse 0.0;

    if (branch_relevance < 0.12) return false;
    if (fallback_relevance > branch_relevance + 0.18 and fallback_score > current_score + 0.08) return false;
    return progress >= -0.02 and current_score + 0.10 >= fallback_score;
}

fn shouldPreferFallbackCandidate(
    strategy: RetrievalStrategy,
    attempt_summary: AttemptEvaluationSummary,
    previous_attempt_summary: ?AttemptEvaluationSummary,
    best_fallback: AgenticCandidateScore,
) bool {
    const current_score = attemptPlannerScore(attempt_summary, strategy);
    const fallback_score = candidatePlannerScore(best_fallback);
    const progress = plannerProgressScore(previous_attempt_summary, attempt_summary, strategy) orelse 0.0;

    const current_hits = attempt_summary.hit_count;
    const current_relevance = attempt_summary.context_relevance orelse 0.0;
    const current_top = attempt_summary.top_score orelse 0.0;

    const fallback_hits = best_fallback.probe_hits orelse 0;
    const fallback_relevance = best_fallback.probe_relevance orelse 0.0;
    const fallback_top = best_fallback.probe_top_score orelse 0.0;
    const current_tree_relevance = attempt_summary.top_tree_branch_relevance orelse 0.0;

    if (fallback_score > current_score + 0.10 and progress <= 0.02) return true;

    if (fallback_hits > current_hits + 1 and fallback_relevance > current_relevance + 0.08) return true;

    if (fallback_relevance > current_relevance + 0.12 and fallback_top >= current_top - 0.02) return true;
    if (strategy == .tree and fallback_relevance > current_tree_relevance + 0.15 and fallback_score > current_score + 0.06) return true;

    return false;
}

fn shouldClarifyBetweenCurrentAndFallback(
    strategy: RetrievalStrategy,
    attempt_summary: AttemptEvaluationSummary,
    previous_attempt_summary: ?AttemptEvaluationSummary,
    best_fallback: AgenticCandidateScore,
) bool {
    const current_score = attemptPlannerScore(attempt_summary, strategy);
    const fallback_score = candidatePlannerScore(best_fallback);
    const progress = plannerProgressScore(previous_attempt_summary, attempt_summary, strategy) orelse 0.0;

    const current_hits = attempt_summary.hit_count;
    const current_relevance = attempt_summary.context_relevance orelse 0.0;
    const current_top = attempt_summary.top_score orelse 0.0;

    const fallback_hits = best_fallback.probe_hits orelse 0;
    const fallback_relevance = best_fallback.probe_relevance orelse 0.0;
    const fallback_top = best_fallback.probe_top_score orelse 0.0;
    const current_tree_relevance = attempt_summary.top_tree_branch_relevance orelse 0.0;

    if (!std.math.approxEqAbs(f32, current_score, fallback_score, 0.08)) return false;
    if (@abs(current_hits - fallback_hits) > 1) return false;
    if (!std.math.approxEqAbs(f32, current_relevance, fallback_relevance, 0.08)) return false;
    if (!std.math.approxEqAbs(f32, current_top, fallback_top, 0.08)) return false;
    if (strategy == .tree and !std.math.approxEqAbs(f32, current_tree_relevance, fallback_relevance, 0.10)) return false;

    return progress >= -0.01;
}

fn shouldClarifyBetweenFallbackCandidates(
    strategy: RetrievalStrategy,
    attempt_summary: AttemptEvaluationSummary,
    previous_attempt_summary: ?AttemptEvaluationSummary,
    best_fallback: AgenticCandidateScore,
    second_fallback: AgenticCandidateScore,
) bool {
    const current_score = attemptPlannerScore(attempt_summary, strategy);
    const progress = plannerProgressScore(previous_attempt_summary, attempt_summary, strategy) orelse 0.0;

    const best_score = candidatePlannerScore(best_fallback);
    const second_score = candidatePlannerScore(second_fallback);

    if (best_score <= current_score + 0.05 or second_score <= current_score + 0.03) return false;
    if (best_fallback.strategy == second_fallback.strategy) return false;

    const best_relevance = best_fallback.probe_relevance orelse 0.0;
    const second_relevance = second_fallback.probe_relevance orelse best_relevance;
    const best_hits = best_fallback.probe_hits orelse 0;
    const second_hits = second_fallback.probe_hits orelse best_hits;

    if (!std.math.approxEqAbs(f32, best_score, second_score, 0.08)) return false;
    if (!std.math.approxEqAbs(f32, best_relevance, second_relevance, 0.08)) return false;
    if (@abs(best_hits - second_hits) > 1) return false;

    return progress >= -0.01;
}

fn decideAgenticPlannerAction(
    trigger: AgenticEvaluationTrigger,
    strategy: RetrievalStrategy,
    attempt_summary: AttemptEvaluationSummary,
    previous_attempt_summary: ?AttemptEvaluationSummary,
    candidate_scores: []const AgenticCandidateScore,
    attempted_query_indices: []const bool,
    can_refine: bool,
    can_expand_tree_branch: bool,
    can_clarify: bool,
) AgenticPlannerDecision {
    if (trigger == .none) return .accept_result;

    const current_score = attemptPlannerScore(attempt_summary, strategy);
    const best_fallback = bestRemainingCandidate(candidate_scores, attempted_query_indices) orelse return .accept_result;
    const second_fallback = secondRemainingCandidate(candidate_scores, attempted_query_indices);
    const best_fallback_score = candidatePlannerScore(best_fallback);
    const progress_score = plannerProgressScore(previous_attempt_summary, attempt_summary, strategy);

    if (trigger == .empty_result) {
        if (can_clarify and shouldClarifyAfterEvaluationFallback(trigger, attempt_summary, candidate_scores, attempted_query_indices)) {
            return .clarify;
        }
        return .switch_strategy;
    }

    if (can_expand_tree_branch and strategy == .tree and shouldExpandTreeBranch(attempt_summary, previous_attempt_summary, best_fallback)) {
        return .expand_branch;
    }

    if (can_refine and (trigger == .weak_result or trigger == .partial_result)) {
        if (current_score + 0.08 >= best_fallback_score) return .refine_query;
    }

    if (shouldAcceptCurrentAttemptOverFallback(strategy, attempt_summary, previous_attempt_summary, best_fallback)) {
        return .accept_result;
    }

    if (shouldPreferFallbackCandidate(strategy, attempt_summary, previous_attempt_summary, best_fallback)) {
        if (can_clarify and shouldClarifyAfterEvaluationFallback(trigger, attempt_summary, candidate_scores, attempted_query_indices)) {
            return .clarify;
        }
        return .switch_strategy;
    }

    if (can_clarify and shouldClarifyBetweenCurrentAndFallback(strategy, attempt_summary, previous_attempt_summary, best_fallback)) {
        return .clarify;
    }

    if (can_clarify) {
        if (second_fallback) |candidate| {
            if (shouldClarifyBetweenFallbackCandidates(strategy, attempt_summary, previous_attempt_summary, best_fallback, candidate)) {
                return .clarify;
            }
        }
    }

    if (progress_score) |progress| {
        if (progress <= 0.01 and best_fallback_score > current_score + 0.03) {
            if (can_clarify and shouldClarifyAfterEvaluationFallback(trigger, attempt_summary, candidate_scores, attempted_query_indices)) {
                return .clarify;
            }
            return .switch_strategy;
        }
    }

    if (trigger == .partial_result and current_score + 0.10 >= best_fallback_score) {
        return .accept_result;
    }

    if (can_clarify and shouldClarifyAfterEvaluationFallback(trigger, attempt_summary, candidate_scores, attempted_query_indices)) {
        return .clarify;
    }

    return if (best_fallback_score > current_score + 0.06) .switch_strategy else .accept_result;
}

fn shouldClarifyAfterEvaluationFallback(
    trigger: AgenticEvaluationTrigger,
    attempt_summary: AttemptEvaluationSummary,
    candidate_scores: []const AgenticCandidateScore,
    attempted_query_indices: []const bool,
) bool {
    var best: ?AgenticCandidateScore = null;
    var second: ?AgenticCandidateScore = null;

    for (candidate_scores) |candidate| {
        if (candidate.index >= attempted_query_indices.len) continue;
        if (attempted_query_indices[candidate.index]) continue;
        if (best == null or compareAgenticCandidatePriority(candidate, best.?) > 0) {
            second = best;
            best = candidate;
        } else if (second == null or compareAgenticCandidatePriority(candidate, second.?) > 0) {
            second = candidate;
        }
    }

    const lhs = best orelse return false;
    const rhs = second orelse return false;

    if (trigger == .partial_result and attempt_summary.hit_count > 0) {
        if (attempt_summary.context_relevance) |current_relevance| {
            if (lhs.probe_relevance) |best_relevance| {
                const second_relevance = rhs.probe_relevance orelse best_relevance;
                if (current_relevance >= best_relevance - 0.04 and
                    std.math.approxEqAbs(f32, best_relevance, second_relevance, 0.08))
                {
                    return true;
                }
            }
        }
    }

    if (lhs.probe_hits != null and rhs.probe_hits != null) {
        const lhs_hits = lhs.probe_hits.?;
        const rhs_hits = rhs.probe_hits.?;
        const lhs_relevance = lhs.probe_relevance orelse -1.0;
        const rhs_relevance = rhs.probe_relevance orelse -1.0;
        const lhs_top = lhs.probe_top_score orelse -1.0;
        const rhs_top = rhs.probe_top_score orelse -1.0;
        return @abs(lhs_hits - rhs_hits) <= 1 and
            std.math.approxEqAbs(f32, lhs_relevance, rhs_relevance, 0.08) and
            std.math.approxEqAbs(f32, lhs_top, rhs_top, 0.08);
    }

    return @abs(lhs.score - rhs.score) <= 4;
}

fn topProbeCandidateIndices(
    alloc: std.mem.Allocator,
    candidate_scores: []const AgenticCandidateScore,
    retrieval_queries: []const RetrievalQueryRequest,
) ![]const usize {
    var best_index: ?usize = null;
    var second_index: ?usize = null;

    for (candidate_scores, 0..) |candidate, i| {
        if (!isProbeableRetrievalQuery(retrieval_queries[i])) continue;
        if (best_index == null or candidate.score > candidate_scores[best_index.?].score) {
            second_index = best_index;
            best_index = i;
        } else if (second_index == null or candidate.score > candidate_scores[second_index.?].score) {
            second_index = i;
        }
    }

    const winner = best_index orelse return &.{};
    if (second_index == null) return try alloc.dupe(usize, &[_]usize{winner});
    return try alloc.dupe(usize, &[_]usize{ winner, second_index.? });
}

fn topRemainingProbeCandidateIndices(
    alloc: std.mem.Allocator,
    candidate_scores: []const AgenticCandidateScore,
    retrieval_queries: []const RetrievalQueryRequest,
    attempted_query_indices: []const bool,
) ![]const usize {
    var best_index: ?usize = null;
    var second_index: ?usize = null;

    for (candidate_scores, 0..) |candidate, i| {
        if (candidate.index >= attempted_query_indices.len) continue;
        if (attempted_query_indices[candidate.index]) continue;
        if (!isProbeableRetrievalQuery(retrieval_queries[i])) continue;
        if (best_index == null or candidate.score > candidate_scores[best_index.?].score) {
            second_index = best_index;
            best_index = i;
        } else if (second_index == null or candidate.score > candidate_scores[second_index.?].score) {
            second_index = i;
        }
    }

    const winner = best_index orelse return &.{};
    if (second_index == null) return try alloc.dupe(usize, &[_]usize{winner});
    return try alloc.dupe(usize, &[_]usize{ winner, second_index.? });
}

fn isProbeableRetrievalQuery(retrieval_query: RetrievalQueryRequest) bool {
    if (retrieval_query.table == null) return false;
    if (retrieval_query.tree_search) |tree_search| {
        if (tree_search.start_nodes) |start_nodes| {
            const trimmed = std.mem.trim(u8, start_nodes, " \t\r\n");
            if (trimmed.len == 0) return false;
            if (std.mem.eql(u8, trimmed, "$roots")) return true;
            return trimmed[0] != '$';
        }
    }
    return true;
}

fn selectProbeWinner(
    candidate_scores: []const AgenticCandidateScore,
    probe_indices: []const usize,
) ?usize {
    var best_index: ?usize = null;
    var second_index: ?usize = null;

    for (probe_indices) |index| {
        if (candidate_scores[index].probe_hits == null) continue;
        if (best_index == null or compareProbeCandidate(candidate_scores[index], candidate_scores[best_index.?]) > 0) {
            second_index = best_index;
            best_index = index;
        } else if (second_index == null or compareProbeCandidate(candidate_scores[index], candidate_scores[second_index.?]) > 0) {
            second_index = index;
        }
    }

    const winner = best_index orelse return null;
    if ((candidate_scores[winner].probe_hits orelse 0) == 0) return null;
    if (second_index == null) return winner;
    return if (compareProbeCandidate(candidate_scores[winner], candidate_scores[second_index.?]) > 0) winner else null;
}

fn compareProbeCandidate(lhs: AgenticCandidateScore, rhs: AgenticCandidateScore) i32 {
    const lhs_hits = lhs.probe_hits orelse -1;
    const rhs_hits = rhs.probe_hits orelse -1;
    if (lhs_hits != rhs_hits) return if (lhs_hits > rhs_hits) 1 else -1;

    const lhs_relevance = lhs.probe_relevance orelse -1.0;
    const rhs_relevance = rhs.probe_relevance orelse -1.0;
    if (!std.math.approxEqAbs(f32, lhs_relevance, rhs_relevance, 0.05)) {
        return if (lhs_relevance > rhs_relevance) 1 else -1;
    }

    const lhs_top = lhs.probe_top_score orelse -1.0;
    const rhs_top = rhs.probe_top_score orelse -1.0;
    if (std.math.approxEqAbs(f32, lhs_top, rhs_top, 0.05)) return 0;
    return if (lhs_top > rhs_top) 1 else -1;
}

fn queryTextForProbe(
    classification_result: ?ai_openapi.ClassificationTransformationResult,
    retrieval_query: RetrievalQueryRequest,
) []const u8 {
    if (classification_result) |classification| {
        if (classification.semantic_query.len > 0) return classification.semantic_query;
        if (classification.improved_query.len > 0) return classification.improved_query;
    }
    if (retrieval_query.semantic_search) |semantic_search| return semantic_search;
    if (retrieval_query.full_text_search) |full_text| {
        if (extractQueryString(full_text)) |query| return query;
    }
    if (retrieval_query.filter_query) |filter_query| {
        if (extractQueryString(filter_query)) |query| return query;
    }
    if (retrieval_query.tree_search) |tree_search| {
        if (tree_search.start_nodes) |start_nodes| return start_nodes;
    }
    return "";
}

fn extractQueryString(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => |text| text,
        .object => |object| switch (object.get("query") orelse .null) {
            .string => |text| text,
            else => null,
        },
        else => null,
    };
}

fn preferredAgenticQueryStrategy(request: RetrievalAgentRequest) ai_openapi.QueryStrategy {
    if (request.steps) |steps| {
        if (steps.classification) |classification| {
            if (classification.force_strategy) |strategy| return strategy;
        }
    }
    return inferClassificationStrategy(request.query);
}

fn decisionApproved(
    decisions: []const AgentDecision,
    question_id: []const u8,
) bool {
    for (decisions) |decision| {
        if (!std.mem.eql(u8, decision.question_id, question_id)) continue;
        if (decision.approved != null) return decision.approved.?;
        if (decision.answer) |answer| return isAffirmativeAnswer(answer);
        return false;
    }
    return false;
}

fn isAffirmativeAnswer(answer: std.json.Value) bool {
    return switch (answer) {
        .bool => |value| value,
        .string => |value| {
            const trimmed = std.mem.trim(u8, value, " \t\r\n");
            return std.ascii.eqlIgnoreCase(trimmed, "yes") or
                std.ascii.eqlIgnoreCase(trimmed, "true") or
                std.ascii.eqlIgnoreCase(trimmed, "broaden");
        },
        else => false,
    };
}

fn allQueryIndices(alloc: std.mem.Allocator, count: usize) ![]const usize {
    const out = try alloc.alloc(usize, count);
    for (out, 0..) |*slot, i| slot.* = i;
    return out;
}

fn buildAgenticCandidateScores(
    alloc: std.mem.Allocator,
    query: []const u8,
    retrieval_queries: []const RetrievalQueryRequest,
    preferred_strategy: ai_openapi.QueryStrategy,
) ![]const AgenticCandidateScore {
    const out = try alloc.alloc(AgenticCandidateScore, retrieval_queries.len);
    for (retrieval_queries, out, 0..) |retrieval_query, *slot, i| {
        slot.* = .{
            .index = i,
            .strategy = detectStrategy(retrieval_query),
            .score = scoreAgenticQueryCandidate(query, retrieval_query, preferred_strategy),
        };
    }
    return out;
}

fn resolveAgenticDecisionSelection(
    decisions: []const AgentDecision,
    query_count: usize,
) !?usize {
    for (decisions) |decision| {
        if (!std.mem.eql(u8, decision.question_id, "select_query")) continue;
        if (decision.answer) |answer| {
            switch (answer) {
                .integer => |value| {
                    if (value < 0) return error.InvalidRetrievalAgentRequest;
                    const index: usize = @intCast(value);
                    if (index >= query_count) return error.InvalidRetrievalAgentRequest;
                    return index;
                },
                .string => |value| {
                    const trimmed = std.mem.trim(u8, value, " \t\r\n");
                    if (trimmed.len == 0) return error.InvalidRetrievalAgentRequest;
                    if (std.mem.eql(u8, trimmed, "first")) return 0;
                    if (std.mem.eql(u8, trimmed, "second")) {
                        if (query_count < 2) return error.InvalidRetrievalAgentRequest;
                        return 1;
                    }
                    const parsed_int = std.fmt.parseInt(usize, trimmed, 10) catch null;
                    if (parsed_int) |index| {
                        if (index >= query_count) return error.InvalidRetrievalAgentRequest;
                        return index;
                    }
                    for ([_][]const u8{ "semantic", "bm25", "metadata", "tree", "graph", "hybrid" }, 0..) |label, i| {
                        if (std.ascii.eqlIgnoreCase(trimmed, label) and i < query_count) return i;
                    }
                    return error.InvalidRetrievalAgentRequest;
                },
                else => return error.InvalidRetrievalAgentRequest,
            }
        }
        if (decision.approved != null and decision.approved.?) return 0;
        return error.InvalidRetrievalAgentRequest;
    }
    return null;
}

fn buildAgenticSelectionQuestion(
    alloc: std.mem.Allocator,
    query: []const u8,
    retrieval_queries: []const RetrievalQueryRequest,
) !AgentQuestion {
    const options = try alloc.alloc([]const u8, retrieval_queries.len);
    for (retrieval_queries, options, 0..) |retrieval_query, *slot, i| {
        slot.* = try std.fmt.allocPrint(alloc, "{d}: {s}", .{ i, describeRetrievalQuery(retrieval_query) });
    }
    const affects = try alloc.dupe([]const u8, &[_][]const u8{ "retrieval_strategy", "retrieval_hits" });
    return .{
        .id = "select_query",
        .kind = .single_choice,
        .question = try std.fmt.allocPrint(alloc, "Which retrieval approach should be used for: {s}?", .{query}),
        .reason = "Multiple retrieval strategies looked plausible and the bounded agent needs a user choice.",
        .options = options,
        .default_answer = options[0],
        .affects = affects,
    };
}

fn buildBroadenSearchQuestion(
    alloc: std.mem.Allocator,
    query: []const u8,
) !AgentQuestion {
    const options = try alloc.dupe([]const u8, &[_][]const u8{ "yes", "no" });
    const affects = try alloc.dupe([]const u8, &[_][]const u8{ "retrieval_strategy", "retrieval_hits" });
    return .{
        .id = "broaden_search",
        .kind = .confirm,
        .question = try std.fmt.allocPrint(alloc, "No strong results were found for '{s}'. Should I broaden retrieval to the other available search strategies?", .{query}),
        .reason = "The first bounded retrieval pass returned no hits.",
        .options = options,
        .default_answer = "yes",
        .affects = affects,
    };
}

fn buildAgenticSelectionQuestionForIndices(
    alloc: std.mem.Allocator,
    query: []const u8,
    retrieval_queries: []const RetrievalQueryRequest,
    candidate_indices: []const usize,
) !AgentQuestion {
    const options = try alloc.alloc([]const u8, candidate_indices.len);
    for (candidate_indices, options) |candidate_index, *slot| {
        slot.* = try std.fmt.allocPrint(alloc, "{d}: {s}", .{ candidate_index, describeRetrievalQuery(retrieval_queries[candidate_index]) });
    }
    const affects = try alloc.dupe([]const u8, &[_][]const u8{ "retrieval_strategy", "retrieval_hits" });
    return .{
        .id = "select_query",
        .kind = .single_choice,
        .question = try std.fmt.allocPrint(alloc, "Which retrieval approach should be used for: {s}?", .{query}),
        .reason = "Refinement still left multiple plausible bounded-agent fallback strategies.",
        .options = options,
        .default_answer = options[0],
        .affects = affects,
    };
}

fn describeRetrievalQuery(retrieval_query: RetrievalQueryRequest) []const u8 {
    return switch (detectStrategy(retrieval_query)) {
        .semantic => "semantic",
        .bm25 => "bm25",
        .metadata => "metadata",
        .tree => "tree",
        .graph => "graph",
        .hybrid => "hybrid",
    };
}

fn collectRemainingCandidateIndices(
    alloc: std.mem.Allocator,
    candidate_scores: []const AgenticCandidateScore,
    attempted_query_indices: []const bool,
) ![]const usize {
    var out = std.ArrayListUnmanaged(usize).empty;
    errdefer out.deinit(alloc);
    for (candidate_scores) |candidate| {
        if (candidate.index >= attempted_query_indices.len) continue;
        if (attempted_query_indices[candidate.index]) continue;
        try out.append(alloc, candidate.index);
    }
    return try out.toOwnedSlice(alloc);
}

fn scoreAgenticQueryCandidate(
    query: []const u8,
    retrieval_query: RetrievalQueryRequest,
    preferred_strategy: ai_openapi.QueryStrategy,
) i32 {
    const strategy = detectStrategy(retrieval_query);
    var score: i32 = switch (strategy) {
        .hybrid => 50,
        .semantic => 40,
        .bm25 => 30,
        .tree => 25,
        .graph => 20,
        .metadata => 10,
    };

    if (containsAnyIgnoreCase(query, &.{ "how", "why", "what", "architecture", "consensus", "work" })) {
        if (strategy == .semantic or strategy == .hybrid or strategy == .tree) score += 20;
    }
    if (containsAnyIgnoreCase(query, &.{ "list", "exact", "field", "status", "metadata" })) {
        if (strategy == .metadata or strategy == .bm25) score += 15;
    }
    if (containsAnyIgnoreCase(query, &.{ "graph", "relationship", "path", "connected" })) {
        if (strategy == .graph or strategy == .tree) score += 20;
    }

    score += switch (preferred_strategy) {
        .simple => switch (strategy) {
            .bm25 => 20,
            .metadata => 12,
            .semantic => 8,
            .hybrid => 6,
            .tree, .graph => 0,
        },
        .step_back => switch (strategy) {
            .tree => 28,
            .hybrid => 24,
            .semantic => 18,
            .graph => 10,
            .bm25, .metadata => 0,
        },
        .hyde => switch (strategy) {
            .semantic => 28,
            .hybrid => 22,
            .tree => 8,
            .bm25, .metadata, .graph => 0,
        },
        .decompose => 0,
    };
    return score;
}

fn detectSelectedAgenticStrategy(
    retrieval_queries: []const RetrievalQueryRequest,
    selected_query_indices: []const usize,
) RetrievalStrategy {
    if (selected_query_indices.len == 0) return .hybrid;
    const first = detectStrategy(retrieval_queries[selected_query_indices[0]]);
    for (selected_query_indices[1..]) |index| {
        if (detectStrategy(retrieval_queries[index]) != first) return .hybrid;
    }
    return first;
}

fn containsAnyIgnoreCase(haystack: []const u8, needles: []const []const u8) bool {
    for (needles) |needle| {
        if (std.ascii.indexOfIgnoreCase(haystack, needle) != null) return true;
    }
    return false;
}

fn containsIndex(items: []const usize, needle: usize) bool {
    for (items) |item| {
        if (item == needle) return true;
    }
    return false;
}

fn buildFollowupQuestions(
    alloc: std.mem.Allocator,
    query: []const u8,
    generated_content: ?[]const u8,
    cfg: ParsedFollowupConfig,
) ![]const []const u8 {
    _ = generated_content;
    const templates = [_][]const u8{
        "What configuration details matter most for this topic?",
        "Which related Antfly features should I review next?",
        "How would this change in a multi-node deployment?",
        "What are the main operational tradeoffs here?",
    };
    const count = @min(cfg.count, templates.len);
    const out = try alloc.alloc([]const u8, count);
    for (out, 0..) |*slot, i| {
        slot.* = if (i == 0)
            try std.fmt.allocPrint(alloc, "What else should I know about: {s}?", .{query})
        else
            try alloc.dupe(u8, templates[i]);
    }
    return out;
}

fn isQuestionLike(query: []const u8) bool {
    if (std.mem.indexOfScalar(u8, query, '?') != null) return true;
    for ([_][]const u8{ "what", "how", "why", "when", "where", "who", "which" }) |prefix| {
        if (std.ascii.startsWithIgnoreCase(query, prefix)) return true;
    }
    return false;
}

fn encodeSse(
    alloc: std.mem.Allocator,
    result: RetrievalAgentResult,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    const steps = result.steps orelse &.{};
    if (result.classification) |classification| {
        try appendSseEventValue(alloc, &out, "classification", classification);
        if (classification.reasoning) |reasoning| {
            try appendSseTextChunks(alloc, &out, "reasoning", reasoning, null, "classification");
        }
        if (classification.sub_questions) |sub_questions| {
            for (sub_questions, 0..) |sub_question, i| {
                try appendSseEventValue(alloc, &out, "step_progress", .{
                    .name = "classification",
                    .phase = "decompose",
                    .index = i,
                    .sub_question = sub_question,
                });
            }
        }
    }
    for (steps, 0..) |step, i| {
        const step_id = try std.fmt.allocPrint(alloc, "step_{d}", .{i});
        defer alloc.free(step_id);

        try appendSseEventValue(alloc, &out, "step_started", .{
            .id = step_id,
            .kind = step.kind,
            .name = step.name,
            .action = step.action,
        });

        if (std.mem.eql(u8, step.name, "pipeline")) {
            if (result.strategy_used == .tree or hasTreeHits(result.hits)) {
                const tree_depth = maxTreeHitDepth(result.hits);
                try appendSseEventValue(alloc, &out, "step_progress", .{
                    .id = step_id,
                    .kind = step.kind,
                    .name = step.name,
                    .phase = "tree_search",
                    .depth = tree_depth,
                    .num_nodes = result.hits.len,
                    .collected = result.hits.len,
                    .complete = true,
                    .sufficient = result.hits.len > 0,
                    .action = step.action,
                    .details = step.details,
                });
            }
            for (result.hits) |hit| {
                try appendSseEventValue(alloc, &out, "hit", hit);
            }
        } else if (std.mem.eql(u8, step.name, "select_strategy")) {
            if (step.details) |details| {
                if (details == .object) {
                    if (details.object.get("selection_source")) |selection_source| {
                        if (selection_source == .string and std.mem.eql(u8, selection_source.string, "probe")) {
                            try appendSseEventValue(alloc, &out, "step_progress", .{
                                .id = step_id,
                                .kind = step.kind,
                                .name = step.name,
                                .phase = "probe",
                                .action = step.action,
                                .details = step.details,
                            });
                        }
                    }
                }
            }
            try appendSseTextChunks(alloc, &out, "reasoning", step.action, step_id, step.name);
            try appendSseEventValue(alloc, &out, "step_progress", .{
                .id = step_id,
                .kind = step.kind,
                .name = step.name,
                .phase = "select_strategy",
                .action = step.action,
                .details = step.details,
            });
        } else if (std.mem.eql(u8, step.name, "refine_query")) {
            try appendSseTextChunks(alloc, &out, "reasoning", step.action, step_id, step.name);
            try appendSseEventValue(alloc, &out, "step_progress", .{
                .id = step_id,
                .kind = step.kind,
                .name = step.name,
                .phase = stepProgressPhase(step.details, "refine_query"),
                .action = step.action,
                .details = step.details,
            });
        } else if (std.mem.eql(u8, step.name, "evaluate")) {
            try appendSseTextChunks(alloc, &out, "reasoning", step.action, step_id, step.name);
            try appendSseEventValue(alloc, &out, "step_progress", .{
                .id = step_id,
                .kind = step.kind,
                .name = step.name,
                .phase = "evaluate",
                .action = step.action,
                .details = step.details,
            });
            if (step.details) |details| {
                if (details == .object) {
                    if (details.object.get("current_vs_fallback_ambiguous")) |ambiguous| {
                        if (ambiguous == .bool and ambiguous.bool) {
                            try appendSseTextChunks(
                                alloc,
                                &out,
                                "reasoning",
                                "evaluation found the current result and the best fallback to be effectively tied",
                                step_id,
                                step.name,
                            );
                            try appendSseEventValue(alloc, &out, "step_progress", .{
                                .id = step_id,
                                .kind = step.kind,
                                .name = step.name,
                                .phase = "current_vs_fallback_ambiguity",
                                .action = step.action,
                                .details = step.details,
                            });
                        }
                    }
                    if (details.object.get("fallback_consensus_ambiguous")) |ambiguous| {
                        if (ambiguous == .bool and ambiguous.bool) {
                            try appendSseTextChunks(
                                alloc,
                                &out,
                                "reasoning",
                                "evaluation found multiple stronger fallback strategies that remain effectively tied",
                                step_id,
                                step.name,
                            );
                            try appendSseEventValue(alloc, &out, "step_progress", .{
                                .id = step_id,
                                .kind = step.kind,
                                .name = step.name,
                                .phase = "fallback_consensus_ambiguity",
                                .action = step.action,
                                .details = step.details,
                            });
                        }
                    }
                }
            }
        } else if (std.mem.eql(u8, step.name, "agentic")) {
            try appendSseEventValue(alloc, &out, "tool_mode", .{
                .mode = "structured_output",
            });
            try appendSseTextChunks(alloc, &out, "reasoning", step.action, step_id, step.name);
        } else if (step.kind == .tool_call) {
            if (step.details) |details| {
                if (details == .object) {
                    if (details.object.get("strategy")) |strategy| {
                        if (strategy == .string and std.mem.eql(u8, strategy.string, "tree")) {
                            const tree_depth = maxTreeHitDepth(result.hits);
                            try appendSseEventValue(alloc, &out, "step_progress", .{
                                .id = step_id,
                                .kind = step.kind,
                                .name = step.name,
                                .phase = "tree_search",
                                .depth = tree_depth,
                                .num_nodes = result.hits.len,
                                .collected = result.hits.len,
                                .complete = true,
                                .sufficient = result.hits.len > 0,
                                .action = step.action,
                                .details = step.details,
                            });
                        }
                    }
                }
            }
            try appendSseEventValue(alloc, &out, "step_progress", .{
                .id = step_id,
                .kind = step.kind,
                .name = step.name,
                .phase = "tool_call",
                .action = step.action,
                .details = step.details,
            });
        } else if (std.mem.eql(u8, step.name, "clarification")) {
            try appendSseTextChunks(alloc, &out, "reasoning", step.action, step_id, step.name);
            try appendSseEventValue(alloc, &out, "step_progress", .{
                .id = step_id,
                .kind = step.kind,
                .name = step.name,
                .phase = "clarification",
                .action = step.action,
                .details = step.details,
                .questions = result.questions,
            });
        } else if (std.mem.eql(u8, step.name, "generation")) {
            if (result.generation) |generation| {
                try appendSseTextChunks(alloc, &out, "generation", generation, step_id, step.name);
            }
        }

        try appendSseEventValue(alloc, &out, "step_completed", .{
            .id = step_id,
            .kind = step.kind,
            .name = step.name,
            .action = step.action,
            .status = step.status,
            .details = step.details,
        });
    }

    if (result.followup_questions) |followups| {
        for (followups) |followup| {
            try appendSseEventValue(alloc, &out, "followup", followup);
        }
    }

    if (result.eval_result) |eval_result| {
        try appendSseEventValue(alloc, &out, "eval", eval_result);
    }

    try appendSseEventValue(alloc, &out, "done", result);
    return try out.toOwnedSlice(alloc);
}

fn maxTreeHitDepth(hits: []const QueryHit) i64 {
    var max_depth: i64 = 0;
    for (hits) |hit| {
        const depth = treeMetaInteger(hit, "depth") orelse continue;
        if (depth > max_depth) max_depth = depth;
    }
    return max_depth;
}

fn hasTreeHits(hits: []const QueryHit) bool {
    for (hits) |hit| {
        if (treeMetaString(hit, "root") != null or
            treeMetaString(hit, "branch_path_text") != null or
            treeMetaInteger(hit, "depth") != null) return true;
    }
    return false;
}

fn stepProgressPhase(
    details: ?std.json.Value,
    default_phase: []const u8,
) []const u8 {
    const value = details orelse return default_phase;
    if (value != .object) return default_phase;
    if (value.object.get("phase")) |phase| {
        if (phase == .string) return phase.string;
    }
    return default_phase;
}

fn appendSseTextChunks(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    event_name: []const u8,
    text: []const u8,
    _: ?[]const u8,
    _: ?[]const u8,
) !void {
    if (text.len == 0) {
        try appendSseEventValue(alloc, out, event_name, text);
        return;
    }

    const chunk_len: usize = 80;
    var start: usize = 0;
    while (start < text.len) {
        const end = @min(start + chunk_len, text.len);
        try appendSseEventValue(alloc, out, event_name, text[start..end]);
        start = end;
    }
}

fn encodeSseError(
    alloc: std.mem.Allocator,
    message: []const u8,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try appendSseEventValue(alloc, &out, "error", .{ .@"error" = message });
    return try out.toOwnedSlice(alloc);
}

fn appendSseEventValue(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    event_name: []const u8,
    value: anytype,
) !void {
    const encoded = try std.json.Stringify.valueAlloc(alloc, value, .{});
    defer alloc.free(encoded);

    try out.appendSlice(alloc, "event: ");
    try out.appendSlice(alloc, event_name);
    try out.appendSlice(alloc, "\ndata: ");
    try out.appendSlice(alloc, encoded);
    try out.appendSlice(alloc, "\n\n");
}

fn encodeQueryValueForRetrievalQuery(
    alloc: std.mem.Allocator,
    runner: QueryRunner,
    value: std.json.Value,
    retrieval_query: RetrievalQueryRequest,
    previous_query_hits: []const QueryHit,
    classification_result: ?ai_openapi.ClassificationTransformationResult,
    retrieval_query_index: usize,
    refinement_pass: QueryRefinementPass,
) ![]u8 {
    const encoded = try std.json.Stringify.valueAlloc(alloc, value, .{});
    defer alloc.free(encoded);

    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var parsed = std.json.parseFromSlice(QueryRequest, arena, encoded, .{
        .ignore_unknown_fields = true,
    }) catch return error.InvalidRetrievalAgentRequest;
    defer parsed.deinit();

    var query_request = parsed.value;
    applyClassificationRefinement(&query_request, classification_result, retrieval_query_index, refinement_pass);
    if (retrieval_query.tree_search) |tree_search| {
        if (query_request.graph_searches != null) return error.UnsupportedRetrievalAgentRequest;
        query_request.graph_searches = try buildTreeGraphSearches(
            arena,
            runner,
            retrieval_query.table orelse return error.InvalidRetrievalAgentRequest,
            query_request,
            tree_search,
            previous_query_hits,
            retrieval_query.limit,
        );
    }

    return try std.json.Stringify.valueAlloc(alloc, query_request, .{});
}

fn applyClassificationRefinement(
    query_request: *QueryRequest,
    classification_result: ?ai_openapi.ClassificationTransformationResult,
    retrieval_query_index: usize,
    refinement_pass: QueryRefinementPass,
) void {
    const classification = classification_result orelse return;
    const refined_text = selectRefinedQueryText(classification, retrieval_query_index, refinement_pass) orelse return;

    if (query_request.semantic_search != null) {
        query_request.semantic_search = refined_text;
    }
    if (refinement_pass == .evaluation) {
        if (query_request.full_text_search) |*full_text| {
            if (full_text.* == .object) {
                if (full_text.object.getPtr("query")) |query_value| {
                    query_value.* = .{ .string = refined_text };
                }
            }
        }
    }
}

fn selectRefinedQueryText(
    classification: ai_openapi.ClassificationTransformationResult,
    retrieval_query_index: usize,
    refinement_pass: QueryRefinementPass,
) ?[]const u8 {
    const semantic_query = if (classification.semantic_query.len > 0)
        classification.semantic_query
    else
        classification.improved_query;
    return switch (classification.strategy) {
        .decompose => if (classification.sub_questions) |sub_questions|
            sub_questions[@min(retrieval_query_index, sub_questions.len - 1)]
        else
            classification.improved_query,
        .step_back => if (refinement_pass == .initial and retrieval_query_index == 0)
            classification.step_back_query orelse classification.improved_query
        else if (refinement_pass == .evaluation)
            selectEvaluationQueryText(classification, semantic_query)
        else
            semantic_query,
        .hyde => if (refinement_pass == .evaluation)
            selectEvaluationQueryText(classification, semantic_query)
        else
            semantic_query,
        .simple => if (refinement_pass == .evaluation)
            selectEvaluationQueryText(classification, semantic_query)
        else
            semantic_query,
    };
}

fn selectEvaluationQueryText(
    classification: ai_openapi.ClassificationTransformationResult,
    current_refined: []const u8,
) ?[]const u8 {
    if (classification.multi_phrases) |multi_phrases| {
        for (multi_phrases) |phrase| {
            if (phrase.len == 0) continue;
            if (!std.mem.eql(u8, phrase, current_refined)) return phrase;
        }
    }
    if (!std.mem.eql(u8, classification.improved_query, current_refined)) return classification.improved_query;
    return null;
}

fn initialRefinedQueryText(
    classification_result: ?ai_openapi.ClassificationTransformationResult,
    retrieval_query: RetrievalQueryRequest,
    retrieval_query_index: usize,
) ?[]const u8 {
    _ = retrieval_query.semantic_search orelse return null;
    const classification = classification_result orelse return null;
    const refined = selectRefinedQueryText(classification, retrieval_query_index, .initial) orelse return null;
    if (std.mem.eql(u8, retrieval_query.semantic_search.?, refined)) return null;
    return refined;
}

fn currentRetrievalQueryText(
    retrieval_query: RetrievalQueryRequest,
) ?[]const u8 {
    if (retrieval_query.semantic_search) |semantic_search| return semantic_search;
    if (retrieval_query.full_text_search) |full_text| {
        if (extractQueryString(full_text)) |query| return query;
    }
    if (retrieval_query.filter_query) |filter_query| {
        if (extractQueryString(filter_query)) |query| return query;
    }
    return null;
}

fn containsUsedQueryText(
    used_queries: []const []const u8,
    candidate: []const u8,
) bool {
    for (used_queries) |used_query| {
        if (std.mem.eql(u8, used_query, candidate)) return true;
    }
    return false;
}

fn nextEvaluationRefinedQueryText(
    classification_result: ?ai_openapi.ClassificationTransformationResult,
    retrieval_query: RetrievalQueryRequest,
    retrieval_query_index: usize,
    used_queries: []const []const u8,
) ?[]const u8 {
    const classification = classification_result orelse return null;
    if (classification.multi_phrases) |multi_phrases| {
        for (multi_phrases) |phrase| {
            if (phrase.len == 0) continue;
            if (!containsUsedQueryText(used_queries, phrase)) return phrase;
        }
    }

    if (selectRefinedQueryText(classification, retrieval_query_index, .evaluation)) |refined| {
        if (!containsUsedQueryText(used_queries, refined)) return refined;
    }

    const semantic_query = if (classification.semantic_query.len > 0)
        classification.semantic_query
    else
        classification.improved_query;
    if (!containsUsedQueryText(used_queries, semantic_query)) return semantic_query;
    if (!containsUsedQueryText(used_queries, classification.improved_query)) return classification.improved_query;

    if (currentRetrievalQueryText(retrieval_query)) |current_query| {
        if (!containsUsedQueryText(used_queries, current_query)) return current_query;
    }
    return null;
}

fn shouldRunStepBackFollowup(
    agentic_mode: bool,
    max_internal_iterations: i64,
    tool_calls_made: i64,
    classification_result: ?ai_openapi.ClassificationTransformationResult,
    retrieval_query: RetrievalQueryRequest,
) bool {
    if (!agentic_mode) return false;
    const classification = classification_result orelse return false;
    if (classification.strategy != .step_back) return false;
    if (tool_calls_made >= max_internal_iterations) return false;
    return retrieval_query.semantic_search != null;
}

fn buildTreeGraphSearches(
    alloc: std.mem.Allocator,
    runner: QueryRunner,
    table_name: []const u8,
    query_request: QueryRequest,
    tree_search: TreeSearchConfig,
    previous_query_hits: []const QueryHit,
    query_limit: ?i64,
) !std.json.ArrayHashMap(indexes_openapi.GraphQuery) {
    var graph_searches = std.json.ArrayHashMap(indexes_openapi.GraphQuery){};
    errdefer graph_searches.deinit(alloc);

    const start_nodes = try buildTreeStartNodes(alloc, runner, table_name, query_request, tree_search, previous_query_hits);
    const max_depth = tree_search.max_depth orelse 5;
    const beam_width = tree_search.beam_width orelse 3;
    const max_results = if (query_limit) |limit|
        @max(limit, @as(i64, 1))
    else
        @max(@as(i64, 1), max_depth * beam_width);

    try graph_searches.map.put(alloc, "tree_search", .{
        .type = .traverse,
        .index_name = tree_search.index,
        .start_nodes = start_nodes,
        .params = .{
            .direction = .out,
            .max_depth = max_depth,
            .max_results = max_results,
            .deduplicate_nodes = true,
        },
        .include_documents = true,
    });
    return graph_searches;
}

fn buildTreeStartNodes(
    alloc: std.mem.Allocator,
    runner: QueryRunner,
    table_name: []const u8,
    query_request: QueryRequest,
    tree_search: TreeSearchConfig,
    previous_query_hits: []const QueryHit,
) !indexes_openapi.GraphNodeSelector {
    if (tree_search.start_nodes) |start_nodes| {
        const trimmed = std.mem.trim(u8, start_nodes, " \t\r\n");
        if (trimmed.len == 0) return error.InvalidRetrievalAgentRequest;
        if (std.mem.eql(u8, trimmed, "$roots")) {
            return .{ .keys = try discoverTreeRootKeys(alloc, runner, table_name, tree_search.index) };
        }
        if (trimmed[0] == '$') {
            if (hasInlineTreeSeedSearch(query_request)) {
                return .{ .result_ref = "$tree_search" };
            }
            return .{ .keys = try buildTreeStartKeysFromHits(alloc, previous_query_hits) };
        }
        return .{ .keys = try buildTreeStartKeysFromCsv(alloc, trimmed) };
    }

    if (hasInlineTreeSeedSearch(query_request)) {
        return .{ .result_ref = "$tree_search" };
    }
    return .{ .keys = try buildTreeStartKeysFromHits(alloc, previous_query_hits) };
}

fn hasInlineTreeSeedSearch(query_request: QueryRequest) bool {
    return query_request.full_text_search != null or
        query_request.semantic_search != null or
        query_request.embeddings != null or
        query_request.filter_query != null or
        query_request.exclusion_query != null;
}

fn buildTreeStartKeysFromHits(
    alloc: std.mem.Allocator,
    hits: []const QueryHit,
) ![]const []const u8 {
    if (hits.len == 0) return error.InvalidRetrievalAgentRequest;
    const keys = try alloc.alloc([]const u8, hits.len);
    for (hits, 0..) |hit, i| keys[i] = hit._id;
    return keys;
}

fn buildTreeStartKeysFromCsv(
    alloc: std.mem.Allocator,
    csv: []const u8,
) ![]const []const u8 {
    var count: usize = 1;
    for (csv) |ch| {
        if (ch == ',') count += 1;
    }

    const keys = try alloc.alloc([]const u8, count);
    var it = std.mem.splitScalar(u8, csv, ',');
    var idx: usize = 0;
    while (it.next()) |part| {
        const trimmed = std.mem.trim(u8, part, " \t\r\n");
        if (trimmed.len == 0) return error.InvalidRetrievalAgentRequest;
        keys[idx] = trimmed;
        idx += 1;
    }
    return keys[0..idx];
}

fn discoverTreeRootKeys(
    alloc: std.mem.Allocator,
    runner: QueryRunner,
    table_name: []const u8,
    index_name: []const u8,
) ![]const []const u8 {
    const all_keys = try runner.scanKeys(alloc, table_name);
    defer {
        for (all_keys) |key| alloc.free(key);
        alloc.free(all_keys);
    }
    var roots = std.ArrayListUnmanaged([]const u8).empty;
    defer roots.deinit(alloc);

    for (all_keys) |key| {
        if (!try treeNodeHasIncomingEdges(alloc, runner, table_name, index_name, key)) {
            try roots.append(alloc, try alloc.dupe(u8, key));
        }
    }

    // During fresh tree-index bring-up, incoming-edge probes can briefly lag the
    // document scan even after the write path returns. If root detection is
    // temporarily ambiguous, seed traversal from every visible document instead
    // of collapsing "$roots" into an empty search.
    if (roots.items.len == 0 and all_keys.len > 0) {
        for (all_keys) |key| {
            try roots.append(alloc, try alloc.dupe(u8, key));
        }
    }

    return try roots.toOwnedSlice(alloc);
}

fn treeNodeHasIncomingEdges(
    alloc: std.mem.Allocator,
    runner: QueryRunner,
    table_name: []const u8,
    index_name: []const u8,
    key: []const u8,
) !bool {
    var graph_searches = std.json.ArrayHashMap(indexes_openapi.GraphQuery){};
    defer graph_searches.deinit(alloc);
    try graph_searches.map.put(alloc, "incoming", .{
        .type = .neighbors,
        .index_name = index_name,
        .start_nodes = .{ .keys = @constCast((&[_][]const u8{key})[0..]) },
        .params = .{
            .direction = .in,
            .max_results = 1,
        },
    });

    const body = try std.json.Stringify.valueAlloc(alloc, QueryRequest{
        .graph_searches = graph_searches,
        .limit = 1,
    }, .{});
    defer alloc.free(body);

    var response = try runner.runQuery(alloc, table_name, body);
    defer response.deinit(alloc);

    var parsed = std.json.parseFromSlice(QueryResponses, alloc, response.json, .{}) catch {
        return error.InvalidRetrievalAgentRequest;
    };
    defer parsed.deinit();
    return graphResponsesHaveHits(parsed.value);
}

fn graphResponsesHaveHits(responses: QueryResponses) bool {
    const query_responses = responses.responses orelse return false;
    for (query_responses) |response| {
        const graph_results = response.graph_results orelse continue;
        var it = graph_results.map.iterator();
        while (it.next()) |entry| {
            const value = entry.value_ptr.*;
            if (value.total > 0) return true;
            if (value.nodes) |nodes| if (nodes.len > 0) return true;
            if (value.paths) |paths| if (paths.len > 0) return true;
            if (value.matches) |matches| if (matches.len > 0) return true;
        }
    }
    return false;
}

fn extractHits(responses: QueryResponses) []const QueryHit {
    const query_responses = responses.responses orelse return &.{};
    for (query_responses) |response| {
        if (response.hits) |hits| {
            if (hits.hits) |emitted| return emitted;
        }
    }
    return &.{};
}

fn extractTreeHits(
    alloc: std.mem.Allocator,
    responses: QueryResponses,
    query: []const u8,
    fallback_root_key: ?[]const u8,
) ![]const QueryHit {
    const query_responses = responses.responses orelse return &.{};
    var hits = std.ArrayListUnmanaged(QueryHit).empty;
    defer hits.deinit(alloc);

    for (query_responses) |response| {
        const graph_results = response.graph_results orelse continue;
        var it = graph_results.map.iterator();
        while (it.next()) |entry| {
            const graph_result = entry.value_ptr.*;
            const nodes = entry.value_ptr.nodes orelse continue;
            for (nodes) |node| {
                const source = if (node.document) |document|
                    try annotateTreeDocument(alloc, document, entry.key_ptr.*, node, graph_result.paths orelse &.{}, fallback_root_key)
                else
                    null;
                try hits.append(alloc, .{
                    ._id = node.key,
                    ._score = if (node.depth) |depth|
                        1.0 / (1.0 + @as(f32, @floatFromInt(depth)))
                    else if (node.distance) |distance|
                        @floatCast(distance)
                    else
                        1.0,
                    ._source = source,
                });
            }
        }
    }

    if (hits.items.len > 1) {
        const maybe_branches = try rankedTreeBranchesForQuery(alloc, query, hits.items);
        defer if (maybe_branches) |branches| alloc.free(branches);
        if (maybe_branches) |branches| sortTreeHitsByBranchRank(hits.items, branches);
    }

    return try hits.toOwnedSlice(alloc);
}

fn sortTreeHitsByBranchRank(
    hits: []QueryHit,
    branches: []const TreeBranchSummary,
) void {
    var i: usize = 1;
    while (i < hits.len) : (i += 1) {
        var j = i;
        while (j > 0) : (j -= 1) {
            if (compareTreeRetrievalHitOrder(hits[j - 1], hits[j], branches) <= 0) break;
            const tmp = hits[j - 1];
            hits[j - 1] = hits[j];
            hits[j] = tmp;
        }
    }
}

fn compareTreeRetrievalHitOrder(
    lhs: QueryHit,
    rhs: QueryHit,
    branches: []const TreeBranchSummary,
) i32 {
    const lhs_branch_rank = treeHitBranchRank(lhs, branches);
    const rhs_branch_rank = treeHitBranchRank(rhs, branches);
    if (lhs_branch_rank != rhs_branch_rank) return if (lhs_branch_rank < rhs_branch_rank) -1 else 1;

    const lhs_depth = treeMetaInteger(lhs, "depth") orelse 0;
    const rhs_depth = treeMetaInteger(rhs, "depth") orelse 0;
    if (lhs_depth != rhs_depth) return if (lhs_depth < rhs_depth) -1 else 1;

    const lhs_leaf = treeMetaBool(lhs, "leaf") orelse false;
    const rhs_leaf = treeMetaBool(rhs, "leaf") orelse false;
    if (lhs_leaf != rhs_leaf) return if (!lhs_leaf) -1 else 1;

    if (!std.math.approxEqAbs(f32, lhs._score, rhs._score, 0.0001)) {
        return if (lhs._score > rhs._score) -1 else 1;
    }

    return switch (std.mem.order(u8, lhs._id, rhs._id)) {
        .lt => -1,
        .gt => 1,
        .eq => 0,
    };
}

fn treeHitBranchRank(
    hit: QueryHit,
    branches: []const TreeBranchSummary,
) usize {
    const branch_path = treeMetaString(hit, "branch_path_text") orelse treeMetaString(hit, "path_text") orelse return branches.len;
    for (branches, 0..) |branch, idx| {
        if (std.mem.eql(u8, branch.path, branch_path)) return idx;
    }
    return branches.len;
}

fn annotateTreeDocument(
    alloc: std.mem.Allocator,
    document: std.json.Value,
    search_name: []const u8,
    node: indexes_openapi.GraphResultNode,
    graph_paths: []const GraphPath,
    fallback_root_key: ?[]const u8,
) !std.json.Value {
    if (document != .object) return try json_helpers.cloneJsonValue(alloc, document);

    var object = std.json.ObjectMap.empty;
    errdefer object.deinit(alloc);

    var it = document.object.iterator();
    while (it.next()) |entry| {
        try object.put(alloc, try alloc.dupe(u8, entry.key_ptr.*), try json_helpers.cloneJsonValue(alloc, entry.value_ptr.*));
    }

    var tree_meta = std.json.ObjectMap.empty;
    errdefer tree_meta.deinit(alloc);
    try tree_meta.put(alloc, "search", .{ .string = try alloc.dupe(u8, search_name) });
    const depth = node.depth orelse 0;
    if (node.depth) |node_depth| try tree_meta.put(alloc, "depth", .{ .integer = @intCast(node_depth) });
    const node_path = bestTreePathPrefixForNode(graph_paths, node.key) orelse node.path;
    if (node_path) |path| {
        if (path.len >= 1) {
            try tree_meta.put(alloc, "root", .{ .string = try alloc.dupe(u8, path[0]) });
        }
        if (path.len >= 2) {
            try tree_meta.put(alloc, "parent", .{ .string = try alloc.dupe(u8, path[path.len - 2]) });
        }
        try tree_meta.put(alloc, "path_length", .{ .integer = @intCast(path.len) });
        var path_text = std.ArrayListUnmanaged(u8).empty;
        defer path_text.deinit(alloc);
        for (path, 0..) |segment, i| {
            if (i != 0) try path_text.appendSlice(alloc, " > ");
            try path_text.appendSlice(alloc, segment);
        }
        try tree_meta.put(alloc, "path_text", .{ .string = try path_text.toOwnedSlice(alloc) });
        if (bestTreeBranchPathForNode(graph_paths, node.key)) |branch_path| {
            try tree_meta.put(alloc, "branch_path_length", .{ .integer = @intCast(branch_path.len) });
            try tree_meta.put(alloc, "leaf", .{ .bool = std.mem.eql(u8, branch_path[branch_path.len - 1], node.key) });
            var branch_path_text = std.ArrayListUnmanaged(u8).empty;
            defer branch_path_text.deinit(alloc);
            for (branch_path, 0..) |segment, i| {
                if (i != 0) try branch_path_text.appendSlice(alloc, " > ");
                try branch_path_text.appendSlice(alloc, segment);
            }
            try tree_meta.put(alloc, "branch_path_text", .{ .string = try branch_path_text.toOwnedSlice(alloc) });
        }
    } else if (fallback_root_key) |root_key| {
        try tree_meta.put(alloc, "root", .{ .string = try alloc.dupe(u8, root_key) });
        if (depth == 1 and !std.mem.eql(u8, root_key, node.key)) {
            try tree_meta.put(alloc, "parent", .{ .string = try alloc.dupe(u8, root_key) });
            try tree_meta.put(alloc, "path_length", .{ .integer = 2 });
            try tree_meta.put(alloc, "path_text", .{ .string = try std.fmt.allocPrint(alloc, "{s} > {s}", .{ root_key, node.key }) });
        } else if (depth == 0 or std.mem.eql(u8, root_key, node.key)) {
            try tree_meta.put(alloc, "path_length", .{ .integer = 1 });
            try tree_meta.put(alloc, "path_text", .{ .string = try alloc.dupe(u8, root_key) });
        }
    }

    try object.put(alloc, "_tree", .{ .object = tree_meta });
    return .{ .object = object };
}

fn bestTreePathPrefixForNode(
    graph_paths: []const GraphPath,
    node_key: []const u8,
) ?[]const []const u8 {
    var best: ?[]const []const u8 = null;
    for (graph_paths) |path| {
        const nodes = path.nodes orelse continue;
        const prefix = treePathPrefixForNode(nodes, node_key) orelse continue;
        if (best == null or prefix.len > best.?.len) best = prefix;
    }
    return best;
}

fn bestTreeBranchPathForNode(
    graph_paths: []const GraphPath,
    node_key: []const u8,
) ?[]const []const u8 {
    var best: ?[]const []const u8 = null;
    for (graph_paths) |path| {
        const nodes = path.nodes orelse continue;
        if (treePathPrefixForNode(nodes, node_key) == null) continue;
        if (best == null or nodes.len > best.?.len) best = nodes;
    }
    return best;
}

fn treePathPrefixForNode(
    path_nodes: []const []const u8,
    node_key: []const u8,
) ?[]const []const u8 {
    for (path_nodes, 0..) |segment, i| {
        if (std.mem.eql(u8, segment, node_key)) return path_nodes[0 .. i + 1];
    }
    return null;
}

fn extractTreeFallbackRootKey(
    alloc: std.mem.Allocator,
    query_json: []const u8,
) !?[]const u8 {
    var parsed = std.json.parseFromSlice(QueryRequest, alloc, query_json, .{
        .ignore_unknown_fields = true,
    }) catch return null;
    defer parsed.deinit();

    const graph_searches = parsed.value.graph_searches orelse return null;
    const tree_query = graph_searches.map.get("tree_search") orelse return null;
    const start_nodes = tree_query.start_nodes orelse return null;
    const keys = start_nodes.keys orelse return null;
    if (keys.len != 1) return null;
    return keys[0];
}

fn detectAggregateStrategy(strategies: []const RetrievalStrategy) ?RetrievalStrategy {
    if (strategies.len == 0) return null;
    const first = strategies[0];
    for (strategies[1..]) |strategy| {
        if (strategy != first) return .hybrid;
    }
    return first;
}

fn detectStrategy(retrieval_query: RetrievalQueryRequest) RetrievalStrategy {
    if (retrieval_query.tree_search != null) return .tree;
    if (retrieval_query.graph_searches != null) return .graph;
    const has_semantic = retrieval_query.semantic_search != null or retrieval_query.embeddings != null;
    const has_full_text = retrieval_query.full_text_search != null;
    if (has_semantic and has_full_text) return .hybrid;
    if (has_semantic) return .semantic;
    if (has_full_text) return .bm25;
    return .metadata;
}

test "retrieval agent executes explicit query pipeline" {
    const FakeRunner = struct {
        fn iface() QueryRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .run_query = runQuery,
                },
            };
        }

        fn runQuery(_: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, query_json: []const u8) !query_api.QueryResponse {
            try std.testing.expectEqualStrings("docs", table_name);
            var parsed_query = try parseJsonBody(QueryRequest, alloc, query_json);
            defer parsed_query.deinit();
            try std.testing.expectEqualStrings("alpha concept", parsed_query.value.semantic_search.?);
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"hits":{"hits":[{"_id":"doc:a","_score":1.0,"fields":{"title":"alpha"}}]}}]}
                ),
            };
        }
    };

    const body =
        \\{"query":"find alpha","stream":false,"queries":[{"table":"docs","semantic_search":"alpha concept","indexes":["semantic_idx"],"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, FakeRunner.iface(), null, body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(AgentStatus.completed, parsed.value.status);
    try std.testing.expectEqual(RetrievalStrategy.semantic, parsed.value.strategy_used.?);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.hits.len);
    try std.testing.expectEqualStrings("doc:a", parsed.value.hits[0]._id);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.steps.?.len);
}

test "retrieval agent supports inline tree search" {
    const FakeRunner = struct {
        fn iface() QueryRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(_: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, query_json: []const u8) !query_api.QueryResponse {
            try std.testing.expectEqualStrings("docs", table_name);
            var parsed_query = try parseJsonBody(QueryRequest, alloc, query_json);
            defer parsed_query.deinit();
            const graph_searches = parsed_query.value.graph_searches.?;
            const tree_query = graph_searches.map.get("tree_search").?;
            try std.testing.expectEqualStrings("$tree_search", tree_query.start_nodes.?.result_ref.?);
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"graph_results":{"tree_search":{"type":"traverse","nodes":[{"key":"doc:b","depth":1,"document":{"title":"beta"}}],"paths":[],"total":1,"took":1}}}]}
                ),
            };
        }
    };

    const body =
        \\{"query":"find alpha","stream":false,"queries":[{"table":"docs","full_text_search":{"query":"body:alpha"},"tree_search":{"index":"doc_hierarchy","max_depth":3},"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, FakeRunner.iface(), null, body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(RetrievalStrategy.tree, parsed.value.strategy_used.?);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.hits.len);
    try std.testing.expectEqualStrings("doc:b", parsed.value.hits[0]._id);
}

test "retrieval agent supports pipeline tree search from previous hits" {
    const FakeRunner = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) QueryRunner {
            return .{
                .ptr = self,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            if (self.call_count == 1) {
                var parsed_query = try parseJsonBody(QueryRequest, alloc, query_json);
                defer parsed_query.deinit();
                try std.testing.expectEqualStrings("alpha concept", parsed_query.value.semantic_search.?);
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"title":"alpha"}}]}}]}
                    ),
                };
            }
            const start_key = (try extractTreeFallbackRootKey(alloc, query_json)).?;
            try std.testing.expectEqualStrings("doc:a", start_key);
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"graph_results":{"tree_search":{"type":"traverse","nodes":[{"key":"doc:b","depth":1,"document":{"title":"beta"}}],"paths":[],"total":1,"took":1}}}]}
                ),
            };
        }
    };

    var runner = FakeRunner{};
    const body =
        \\{"query":"find alpha tree","stream":false,"queries":[{"table":"docs","semantic_search":"alpha concept","indexes":["semantic_idx"],"limit":5},{"table":"docs","tree_search":{"index":"doc_hierarchy","start_nodes":"$find_start","max_depth":2},"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, runner.iface(), null, body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(RetrievalStrategy.hybrid, parsed.value.strategy_used.?);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.hits.len);
    try std.testing.expectEqualStrings("doc:a", parsed.value.hits[0]._id);
    try std.testing.expectEqualStrings("doc:b", parsed.value.hits[1]._id);
}

test "retrieval agent supports roots tree search" {
    const FakeRunner = struct {
        fn iface() QueryRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .run_query = runQuery,
                    .scan_keys = scanKeys,
                },
            };
        }

        fn scanKeys(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8) ![]const []const u8 {
            const keys = try alloc.alloc([]const u8, 2);
            keys[0] = try alloc.dupe(u8, "doc:root");
            keys[1] = try alloc.dupe(u8, "doc:child");
            return keys;
        }

        fn runQuery(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            var parsed_query = try parseJsonBody(QueryRequest, alloc, query_json);
            defer parsed_query.deinit();
            if (parsed_query.value.graph_searches) |graph_searches| {
                if (graph_searches.map.get("incoming") != null) {
                    const start_nodes = graph_searches.map.get("incoming").?.start_nodes.?;
                    if (start_nodes.keys) |keys| {
                        if (keys.len == 1 and std.mem.eql(u8, keys[0], "doc:root")) {
                            return .{
                                .json = try alloc.dupe(u8,
                                    \\{"responses":[{"graph_results":{"incoming":{"type":"neighbors","nodes":[],"paths":[],"total":0,"took":1}}}]}
                                ),
                            };
                        }
                    }
                    return .{
                        .json = try alloc.dupe(u8,
                            \\{"responses":[{"graph_results":{"incoming":{"type":"neighbors","nodes":[{"key":"doc:root","depth":1,"document":{"title":"root"}}],"paths":[],"total":1,"took":1}}}]}
                        ),
                    };
                }
            }
            const start_key = (try extractTreeFallbackRootKey(alloc, query_json)).?;
            try std.testing.expectEqualStrings("doc:root", start_key);
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"graph_results":{"tree_search":{"type":"traverse","nodes":[{"key":"doc:child","depth":1,"document":{"title":"child"}}],"paths":[],"total":1,"took":1}}}]}
                ),
            };
        }
    };

    const body =
        \\{"query":"find alpha","stream":false,"queries":[{"table":"docs","tree_search":{"index":"doc_hierarchy","start_nodes":"$roots","max_depth":2},"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, FakeRunner.iface(), null, body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(RetrievalStrategy.tree, parsed.value.strategy_used.?);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.hits.len);
    try std.testing.expectEqualStrings("doc:child", parsed.value.hits[0]._id);
}

test "describe hit for generation includes tree lineage" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var tree = std.json.ObjectMap.empty;
    try tree.put(alloc, "search", .{ .string = try alloc.dupe(u8, "tree_search") });
    try tree.put(alloc, "depth", .{ .integer = 1 });
    try tree.put(alloc, "root", .{ .string = try alloc.dupe(u8, "doc:root") });
    try tree.put(alloc, "parent", .{ .string = try alloc.dupe(u8, "doc:root") });
    try tree.put(alloc, "path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:child") });
    try tree.put(alloc, "branch_path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:child > doc:leaf") });
    try tree.put(alloc, "leaf", .{ .bool = true });

    var source = std.json.ObjectMap.empty;
    try source.put(alloc, "title", .{ .string = try alloc.dupe(u8, "child") });
    try source.put(alloc, "_tree", .{ .object = tree });

    const description = try describeHitForGeneration(alloc, .{
        ._id = "doc:child",
        ._score = 1.0,
        ._source = .{ .object = source },
    });
    defer alloc.free(description);

    try std.testing.expect(std.mem.indexOf(u8, description, "root=doc:root") != null);
    try std.testing.expect(std.mem.indexOf(u8, description, "parent=doc:root") != null);
    try std.testing.expect(std.mem.indexOf(u8, description, "path=doc:root > doc:child > doc:leaf") != null);
    try std.testing.expect(std.mem.indexOf(u8, description, "leaf=true") != null);
}

test "build generation messages includes tree hierarchy context" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var tree = std.json.ObjectMap.empty;
    try tree.put(alloc, "search", .{ .string = try alloc.dupe(u8, "tree_search") });
    try tree.put(alloc, "depth", .{ .integer = 1 });
    try tree.put(alloc, "root", .{ .string = try alloc.dupe(u8, "doc:root") });
    try tree.put(alloc, "parent", .{ .string = try alloc.dupe(u8, "doc:root") });
    try tree.put(alloc, "path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:child") });
    try tree.put(alloc, "branch_path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:child > doc:leaf") });
    try tree.put(alloc, "leaf", .{ .bool = true });

    var source = std.json.ObjectMap.empty;
    try source.put(alloc, "title", .{ .string = try alloc.dupe(u8, "child") });
    try source.put(alloc, "body", .{ .string = try alloc.dupe(u8, "details about the architecture") });
    try source.put(alloc, "_tree", .{ .object = tree });

    const messages = try buildGenerationMessages(alloc, "summarize the hierarchy", &[_]QueryHit{
        .{
            ._id = "doc:child",
            ._score = 1.0,
            ._source = .{ .object = source },
        },
    }, .{
        .chain = &[_]generating.ChainLink{
            .{ .generator = .{
                .provider = .termite,
                .model = "local-generator",
                .url = "http://127.0.0.1:8082",
            } },
        },
        .system_prompt = null,
        .generation_context = null,
    });

    try std.testing.expectEqual(@as(usize, 2), messages.len);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "Tree hierarchy context:") != null);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "Tree roots=1, tree_hits=1") != null);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "Selected tree branches:") != null);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "Branch 1 (root=doc:root, path=doc:root > doc:child > doc:leaf)") != null);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "Summary: child") != null);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "1. root=doc:root path=doc:root > doc:child > doc:leaf") != null);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "Root doc:root") != null);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "Path doc:root > doc:child") != null);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "Branch doc:root > doc:child > doc:leaf") != null);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "Leaf true") != null);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "Title child") != null);
}

test "tree branch selection context ranks strongest branches first" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var branch_a_tree = std.json.ObjectMap.empty;
    try branch_a_tree.put(alloc, "root", .{ .string = try alloc.dupe(u8, "doc:root") });
    try branch_a_tree.put(alloc, "path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:a") });
    try branch_a_tree.put(alloc, "branch_path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:a") });
    var branch_a_source = std.json.ObjectMap.empty;
    try branch_a_source.put(alloc, "_tree", .{ .object = branch_a_tree });

    var branch_b_tree = std.json.ObjectMap.empty;
    try branch_b_tree.put(alloc, "root", .{ .string = try alloc.dupe(u8, "doc:root") });
    try branch_b_tree.put(alloc, "path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:b") });
    try branch_b_tree.put(alloc, "branch_path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:b") });
    var branch_b_source = std.json.ObjectMap.empty;
    try branch_b_source.put(alloc, "_tree", .{ .object = branch_b_tree });

    const summary = try buildTreeBranchSelectionContext(alloc, &[_]QueryHit{
        .{ ._id = "doc:a", ._score = 0.6, ._source = .{ .object = branch_a_source } },
        .{ ._id = "doc:b", ._score = 0.9, ._source = .{ .object = branch_b_source } },
    });
    try std.testing.expect(summary != null);
    try std.testing.expect(std.mem.indexOf(u8, summary.?, "1. root=doc:root path=doc:root > doc:b") != null);
    try std.testing.expect(std.mem.indexOf(u8, summary.?, "2. root=doc:root path=doc:root > doc:a") != null);
}

test "generation messages keep only the strongest tree branches" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var branch_a_tree = std.json.ObjectMap.empty;
    try branch_a_tree.put(alloc, "root", .{ .string = try alloc.dupe(u8, "doc:root") });
    try branch_a_tree.put(alloc, "path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:a") });
    try branch_a_tree.put(alloc, "branch_path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:a") });
    try branch_a_tree.put(alloc, "depth", .{ .integer = 1 });
    var branch_a_source = std.json.ObjectMap.empty;
    try branch_a_source.put(alloc, "title", .{ .string = try alloc.dupe(u8, "branch a") });
    try branch_a_source.put(alloc, "_tree", .{ .object = branch_a_tree });

    var branch_b_tree = std.json.ObjectMap.empty;
    try branch_b_tree.put(alloc, "root", .{ .string = try alloc.dupe(u8, "doc:root") });
    try branch_b_tree.put(alloc, "path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:b") });
    try branch_b_tree.put(alloc, "branch_path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:b") });
    try branch_b_tree.put(alloc, "depth", .{ .integer = 1 });
    var branch_b_source = std.json.ObjectMap.empty;
    try branch_b_source.put(alloc, "title", .{ .string = try alloc.dupe(u8, "branch b") });
    try branch_b_source.put(alloc, "_tree", .{ .object = branch_b_tree });

    var branch_c_tree = std.json.ObjectMap.empty;
    try branch_c_tree.put(alloc, "root", .{ .string = try alloc.dupe(u8, "doc:root") });
    try branch_c_tree.put(alloc, "path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:c") });
    try branch_c_tree.put(alloc, "branch_path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:c") });
    try branch_c_tree.put(alloc, "depth", .{ .integer = 1 });
    var branch_c_source = std.json.ObjectMap.empty;
    try branch_c_source.put(alloc, "title", .{ .string = try alloc.dupe(u8, "branch c") });
    try branch_c_source.put(alloc, "_tree", .{ .object = branch_c_tree });

    const messages = try buildGenerationMessages(alloc, "pick the strongest branch", &[_]QueryHit{
        .{ ._id = "doc:a", ._score = 0.92, ._source = .{ .object = branch_a_source } },
        .{ ._id = "doc:b", ._score = 0.87, ._source = .{ .object = branch_b_source } },
        .{ ._id = "doc:c", ._score = 0.21, ._source = .{ .object = branch_c_source } },
    }, .{
        .chain = &[_]generating.ChainLink{
            .{ .generator = .{
                .provider = .termite,
                .model = "local-generator",
                .url = "http://127.0.0.1:8082",
            } },
        },
        .system_prompt = null,
        .generation_context = null,
    });

    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "doc:root > doc:a") != null);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "doc:root > doc:b") != null);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "doc:root > doc:c") == null);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "Branch 1 (root=doc:root, path=doc:root > doc:a)") != null);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "Branch 2 (root=doc:root, path=doc:root > doc:b)") != null);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "Summary: branch a") != null);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "Summary: branch b") != null);
}

test "generation messages prefer query-relevant tree branches" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var infra_tree = std.json.ObjectMap.empty;
    try infra_tree.put(alloc, "root", .{ .string = try alloc.dupe(u8, "doc:root") });
    try infra_tree.put(alloc, "path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:infra") });
    try infra_tree.put(alloc, "branch_path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:infra") });
    try infra_tree.put(alloc, "depth", .{ .integer = 1 });
    var infra_source = std.json.ObjectMap.empty;
    try infra_source.put(alloc, "title", .{ .string = try alloc.dupe(u8, "infrastructure overview") });
    try infra_source.put(alloc, "_tree", .{ .object = infra_tree });

    var payments_tree = std.json.ObjectMap.empty;
    try payments_tree.put(alloc, "root", .{ .string = try alloc.dupe(u8, "doc:root") });
    try payments_tree.put(alloc, "path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:payments") });
    try payments_tree.put(alloc, "branch_path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:payments") });
    try payments_tree.put(alloc, "depth", .{ .integer = 1 });
    var payments_source = std.json.ObjectMap.empty;
    try payments_source.put(alloc, "title", .{ .string = try alloc.dupe(u8, "payments architecture") });
    try payments_source.put(alloc, "_tree", .{ .object = payments_tree });

    var storage_tree = std.json.ObjectMap.empty;
    try storage_tree.put(alloc, "root", .{ .string = try alloc.dupe(u8, "doc:root") });
    try storage_tree.put(alloc, "path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:storage") });
    try storage_tree.put(alloc, "branch_path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:storage") });
    try storage_tree.put(alloc, "depth", .{ .integer = 1 });
    var storage_source = std.json.ObjectMap.empty;
    try storage_source.put(alloc, "title", .{ .string = try alloc.dupe(u8, "storage internals") });
    try storage_source.put(alloc, "_tree", .{ .object = storage_tree });

    const messages = try buildGenerationMessages(alloc, "explain payments architecture", &[_]QueryHit{
        .{ ._id = "doc:infra", ._score = 0.93, ._source = .{ .object = infra_source } },
        .{ ._id = "doc:storage", ._score = 0.91, ._source = .{ .object = storage_source } },
        .{ ._id = "doc:payments", ._score = 0.22, ._source = .{ .object = payments_source } },
    }, .{
        .chain = &[_]generating.ChainLink{
            .{ .generator = .{
                .provider = .termite,
                .model = "local-generator",
                .url = "http://127.0.0.1:8082",
            } },
        },
        .system_prompt = null,
        .generation_context = null,
    });

    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "doc:root > doc:payments") != null);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "query_relevance=") != null);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "doc:root > doc:storage") == null);
}

test "generation messages trim branch context after ancestor-first limit" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const ids = [_][]const u8{ "doc:root", "doc:child", "doc:grandchild", "doc:leaf" };
    const depths = [_]i64{ 0, 1, 2, 3 };

    var hits = std.ArrayListUnmanaged(QueryHit).empty;
    defer hits.deinit(alloc);

    for (ids, depths) |id, depth| {
        var tree = std.json.ObjectMap.empty;
        try tree.put(alloc, "root", .{ .string = try alloc.dupe(u8, "doc:root") });
        try tree.put(alloc, "path_text", .{ .string = try alloc.dupe(u8, id) });
        try tree.put(alloc, "branch_path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:child > doc:grandchild > doc:leaf") });
        try tree.put(alloc, "depth", .{ .integer = depth });
        try tree.put(alloc, "leaf", .{ .bool = std.mem.eql(u8, id, "doc:leaf") });
        if (depth > 0) {
            const parent = switch (depth) {
                1 => "doc:root",
                2 => "doc:child",
                else => "doc:grandchild",
            };
            try tree.put(alloc, "parent", .{ .string = try alloc.dupe(u8, parent) });
        }
        var source = std.json.ObjectMap.empty;
        try source.put(alloc, "title", .{ .string = try alloc.dupe(u8, id) });
        try source.put(alloc, "_tree", .{ .object = tree });
        try hits.append(alloc, .{
            ._id = id,
            ._score = 1.0 - @as(f32, @floatFromInt(depth)) * 0.1,
            ._source = .{ .object = source },
        });
    }

    const messages = try buildGenerationMessages(alloc, "trim the branch", hits.items, .{
        .chain = &[_]generating.ChainLink{
            .{ .generator = .{
                .provider = .termite,
                .model = "local-generator",
                .url = "http://127.0.0.1:8082",
            } },
        },
        .system_prompt = null,
        .generation_context = null,
    });

    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "doc:root") != null);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "doc:child") != null);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "doc:grandchild") != null);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "doc:leaf") == null);
}

test "generation messages expand branch when deeper node is query-relevant" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const ids = [_][]const u8{ "doc:root", "doc:child", "doc:grandchild", "doc:leaf" };
    const titles = [_][]const u8{ "root", "child", "grandchild", "payments rollout" };
    const depths = [_]i64{ 0, 1, 2, 3 };

    var hits = std.ArrayListUnmanaged(QueryHit).empty;
    defer hits.deinit(alloc);

    for (ids, titles, depths) |id, title, depth| {
        var tree = std.json.ObjectMap.empty;
        try tree.put(alloc, "root", .{ .string = try alloc.dupe(u8, "doc:root") });
        try tree.put(alloc, "path_text", .{ .string = try alloc.dupe(u8, id) });
        try tree.put(alloc, "branch_path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:child > doc:grandchild > doc:leaf") });
        try tree.put(alloc, "depth", .{ .integer = depth });
        try tree.put(alloc, "leaf", .{ .bool = std.mem.eql(u8, id, "doc:leaf") });
        if (depth > 0) {
            const parent = switch (depth) {
                1 => "doc:root",
                2 => "doc:child",
                else => "doc:grandchild",
            };
            try tree.put(alloc, "parent", .{ .string = try alloc.dupe(u8, parent) });
        }
        var source = std.json.ObjectMap.empty;
        try source.put(alloc, "title", .{ .string = try alloc.dupe(u8, title) });
        try source.put(alloc, "_tree", .{ .object = tree });
        try hits.append(alloc, .{
            ._id = id,
            ._score = 1.0 - @as(f32, @floatFromInt(depth)) * 0.1,
            ._source = .{ .object = source },
        });
    }

    const messages = try buildGenerationMessages(alloc, "payments rollout", hits.items, .{
        .chain = &[_]generating.ChainLink{
            .{ .generator = .{
                .provider = .termite,
                .model = "local-generator",
                .url = "http://127.0.0.1:8082",
            } },
        },
        .system_prompt = null,
        .generation_context = null,
    });

    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "doc:leaf") != null);
    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "payments rollout") != null);
}

test "generation messages can expand to a deeply relevant descendant" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const ids = [_][]const u8{
        "doc:root",
        "doc:child",
        "doc:grandchild",
        "doc:section",
        "doc:topic",
        "doc:leaf",
    };
    const titles = [_][]const u8{
        "root",
        "child",
        "grandchild",
        "section",
        "topic",
        "quarterly revenue forecast",
    };
    const depths = [_]i64{ 0, 1, 2, 3, 4, 5 };

    var hits = std.ArrayListUnmanaged(QueryHit).empty;
    defer hits.deinit(alloc);

    for (ids, titles, depths) |id, title, depth| {
        var tree = std.json.ObjectMap.empty;
        try tree.put(alloc, "root", .{ .string = try alloc.dupe(u8, "doc:root") });
        try tree.put(alloc, "path_text", .{ .string = try alloc.dupe(u8, id) });
        try tree.put(alloc, "branch_path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:child > doc:grandchild > doc:section > doc:topic > doc:leaf") });
        try tree.put(alloc, "depth", .{ .integer = depth });
        try tree.put(alloc, "leaf", .{ .bool = std.mem.eql(u8, id, "doc:leaf") });
        if (depth > 0) {
            const parent = switch (depth) {
                1 => "doc:root",
                2 => "doc:child",
                3 => "doc:grandchild",
                4 => "doc:section",
                else => "doc:topic",
            };
            try tree.put(alloc, "parent", .{ .string = try alloc.dupe(u8, parent) });
        }
        var source = std.json.ObjectMap.empty;
        try source.put(alloc, "title", .{ .string = try alloc.dupe(u8, title) });
        try source.put(alloc, "_tree", .{ .object = tree });
        try hits.append(alloc, .{
            ._id = id,
            ._score = 1.0 - @as(f32, @floatFromInt(depth)) * 0.05,
            ._source = .{ .object = source },
        });
    }

    const messages = try buildGenerationMessages(alloc, "revenue forecast", hits.items, .{
        .chain = &[_]generating.ChainLink{
            .{ .generator = .{
                .provider = .termite,
                .model = "local-generator",
                .url = "http://127.0.0.1:8082",
            } },
        },
        .system_prompt = null,
        .generation_context = null,
    });

    try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "id=doc:leaf") != null);
}

test "generation ordering prefers tree ancestors before leaves" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var root_tree = std.json.ObjectMap.empty;
    try root_tree.put(alloc, "root", .{ .string = try alloc.dupe(u8, "doc:root") });
    try root_tree.put(alloc, "depth", .{ .integer = 0 });
    try root_tree.put(alloc, "path_text", .{ .string = try alloc.dupe(u8, "doc:root") });
    try root_tree.put(alloc, "branch_path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:child > doc:leaf") });
    try root_tree.put(alloc, "leaf", .{ .bool = false });
    var root_source = std.json.ObjectMap.empty;
    try root_source.put(alloc, "title", .{ .string = try alloc.dupe(u8, "root") });
    try root_source.put(alloc, "_tree", .{ .object = root_tree });

    var leaf_tree = std.json.ObjectMap.empty;
    try leaf_tree.put(alloc, "root", .{ .string = try alloc.dupe(u8, "doc:root") });
    try leaf_tree.put(alloc, "parent", .{ .string = try alloc.dupe(u8, "doc:child") });
    try leaf_tree.put(alloc, "depth", .{ .integer = 2 });
    try leaf_tree.put(alloc, "path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:child > doc:leaf") });
    try leaf_tree.put(alloc, "branch_path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:child > doc:leaf") });
    try leaf_tree.put(alloc, "leaf", .{ .bool = true });
    var leaf_source = std.json.ObjectMap.empty;
    try leaf_source.put(alloc, "title", .{ .string = try alloc.dupe(u8, "leaf") });
    try leaf_source.put(alloc, "_tree", .{ .object = leaf_tree });

    const ordered = try orderHitsForGeneration(alloc, &[_]QueryHit{
        .{ ._id = "doc:leaf", ._score = 1.0, ._source = .{ .object = leaf_source } },
        .{ ._id = "doc:root", ._score = 0.5, ._source = .{ .object = root_source } },
    });
    try std.testing.expectEqualStrings("doc:root", ordered[0]._id);
    try std.testing.expectEqualStrings("doc:leaf", ordered[1]._id);
}

test "annotate tree document prefers graph path branch metadata" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var document = std.json.ObjectMap.empty;
    try document.put(alloc, "title", .{ .string = try alloc.dupe(u8, "child") });

    const paths = [_]GraphPath{
        .{ .nodes = &[_][]const u8{ "doc:root", "doc:child", "doc:leaf" } },
    };
    const annotated = try annotateTreeDocument(
        alloc,
        .{ .object = document },
        "tree_search",
        .{
            .key = "doc:child",
            .depth = 1,
            .document = .{ .object = document },
        },
        &paths,
        null,
    );
    const meta = annotated.object.get("_tree").?.object;
    try std.testing.expectEqualStrings("doc:root", meta.get("root").?.string);
    try std.testing.expectEqualStrings("doc:root", meta.get("parent").?.string);
    try std.testing.expectEqualStrings("doc:root > doc:child", meta.get("path_text").?.string);
    try std.testing.expectEqualStrings("doc:root > doc:child > doc:leaf", meta.get("branch_path_text").?.string);
    try std.testing.expectEqual(false, meta.get("leaf").?.bool);
}

test "extract tree hits prefers strongest branches and ancestor ordering" {
    const alloc = std.testing.allocator;

    const response_json =
        \\{"responses":[{"graph_results":{"tree_search":{"type":"traverse","nodes":[
        \\{"key":"doc:b","depth":1,"document":{"title":"branch b"}},
        \\{"key":"doc:a","depth":1,"document":{"title":"branch a"}},
        \\{"key":"doc:a:leaf","depth":2,"document":{"title":"branch a leaf"}}
        \\],"paths":[
        \\{"nodes":["doc:root","doc:a","doc:a:leaf"]},
        \\{"nodes":["doc:root","doc:b"]}
        \\],"total":3,"took":1}}}]}
    ;

    var parsed = try std.json.parseFromSlice(QueryResponses, alloc, response_json, .{});
    defer parsed.deinit();

    const hits = try extractTreeHits(alloc, parsed.value, "find alpha", "doc:root");
    defer alloc.free(hits);

    try std.testing.expectEqual(@as(usize, 3), hits.len);
    try std.testing.expectEqualStrings("doc:a", hits[0]._id);
    try std.testing.expectEqualStrings("doc:a:leaf", hits[1]._id);
    try std.testing.expectEqualStrings("doc:b", hits[2]._id);
}

test "tree branch expansion plan picks strongest visible branch seed" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var root_tree = std.json.ObjectMap.empty;
    try root_tree.put(alloc, "root", .{ .string = try alloc.dupe(u8, "doc:root") });
    try root_tree.put(alloc, "path_text", .{ .string = try alloc.dupe(u8, "doc:root") });
    try root_tree.put(alloc, "branch_path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:payments") });
    try root_tree.put(alloc, "depth", .{ .integer = 0 });
    try root_tree.put(alloc, "leaf", .{ .bool = false });
    var root_source = std.json.ObjectMap.empty;
    try root_source.put(alloc, "title", .{ .string = try alloc.dupe(u8, "architecture root") });
    try root_source.put(alloc, "_tree", .{ .object = root_tree });

    var payments_tree = std.json.ObjectMap.empty;
    try payments_tree.put(alloc, "root", .{ .string = try alloc.dupe(u8, "doc:root") });
    try payments_tree.put(alloc, "parent", .{ .string = try alloc.dupe(u8, "doc:root") });
    try payments_tree.put(alloc, "path_text", .{ .string = try alloc.dupe(u8, "doc:payments") });
    try payments_tree.put(alloc, "branch_path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:payments") });
    try payments_tree.put(alloc, "depth", .{ .integer = 1 });
    try payments_tree.put(alloc, "leaf", .{ .bool = false });
    var payments_source = std.json.ObjectMap.empty;
    try payments_source.put(alloc, "title", .{ .string = try alloc.dupe(u8, "payments roadmap") });
    try payments_source.put(alloc, "_tree", .{ .object = payments_tree });

    var infra_tree = std.json.ObjectMap.empty;
    try infra_tree.put(alloc, "root", .{ .string = try alloc.dupe(u8, "doc:root") });
    try infra_tree.put(alloc, "parent", .{ .string = try alloc.dupe(u8, "doc:root") });
    try infra_tree.put(alloc, "path_text", .{ .string = try alloc.dupe(u8, "doc:infra") });
    try infra_tree.put(alloc, "branch_path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:infra") });
    try infra_tree.put(alloc, "depth", .{ .integer = 1 });
    try infra_tree.put(alloc, "leaf", .{ .bool = false });
    var infra_source = std.json.ObjectMap.empty;
    try infra_source.put(alloc, "title", .{ .string = try alloc.dupe(u8, "infrastructure overview") });
    try infra_source.put(alloc, "_tree", .{ .object = infra_tree });

    const hits = [_]QueryHit{
        .{ ._id = "doc:root", ._score = 0.7, ._source = .{ .object = root_source } },
        .{ ._id = "doc:payments", ._score = 0.5, ._source = .{ .object = payments_source } },
        .{ ._id = "doc:infra", ._score = 0.95, ._source = .{ .object = infra_source } },
    };

    const plan = (try selectTreeBranchExpansionPlan(alloc, "payments roadmap", .{
        .index = "doc_hierarchy",
        .start_nodes = "$roots",
        .max_depth = 3,
        .beam_width = 3,
    }, &hits)).?;

    try std.testing.expectEqualStrings("doc:root > doc:payments", plan.branch.path);
    try std.testing.expectEqualStrings("doc:payments", plan.seed_key);
    try std.testing.expectEqual(@as(usize, 1), plan.seed_depth);
}

test "attempt evaluation summary includes top tree branch quality" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var root_tree = std.json.ObjectMap.empty;
    try root_tree.put(alloc, "root", .{ .string = try alloc.dupe(u8, "doc:root") });
    try root_tree.put(alloc, "path_text", .{ .string = try alloc.dupe(u8, "doc:root") });
    try root_tree.put(alloc, "branch_path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:payments") });
    try root_tree.put(alloc, "depth", .{ .integer = 0 });
    try root_tree.put(alloc, "leaf", .{ .bool = false });
    var root_source = std.json.ObjectMap.empty;
    try root_source.put(alloc, "title", .{ .string = try alloc.dupe(u8, "payments root") });
    try root_source.put(alloc, "_tree", .{ .object = root_tree });

    var leaf_tree = std.json.ObjectMap.empty;
    try leaf_tree.put(alloc, "root", .{ .string = try alloc.dupe(u8, "doc:root") });
    try leaf_tree.put(alloc, "parent", .{ .string = try alloc.dupe(u8, "doc:root") });
    try leaf_tree.put(alloc, "path_text", .{ .string = try alloc.dupe(u8, "doc:leaf") });
    try leaf_tree.put(alloc, "branch_path_text", .{ .string = try alloc.dupe(u8, "doc:root > doc:payments") });
    try leaf_tree.put(alloc, "depth", .{ .integer = 1 });
    try leaf_tree.put(alloc, "leaf", .{ .bool = true });
    var leaf_source = std.json.ObjectMap.empty;
    try leaf_source.put(alloc, "title", .{ .string = try alloc.dupe(u8, "payments details") });
    try leaf_source.put(alloc, "_tree", .{ .object = leaf_tree });

    const summary = summarizeAttemptEvaluation(alloc, "payments", &[_]QueryHit{
        .{ ._id = "doc:root", ._score = 0.8, ._source = .{ .object = root_source } },
        .{ ._id = "doc:leaf", ._score = 0.7, ._source = .{ .object = leaf_source } },
    });

    try std.testing.expect(summary.top_tree_branch_relevance != null);
    try std.testing.expect(summary.top_tree_branch_relevance.? > 0.0);
    try std.testing.expectEqual(@as(i64, 2), summary.top_tree_branch_nodes.?);
    try std.testing.expectEqual(@as(i64, 1), summary.top_tree_branch_leaf_hits.?);
}

test "retrieval agent can select multiple evaluation refinement phrases" {
    const classification = ai_openapi.ClassificationTransformationResult{
        .route_type = .question,
        .strategy = .simple,
        .improved_query = "improved architecture query",
        .semantic_mode = .rewrite,
        .semantic_query = "semantic architecture query",
        .confidence = 0.9,
        .multi_phrases = &[_][]const u8{
            "architecture overview",
            "architecture planning",
        },
    };
    const retrieval_query: RetrievalQueryRequest = .{
        .table = "docs",
        .semantic_search = "architecture",
        .indexes = &[_][]const u8{"semantic_idx"},
        .limit = 5,
    };

    const first = nextEvaluationRefinedQueryText(classification, retrieval_query, 0, &[_][]const u8{
        "architecture",
        "semantic architecture query",
    }).?;
    try std.testing.expectEqualStrings("architecture overview", first);

    const second = nextEvaluationRefinedQueryText(classification, retrieval_query, 0, &[_][]const u8{
        "architecture",
        "semantic architecture query",
        "architecture overview",
    }).?;
    try std.testing.expectEqualStrings("architecture planning", second);

    const third = nextEvaluationRefinedQueryText(classification, retrieval_query, 0, &[_][]const u8{
        "architecture",
        "semantic architecture query",
        "architecture overview",
        "architecture planning",
    }).?;
    try std.testing.expectEqualStrings("improved architecture query", third);
}

test "planner can keep current semantic result over fallback on direct quality comparison" {
    const attempt_summary: AttemptEvaluationSummary = .{
        .hit_count = 2,
        .top_score = 0.88,
        .context_relevance = 0.84,
        .context_length = 120,
    };
    const previous_summary: AttemptEvaluationSummary = .{
        .hit_count = 2,
        .top_score = 0.71,
        .context_relevance = 0.70,
        .context_length = 58,
    };
    const candidates = [_]AgenticCandidateScore{
        .{
            .index = 1,
            .strategy = .hybrid,
            .score = 60,
            .probe_hits = 3,
            .probe_relevance = 0.79,
            .probe_top_score = 0.83,
        },
        .{
            .index = 2,
            .strategy = .semantic,
            .score = 58,
            .probe_hits = 2,
            .probe_relevance = 0.76,
            .probe_top_score = 0.80,
        },
    };
    const attempted = [_]bool{ true, false, false };
    try std.testing.expectEqual(
        AgenticPlannerDecision.accept_result,
        decideAgenticPlannerAction(
            .partial_result,
            .semantic,
            attempt_summary,
            previous_summary,
            &candidates,
            &attempted,
            false,
            false,
            false,
        ),
    );
}

test "planner can clarify when current result and fallback are effectively tied" {
    const attempt_summary: AttemptEvaluationSummary = .{
        .hit_count = 2,
        .top_score = 0.82,
        .context_relevance = 0.72,
        .context_length = 88,
    };
    const previous_summary: AttemptEvaluationSummary = .{
        .hit_count = 1,
        .top_score = 0.80,
        .context_relevance = 0.70,
        .context_length = 60,
    };
    const candidates = [_]AgenticCandidateScore{
        .{
            .index = 1,
            .strategy = .hybrid,
            .score = 57,
            .probe_hits = 2,
            .probe_relevance = 0.75,
            .probe_top_score = 0.84,
        },
        .{
            .index = 2,
            .strategy = .semantic,
            .score = 55,
            .probe_hits = 1,
            .probe_relevance = 0.69,
            .probe_top_score = 0.79,
        },
    };
    const attempted = [_]bool{ true, false, false };
    try std.testing.expectEqual(
        AgenticPlannerDecision.clarify,
        decideAgenticPlannerAction(
            .partial_result,
            .semantic,
            attempt_summary,
            previous_summary,
            &candidates,
            &attempted,
            false,
            false,
            true,
        ),
    );
}

test "planner can clarify when fallback candidates disagree but both beat current result" {
    const attempt_summary: AttemptEvaluationSummary = .{
        .hit_count = 1,
        .top_score = 0.61,
        .context_relevance = 0.44,
        .context_length = 54,
    };
    const previous_summary: AttemptEvaluationSummary = .{
        .hit_count = 1,
        .top_score = 0.60,
        .context_relevance = 0.43,
        .context_length = 50,
    };
    const candidates = [_]AgenticCandidateScore{
        .{
            .index = 1,
            .strategy = .hybrid,
            .score = 62,
            .probe_hits = 3,
            .probe_relevance = 0.77,
            .probe_top_score = 0.83,
        },
        .{
            .index = 2,
            .strategy = .tree,
            .score = 61,
            .probe_hits = 2,
            .probe_relevance = 0.74,
            .probe_top_score = 0.81,
        },
    };
    const attempted = [_]bool{ true, false, false };
    try std.testing.expectEqual(
        AgenticPlannerDecision.clarify,
        decideAgenticPlannerAction(
            .partial_result,
            .semantic,
            attempt_summary,
            previous_summary,
            &candidates,
            &attempted,
            false,
            false,
            true,
        ),
    );
}

test "planner can expand a thin but promising tree branch before switching" {
    const attempt_summary: AttemptEvaluationSummary = .{
        .hit_count = 2,
        .top_score = 0.74,
        .context_relevance = 0.48,
        .context_length = 90,
        .top_tree_branch_relevance = 0.66,
        .top_tree_branch_nodes = 2,
        .top_tree_branch_leaf_hits = 0,
    };
    const previous_summary: AttemptEvaluationSummary = .{
        .hit_count = 1,
        .top_score = 0.70,
        .context_relevance = 0.43,
        .context_length = 58,
        .top_tree_branch_relevance = 0.55,
        .top_tree_branch_nodes = 2,
        .top_tree_branch_leaf_hits = 0,
    };
    const candidates = [_]AgenticCandidateScore{
        .{
            .index = 1,
            .strategy = .semantic,
            .score = 58,
            .probe_hits = 2,
            .probe_relevance = 0.62,
            .probe_top_score = 0.79,
        },
        .{
            .index = 2,
            .strategy = .hybrid,
            .score = 55,
            .probe_hits = 2,
            .probe_relevance = 0.59,
            .probe_top_score = 0.77,
        },
    };
    const attempted = [_]bool{ true, false, false };
    try std.testing.expectEqual(
        AgenticPlannerDecision.expand_branch,
        decideAgenticPlannerAction(
            .partial_result,
            .tree,
            attempt_summary,
            previous_summary,
            &candidates,
            &attempted,
            false,
            true,
            true,
        ),
    );
}

test "retrieval agent supports bounded agentic mode" {
    const FakeRunner = struct {
        call_count: usize = 0,

        fn iface() QueryRunner {
            @panic("use ifaceWithState");
        }

        fn ifaceWithState(self: *@This()) QueryRunner {
            return .{
                .ptr = self,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            if (self.call_count == 1) {
                try std.testing.expect(std.mem.indexOf(u8, query_json, "Background context and core Antfly concepts needed for: How does Raft work?") != null);
            } else if (self.call_count == 2) {
                try std.testing.expect(std.mem.indexOf(u8, query_json, "antfly background concepts and context for How does Raft work?") != null);
            }
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"content":"alpha body"}}]}}]}
                ),
            };
        }
    };

    const body =
        \\{"query":"How does Raft work?","stream":false,"generator":{"provider":"termite","model":"local-generator","api_url":"http://127.0.0.1:8082"},"max_internal_iterations":3,"queries":[{"table":"docs","semantic_search":"raft consensus","indexes":["semantic_idx"],"limit":5}]}
    ;
    var runner = FakeRunner{};
    const encoded = try executeJson(std.testing.allocator, runner.ifaceWithState(), null, body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(AgentStatus.completed, parsed.value.status);
    try std.testing.expectEqual(@as(i64, 2), parsed.value.tool_calls_made.?);
    try std.testing.expectEqual(@as(i64, 2), parsed.value.iteration.?);
    try std.testing.expectEqual(@as(i64, 1), parsed.value.remaining_internal_iterations.?);
    try std.testing.expect(parsed.value.steps != null);
    try std.testing.expect(runner.call_count == 2);
}

test "retrieval agent agentic streaming emits tool mode" {
    const FakeRunner = struct {
        call_count: usize = 0,

        fn iface() QueryRunner {
            @panic("use ifaceWithState");
        }

        fn ifaceWithState(self: *@This()) QueryRunner {
            return .{
                .ptr = self,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            if (self.call_count == 1) {
                try std.testing.expect(std.mem.indexOf(u8, query_json, "Background context and core Antfly concepts needed for: How does Raft work?") != null);
            } else if (self.call_count == 2) {
                try std.testing.expect(std.mem.indexOf(u8, query_json, "antfly background concepts and context for How does Raft work?") != null);
            }
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"content":"alpha body"}}]}}]}
                ),
            };
        }
    };

    const body =
        \\{"query":"How does Raft work?","stream":true,"generator":{"provider":"termite","model":"local-generator","api_url":"http://127.0.0.1:8082"},"max_internal_iterations":3,"queries":[{"table":"docs","semantic_search":"raft consensus","indexes":["semantic_idx"],"limit":5}]}
    ;
    var runner = FakeRunner{};
    const encoded = try execute(std.testing.allocator, runner.ifaceWithState(), null, body);
    defer std.testing.allocator.free(encoded.body);
    const events = try parseSseEventsAlloc(std.testing.allocator, encoded.body);
    defer std.testing.allocator.free(events);

    try std.testing.expectEqualStrings("text/event-stream", encoded.content_type);
    try std.testing.expect(countSseEvents(events, "tool_mode") >= 1);
    try std.testing.expect(countSseEvents(events, "classification") >= 1);
    try std.testing.expect(countSseEvents(events, "reasoning") >= 1);
    try std.testing.expect(countSseEvents(events, "step_progress") >= 1);
    var parsed_tool_mode = try parseJsonBody(TestToolModeEvent, std.testing.allocator, firstSseEventData(events, "tool_mode").?);
    defer parsed_tool_mode.deinit();
    try std.testing.expectEqualStrings("structured_output", parsed_tool_mode.value.mode);
    try std.testing.expect(parsed_tool_mode.value.tools_count == null);
    var saw_select_strategy = false;
    var saw_step_back_followup = false;
    for (events) |event| {
        if (!std.mem.eql(u8, event.event, "step_progress")) continue;
        var parsed_progress = try parseJsonBody(TestStepProgressEvent, std.testing.allocator, event.data);
        defer parsed_progress.deinit();
        if (std.mem.eql(u8, parsed_progress.value.phase, "select_strategy")) saw_select_strategy = true;
        if (std.mem.eql(u8, parsed_progress.value.phase, "step_back_followup")) saw_step_back_followup = true;
    }
    try std.testing.expect(saw_select_strategy);
    try std.testing.expect(saw_step_back_followup);
    var parsed_reasoning = try parseJsonBody([]const u8, std.testing.allocator, firstSseEventData(events, "reasoning").?);
    defer parsed_reasoning.deinit();
    try std.testing.expect(std.mem.indexOf(u8, parsed_reasoning.value, "Selected step_back retrieval in rewrite mode") != null);
    try std.testing.expect(runner.call_count == 2);
}

test "retrieval agent streaming emits go-shaped tree search progress" {
    const FakeRunner = struct {
        fn iface() QueryRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const u8) !query_api.QueryResponse {
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"graph_results":{"tree_search":{"type":"traverse","nodes":[{"key":"doc:child","depth":1,"document":{"title":"child","body":"details about the architecture"}}],"paths":[{"nodes":["doc:root","doc:child"]}],"total":1,"took":1}}}]}
                ),
            };
        }
    };

    const body =
        \\{"query":"summarize the architecture tree","stream":true,"queries":[{"table":"docs","tree_search":{"index":"doc_hierarchy","start_nodes":"$roots","max_depth":2,"beam_width":2},"limit":5}]}
    ;
    const encoded = try execute(std.testing.allocator, FakeRunner.iface(), null, body);
    defer std.testing.allocator.free(encoded.body);
    const events = try parseSseEventsAlloc(std.testing.allocator, encoded.body);
    defer std.testing.allocator.free(events);

    try std.testing.expectEqualStrings("text/event-stream", encoded.content_type);
    var saw_tree_search = false;
    for (events) |event| {
        if (!std.mem.eql(u8, event.event, "step_progress")) continue;
        var parsed_progress = try parseJsonBody(TestStepProgressEvent, std.testing.allocator, event.data);
        defer parsed_progress.deinit();
        if (!std.mem.eql(u8, parsed_progress.value.phase, "tree_search")) continue;
        saw_tree_search = true;
        try std.testing.expectEqual(@as(i64, 1), parsed_progress.value.num_nodes.?);
        try std.testing.expectEqual(@as(i64, 1), parsed_progress.value.collected.?);
        try std.testing.expectEqual(true, parsed_progress.value.complete.?);
    }
    try std.testing.expect(saw_tree_search);
}

test "retrieval agent agentic mode selects one best query" {
    const FakeRunner = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) QueryRunner {
            return .{
                .ptr = self,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            var parsed_query = try parseJsonBody(QueryRequest, alloc, query_json);
            defer parsed_query.deinit();
            try std.testing.expect(parsed_query.value.full_text_search != null);
            try expectFullTextQueryValue(parsed_query.value.full_text_search.?, "body:raft");
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"content":"raft consensus in antfly"}}]}}]}
                ),
            };
        }
    };

    var runner = FakeRunner{};
    const body =
        \\{"query":"How does Raft consensus work in Antfly?","stream":false,"max_internal_iterations":3,"queries":[{"table":"docs","full_text_search":{"query":"body:raft"},"limit":5},{"table":"docs","filter_query":{"query":"status:active"},"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, runner.iface(), null, body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), runner.call_count);
    try std.testing.expectEqual(@as(i64, 1), parsed.value.tool_calls_made.?);
    try std.testing.expectEqual(RetrievalStrategy.bm25, parsed.value.strategy_used.?);
    try std.testing.expect(parsed.value.classification != null);
    try std.testing.expectEqualStrings("select_strategy", parsed.value.steps.?[1].name);
    const selection_details = parsed.value.steps.?[1].details.?;
    try std.testing.expect(std.mem.eql(u8, selection_details.object.get("selection_source").?.string, "heuristic"));
    try std.testing.expect(selection_details.object.get("candidate_scores").?.array.items.len == 2);
}

test "retrieval agent agentic mode can resolve ambiguity by probing candidates" {
    const FakeRunner = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) QueryRunner {
            return .{
                .ptr = self,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            var parsed_query = try parseQueryRequestBody(alloc, query_json);
            defer parsed_query.deinit();
            if (parsed_query.value.semantic_search != null) {
                if (self.call_count == 1) {
                    return .{
                        .json = try alloc.dupe(u8,
                            \\{"responses":[{"hits":{"hits":[]}}]}
                        ),
                    };
                }
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:semantic","_score":0.5,"_source":{"body":"semantic fallback"}}]}}]}
                    ),
                };
            }
            if (parsed_query.value.full_text_search != null and parsed_query.value.embeddings != null) {
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:hybrid","_score":1.0,"_source":{"body":"hybrid winner"}}]}}]}
                    ),
                };
            }
            return error.TestUnexpectedResult;
        }
    };

    var runner = FakeRunner{};
    const body =
        \\{"query":"architecture overview","stream":false,"max_internal_iterations":3,"steps":{"classification":{"enabled":true,"force_strategy":"simple","with_reasoning":true}},"queries":[{"table":"docs","semantic_search":"architecture overview","indexes":["semantic_idx"],"limit":5},{"table":"docs","full_text_search":{"query":"body:architecture"},"embeddings":{"dense_idx":[1.0,0.0,0.0]},"indexes":["dense_idx"],"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, runner.iface(), null, body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();

    try std.testing.expectEqual(@as(usize, 3), runner.call_count);
    try std.testing.expectEqual(RetrievalStrategy.hybrid, parsed.value.strategy_used.?);
    const selection_details = parsed.value.steps.?[1].details.?;
    try std.testing.expect(std.mem.eql(u8, selection_details.object.get("selection_source").?.string, "probe"));
    const candidate_scores = selection_details.object.get("candidate_scores").?.array.items;
    try std.testing.expect(candidate_scores.len == 2);
    try std.testing.expect(candidate_scores[0].object.get("probe_hits") != null);
    try std.testing.expect(candidate_scores[1].object.get("probe_hits") != null);
    var saw_probe_relevance = false;
    for (candidate_scores) |candidate| {
        if (candidate.object.get("probe_relevance") != null) {
            saw_probe_relevance = true;
            break;
        }
    }
    try std.testing.expect(saw_probe_relevance);
}

test "retrieval agent agentic mode evaluates misses and falls back to the next query" {
    const FakeRunner = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) QueryRunner {
            return .{
                .ptr = self,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            var parsed_query = try parseQueryRequestBody(alloc, query_json);
            defer parsed_query.deinit();
            if (parsed_query.value.full_text_search != null) {
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[]}}]}
                    ),
                };
            }
            if (parsed_query.value.filter_query != null) {
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"body":"fallback winner","status":"active"}}]}}]}
                    ),
                };
            }
            return error.TestUnexpectedResult;
        }
    };

    var runner = FakeRunner{};
    const body =
        \\{"query":"How does Raft consensus work in Antfly?","stream":false,"max_internal_iterations":3,"queries":[{"table":"docs","full_text_search":{"query":"body:missing"},"limit":5},{"table":"docs","filter_query":{"query":"status:active"},"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, runner.iface(), null, body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();

    try std.testing.expectEqual(@as(usize, 2), runner.call_count);
    try std.testing.expectEqual(@as(i64, 2), parsed.value.tool_calls_made.?);
    try std.testing.expectEqual(RetrievalStrategy.hybrid, parsed.value.strategy_used.?);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.hits.len);
    try std.testing.expectEqualStrings("doc:a", parsed.value.hits[0]._id);
    const evaluation_details = parsed.value.steps.?[3].details.?;
    const candidate_scores = evaluation_details.object.get("candidate_scores").?.array.items;
    try std.testing.expect(candidate_scores.len == 2);
    try std.testing.expect(candidate_scores[1].object.get("probe_hits") != null);

    var saw_evaluate = false;
    var saw_evaluation_selection = false;
    for (parsed.value.steps.?) |step| {
        if (std.mem.eql(u8, step.name, "evaluate")) saw_evaluate = true;
        if (std.mem.eql(u8, step.name, "select_strategy") and step.details != null and step.details.? == .object) {
            if (step.details.?.object.get("selection_source")) |selection_source| {
                if (selection_source == .string and std.mem.eql(u8, selection_source.string, "evaluation")) {
                    saw_evaluation_selection = true;
                }
            }
        }
    }
    try std.testing.expect(saw_evaluate);
    try std.testing.expect(saw_evaluation_selection);
}

test "retrieval agent agentic mode evaluates weak lexical hits and falls back to semantic" {
    const FakeRunner = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) QueryRunner {
            return .{
                .ptr = self,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            var parsed_query = try parseQueryRequestBody(alloc, query_json);
            defer parsed_query.deinit();
            if (parsed_query.value.full_text_search) |full_text| {
                if (self.call_count == 1) {
                    try expectFullTextQueryValue(full_text, "body:raft");
                } else {
                    try expectFullTextQueryValue(full_text, "raft");
                }
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:thin","_score":0.2,"_source":{"title":"raft","body":"raft"}}]}}]}
                    ),
                };
            }
            if (parsed_query.value.embeddings != null) {
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:semantic","_score":1.0,"_source":{"title":"Raft Consensus","body":"raft consensus architecture overview"}}]}}]}
                    ),
                };
            }
            return error.TestUnexpectedResult;
        }
    };

    var runner = FakeRunner{};
    const body =
        \\{"query":"How does raft consensus work in Antfly?","stream":false,"max_internal_iterations":3,"steps":{"classification":{"enabled":true,"force_strategy":"simple","with_reasoning":true}},"queries":[{"table":"docs","full_text_search":{"query":"body:raft"},"limit":5},{"table":"docs","embeddings":{"dense_idx":[1.0,0.0,0.0]},"indexes":["dense_idx"],"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, runner.iface(), null, body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();

    try std.testing.expectEqual(@as(usize, 3), runner.call_count);
    try std.testing.expectEqual(@as(i64, 3), parsed.value.tool_calls_made.?);
    try std.testing.expectEqual(RetrievalStrategy.hybrid, parsed.value.strategy_used.?);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.hits.len);

    var saw_weak_evaluate = false;
    var saw_refine_phase = false;
    for (parsed.value.steps.?) |step| {
        if (std.mem.eql(u8, step.name, "evaluate")) {
            if (step.details != null and step.details.? == .object) {
                if (step.details.?.object.get("trigger")) |trigger| {
                    if (trigger == .string and std.mem.eql(u8, trigger.string, "weak_result")) {
                        saw_weak_evaluate = true;
                    }
                }
            }
        } else if (std.mem.eql(u8, step.name, "refine_query")) {
            if (step.details != null and step.details.? == .object) {
                if (step.details.?.object.get("phase")) |phase| {
                    if (phase == .string and std.mem.eql(u8, phase.string, "evaluation_refine")) {
                        saw_refine_phase = true;
                    }
                }
            }
        }
    }
    try std.testing.expect(saw_weak_evaluate);
    try std.testing.expect(saw_refine_phase);
}

test "retrieval agent agentic mode evaluates weak multi-hit lexical results and falls back to semantic" {
    const FakeRunner = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) QueryRunner {
            return .{
                .ptr = self,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            var parsed_query = try parseQueryRequestBody(alloc, query_json);
            defer parsed_query.deinit();
            if (parsed_query.value.full_text_search) |full_text| {
                if (self.call_count == 1) {
                    try expectFullTextQueryValue(full_text, "body:raft");
                } else {
                    try expectFullTextQueryValue(full_text, "raft");
                }
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:thin","_score":0.4,"_source":{"title":"raft","body":"raft"}},{"_id":"doc:other","_score":0.3,"_source":{"title":"other","body":"raft note"}}]}}]}
                    ),
                };
            }
            if (parsed_query.value.embeddings != null) {
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:semantic","_score":1.0,"_source":{"title":"Raft Consensus","body":"raft consensus architecture overview"}}]}}]}
                    ),
                };
            }
            return error.TestUnexpectedResult;
        }
    };

    var runner = FakeRunner{};
    const body =
        \\{"query":"How does raft consensus work in Antfly?","stream":false,"max_internal_iterations":3,"steps":{"classification":{"enabled":true,"force_strategy":"simple","with_reasoning":true}},"queries":[{"table":"docs","full_text_search":{"query":"body:raft"},"limit":5},{"table":"docs","embeddings":{"dense_idx":[1.0,0.0,0.0]},"indexes":["dense_idx"],"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, runner.iface(), null, body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();

    try std.testing.expectEqual(@as(usize, 3), runner.call_count);
    try std.testing.expectEqual(@as(i64, 3), parsed.value.tool_calls_made.?);
    try std.testing.expectEqual(RetrievalStrategy.hybrid, parsed.value.strategy_used.?);
    try std.testing.expectEqual(@as(usize, 3), parsed.value.hits.len);

    var saw_weak_evaluate = false;
    var saw_refine_phase = false;
    for (parsed.value.steps.?) |step| {
        if (std.mem.eql(u8, step.name, "evaluate")) {
            if (step.details != null and step.details.? == .object) {
                if (step.details.?.object.get("trigger")) |trigger| {
                    if (trigger == .string and std.mem.eql(u8, trigger.string, "weak_result")) {
                        saw_weak_evaluate = true;
                    }
                }
            }
        } else if (std.mem.eql(u8, step.name, "refine_query")) {
            if (step.details != null and step.details.? == .object) {
                if (step.details.?.object.get("phase")) |phase| {
                    if (phase == .string and std.mem.eql(u8, phase.string, "evaluation_refine")) {
                        saw_refine_phase = true;
                    }
                }
            }
        }
    }
    try std.testing.expect(saw_weak_evaluate);
    try std.testing.expect(saw_refine_phase);
}

test "retrieval agent asks for clarification after ambiguous post-refinement fallback" {
    const FakeRunner = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) QueryRunner {
            return .{
                .ptr = self,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            var parsed_query = try parseQueryRequestBody(alloc, query_json);
            defer parsed_query.deinit();
            if (parsed_query.value.full_text_search != null and parsed_query.value.embeddings == null) {
                const full_text = parsed_query.value.full_text_search.?;
                if (self.call_count == 1) {
                    try expectFullTextQueryValue(full_text, "body:raft");
                } else {
                    try expectFullTextQueryValue(full_text, "raft");
                }
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:thin","_score":0.2,"_source":{"title":"raft","body":"raft"}}]}}]}
                    ),
                };
            }
            if (parsed_query.value.full_text_search != null and parsed_query.value.embeddings != null) {
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:hybrid","_score":0.9,"_source":{"title":"Raft Overview","body":"raft consensus architecture overview"}}]}}]}
                    ),
                };
            }
            if (parsed_query.value.embeddings != null) {
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:semantic","_score":0.9,"_source":{"title":"Raft Overview","body":"raft consensus architecture overview"}}]}}]}
                    ),
                };
            }
            return error.TestUnexpectedResult;
        }
    };

    var runner = FakeRunner{};
    const body =
        \\{"query":"How does raft consensus architecture work in Antfly clusters?","stream":false,"max_internal_iterations":4,"max_user_clarifications":1,"steps":{"classification":{"enabled":true,"force_strategy":"simple","with_reasoning":true}},"queries":[{"table":"docs","full_text_search":{"query":"body:raft"},"limit":5},{"table":"docs","embeddings":{"dense_idx":[1.0,0.0,0.0]},"indexes":["dense_idx"],"limit":5},{"table":"docs","full_text_search":{"query":"body:architecture"},"embeddings":{"dense_idx":[1.0,0.0,0.0]},"indexes":["dense_idx"],"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, runner.iface(), null, body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();

    try std.testing.expectEqual(@as(usize, 4), runner.call_count);
    try std.testing.expectEqual(AgentStatus.clarification_required, parsed.value.status);
    try std.testing.expectEqual(@as(i64, 2), parsed.value.tool_calls_made.?);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.hits.len);
    try std.testing.expect(parsed.value.questions != null);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.questions.?.len);
    try std.testing.expectEqualStrings("select_query", parsed.value.questions.?[0].id);
    try std.testing.expect(parsed.value.questions.?[0].options != null);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.questions.?[0].options.?.len);
    try std.testing.expectEqualStrings("1: semantic", parsed.value.questions.?[0].options.?[0]);
    try std.testing.expectEqualStrings("2: hybrid", parsed.value.questions.?[0].options.?[1]);

    var saw_evaluate_clarify = false;
    var saw_clarification_step = false;
    for (parsed.value.steps.?) |step| {
        if (std.mem.eql(u8, step.name, "evaluate")) {
            if (step.details != null and step.details.? == .object) {
                const planner_decision = step.details.?.object.get("planner_decision") orelse continue;
                const trigger = step.details.?.object.get("trigger") orelse continue;
                if (planner_decision == .string and trigger == .string and
                    std.mem.eql(u8, planner_decision.string, "clarify") and
                    std.mem.eql(u8, trigger.string, "weak_result"))
                {
                    saw_evaluate_clarify = true;
                }
            }
        } else if (std.mem.eql(u8, step.name, "clarification")) {
            saw_clarification_step = true;
        }
    }
    try std.testing.expect(saw_evaluate_clarify);
    try std.testing.expect(saw_clarification_step);
}

test "retrieval agent agentic mode refines partial semantic results before switching strategy" {
    const FakeRunner = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) QueryRunner {
            return .{
                .ptr = self,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            var parsed_query = try parseQueryRequestBody(alloc, query_json);
            defer parsed_query.deinit();
            if (parsed_query.value.semantic_search) |semantic_search| {
                if (self.call_count == 1) {
                    try std.testing.expectEqualStrings("antfly Explain the architecture of Antfly in detail", semantic_search);
                    return .{
                        .json = try alloc.dupe(u8,
                            \\{"responses":[{"hits":{"hits":[{"_id":"doc:thin","_score":0.7,"_source":{"body":"architecture"}},{"_id":"doc:other","_score":0.6,"_source":{"body":"overview"}}]}}]}
                        ),
                    };
                }
                try std.testing.expectEqualStrings("Explain the architecture of Antfly in detail", semantic_search);
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:semantic","_score":1.0,"_source":{"body":"Explain the architecture of Antfly in detail with cluster topology, storage roles, and retrieval planning."}}]}}]}
                    ),
                };
            }
            if (parsed_query.value.filter_query != null) return error.TestUnexpectedResult;
            return error.TestUnexpectedResult;
        }
    };

    var runner = FakeRunner{};
    const body =
        \\{"query":"Explain the architecture of Antfly in detail","stream":false,"max_internal_iterations":3,"steps":{"classification":{"enabled":true,"force_strategy":"simple","with_reasoning":true}},"queries":[{"table":"docs","semantic_search":"architecture overview","indexes":["semantic_idx"],"limit":5},{"table":"docs","filter_query":{"query":"status:active"},"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, runner.iface(), null, body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();

    try std.testing.expectEqual(@as(usize, 2), runner.call_count);
    try std.testing.expectEqual(@as(i64, 2), parsed.value.tool_calls_made.?);
    try std.testing.expectEqual(RetrievalStrategy.semantic, parsed.value.strategy_used.?);

    var saw_evaluate_refine = false;
    var saw_refine_phase = false;
    for (parsed.value.steps.?) |step| {
        if (std.mem.eql(u8, step.name, "evaluate") and step.details != null and step.details.? == .object) {
            if (step.details.?.object.get("planner_decision")) |planner_decision| {
                if (planner_decision == .string and std.mem.eql(u8, planner_decision.string, "refine_query")) {
                    saw_evaluate_refine = true;
                }
            }
        }
        if (std.mem.eql(u8, step.name, "refine_query") and step.details != null and step.details.? == .object) {
            if (step.details.?.object.get("phase")) |phase| {
                if (phase == .string and std.mem.eql(u8, phase.string, "evaluation_refine")) {
                    saw_refine_phase = true;
                }
            }
        }
    }
    try std.testing.expect(saw_evaluate_refine);
    try std.testing.expect(saw_refine_phase);
}

test "retrieval agent can clarify after ambiguous partial semantic refinement" {
    const FakeRunner = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) QueryRunner {
            return .{
                .ptr = self,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            var parsed_query = try parseQueryRequestBody(alloc, query_json);
            defer parsed_query.deinit();
            if (parsed_query.value.semantic_search) |semantic_search| {
                if (self.call_count == 1) {
                    try std.testing.expectEqualStrings("antfly Explain the architecture of Antfly in detail", semantic_search);
                } else {
                    try std.testing.expectEqualStrings("Explain the architecture of Antfly in detail", semantic_search);
                }
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:thin","_score":0.7,"_source":{"body":"architecture"}},{"_id":"doc:other","_score":0.6,"_source":{"body":"overview"}}]}}]}
                    ),
                };
            }
            if (parsed_query.value.full_text_search != null and parsed_query.value.embeddings != null) {
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:hybrid","_score":0.8,"_source":{"body":"architecture overview"}}]}}]}
                    ),
                };
            }
            if (parsed_query.value.embeddings != null) {
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:semantic_fallback","_score":0.8,"_source":{"body":"architecture overview"}}]}}]}
                    ),
                };
            }
            return error.TestUnexpectedResult;
        }
    };

    var runner = FakeRunner{};
    const body =
        \\{"query":"Explain the architecture of Antfly in detail","stream":false,"max_internal_iterations":4,"max_user_clarifications":1,"steps":{"classification":{"enabled":true,"force_strategy":"simple","with_reasoning":true}},"queries":[{"table":"docs","semantic_search":"architecture overview","indexes":["semantic_idx"],"limit":5},{"table":"docs","embeddings":{"dense_idx":[1.0,0.0,0.0]},"indexes":["dense_idx"],"limit":5},{"table":"docs","full_text_search":{"query":"body:architecture"},"embeddings":{"dense_idx":[1.0,0.0,0.0]},"indexes":["dense_idx"],"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, runner.iface(), null, body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();

    try std.testing.expectEqual(@as(usize, 4), runner.call_count);
    try std.testing.expectEqual(AgentStatus.clarification_required, parsed.value.status);
    try std.testing.expectEqual(@as(i64, 2), parsed.value.tool_calls_made.?);
    try std.testing.expect(parsed.value.questions != null);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.questions.?.len);
    try std.testing.expectEqualStrings("select_query", parsed.value.questions.?[0].id);
    try std.testing.expect(parsed.value.questions.?[0].options != null);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.questions.?[0].options.?.len);
    try std.testing.expectEqualStrings("1: semantic", parsed.value.questions.?[0].options.?[0]);
    try std.testing.expectEqualStrings("2: hybrid", parsed.value.questions.?[0].options.?[1]);

    var saw_partial_clarify = false;
    for (parsed.value.steps.?) |step| {
        if (!std.mem.eql(u8, step.name, "evaluate")) continue;
        if (step.details == null or step.details.? != .object) continue;
        const planner_decision = step.details.?.object.get("planner_decision") orelse continue;
        const trigger = step.details.?.object.get("trigger") orelse continue;
        if (planner_decision == .string and trigger == .string and
            std.mem.eql(u8, planner_decision.string, "clarify") and
            std.mem.eql(u8, trigger.string, "partial_result"))
        {
            saw_partial_clarify = true;
            break;
        }
    }
    try std.testing.expect(saw_partial_clarify);
}

test "retrieval agent can keep refined partial semantic result when fallback is weaker" {
    const FakeRunner = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) QueryRunner {
            return .{
                .ptr = self,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            var parsed_query = try parseQueryRequestBody(alloc, query_json);
            defer parsed_query.deinit();
            if (parsed_query.value.semantic_search != null) {
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:semantic","_score":0.92,"_source":{"body":"Explain the architecture of Antfly in detail with storage roles and retrieval planning."}},{"_id":"doc:semantic-2","_score":0.80,"_source":{"body":"Antfly architecture overview with cluster storage routing details."}}]}}]}
                    ),
                };
            }
            if (parsed_query.value.full_text_search != null and parsed_query.value.embeddings != null) {
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:hybrid","_score":0.55,"_source":{"body":"architecture notes"}}]}}]}
                    ),
                };
            }
            if (parsed_query.value.embeddings != null) {
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:fallback","_score":0.54,"_source":{"body":"architecture summary"}}]}}]}
                    ),
                };
            }
            return error.TestUnexpectedResult;
        }
    };

    var runner = FakeRunner{};
    const body =
        \\{"query":"Explain the architecture of Antfly in detail","stream":false,"max_internal_iterations":4,"steps":{"classification":{"enabled":true,"force_strategy":"simple","with_reasoning":true}},"queries":[{"table":"docs","semantic_search":"architecture overview","indexes":["semantic_idx"],"limit":5},{"table":"docs","embeddings":{"dense_idx":[1.0,0.0,0.0]},"indexes":["dense_idx"],"limit":5},{"table":"docs","full_text_search":{"query":"body:architecture"},"embeddings":{"dense_idx":[1.0,0.0,0.0]},"indexes":["dense_idx"],"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, runner.iface(), null, body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();

    try std.testing.expectEqual(AgentStatus.completed, parsed.value.status);
    try std.testing.expectEqual(RetrievalStrategy.semantic, parsed.value.strategy_used.?);
    try std.testing.expectEqual(@as(i64, 2), parsed.value.tool_calls_made.?);

    var saw_accept = false;
    var saw_switch = false;
    for (parsed.value.steps.?) |step| {
        if (!std.mem.eql(u8, step.name, "evaluate")) continue;
        if (step.details == null or step.details.? != .object) continue;
        const planner_decision = step.details.?.object.get("planner_decision") orelse continue;
        if (planner_decision != .string) continue;
        if (std.mem.eql(u8, planner_decision.string, "accept_result")) saw_accept = true;
        if (std.mem.eql(u8, planner_decision.string, "switch_strategy")) saw_switch = true;
    }
    try std.testing.expect(saw_accept);
    try std.testing.expect(!saw_switch);
}

test "retrieval agent agentic mode uses multiple tools for decompose queries" {
    const FakeRunner = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) QueryRunner {
            return .{
                .ptr = self,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            var parsed_query = try parseQueryRequestBody(alloc, query_json);
            defer parsed_query.deinit();
            if (self.call_count == 1) {
                try std.testing.expect(parsed_query.value.full_text_search != null);
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"body":"raft consensus"}}]}}]}
                    ),
                };
            }
            try std.testing.expect(parsed_query.value.filter_query != null);
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"hits":{"hits":[{"_id":"doc:b","_score":1.0,"_source":{"status":"active"}}]}}]}
                ),
            };
        }
    };

    var runner = FakeRunner{};
    const body =
        \\{"query":"Compare raft consensus and active document status","stream":false,"max_internal_iterations":3,"queries":[{"table":"docs","full_text_search":{"query":"body:raft"},"limit":5},{"table":"docs","filter_query":{"query":"status:active"},"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, runner.iface(), null, body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 2), runner.call_count);
    try std.testing.expectEqual(@as(i64, 2), parsed.value.tool_calls_made.?);
    try std.testing.expectEqual(RetrievalStrategy.hybrid, parsed.value.strategy_used.?);
    try std.testing.expectEqual(ai_openapi.QueryStrategy.decompose, parsed.value.classification.?.strategy);
}

test "retrieval agent refines decompose queries before execution" {
    const FakeRunner = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) QueryRunner {
            return .{
                .ptr = self,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            var parsed_query = try parseQueryRequestBody(alloc, query_json);
            defer parsed_query.deinit();
            const full_text = parsed_query.value.full_text_search.?;
            if (self.call_count == 1) {
                try expectFullTextQueryValue(full_text, "Compare raft consensus?");
            } else {
                try expectFullTextQueryValue(full_text, "active document status?");
            }
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"body":"match"}}]}}]}
                ),
            };
        }
    };

    var runner = FakeRunner{};
    const body =
        \\{"query":"Compare raft consensus and active document status","stream":false,"max_internal_iterations":3,"queries":[{"table":"docs","full_text_search":{"query":"body:placeholder"},"limit":5},{"table":"docs","full_text_search":{"query":"body:placeholder"},"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, runner.iface(), null, body);
    defer std.testing.allocator.free(encoded);

    try std.testing.expectEqual(@as(usize, 2), runner.call_count);
}

test "retrieval agent refines step-back semantic queries before execution" {
    const FakeRunner = struct {
        fn iface() QueryRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            var parsed_query = try parseQueryRequestBody(alloc, query_json);
            defer parsed_query.deinit();
            try std.testing.expectEqualStrings("Background context and core Antfly concepts needed for: How does retrieval work?", parsed_query.value.semantic_search.?);
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"body":"match"}}]}}]}
                ),
            };
        }
    };

    const body =
        \\{"query":"How does retrieval work?","stream":false,"max_internal_iterations":3,"queries":[{"table":"docs","semantic_search":"placeholder","indexes":["semantic_idx"],"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, FakeRunner.iface(), null, body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    var found = false;
    for (parsed.value.steps.?) |step| {
        if (!std.mem.eql(u8, step.name, "refine_query")) continue;
        const details = step.details orelse continue;
        const phase = details.object.get("phase") orelse continue;
        if (phase == .string and std.mem.eql(u8, phase.string, "step_back_initial")) {
            found = true;
            break;
        }
    }
    try std.testing.expect(found);
}

test "retrieval agent can require clarification before bounded agentic execution" {
    const FakeRunner = struct {
        fn iface() QueryRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8) !query_api.QueryResponse {
            return error.TestUnexpectedResult;
        }
    };

    const body =
        \\{"query":"How does Raft consensus work in Antfly?","stream":false,"max_internal_iterations":3,"max_user_clarifications":1,"require_decision_after":0,"queries":[{"table":"docs","full_text_search":{"query":"body:raft"},"limit":5},{"table":"docs","filter_query":{"query":"status:active"},"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, FakeRunner.iface(), null, body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(AgentStatus.clarification_required, parsed.value.status);
    try std.testing.expectEqual(@as(i64, 1), parsed.value.remaining_user_clarifications.?);
    try std.testing.expect(parsed.value.questions != null);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.questions.?.len);
    try std.testing.expectEqualStrings("select_query", parsed.value.questions.?[0].id);
    try std.testing.expect(parsed.value.steps.?[1].details.?.object.get("candidate_scores").?.array.items.len == 2);
}

test "retrieval agent reports incomplete when a decision is required but clarifications are disabled" {
    const FakeRunner = struct {
        fn iface() QueryRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8) !query_api.QueryResponse {
            return error.TestUnexpectedResult;
        }
    };

    const body =
        \\{"query":"How does Raft consensus work in Antfly?","stream":false,"interactive":false,"max_internal_iterations":3,"require_decision_after":0,"queries":[{"table":"docs","full_text_search":{"query":"body:raft"},"limit":5},{"table":"docs","filter_query":{"query":"status:active"},"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, FakeRunner.iface(), null, body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(AgentStatus.incomplete, parsed.value.status);
    try std.testing.expect(parsed.value.incomplete_details != null);
    try std.testing.expectEqualStrings("clarification_required", parsed.value.incomplete_details.?.reason);
}

test "retrieval agent can continue from a decision" {
    const FakeRunner = struct {
        fn iface() QueryRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            var parsed_query = try parseQueryRequestBody(alloc, query_json);
            defer parsed_query.deinit();
            try std.testing.expect(parsed_query.value.full_text_search != null);
            try std.testing.expect(parsed_query.value.filter_query == null);
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"body":"raft consensus in antfly"}}]}}]}
                ),
            };
        }
    };

    const body =
        \\{"query":"How does Raft consensus work in Antfly?","stream":false,"session_id":"retrieval-session","max_internal_iterations":3,"max_user_clarifications":1,"require_decision_after":0,"decisions":[{"question_id":"select_query","answer":0}],"queries":[{"table":"docs","full_text_search":{"query":"body:raft"},"limit":5},{"table":"docs","filter_query":{"query":"status:active"},"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, FakeRunner.iface(), null, body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(AgentStatus.completed, parsed.value.status);
    try std.testing.expectEqual(@as(i64, 1), parsed.value.clarification_count.?);
    try std.testing.expectEqual(@as(i64, 0), parsed.value.remaining_user_clarifications.?);
    try std.testing.expectEqual(@as(i64, 1), parsed.value.tool_calls_made.?);
    try std.testing.expectEqual(RetrievalStrategy.bm25, parsed.value.strategy_used.?);
    try std.testing.expect(std.mem.eql(u8, parsed.value.steps.?[1].details.?.object.get("selection_source").?.string, "user_decision"));
}

test "retrieval agent can ask to broaden after a user-selected query misses" {
    const FakeRunner = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) QueryRunner {
            return .{
                .ptr = self,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            var parsed_query = try parseQueryRequestBody(alloc, query_json);
            defer parsed_query.deinit();
            try std.testing.expect(parsed_query.value.full_text_search != null);
            return .{
                .json = try alloc.dupe(u8, "{\"responses\":[{\"hits\":{\"hits\":[]}}]}"),
            };
        }
    };

    var runner = FakeRunner{};
    const body =
        \\{"query":"How does Raft consensus work in Antfly?","stream":false,"session_id":"retrieval-broaden-session","max_internal_iterations":3,"max_user_clarifications":2,"decisions":[{"question_id":"select_query","answer":0}],"queries":[{"table":"docs","full_text_search":{"query":"body:raft"},"limit":5},{"table":"docs","filter_query":{"query":"status:active"},"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, runner.iface(), null, body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), runner.call_count);
    try std.testing.expectEqual(AgentStatus.clarification_required, parsed.value.status);
    try std.testing.expectEqualStrings("broaden_search", parsed.value.questions.?[0].id);
}

test "retrieval agent can broaden after user approval" {
    const FakeRunner = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) QueryRunner {
            return .{
                .ptr = self,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            var parsed_query = try parseQueryRequestBody(alloc, query_json);
            defer parsed_query.deinit();
            if (self.call_count == 1) {
                try std.testing.expect(parsed_query.value.full_text_search != null);
                return .{
                    .json = try alloc.dupe(u8, "{\"responses\":[{\"hits\":{\"hits\":[]}}]}"),
                };
            }
            try std.testing.expect(parsed_query.value.filter_query != null);
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"status":"active"}}]}}]}
                ),
            };
        }
    };

    var runner = FakeRunner{};
    const body =
        \\{"query":"How does Raft consensus work in Antfly?","stream":false,"session_id":"retrieval-broaden-session","max_internal_iterations":3,"max_user_clarifications":2,"decisions":[{"question_id":"select_query","answer":0},{"question_id":"broaden_search","approved":true}],"queries":[{"table":"docs","full_text_search":{"query":"body:raft"},"limit":5},{"table":"docs","filter_query":{"query":"status:active"},"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, runner.iface(), null, body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 2), runner.call_count);
    try std.testing.expectEqual(AgentStatus.completed, parsed.value.status);
    try std.testing.expectEqual(@as(i64, 2), parsed.value.tool_calls_made.?);
    try std.testing.expectEqual(RetrievalStrategy.hybrid, parsed.value.strategy_used.?);
}

test "retrieval agent supports generation step in phase 2" {
    const FakeRunner = struct {
        fn iface() QueryRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const u8) !query_api.QueryResponse {
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"content":"alpha body"}}]}}]}
                ),
            };
        }
    };

    const FakeGeneration = struct {
        fn iface() GenerationRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(_: *anyopaque, alloc: std.mem.Allocator, chain: []const generating.ChainLink, messages: []const generating.ChatMessage) !generating.GenerateResult {
            try std.testing.expectEqual(@as(usize, 1), chain.len);
            try std.testing.expectEqualStrings("local-generator", chain[0].generator.model);
            try std.testing.expect(std.mem.indexOf(u8, messages[1].content, "doc:a") != null);
            return .{
                .content = try alloc.dupe(u8, "Generated answer citing doc:a"),
                .allocator = alloc,
            };
        }
    };

    const body =
        \\{"query":"find alpha","stream":false,"generator":{"provider":"termite","model":"local-generator","api_url":"http://127.0.0.1:8082"},"steps":{"generation":{"enabled":true}},"queries":[{"table":"docs","semantic_search":"alpha concept","indexes":["semantic_idx"],"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, FakeRunner.iface(), FakeGeneration.iface(), body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("Generated answer citing doc:a", parsed.value.generation.?);
    try std.testing.expectEqualStrings("local-generator", parsed.value.model.?);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.steps.?.len);
}

test "retrieval agent event sink receives live milestones" {
    const FakeRunner = struct {
        fn iface() QueryRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const u8) !query_api.QueryResponse {
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"content":"alpha body"}}]}}]}
                ),
            };
        }
    };

    const Sink = struct {
        names: std.ArrayListUnmanaged([]const u8) = .empty,

        fn iface(self: *@This()) EventSink {
            return .{ .ptr = self, .emit_json_fn = emitJson };
        }

        fn emitJson(ptr: *anyopaque, alloc: std.mem.Allocator, event_name: []const u8, json: []const u8) !void {
            _ = json;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.names.append(alloc, try alloc.dupe(u8, event_name));
        }

        fn count(self: *@This(), name: []const u8) usize {
            var total: usize = 0;
            for (self.names.items) |event_name| {
                if (std.mem.eql(u8, event_name, name)) total += 1;
            }
            return total;
        }

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            for (self.names.items) |event_name| alloc.free(event_name);
            self.names.deinit(alloc);
        }
    };

    const body =
        \\{"query":"find alpha","stream":false,"queries":[{"table":"docs","semantic_search":"alpha concept","indexes":["semantic_idx"],"limit":5}]}
    ;
    var sink = Sink{};
    defer sink.deinit(std.testing.allocator);
    const encoded = try executeWithEventSink(std.testing.allocator, FakeRunner.iface(), null, body, sink.iface());
    defer std.testing.allocator.free(encoded.body);

    try std.testing.expectEqualStrings("application/json", encoded.content_type);
    try std.testing.expect(sink.count("step_started") >= 1);
    try std.testing.expect(sink.count("hit") >= 1);
    try std.testing.expectEqual(@as(usize, 1), sink.count("done"));
}

test "retrieval agent supports classification confidence and followup" {
    const FakeRunner = struct {
        fn iface() QueryRunner {
            return .{ .ptr = undefined, .vtable = &.{ .run_query = runQuery } };
        }

        fn runQuery(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const u8) !query_api.QueryResponse {
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"content":"alpha body"}}]}}]}
                ),
            };
        }
    };

    const FakeGeneration = struct {
        fn iface() GenerationRunner {
            return .{ .ptr = undefined, .vtable = &.{ .execute_chain = executeChain } };
        }

        fn executeChain(_: *anyopaque, alloc: std.mem.Allocator, _: []const generating.ChainLink, _: []const generating.ChatMessage) !generating.GenerateResult {
            return .{
                .content = try alloc.dupe(u8, "Generated answer citing doc:a"),
                .allocator = alloc,
            };
        }
    };

    const body =
        \\{"query":"How does retrieval work?","stream":false,"generator":{"provider":"termite","model":"local-generator","api_url":"http://127.0.0.1:8082"},"steps":{"classification":{"enabled":true,"with_reasoning":true},"generation":{"enabled":true},"confidence":{"enabled":true},"followup":{"enabled":true,"count":3}},"queries":[{"table":"docs","semantic_search":"retrieval docs","indexes":["semantic_idx"],"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, FakeRunner.iface(), FakeGeneration.iface(), body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.classification != null);
    try std.testing.expectEqual(ai_openapi.RouteType.question, parsed.value.classification.?.route_type);
    try std.testing.expectEqual(ai_openapi.QueryStrategy.step_back, parsed.value.classification.?.strategy);
    try std.testing.expect(parsed.value.classification.?.step_back_query != null);
    try std.testing.expect(parsed.value.classification.?.multi_phrases != null);
    try std.testing.expect(parsed.value.classification.?.reasoning != null);
    try std.testing.expect(parsed.value.generation_confidence != null);
    try std.testing.expect(parsed.value.context_relevance != null);
    try std.testing.expect(parsed.value.followup_questions != null);
    try std.testing.expectEqual(@as(usize, 3), parsed.value.followup_questions.?.len);
    try std.testing.expectEqual(@as(usize, 3), parsed.value.steps.?.len);
}

test "retrieval agent supports inline eval" {
    const FakeRunner = struct {
        fn iface() QueryRunner {
            return .{ .ptr = undefined, .vtable = &.{ .run_query = runQuery } };
        }

        fn runQuery(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const u8) !query_api.QueryResponse {
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"content":"raft consensus leader follower log replication"}}]}}]}
                ),
            };
        }
    };

    const FakeGeneration = struct {
        fn iface() GenerationRunner {
            return .{ .ptr = undefined, .vtable = &.{ .execute_chain = executeChain } };
        }

        fn executeChain(_: *anyopaque, alloc: std.mem.Allocator, _: []const generating.ChainLink, _: []const generating.ChatMessage) !generating.GenerateResult {
            return .{
                .content = try alloc.dupe(u8, "Generated answer citing doc:a and mentioning raft consensus leader follower log replication."),
                .allocator = alloc,
            };
        }
    };

    const body =
        \\{"query":"Explain raft consensus in Antfly","stream":false,"generator":{"provider":"termite","model":"local-generator","api_url":"http://127.0.0.1:8082"},"steps":{"generation":{"enabled":true},"eval":{"evaluators":["relevance","faithfulness","precision","recall"],"judge":{"provider":"termite","model":"judge","api_url":"http://127.0.0.1:8082"},"ground_truth":{"relevant_ids":["doc:a"],"expectations":"raft consensus leader follower log replication"}}},"queries":[{"table":"docs","semantic_search":"raft consensus","indexes":["semantic_idx"],"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, FakeRunner.iface(), FakeGeneration.iface(), body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.eval_result != null);
    try std.testing.expect(parsed.value.eval_result.?.scores != null);
    try std.testing.expect(parsed.value.eval_result.?.scores.?.retrieval != null);
    try std.testing.expect(parsed.value.eval_result.?.scores.?.generation != null);
    try std.testing.expect(parsed.value.eval_result.?.summary != null);
    try std.testing.expectEqual(@as(i64, 4), parsed.value.eval_result.?.summary.?.total.?);
    try std.testing.expectEqualStrings("eval", parsed.value.steps.?[2].name);
}

test "retrieval agent classification can decompose multi-part queries" {
    const FakeRunner = struct {
        fn iface() QueryRunner {
            return .{ .ptr = undefined, .vtable = &.{ .run_query = runQuery } };
        }

        fn runQuery(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const u8) !query_api.QueryResponse {
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"content":"alpha body"}}]}}]}
                ),
            };
        }
    };

    const body =
        \\{"query":"Compare raft consensus and termite embeddings","stream":false,"steps":{"classification":{"enabled":true,"with_reasoning":true}},"queries":[{"table":"docs","full_text_search":{"query":"body:raft"},"limit":5}]}
    ;
    const encoded = try executeJson(std.testing.allocator, FakeRunner.iface(), null, body);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(RetrievalAgentResult, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.classification != null);
    try std.testing.expectEqual(ai_openapi.QueryStrategy.decompose, parsed.value.classification.?.strategy);
    try std.testing.expect(parsed.value.classification.?.sub_questions != null);
    try std.testing.expect(parsed.value.classification.?.sub_questions.?.len >= 2);
}

test "retrieval agent supports fixed-body sse streaming" {
    const FakeRunner = struct {
        fn iface() QueryRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const u8) !query_api.QueryResponse {
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"content":"alpha body"}}]}}]}
                ),
            };
        }
    };

    const FakeGeneration = struct {
        fn iface() GenerationRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(_: *anyopaque, alloc: std.mem.Allocator, _: []const generating.ChainLink, _: []const generating.ChatMessage) !generating.GenerateResult {
            return .{
                .content = try alloc.dupe(u8, "Generated answer citing doc:a with additional supporting detail that is long enough to require multiple streamed generation chunks."),
                .allocator = alloc,
            };
        }
    };

    const body =
        \\{"query":"find alpha","stream":true,"generator":{"provider":"termite","model":"local-generator","api_url":"http://127.0.0.1:8082"},"steps":{"generation":{"enabled":true}},"queries":[{"table":"docs","semantic_search":"alpha concept","indexes":["semantic_idx"],"limit":5}]}
    ;
    const encoded = try execute(std.testing.allocator, FakeRunner.iface(), FakeGeneration.iface(), body);
    defer std.testing.allocator.free(encoded.body);
    const events = try parseSseEventsAlloc(std.testing.allocator, encoded.body);
    defer std.testing.allocator.free(events);

    try std.testing.expectEqualStrings("text/event-stream", encoded.content_type);
    try std.testing.expect(countSseEvents(events, "step_started") >= 1);
    try std.testing.expect(countSseEvents(events, "hit") >= 1);
    try std.testing.expect(countSseEvents(events, "generation") >= 2);
    try std.testing.expect(countSseEvents(events, "done") >= 1);
    var parsed_hit = try parseJsonBody(QueryHit, std.testing.allocator, firstSseEventData(events, "hit").?);
    defer parsed_hit.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_hit.value._id);
    var parsed_generation = try parseJsonBody([]const u8, std.testing.allocator, firstSseEventData(events, "generation").?);
    defer parsed_generation.deinit();
    try std.testing.expect(std.mem.indexOf(u8, parsed_generation.value, "Generated answer citing doc:a") != null);
    var parsed_done = try parseJsonBody(RetrievalAgentResult, std.testing.allocator, firstSseEventData(events, "done").?);
    defer parsed_done.deinit();
    try std.testing.expect(std.mem.indexOf(u8, parsed_done.value.generation.?, "Generated answer citing doc:a") != null);
}

test "retrieval agent sse emits followup events" {
    const FakeRunner = struct {
        fn iface() QueryRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const u8) !query_api.QueryResponse {
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"content":"alpha body"}}]}}]}
                ),
            };
        }
    };

    const FakeGeneration = struct {
        fn iface() GenerationRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .execute_chain = executeChain },
            };
        }

        fn executeChain(_: *anyopaque, alloc: std.mem.Allocator, _: []const generating.ChainLink, _: []const generating.ChatMessage) !generating.GenerateResult {
            return .{
                .content = try alloc.dupe(u8, "Generated answer citing doc:a"),
                .allocator = alloc,
            };
        }
    };

    const body =
        \\{"query":"How does retrieval work?","stream":true,"generator":{"provider":"termite","model":"local-generator","api_url":"http://127.0.0.1:8082"},"steps":{"generation":{"enabled":true},"followup":{"enabled":true,"count":2}},"queries":[{"table":"docs","semantic_search":"retrieval docs","indexes":["semantic_idx"],"limit":5}]}
    ;
    const encoded = try execute(std.testing.allocator, FakeRunner.iface(), FakeGeneration.iface(), body);
    defer std.testing.allocator.free(encoded.body);
    const events = try parseSseEventsAlloc(std.testing.allocator, encoded.body);
    defer std.testing.allocator.free(events);
    var parsed_followup = try parseJsonBody([]const u8, std.testing.allocator, firstSseEventData(events, "followup").?);
    defer parsed_followup.deinit();
    try std.testing.expectEqualStrings("What else should I know about: How does retrieval work?", parsed_followup.value);
}

test "retrieval agent sse emits eval events" {
    const FakeRunner = struct {
        fn iface() QueryRunner {
            return .{ .ptr = undefined, .vtable = &.{ .run_query = runQuery } };
        }

        fn runQuery(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const u8) !query_api.QueryResponse {
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"content":"raft consensus leader follower"}}]}}]}
                ),
            };
        }
    };

    const FakeGeneration = struct {
        fn iface() GenerationRunner {
            return .{ .ptr = undefined, .vtable = &.{ .execute_chain = executeChain } };
        }

        fn executeChain(_: *anyopaque, alloc: std.mem.Allocator, _: []const generating.ChainLink, _: []const generating.ChatMessage) !generating.GenerateResult {
            return .{
                .content = try alloc.dupe(u8, "Generated answer citing doc:a."),
                .allocator = alloc,
            };
        }
    };

    const body =
        \\{"query":"Explain raft consensus in Antfly","stream":true,"generator":{"provider":"termite","model":"local-generator","api_url":"http://127.0.0.1:8082"},"steps":{"generation":{"enabled":true},"eval":{"evaluators":["relevance","faithfulness"],"ground_truth":{"expectations":"raft consensus"}}},"queries":[{"table":"docs","semantic_search":"raft consensus","indexes":["semantic_idx"],"limit":5}]}
    ;
    const encoded = try execute(std.testing.allocator, FakeRunner.iface(), FakeGeneration.iface(), body);
    defer std.testing.allocator.free(encoded.body);
    const events = try parseSseEventsAlloc(std.testing.allocator, encoded.body);
    defer std.testing.allocator.free(events);
    var parsed_eval = try parseJsonBody(TestStepProgressEvent, std.testing.allocator, firstSseEventData(events, "eval").?);
    defer parsed_eval.deinit();
    try std.testing.expect(parsed_eval.value.generation != null);
}

test "retrieval agent sse encodes clarification through step events" {
    const FakeRunner = struct {
        fn iface() QueryRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8) !query_api.QueryResponse {
            return error.TestUnexpectedResult;
        }
    };

    const body =
        \\{"query":"How does Raft consensus work in Antfly?","stream":true,"max_internal_iterations":3,"max_user_clarifications":1,"require_decision_after":0,"queries":[{"table":"docs","full_text_search":{"query":"body:raft"},"limit":5},{"table":"docs","filter_query":{"query":"status:active"},"limit":5}]}
    ;
    const encoded = try execute(std.testing.allocator, FakeRunner.iface(), null, body);
    defer std.testing.allocator.free(encoded.body);
    const events = try parseSseEventsAlloc(std.testing.allocator, encoded.body);
    defer std.testing.allocator.free(events);

    try std.testing.expectEqualStrings("text/event-stream", encoded.content_type);
    try std.testing.expect(firstSseEventData(events, "clarification") == null);
    try std.testing.expect(countSseEvents(events, "reasoning") >= 1);
    try std.testing.expect(countSseEvents(events, "step_started") >= 1);
    try std.testing.expect(countSseEvents(events, "step_completed") >= 1);
    try std.testing.expect(countSseEvents(events, "done") >= 1);
    var saw_clarification = false;
    for (events) |event| {
        if (!(std.mem.eql(u8, event.event, "step_started") or std.mem.eql(u8, event.event, "step_progress") or std.mem.eql(u8, event.event, "step_completed"))) continue;
        var parsed_progress = try parseJsonBody(TestStepProgressEvent, std.testing.allocator, event.data);
        defer parsed_progress.deinit();
        if (!std.mem.eql(u8, parsed_progress.value.phase, "clarification")) continue;
        saw_clarification = true;
        if (parsed_progress.value.questions) |questions| {
            try std.testing.expectEqualStrings("select_query", questions[0].id);
        } else if (parsed_progress.value.id) |id| {
            try std.testing.expectEqualStrings("select_query", id);
        }
    }
    try std.testing.expect(saw_clarification);
}

test "retrieval agent sse emits decomposition progress" {
    const FakeRunner = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) QueryRunner {
            return .{
                .ptr = self,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const u8) !query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            if (self.call_count == 1) {
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"body":"raft consensus"}}]}}]}
                    ),
                };
            }
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"hits":{"hits":[{"_id":"doc:b","_score":1.0,"_source":{"status":"active"}}]}}]}
                ),
            };
        }
    };

    var runner = FakeRunner{};
    const body =
        \\{"query":"Compare raft consensus and active document status","stream":true,"max_internal_iterations":3,"queries":[{"table":"docs","full_text_search":{"query":"body:raft"},"limit":5},{"table":"docs","filter_query":{"query":"status:active"},"limit":5}]}
    ;
    const encoded = try execute(std.testing.allocator, runner.iface(), null, body);
    defer std.testing.allocator.free(encoded.body);
    const events = try parseSseEventsAlloc(std.testing.allocator, encoded.body);
    defer std.testing.allocator.free(events);
    try std.testing.expect(countSseEvents(events, "classification") >= 1);
    var saw_decompose = false;
    var saw_tool_call = false;
    for (events) |event| {
        if (!std.mem.eql(u8, event.event, "step_progress")) continue;
        var parsed_progress = try parseJsonBody(TestStepProgressEvent, std.testing.allocator, event.data);
        defer parsed_progress.deinit();
        if (std.mem.eql(u8, parsed_progress.value.phase, "decompose")) {
            saw_decompose = true;
            try std.testing.expect(parsed_progress.value.sub_question != null);
        }
        if (std.mem.eql(u8, parsed_progress.value.phase, "tool_call")) saw_tool_call = true;
    }
    try std.testing.expect(saw_decompose);
    try std.testing.expect(saw_tool_call);
}

test "retrieval agent sse emits probe progress for ambiguous agentic selection" {
    const FakeRunner = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) QueryRunner {
            return .{
                .ptr = self,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            var parsed_query = try parseQueryRequestBody(alloc, query_json);
            defer parsed_query.deinit();
            if (parsed_query.value.semantic_search != null) {
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[]}}]}
                    ),
                };
            }
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"hits":{"hits":[{"_id":"doc:hybrid","_score":1.0,"_source":{"body":"hybrid winner"}}]}}]}
                ),
            };
        }
    };

    var runner = FakeRunner{};
    const body =
        \\{"query":"architecture overview","stream":true,"max_internal_iterations":3,"steps":{"classification":{"enabled":true,"force_strategy":"simple","with_reasoning":true}},"queries":[{"table":"docs","semantic_search":"architecture overview","indexes":["semantic_idx"],"limit":5},{"table":"docs","full_text_search":{"query":"body:architecture"},"embeddings":{"dense_idx":[1.0,0.0,0.0]},"indexes":["dense_idx"],"limit":5}]}
    ;
    const encoded = try execute(std.testing.allocator, runner.iface(), null, body);
    defer std.testing.allocator.free(encoded.body);
    const events = try parseSseEventsAlloc(std.testing.allocator, encoded.body);
    defer std.testing.allocator.free(events);
    try std.testing.expect(countSseEvents(events, "reasoning") >= 1);
    var saw_probe = false;
    for (events) |event| {
        if (!std.mem.eql(u8, event.event, "step_progress")) continue;
        var parsed_progress = try parseJsonBody(TestStepProgressEvent, std.testing.allocator, event.data);
        defer parsed_progress.deinit();
        if (!std.mem.eql(u8, parsed_progress.value.phase, "probe")) continue;
        saw_probe = true;
        try std.testing.expectEqualStrings("probe", parsed_progress.value.selection_source.?);
        try std.testing.expect(parsed_progress.value.probe_relevance != null);
        try std.testing.expect(parsed_progress.value.id != null);
        try std.testing.expectEqualStrings("planning", parsed_progress.value.kind.?);
    }
    try std.testing.expect(saw_probe);
}

test "retrieval agent sse emits evaluation progress for fallback planning" {
    const FakeRunner = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) QueryRunner {
            return .{
                .ptr = self,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            var parsed_query = try parseQueryRequestBody(alloc, query_json);
            defer parsed_query.deinit();
            if (parsed_query.value.full_text_search != null) {
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[]}}]}
                    ),
                };
            }
            return .{
                .json = try alloc.dupe(u8,
                    \\{"responses":[{"hits":{"hits":[{"_id":"doc:a","_score":1.0,"_source":{"body":"fallback winner","status":"active"}}]}}]}
                ),
            };
        }
    };

    var runner = FakeRunner{};
    const body =
        \\{"query":"How does Raft consensus work in Antfly?","stream":true,"max_internal_iterations":3,"queries":[{"table":"docs","full_text_search":{"query":"body:missing"},"limit":5},{"table":"docs","filter_query":{"query":"status:active"},"limit":5}]}
    ;
    const encoded = try execute(std.testing.allocator, runner.iface(), null, body);
    defer std.testing.allocator.free(encoded.body);
    const events = try parseSseEventsAlloc(std.testing.allocator, encoded.body);
    defer std.testing.allocator.free(events);
    try std.testing.expect(countSseEvents(events, "step_completed") >= 1);
    var saw_evaluate = false;
    for (events) |event| {
        if (!std.mem.eql(u8, event.event, "step_progress")) continue;
        var parsed_progress = try parseJsonBody(TestStepProgressEvent, std.testing.allocator, event.data);
        defer parsed_progress.deinit();
        if (!std.mem.eql(u8, parsed_progress.value.phase, "evaluate")) continue;
        saw_evaluate = true;
        try std.testing.expectEqualStrings("evaluation", parsed_progress.value.selection_source.?);
        try std.testing.expectEqualStrings("evaluation", parsed_progress.value.next_selection_source.?);
        try std.testing.expect(parsed_progress.value.probe_hits != null);
    }
    try std.testing.expect(saw_evaluate);
}

test "retrieval agent sse emits evaluation refinement progress" {
    const FakeRunner = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) QueryRunner {
            return .{
                .ptr = self,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            var parsed_query = try parseQueryRequestBody(alloc, query_json);
            defer parsed_query.deinit();
            if (parsed_query.value.semantic_search != null) {
                if (self.call_count == 1) {
                    return .{
                        .json = try alloc.dupe(u8,
                            \\{"responses":[{"hits":{"hits":[{"_id":"doc:thin","_score":0.7,"_source":{"body":"architecture"}},{"_id":"doc:other","_score":0.6,"_source":{"body":"overview"}}]}}]}
                        ),
                    };
                }
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:semantic","_score":1.0,"_source":{"body":"Explain the architecture of Antfly in detail with cluster topology, storage roles, and retrieval planning."}}]}}]}
                    ),
                };
            }
            if (parsed_query.value.filter_query != null) return error.TestUnexpectedResult;
            return error.TestUnexpectedResult;
        }
    };

    var runner = FakeRunner{};
    const body =
        \\{"query":"Explain the architecture of Antfly in detail","stream":true,"max_internal_iterations":3,"steps":{"classification":{"enabled":true,"force_strategy":"simple","with_reasoning":true}},"queries":[{"table":"docs","semantic_search":"architecture overview","indexes":["semantic_idx"],"limit":5},{"table":"docs","filter_query":{"query":"status:active"},"limit":5}]}
    ;
    const encoded = try execute(std.testing.allocator, runner.iface(), null, body);
    defer std.testing.allocator.free(encoded.body);
    const events = try parseSseEventsAlloc(std.testing.allocator, encoded.body);
    defer std.testing.allocator.free(events);
    try std.testing.expect(countSseEvents(events, "reasoning") >= 2);
    try std.testing.expect(countSseEvents(events, "step_completed") >= 1);
    var saw_refine = false;
    for (events) |event| {
        if (!std.mem.eql(u8, event.event, "step_progress")) continue;
        var parsed_progress = try parseJsonBody(TestStepProgressEvent, std.testing.allocator, event.data);
        defer parsed_progress.deinit();
        if (!std.mem.eql(u8, parsed_progress.value.phase, "evaluation_refine")) continue;
        saw_refine = true;
        try std.testing.expectEqualStrings("refine_query", parsed_progress.value.planner_decision.?);
    }
    try std.testing.expect(saw_refine);
}

test "retrieval agent sse emits fallback consensus ambiguity progress" {
    const FakeRunner = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) QueryRunner {
            return .{
                .ptr = self,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, query_json: []const u8) !query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            var parsed_query = try parseQueryRequestBody(alloc, query_json);
            defer parsed_query.deinit();
            if (parsed_query.value.semantic_search != null) {
                if (self.call_count == 1) {
                    return .{
                        .json = try alloc.dupe(u8,
                            \\{"responses":[{"hits":{"hits":[{"_id":"doc:thin","_score":0.7,"_source":{"body":"architecture"}},{"_id":"doc:other","_score":0.6,"_source":{"body":"overview"}}]}}]}
                        ),
                    };
                }
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:thin","_score":0.71,"_source":{"body":"architecture"}},{"_id":"doc:other","_score":0.61,"_source":{"body":"overview"}}]}}]}
                    ),
                };
            }
            if (parsed_query.value.full_text_search != null and parsed_query.value.embeddings != null) {
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:hybrid","_score":0.8,"_source":{"body":"architecture overview"}}]}}]}
                    ),
                };
            }
            if (parsed_query.value.embeddings != null) {
                return .{
                    .json = try alloc.dupe(u8,
                        \\{"responses":[{"hits":{"hits":[{"_id":"doc:semantic_fallback","_score":0.8,"_source":{"body":"architecture overview"}}]}}]}
                    ),
                };
            }
            return error.TestUnexpectedResult;
        }
    };

    var runner = FakeRunner{};
    const body =
        \\{"query":"Explain the architecture of Antfly in detail","stream":true,"max_internal_iterations":4,"max_user_clarifications":1,"steps":{"classification":{"enabled":true,"force_strategy":"simple","with_reasoning":true}},"queries":[{"table":"docs","semantic_search":"architecture overview","indexes":["semantic_idx"],"limit":5},{"table":"docs","embeddings":{"dense_idx":[1.0,0.0,0.0]},"indexes":["dense_idx"],"limit":5},{"table":"docs","full_text_search":{"query":"body:architecture"},"embeddings":{"dense_idx":[1.0,0.0,0.0]},"indexes":["dense_idx"],"limit":5}]}
    ;
    const encoded = try execute(std.testing.allocator, runner.iface(), null, body);
    defer std.testing.allocator.free(encoded.body);
    const events = try parseSseEventsAlloc(std.testing.allocator, encoded.body);
    defer std.testing.allocator.free(events);
    var saw_ambiguity = false;
    for (events) |event| {
        if (std.mem.eql(u8, event.event, "step_progress")) {
            var parsed_progress = try parseJsonBody(TestStepProgressEvent, std.testing.allocator, event.data);
            defer parsed_progress.deinit();
            if (!std.mem.eql(u8, parsed_progress.value.phase, "fallback_consensus_ambiguity")) continue;
            saw_ambiguity = true;
            try std.testing.expectEqualStrings("clarify", parsed_progress.value.planner_decision.?);
            try std.testing.expectEqual(true, parsed_progress.value.fallback_consensus_ambiguous.?);
        } else if (std.mem.eql(u8, event.event, "reasoning")) {
            var parsed_reasoning = try parseJsonBody([]const u8, std.testing.allocator, event.data);
            defer parsed_reasoning.deinit();
            if (std.mem.indexOf(u8, parsed_reasoning.value, "multiple stronger fallback strategies remain effectively tied") != null) {
                saw_ambiguity = true;
            }
        }
    }
    try std.testing.expect(saw_ambiguity);
}

test "retrieval agent sse emits error events on query failure" {
    const FakeRunner = struct {
        fn iface() QueryRunner {
            return .{
                .ptr = undefined,
                .vtable = &.{ .run_query = runQuery },
            };
        }

        fn runQuery(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8) !query_api.QueryResponse {
            return error.TestSyntheticFailure;
        }
    };

    const body =
        \\{"query":"find alpha","stream":true,"queries":[{"table":"docs","full_text_search":{"query":"body:alpha"},"limit":5}]}
    ;
    const encoded = try execute(std.testing.allocator, FakeRunner.iface(), null, body);
    defer std.testing.allocator.free(encoded.body);
    const events = try parseSseEventsAlloc(std.testing.allocator, encoded.body);
    defer std.testing.allocator.free(events);

    try std.testing.expectEqualStrings("text/event-stream", encoded.content_type);
    var parsed_error = try parseJsonBody([]const u8, std.testing.allocator, firstSseEventData(events, "error").?);
    defer parsed_error.deinit();
    try std.testing.expect(std.mem.indexOf(u8, parsed_error.value, "TestSyntheticFailure") != null);
}

fn unreachableRunQuery(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8) anyerror!query_api.QueryResponse {
    return error.UnexpectedRunQuery;
}
