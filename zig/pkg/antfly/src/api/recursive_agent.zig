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
const metadata_openapi = @import("antfly_metadata_openapi");
const platform_time = @import("../platform/time.zig");

pub const Defaults = struct {
    pub const max_depth: i64 = 1;
    pub const max_subcalls: i64 = 8;
    pub const max_concurrency: i64 = 4;
    pub const max_wall_time_ms: i64 = 30_000;
    pub const split_policy: metadata_openapi.RecursiveSplitPolicy = .auto;
    pub const merge_policy: metadata_openapi.RecursiveMergePolicy = .synthesize;
    pub const child_tool_policy: metadata_openapi.RecursiveChildToolPolicy = .inherit_narrowed;
};

pub const Limits = struct {
    pub const max_depth: i64 = 1;
    pub const max_subcalls: i64 = 64;
    pub const max_concurrency: i64 = 16;
    pub const max_wall_time_ms: i64 = 300_000;
    pub const max_child_context_tokens: i64 = 200_000;
};

pub const RecursiveConfig = struct {
    max_depth: i64 = Defaults.max_depth,
    max_subcalls: i64 = Defaults.max_subcalls,
    max_concurrency: i64 = Defaults.max_concurrency,
    max_wall_time_ms: i64 = Defaults.max_wall_time_ms,
    max_child_context_tokens: ?i64 = null,
    split_policy: metadata_openapi.RecursiveSplitPolicy = Defaults.split_policy,
    merge_policy: metadata_openapi.RecursiveMergePolicy = Defaults.merge_policy,
    child_tool_policy: metadata_openapi.RecursiveChildToolPolicy = Defaults.child_tool_policy,
    allowed_context_object_types: ?[]const metadata_openapi.ContextObjectKind = null,
};

pub const ContextObject = struct {
    kind: metadata_openapi.ContextObjectKind,
    id: []const u8,
    label: ?[]const u8 = null,
    metadata: ?std.json.Value = null,
};

pub const AgentFrame = struct {
    id: []const u8,
    parent_id: ?[]const u8 = null,
    depth: i64,
    query: []const u8,
    context_objects: []const ContextObject = &.{},
    recursive: RecursiveConfig,
};

pub const Subcall = struct {
    id: []const u8,
    parent_frame_id: []const u8,
    child_frame_id: []const u8,
    prompt: []const u8,
    context_objects: []const ContextObject = &.{},
};

pub const TraceArtifact = struct {
    root_frame_id: []const u8,
    final_status: metadata_openapi.AgentStatus,
    decomposition_plan: ?std.json.Value = null,
    context_objects: []const ContextObject = &.{},
    subcalls: []const Subcall = &.{},
    steps: []const metadata_openapi.AgentStep = &.{},
};

pub const Budget = struct {
    start_ns: u64,
    deadline_ns: u64,

    pub fn start(cfg: RecursiveConfig) Budget {
        const start_ns = platform_time.monotonicNs();
        const budget_ns = @as(u64, @intCast(cfg.max_wall_time_ms)) * std.time.ns_per_ms;
        return .{
            .start_ns = start_ns,
            .deadline_ns = start_ns + budget_ns,
        };
    }

    pub fn expired(self: Budget) bool {
        return platform_time.monotonicNs() >= self.deadline_ns;
    }

    pub fn remainingMs(self: Budget) ?u64 {
        const now_ns = platform_time.monotonicNs();
        if (now_ns >= self.deadline_ns) return null;
        const remaining_ns = self.deadline_ns - now_ns;
        return @max(@as(u64, 1), @divTrunc(remaining_ns + std.time.ns_per_ms - 1, std.time.ns_per_ms));
    }

    pub fn elapsedMs(self: Budget) i64 {
        const now_ns = platform_time.monotonicNs();
        if (now_ns <= self.start_ns) return 0;
        return @intCast(@divTrunc(now_ns - self.start_ns, std.time.ns_per_ms));
    }
};

pub fn normalizeConfig(config: ?metadata_openapi.RecursiveAgentConfig) !RecursiveConfig {
    const raw = config orelse return .{};
    const max_depth = try boundedPositive(raw.max_depth orelse Defaults.max_depth, 1, Limits.max_depth);
    const max_subcalls = try boundedPositive(raw.max_subcalls orelse Defaults.max_subcalls, 1, Limits.max_subcalls);
    const max_concurrency = try boundedPositive(raw.max_concurrency orelse Defaults.max_concurrency, 1, Limits.max_concurrency);
    if (max_concurrency > max_subcalls) return error.InvalidRecursiveAgentConfig;

    const max_wall_time_ms = try boundedPositive(raw.max_wall_time_ms orelse Defaults.max_wall_time_ms, 1, Limits.max_wall_time_ms);
    const max_child_context_tokens = if (raw.max_child_context_tokens) |tokens|
        try boundedPositive(tokens, 1, Limits.max_child_context_tokens)
    else
        null;

    return .{
        .max_depth = max_depth,
        .max_subcalls = max_subcalls,
        .max_concurrency = max_concurrency,
        .max_wall_time_ms = max_wall_time_ms,
        .max_child_context_tokens = max_child_context_tokens,
        .split_policy = raw.split_policy orelse Defaults.split_policy,
        .merge_policy = raw.merge_policy orelse Defaults.merge_policy,
        .child_tool_policy = raw.child_tool_policy orelse Defaults.child_tool_policy,
        .allowed_context_object_types = raw.allowed_context_object_types,
    };
}

fn boundedPositive(value: i64, min: i64, max: i64) !i64 {
    if (value < min or value > max) return error.InvalidRecursiveAgentConfig;
    return value;
}

pub fn contextKindAllowed(cfg: RecursiveConfig, kind: metadata_openapi.ContextObjectKind) bool {
    const allowed = cfg.allowed_context_object_types orelse return true;
    for (allowed) |candidate| {
        if (candidate == kind) return true;
    }
    return false;
}

pub fn childCountForContextCount(cfg: RecursiveConfig, context_count: usize) usize {
    return @min(context_count, @as(usize, @intCast(cfg.max_subcalls)));
}

pub fn scheduledConcurrency(cfg: RecursiveConfig, child_count: usize) usize {
    return @min(child_count, @as(usize, @intCast(cfg.max_concurrency)));
}

pub fn incompleteReason(
    truncated_by_subcalls: bool,
    skipped_by_token_budget_count: usize,
    wall_time_exhausted: bool,
) ?[]const u8 {
    if (wall_time_exhausted) return "max_wall_time_ms";
    if (truncated_by_subcalls) return "max_subcalls";
    if (skipped_by_token_budget_count > 0) return "max_child_context_tokens";
    return null;
}

pub fn isGenerationTimeoutError(err: anyerror) bool {
    return err == error.DeadlineExceeded or err == error.Timeout or err == error.ConnectionTimedOut;
}

pub fn traceContextObjectFromContextObject(context_object: ContextObject) metadata_openapi.RecursiveTraceContextObject {
    return .{
        .kind = context_object.kind,
        .id = context_object.id,
        .label = context_object.label,
        .metadata = context_object.metadata,
    };
}

pub fn buildTraceArtifact(
    root_frame_id: []const u8,
    final_status: metadata_openapi.AgentStatus,
    context_objects: []const metadata_openapi.RecursiveTraceContextObject,
    subcalls: []const metadata_openapi.RecursiveTraceSubcall,
    steps: []const metadata_openapi.AgentStep,
) metadata_openapi.RecursiveTraceArtifact {
    return .{
        .root_frame_id = root_frame_id,
        .final_status = final_status,
        .context_objects = context_objects,
        .subcalls = subcalls,
        .steps = steps,
    };
}

pub fn validateRetrievalExecutionMode(
    request: metadata_openapi.RetrievalAgentRequest,
    max_internal_iterations: i64,
) !metadata_openapi.AgentExecutionMode {
    if (max_internal_iterations < 0) return error.InvalidRetrievalAgentRequest;

    const mode = request.execution_mode orelse {
        if (request.recursive != null) return error.InvalidRetrievalAgentRequest;
        return if (max_internal_iterations > 0) .agentic else .pipeline;
    };

    switch (mode) {
        .pipeline => {
            if (request.recursive != null) return error.InvalidRetrievalAgentRequest;
            if (max_internal_iterations != 0) return error.InvalidRetrievalAgentRequest;
        },
        .agentic => {
            if (request.recursive != null) return error.InvalidRetrievalAgentRequest;
            if (max_internal_iterations <= 0) return error.InvalidRetrievalAgentRequest;
        },
        .recursive => {
            _ = normalizeConfig(request.recursive) catch return error.InvalidRetrievalAgentRequest;
        },
    }
    return mode;
}

pub fn validateQueryBuilderExecutionMode(request: metadata_openapi.QueryBuilderRequest) !void {
    const mode = request.execution_mode orelse {
        if (request.recursive != null) return error.InvalidQueryBuilderRequest;
        return;
    };

    switch (mode) {
        .pipeline => {
            if (request.recursive != null) return error.InvalidQueryBuilderRequest;
        },
        .agentic => {
            if (request.recursive != null) return error.InvalidQueryBuilderRequest;
            return error.UnsupportedQueryBuilderRequest;
        },
        .recursive => {
            _ = normalizeConfig(request.recursive) catch return error.InvalidQueryBuilderRequest;
        },
    }
}

test "recursive agent config applies conservative defaults" {
    const cfg = try normalizeConfig(null);
    try std.testing.expectEqual(@as(i64, Defaults.max_depth), cfg.max_depth);
    try std.testing.expectEqual(@as(i64, Defaults.max_subcalls), cfg.max_subcalls);
    try std.testing.expectEqual(@as(i64, Defaults.max_concurrency), cfg.max_concurrency);
    try std.testing.expectEqual(Defaults.split_policy, cfg.split_policy);
}

test "recursive agent config rejects unbounded fanout" {
    try std.testing.expectError(error.InvalidRecursiveAgentConfig, normalizeConfig(.{
        .max_depth = 2,
    }));
    try std.testing.expectError(error.InvalidRecursiveAgentConfig, normalizeConfig(.{
        .max_depth = 1,
        .max_subcalls = 4,
        .max_concurrency = 5,
    }));
}

test "recursive agent shared budget helpers fail closed" {
    const cfg = try normalizeConfig(.{
        .max_subcalls = 2,
        .max_concurrency = 1,
        .allowed_context_object_types = &.{ .document, .section },
    });

    try std.testing.expectEqual(@as(usize, 2), childCountForContextCount(cfg, 5));
    try std.testing.expectEqual(@as(usize, 1), scheduledConcurrency(cfg, 2));
    try std.testing.expect(contextKindAllowed(cfg, .document));
    try std.testing.expect(!contextKindAllowed(cfg, .memory_set));
    try std.testing.expectEqualStrings("max_wall_time_ms", incompleteReason(true, 1, true).?);
    try std.testing.expectEqualStrings("max_subcalls", incompleteReason(true, 1, false).?);
    try std.testing.expectEqualStrings("max_child_context_tokens", incompleteReason(false, 1, false).?);
    try std.testing.expect(incompleteReason(false, 0, false) == null);
}

test "recursive agent classifies generation timeout errors" {
    try std.testing.expect(isGenerationTimeoutError(error.DeadlineExceeded));
    try std.testing.expect(isGenerationTimeoutError(error.Timeout));
    try std.testing.expect(isGenerationTimeoutError(error.ConnectionTimedOut));
    try std.testing.expect(!isGenerationTimeoutError(error.InvalidRecursiveAgentConfig));
}
