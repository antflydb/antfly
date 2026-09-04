// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: ELv2

//! Immutable execution inputs shared by every model-family adapter.
//!
//! Model configuration describes what to run. This context describes where
//! Antfly-owned work runs and which tenant route selected it. Keeping these
//! concerns separate prevents document producers, embedders, rerankers, and
//! future task families from independently inventing localhost fallbacks.

const std = @import("std");
const remote_capabilities = @import("remote_capabilities.zig");
const CancellationToken = @import("../common/cancellation.zig").CancellationToken;
const platform_time = @import("antfly_platform").time;

pub const source_table_header = "X-Antfly-Source-Table";

pub const RoutingContext = struct {
    /// Internal, trusted table identity. Never populate this from a public
    /// model config: the inference proxy uses it only to select a provisioned
    /// route after authenticating the request.
    source_table: []const u8 = "",

    pub fn headerCount(self: RoutingContext) usize {
        return @intFromBool(self.source_table.len > 0);
    }

    pub fn appendHeaders(
        self: RoutingContext,
        storage: []([2][]const u8),
        start: usize,
    ) !usize {
        var count = start;
        if (self.source_table.len > 0) {
            if (count >= storage.len) return error.InferenceRoutingHeaderCapacityExceeded;
            storage[count] = .{ source_table_header, self.source_table };
            count += 1;
        }
        return count;
    }
};

pub const InferenceExecutionContext = struct {
    /// Default distributed inference endpoint. Explicit per-model URLs retain
    /// precedence; an available linked callback retains precedence over this
    /// default for configs that did not explicitly request a remote endpoint.
    default_endpoint: ?[]const u8 = null,
    capability_cache: ?*remote_capabilities.Cache = null,
    io: ?std.Io = null,
    routing: RoutingContext = .{},
    /// Absolute monotonic deadline for discovery and execution. Task adapters
    /// may apply a stricter family-specific ceiling, but must never extend it.
    deadline_ns: ?u64 = null,
    cancellation: CancellationToken = .none,
    /// Optional caller-owned response ceiling. Task adapters intersect this
    /// with their own hard limit rather than treating it as permission to
    /// allocate more.
    max_response_bytes: ?usize = null,

    pub fn resolveAntflyEndpoint(
        self: InferenceExecutionContext,
        explicit_endpoint: ?[]const u8,
        linked_callback_available: bool,
    ) ?[]const u8 {
        if (explicit_endpoint) |endpoint| {
            if (std.mem.trim(u8, endpoint, " \t\r\n").len > 0) return endpoint;
        }
        if (linked_callback_available) return null;
        if (self.default_endpoint) |endpoint| {
            if (std.mem.trim(u8, endpoint, " \t\r\n").len > 0) return endpoint;
        }
        return null;
    }

    pub fn waitContext(self: InferenceExecutionContext) remote_capabilities.WaitContext {
        return .{
            .deadline_ns = self.deadline_ns,
            .cancellation = self.cancellation,
        };
    }

    pub fn requestContext(self: InferenceExecutionContext, io: std.Io) RequestContext {
        return .{
            .io = io,
            .deadline_ns = self.deadline_ns,
            .cancellation = self.cancellation,
        };
    }

    pub fn check(self: InferenceExecutionContext, now_ns: u64) !void {
        try self.cancellation.check();
        if (self.deadline_ns) |deadline| if (now_ns >= deadline) return error.Timeout;
    }

    pub fn boundedResponseBytes(self: InferenceExecutionContext, family_limit: usize) usize {
        return if (self.max_response_bytes) |requested|
            @min(requested, family_limit)
        else
            family_limit;
    }

    pub fn remainingTimeoutMs(
        self: InferenceExecutionContext,
        now_ns: u64,
        family_limit_ms: u64,
    ) !u64 {
        try self.check(now_ns);
        const deadline = self.deadline_ns orelse return family_limit_ms;
        const remaining_ns = deadline - now_ns;
        const rounded_ms = @max(
            @as(u64, 1),
            std.math.divCeil(u64, remaining_ns, std.time.ns_per_ms) catch 1,
        );
        return @min(rounded_ms, family_limit_ms);
    }
};

pub const Context = InferenceExecutionContext;

/// Invocation-local control plane for linked task callbacks. It deliberately
/// excludes routing, caches, and provider configuration: callbacks receive
/// only the executor and controls they must observe while work is in flight.
pub const RequestContext = struct {
    io: std.Io,
    deadline_ns: ?u64 = null,
    cancellation: CancellationToken = .none,

    pub fn check(self: RequestContext) !void {
        try self.cancellation.check();
        if (self.deadline_ns) |deadline| {
            if (platform_time.monotonicNs() >= deadline) return error.Timeout;
        }
    }
};

test "execution context gives explicit and linked routes precedence" {
    const context = Context{ .default_endpoint = "http://distributed" };
    try std.testing.expectEqualStrings(
        "http://explicit",
        context.resolveAntflyEndpoint("http://explicit", true).?,
    );
    try std.testing.expect(context.resolveAntflyEndpoint(null, true) == null);
    try std.testing.expectEqualStrings(
        "http://distributed",
        context.resolveAntflyEndpoint(null, false).?,
    );
    try std.testing.expect((Context{ .default_endpoint = " \t" }).resolveAntflyEndpoint(null, false) == null);
}

test "routing context appends trusted table header" {
    var storage: [2][2][]const u8 = undefined;
    storage[0] = .{ "Authorization", "Bearer token" };
    const count = try (RoutingContext{ .source_table = "docs" }).appendHeaders(&storage, 1);
    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expectEqualStrings(source_table_header, storage[1][0]);
    try std.testing.expectEqualStrings("docs", storage[1][1]);
}

test "execution context preserves control and resource bounds" {
    var canceled = std.atomic.Value(bool).init(false);
    const context = Context{
        .deadline_ns = 200,
        .cancellation = CancellationToken.fromAtomic(&canceled),
        .max_response_bytes = 512,
    };
    try context.check(199);
    try std.testing.expectError(error.Timeout, context.check(200));
    try std.testing.expectEqual(@as(usize, 512), context.boundedResponseBytes(1024));
    try std.testing.expectEqual(@as(?u64, 200), context.waitContext().deadline_ns);
    canceled.store(true, .release);
    try std.testing.expectError(error.Canceled, context.check(0));
}
