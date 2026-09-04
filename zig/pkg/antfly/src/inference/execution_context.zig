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
};

pub const Context = InferenceExecutionContext;

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
