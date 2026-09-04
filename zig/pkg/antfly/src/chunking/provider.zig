// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Lightweight, family-specific view of the embedded inference provider.
//!
//! Storage runtimes keep this borrowed descriptor without importing the full
//! inference provider graph. The executor restores the typed callback before
//! crossing the checked native boundary.

const runtime_callback_abi = @import("../runtime_callback_abi.zig");
const execution_context = @import("../inference/execution_context.zig");
const std = @import("std");

pub const Provider = struct {
    ptr: ?*anyopaque = null,
    boundary_dispatch: ?runtime_callback_abi.CallbackDispatch = null,
    chunk_input_callback: ?*const anyopaque = null,
    chunk_input_with_context_callback: ?*const anyopaque = null,
    execution: execution_context.Context = .{},
    owned_allocator: ?std.mem.Allocator = null,
    owned_default_endpoint: ?[]u8 = null,
    owned_source_table: ?[]u8 = null,

    /// Create the persistent form stored by an enrichment runtime. The
    /// execution context otherwise borrows its strings for one invocation;
    /// durable providers must copy request/catalog-owned routing values.
    pub fn ownExecutionStrings(self: Provider, alloc: std.mem.Allocator) !Provider {
        var owned = self;
        // A second durable copy must never alias or free the first copy's
        // ownership bookkeeping. Only the execution slices are treated as
        // borrowed inputs to this construction.
        owned.owned_allocator = null;
        owned.owned_default_endpoint = null;
        owned.owned_source_table = null;
        owned.owned_allocator = alloc;
        errdefer owned.deinit();
        if (self.execution.default_endpoint) |endpoint| {
            owned.owned_default_endpoint = try alloc.dupe(u8, endpoint);
            owned.execution.default_endpoint = owned.owned_default_endpoint;
        }
        if (self.execution.routing.source_table.len > 0) {
            owned.owned_source_table = try alloc.dupe(u8, self.execution.routing.source_table);
            owned.execution.routing.source_table = owned.owned_source_table.?;
        }
        return owned;
    }

    pub fn deinit(self: *Provider) void {
        const alloc = self.owned_allocator orelse {
            self.* = undefined;
            return;
        };
        if (self.owned_default_endpoint) |value| alloc.free(value);
        if (self.owned_source_table) |value| alloc.free(value);
        self.* = undefined;
    }
};

test "durable chunk provider owns execution routing strings" {
    var endpoint = [_]u8{ 'h', 't', 't', 'p' };
    var table = [_]u8{ 'd', 'o', 'c', 's' };
    var provider = try (Provider{ .execution = .{
        .default_endpoint = &endpoint,
        .routing = .{ .source_table = &table },
    } }).ownExecutionStrings(std.testing.allocator);
    defer provider.deinit();

    endpoint[0] = 'x';
    table[0] = 'x';
    try std.testing.expectEqualStrings("http", provider.execution.default_endpoint.?);
    try std.testing.expectEqualStrings("docs", provider.execution.routing.source_table);

    var copy = try provider.ownExecutionStrings(std.testing.allocator);
    defer copy.deinit();
    try std.testing.expect(copy.execution.default_endpoint.?.ptr != provider.execution.default_endpoint.?.ptr);
    try std.testing.expect(copy.execution.routing.source_table.ptr != provider.execution.routing.source_table.ptr);
}
