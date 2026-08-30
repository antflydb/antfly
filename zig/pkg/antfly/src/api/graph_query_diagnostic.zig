// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
// https://www.antfly.io/licensing/ELv2-license

const std = @import("std");
const graph_query = @import("../graph/query.zig");

pub const Reason = enum {
    legacy_graph_searches_not_supported,
    request_control_not_supported,
    external_alias_document_filter_not_supported,
    external_alias_source_not_supported,
    reverse_variable_path_not_supported,
};

pub const Diagnostic = struct {
    operation: []const u8,
    feature: []const u8,
    reason: Reason,
};

/// Request-owned storage. The active thread only retains a scoped pointer to
/// this value while synchronous query execution is in progress.
pub const Storage = struct {
    diagnostic: ?Diagnostic = null,
    operation_buf: [graph_query.max_identifier_bytes]u8 = undefined,
};

threadlocal var active_storage: ?*Storage = null;

pub const Binding = struct {
    previous: ?*Storage,

    pub fn deinit(self: Binding) void {
        active_storage = self.previous;
    }
};

pub fn bind(storage: *Storage) Binding {
    const previous = active_storage;
    active_storage = storage;
    return .{ .previous = previous };
}

pub fn reset() void {
    if (active_storage) |storage| storage.diagnostic = null;
}

pub fn take() ?Diagnostic {
    const storage = active_storage orelse return null;
    const diagnostic = storage.diagnostic;
    storage.diagnostic = null;
    return diagnostic;
}

pub fn reasonForError(err: anyerror) ?Reason {
    return switch (err) {
        error.GraphExternalAliasDocumentFilterUnsupported => .external_alias_document_filter_not_supported,
        error.GraphExternalAliasSourceUnsupported => .external_alias_source_not_supported,
        error.GraphReverseVariablePathUnsupported => .reverse_variable_path_not_supported,
        else => null,
    };
}

pub fn feature(query: graph_query.GraphQuery) []const u8 {
    return switch (query.query_type) {
        .neighbors => "neighbors",
        .traverse => "traverse",
        .shortest_path => "shortest_path",
        .k_shortest_paths => "k_shortest_paths",
        .pattern => if (query.match_pattern != null) "match" else "pattern",
    };
}

pub fn record(operation: []const u8, query_feature: []const u8, reason: Reason) void {
    // Admission guarantees this bound for public requests. Fail closed for an
    // internal caller that bypasses admission instead of emitting a truncated,
    // misleading operation name.
    const storage = active_storage orelse return;
    if (operation.len == 0 or operation.len > storage.operation_buf.len) {
        storage.diagnostic = null;
        return;
    }
    @memcpy(storage.operation_buf[0..operation.len], operation);
    storage.diagnostic = .{
        .operation = storage.operation_buf[0..operation.len],
        .feature = query_feature,
        .reason = reason,
    };
}

test "graph capability diagnostic owns operation name and clears on take" {
    var storage: Storage = .{};
    const binding = bind(&storage);
    defer binding.deinit();
    var operation = [_]u8{ 'l', 'a', 't', 'e', 'r' };
    record(&operation, "match", .external_alias_source_not_supported);
    @memset(&operation, 'x');

    const diagnostic = take() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("later", diagnostic.operation);
    try std.testing.expectEqualStrings("match", diagnostic.feature);
    try std.testing.expectEqual(Reason.external_alias_source_not_supported, diagnostic.reason);
    try std.testing.expect(take() == null);
}
