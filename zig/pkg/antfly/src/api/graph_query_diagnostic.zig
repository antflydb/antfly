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
    external_alias_document_filter_not_supported,
    external_alias_source_not_supported,
    reverse_variable_path_not_supported,
};

pub const Diagnostic = struct {
    operation: []const u8,
    mode: []const u8,
    reason: Reason,
};

// Public graph operation names are admitted at no more than 512 UTF-8 bytes.
// Copy the name so the diagnostic remains valid while request-owned query
// storage unwinds through the error path.
threadlocal var last_diagnostic: ?Diagnostic = null;
threadlocal var operation_buf: [graph_query.max_identifier_bytes]u8 = undefined;

pub fn reset() void {
    last_diagnostic = null;
}

pub fn take() ?Diagnostic {
    const diagnostic = last_diagnostic;
    last_diagnostic = null;
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

pub fn mode(query: graph_query.GraphQuery) []const u8 {
    return switch (query.query_type) {
        .neighbors => "neighbors",
        .traverse => "traverse",
        .shortest_path => "shortest_path",
        .k_shortest_paths => "k_shortest_paths",
        .pattern => if (query.match_pattern != null and !query.legacy_response) "match" else "pattern",
    };
}

pub fn record(operation: []const u8, operation_mode: []const u8, reason: Reason) void {
    // Admission guarantees this bound for public requests. Fail closed for an
    // internal caller that bypasses admission instead of emitting a truncated,
    // misleading operation name.
    if (operation.len == 0 or operation.len > operation_buf.len) {
        last_diagnostic = null;
        return;
    }
    @memcpy(operation_buf[0..operation.len], operation);
    last_diagnostic = .{
        .operation = operation_buf[0..operation.len],
        .mode = operation_mode,
        .reason = reason,
    };
}

test "graph capability diagnostic owns operation name and clears on take" {
    var operation = [_]u8{ 'l', 'a', 't', 'e', 'r' };
    record(&operation, "match", .external_alias_source_not_supported);
    @memset(&operation, 'x');

    const diagnostic = take() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("later", diagnostic.operation);
    try std.testing.expectEqualStrings("match", diagnostic.mode);
    try std.testing.expectEqual(Reason.external_alias_source_not_supported, diagnostic.reason);
    try std.testing.expect(take() == null);
}
