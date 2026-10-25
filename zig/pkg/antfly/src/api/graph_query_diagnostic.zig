// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const types = @import("antfly_local_sources").storage_db_types;
const graph_query = @import("antfly_local_sources").graph_query;

pub const Reason = enum {
    expand_strategy_not_supported,
    deduplicate_nodes_must_be_true,
    start_selector_not_supported,
    target_selector_not_supported,
    weight_mode_must_be_min_hops,
    target_required,
    k_must_equal_one,
    k_must_be_positive,
    pattern_required,
    qualified_start_identity_required,
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

pub fn sink() ?types.GraphQueryDiagnosticSink {
    return .{ .ptr = active_storage orelse return null, .record_fn = recordForwarded };
}

fn recordForwarded(ptr: *anyopaque, operation: []const u8, query_feature: []const u8, reason: []const u8) void {
    const storage: *Storage = @ptrCast(@alignCast(ptr));
    const typed_reason = std.meta.stringToEnum(Reason, reason) orelse {
        storage.diagnostic = null;
        return;
    };
    recordInto(storage, operation, query_feature, typed_reason);
}

pub fn recordForRequest(req: types.SearchRequest, operation: []const u8, query_feature: []const u8, reason: Reason) void {
    if (req.graph_query_diagnostic_sink) |target| {
        target.record_fn(target.ptr, operation, query_feature, @tagName(reason));
    } else record(operation, query_feature, reason);
}

pub fn record(operation: []const u8, query_feature: []const u8, reason: Reason) void {
    recordInto(active_storage orelse return, operation, query_feature, reason);
}

fn recordInto(storage: *Storage, operation: []const u8, query_feature: []const u8, reason: Reason) void {
    // Admission guarantees this bound for public requests. Fail closed for an
    // internal caller that bypasses admission instead of emitting a truncated,
    // misleading operation name.
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

test "graph capability diagnostic forwards into its owning request across bindings" {
    var caller: Storage = .{};
    const caller_binding = bind(&caller);
    defer caller_binding.deinit();
    const req: types.SearchRequest = .{ .graph_query_diagnostic_sink = sink() };
    var callee: Storage = .{};
    const callee_binding = bind(&callee);
    var operation = [_]u8{ 'l', 'a', 't', 'e', 'r' };
    recordForRequest(req, &operation, "traverse", .qualified_start_identity_required);
    @memset(&operation, 'x');
    try std.testing.expect(take() == null);
    callee_binding.deinit();
    const diagnostic = take().?;
    try std.testing.expectEqualStrings("later", diagnostic.operation);
    try std.testing.expectEqual(Reason.qualified_start_identity_required, diagnostic.reason);
    try std.testing.expect(take() == null);
}
