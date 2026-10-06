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

/// Discovery is restricted to immutable reads. Mismatched authority, corrupt
/// bytes, authentication failures and cancellation are never hidden by retry.
pub fn mayTryReplica(request: anytype, err: anyerror) bool {
    if (request.operation != .artifact) return false;
    switch (request.operation.artifact) {
        .describe, .read => {},
        else => return false,
    }
    return switch (err) {
        error.OnlineSourcePinMissing,
        error.FileNotFound,
        error.GroupLeaderUnavailable,
        error.ConnectionRefused,
        error.ConnectionResetByPeer,
        error.ConnectionTimedOut,
        error.Timeout,
        => true,
        else => false,
    };
}

/// The caller obtains these IDs from current readable group placements and
/// rechecks the selected node's catalog URI before dispatch. Skip the already
/// attempted leader without spending a remote request.
pub fn candidate(nodes: []const u64, excluded: u64, start: usize) ?u64 {
    if (nodes.len == 0) return null;
    for (0..nodes.len) |offset| {
        const node = nodes[(start % nodes.len + offset) % nodes.len];
        if (node != excluded) return node;
    }
    return null;
}

test "rewrite artifact replica fallback is read only and never masks identity corruption or cancellation" {
    const Request = struct { operation: union(enum) { artifact: enum { describe, read, status, write, finish, reset }, status, tail, admission } };
    for ([_]@FieldType(Request, "operation"){ .{ .artifact = .describe }, .{ .artifact = .read } }) |op| {
        const req: Request = .{ .operation = op };
        try std.testing.expect(mayTryReplica(req, error.OnlineSourcePinMissing));
        try std.testing.expect(mayTryReplica(req, error.GroupLeaderUnavailable));
        try std.testing.expect(mayTryReplica(req, error.ConnectionRefused));
        for ([_]anyerror{ error.OnlineSourceScopeChanged, error.SourceSnapshotCutMismatch, error.InvalidSourceSnapshot, error.OnlineMergeReceiptMismatch, error.Unauthorized, error.UnexpectedHttpStatus, error.Canceled, error.OutOfMemory }) |err| {
            try std.testing.expect(!mayTryReplica(req, err));
        }
    }
    for ([_]@FieldType(Request, "operation"){ .{ .artifact = .status }, .{ .artifact = .write }, .{ .artifact = .finish }, .{ .artifact = .reset }, .status, .tail, .admission }) |op| {
        try std.testing.expect(!mayTryReplica(Request{ .operation = op }, error.OnlineSourcePinMissing));
    }
}

test "rewrite artifact replica fallback rotates missing replicas with one candidate per slice" {
    const nodes = &.{ @as(u64, 11), 22, 33 };
    try std.testing.expectEqual(@as(?u64, 22), candidate(nodes, 11, 0));
    try std.testing.expectEqual(@as(?u64, 22), candidate(nodes, 11, 1));
    try std.testing.expectEqual(@as(?u64, 33), candidate(nodes, 11, 2));
    try std.testing.expectEqual(@as(?u64, null), candidate(&.{11}, 11, 1));
    try std.testing.expectEqual(@as(?u64, null), candidate(&.{}, 11, 1));
}
