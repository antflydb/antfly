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

//! Optional cleanup gets a short, independent turn. Expiry proves eligibility;
//! cancellation only leaves garbage for a later pass and cannot revoke reads.
const std = @import("std");
const Context = @import("antfly_local_sources").serverless_query_lake_read_context.Context;
const Token = @import("antfly_cancellation").CancellationToken;
const stores = @import("../serverless/artifacts/store.zig");
pub const quantum_ns = 5 * std.time.ns_per_s;
pub fn turnContext(parent: Context) Context {
    var bounded = parent;
    const deadline = @import("antfly_platform").time.monotonicNs() +| quantum_ns;
    bounded.deadline_ns = if (parent.deadline_ns) |existing| @min(existing, deadline) else deadline;
    return bounded;
}
/// Borrowed for synchronous provider operations; the context must outlive them.
pub fn contextToken(context: *const Context) Token {
    return .{
        .ptr = context,
        .is_cancelled_fn = struct {
            fn canceled(raw: *const anyopaque) bool {
                const value: *const Context = @ptrCast(@alignCast(raw));
                // Transport probes do not run fallible checkpoint callbacks.
                if (value.deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() >= deadline) return true;
                if (value.cancellation) |parent| if (parent.isCancelled()) return true;
                return false;
            }
        }.canceled,
        .check_fn = struct {
            fn check(raw: *const anyopaque) !void {
                const value: *const Context = @ptrCast(@alignCast(raw));
                try value.ensureActive();
            }
        }.check,
    };
}
const Budget = struct {
    parent: Token,
    deadline_ns: u64,
    fn expired(self: @This()) bool {
        return @import("antfly_platform").time.monotonicNs() >= self.deadline_ns;
    }
    fn token(self: *const @This()) Token {
        return .{ .ptr = self, .is_cancelled_fn = canceled, .check_fn = check };
    }
    fn canceled(raw: *const anyopaque) bool {
        const self: *const @This() = @ptrCast(@alignCast(raw));
        return self.parent.isCancelled() or self.expired();
    }
    fn check(raw: *const anyopaque) !void {
        const self: *const @This() = @ptrCast(@alignCast(raw));
        try self.parent.check();
        if (self.expired()) return error.DeadlineExceeded;
    }
};
/// Expired immutable scopes need no mutable continuation: completed permanent
/// deletions disappear from the next prefix scan, including after restart.
pub fn collectExpiredScopes(store: *stores.ArtifactStore, domain: [32]u8, cutoff: u64, parent: Token) !void {
    try parent.check();
    const budget: Budget = .{ .parent = parent, .deadline_ns = @import("antfly_platform").time.monotonicNs() +| quantum_ns };
    const token = budget.token();
    const Visitor = struct {
        store: *stores.ArtifactStore,
        token: Token,
        cutoff: u64,
        visited: usize = 0,
        fn visit(raw: *anyopaque, scope: stores.UploadScope, id: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.token.check();
            if (scope.fencingToken() >= self.cutoff) return;
            if (self.visited == 128) return error.LakeCleanupTurnBound;
            try self.store.deleteWithCancellation(id, self.token);
            self.visited += 1;
        }
    };
    var visitor: Visitor = .{ .store = store, .token = token, .cutoff = cutoff };
    store.visitScopedUploads(domain, .{ .ptr = &visitor, .visit = Visitor.visit, .fencing_cutoff = cutoff, .max_entries = 128 }, token) catch |err| {
        // Preserve parent cancellation even if the optional budget also ended.
        try parent.check();
        switch (err) {
            error.LakeCleanupTurnBound, error.ArtifactEnumerationPaused, error.ArtifactVersionDeletionPaused => return,
            error.Canceled, error.Cancelled, error.DeadlineExceeded => if (budget.expired()) return else return err,
            else => return err,
        }
    };
    try parent.check();
}

test "external lake vacuum cleanup budget never extends a parent deadline" {
    const original: Context = .{ .deadline_ns = std.math.maxInt(u64) };
    const short = turnContext(.{ .deadline_ns = 1 });
    try std.testing.expectEqual(@as(?u64, 1), short.deadline_ns);
    try std.testing.expectError(error.DeadlineExceeded, contextToken(&short).check());
    const bounded = turnContext(original);
    try std.testing.expect(bounded.deadline_ns.? < original.deadline_ns.?);
    try original.ensureActive();
    const budget: Budget = .{ .parent = .none, .deadline_ns = 1 };
    try std.testing.expect(budget.token().isCancelled());
    try std.testing.expectError(error.DeadlineExceeded, budget.token().check());
}
