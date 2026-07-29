// Copyright 2026 Antfly, Inc.
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

// Session pool: manages a fixed-size pool of inference sessions for concurrent use.
//
// httpx uses fiber-based concurrency on a single OS thread, so we use
// a simple available-list pattern without locks.

const std = @import("std");
const Session = @import("session.zig").Session;

pub const SessionPool = struct {
    pub const OwnedSession = struct {
        session: Session,
        close_context: ?*anyopaque = null,
        close_fn: *const fn (?*anyopaque, Session) void = defaultClose,

        pub fn deinit(self: *OwnedSession) void {
            self.close_fn(self.close_context, self.session);
            self.* = undefined;
        }

        fn defaultClose(_: ?*anyopaque, session: Session) void {
            session.close();
        }
    };

    pub const Loader = struct {
        context: *anyopaque,
        load_fn: *const fn (*anyopaque, []const u8) anyerror!OwnedSession,
        deinit_fn: *const fn (*anyopaque) void,

        pub fn load(self: Loader, model_path: []const u8) !OwnedSession {
            return self.load_fn(self.context, model_path);
        }

        pub fn deinit(self: *Loader) void {
            self.deinit_fn(self.context);
            self.* = undefined;
        }
    };

    sessions: []?OwnedSession,
    in_use: []bool,
    model_path: []u8,
    loader: Loader,
    allocator: std.mem.Allocator,
    size: usize,

    /// Construct a pool by taking ownership of a resource-aware loader. The
    /// loader context remains alive until pool deinit, so lazy acquisition
    /// cannot retain caller stack state.
    pub fn initWithLoader(
        allocator: std.mem.Allocator,
        loader: Loader,
        model_path: []const u8,
        size: usize,
    ) !SessionPool {
        var owned_loader = loader;
        errdefer owned_loader.deinit();
        const owned_model_path = try allocator.dupe(u8, model_path);
        errdefer allocator.free(owned_model_path);
        const sessions = try allocator.alloc(?OwnedSession, size);
        errdefer allocator.free(sessions);
        @memset(sessions, null);
        const in_use = try allocator.alloc(bool, size);
        @memset(in_use, false);

        return .{
            .sessions = sessions,
            .in_use = in_use,
            .model_path = owned_model_path,
            .loader = owned_loader,
            .allocator = allocator,
            .size = size,
        };
    }

    pub fn deinit(self: *SessionPool) void {
        for (self.sessions) |maybe_session| {
            if (maybe_session) |owned| {
                var session = owned;
                session.deinit();
            }
        }
        self.allocator.free(self.sessions);
        self.allocator.free(self.in_use);
        self.allocator.free(self.model_path);
        self.loader.deinit();
    }

    /// Acquire a session from the pool. Creates lazily if needed.
    /// Returns error.PoolExhausted if all sessions are in use.
    pub fn acquire(self: *SessionPool) !Session {
        // Find an available slot
        for (self.sessions, self.in_use) |*maybe_session, *used| {
            if (!used.*) {
                // Create session lazily on first use
                if (maybe_session.* == null) {
                    maybe_session.* = try self.loader.load(self.model_path);
                }
                used.* = true;
                return maybe_session.*.?.session;
            }
        }
        return error.PoolExhausted;
    }

    /// Release a session back to the pool.
    pub fn release(self: *SessionPool, session: Session) void {
        for (self.sessions, self.in_use) |maybe_session, *used| {
            if (maybe_session) |owned| {
                if (owned.session.ptr == session.ptr) {
                    used.* = false;
                    return;
                }
            }
        }
    }

    /// Number of sessions currently in use.
    pub fn activeCount(self: *const SessionPool) usize {
        var count: usize = 0;
        for (self.in_use) |used| {
            if (used) count += 1;
        }
        return count;
    }

    /// Number of sessions available (created but not in use).
    pub fn availableCount(self: *const SessionPool) usize {
        var count: usize = 0;
        for (self.sessions, self.in_use) |maybe_session, used| {
            if (maybe_session != null and !used) count += 1;
        }
        return count;
    }
};

test "pool acquire release" {
    // Basic test with no real sessions — just verify the bookkeeping
    const allocator = std.testing.allocator;

    // We can't create real sessions without a model, so just test init/deinit
    const TestLoader = struct {
        fn load(_: *anyopaque, _: []const u8) !SessionPool.OwnedSession {
            return error.FileNotFound;
        }
        fn deinit(_: *anyopaque) void {}
    };
    var context: u8 = 0;
    const input_path = try allocator.dupe(u8, "/nonexistent");
    var pool = try SessionPool.initWithLoader(allocator, .{
        .context = &context,
        .load_fn = TestLoader.load,
        .deinit_fn = TestLoader.deinit,
    }, input_path, 2);
    defer pool.deinit();
    allocator.free(input_path);

    try std.testing.expectEqualStrings("/nonexistent", pool.model_path);
    try std.testing.expectEqual(@as(usize, 0), pool.activeCount());
    try std.testing.expectEqual(@as(usize, 0), pool.availableCount());
}
