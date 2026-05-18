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
const SessionManager = @import("backends.zig").SessionManager;

pub const SessionPool = struct {
    sessions: []?Session,
    in_use: []bool,
    model_path: []const u8,
    session_manager: *SessionManager,
    allocator: std.mem.Allocator,
    size: usize,

    pub fn init(allocator: std.mem.Allocator, session_manager: *SessionManager, model_path: []const u8, size: usize) !SessionPool {
        const sessions = try allocator.alloc(?Session, size);
        @memset(sessions, null);
        const in_use = try allocator.alloc(bool, size);
        @memset(in_use, false);

        return .{
            .sessions = sessions,
            .in_use = in_use,
            .model_path = model_path,
            .session_manager = session_manager,
            .allocator = allocator,
            .size = size,
        };
    }

    pub fn deinit(self: *SessionPool) void {
        for (self.sessions) |maybe_session| {
            if (maybe_session) |s| s.close();
        }
        self.allocator.free(self.sessions);
        self.allocator.free(self.in_use);
    }

    /// Acquire a session from the pool. Creates lazily if needed.
    /// Returns error.PoolExhausted if all sessions are in use.
    pub fn acquire(self: *SessionPool) !Session {
        // Find an available slot
        for (self.sessions, self.in_use) |*maybe_session, *used| {
            if (!used.*) {
                // Create session lazily on first use
                if (maybe_session.* == null) {
                    maybe_session.* = try self.session_manager.loadModel(self.model_path);
                }
                used.* = true;
                return maybe_session.*.?;
            }
        }
        return error.PoolExhausted;
    }

    /// Release a session back to the pool.
    pub fn release(self: *SessionPool, session: Session) void {
        for (self.sessions, self.in_use) |maybe_session, *used| {
            if (maybe_session) |s| {
                if (s.ptr == session.ptr) {
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
    var sm = @import("backends.zig").SessionManager.init(allocator);
    var pool = try SessionPool.init(allocator, &sm, "/nonexistent", 2);
    defer pool.deinit();

    try std.testing.expectEqual(@as(usize, 0), pool.activeCount());
    try std.testing.expectEqual(@as(usize, 0), pool.availableCount());
}
