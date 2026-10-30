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

//! Shared transactional-root authority. Journal namespaces preserve their exact
//! protocol; new conditional namespaces replace one permanent root atomically.
const std = @import("std");
const journal = @import("immutable_log.zig");
const conditional = @import("conditional_root.zig");
const A = std.mem.Allocator;
pub const Backend = enum { journal, conditional };
pub const Authority = struct {
    log: journal.Log,
    root: ?conditional.Context = null,
    cursor: ?conditional.Cursor = null,

    fn accept(self: *Authority, value: conditional.Snapshot) journal.Recovery {
        self.cursor = value.cursor();
        return .{ .state = value.state };
    }
    pub fn recover(self: *Authority, a: A, state: ?journal.State, takeover: bool) !journal.Recovery {
        const root = self.root orelse return self.log.recover(a, state, takeover);
        if (takeover) return self.accept(try root.begin(a));
        const current = (try root.observe(a)) orelse return error.ConditionalRootMissing;
        if (current.retired) return error.ConditionalRootRetired;
        if (!std.mem.eql(u8, &current.state.owner, &root.owner)) return error.ImmutableLogFenced;
        return self.accept(current);
    }
    pub fn observe(self: *Authority, a: A, state: journal.State) !journal.Recovery {
        const root = self.root orelse return self.log.observe(a, state);
        const current = (try root.observe(a)) orelse return error.ConditionalRootMissing;
        if (current.retired) return error.ConditionalRootRetired;
        if (!std.mem.eql(u8, &current.state.owner, &root.owner)) return error.ImmutableLogFenced;
        return self.accept(current);
    }
    pub fn observeCommitted(self: *Authority, a: A, state: ?journal.State) !journal.Recovery {
        const root = self.root orelse return self.log.observeCommitted(a, state);
        const current = (try root.observe(a)) orelse return error.ConditionalRootMissing;
        if (current.retired) return error.ConditionalRootRetired;
        return self.accept(current);
    }
    fn snapshot(self: Authority, state: journal.State) !conditional.Snapshot {
        const c = self.cursor orelse return error.ConditionalRootRevisionRequired;
        return .{ .state = state, .revision = c.revision, .nonce = c.nonce, .retired = c.retired, .auth = c.auth };
    }
    pub fn append(self: *Authority, a: A, state: journal.State, bytes: []const u8) !journal.State {
        const root = self.root orelse return self.log.append(a, state, bytes);
        return self.accept(try root.append(a, try self.snapshot(state), bytes)).state;
    }
    pub fn writeCheckpoint(self: *Authority, a: A, state: journal.State, force: bool) !bool {
        var root = self.root orelse return self.log.writeCheckpoint(a, state, force);
        // A GC barrier must still authenticate current ownership. ROOT already
        // is the durable recovery floor; it never needs a second checkpoint PUT.
        root.proofs = null;
        root.proof_key = null;
        root.proof_scope = @splat(0);
        try root.verifySource(a, try self.snapshot(state));
        return true;
    }
    pub fn retirable(self: *Authority, a: A, keys: []const []const u8, before: u64) ![]const []const u8 {
        if (self.root != null) return &.{};
        return self.log.retirable(a, keys, before);
    }
};
