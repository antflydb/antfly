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

const types = @import("types.zig");
const message = @import("message.zig");
const std = @import("std");

pub const Ready = struct {
    soft_state: ?types.SoftState = null,
    hard_state: ?types.HardState = null,
    // Set by the runtime after applying committed configuration entries. The
    // slices are borrowed from the group and must be consumed synchronously.
    conf_state: ?types.ConfState = null,
    snapshot: ?types.Snapshot = null,
    entries: []const types.Entry = &.{},
    committed_entries: []const types.Entry = &.{},
    read_states: []const types.ReadState = &.{},
    messages: []const message.Message = &.{},

    pub fn isEmpty(self: Ready) bool {
        return self.soft_state == null and
            self.hard_state == null and
            self.conf_state == null and
            self.snapshot == null and
            self.entries.len == 0 and
            self.committed_entries.len == 0 and
            self.read_states.len == 0 and
            self.messages.len == 0;
    }

    pub fn requiresPersistence(self: Ready) bool {
        return self.hard_state != null or
            self.conf_state != null or
            self.snapshot != null or
            self.entries.len > 0;
    }

    pub fn clone(self: Ready, alloc: std.mem.Allocator) !Ready {
        var owned = Ready{ .soft_state = self.soft_state, .hard_state = self.hard_state };
        errdefer owned.deinit(alloc);
        if (self.conf_state) |conf| owned.conf_state = try conf.clone(alloc);
        if (self.snapshot) |snapshot| owned.snapshot = try snapshot.clone(alloc);
        owned.entries = try types.cloneEntries(alloc, self.entries);
        owned.committed_entries = try types.cloneEntries(alloc, self.committed_entries);
        owned.messages = try message.cloneMessages(alloc, self.messages);
        const reads = try alloc.alloc(types.ReadState, self.read_states.len);
        var initialized: usize = 0;
        errdefer {
            for (reads[0..initialized]) |*read| read.deinit(alloc);
            alloc.free(reads);
        }
        for (self.read_states, 0..) |read, i| {
            reads[i] = try read.clone(alloc);
            initialized += 1;
        }
        owned.read_states = reads;
        return owned;
    }

    pub fn deinit(self: *Ready, alloc: std.mem.Allocator) void {
        if (self.conf_state) |*conf| conf.deinit(alloc);
        if (self.snapshot) |*snapshot| snapshot.deinit(alloc);
        types.freeEntries(alloc, @constCast(self.entries));
        types.freeEntries(alloc, @constCast(self.committed_entries));
        message.freeMessages(alloc, @constCast(self.messages));
        for (@constCast(self.read_states)) |*read| read.deinit(alloc);
        alloc.free(self.read_states);
        self.* = undefined;
    }
};

test "ready persistence predicate only includes durable raft state" {
    var request_ctx = [_]u8{ 'c', 't', 'x' };
    try std.testing.expect(!(Ready{ .messages = &.{.{
        .msg_type = .heartbeat,
        .from = 1,
        .to = 2,
    }} }).requiresPersistence());
    try std.testing.expect(!(Ready{ .committed_entries = &.{.{ .term = 1, .index = 1 }} }).requiresPersistence());
    try std.testing.expect(!(Ready{ .read_states = &.{.{ .index = 1, .request_ctx = &request_ctx }} }).requiresPersistence());
    try std.testing.expect((Ready{ .conf_state = .{ .voters = @constCast((&[_]u64{1})[0..]) } }).requiresPersistence());
    try std.testing.expect((Ready{ .hard_state = .{ .current_term = 2 } }).requiresPersistence());
    try std.testing.expect((Ready{ .entries = &.{.{ .term = 2, .index = 3 }} }).requiresPersistence());
    try std.testing.expect((Ready{ .snapshot = .{} }).requiresPersistence());
}
