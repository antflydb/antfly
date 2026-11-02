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

//! Typed statement-local relations. These identities never enter the catalog.
//! Binding metadata can be retained with a plan; execution callbacks cannot.
const std = @import("std");
const catalog = @import("catalog.zig");

pub const Definition = struct {
    name: []const u8,
    columns: []const @import("describe.zig").Column = &.{},
    returns_rows: bool = false,
    /// Pure single-use read definitions retain the ordinary inlining path.
    inline_read: bool = false,
    ordered_effects: bool = false,
    bound: bool = false,
};
pub const Scope = struct {
    definitions: []const Definition,
    binding_scope: usize,

    pub fn lookup(self: Scope, name: []const u8, visible: usize) !?usize {
        for (self.definitions[0..@min(visible, self.definitions.len)], 0..) |definition, i| {
            if (!std.mem.eql(u8, definition.name, name)) continue;
            if (definition.inline_read) return null;
            if (!definition.bound) return error.SqlInvalidRecursion;
            if (!definition.returns_rows) return error.SqlCteReturningRequired;
            return i;
        }
        return null;
    }
};

/// One command owns these callbacks, its typed replay and tuple directory.
/// The backend's native transaction remains the sole publication authority.
pub const PendingTuple = struct {
    key: []const u8,
    version: u64,
    digest: ?[32]u8,
    retired: bool,
};

pub const Execution = struct {
    ptr: *anyopaque,
    open: *const fn (*anyopaque, usize) anyerror!*@import("replay_rows.zig").Replay.Reader,
    claim: *const fn (*anyopaque, std.mem.Allocator, catalog.Table, []const catalog.Mutation, []const u8) anyerror![]const catalog.Mutation,
    checkpoint: *const fn (*anyopaque) anyerror!void,
    scan_rows: *const fn (*anyopaque, usize) anyerror!void,
    scan_page: *const fn (*anyopaque) anyerror!void,
    pending_owner: *const fn (*anyopaque, catalog.Table, []const u8) ?PendingTuple,
};
