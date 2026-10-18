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

//! Bounded block nested loops for inseparable hash partitions. Replay typed
//! build blocks once per probe block; track accepted outer matches on disk.
const std = @import("std");
const spill = @import("spill.zig");
const scalar = @import("scalar.zig");
const A = std.mem.Allocator;
pub const Pair = struct { left: ?[]const scalar.Datum, right: ?[]const scalar.Datum, match: ?usize = null };
pub const NativeRow = struct { block: *spill.Sequential.OwnedBlock, index: usize };
pub const NativePair = struct { left: ?NativeRow, right: ?NativeRow, match: ?usize = null };
pub const Join = struct {
    a: A,
    manager: *spill.Manager,
    outer_left: bool,
    outer_right: bool,
    build_input: ?spill.Sequential.InputBlock = null,
    probe_input: ?spill.Sequential.InputBlock = null,
    build_first: u64 = 0,
    build_next: u64 = 0,
    probe_next: u64 = 0,
    build_row: usize = 0,
    probe_row: usize = 0,
    unmatched_probe: usize = 0,
    probe_done: bool = false,
    last_probe: ?usize = null,
    last_match: ?usize = null,
    probe_matches: [256]bool = @splat(false),
    build_matches: [256]u8 = @splat(0),
    dirty: bool = false,
    matches: ?spill.File = null,
    output: std.heap.ArenaAllocator,
    blocks_read: usize = 0,

    pub fn create(manager: *spill.Manager, outer_left: bool, outer_right: bool) !*Join {
        const a = manager.allocator();
        const self = try a.create(Join);
        errdefer a.destroy(self);
        self.* = .{ .a = a, .manager = manager, .outer_left = outer_left, .outer_right = outer_right, .output = .init(a) };
        if (outer_right) self.matches = try manager.create();
        if (self.matches) |*file| file.buffer_bytes = 256;
        return self;
    }
    pub fn close(self: *Join) void {
        if (self.build_input) |*input| input.deinit();
        if (self.probe_input) |*input| input.deinit();
        if (self.matches) |*file| file.close();
        self.output.deinit();
        self.a.destroy(self);
    }
    fn flushMatches(self: *Join) !void {
        if (self.dirty) {
            try self.matches.?.writeRaw(self.build_first, self.build_matches[0..self.build_input.?.view().count()]);
            self.dirty = false;
        }
    }
    fn releaseBuild(self: *Join) !void {
        try self.flushMatches();
        if (self.build_input) |*input| input.deinit();
        self.build_input = null;
    }
    fn loadBuild(self: *Join, build: *spill.Sequential) !bool {
        if (self.build_input != null) return true;
        if (self.build_next == build.size) return false;
        self.build_first = self.build_next;
        self.build_input = try build.readInputBlockBorrowed(self.build_next);
        const count = self.build_input.?.view().count();
        if (count == 0 or count > 256) return error.InvalidSqlSpill;
        self.build_next += count;
        self.build_row = 0;
        self.probe_row = 0;
        self.blocks_read += 1;
        if (self.matches) |*file| {
            const flags = self.build_matches[0..count];
            if (self.build_first < file.size) {
                try file.readRaw(self.build_first, flags);
                for (flags) |flag| if (flag > 1) return error.InvalidSqlSpill;
            } else {
                @memset(flags, 0);
                try file.writeRaw(self.build_first, flags);
            }
        }
        return true;
    }
    pub fn accept(self: *Join, index: usize) !void {
        if (self.last_match != index or self.last_probe == null or self.build_input == null) return error.InvalidSqlBackendResponse;
        return self.acceptAt(index, self.last_probe.?);
    }
    /// Batched residuals identify both rows within the still-pinned block pair.
    pub fn acceptAt(self: *Join, index: usize, probe: usize) !void {
        if (self.build_input == null or self.probe_input == null or probe >= self.probe_input.?.view().count() or index < self.build_first or index - self.build_first >= self.build_input.?.view().count()) return error.InvalidSqlBackendResponse;
        self.probe_matches[probe] = true;
        if (self.outer_right) {
            self.build_matches[index - @as(usize, @intCast(self.build_first))] = 1;
            self.dirty = true;
        }
    }
    pub fn next(self: *Join, build: *spill.Sequential, probes: ?*spill.Sequential) !?Pair {
        _ = self.output.reset(.{ .retain_with_limit = 8192 });
        const a = self.output.allocator();
        const pair = try self.nextNative(build, probes, true) orelse return null;
        return .{ .left = if (pair.left) |row| (try row.block.row(a, row.index)).values else null, .right = if (pair.right) |row| (try row.block.row(a, row.index)).values else null, .match = pair.match };
    }
    /// Returned rows borrow one probe/build block pair until the next advancing pull.
    pub fn nextNative(self: *Join, build: *spill.Sequential, probes: ?*spill.Sequential, may_advance: bool) !?NativePair {
        self.last_match = null;
        self.last_probe = null;
        while (true) {
            try self.manager.check();
            if (self.probe_done) {
                if (!self.outer_right) return null;
                if (!try self.loadBuild(build)) return null;
                const block = self.build_input.?.view();
                while (self.build_row < block.count()) {
                    const row = self.build_row;
                    self.build_row += 1;
                    if (self.build_matches[row] == 0) return .{ .left = null, .right = .{ .block = block, .index = row } };
                }
                if (!may_advance) return null;
                try self.releaseBuild();
                continue;
            }
            if (self.probe_input == null) {
                if (!may_advance) return null;
                if (probes == null or self.probe_next == probes.?.size) {
                    self.probe_done = true;
                    try self.releaseBuild();
                    self.build_next = 0;
                    build.rewind();
                    continue;
                }
                self.probe_input = try probes.?.readInputBlockBorrowed(self.probe_next);
                const count = self.probe_input.?.view().count();
                if (count == 0 or count > 256) return error.InvalidSqlSpill;
                self.probe_next += count;
                @memset(&self.probe_matches, false);
                self.unmatched_probe = 0;
                self.build_next = 0;
                build.rewind();
            }
            const probe_block = self.probe_input.?.view();
            if (!try self.loadBuild(build)) {
                if (self.outer_left) while (self.unmatched_probe < probe_block.count()) {
                    const row = self.unmatched_probe;
                    self.unmatched_probe += 1;
                    if (!self.probe_matches[row]) return .{ .left = .{ .block = probe_block, .index = row }, .right = null };
                };
                if (!may_advance) return null;
                self.probe_input.?.deinit();
                self.probe_input = null;
                continue;
            }
            const build_block = self.build_input.?.view();
            if (build_block.keyWidth() != probe_block.keyWidth()) return error.InvalidSqlSpill;
            while (self.build_row < build_block.count()) {
                try self.manager.check();
                while (self.probe_row < probe_block.count()) {
                    const row = self.probe_row;
                    self.probe_row += 1;
                    var equal = true;
                    for (0..build_block.keyWidth()) |column| {
                        const left = try probe_block.keyCell(row, column);
                        const right = try build_block.keyCell(self.build_row, column);
                        if (left.sql_null or right.sql_null or (try scalar.compareDatums(left, right)) != .eq) {
                            equal = false;
                            break;
                        }
                    }
                    if (!equal) continue;
                    const index = @as(usize, @intCast(self.build_first)) + self.build_row;
                    self.last_match = index;
                    self.last_probe = row;
                    return .{ .left = .{ .block = probe_block, .index = row }, .right = .{ .block = build_block, .index = self.build_row }, .match = index };
                }
                self.probe_row = 0;
                self.build_row += 1;
            }
            if (!may_advance) return null;
            try self.releaseBuild();
        }
    }
};
