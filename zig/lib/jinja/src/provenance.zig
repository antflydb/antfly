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

//! Request-arena byte provenance. Zero denotes protected template/metadata;
//! nonzero labels identify explicitly selected payload parts. Selected inputs
//! are copied so identical strings (even aliased caller storage) stay distinct.
const std = @import("std");

pub const Provenance = struct {
    arena: std.mem.Allocator,
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    charged_bytes: usize = 0,
    limit_bytes: usize = 64 * 1024 * 1024,
    const Entry = struct { text: []const u8, labels: []const u32 };

    pub fn charge(self: *Provenance, bytes: usize) !void {
        if (bytes > self.limit_bytes -| self.charged_bytes) return error.ProvenanceBudgetExceeded;
        self.charged_bytes += bytes;
    }

    pub fn select(self: *Provenance, text: []const u8, label: u32) ![]const u8 {
        if (label == 0 or text.len == 0) return error.InvalidPayloadProvenance;
        try self.charge(try std.math.mul(usize, text.len, 5));
        const copy = try self.arena.dupe(u8, text);
        const labels = try self.arena.alloc(u32, text.len);
        @memset(labels, label);
        try self.register(copy, labels);
        return copy;
    }

    pub fn register(self: *Provenance, text: []const u8, labels: []const u32) !void {
        if (text.len != labels.len) return error.InvalidPayloadProvenance;
        if (text.len == 0 or std.mem.allEqual(u32, labels, 0)) return;
        try self.charge(@sizeOf(Entry));
        try self.entries.append(self.arena, .{ .text = text, .labels = labels });
    }

    pub fn lookup(self: *const Provenance, text: []const u8) ?[]const u32 {
        if (text.len == 0) return null;
        const start = @intFromPtr(text.ptr);
        // Entries are immutable arena allocations. Slices from trim/split/index
        // inherit labels by address, never by text equality or substring search.
        for (self.entries.items) |entry| {
            const base = @intFromPtr(entry.text.ptr);
            if (start >= base and start - base <= entry.text.len and
                text.len <= entry.text.len - (start - base))
                return entry.labels[start - base ..][0..text.len];
        }
        return null;
    }

    pub fn append(self: *Provenance, output: *std.ArrayListUnmanaged(u32), text: []const u8, tracked: bool) !void {
        try self.charge(try std.math.mul(usize, text.len, @sizeOf(u32)));
        if (tracked) {
            if (self.lookup(text)) |labels| return output.appendSlice(self.arena, labels);
        }
        try output.appendNTimes(self.arena, 0, text.len);
    }

    pub fn concat(self: *Provenance, result: []const u8, left: []const u8, right: []const u8) !void {
        var labels: std.ArrayListUnmanaged(u32) = .empty;
        try self.append(&labels, left, true);
        try self.append(&labels, right, true);
        try self.register(result, try labels.toOwnedSlice(self.arena));
    }
};
