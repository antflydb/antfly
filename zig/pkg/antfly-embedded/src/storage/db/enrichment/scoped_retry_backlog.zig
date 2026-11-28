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

// Sleeping retry tickets have a separate disk allowance from the active wave.
// Only the fixed-width index is resident; ticket payloads are read on demand.
const std = @import("std");
const binary = @import("scoped_retry_encoding.zig");
const Allocator = std.mem.Allocator;

pub const max_entries = 4096;
pub const max_bytes = 256 * 1024 * 1024;
pub const magic = "ESQ6";
const index_magic = "ESI1";
const key_prefix = "\x00\x00__metadata__:enrichment_scoped_retry_backlog:v1:";
pub const index_key = key_prefix ++ "index";

pub const Identity = struct {
    document: [32]u8,
    sequence: u64,

    pub fn init(document: []const u8, sequence: u64) Identity {
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(document, &hash, .{});
        return .{ .document = hash, .sequence = sequence };
    }
    pub fn order(left: Identity, right: Identity) std.math.Order {
        const result = std.mem.order(u8, &left.document, &right.document);
        return if (result == .eq) std.math.order(left.sequence, right.sequence) else result;
    }
    pub fn key(self: Identity) [key_prefix.len + 40]u8 {
        var suffix: [40]u8 = undefined;
        @memcpy(suffix[0..32], &self.document);
        std.mem.writeInt(u64, suffix[32..40], self.sequence, .big);
        return key_prefix.* ++ suffix;
    }
};

pub const Entry = struct {
    id: Identity,
    deadline_ms: u64,
    bytes: usize,
    charged_bytes: usize,
    replan: bool = false,
};

const entry_bytes = 8 + 32 + 8 + 4 + 4 + 1;
const header_bytes = 4 + 8 + 8 + 32 + 8 + 2;
pub const empty_identity: Identity = .{ .document = @splat(0), .sequence = 0 };

pub const Backlog = struct {
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    charged_bytes: usize = 0,
    dirty: bool = false,
    /// Deletion need not appear in the replay journal. Visit a bounded page
    /// after primary activity, retaining the reclamation frontier on disk.
    prune_through: u64 = 0,
    prune_target: u64 = 0,
    prune_after: Identity = empty_identity,
    puts: std.ArrayListUnmanaged(Put) = .empty,
    deletes: std.ArrayListUnmanaged(Identity) = .empty,

    const Put = struct { id: Identity, bytes: []u8 };

    pub fn deinit(self: *Backlog, a: Allocator) void {
        self.entries.deinit(a);
        self.clearChanges(a);
        self.puts.deinit(a);
        self.deletes.deinit(a);
    }

    pub fn clearChanges(self: *Backlog, a: Allocator) void {
        for (self.puts.items) |put| a.free(put.bytes);
        self.puts.clearRetainingCapacity();
        self.deletes.clearRetainingCapacity();
        self.dirty = false;
    }

    pub fn canAdd(self: Backlog, bytes: usize) bool {
        return self.entries.items.len < max_entries and bytes <= max_bytes -| self.charged_bytes;
    }

    pub fn lowerBound(self: Backlog, id: Identity) usize {
        var lo: usize = 0;
        var hi = self.entries.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (Identity.order(self.entries.items[mid].id, id) == .lt) lo = mid + 1 else hi = mid;
        }
        return lo;
    }

    pub fn insert(self: *Backlog, a: Allocator, entry: Entry, raw: []u8) !void {
        const index = self.lowerBound(entry.id);
        if (index < self.entries.items.len and Identity.order(self.entries.items[index].id, entry.id) == .eq)
            return error.InvalidEnrichmentState;
        try self.puts.ensureUnusedCapacity(a, 1);
        try self.entries.insert(a, index, entry);
        self.puts.appendAssumeCapacity(.{ .id = entry.id, .bytes = raw });
        self.charged_bytes += entry.charged_bytes;
        self.dirty = true;
    }

    pub fn remove(self: *Backlog, a: Allocator, index: usize) !void {
        const entry = self.entries.items[index];
        for (self.puts.items, 0..) |put, i| {
            if (Identity.order(put.id, entry.id) != .eq) continue;
            a.free(self.puts.swapRemove(i).bytes);
            break;
        } else try self.deletes.append(a, entry.id);
        self.charged_bytes -= entry.charged_bytes;
        _ = self.entries.orderedRemove(index);
        self.dirty = true;
    }

    pub fn decode(a: Allocator, raw: []const u8) !Backlog {
        if (raw.len < header_bytes or raw.len > header_bytes + max_entries * entry_bytes or
            !std.mem.startsWith(u8, raw, index_magic)) return error.InvalidEnrichmentState;
        var reader = binary.Reader{ .bytes = raw, .offset = 4 };
        var self = Backlog{};
        errdefer self.deinit(a);
        self.prune_through = try reader.int(u64);
        self.prune_target = try reader.int(u64);
        self.prune_after = .{ .document = (try reader.take(32))[0..32].*, .sequence = try reader.int(u64) };
        const n = try reader.int(u16);
        if (n > max_entries or raw.len != header_bytes + @as(usize, n) * entry_bytes)
            return error.InvalidEnrichmentState;
        try self.entries.ensureTotalCapacity(a, n);
        for (0..n) |_| {
            const entry = Entry{
                .id = .{ .document = (try reader.take(32))[0..32].*, .sequence = try reader.int(u64) },
                .deadline_ms = try reader.int(u64),
                .bytes = try reader.int(u32),
                .charged_bytes = try reader.int(u32),
                .replan = switch (try reader.int(u8)) {
                    0 => false,
                    1 => true,
                    else => return error.InvalidEnrichmentState,
                },
            };
            if (entry.bytes == 0 or entry.bytes > entry.charged_bytes or !self.canAdd(entry.charged_bytes)) return error.InvalidEnrichmentState;
            if (self.entries.getLastOrNull()) |previous| {
                if (Identity.order(previous.id, entry.id) != .lt) return error.InvalidEnrichmentState;
            }
            self.entries.appendAssumeCapacity(entry);
            self.charged_bytes += entry.charged_bytes;
        }
        try reader.end();
        return self;
    }

    pub fn write(self: Backlog, a: Allocator, txn: anytype) !void {
        if (!self.dirty) return;
        for (self.deletes.items) |id| txn.delete(&id.key()) catch |err| if (err != error.NotFound) {
            return err;
        };
        for (self.puts.items) |put| try txn.put(&put.id.key(), put.bytes);
        if (self.entries.items.len == 0) {
            txn.delete(index_key) catch |err| if (err != error.NotFound) return err;
            return;
        }
        const raw = try a.alloc(u8, header_bytes + self.entries.items.len * entry_bytes);
        defer a.free(raw);
        var writer = binary.Writer{ .bytes = raw };
        writer.put(index_magic);
        writer.int(u64, self.prune_through);
        writer.int(u64, self.prune_target);
        writer.put(&self.prune_after.document);
        writer.int(u64, self.prune_after.sequence);
        writer.int(u16, @intCast(self.entries.items.len));
        for (self.entries.items) |entry| {
            writer.put(&entry.id.document);
            writer.int(u64, entry.id.sequence);
            writer.int(u64, entry.deadline_ms);
            writer.int(u32, @intCast(entry.bytes));
            writer.int(u32, @intCast(entry.charged_bytes));
            writer.int(u8, @intFromBool(entry.replan));
        }
        try txn.put(index_key, raw);
    }
};
