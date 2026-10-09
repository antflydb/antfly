// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Before-image journal for one metadata command inside a larger storage
//! transaction. A rejected catalog proposal can restore its own writes while
//! retaining earlier accepted commands. This is NOT a user-row savepoint:
//! native row/index/artifact side effects require their own complete protocol.
const std = @import("std");
const local = @import("antfly_local_sources");
const Txn = local.storage_docstore.DocStore.Txn;
const Capture = local.storage_txn_mutation_capture.Capture;

pub const Journal = struct {
    const max_bytes = 64 * 1024 * 1024;
    const max_keys = 65536;
    txn: *Txn,
    arena: std.heap.ArenaAllocator,
    capture: Capture,
    originals: std.StringHashMapUnmanaged(?[]const u8) = .empty,
    bytes: usize = 0,
    parent: ?*Capture = null,
    attached: bool = false,
    started: bool = false,
    verification_filter: ?struct {
        group_id: u64,
        call: *const fn (u64, []const u8) anyerror!bool,
    } = null,

    pub fn init(a: std.mem.Allocator, txn: *Txn) Journal {
        return .{ .txn = txn, .arena = .init(a), .capture = Capture.init(a) };
    }
    /// Replay verification needs before images only for authoritative inputs
    /// and ownership effects, not unrelated status/report payloads. This mode
    /// cannot perform command rollback: on failure abort the enclosing txn.
    pub fn initVerification(a: std.mem.Allocator, txn: *Txn, group_id: u64, filter: *const fn (u64, []const u8) anyerror!bool) Journal {
        var journal = init(a, txn);
        journal.verification_filter = .{ .group_id = group_id, .call = filter };
        return journal;
    }
    /// Attach only after the journal reaches its stable address. Detach before
    /// ending the transaction. Abandoning an attached journal requires aborting
    /// that transaction; deinit alone does not roll back durable intentions.
    pub fn attach(self: *Journal) !void {
        if (self.started or self.txn.write == null) return error.InvalidMetadataCommandJournal;
        if (self.txn.mutation_capture) |parent| if (parent.on_first_touch != null) return error.InvalidMetadataCommandJournal;
        self.parent = self.txn.mutation_capture;
        self.capture.on_first_touch = .{ .ptr = self, .call = observe };
        self.txn.mutation_capture = &self.capture;
        self.attached = true;
        self.started = true;
    }
    pub fn deinit(self: *Journal) void {
        if (self.attached) self.detach();
        self.capture.deinit();
        self.arena.deinit();
        self.* = undefined;
    }
    pub const BeforeReader = struct {
        journal: *Journal,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            if (self.journal.originals.get(key)) |before| return before orelse error.NotFound;
            return self.journal.txn.get(key);
        }
    };
    pub fn beforeReader(self: *Journal) BeforeReader {
        return .{ .journal = self };
    }
    fn detach(self: *Journal) void {
        std.debug.assert(self.txn.mutation_capture == &self.capture);
        self.txn.mutation_capture = self.parent;
        self.attached = false;
    }
    fn observe(ptr: *anyopaque, key: []const u8) !void {
        const self: *Journal = @ptrCast(@alignCast(ptr));
        if (!std.mem.startsWith(u8, key, "\x00\x00__metadata__:") and
            !std.mem.startsWith(u8, key, "\x00\x00__metadata_derived__:")) return error.MetadataCommandMutationScope;
        if (self.verification_filter) |filter| if (!try filter.call(filter.group_id, key)) return;
        if (self.originals.contains(key)) return;
        if (self.originals.count() == max_keys) return error.MetadataCommandTooLarge;
        const before = self.txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        var bytes = std.math.add(usize, self.bytes, key.len + @sizeOf(?[]const u8)) catch return error.MetadataCommandTooLarge;
        if (before) |value| bytes = std.math.add(usize, bytes, value.len) catch return error.MetadataCommandTooLarge;
        if (bytes > max_bytes) return error.MetadataCommandTooLarge;
        const a = self.arena.allocator();
        const name = try a.dupe(u8, key);
        const value = if (before) |old| try a.dupe(u8, old) else null;
        try self.originals.put(a, name, value);
        self.bytes = bytes;
    }
    /// Publish accepted keys into the enclosing final-effect capture only now.
    /// Rejected commands never inflate standby effects with restored values.
    /// On error the enclosing transaction must abort.
    pub fn accept(self: *Journal) !void {
        if (!self.attached) return error.InvalidMetadataCommandJournal;
        self.detach();
        if (self.parent) |parent| {
            var it = self.capture.keys.keyIterator();
            while (it.next()) |key| try parent.touch(key.*);
        }
    }
    /// Restore first-touch values, including earlier commands' pending puts.
    /// An I/O or allocation failure requires aborting the enclosing transaction.
    pub fn rollback(self: *Journal) !void {
        if (!self.attached or self.verification_filter != null) return error.InvalidMetadataCommandJournal;
        self.detach();
        self.txn.mutation_capture = null;
        defer self.txn.mutation_capture = self.parent;
        var it = self.originals.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.*) |value| {
                try self.txn.put(entry.key_ptr.*, value);
            } else self.txn.delete(entry.key_ptr.*) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
    }
};
