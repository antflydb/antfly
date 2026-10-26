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

//! Deterministic in-memory persistence for cross-layer document protocol tests.
const std = @import("std");
const runs = @import("document_runs.zig");
const A = std.mem.Allocator;
const Record = runs.Record;
const Version = runs.Version;
const Scope = runs.Scope;
const Ref = runs.Ref;
const Digest = runs.Digest;
pub const Store = struct {
    const TestStore = @This();
    mutex: std.atomic.Mutex = .unlocked,
    a: A,
    metadata: ?[]u8 = null,
    blobs: std.StringHashMapUnmanaged([]u8) = .empty,
    reject_once: bool = false,
    cas_error_once: bool = false,
    corrupt: bool = false,
    entropy: u8 = 0,
    fn nonce(raw: *anyopaque, bytes: []u8) void {
        const self: *TestStore = @ptrCast(@alignCast(raw));
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        self.entropy +%= 1;
        @memset(bytes, self.entropy);
    }
    pub fn interface(self: *TestStore) runs.Store {
        return .{ .ptr = self, .load_fn = load, .cas_fn = cas, .nonce_fn = nonce, .put_fn = put, .get_fn = get };
    }
    pub fn deinit(self: *TestStore) void {
        if (self.metadata) |bytes| self.a.free(bytes);
        var it = self.blobs.iterator();
        while (it.next()) |entry| {
            self.a.free(entry.key_ptr.*);
            self.a.free(entry.value_ptr.*);
        }
        self.blobs.deinit(self.a);
    }
    fn load(raw: *anyopaque, a: A, id: []const u8, owner: []const u8) !?Record {
        const self: *TestStore = @ptrCast(@alignCast(raw));
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        const bytes = self.metadata orelse return null;
        const record = try std.json.parseFromSliceLeaky(Record, a, bytes, .{ .allocate = .alloc_always });
        if (!std.mem.eql(u8, record.run_id, id) or !std.mem.eql(u8, record.owner, owner)) return null;
        return record;
    }
    fn cas(raw: *anyopaque, expected: ?Version, record: Record) !bool {
        const self: *TestStore = @ptrCast(@alignCast(raw));
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (self.cas_error_once) {
            self.cas_error_once = false;
            return error.TestStorageUnavailable;
        }
        if (self.reject_once and expected != null) {
            self.reject_once = false;
            return false;
        }
        if (self.metadata) |bytes| {
            var previous = try std.json.parseFromSlice(Record, self.a, bytes, .{});
            defer previous.deinit();
            if (expected == null or !std.meta.eql(previous.value.version(), expected.?) or !std.mem.eql(u8, previous.value.owner, record.owner) or record.revision != expected.?.revision + 1) return false;
        } else if (expected != null) return false;
        const bytes = try std.json.Stringify.valueAlloc(self.a, record, .{});
        if (self.metadata) |old| self.a.free(old);
        self.metadata = bytes;
        return true;
    }
    fn put(raw: *anyopaque, a: A, _: Scope, bytes: []const u8) !Ref {
        const self: *TestStore = @ptrCast(@alignCast(raw));
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        var hash: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
        const id = std.fmt.bytesToHex(hash, .lower);
        if (!self.blobs.contains(&id)) {
            const copy = try self.a.dupe(u8, bytes);
            errdefer self.a.free(copy);
            const key = try self.a.dupe(u8, &id);
            errdefer self.a.free(key);
            try self.blobs.put(self.a, key, copy);
        }
        return .{ .id = try a.dupe(u8, &id), .sha256 = hash, .bytes = bytes.len };
    }
    fn get(raw: *anyopaque, a: A, ref: Ref, max_bytes: usize) ![]u8 {
        const self: *TestStore = @ptrCast(@alignCast(raw));
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        const bytes = self.blobs.get(ref.id) orelse return error.NotFound;
        if (bytes.len > max_bytes) return error.DocumentOutputTooLarge;
        return a.dupe(u8, if (self.corrupt) "corrupt" else bytes);
    }
};
