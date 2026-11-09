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

//! A scalar view of one unnamed transactional namespace. Ownership of the
//! namespace backend remains explicit; transactions independently retain their
//! snapshots. This adapts native index namespaces without an intermediate LSM.
const std = @import("std");
const erased = @import("backend_erased.zig");
const types = @import("backend_types.zig");
const Allocator = std.mem.Allocator;

pub const Backend = struct {
    allocator: Allocator,
    namespaces: erased.NamespaceStore,
    serialized_write_mutex: std.atomic.Mutex = .unlocked,

    /// Ownership transfers only on success.
    pub fn init(allocator: Allocator, namespaces: erased.NamespaceStore) !*Backend {
        const self = try allocator.create(Backend);
        self.* = .{ .allocator = allocator, .namespaces = namespaces };
        return self;
    }
    pub fn close(self: *Backend) void {
        self.namespaces.deinit();
        self.allocator.destroy(self);
    }
    pub fn runtimeStore(self: *Backend, allocator: Allocator) !erased.Store {
        var store = try erased.storeFrom(allocator, View{ .backend = self });
        store.write_gate = &self.serialized_write_mutex;
        return store;
    }
    pub fn capabilities(self: *Backend) types.Capabilities {
        var result = self.namespaces.capabilities();
        result.native_namespaces = false;
        result.ordered_ranges = true;
        result.cursors = true;
        result.write_batches = .atomic;
        result.read_snapshots = .snapshot;
        return result;
    }
    pub fn beginRead(self: *Backend) !Read {
        return .{ .txn = try self.namespaces.beginRead() };
    }
    pub fn beginWrite(self: *Backend) !Write {
        return .{ .txn = try self.namespaces.beginWrite() };
    }
    pub fn beginBatch(self: *Backend) !Write {
        return self.beginWrite();
    }
    pub fn beginBatchWithOptions(self: *Backend, _: types.BatchOptions) !Write {
        return self.beginWrite();
    }
};
const View = struct {
    backend: *Backend,
    pub fn capabilities(self: *View) types.Capabilities {
        return self.backend.capabilities();
    }
    pub fn beginRead(self: *View) !Read {
        return self.backend.beginRead();
    }
    pub fn beginWrite(self: *View) !Write {
        return self.backend.beginWrite();
    }
    pub fn beginBatch(self: *View) !Write {
        return self.backend.beginBatch();
    }
    pub fn beginBatchWithOptions(self: *View, options: types.BatchOptions) !Write {
        return self.backend.beginBatchWithOptions(options);
    }
};
const Read = struct {
    txn: erased.NamespaceReadTxn,
    borrowed: bool = false,
    pub fn abort(self: *Read) void {
        if (!self.borrowed) self.txn.abort();
        self.* = undefined;
    }
    /// The erased scalar wrapper pins the original owner for every fork. The
    /// native namespace serializes its arena allocations; cursor scratch is
    /// private to each cursor and native page-cache reads are synchronized.
    pub fn forkBorrowedRead(self: *Read) !Read {
        return .{ .txn = self.txn, .borrowed = true };
    }
    pub fn get(self: *Read, key: []const u8) ![]const u8 {
        return self.txn.get(.{}, key);
    }
    pub fn getManySorted(self: *Read, keys: []const []const u8, values: []?[]const u8) !void {
        return self.txn.getManySorted(.{}, keys, values);
    }
    pub fn openCursor(self: *Read) !erased.Cursor {
        return self.txn.openCursor(.{});
    }
};
const Write = struct {
    txn: erased.NamespaceWriteTxn,
    pub fn abort(self: *Write) void {
        self.txn.abort();
        self.* = undefined;
    }
    pub fn commit(self: *Write) !void {
        try self.txn.commit();
        self.* = undefined;
    }
    pub fn get(self: *Write, key: []const u8) ![]const u8 {
        return self.txn.get(.{}, key);
    }
    pub fn getManySorted(self: *Write, keys: []const []const u8, values: []?[]const u8) !void {
        return self.txn.getManySorted(.{}, keys, values);
    }
    pub fn put(self: *Write, key: []const u8, value: []const u8) !void {
        return self.txn.put(.{}, key, value);
    }
    pub fn appendPut(self: *Write, key: []const u8, value: []const u8) !void {
        return self.txn.appendPut(.{}, key, value);
    }
    pub fn delete(self: *Write, key: []const u8) !void {
        return self.txn.delete(.{}, key);
    }
    pub fn openCursor(self: *Write) !erased.Cursor {
        return self.txn.openCursor(.{});
    }
};
