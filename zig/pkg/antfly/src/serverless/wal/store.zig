// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const Allocator = std.mem.Allocator;
const wal_types = @import("types.zig");

pub const WalStore = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        deinit: *const fn (Allocator, *anyopaque) void,
        append: *const fn (*anyopaque, []const u8, u64, []const u8) anyerror!u64,
        read_from_alloc: *const fn (*anyopaque, Allocator, []const u8, u64) anyerror![]wal_types.Record,
        latest_lsn: *const fn (*anyopaque, []const u8) anyerror!u64,
        truncate_prefix: *const fn (*anyopaque, []const u8, u64) anyerror!u64,
    };

    pub fn deinit(self: *WalStore) void {
        self.vtable.deinit(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn append(self: *WalStore, namespace: []const u8, timestamp_ns: u64, payload: []const u8) !u64 {
        return try self.vtable.append(self.ptr, namespace, timestamp_ns, payload);
    }

    pub fn readFromAlloc(self: *WalStore, namespace: []const u8, start_lsn: u64) ![]wal_types.Record {
        return try self.vtable.read_from_alloc(self.ptr, self.allocator, namespace, start_lsn);
    }

    pub fn latestLsn(self: *WalStore, namespace: []const u8) !u64 {
        return try self.vtable.latest_lsn(self.ptr, namespace);
    }

    pub fn truncatePrefix(self: *WalStore, namespace: []const u8, keep_from_lsn: u64) !u64 {
        return try self.vtable.truncate_prefix(self.ptr, namespace, keep_from_lsn);
    }
};
