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

//! Authoritative job state shared by resumable workflow owners. Scheduling is
//! borrowed from BackendRuntime; this capability owns only bounded state CAS.
const std = @import("std");
const memory = @import("runtime_memory_abi");
const errors = @import("antfly_runtime_abi").error_abi;
const A = std.mem.Allocator;
pub const max_value_bytes = 512 * 1024;
pub const max_page_rows = 32;
pub const max_active_per_owner: u64 = 16;
pub const Row = struct { key: []const u8, value: []const u8 };
pub const Page = struct { rows: []const Row, next: ?[]const u8 = null };
pub fn validateKey(key: []const u8) !void {
    if (key.len == 0 or key.len > 128) return error.InvalidDocumentCheckpoint;
    for (key) |byte| if (!std.ascii.isAlphanumeric(byte) and byte != '/' and byte != '_' and byte != '-' and byte != ':') return error.InvalidDocumentCheckpoint;
}
pub fn digest(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}
/// No callbacks cross the native boundary with Zig allocator/error layouts.
/// Context is borrowed until the API owner drains its maintenance registrations.
pub const Persistence = extern struct {
    version: u32 = 1,
    struct_size: u32 = @sizeOf(@This()),
    context: *anyopaque,
    get_fn: *const fn (*anyopaque, *const memory.Allocator, memory.Bytes, *memory.OptionalOwnedBytes) callconv(.c) errors.Status,
    scan_fn: *const fn (*anyopaque, *const memory.Allocator, memory.Bytes, *memory.OwnedBytes) callconv(.c) errors.Status,
    cas_fn: *const fn (*anyopaque, memory.Bytes, memory.Bytes, memory.Bytes, u64, *u8) callconv(.c) errors.Status,
    pub fn disabled() Persistence {
        return .{ .version = 0, .context = undefined, .get_fn = undefined, .scan_fn = undefined, .cas_fn = undefined };
    }
    pub fn valid(self: Persistence) bool {
        return self.version == 1 and self.struct_size == @sizeOf(Persistence);
    }
    pub const Local = struct {
        get: *const fn (*anyopaque, A, []const u8) anyerror!?[]u8,
        scan: *const fn (*anyopaque, A, ?[]const u8) anyerror![]u8,
        cas: *const fn (*anyopaque, []const u8, ?[]const u8, []const u8, u64) anyerror!bool,
    };
    pub fn fromLocal(context: *anyopaque, comptime local: Local) Persistence {
        const Adapter = struct {
            fn failure(err: anyerror) errors.Status {
                return errors.statusFromErrorWithFallback(err, error.RestoreJobPersistenceUnavailable);
            }
            fn get(raw: *anyopaque, allocator: *const memory.Allocator, key: memory.Bytes, out: *memory.OptionalOwnedBytes) callconv(.c) errors.Status {
                if (!allocator.valid()) return failure(error.UnsupportedVersion);
                const bytes = local.get(raw, allocator.asStd(), key.slice()) catch |err| return failure(err);
                out.* = if (bytes) |value| .{ .bytes = .{ .ptr = value.ptr, .len = value.len }, .present = 1 } else .{};
                return .ok;
            }
            fn scan(raw: *anyopaque, allocator: *const memory.Allocator, after: memory.Bytes, out: *memory.OwnedBytes) callconv(.c) errors.Status {
                if (!allocator.valid()) return failure(error.UnsupportedVersion);
                const value = local.scan(raw, allocator.asStd(), if (after.len == 0) null else after.slice()) catch |err| return failure(err);
                out.* = .{ .ptr = value.ptr, .len = value.len };
                return .ok;
            }
            fn cas(raw: *anyopaque, key: memory.Bytes, expected: memory.Bytes, value: memory.Bytes, term: u64, out: *u8) callconv(.c) errors.Status {
                out.* = @intFromBool(local.cas(raw, key.slice(), if (expected.len == 0) null else expected.slice(), value.slice(), term) catch |err| return failure(err));
                return .ok;
            }
        };
        return .{ .context = context, .get_fn = Adapter.get, .scan_fn = Adapter.scan, .cas_fn = Adapter.cas };
    }
    pub fn get(self: Persistence, a: A, key: []const u8) !?[]u8 {
        if (!self.valid()) return error.UnsupportedVersion;
        try validateKey(key);
        var backing = a;
        var allocator = memory.Allocator.fromStd(&backing);
        var out: memory.OptionalOwnedBytes = .{};
        try check(self.get_fn(self.context, &allocator, .fromSlice(key), &out));
        return if (out.present != 0) out.bytes.slice() else null;
    }
    pub fn scan(self: Persistence, a: A, after: ?[]const u8) ![]u8 {
        if (!self.valid()) return error.UnsupportedVersion;
        var backing = a;
        var allocator = memory.Allocator.fromStd(&backing);
        var out: memory.OwnedBytes = .{};
        try check(self.scan_fn(self.context, &allocator, .fromSlice(after orelse ""), &out));
        return out.slice();
    }
    pub fn compareExchange(self: Persistence, key: []const u8, expected: ?[]const u8, value: []const u8, term: u64) !bool {
        if (!self.valid()) return error.UnsupportedVersion;
        try validateKey(key);
        if (value.len == 0 or value.len > max_value_bytes or (expected != null and expected.?.len != 32)) return error.InvalidDocumentCheckpoint;
        var out: u8 = 0;
        try check(self.cas_fn(self.context, .fromSlice(key), .fromSlice(expected orelse ""), .fromSlice(value), term, &out));
        return out != 0;
    }
};

fn check(status: errors.Status) !void {
    if (!status.isOk()) return errors.errorFromStatus(status);
}

pub const OwnerState = struct { owner: []const u8, state: enum { queued, running, succeeded, failed, cancelled }, expires_at_ms: u64 = 0, lease_until_ms: u64 = 0, created_at_ms: u64 = 0, not_before_ms: u64 = 0, maintenance_not_before_ms: u64 = 0 };
pub fn ownerState(a: A, value: []const u8) !OwnerState {
    const state = std.json.parseFromSliceLeaky(OwnerState, a, value, .{ .ignore_unknown_fields = true, .allocate = .alloc_always }) catch return error.InvalidDocumentCheckpoint;
    if (state.owner.len == 0 or state.owner.len > 4096) return error.InvalidDocumentCheckpoint;
    return state;
}
pub fn active(state: OwnerState) bool {
    return state.state == .queued or state.state == .running;
}

test "document artifact run quota errors preserve their identity across owner callbacks" {
    const Backend = struct {
        fn get(_: *anyopaque, _: A, _: []const u8) !?[]u8 {
            return null;
        }
        fn scan(_: *anyopaque, a: A, _: ?[]const u8) ![]u8 {
            return a.dupe(u8, "{\"rows\":[]}");
        }
        fn cas(_: *anyopaque, _: []const u8, _: ?[]const u8, _: []const u8, _: u64) !bool {
            return error.TooManyActiveJobs;
        }
    };
    var raw: u8 = 0;
    const state = Persistence.fromLocal(&raw, .{ .get = Backend.get, .scan = Backend.scan, .cas = Backend.cas });
    try std.testing.expectError(error.TooManyActiveJobs, state.compareExchange("documents/id", null, "{}", 1));
}
