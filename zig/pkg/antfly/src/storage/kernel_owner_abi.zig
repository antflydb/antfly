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

//! Versioned internal ABI for the storage kernel's live DB owner. Keep this
//! module free of storage and distributed-runtime imports.

pub const abi_version: u32 = 3;

pub const BorrowedBytes = extern struct {
    ptr: ?[*]const u8 = null,
    len: u64 = 0,

    pub fn fromSlice(value: []const u8) BorrowedBytes {
        return .{
            .ptr = if (value.len == 0) null else value.ptr,
            .len = @intCast(value.len),
        };
    }

    pub fn slice(self: BorrowedBytes) []const u8 {
        if (self.len == 0) return "";
        return self.ptr.?[0..@intCast(self.len)];
    }
};

pub const OwnedBytes = extern struct {
    ptr: ?[*]u8 = null,
    len: u64 = 0,

    pub fn slice(self: OwnedBytes) []const u8 {
        if (self.len == 0) return "";
        return self.ptr.?[0..@intCast(self.len)];
    }
};

pub const VersionedOwnedBytes = extern struct {
    buffer: OwnedBytes = .{},
    version: u64 = 0,
};

pub const Status = enum(u32) {
    ok = 0,
    invalid_abi = 1,
    invalid_argument = 2,
    not_found = 3,
    busy = 4,
    version_conflict = 5,
    intent_conflict = 6,
    transaction_not_found = 7,
    read_only = 8,
    out_of_memory = 9,
    corrupted = 10,
    identity_namespace_mismatch = 11,
    invalid_query = 12,
    unsupported_query = 13,
    index_not_found = 14,
    identity_read_generation_changed = 15,
    timeout = 16,
    internal = 255,
};

pub const OpenRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    path: BorrowedBytes = .{},
    table_name: BorrowedBytes = .{},
    lsm_root_generation: u64 = 0,
    has_identity_namespace: u8 = 0,
    _reserved1: [7]u8 = @splat(0),
    identity_table_id: u64 = 0,
    identity_shard_id: u64 = 0,
    identity_range_id: u64 = 0,
    schema_json: BorrowedBytes = .{},
    indexes_json: BorrowedBytes = .{},
};

pub const JsonOperationRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    table_name: BorrowedBytes = .{},
    request_json: BorrowedBytes = .{},
};

pub extern fn antfly_storage_owner_open(
    request: *const OpenRequest,
    out_owner: *?*anyopaque,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_close(owner: ?*anyopaque) callconv(.c) void;

pub extern fn antfly_storage_owner_batch_json(
    owner: ?*anyopaque,
    request: *const JsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_query_json(
    owner: ?*anyopaque,
    request: *const JsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_lookup_json(
    owner: ?*anyopaque,
    request: *const JsonOperationRequest,
    out_response: *VersionedOwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_scan_ndjson(
    owner: ?*anyopaque,
    request: *const JsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_preflight_json(
    owner: ?*anyopaque,
    request: *const JsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_text_stats_json(
    owner: ?*anyopaque,
    request: *const JsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_algebraic_partials_json(
    owner: ?*anyopaque,
    request: *const JsonOperationRequest,
    out_response: *OwnedBytes,
) callconv(.c) Status;

pub extern fn antfly_storage_owner_buffer_destroy(
    buffer: *OwnedBytes,
) callconv(.c) void;
