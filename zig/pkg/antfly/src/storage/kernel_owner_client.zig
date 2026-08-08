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

//! Typed consumer for the compiled storage owner. It intentionally imports
//! only the ABI contract, never DB, LSM, indexes, or storage implementation.

const abi = @import("kernel_owner_abi");

pub const Response = struct {
    buffer: abi.OwnedBytes = .{},

    pub fn bytes(self: Response) []const u8 {
        return self.buffer.slice();
    }

    pub fn deinit(self: *Response) void {
        abi.antfly_storage_owner_buffer_destroy(&self.buffer);
        self.* = undefined;
    }
};

pub const Owner = struct {
    handle: ?*anyopaque,

    pub fn open(request: abi.OpenRequest) !Owner {
        var handle: ?*anyopaque = null;
        try statusToError(abi.antfly_storage_owner_open(&request, &handle));
        return .{ .handle = handle orelse return error.StorageKernelFailure };
    }

    pub fn deinit(self: *Owner) void {
        abi.antfly_storage_owner_close(self.handle);
        self.* = undefined;
    }

    pub fn batchJson(self: *Owner, table_name: []const u8, request_json: []const u8) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_batch_json(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
            },
            &response.buffer,
        ));
        return response;
    }

    pub fn queryJson(self: *Owner, table_name: []const u8, request_json: []const u8) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_query_json(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
            },
            &response.buffer,
        ));
        return response;
    }
};

pub fn statusToError(status: abi.Status) !void {
    return switch (status) {
        .ok => {},
        .invalid_abi => error.InvalidAbiVersion,
        .invalid_argument => error.InvalidArgument,
        .not_found => error.NotFound,
        .busy => error.StorageBusy,
        .version_conflict => error.VersionConflict,
        .intent_conflict => error.IntentConflict,
        .transaction_not_found => error.TxnNotFound,
        .read_only => error.ReadOnly,
        .out_of_memory => error.OutOfMemory,
        .corrupted => error.Corrupted,
        .identity_namespace_mismatch => error.DocIdentityNamespaceMismatch,
        .internal => error.StorageKernelFailure,
    };
}
