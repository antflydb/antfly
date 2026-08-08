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

pub const VersionedResponse = struct {
    response: abi.VersionedOwnedBytes = .{},

    pub fn bytes(self: VersionedResponse) []const u8 {
        return self.response.buffer.slice();
    }

    pub fn version(self: VersionedResponse) u64 {
        return self.response.version;
    }

    pub fn deinit(self: *VersionedResponse) void {
        abi.antfly_storage_owner_buffer_destroy(&self.response.buffer);
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

    pub fn lookupJson(self: *Owner, table_name: []const u8, request_json: []const u8) !VersionedResponse {
        var response: VersionedResponse = .{};
        try statusToError(abi.antfly_storage_owner_lookup_json(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
            },
            &response.response,
        ));
        return response;
    }

    pub fn scanNdjson(self: *Owner, table_name: []const u8, request_json: []const u8) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_scan_ndjson(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
            },
            &response.buffer,
        ));
        return response;
    }

    pub fn preflightJson(self: *Owner, table_name: []const u8, request_json: []const u8) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_preflight_json(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
            },
            &response.buffer,
        ));
        return response;
    }

    pub fn textStatsJson(self: *Owner, table_name: []const u8, request_json: []const u8) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_text_stats_json(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
            },
            &response.buffer,
        ));
        return response;
    }

    pub fn algebraicPartialsJson(self: *Owner, table_name: []const u8, request_json: []const u8) !Response {
        var response: Response = .{};
        try statusToError(abi.antfly_storage_owner_algebraic_partials_json(
            self.handle,
            &.{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(request_json),
            },
            &response.buffer,
        ));
        return response;
    }

    pub fn graphExpandJson(
        self: *Owner,
        table_name: []const u8,
        request_json: []const u8,
        execution_deadline_ns: ?u64,
        cancellation_flag: ?*const anyopaque,
    ) !Response {
        var response: Response = .{};
        const request = controlledRequest(table_name, request_json, execution_deadline_ns, cancellation_flag);
        try statusToError(abi.antfly_storage_owner_graph_expand_json(
            self.handle,
            &request,
            &response.buffer,
        ));
        return response;
    }

    pub fn graphHydrateJson(
        self: *Owner,
        table_name: []const u8,
        request_json: []const u8,
        execution_deadline_ns: ?u64,
        cancellation_flag: ?*const anyopaque,
    ) !Response {
        var response: Response = .{};
        const request = controlledRequest(table_name, request_json, execution_deadline_ns, cancellation_flag);
        try statusToError(abi.antfly_storage_owner_graph_hydrate_json(
            self.handle,
            &request,
            &response.buffer,
        ));
        return response;
    }

    pub fn graphEdgesJson(
        self: *Owner,
        table_name: []const u8,
        request_json: []const u8,
        execution_deadline_ns: ?u64,
        cancellation_flag: ?*const anyopaque,
    ) !Response {
        var response: Response = .{};
        const request = controlledRequest(table_name, request_json, execution_deadline_ns, cancellation_flag);
        try statusToError(abi.antfly_storage_owner_graph_edges_json(
            self.handle,
            &request,
            &response.buffer,
        ));
        return response;
    }

    pub fn documentArtifactManifestJson(
        self: *Owner,
        table_name: []const u8,
        request_json: []const u8,
    ) !Response {
        var response: Response = .{};
        const request = operationRequest(table_name, request_json);
        try statusToError(abi.antfly_storage_owner_document_artifact_manifest_json(
            self.handle,
            &request,
            &response.buffer,
        ));
        return response;
    }

    pub fn documentArtifactManifestsJson(
        self: *Owner,
        table_name: []const u8,
        request_json: []const u8,
    ) !Response {
        var response: Response = .{};
        const request = operationRequest(table_name, request_json);
        try statusToError(abi.antfly_storage_owner_document_artifact_manifests_json(
            self.handle,
            &request,
            &response.buffer,
        ));
        return response;
    }

    pub fn runtimeStatusJson(self: *Owner, table_name: []const u8) !Response {
        var response: Response = .{};
        const request = operationRequest(table_name, "");
        try statusToError(abi.antfly_storage_owner_runtime_status_json(
            self.handle,
            &request,
            &response.buffer,
        ));
        return response;
    }
};

fn operationRequest(
    table_name: []const u8,
    request_json: []const u8,
) abi.JsonOperationRequest {
    return .{
        .table_name = .fromSlice(table_name),
        .request_json = .fromSlice(request_json),
    };
}

fn controlledRequest(
    table_name: []const u8,
    request_json: []const u8,
    execution_deadline_ns: ?u64,
    cancellation_flag: ?*const anyopaque,
) abi.ControlledJsonOperationRequest {
    return .{
        .table_name = .fromSlice(table_name),
        .request_json = .fromSlice(request_json),
        .execution_deadline_ns = execution_deadline_ns orelse 0,
        .has_execution_deadline = @intFromBool(execution_deadline_ns != null),
        .cancellation_flag = cancellation_flag,
    };
}

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
        .invalid_query => error.InvalidQueryRequest,
        .unsupported_query => error.UnsupportedQueryRequest,
        .index_not_found => error.IndexNotFound,
        .identity_read_generation_changed => error.IdentityReadGenerationChanged,
        .timeout => error.Timeout,
        .cancelled => error.Cancelled,
        .internal => error.StorageKernelFailure,
    };
}
