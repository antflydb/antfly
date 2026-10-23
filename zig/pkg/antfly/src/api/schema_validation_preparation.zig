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

//! Private, read-only data-owner admission preparation. No caller supplies a
//! physical root, source position, admission epoch or serving authority. Raw
//! ASVI inputs remain binary; repeated cohort inputs can be omitted only when
//! the caller explicitly names their digest and already owns those bytes.
const std = @import("std");
const local = @import("antfly_local_sources");
const contract = local.storage_db_relational_schema_owner_contract;
const attestation = @import("../metadata/schema_validation_attestation.zig");
const operation = local.api_operation;
pub const max_known_inputs = 32;
pub const remaining_ms_header = "X-Antfly-Schema-Preparation-Remaining-Ms";
pub const max_budget_ms = 5000;
pub const max_request_bytes = 12 * contract.max_schema + 6 * contract.max_names + 16 * 1024;
const max_header = attestation.SignedPreparation.max_wire_bytes + 64;
pub const max_response_bytes = 48 + max_header + contract.max_input_bytes;
const magic = "ASPR\x01\x00\x00\x00";

pub const Request = struct {
    group_id: u64,
    metadata_incarnation: [32]u8,
    id: [16]u8,
    provisional_plan: [32]u8,
    transition_id: u64,
    namespace: local.storage_db_doc_identity_namespace.Namespace,
    before_schema: []const u8,
    after_schema: []const u8,
    columns: []const []const u8,
    known_inputs: []const [32]u8 = &.{},

    pub fn validate(self: @This(), group: u64) !void {
        if (group == 0 or self.group_id != group or self.transition_id == 0 or !local.metadata_incarnation.isValid(self.metadata_incarnation) or
            std.mem.allEqual(u8, &self.id, 0) or std.mem.allEqual(u8, &self.provisional_plan, 0) or
            self.namespace.table_id == 0 or self.namespace.shard_id == 0 or self.namespace.range_id == 0 or
            self.before_schema.len == 0 or self.before_schema.len > contract.max_schema or
            self.after_schema.len == 0 or self.after_schema.len > contract.max_schema or self.known_inputs.len > max_known_inputs) return error.InvalidSchemaValidationJob;
        var projection = contract.ProjectionHasher.init();
        for (self.columns) |column| try projection.add(column);
        _ = try projection.finish();
        for (self.known_inputs, 0..) |digest, i| {
            if (std.mem.allEqual(u8, &digest, 0)) return error.InvalidSchemaValidationJob;
            for (self.known_inputs[0..i]) |prior| if (std.mem.eql(u8, &prior, &digest)) return error.InvalidSchemaValidationJob;
        }
    }

    pub fn hasInput(self: @This(), digest: [32]u8) bool {
        for (self.known_inputs) |known| if (std.mem.eql(u8, &known, &digest)) return true;
        return false;
    }
};

const Header = struct { signed: attestation.SignedPreparation, has_inputs: bool };
pub const Response = struct {
    signed: attestation.SignedPreparation,
    /// Borrows the frame. The caller keeps its transport response alive until
    /// copying a previously unknown input into the bounded admission arena.
    inputs: []const u8,

    pub fn requireFor(self: @This(), group: u64, request: Request) !void {
        try request.validate(group);
        const binding = self.signed.value.binding;
        try self.signed.verifyFor(binding, self.signed.value.physical_root_incarnation);
        var projection = contract.ProjectionHasher.init();
        for (request.columns) |column| try projection.add(column);
        if (!std.mem.eql(u8, &self.signed.proof.identity.metadata_incarnation, &request.metadata_incarnation) or
            !std.mem.eql(u8, &binding.id, &request.id) or !std.mem.eql(u8, &binding.plan_digest, &request.provisional_plan) or
            !binding.fence.namespace.eql(request.namespace) or binding.fence.owner_group_id != group or
            binding.fence.transition_id != request.transition_id or binding.columns != request.columns.len or
            !std.mem.eql(u8, &binding.before_public, &contract.hash(request.before_schema)) or
            !std.mem.eql(u8, &binding.after_public, &contract.hash(request.after_schema)) or
            !std.mem.eql(u8, &binding.projection_identity, &try projection.finish())) return error.SchemaValidationJobChanged;
        if (self.inputs.len == 0) {
            if (!request.hasInput(binding.inputs)) return error.SchemaValidationJobChanged;
        } else {
            const parts = try contract.inputParts(self.inputs);
            if (!std.mem.eql(u8, &binding.inputs, &contract.hash(self.inputs)) or
                !std.mem.eql(u8, parts[0], request.before_schema) or !std.mem.eql(u8, parts[1], request.after_schema)) return error.SchemaValidationJobChanged;
        }
    }
};

pub fn encode(a: std.mem.Allocator, signed: attestation.SignedPreparation, inputs: []const u8) ![]u8 {
    try signed.verifyFor(signed.value.binding, signed.value.physical_root_incarnation);
    if (inputs.len != 0) {
        _ = try contract.inputParts(inputs);
        if (!std.mem.eql(u8, &contract.hash(inputs), &signed.value.binding.inputs)) return error.SchemaValidationJobChanged;
    }
    var buffer: [max_header]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);
    try std.json.Stringify.value(Header{ .signed = signed, .has_inputs = inputs.len != 0 }, .{}, &writer);
    const header = writer.buffered();
    const bytes = try a.alloc(u8, 48 + header.len + inputs.len);
    @memcpy(bytes[0..8], magic);
    std.mem.writeInt(u32, bytes[8..12], @intCast(header.len), .little);
    std.mem.writeInt(u32, bytes[12..16], @intCast(inputs.len), .little);
    @memcpy(bytes[16..][0..header.len], header);
    @memcpy(bytes[16 + header.len ..][0..inputs.len], inputs);
    @memcpy(bytes[bytes.len - 32 ..][0..32], &contract.hash(bytes[0 .. bytes.len - 32]));
    return bytes;
}

pub fn decode(bytes: []const u8) !Response {
    if (bytes.len < 48 or bytes.len > max_response_bytes or !std.mem.eql(u8, bytes[0..8], magic) or
        !std.mem.eql(u8, bytes[bytes.len - 32 ..], &contract.hash(bytes[0 .. bytes.len - 32]))) return error.InvalidSchemaValidationJob;
    const header_len: usize = std.mem.readInt(u32, bytes[8..12], .little);
    const input_len: usize = std.mem.readInt(u32, bytes[12..16], .little);
    if (header_len == 0 or header_len > max_header or input_len > contract.max_input_bytes or header_len + input_len != bytes.len - 48) return error.InvalidSchemaValidationJob;
    var scratch: [128 * 1024]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&scratch);
    const header = std.json.parseFromSliceLeaky(Header, fixed.allocator(), bytes[16..][0..header_len], .{ .ignore_unknown_fields = false }) catch return error.InvalidSchemaValidationJob;
    var canonical: [max_header]u8 = undefined;
    var writer = std.Io.Writer.fixed(&canonical);
    try std.json.Stringify.value(header, .{}, &writer);
    if (!std.mem.eql(u8, writer.buffered(), bytes[16..][0..header_len]) or header.has_inputs != (input_len != 0)) return error.InvalidSchemaValidationJob;
    const inputs = bytes[16 + header_len ..][0..input_len];
    if (input_len != 0) {
        _ = try contract.inputParts(inputs);
        if (!std.mem.eql(u8, &header.signed.value.binding.inputs, &contract.hash(inputs))) return error.SchemaValidationJobChanged;
    }
    try header.signed.verifyFor(header.signed.value.binding, header.signed.value.physical_root_incarnation);
    return .{ .signed = header.signed, .inputs = inputs };
}

pub const Port = struct {
    ptr: *anyopaque,
    prepare_fn: *const fn (*anyopaque, std.mem.Allocator, u64, []const u8, Request, operation.RequestContext) anyerror![]u8,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    const VTable = struct { prepare_fn: *const fn (*anyopaque, std.mem.Allocator, u64, []const u8, Request, operation.RequestContext) anyerror![]u8 };
    const BoundaryAbi = local.runtime_callback_abi.Boundary(VTable);

    pub fn prepare(self: @This(), a: std.mem.Allocator, group: u64, table: []const u8, request: Request, context: operation.RequestContext) ![]u8 {
        try context.ensureActive();
        try request.validate(group);
        const bytes = try BoundaryAbi.call("prepare_fn", self.boundary_dispatch, self.prepare_fn, .{ self.ptr, a, group, table, request, context });
        errdefer a.free(bytes);
        const value = try decode(bytes);
        try value.requireFor(group, request);
        return bytes;
    }
};
