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

//! Native schema-owner effects over the existing metadata journal/outbox.
//! Trusted storage adapter only: metadata/root/cohort authorization and
//! producer capability admission belong to the receiving owner. No public
//! mutation escape hatch, signature, publication or fence release is provided.
const std = @import("std");
const A = std.mem.Allocator;
const job = @import("relational_nullability_job.zig");
const positions = @import("receipt_position.zig");
const core_mod = @import("core.zig");
const schema_api = @import("../../schema/mod.zig");
const operation = @import("../../api/operation.zig");
pub const Native = positions.Native;
const Digest = [32]u8;
pub const magic = "ASVC\x01\x00\x00\x00";
const max_metadata = 8192;
pub const max_wire_bytes = 81 + max_metadata + job.Candidate.max_input_bytes;
pub const Action = enum { begin, freeze, page, cancel, install };

pub const Command = struct {
    binding: job.Binding,
    action: Action,
    /// Borrowed immutable ASVI inputs, ASVP page, or decision+ready digests.
    payload: []const u8 = "",

    pub fn validate(self: @This()) !void {
        try self.binding.validate();
        if (self.binding.authority != .native) return error.InvalidControlReceiptPosition;
        switch (self.action) {
            .begin => {
                _ = try job.Candidate.parts(self.payload);
                if (!equal(job.hash(self.payload), self.binding.inputs)) return error.SchemaValidationJobChanged;
            },
            .page => {
                const command = try job.PageCommand.decode(self.payload);
                if (!std.meta.eql(command.binding, self.binding)) return error.SchemaValidationJobChanged;
            },
            .freeze, .cancel => if (self.payload.len != 0) return error.InvalidSchemaOwnerControl,
            .install => if (self.payload.len != 64 or std.mem.allEqual(u8, self.payload[0..32], 0) or
                std.mem.allEqual(u8, self.payload[32..64], 0)) return error.InvalidSchemaOwnerControl,
        }
    }
};

fn equal(a: Digest, b: Digest) bool {
    return std.mem.eql(u8, &a, &b);
}
pub fn isFrame(bytes: []const u8) bool {
    return bytes.len >= magic.len and std.mem.eql(u8, bytes[0..magic.len], magic);
}

/// Native position is a fixed trailer. Its prefix is hashed off-lock, so
/// assigning the actual commit clock never rehashes schema-sized input under
/// apply. These bytes do not pass through a JSON/base64 mutation envelope.
pub const Payload = struct {
    alloc: A,
    bytes: []u8,
    prefix_len: usize,
    prefix_hash: std.crypto.hash.Blake3,
    binding: job.Binding,

    pub fn init(a: A, command: Command) !@This() {
        try command.validate();
        const descriptor = Descriptor{ .binding = command.binding, .action = command.action };
        var buffer: [max_metadata]u8 = undefined;
        var writer = std.Io.Writer.fixed(&buffer);
        try std.json.Stringify.value(descriptor, .{}, &writer);
        const metadata = writer.buffered();
        const prefix_len = 16 + metadata.len + command.payload.len;
        if (prefix_len + 65 > max_wire_bytes) return error.InvalidSchemaOwnerControl;
        const bytes = try a.alloc(u8, prefix_len + 65);
        @memcpy(bytes[0..8], magic);
        std.mem.writeInt(u32, bytes[8..12], @intCast(metadata.len), .little);
        std.mem.writeInt(u32, bytes[12..16], @intCast(command.payload.len), .little);
        @memcpy(bytes[16..][0..metadata.len], metadata);
        @memcpy(bytes[16 + metadata.len ..][0..command.payload.len], command.payload);
        @memset(bytes[prefix_len..], 0);
        var prefix_hash = std.crypto.hash.Blake3.init(.{});
        prefix_hash.update(bytes[0..prefix_len]);
        return .{ .alloc = a, .bytes = bytes, .prefix_len = prefix_len, .prefix_hash = prefix_hash, .binding = command.binding };
    }
    pub fn deinit(self: *@This()) void {
        self.alloc.free(self.bytes);
        self.* = undefined;
    }
    pub fn finalize(self: *@This(), position: Native) ![]const u8 {
        try position.validate();
        if (!position.namespace.eql(self.binding.fence.namespace)) return error.IdentityNamespaceMismatch;
        const encoded = try (positions.Position{ .native = position }).encode();
        @memcpy(self.bytes[self.prefix_len..][0..33], &encoded);
        var state = self.prefix_hash;
        state.update(&encoded);
        var checksum: Digest = undefined;
        state.final(&checksum);
        @memcpy(self.bytes[self.prefix_len + 33 ..][0..32], &checksum);
        return self.bytes;
    }
};
const Descriptor = struct { binding: job.Binding, action: Action };
pub const Frame = struct {
    command: Command,
    position: Native,

    pub fn encode(self: @This(), a: A) ![]u8 {
        var payload = try Payload.init(a, self.command);
        errdefer payload.deinit();
        _ = try payload.finalize(self.position);
        return payload.bytes;
    }
    /// Only payload borrows the frame; descriptor and commit position are
    /// fixed owned values. Decoding cannot grant metadata/root authority.
    pub fn decode(bytes: []const u8) !@This() {
        if (bytes.len < 81 or bytes.len > max_wire_bytes or !isFrame(bytes) or
            !equal(job.hash(bytes[0 .. bytes.len - 32]), bytes[bytes.len - 32 ..][0..32].*)) return error.InvalidSchemaOwnerControl;
        const metadata_len: usize = std.mem.readInt(u32, bytes[8..12], .little);
        const payload_len: usize = std.mem.readInt(u32, bytes[12..16], .little);
        if (metadata_len == 0 or metadata_len > max_metadata or payload_len > job.Candidate.max_input_bytes or
            metadata_len + payload_len != bytes.len - 81) return error.InvalidSchemaOwnerControl;
        var buffer: [64 * 1024]u8 = undefined;
        var fixed = std.heap.FixedBufferAllocator.init(&buffer);
        const descriptor = std.json.parseFromSliceLeaky(Descriptor, fixed.allocator(), bytes[16..][0..metadata_len], .{}) catch return error.InvalidSchemaOwnerControl;
        var canonical: [max_metadata]u8 = undefined;
        var writer = std.Io.Writer.fixed(&canonical);
        try std.json.Stringify.value(descriptor, .{}, &writer);
        if (!std.mem.eql(u8, writer.buffered(), bytes[16..][0..metadata_len])) return error.InvalidSchemaOwnerControl;
        const position = try positions.Position.decode(bytes[bytes.len - 65 ..][0..33]);
        if (position != .native) return error.InvalidControlReceiptPosition;
        const command: Command = .{ .binding = descriptor.binding, .action = descriptor.action, .payload = bytes[16 + metadata_len ..][0..payload_len] };
        try command.validate();
        if (!position.native.namespace.eql(command.binding.fence.namespace)) return error.IdentityNamespaceMismatch;
        return .{ .command = command, .position = position.native };
    }
};

pub const Outcome = struct {
    /// Trusted duplicate standby LSNs may skip work without asserting a
    /// current header. No logical/signed publication receipt is returned.
    header: ?job.Header,
    position: Native,
    duplicate: bool,
    replication_pending: bool = false,
    error_name: ?[]const u8 = null,
};

/// Independently read local facts, never a signed receipt or metadata grant.
/// Zero is an observed empty native clock, not a fabricated receipt position.
pub const Observation = struct {
    pub const max_wire_bytes = 32 * 1024;
    binding: job.Binding,
    physical_root_incarnation: u128,
    sequence: u64,
    header: ?job.Header,
    replication_pending: bool,

    pub fn validateFor(self: @This(), binding: job.Binding) !void {
        try binding.validate();
        if (binding.authority != .native or self.physical_root_incarnation == 0 or
            !std.meta.eql(self.binding, binding)) return error.SchemaValidationJobChanged;
        if (self.header) |header| {
            try header.validate();
            if (!std.meta.eql(header.binding, binding) or header.updated != .native or
                header.updated.native.sequence > self.sequence) return error.SchemaValidationJobChanged;
        }
    }
};

pub const Prepared = struct {
    pub const ApplyOutcome = Outcome;
    pub const metadata_control = true;
    pub const SchemaRecord = job.Header;
    const SchemaData = struct { prepared: core_mod.PreparedSchemaMetadata, json: []u8, version_key: []u8 };
    alloc: A,
    command: Command,
    context: operation.RequestContext,
    owned: ?[]u8 = null,
    candidate: ?job.Candidate = null,
    page: ?job.PreparedPage = null,
    schema_data: ?SchemaData = null,
    payload: ?Payload = null,

    pub fn init(a: A, core: *core_mod.DBCore, command: Command, context: operation.RequestContext) !@This() {
        try context.ensureActive();
        try command.validate();
        if (!command.binding.fence.namespace.eql(core.identity_namespace)) return error.IdentityNamespaceMismatch;
        var self: @This() = .{ .alloc = a, .command = command, .context = context };
        errdefer self.deinit();
        // Nonempty payloads must outlive the request/transport buffer.
        if (command.action != .begin and command.payload.len != 0) {
            self.owned = try a.dupe(u8, command.payload);
            self.command.payload = self.owned.?;
        }
        if (command.action == .begin) {
            const parts = try job.Candidate.parts(command.payload);
            var names = try std.json.parseFromSlice([]const []const u8, a, parts[3], .{});
            defer names.deinit();
            self.candidate = try job.Candidate.init(a, command.binding.fence.namespace.table_id, parts[0], parts[1], parts[2], names.value, control(&context));
            try command.binding.requireCandidate(&self.candidate.?);
            self.command.payload = self.candidate.?.inputs;
        }
        var read = try core.store.beginReadTxn();
        defer read.abort();
        const current = try job.load(&read);
        if (current) |header| if (std.meta.eql(header.binding, command.binding)) {
            const observed = try job.observe(a, &read, command.binding);
            if (try duplicate(self.command, observed)) |position| {
                if (command.action == .install) {
                    core.lockApplyShared();
                    defer core.unlockApplyShared();
                    try requireResident(core, observed);
                }
                _ = position;
                return self;
            }
        };
        switch (command.action) {
            .begin, .freeze, .cancel => {},
            .page => self.page = try job.PreparedPage.replay(a, core, try job.PageCommand.decode(self.command.payload), control(&context)),
            .install => {
                const ready = try job.requireReady(&read, command.binding);
                if (!equal(try ready.digest(), self.command.payload[32..64].*)) return error.SchemaValidationJobChanged;
                var candidate = try job.Candidate.load(a, &read, command.binding, control(&context));
                defer candidate.deinit();
                const parts = try job.Candidate.parts(candidate.inputs);
                const json = try a.dupe(u8, parts[1]);
                errdefer a.free(json);
                const version_key = try schema_api.versionedSchemaKeyAlloc(a, command.binding.after_version);
                errdefer a.free(version_key);
                var definition = try schema_api.PreparedSchemaDefinition.init(a, json);
                defer definition.deinit(a);
                const writes = [_]@import("../docstore.zig").KVPair{ .{ .key = "\x00\x00__metadata__:schema_json", .value = json }, .{ .key = version_key, .value = json } };
                self.schema_data = .{ .prepared = try core.prepareSchemaMetadataValidatedNullability(definition.runtime_schema, &writes, command.binding, &candidate), .json = json, .version_key = version_key };
            },
        }
        try context.ensureActive();
        return self;
    }
    pub fn deinit(self: *@This()) void {
        if (self.schema_data) |*data| {
            data.prepared.deinit();
            self.alloc.free(data.json);
            self.alloc.free(data.version_key);
        }
        if (self.payload) |*value| value.deinit();
        if (self.page) |*value| value.deinit();
        if (self.candidate) |*value| value.deinit();
        if (self.owned) |bytes| self.alloc.free(bytes);
        self.* = undefined;
    }
    /// Trusted local producer: prepare/scan once, rather than serialize a
    /// locally computed page only to execute it again on the same owner.
    /// Standby/transport callers still use init's independent replay path.
    pub fn preparePage(a: A, core: *core_mod.DBCore, binding: job.Binding, budget: @import("relational_rows.zig").Budget, context: operation.RequestContext) !@This() {
        if (binding.authority != .native) return error.InvalidControlReceiptPosition;
        var page = try job.PreparedPage.prepare(a, core, binding, budget, control(&context));
        errdefer page.deinit();
        const encoded = try (try page.command()).encode(a);
        errdefer a.free(encoded);
        return .{ .alloc = a, .command = .{ .binding = binding, .action = .page, .payload = encoded }, .context = context, .owned = encoded, .page = page };
    }
    pub fn validate(self: *const @This()) !void {
        try self.context.ensureActive();
        try self.command.binding.validate();
    }
    pub fn namespace(self: *const @This()) @FieldType(Native, "namespace") {
        return self.command.binding.fence.namespace;
    }
    pub fn validateReplay(self: *const @This(), position: Native) !void {
        try position.validate();
        if (!position.namespace.eql(self.namespace())) return error.IdentityNamespaceMismatch;
    }
    pub fn replayDuplicateOutcome(_: *const @This(), position: Native) Outcome {
        return .{ .header = null, .position = position, .duplicate = true };
    }
    pub fn requiresSchemaCommit(self: *const @This()) bool {
        return if (self.schema_data) |data| data.prepared.publication != null else false;
    }
    pub fn commitSchema(self: *@This(), core: *core_mod.DBCore, _: ?Native, participant: anytype) !Outcome {
        try self.validate();
        const data = if (self.schema_data) |*value| value else return error.SchemaValidationPublicationRequired;
        const writes = [_]@import("../docstore.zig").KVPair{ .{ .key = "\x00\x00__metadata__:schema_json", .value = data.json }, .{ .key = data.version_key, .value = data.json } };
        const installed = try core.commitPreparedSchemaMetadataValidatedNullability(&data.prepared, &writes, self.command.payload[0..32].*, null, participant);
        return .{ .header = installed, .position = installed.updated.native, .duplicate = false };
    }
    pub fn stage(self: *@This(), core: *core_mod.DBCore, txn: anytype, replay: ?Native) !Outcome {
        try self.validate();
        if (self.requiresSchemaCommit()) return error.SchemaValidationPublicationRequired;
        if (try job.load(txn)) |header| if (std.meta.eql(header.binding, self.command.binding)) {
            const observed = try job.observe(self.alloc, txn, self.command.binding);
            if (try duplicate(self.command, observed)) |position| {
                if (replay) |expected| if (!std.meta.eql(position, expected)) return error.InvalidControlReceiptPosition;
                if (self.command.action == .install) try requireResident(core, observed);
                return .{ .header = observed, .position = position, .duplicate = true };
            }
        };
        var manager = try core.initTxnManager();
        defer manager.deinit();
        const header = switch (self.command.action) {
            .begin => try job.stageBegin(self.alloc, txn, self.command.binding, if (self.candidate) |*value| value else return error.SchemaValidationJobRequired, null),
            .freeze => try job.stageFrozen(self.alloc, txn, &manager, self.command.binding, null),
            .page => try job.stagePage(self.alloc, txn, &manager, if (self.page) |*value| value else return error.SchemaValidationJobChanged, null),
            .cancel => try job.stageCancel(self.alloc, txn, &manager, self.command.binding, null),
            .install => return error.SchemaValidationPublicationRequired,
        };
        if (replay) |expected| if (!std.meta.eql(header.updated.native, expected)) return error.InvalidControlReceiptPosition;
        return .{ .header = header, .position = header.updated.native, .duplicate = false };
    }
    pub fn preparePayload(self: *@This()) !void {
        if (self.payload == null) self.payload = try Payload.init(self.alloc, self.command);
    }
    pub fn finalizePayload(self: *@This(), position: Native) ![]const u8 {
        return (if (self.payload) |*value| value else return error.InvalidSchemaOwnerControl).finalize(position);
    }
    pub fn hasPublicationReceipt(_: *const @This()) bool {
        return false;
    }
    pub fn populateReceipt(_: *const @This(), _: anytype, _: *Outcome) !void {
        return error.SchemaValidationPublicationRequired;
    }
};

fn control(context: *const operation.RequestContext) @import("relational_nullability.zig").Control {
    // The caller owns this context while preparation executes; it is not
    // retained by a returned row page or encoded payload.
    return .{ .context = @constCast(context), .checkpoint = struct {
        fn check(raw: ?*anyopaque) !void {
            const value: *const operation.RequestContext = @ptrCast(@alignCast(raw.?));
            try value.ensureActive();
        }
    }.check };
}

fn duplicate(command: Command, header: job.Header) !?Native {
    const position: ?positions.Position = switch (command.action) {
        .begin => if (header.phase == .canceled) return error.SchemaValidationCanceled else header.admitted,
        .freeze => if (header.phase == .canceled) return error.SchemaValidationCanceled else header.frozen,
        .cancel => if (header.phase == .canceled) header.updated else null,
        .install => if (header.phase == .installed) blk: {
            if (!equal(header.installation.?.decision, command.payload[0..32].*) or
                !equal(header.installation.?.ready_digest, command.payload[32..64].*)) return error.SchemaValidationJobChanged;
            break :blk header.updated;
        } else null,
        .page => if (equal(header.last_effect, try (try job.PageCommand.decode(command.payload)).effect())) header.updated else null,
    };
    return if (position) |value| if (value == .native) value.native else error.InvalidControlReceiptPosition else null;
}
/// Capture beside the immutable store read under apply-shared, then verify
/// expensive durable inputs off-lock. Pins survive later runtime publication.
pub const Resident = struct {
    view: ?@import("schema_registry.zig").SchemaView,
    indexes: ?@import("relational_index_catalog.zig").WriteSnapshot,

    pub fn pin(core: *core_mod.DBCore) @This() {
        return .{ .view = core.acquireSchemaView(), .indexes = core.relational_indexes.acquire() };
    }
    pub fn deinit(self: *@This()) void {
        if (self.indexes) |*value| value.deinit();
        if (self.view) |*value| value.release();
        self.* = undefined;
    }
    pub fn require(self: @This(), header: job.Header) !void {
        if (header.phase != .installed or header.installation == null) return error.SchemaValidationRuntimeRecoveryRequired;
        const view = self.view orelse return error.SchemaValidationRuntimeRecoveryRequired;
        if (view.version() != header.binding.after_version) return error.SchemaValidationRuntimeRecoveryRequired;
        const physical_hash = if (self.indexes) |value| job.hash(&value.head.encode()) else job.hash("");
        if (!equal(physical_hash, header.installation.?.physical_catalog) or
            (self.indexes != null and self.indexes.?.plan.schemaView().epoch != view.epoch)) return error.SchemaValidationRuntimeRecoveryRequired;
    }
};
pub fn requireResident(core: *core_mod.DBCore, header: job.Header) !void {
    var resident = Resident.pin(core);
    defer resident.deinit();
    try resident.require(header);
}
