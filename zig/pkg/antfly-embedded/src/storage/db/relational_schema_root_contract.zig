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

//! Immutable audited-root wire verification: no DB owner is imported and no
//! files are opened here. Native audit production stays with the physical
//! owner. Enrollment and live authority must be resolved independently by the
//! accepting metadata transaction; a valid signature grants neither.
const std = @import("std");
const contract = @import("relational_schema_owner_contract.zig");
const origin = @import("native_backup_origin.zig");
const debt = @import("../transaction_debt.zig");
const history = @import("relational_schema_apply_outcome.zig");
const A = std.mem.Allocator;
const Digest = [32]u8;
const magic = "ASRP\x01\x00\x00\x00";
const max_body = contract.max_header + 8192;
pub const max_encoded = 76 + max_body;
pub const Coverage = enum { none, prefix, full };
pub const RowSummary = struct {
    header: contract.Header,
    /// Actual audited read cut, not inferred Raft completion.
    snapshot_cut: @import("receipt_position.zig").Position,
    coverage: Coverage,
    records: u64,
    rows: u64,
    cursor_digest: Digest,
    reference_coverage: ?contract.ReferenceCoverage = null,

    pub fn jsonStringify(self: @This(), writer: anytype) @TypeOf(writer.*).Error!void {
        try writer.beginObject();
        inline for (@typeInfo(@This()).@"struct".field_names) |field| {
            if (!std.mem.eql(u8, field, "reference_coverage") or self.reference_coverage != null) {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }
    pub fn validate(self: @This()) !void {
        try self.validateCoverage(true);
    }
    pub fn validateProgress(self: @This()) !void {
        try self.validateCoverage(false);
    }
    fn validateCoverage(self: @This(), comptime complete: bool) !void {
        try self.header.validate();
        if ((self.header.binding.purpose == .not_null) != (self.reference_coverage == null)) return error.InvalidSchemaValidationJob;
        if (self.reference_coverage) |coverage| try coverage.validate(self.header.binding.purpose, self.records, self.rows, self.header.binding.columns);
        if (complete and self.coverage != .none and self.header.phase != .released and !std.meta.eql(self.reference_coverage, self.header.reference_coverage)) return error.InvalidSchemaValidationJob;
        try self.snapshot_cut.requireNamespace(self.header.binding.fence.namespace);
        if (!std.meta.eql(self.snapshot_cut, self.header.updated) and
            !self.snapshot_cut.strictlyAfter(self.header.updated)) return error.InvalidSchemaValidationJob;
        const expected: Coverage = switch (self.header.phase) {
            .fencing, .canceling, .canceled => .none,
            .scanning, .failed => .prefix,
            .ready, .installed, .released => .full,
        };
        if (self.coverage != expected or self.rows > self.records or std.mem.allEqual(u8, &self.cursor_digest, 0)) return error.InvalidSchemaValidationJob;
        switch (expected) {
            .none => if (self.records != 0 or self.rows != 0 or !std.mem.eql(u8, &self.cursor_digest, &contract.hash(""))) return error.InvalidSchemaValidationJob,
            .prefix, .full => if (self.header.phase != .released) {
                if (complete) {
                    if (self.records != self.header.records or self.rows != self.header.rows_scanned or
                        !std.mem.eql(u8, &self.cursor_digest, &self.header.cursor_digest)) return error.InvalidSchemaValidationJob;
                } else if (self.records > self.header.records or self.rows > self.header.rows_scanned) return error.InvalidSchemaValidationJob;
            },
        }
    }
};

pub const Qualification = struct {
    origin: origin.Origin,
    directory_head: ?debt.Header,
    directory: debt.SnapshotStats,
    transactions: @import("../transaction_snapshot.zig").Stats,
    history_head: ?history.Head,
    history: history.SnapshotStats,
    installation: ?origin.Installation,
    source_history_head: ?history.Head,
};

pub const Facts = struct {
    version: u8 = 1,
    signing_root: u128,
    physical_root: u128,
    rows: RowSummary,
    /// Link to the exact durably signed row continuation, including its binary
    /// cursor. A row-only checkpoint is insufficient to construct this proof.
    checkpoint_digest: Digest,
    qualification: Qualification,

    pub fn validate(self: Facts) !void {
        if (self.version != 1 or self.signing_root == 0 or self.physical_root == 0 or
            std.mem.allEqual(u8, &self.checkpoint_digest, 0)) return error.InvalidSchemaValidationJob;
        try self.rows.validate();
        const ns = self.rows.header.binding.fence.namespace;
        const q = self.qualification;
        try q.origin.validate();
        if (q.origin.physical_root_incarnation != self.physical_root or q.origin.namespace == null or
            !q.origin.namespace.?.eql(ns)) return error.InvalidSchemaValidationJob;
        const authority = q.origin.authority orelse return error.InvalidSchemaValidationJob;
        if (authority.kind != self.rows.header.binding.authority) return error.InvalidSchemaValidationJob;
        switch (self.rows.snapshot_cut) {
            .native => |cut| if (authority.kind != .native or authority.sequence != cut.sequence or q.origin.applied != null) return error.InvalidSchemaValidationJob,
            .raft => |cut| if (authority.kind != .raft or q.origin.applied == null or
                !std.meta.eql(q.origin.applied.?, @import("apply_receipts.zig").OrderedApplyReceipt{ .term = cut.term, .index = cut.index })) return error.InvalidSchemaValidationJob,
        }
        // Origin commits full canonical physical records, not just each
        // codec's logical digest or its embedded physical checksum.
        var buffer: [contract.max_header + 44]u8 = undefined;
        var fixed = std.heap.FixedBufferAllocator.init(&buffer);
        const header_bytes = try self.rows.header.encode(fixed.allocator());
        if (!std.meta.eql(q.origin.schema_header, @as(?Digest, contract.hash(header_bytes)))) return error.InvalidSchemaValidationJob;
        if (q.directory_head) |head| {
            const encoded = try head.encode();
            if (!std.meta.eql(q.origin.transaction_directory, @as(?Digest, contract.hash(&encoded))) or
                q.directory.members != head.debt or q.directory.dirty > q.directory.members) return error.InvalidSchemaValidationJob;
            if (self.rows.header.admitted != null) {
                if (!head.scope.eql(try debt.Scope.fromBinding(self.rows.header.binding))) return error.InvalidSchemaValidationJob;
            } else if (head.phase != .retired) return error.InvalidSchemaValidationJob;
        } else if (q.origin.transaction_directory != null or self.rows.header.admitted != null or
            !std.meta.eql(q.directory, debt.SnapshotStats{})) return error.InvalidSchemaValidationJob;
        if (q.history.foreign_records > q.history.records) return error.InvalidSchemaValidationJob;
        const local_count = q.history.records - q.history.foreign_records;
        if (q.history_head) |head| {
            const encoded = try head.encode();
            if (!head.namespace.eql(ns) or head.count != local_count or
                !std.meta.eql(q.origin.schema_outcomes, @as(?Digest, contract.hash(&encoded)))) return error.InvalidSchemaValidationJob;
            try head.requireCoveredBy(q.origin.applied orelse return error.InvalidSchemaValidationJob);
        } else if (q.origin.schema_outcomes != null or local_count != 0) return error.InvalidSchemaValidationJob;
        if (q.installation) |installed| {
            if (authority.kind != .raft) return error.InvalidSchemaValidationJob;
            try installed.requireDestination(self.rows.header.binding.fence.owner_group_id, @import("online_source_contract.zig").namespaceBytes(ns), self.physical_root);
            const anchor = try installed.historyAnchor(q.origin.applied);
            try anchor.require(q.source_history_head);
            if (q.source_history_head) |head| if (!head.namespace.eql(ns)) return error.InvalidSchemaValidationJob;
        } else if (q.source_history_head != null) return error.InvalidSchemaValidationJob;
    }
};

fn message(bytes: []const u8) Digest {
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly/schema-validation-qualified-root-image/v1");
    state.update(bytes);
    var result: Digest = undefined;
    state.final(&result);
    return result;
}

pub const Evidence = struct {
    value: Facts,
    signature: [64]u8,

    fn body(self: *const Evidence, buffer: []u8) ![]const u8 {
        try self.value.validate();
        var writer = std.Io.Writer.fixed(buffer);
        try std.json.Stringify.value(self.value, .{}, &writer);
        return writer.buffered();
    }
    /// Canonical signing preimage, not a producer or a signing operation.
    pub fn signatureDigest(self: *const Evidence) !Digest {
        var buffer: [max_body]u8 = undefined;
        return message(try self.body(&buffer));
    }
    /// `public_key` must come from independently checked root enrollment,
    /// never from this envelope. Verification is not an adoption decision.
    pub fn verify(self: *const Evidence, public_key: [32]u8) !void {
        var buffer: [max_body]u8 = undefined;
        const bytes = try self.body(&buffer);
        verifySignature(public_key, &message(bytes), self.signature) catch return error.InvalidSchemaValidationJob;
    }
    pub fn encode(self: *const Evidence, a: A) ![]u8 {
        var buffer: [max_body]u8 = undefined;
        const json = try self.body(&buffer);
        const bytes = try a.alloc(u8, 76 + json.len);
        @memcpy(bytes[0..8], magic);
        std.mem.writeInt(u32, bytes[8..12], @intCast(json.len), .little);
        @memcpy(bytes[12..][0..json.len], json);
        @memcpy(bytes[bytes.len - 64 ..], &self.signature);
        return bytes;
    }
    pub fn decode(a: A, bytes: []const u8, public_key: [32]u8) !Evidence {
        if (bytes.len < 76 or bytes.len > max_encoded or !std.mem.eql(u8, bytes[0..8], magic) or
            std.mem.readInt(u32, bytes[8..12], .little) != bytes.len - 76) return error.InvalidSchemaValidationJob;
        const json = bytes[12 .. bytes.len - 64];
        const signature = bytes[bytes.len - 64 ..][0..64].*;
        // Authenticate before parsing/allocating attacker-controlled input.
        verifySignature(public_key, &message(json), signature) catch return error.InvalidSchemaValidationJob;
        var parsed = std.json.parseFromSlice(Facts, a, json, .{ .ignore_unknown_fields = false, .max_value_len = 128 }) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidSchemaValidationJob,
        };
        defer parsed.deinit();
        const result: Evidence = .{ .value = parsed.value, .signature = signature };
        var buffer: [max_body]u8 = undefined;
        if (!std.mem.eql(u8, json, try result.body(&buffer))) return error.InvalidSchemaValidationJob;
        return result;
    }
};

/// Transport of bounded native work. Pending rows are progress only; complete
/// evidence includes independent transaction/debt/history/image qualification.
pub const AuditResult = union(enum) {
    /// Empty-input probes resume an owned native continuation; only a cache
    /// miss requests immutable inputs from metadata. This is not progress.
    needs_inputs: void,
    pending: RowSummary,
    complete: Evidence,

    pub const max_wire_bytes = max_encoded + 1024;
    pub fn requireFor(self: @This(), binding: contract.Binding, signing_root: u128, public_key: [32]u8) !void {
        try binding.validate();
        if (signing_root == 0) return error.InvalidSchemaValidationJob;
        switch (self) {
            .needs_inputs => {},
            .pending => |value| {
                try value.validateProgress();
                if (!std.meta.eql(value.header.binding, binding)) return error.SchemaValidationJobChanged;
            },
            .complete => |value| {
                if (value.value.signing_root != signing_root or !std.meta.eql(value.value.rows.header.binding, binding)) return error.SchemaValidationJobChanged;
                try value.verify(public_key);
            },
        }
    }
};

pub const AuditBudget = struct {
    records: u32 = 1024,
    time_ns: u64 = 5 * std.time.ns_per_ms,

    pub fn validate(self: @This()) !void {
        if (self.records == 0 or self.records > 65_536 or self.time_ns == 0 or self.time_ns > std.time.ns_per_s) return error.InvalidRelationalRowsBudget;
    }
};

fn verifySignature(key: [32]u8, digest: *const Digest, signature: [64]u8) !void {
    const Ed25519 = std.crypto.sign.Ed25519;
    const public_key = try Ed25519.PublicKey.fromBytes(key);
    try Ed25519.Signature.fromBytes(signature).verify(digest, public_key);
}
