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

//! Immutable rewrite identity carried by the shared restore reservation. It is
//! not permission to recapture a source or publish a target. The coordinator
//! must reserve retention before the certified snapshot cut and publish the
//! entire dependency cohort only after every owner's final-tail receipt.
const std = @import("std");
pub const Digest = [32]u8;
pub const max_source_schemas = 256;
pub const max_schema_bytes = 4 * 1024 * 1024;

pub const Intent = struct {
    version: u8 = 1,
    /// Unchanged document tables in the dependency cohort retain their exact
    /// logical values. They still participate in the same tail/cutover proof.
    preserve_document: bool = false,
    source_schemas: []const []const u8,
    target_schema: []const u8,
    target_read_schema: []const u8 = "",
    apply_defaults_to_absent: bool = false,
    allow_column_drops: bool = false,
    /// Digest of compiled semantic programs, NOT numeric schema versions.
    program_digest: Digest,

    pub fn validate(self: Intent) !void {
        if (self.version != 1 or self.source_schemas.len == 0 or self.source_schemas.len > max_source_schemas or
            self.target_schema.len == 0 or std.mem.allEqual(u8, &self.program_digest, 0)) return error.InvalidRestoreStagingCommand;
        var bytes = self.target_schema.len +| self.target_read_schema.len;
        for (self.source_schemas) |source| {
            if (source.len == 0) return error.InvalidRestoreStagingCommand;
            bytes = std.math.add(usize, bytes, source.len) catch return error.InvalidRestoreStagingCommand;
        }
        if (bytes > max_schema_bytes) return error.InvalidRestoreStagingCommand;
        if (self.preserve_document) {
            if (self.apply_defaults_to_absent or self.allow_column_drops or self.source_schemas.len != @as(usize, if (self.target_read_schema.len == 0) 1 else 2)) return error.InvalidRestoreStagingCommand;
            var active = false;
            var previous = self.target_read_schema.len == 0;
            for (self.source_schemas) |definition| {
                if (std.mem.eql(u8, definition, self.target_schema) and !active) active = true else if (std.mem.eql(u8, definition, self.target_read_schema) and !previous) previous = true else return error.InvalidRestoreStagingCommand;
            }
            if (!active or !previous) return error.InvalidRestoreStagingCommand;
        } else if (self.target_read_schema.len != 0) return error.InvalidRestoreStagingCommand;
    }
};

pub const Binding = struct {
    version: u8 = 1,
    program_digest: Digest,
    /// Immutable source pin; distinct from the artifact's transport checksum.
    retained_pin: Digest,
    snapshot_certificate: Digest,
    retained_epoch: u64,
    retained_start: u64,
    source_applied_index: u64,
    source_scope: ?@import("online_source_contract.zig").Scope = null,

    pub fn validate(self: Binding) !void {
        if (self.version != 1 or self.retained_epoch == 0 or self.source_applied_index == 0 or
            std.mem.allEqual(u8, &self.program_digest, 0) or std.mem.allEqual(u8, &self.retained_pin, 0) or std.mem.allEqual(u8, &self.snapshot_certificate, 0)) return error.InvalidRestoreStagingCommand;
        if (self.source_scope) |scope| {
            try scope.validate();
            if (scope.fence.role != .rewrite_source or scope.consumer_epoch != self.retained_epoch or !std.mem.eql(u8, &scope.pin(), &self.retained_pin)) return error.InvalidRestoreStagingCommand;
        }
    }
};

pub const FinalCut = struct {
    sequence: u64,
    applied_index: u64,
    digest: Digest,
};

/// Obtained from an authenticated leader-fenced source status, never client
/// input. Self-consistency binds the private receipt to its admitted scope;
/// source authority still belongs to the coordinator's ReadIndex operation.
pub const FinalReceipt = struct {
    pin: Digest,
    start: u64,
    admitted_applied_index: u64,
    certificate_digest: Digest,
    cut: FinalCut,

    pub fn fromProgress(binding: Binding, value: anytype) !FinalReceipt {
        const scope = binding.source_scope orelse return error.RestoreSourceProofMissing;
        const certificate = value.published_certificate orelse return error.RestoreSourceProofMissing;
        if (value.phase != .fenced or value.snapshot_phase != .published or !std.mem.eql(u8, &value.namespace, &scope.namespace()) or
            value.consumer_epoch != binding.retained_epoch or !certificate.cut.namespace.eql(scope.fence.namespace) or
            certificate.cut.retained_start != binding.retained_start or certificate.cut.applied_index != binding.source_applied_index or
            !std.mem.eql(u8, &value.snapshot_certificate, &try certificate.digest())) return error.RestoreStagingScopeChanged;
        const receipt: FinalReceipt = .{ .pin = value.pin, .start = value.start, .admitted_applied_index = value.admitted_applied_index, .certificate_digest = value.snapshot_certificate, .cut = .{ .sequence = value.through_sequence, .applied_index = value.applied_index, .digest = value.cut_digest } };
        try receipt.validate(binding);
        return receipt;
    }

    pub fn validate(self: FinalReceipt, binding: Binding) !void {
        try binding.validate();
        const scope = binding.source_scope orelse return error.RestoreSourceProofMissing;
        if (!std.mem.eql(u8, &self.pin, &binding.retained_pin) or self.start != binding.retained_start or
            self.admitted_applied_index != binding.source_applied_index or self.cut.sequence < self.start or self.cut.applied_index <= self.admitted_applied_index or
            !std.mem.eql(u8, &self.certificate_digest, &binding.snapshot_certificate) or
            !std.mem.eql(u8, &self.cut.digest, &@import("online_source_contract.zig").finalCutDigest(scope, self.cut.sequence, self.cut.applied_index))) return error.RestoreStagingScopeChanged;
    }
};

/// Bounded physical REF3 transfer. Receiver progress advances only after the
/// complete frame checksum and all transformed effects have been committed.
pub const TailChunk = struct {
    pin: Digest,
    sequence: u64,
    frame_digest: Digest,
    total: u32,
    offset: u32,
    data: []const u8,

    pub fn validate(self: TailChunk) !void {
        if (self.sequence == 0 or self.total == 0 or self.total > 16 * 1024 * 1024 or self.offset >= self.total or
            self.data.len == 0 or self.data.len > 64 * 1024 or self.data.len > self.total - self.offset or
            std.mem.allEqual(u8, &self.pin, 0) or std.mem.allEqual(u8, &self.frame_digest, 0)) return error.InvalidRestoreStagingCommand;
    }

    pub fn jsonStringify(self: TailChunk, stream: anytype) !void {
        try @import("relational_integrity_json.zig").write(self, stream);
    }
};

pub const Progress = struct {
    snapshot_complete: bool = false,
    /// Last completely applied REF3 frame. Acknowledgement may advance only
    /// through this value, never through a partially applied frame.
    sequence: u64,
    frame_digest: Digest = @splat(0),
    frame_offset: u32 = 0,
    frame_remaining: u32 = 0,
    final_cut: ?FinalCut = null,

    pub fn validate(self: Progress, binding: Binding) !void {
        try binding.validate();
        if (self.sequence < binding.retained_start or
            ((self.frame_offset == 0) != (self.frame_remaining == 0)) or
            ((self.frame_offset == 0) != std.mem.allEqual(u8, &self.frame_digest, 0)) or
            self.frame_offset > 16 * 1024 * 1024 or self.frame_remaining > 65536 or
            (!self.snapshot_complete and (self.sequence != binding.retained_start or self.frame_offset != 0 or self.final_cut != null))) return error.InvalidRestoreStagingCommand;
        if (self.final_cut) |cut| if (!self.snapshot_complete or self.frame_offset != 0 or cut.sequence != self.sequence or
            cut.applied_index < binding.source_applied_index or std.mem.allEqual(u8, &cut.digest, 0)) return error.InvalidRestoreStagingCommand;
    }
};
