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

//! Pure, private shared-restore owner request and response contract.
const std = @import("std");
const staging = @import("../storage/db/restore_staging_contract.zig");
const metadata_staging = @import("../metadata/restore_provisioning_contract.zig");
const backups = @import("backup_contract.zig");
/// Includes the 4 MiB rewrite program, a base64-encoded 1 MiB source
/// chunk, and their binary-safe JSON byte-array expansion (at most four
/// bytes per input byte). Fixed scope/certificate overhead fits in the
/// remaining >10 MiB. This is below the existing 64 MiB HTTP body limit.
pub const max_request_bytes = 32 * 1024 * 1024;

pub fn validateRequestSize(size: usize) !void {
    // An immutable oversized request cannot be repaired by retrying its job.
    if (size > max_request_bytes) return error.InvalidBackupRequest;
}
pub const Source = struct {
    location: []const u8,
    connection: []const u8 = "",
    artifact: metadata_staging.SourceArtifact,
    /// Private rewrite transport. Its artifact digest is the immutable logical
    /// certificate digest, not a repository object's byte checksum.
    peer_descriptor: ?@import("../storage/db/source_artifact_transfer.zig").Descriptor = null,
};
pub const Request = struct {
    scope: staging.Scope,
    action: enum { begin, import_page, status, validate, publish, cancel },
    source: ?Source = null,
    source_chunk: ?@import("../storage/db/source_artifact_transfer.zig").ReadResponse = null,
    rewrite: ?@import("../storage/db/relational_rewrite_contract.zig").Intent = null,
    rewrite_tail: ?@import("../storage/db/relational_rewrite_contract.zig").TailChunk = null,
    rewrite_finish: ?@import("../storage/db/relational_rewrite_contract.zig").FinalReceipt = null,
    max_rows: u16 = 128,
    pub fn jsonStringify(self: @This(), jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("../storage/db/relational_integrity_json.zig").write(self, jw);
    }
    /// A post-proposal receipt read must not replay import-only data or final
    /// cut commands. Its exact scope is all the authority a status read needs.
    pub fn statusRead(self: Request) Request {
        return .{ .scope = self.scope, .action = .status, .max_rows = self.max_rows };
    }
    pub fn validate(self: Request, group_id: u64) !void {
        try self.scope.validate();
        if (self.scope.empty_generation and (self.action == .import_page or self.source != null or self.source_chunk != null or self.rewrite != null or self.rewrite_tail != null or self.rewrite_finish != null)) return error.InvalidRestoreStagingCommand;
        if (self.scope.target_namespace.shard_id != group_id or self.max_rows == 0 or self.max_rows > 128) return error.InvalidRestoreStagingCommand;
        if (self.source_chunk != null and (self.action != .import_page or self.source == null or self.source.?.peer_descriptor == null or self.rewrite_tail != null or self.rewrite_finish != null)) return error.InvalidRestoreStagingCommand;
        if (self.rewrite) |intent| {
            try intent.validate();
            const binding = self.scope.rewrite orelse return error.InvalidRestoreStagingCommand;
            if (!std.mem.eql(u8, &intent.program_digest, &binding.program_digest)) return error.RestoreStagingScopeChanged;
        }
        if (self.rewrite_tail) |chunk| {
            try chunk.validate();
            const binding = self.scope.rewrite orelse return error.InvalidRestoreStagingCommand;
            if (self.action != .import_page or self.source != null or self.rewrite == null or self.rewrite_finish != null or
                !std.mem.eql(u8, &chunk.pin, &binding.retained_pin)) return error.InvalidRestoreStagingCommand;
        }
        if (self.rewrite_finish) |receipt| {
            if (self.action != .import_page or self.source != null or self.rewrite == null) return error.InvalidRestoreStagingCommand;
            try receipt.validate(self.scope.rewrite orelse return error.InvalidRestoreStagingCommand);
        }
        if (self.action == .import_page) {
            if ((self.rewrite == null) != (self.scope.rewrite == null)) return error.InvalidRestoreStagingCommand;
            if (self.rewrite_tail != null or self.rewrite_finish != null) return;
            const source = self.source orelse return error.RestoreSourceProofMissing;
            if ((source.location.len == 0 and source.peer_descriptor == null) or source.location.len > 4096 or source.connection.len > 256 or
                source.artifact.target_group_id != group_id or !source.artifact.source_namespace.eql(self.scope.source_namespace) or
                !std.mem.eql(u8, &source.artifact.artifact_sha256, &self.scope.source_artifact_digest)) return error.RestoreStagingScopeChanged;
            if (source.peer_descriptor) |descriptor| try validatePeerSource(source, self.scope, descriptor);
            try backups.validateArtifactRelativePath(source.artifact.snapshot_path);
        }
    }
};
pub const Response = struct {
    phase: staging.Phase,
    rows: u64,
    receipt: staging.Digest,
    rewrite: ?@import("../storage/db/relational_rewrite_contract.zig").Progress = null,
    tail_next: u32 = 0,
    source_next_offset: u64 = 0,
};

pub fn validatePeerSource(source: Source, scope: staging.Scope, descriptor: @import("../storage/db/source_artifact_transfer.zig").Descriptor) !void {
    const binding = scope.rewrite orelse return error.RestoreSourceProofMissing;
    try binding.validate();
    const source_scope = binding.source_scope orelse return error.RestoreSourceProofMissing;
    const artifact_binding = source.artifact.rewrite orelse return error.RestoreSourceProofMissing;
    if (!std.meta.eql(binding, artifact_binding)) return error.RestoreStagingScopeChanged;
    try descriptor.scope.validate();
    const certificate_digest = try descriptor.certificate.digest();
    if (source.location.len != 0 or source.connection.len != 0 or source.artifact.format != .portable or source.artifact.cohort_seal != null or
        descriptor.total_bytes == 0 or descriptor.total_bytes > std.math.maxInt(i64) or descriptor.total_bytes != source.artifact.artifact_size_bytes or
        !std.meta.eql(descriptor.scope, source_scope) or !source_scope.receiver_namespace.eql(scope.target_namespace) or !descriptor.certificate.cut.namespace.eql(scope.source_namespace) or
        descriptor.certificate.cut.applied_index != binding.source_applied_index or descriptor.certificate.cut.retained_start != binding.retained_start or
        !std.mem.eql(u8, &certificate_digest, &binding.snapshot_certificate) or !std.mem.eql(u8, &certificate_digest, &source.artifact.artifact_sha256)) return error.RestoreStagingScopeChanged;
}
