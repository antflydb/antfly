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

//! Data-only relational lifecycle contract. Physical execution stays in its owner.
const std = @import("std");
const Allocator = std.mem.Allocator;
const identity = @import("doc_identity_namespace.zig");

pub const key = "\x00\x00__metadata__:restore_staging_owner";

pub const bootstrap_key = "\x00\x00__metadata__:restore_staging_bootstrap";
pub const generation_admission_receipt_key = "\x00\x00__metadata__:restore_generation_admissions_installed";
pub const GenerationAdmissionExpectation = struct {
    source_summary_digest: Digest,
    expected_receipt_digest: Digest,
};
pub const EmptyGenerationHandoffExpectation = struct {
    source_summary_digest: Digest,
    retired_digest: Digest,
    retired_count: u64,
    expected_install_receipt_digest: Digest,
};
/// Authenticated by the HA stream and pinned to the immutable reserved owner.
/// New hidden owners created after a seed can therefore be reconstructed before
/// applying their first lifecycle record, without consulting public placement.
pub const OwnerBootstrap = struct {
    scope: Scope,
    table_name: []const u8,
    schema_json: []const u8,
    read_schema_json: []const u8 = "",
    indexes_json: []const u8,
    byte_range: @import("../byte_range.zig").ByteRange,
    /// Exact namespace-bound summary of proof-only source records. Present on
    /// every sealed portable range, even when it accepted no generations.
    source_generation_proof_digest: ?Digest = null,
    /// Small immutable Plan projection. The request may carry bounded mapped
    /// entries, but the owner accepts only the Plan's exact receipt digest.
    generation_admission: ?GenerationAdmissionExpectation = null,
    /// Plan-bound FK authority to be installed only after the old owner has
    /// sealed its closed-fence source summary. The hidden target itself never
    /// trusts source IDs or generations as active authority.
    empty_generation_handoff: ?EmptyGenerationHandoffExpectation = null,

    pub fn jsonStringify(self: @This(), jw: anytype) @TypeOf(jw.*).Error!void {
        try jw.beginObject();
        try jw.objectField("scope");
        try @import("relational_integrity_json.zig").write(self.scope, jw);
        try jw.objectField("table_name");
        try jw.write(self.table_name);
        try jw.objectField("schema_json");
        try jw.write(self.schema_json);
        try jw.objectField("read_schema_json");
        try jw.write(self.read_schema_json);
        try jw.objectField("indexes_json");
        try jw.write(self.indexes_json);
        try jw.objectField("byte_range");
        try @import("relational_integrity_json.zig").write(self.byte_range, jw);
        if (self.source_generation_proof_digest) |proof_digest| {
            try jw.objectField("source_generation_proof_digest");
            try @import("relational_integrity_json.zig").write(proof_digest, jw);
        }
        if (self.generation_admission) |binding| {
            try jw.objectField("generation_admission");
            try @import("relational_integrity_json.zig").write(binding, jw);
        }
        if (self.empty_generation_handoff) |binding| {
            try jw.objectField("empty_generation_handoff");
            try @import("relational_integrity_json.zig").write(binding, jw);
        }
        try jw.endObject();
    }
    pub fn validate(self: @This()) !void {
        try self.scope.validate();
        // Empty-generation targets cannot be opened without the immutable
        // source authority they must install before publication. Catch a
        // missing cold-descriptor projection before it reaches Raft apply.
        if (self.scope.empty_generation != (self.empty_generation_handoff != null))
            return error.InvalidRestoreStagingCommand;
        if (self.table_name.len == 0 or (self.table_name.len > 255 and !(try @import("../../system_catalog/domain.zig").isRestoreTarget(self.table_name))) or std.mem.indexOfAny(u8, self.table_name, "/\\\x00") != null or std.mem.eql(u8, self.table_name, ".") or std.mem.eql(u8, self.table_name, "..") or
            self.schema_json.len +| self.read_schema_json.len > 4 * 1024 * 1024 or self.indexes_json.len == 0 or self.indexes_json.len > 4 * 1024 * 1024 or
            !std.unicode.utf8ValidateSlice(self.table_name) or !std.unicode.utf8ValidateSlice(self.schema_json) or !std.unicode.utf8ValidateSlice(self.read_schema_json) or !std.unicode.utf8ValidateSlice(self.indexes_json) or
            self.byte_range.start.len > 1024 * 1024 or self.byte_range.end.len > 1024 * 1024 or (self.byte_range.end.len != 0 and std.mem.order(u8, self.byte_range.start, self.byte_range.end) != .lt)) return error.InvalidRestoreStagingCommand;
        if (self.source_generation_proof_digest) |proof_digest| {
            if (self.scope.empty_generation or self.scope.rewrite != null or std.mem.allEqual(u8, &proof_digest, 0))
                return error.InvalidRestoreStagingCommand;
        }
        if (self.generation_admission) |binding| {
            const proof_digest = self.source_generation_proof_digest orelse return error.InvalidRestoreStagingCommand;
            if (self.scope.empty_generation or self.scope.rewrite != null or
                !std.mem.eql(u8, &proof_digest, &binding.source_summary_digest) or
                std.mem.allEqual(u8, &binding.source_summary_digest, 0) or
                std.mem.allEqual(u8, &binding.expected_receipt_digest, 0))
                return error.InvalidRestoreStagingCommand;
        }
        if (self.empty_generation_handoff) |binding| {
            if (!self.scope.empty_generation or self.source_generation_proof_digest != null or
                self.generation_admission != null or std.mem.allEqual(u8, &binding.source_summary_digest, 0) or
                std.mem.allEqual(u8, &binding.retired_digest, 0) or
                std.mem.allEqual(u8, &binding.expected_install_receipt_digest, 0))
                return error.InvalidRestoreStagingCommand;
        }
    }
    pub fn encode(self: @This(), alloc: Allocator) ![]u8 {
        try self.validate();
        const body = try std.json.Stringify.valueAlloc(alloc, self, .{});
        defer alloc.free(body);
        const encoded = try alloc.alloc(u8, body.len + 36);
        @memcpy(encoded[0..4], "ARB1");
        @memcpy(encoded[4..][0..body.len], body);
        @memcpy(encoded[encoded.len - 32 ..], &digest(encoded[0 .. encoded.len - 32]));
        return encoded;
    }
    pub fn decode(alloc: Allocator, bytes: []const u8) !std.json.Parsed(@This()) {
        if (bytes.len < 36 or bytes.len > 64 * 1024 * 1024 or !std.mem.eql(u8, bytes[0..4], "ARB1") or !std.mem.eql(u8, bytes[bytes.len - 32 ..], &digest(bytes[0 .. bytes.len - 32]))) return error.InvalidRestoreStagingRecord;
        var parsed = try std.json.parseFromSlice(@This(), alloc, bytes[4 .. bytes.len - 32], .{ .allocate = .alloc_always });
        errdefer parsed.deinit();
        try parsed.value.validate();
        return parsed;
    }
};

pub const Digest = [32]u8;

pub const Phase = enum { reserved, importing, imported, validated, published, canceled };

pub const Timestamp = struct { key: []const u8, timestamp: u64 };
pub const Artifact = struct { key: []const u8, value: []const u8 };

pub const ImportPage = struct {
    expected: Digest,
    next: []const u8,
    scope: Digest,
    timestamps: []const Timestamp,
    /// Native artifact materialization precedes logical row import. These are
    /// logical document-scoped records, never source ordinals or control keys.
    artifact_page: bool = false,
    /// Replay explicit vectors/edges only after all target primary identities
    /// exist. Reuses the ordinary artifact journal and projection consumers.
    projection_page: bool = false,
    artifacts: []const Artifact = &.{},
    /// Rewrite tails consume source integrity effects without copying their
    /// generations. Target claims are rebuilt by shared cohort activation.
    source_effects: u32 = 0,
    /// One bounded, replicated proof-only metadata page precedes row import.
    /// It is never admitted as a user artifact or active FK authority.
    source_generation_proof_page: bool = false,
};

/// Source admission evidence is not portable authority: physical child IDs and
/// FK generations change with the destination incarnation. Metadata derives
/// this ordered mapping from the sealed source manifest and immutable target
/// plan; the hidden parent owner independently compares it to imported proof.
pub const GenerationAdmissionMapping = struct {
    source_child_table_id: u64,
    source_child_table_name: []const u8,
    target_child_table_id: u64,
    target_child_table_name: []const u8,
    constraint_name: []const u8,
    source_generation: ?@import("relational_integrity_contract.zig").Generation,
    target_generation: ?@import("relational_integrity_contract.zig").Generation,
    source_scope_digest: Digest,

    pub fn validate(self: @This()) !void {
        if (self.source_child_table_id == 0 or self.target_child_table_id == 0 or
            self.source_child_table_id == self.target_child_table_id or
            self.source_child_table_name.len == 0 or self.source_child_table_name.len > 256 or
            self.target_child_table_name.len == 0 or self.target_child_table_name.len > 256 or
            self.constraint_name.len == 0 or self.constraint_name.len > 256 or
            !std.unicode.utf8ValidateSlice(self.source_child_table_name) or
            !std.unicode.utf8ValidateSlice(self.target_child_table_name) or
            !std.unicode.utf8ValidateSlice(self.constraint_name) or
            std.mem.allEqual(u8, &self.source_scope_digest, 0) or
            (self.source_generation == null) != (self.target_generation == null))
            return error.InvalidRestoreStagingCommand;
        if (self.source_generation) |generation| if (std.mem.allEqual(u8, &generation, 0)) return error.InvalidRestoreStagingCommand;
        if (self.target_generation) |generation| if (std.mem.allEqual(u8, &generation, 0)) return error.InvalidRestoreStagingCommand;
    }
};

pub const InstallGenerationAdmissions = struct {
    scope: Digest,
    source_summary_digest: Digest,
    mappings: []const GenerationAdmissionMapping,

    pub fn validate(self: @This()) !void {
        if (std.mem.allEqual(u8, &self.scope, 0) or std.mem.allEqual(u8, &self.source_summary_digest, 0) or
            self.mappings.len == 0 or self.mappings.len > 1024) return error.InvalidRestoreStagingCommand;
        for (self.mappings, 0..) |mapping, index| {
            try mapping.validate();
            if (index != 0) {
                const previous = self.mappings[index - 1];
                const order = std.mem.order(u8, previous.source_child_table_name, mapping.source_child_table_name);
                if (order == .gt or (order == .eq and std.mem.order(u8, previous.constraint_name, mapping.constraint_name) != .lt))
                    return error.InvalidRestoreStagingCommand;
            }
        }
    }
};

/// The metadata receipt expectation and replicated owner result share one
/// prefix-free logical digest, independent of Raft term/index or JSON layout.
pub fn admissionReceiptDigest(command: InstallGenerationAdmissions) !Digest {
    try command.validate();
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly restore mapped child generation admissions v1");
    hash.update(&command.scope);
    hash.update(&command.source_summary_digest);
    var number: [8]u8 = undefined;
    std.mem.writeInt(u64, &number, @intCast(command.mappings.len), .little);
    hash.update(&number);
    for (command.mappings) |mapping| {
        inline for (.{ mapping.source_child_table_id, mapping.target_child_table_id }) |id| {
            std.mem.writeInt(u64, &number, id, .little);
            hash.update(&number);
        }
        inline for (.{ mapping.source_child_table_name, mapping.target_child_table_name, mapping.constraint_name }) |name| {
            std.mem.writeInt(u64, &number, name.len, .little);
            hash.update(&number);
            hash.update(name);
        }
        hash.update(if (mapping.source_generation) |generation| &generation else &([_]u8{0} ** 16));
        hash.update(if (mapping.target_generation) |generation| &generation else &([_]u8{0} ** 16));
        hash.update(&mapping.source_scope_digest);
    }
    var result: Digest = undefined;
    hash.final(&result);
    return result;
}

/// Persisted in the same owner transaction as every mapped active scope and
/// the applied Raft marker. The logical digest is stable across HA replay;
/// term/index identify the original committed primary command.
pub const GenerationAdmissionReceipt = struct {
    scope: Digest,
    source_summary_digest: Digest,
    logical_digest: Digest,
    applied_term: u64,
    applied_index: u64,

    const magic = "ARGS";
    const encoded_len = 4 + 32 * 3 + 8 * 2 + 32;

    pub fn validate(self: @This()) !void {
        if (std.mem.allEqual(u8, &self.scope, 0) or
            std.mem.allEqual(u8, &self.source_summary_digest, 0) or
            std.mem.allEqual(u8, &self.logical_digest, 0) or
            self.applied_term == 0 or self.applied_index == 0)
            return error.InvalidRestoreGenerationAdmissionReceipt;
    }

    pub fn encode(self: @This()) ![encoded_len]u8 {
        try self.validate();
        var bytes: [encoded_len]u8 = undefined;
        @memcpy(bytes[0..4], magic);
        @memcpy(bytes[4..36], &self.scope);
        @memcpy(bytes[36..68], &self.source_summary_digest);
        @memcpy(bytes[68..100], &self.logical_digest);
        std.mem.writeInt(u64, bytes[100..108], self.applied_term, .little);
        std.mem.writeInt(u64, bytes[108..116], self.applied_index, .little);
        std.crypto.hash.Blake3.hash(bytes[0..116], bytes[116..148], .{});
        return bytes;
    }

    pub fn decode(bytes: []const u8) !@This() {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..4], magic))
            return error.InvalidRestoreGenerationAdmissionReceipt;
        var checksum: Digest = undefined;
        std.crypto.hash.Blake3.hash(bytes[0..116], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, bytes[116..148])) return error.InvalidRestoreGenerationAdmissionReceipt;
        const result: @This() = .{
            .scope = bytes[4..36].*,
            .source_summary_digest = bytes[36..68].*,
            .logical_digest = bytes[68..100].*,
            .applied_term = std.mem.readInt(u64, bytes[100..108], .little),
            .applied_index = std.mem.readInt(u64, bytes[108..116], .little),
        };
        try result.validate();
        return result;
    }
};

test "mapped restore admissions reject duplicates and bind every source and target identity" {
    const first: GenerationAdmissionMapping = .{
        .source_child_table_id = 11,
        .source_child_table_name = "children",
        .target_child_table_id = 21,
        .target_child_table_name = "restored_children",
        .constraint_name = "parent_fk",
        .source_generation = @splat(1),
        .target_generation = @splat(2),
        .source_scope_digest = @splat(3),
    };
    var mappings = [_]GenerationAdmissionMapping{first};
    const command: InstallGenerationAdmissions = .{
        .scope = @splat(4),
        .source_summary_digest = @splat(5),
        .mappings = &mappings,
    };
    const original = try admissionReceiptDigest(command);
    mappings[0].target_child_table_id += 1;
    try std.testing.expect(!std.mem.eql(u8, &original, &try admissionReceiptDigest(command)));
    mappings[0] = first;
    mappings[0].source_generation = null;
    try std.testing.expectError(error.InvalidRestoreStagingCommand, admissionReceiptDigest(command));
    mappings[0] = first;
    const duplicate = [_]GenerationAdmissionMapping{ first, first };
    try std.testing.expectError(error.InvalidRestoreStagingCommand, admissionReceiptDigest(.{
        .scope = command.scope,
        .source_summary_digest = command.source_summary_digest,
        .mappings = &duplicate,
    }));
}

test "mapped restore admission receipt detects physical corruption" {
    const receipt: GenerationAdmissionReceipt = .{
        .scope = @splat(1),
        .source_summary_digest = @splat(2),
        .logical_digest = @splat(3),
        .applied_term = 4,
        .applied_index = 5,
    };
    var bytes = try receipt.encode();
    try std.testing.expectEqualDeep(receipt, try GenerationAdmissionReceipt.decode(&bytes));
    bytes[70] ^= 1;
    try std.testing.expectError(error.InvalidRestoreGenerationAdmissionReceipt, GenerationAdmissionReceipt.decode(&bytes));
}

pub const Control = union(enum) {
    begin: Scope,
    import_page: ImportPage,
    /// Explicit tag prevents old readers from treating transformed/tail effects
    /// as a preservation-only restore page.
    rewrite_page: ImportPage,
    install_generation_admissions: InstallGenerationAdmissions,
    finish: struct { scope: Digest, phase: Phase },
    pub fn jsonStringify(self: @This(), jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("relational_integrity_json.zig").write(self, jw);
    }
};

pub const Scope = struct {
    pub fn nativeJsonSkipField(self: @This(), comptime name: []const u8) bool {
        return (std.mem.eql(u8, name, "empty_generation") and !self.empty_generation) or
            (std.mem.eql(u8, name, "graph_retirement_digest") and self.graph_retirement_digest == null);
    }
    plan_id: [16]u8,
    plan_digest: Digest,
    source_artifact_digest: Digest,
    source_descriptor_digest: Digest = @splat(0),
    source_namespace: identity.Namespace,
    target_namespace: identity.Namespace,
    target_schema_digest: Digest,
    preserve_artifacts: bool = false,
    /// A fresh, proven-pristine owner; never accepts source rows or artifacts.
    empty_generation: bool = false,
    /// Exact graph declaration/old-new incarnation proof from the immutable
    /// metadata plan. Only an empty owner may carry this retirement barrier.
    graph_retirement_digest: ?Digest = null,
    rewrite: ?@import("relational_rewrite_contract.zig").Binding = null,

    pub fn validateReservation(self: Scope) !void {
        if (self.empty_generation and (self.preserve_artifacts or self.rewrite != null)) return error.InvalidRestoreStagingCommand;
        if (!self.empty_generation and self.graph_retirement_digest != null) return error.InvalidRestoreStagingCommand;
        if (self.rewrite) |rewrite| {
            try rewrite.validate();
            if (rewrite.source_scope) |source| if (!source.receiver_namespace.eql(self.target_namespace) or
                (self.source_namespace.table_id != 0 and !source.fence.namespace.eql(self.source_namespace))) return error.InvalidRestoreStagingCommand;
        }
        if (std.mem.allEqual(u8, &self.plan_id, 0) or std.mem.allEqual(u8, &self.plan_digest, 0) or
            self.target_namespace.table_id == 0 or self.target_namespace.shard_id == 0 or self.target_namespace.range_id == 0)
            return error.InvalidRestoreStagingCommand;
    }

    pub fn validate(self: Scope) !void {
        if (self.empty_generation and (self.preserve_artifacts or self.rewrite != null or !std.mem.allEqual(u8, &self.source_artifact_digest, 0) or !std.mem.allEqual(u8, &self.source_descriptor_digest, 0))) return error.InvalidRestoreStagingCommand;
        if (!self.empty_generation and self.graph_retirement_digest != null) return error.InvalidRestoreStagingCommand;
        if (self.preserve_artifacts and self.rewrite != null) return error.InvalidRestoreStagingCommand;
        if (self.rewrite) |rewrite| {
            try rewrite.validate();
            if (rewrite.source_scope) |source| if (!source.fence.namespace.eql(self.source_namespace) or !source.receiver_namespace.eql(self.target_namespace)) return error.InvalidRestoreStagingCommand;
        }
        if (std.mem.allEqual(u8, &self.plan_id, 0) or std.mem.allEqual(u8, &self.plan_digest, 0) or
            (!self.empty_generation and std.mem.allEqual(u8, &self.source_artifact_digest, 0)) or self.target_namespace.table_id == 0 or
            self.target_namespace.shard_id == 0 or self.target_namespace.range_id == 0 or
            self.source_namespace.table_id == 0 or self.source_namespace.table_id == self.target_namespace.table_id)
            return error.InvalidRestoreStagingCommand;
    }
    pub fn digest(self: Scope) Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly-restore-owner-scope-v1");
        hash.update(&self.plan_id);
        hash.update(&self.plan_digest);
        hash.update(&self.source_artifact_digest);
        hash.update(&self.source_descriptor_digest);
        inline for (.{ self.source_namespace, self.target_namespace }) |namespace| {
            inline for (.{ namespace.table_id, namespace.shard_id, namespace.range_id }) |value| {
                var bytes: [8]u8 = undefined;
                std.mem.writeInt(u64, &bytes, value, .little);
                hash.update(&bytes);
            }
        }
        hash.update(&self.target_schema_digest);
        if (self.preserve_artifacts) hash.update("native-artifact-preservation-v1");
        if (self.empty_generation) hash.update("empty-generation-v1");
        if (self.graph_retirement_digest) |retirement| {
            hash.update("graph-retirement-v1");
            hash.update(&retirement);
        }
        if (self.rewrite) |rewrite| {
            hash.update("relational-rewrite-v1");
            hash.update(&rewrite.program_digest);
            hash.update(&rewrite.retained_pin);
            hash.update(&rewrite.snapshot_certificate);
            inline for (.{ rewrite.retained_epoch, rewrite.retained_start, rewrite.source_applied_index }) |value| {
                var bytes: [8]u8 = undefined;
                std.mem.writeInt(u64, &bytes, value, .little);
                hash.update(&bytes);
            }
        }
        var result: Digest = undefined;
        hash.final(&result);
        return result;
    }
};

test "restore empty generation scope cannot carry source authority or nonempty progress" {
    const alloc = std.testing.allocator;
    var scope: Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(0), .source_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .target_namespace = .{ .table_id = 3, .shard_id = 4, .range_id = 4 }, .target_schema_digest = @splat(5), .empty_generation = true };
    try scope.validate();
    const bytes = try (Progress{ .scope = scope, .phase = .imported }).encode(alloc);
    defer alloc.free(bytes);
    var decoded = try Progress.decode(alloc, bytes);
    defer decoded.deinit();
    try std.testing.expect(decoded.value.scope.empty_generation);
    try std.testing.expectError(error.InvalidRestoreStagingCommand, (Progress{ .scope = scope, .phase = .imported, .rows = 1 }).encode(alloc));
    try std.testing.expectError(error.InvalidRestoreStagingCommand, (Progress{ .scope = scope }).encode(alloc));
    scope.preserve_artifacts = true;
    try std.testing.expectError(error.InvalidRestoreStagingCommand, scope.validate());
    scope.preserve_artifacts = false;
    scope.source_artifact_digest = @splat(9);
    try std.testing.expectError(error.InvalidRestoreStagingCommand, scope.validate());
}

test "empty generation owner bootstrap requires plan-bound handoff authority" {
    const scope: Scope = .{
        .plan_id = @splat(1),
        .plan_digest = @splat(2),
        .source_artifact_digest = @splat(0),
        .source_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 },
        .target_namespace = .{ .table_id = 3, .shard_id = 4, .range_id = 4 },
        .target_schema_digest = @splat(5),
        .empty_generation = true,
    };
    var bootstrap: OwnerBootstrap = .{
        .scope = scope,
        .table_name = "rows",
        .schema_json = "{}",
        .indexes_json = "{}",
        .byte_range = .{ .start = "", .end = "" },
    };
    try std.testing.expectError(error.InvalidRestoreStagingCommand, bootstrap.validate());
    bootstrap.empty_generation_handoff = .{
        .source_summary_digest = @splat(6),
        .retired_digest = @splat(7),
        .expected_install_receipt_digest = @splat(8),
        .retired_count = 0,
    };
    try bootstrap.validate();
}

test "sealed portable bootstrap requires source proof even without mapped admissions" {
    const alloc = std.testing.allocator;
    const scope: Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(3), .source_namespace = .{ .table_id = 4, .shard_id = 5, .range_id = 5 }, .target_namespace = .{ .table_id = 6, .shard_id = 7, .range_id = 7 }, .target_schema_digest = @splat(8) };
    var bootstrap: OwnerBootstrap = .{ .scope = scope, .table_name = "parent", .schema_json = "{}", .indexes_json = "{}", .byte_range = .{ .start = "", .end = "" }, .source_generation_proof_digest = @splat(9) };
    const encoded = try bootstrap.encode(alloc);
    defer alloc.free(encoded);
    var decoded = try OwnerBootstrap.decode(alloc, encoded);
    defer decoded.deinit();
    try std.testing.expectEqualDeep(bootstrap.source_generation_proof_digest, decoded.value.source_generation_proof_digest);
    try std.testing.expect(decoded.value.generation_admission == null);
    bootstrap.generation_admission = .{ .source_summary_digest = @splat(10), .expected_receipt_digest = @splat(11) };
    try std.testing.expectError(error.InvalidRestoreStagingCommand, bootstrap.validate());
    bootstrap.generation_admission.?.source_summary_digest = @splat(9);
    try bootstrap.validate();
    bootstrap.source_generation_proof_digest = null;
    try std.testing.expectError(error.InvalidRestoreStagingCommand, bootstrap.validate());
    bootstrap.generation_admission = null;
    bootstrap.source_generation_proof_digest = @splat(0);
    try std.testing.expectError(error.InvalidRestoreStagingCommand, bootstrap.validate());
}

pub const Progress = struct {
    scope: Scope,
    phase: Phase = .importing,
    source_generation_proofs_complete: bool = false,
    rows: u64 = 0,
    cursor: []const u8 = "",
    logical_digest: Digest = @splat(0),
    artifact_cursor: []const u8 = "",
    artifacts_complete: bool = false,
    rows_complete: bool = false,
    projection_cursor: []const u8 = "",
    rewrite: ?@import("relational_rewrite_contract.zig").Progress = null,
    pub fn jsonStringify(self: @This(), jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("relational_integrity_json.zig").write(self, jw);
    }
    pub fn encode(self: Progress, alloc: Allocator) ![]u8 {
        try self.validateRewrite();
        if (self.phase == .reserved or (self.phase == .canceled and self.scope.source_namespace.table_id == 0 and self.rows == 0 and self.cursor.len == 0)) try self.scope.validateReservation() else try self.scope.validate();
        if (self.cursor.len > 1024 * 1024 or self.artifact_cursor.len > 1024 * 1024 or self.projection_cursor.len > 1024 * 1024) return error.InvalidRestoreStagingCommand;
        const body = try std.json.Stringify.valueAlloc(alloc, self, .{});
        defer alloc.free(body);
        const out = try alloc.alloc(u8, body.len + 36);
        @memcpy(out[0..4], "ARS1");
        @memcpy(out[4..][0..body.len], body);
        @memcpy(out[out.len - 32 ..], &digest(out[0 .. out.len - 32]));
        return out;
    }
    pub fn decode(alloc: Allocator, bytes: []const u8) !std.json.Parsed(Progress) {
        if (bytes.len < 36 or bytes.len > 8 * 1024 * 1024 or !std.mem.eql(u8, bytes[0..4], "ARS1") or
            !std.mem.eql(u8, bytes[bytes.len - 32 ..], &digest(bytes[0 .. bytes.len - 32]))) return error.InvalidRestoreStagingRecord;
        var parsed = try std.json.parseFromSlice(Progress, alloc, bytes[4 .. bytes.len - 32], .{ .allocate = .alloc_always });
        errdefer parsed.deinit();
        if (parsed.value.phase == .reserved or (parsed.value.phase == .canceled and parsed.value.scope.source_namespace.table_id == 0 and parsed.value.rows == 0 and parsed.value.cursor.len == 0)) {
            parsed.value.scope.validateReservation() catch return error.InvalidRestoreStagingRecord;
        } else parsed.value.scope.validate() catch return error.InvalidRestoreStagingRecord;
        if (parsed.value.cursor.len > 1024 * 1024 or parsed.value.artifact_cursor.len > 1024 * 1024 or parsed.value.projection_cursor.len > 1024 * 1024) return error.InvalidRestoreStagingRecord;
        parsed.value.validateRewrite() catch return error.InvalidRestoreStagingRecord;
        return parsed;
    }
    fn validateRewrite(self: Progress) !void {
        if (self.scope.empty_generation and (self.phase == .importing or self.rows != 0 or self.cursor.len != 0 or self.artifact_cursor.len != 0 or self.projection_cursor.len != 0 or self.rewrite != null or !std.mem.allEqual(u8, &self.logical_digest, 0))) return error.InvalidRestoreStagingCommand;
        if (self.scope.rewrite) |binding| {
            // Reservations and terminal cancellation may precede initialization.
            if (self.phase == .reserved or (self.phase == .canceled and self.rewrite == null)) return;
            const progress = self.rewrite orelse return error.InvalidRestoreStagingCommand;
            try progress.validate(binding);
            if ((self.phase == .imported or self.phase == .validated or self.phase == .published) and progress.final_cut == null) return error.InvalidRestoreStagingCommand;
        } else if (self.rewrite != null) return error.InvalidRestoreStagingCommand;
    }
    pub fn receipt(self: Progress) Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly-restore-owner-receipt-v1");
        hash.update(&self.scope.digest());
        hash.update(@tagName(self.phase));
        hash.update(&self.logical_digest);
        if (self.rewrite) |rewrite| {
            var sequence: [8]u8 = undefined;
            std.mem.writeInt(u64, &sequence, rewrite.sequence, .little);
            hash.update(&sequence);
            if (rewrite.final_cut) |cut| {
                std.mem.writeInt(u64, &sequence, cut.applied_index, .little);
                hash.update(&sequence);
                hash.update(&cut.digest);
            }
        }
        var count: [8]u8 = undefined;
        std.mem.writeInt(u64, &count, self.rows, .little);
        hash.update(&count);
        var result: Digest = undefined;
        hash.final(&result);
        return result;
    }
};

pub fn digest(bytes: []const u8) Digest {
    var out: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &out, .{});
    return out;
}
