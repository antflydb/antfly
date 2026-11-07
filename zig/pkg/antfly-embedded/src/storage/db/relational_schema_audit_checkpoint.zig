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

//! Authenticated root-local row audit resumption. This is neither a portable
//! backup artifact nor authority to publish or adopt a schema owner. The host
//! supplies the retained live physical DB and signing-root lifecycle leases.
const std = @import("std");
const job = @import("relational_nullability_job.zig");
const contract = @import("relational_schema_owner_contract.zig");
const signing = @import("root_signing_identity.zig");
const checkpoints = @import("root_audit_checkpoint_io.zig");
const Context = @import("../../api/operation.zig").RequestContext;
const A = std.mem.Allocator;
const Digest = [32]u8;
const errors: checkpoints.Errors = .{ .invalid = error.InvalidSchemaValidationJob, .scope_changed = error.SchemaValidationRuntimeRecoveryRequired };
const magic = "ASRA\x01\x00\x00\x00";
const max_header = contract.max_header + 2048;
const max_cursor = @import("relational_index_limits.zig").max_cursor_key_bytes;
const max_checkpoint = 80 + max_header + max_cursor;
const Header = struct {
    signing_root: u128,
    physical_root: u128,
    summary: job.RowAudit.Summary,
    complete: bool,
};

fn message(bytes: []const u8) Digest {
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly/schema-validation-root-row-checkpoint/v1");
    state.update(bytes);
    var result: Digest = undefined;
    state.final(&result);
    return result;
}

pub const Evidence = struct {
    signing_root: u128,
    physical_root: u128,
    rows: job.RowAudit.Summary,
    checkpoint_digest: Digest,
};
pub const Result = @import("relational_schema_root_contract.zig").AuditResult;

pub const Guard = struct {
    alloc: A,
    io: std.Io,
    root_dir: []const u8,
    root: signing.State,
    path: []u8,
    directory: std.Io.Dir,
    lock: std.Io.File,
    context: Context,
    checkpoint_digest: ?Digest = null,
    physical_root: u128,
    expected_header: Digest,
    expected_cut: @import("receipt_position.zig").Position,

    pub fn deinit(self: *@This()) void {
        checkpoints.close(self);
    }
    fn requireProgress(self: *const @This(), progress: *const job.RowAudit.Progress) !void {
        try progress.validate();
        if (!std.mem.eql(u8, &self.expected_header, &try progress.summary.header.digest()) or
            !std.meta.eql(self.expected_cut, progress.summary.snapshot_cut)) return error.SchemaValidationJobChanged;
    }
    fn encode(self: *const @This(), progress: *const job.RowAudit.Progress) ![]u8 {
        try self.requireProgress(progress);
        const value: Header = .{ .signing_root = self.root.root_incarnation, .physical_root = self.physical_root, .summary = progress.summary, .complete = progress.complete };
        const json = try std.json.Stringify.valueAlloc(self.alloc, value, .{});
        defer self.alloc.free(json);
        if (json.len > max_header) return error.InvalidSchemaValidationJob;
        const bytes = try self.alloc.alloc(u8, 80 + json.len + progress.after.len);
        errdefer self.alloc.free(bytes);
        @memcpy(bytes[0..8], magic);
        std.mem.writeInt(u32, bytes[8..12], @intCast(json.len), .little);
        std.mem.writeInt(u32, bytes[12..16], @intCast(progress.after.len), .little);
        @memcpy(bytes[16..][0..json.len], json);
        @memcpy(bytes[16 + json.len ..][0..progress.after.len], progress.after);
        @memcpy(bytes[bytes.len - 64 ..], &try self.root.sign(&message(bytes[0 .. bytes.len - 64])));
        return bytes;
    }
    fn decode(self: *const @This(), bytes: []const u8) !job.RowAudit.Progress {
        if (bytes.len < 80 or bytes.len > max_checkpoint or !std.mem.eql(u8, bytes[0..8], magic)) return error.InvalidSchemaValidationJob;
        const json_len = std.mem.readInt(u32, bytes[8..12], .little);
        const cursor_len = std.mem.readInt(u32, bytes[12..16], .little);
        if (json_len > max_header or cursor_len > max_cursor or @as(u64, json_len) + cursor_len + 80 != bytes.len) return error.InvalidSchemaValidationJob;
        // Authenticate before allocating or parsing the header/cursor.
        signing.verify(self.root.public_key, &message(bytes[0 .. bytes.len - 64]), bytes[bytes.len - 64 ..][0..64].*) catch return error.InvalidSchemaValidationJob;
        var parsed = std.json.parseFromSlice(Header, self.alloc, bytes[16..][0..json_len], .{ .ignore_unknown_fields = false, .max_value_len = 128 }) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidSchemaValidationJob,
        };
        defer parsed.deinit();
        const canonical = try std.json.Stringify.valueAlloc(self.alloc, parsed.value, .{});
        defer self.alloc.free(canonical);
        if (!std.mem.eql(u8, canonical, bytes[16..][0..json_len]) or parsed.value.signing_root != self.root.root_incarnation or parsed.value.physical_root == 0)
            return error.InvalidSchemaValidationJob;
        const after = try self.alloc.dupe(u8, bytes[16 + json_len ..][0..cursor_len]);
        errdefer self.alloc.free(after);
        const result: job.RowAudit.Progress = .{ .alloc = self.alloc, .summary = parsed.value.summary, .after = after, .complete = parsed.value.complete };
        result.validate() catch return error.InvalidSchemaValidationJob;
        if (parsed.value.physical_root != self.physical_root) return error.SchemaValidationRuntimeRecoveryRequired;
        try self.requireProgress(&result);
        return result;
    }

    pub fn load(self: *@This()) !?job.RowAudit.Progress {
        self.checkpoint_digest = null;
        const bytes = (try checkpoints.read(self, max_checkpoint, errors)) orelse return null;
        defer self.alloc.free(bytes);
        const result = self.decode(bytes) catch |err| switch (err) {
            // Valid progress from a different root, job/header or storage cut
            // is stale cache, not permission to skip any current rows.
            error.SchemaValidationRuntimeRecoveryRequired, error.SchemaValidationJobChanged => return null,
            else => return err,
        };
        self.checkpoint_digest = job.hash(bytes);
        return result;
    }
    pub fn save(self: *@This(), audit: *const job.RowAudit) !void {
        _ = try self.persist(audit);
    }
    pub fn persist(self: *@This(), audit: *const job.RowAudit) !job.RowAudit.Summary {
        var progress = try audit.checkpoint(self.alloc);
        defer progress.deinit();
        const bytes = try self.encode(&progress);
        defer self.alloc.free(bytes);
        try checkpoints.write(self, bytes, errors);
        return progress.summary;
    }
    pub fn evidence(self: *const @This(), audit: *const job.RowAudit) !Evidence {
        try checkpoints.requireRoot(self, errors);
        var progress = try audit.checkpoint(self.alloc);
        defer progress.deinit();
        try self.requireProgress(&progress);
        if (!progress.complete) return error.SchemaValidationNotReady;
        const expected = self.checkpoint_digest orelse return error.SchemaValidationNotReady;
        const bytes = try self.encode(&progress);
        defer self.alloc.free(bytes);
        if (!std.mem.eql(u8, &expected, &job.hash(bytes))) return error.SchemaValidationRuntimeRecoveryRequired;
        return .{ .signing_root = self.root.root_incarnation, .physical_root = self.physical_root, .rows = progress.summary, .checkpoint_digest = expected };
    }
};

/// `audit` is freshly prepared on the retained live DB; its header and actual
/// storage cut are independently captured, never supplied as remote claims.
pub fn open(alloc: A, io: std.Io, root_dir: []const u8, signing_root: u128, physical_root: u128, audit: *const job.RowAudit, context: Context) !Guard {
    try context.ensureActive();
    if (physical_root == 0) return error.SchemaValidationRuntimeRecoveryRequired;
    const header = try audit.header.digest();
    const opened = try checkpoints.open(alloc, io, root_dir, signing_root, @import("online_source_contract.zig").namespaceBytes(audit.header.binding.fence.namespace), ".schema-validation-audits", context, errors);
    return .{ .alloc = opened.alloc, .io = opened.io, .root_dir = opened.root_dir, .root = opened.root, .path = opened.path, .directory = opened.directory, .lock = opened.lock, .context = opened.context, .physical_root = physical_root, .expected_header = header, .expected_cut = audit.snapshot_cut };
}
