// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! One root-local producer slot per namespace. This records obligations, not
//! metadata authority. Callers retain the signing-root lifecycle lease until
//! close, and MUST close the file guard before any RPC. Missing metadata
//! receipts never authorize replay, replacement, or image retirement.
const std = @import("std");
const local = @import("antfly_local_sources");
const checkpoints = local.storage_db_root_audit_checkpoint_io;
const signing = local.storage_db_root_signing_identity;
const contract = local.storage_db_relational_schema_owner_contract;
const wire = local.storage_db_native_raft_snapshot_contract;
const read = @import("schema_validation_read.zig");
const parent = @import("schema_validation_parent.zig");
const receipts = @import("schema_validation_receipt.zig");
const roots = @import("schema_validation_root_epoch.zig");
const retention = @import("../raft/storage/snapshot_retention_source.zig");
const http = @import("../raft/transport/http_common.zig");
const A = std.mem.Allocator;
const Digest = [32]u8;
const magic = "ASPO\x01\x00\x00\x00";
const max_bytes = 128 * 1024;
const directory_name = ".schema-validation-source-outbox";
const pending_directory = "pending";
const errors: checkpoints.Errors = .{ .invalid = error.InvalidGenerationPublication, .scope_changed = error.SchemaValidationRuntimeRecoveryRequired };

pub const Intent = struct {
    lookup: read.Lookup,
    ordinal: u16,
    revision: u64,
    root_epoch: u64,
    owner_digest: Digest,
    binding: contract.Binding,
    scope: @import("schema_validation_attestation.zig").RaftScope,
    image: retention.Request,
    snapshot: @import("../raft/storage/snapshot_metadata.zig").Metadata,

    pub fn eql(self: @This(), other: @This()) bool {
        // The validated digest in image owns metadata equality, not the
        // temporary allocation addresses of its membership slices.
        var left = self;
        var right = other;
        left.snapshot = .{};
        right.snapshot = .{};
        return std.meta.eql(left, right);
    }

    pub fn validate(self: @This()) !void {
        try (read.OwnerReadRequest{ .lookup = self.lookup, .ordinal = self.ordinal }).validate();
        try self.binding.validate();
        try self.scope.requireBinding(self.binding);
        try self.image.validate();
        const metadata_digest = try @import("../raft/storage/snapshot_metadata.zig").digest(self.image.group_id, self.snapshot);
        if (self.snapshot.index != self.image.index or self.snapshot.term != self.image.term or
            !std.mem.eql(u8, &(self.image.metadata_digest orelse return error.InvalidSnapshotRetention), &metadata_digest)) return error.InvalidSnapshotRetention;
        if (self.revision == 0 or self.revision == std.math.maxInt(u64) or self.root_epoch == std.math.maxInt(u64) or
            std.mem.allEqual(u8, &self.owner_digest, 0) or self.lookup.table != self.binding.fence.namespace.table_id or
            !std.mem.eql(u8, &self.lookup.id, &self.binding.id) or !std.mem.eql(u8, &self.lookup.plan_digest, &self.binding.plan_digest) or
            !std.mem.eql(u8, &self.lookup.metadata_incarnation, &self.scope.metadata_incarnation) or
            self.image.group_id != self.scope.group_id or self.image.index > self.scope.reported_applied_index or
            self.image.term > self.scope.reported_term) return error.SchemaValidationJobChanged;
    }
    fn namespace(self: @This()) [24]u8 {
        return local.storage_db_online_source_contract.namespaceBytes(self.binding.fence.namespace);
    }
};

pub const Image = struct {
    payload_len: u64,
    payload_digest: Digest,
    witness: wire.MaterializationWitness,

    fn require(self: @This(), intent: Intent) !void {
        try self.witness.requireFor(intent.image.group_id, intent.image.index, intent.image.term, self.payload_len);
        if (std.mem.allEqual(u8, &self.payload_digest, 0) or
            !std.mem.eql(u8, &self.witness.stream.identity.namespace, &intent.namespace()) or
            self.witness.stream.identity.origin.?.physical_root_incarnation != intent.scope.physical_root_incarnation or
            self.witness.stream.identity.schema_header == null or
            !std.meta.eql(self.witness.stream.identity.schema_header.?.binding, intent.binding)) return error.SchemaValidationRuntimeRecoveryRequired;
    }
};
pub const Phase = enum { reserved, captured, prepared, observing, authorized, retiring, retired };
pub const Admission = struct { request: parent.Request, command: []const u8, grant: []const u8 };
pub const Record = struct {
    signing_root: u128,
    serial: u64,
    intent: Intent,
    phase: Phase = .reserved,
    image: ?Image = null,
    admission: ?Admission = null,

    fn validate(self: @This(), alloc: A) !void {
        try self.intent.validate();
        if (self.serial == 0 or self.signing_root == 0 or self.signing_root != self.intent.scope.identity.root_incarnation)
            return error.SchemaValidationRuntimeRecoveryRequired;
        if (self.image) |image| try image.require(self.intent);
        if (self.admission) |admission| {
            if (admission.grant.len == 0 or admission.grant.len > 256 or admission.command.len == 0 or admission.command.len > max_bytes) return error.InvalidGenerationPublication;
            try admission.request.validate();
            var command = try @import("schema_validation_ledger.zig").parseCommand(alloc, admission.command);
            defer command.deinit();
            if (command.value != .authorize_parent or !std.meta.eql(command.value.authorize_parent, admission.request)) return error.SchemaValidationJobChanged;
            const request = admission.request;
            const image = self.image orelse return error.InvalidGenerationPublication;
            const intent = self.intent;
            const signed = request.signed;
            if (!std.meta.eql(request.lookup, intent.lookup) or request.ordinal != intent.ordinal or
                request.revision != intent.revision or request.root_epoch != intent.root_epoch or
                !std.mem.eql(u8, &request.owner_digest, &intent.owner_digest) or
                !std.meta.eql(signed.value.binding, intent.binding) or !std.meta.eql(signed.proof.identity, intent.scope.identity) or
                signed.proof.reporter_incarnation != intent.scope.reporter_incarnation or signed.proof.authority != .raft or
                signed.proof.authority.raft != intent.scope.replica_id or signed.proof.root_generation != intent.scope.root_generation or
                signed.value.completed.term != intent.scope.reported_term or signed.value.completed.applied_index < intent.scope.reported_applied_index or
                !std.meta.eql(signed.value.origin, image.witness.stream.identity.origin.?) or
                !std.mem.eql(u8, &signed.value.stream_digest, &image.witness.stream.footer_digest) or
                signed.value.through_index != intent.image.index or !std.meta.eql(request.source_header, image.witness.stream.identity.schema_header))
                return error.SchemaValidationJobChanged;
        }
        switch (self.phase) {
            .reserved => if (self.image != null or self.admission != null) return error.InvalidGenerationPublication,
            .captured => if (self.image == null or self.admission != null) return error.InvalidGenerationPublication,
            .prepared, .observing, .authorized => if (self.image == null or self.admission == null) return error.InvalidGenerationPublication,
            .retiring, .retired => {},
        }
    }
    pub fn receipt(self: @This()) !receipts.Request {
        const admission = self.admission orelse return error.SchemaValidationNotReady;
        return .{ .lookup = self.intent.lookup, .expected = .{ .parent_authorization = .{ .ordinal = self.intent.ordinal, .grant = try admission.request.id() } } };
    }
};

pub const Submission = struct {
    alloc: A,
    serial: u64,
    command: []u8,
    grant: []u8,
    expected: receipts.Request,

    pub fn deinit(self: *@This()) void {
        self.alloc.free(self.command);
        self.alloc.free(self.grant);
        self.* = undefined;
    }
};

/// Owned release ticket, including the original membership slices. Its serial
/// prevents a stale callback from completing a newer obligation after the
/// checkpoint guard has been closed. No parsed-store allocation may escape.
pub const Retirement = struct {
    alloc: A,
    serial: u64,
    intent: Intent,
    image: ?Image,
    pub fn deinit(self: *@This()) void {
        self.intent.snapshot.deinit(self.alloc);
        self.* = undefined;
    }
};

fn message(bytes: []const u8) Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/schema-validation-source-outbox/v1");
    hash.update(bytes);
    var result: Digest = undefined;
    hash.final(&result);
    return result;
}

/// The pending directory is a locator index, never authority. Its empty,
/// private files name namespaces; every action reopens the canonical signed
/// checkpoint under its namespace lock. Historical terminal slots are absent
/// from this index, so discovery cost depends on pending work, not job churn.
pub const RecoveryCursor = struct {
    io: std.Io,
    directory: std.Io.Dir,
    iterator: std.Io.Dir.Iterator,

    pub fn init(alloc: A, io: std.Io, root_dir: []const u8) !?@This() {
        const path = try std.fs.path.join(alloc, &.{ root_dir, directory_name });
        defer alloc.free(path);
        const parent_dir = std.Io.Dir.cwd().openDir(io, path, .{ .follow_symlinks = false }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer parent_dir.close(io);
        const directory = parent_dir.openDir(io, pending_directory, .{ .follow_symlinks = false, .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        return .{ .io = io, .directory = directory, .iterator = directory.iterate() };
    }
    pub fn deinit(self: *@This()) void {
        self.directory.close(self.io);
        self.* = undefined;
    }
    /// Visits exactly one directory entry per call. Callers bound work by
    /// entry count and time and retain this cursor across executor slices.
    pub fn next(self: *@This(), context: local.api_operation.RequestContext) !?[24]u8 {
        try context.ensureActive();
        const entry = (try self.iterator.next(self.io)) orelse return null;
        if (entry.kind != .file or entry.name.len != 48) return error.InvalidGenerationPublication;
        var namespace: [24]u8 = undefined;
        _ = std.fmt.hexToBytes(&namespace, entry.name) catch return error.InvalidGenerationPublication;
        // no-follow plus stat rejects symlinks, directories and forged payloads.
        const file = self.directory.openFile(self.io, entry.name, .{ .follow_symlinks = false }) catch |err| switch (err) {
            // A concurrent terminal checkpoint may have removed the hint.
            error.FileNotFound => return error.SchemaValidationDiscoveryChanged,
            else => return err,
        };
        defer file.close(self.io);
        const stat = try file.stat(self.io);
        if (stat.kind != .file or stat.size != 0) return error.InvalidGenerationPublication;
        if (comptime @import("builtin").os.tag != .windows) if ((stat.permissions.toMode() & 0o077) != 0) return error.InvalidGenerationPublication;
        return namespace;
    }
};

fn syncDirectory(io: std.Io, dir: std.Io.Dir) !void {
    const file: std.Io.File = .{ .handle = dir.handle, .flags = .{ .nonblocking = false } };
    try file.sync(io);
}

pub const Store = struct {
    guard: checkpoints.Opened,
    namespace: [24]u8,
    parsed: ?std.json.Parsed(Record) = null,
    poisoned: bool = false,
    pending_synced: bool = false,

    pub fn deinit(self: *@This()) void {
        if (self.parsed) |*parsed| parsed.deinit();
        checkpoints.close(&self.guard);
        self.* = undefined;
    }
    pub fn record(self: *const @This()) ?*const Record {
        return if (self.parsed) |*parsed| &parsed.value else null;
    }
    fn ensureActive(self: *@This()) !void {
        if (self.poisoned) return error.SchemaValidationRuntimeRecoveryRequired;
        try checkpoints.requireRoot(&self.guard, errors);
    }
    fn decode(self: *@This(), bytes: []const u8) !std.json.Parsed(Record) {
        if (bytes.len < 76 or bytes.len > max_bytes or !std.mem.eql(u8, bytes[0..8], magic) or
            @as(u64, std.mem.readInt(u32, bytes[8..12], .little)) + 76 != bytes.len) return error.InvalidGenerationPublication;
        signing.verify(self.guard.root.public_key, &message(bytes[0 .. bytes.len - 64]), bytes[bytes.len - 64 ..][0..64].*) catch return error.InvalidGenerationPublication;
        var parsed = std.json.parseFromSlice(Record, self.guard.alloc, bytes[12 .. bytes.len - 64], .{ .allocate = .alloc_always, .ignore_unknown_fields = false, .max_value_len = max_bytes }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidGenerationPublication;
        errdefer parsed.deinit();
        try parsed.value.validate(self.guard.alloc);
        if (parsed.value.signing_root != self.guard.root.root_incarnation or !std.mem.eql(u8, &parsed.value.intent.namespace(), &self.namespace))
            return error.SchemaValidationRuntimeRecoveryRequired;
        return parsed;
    }
    fn indexPending(self: *@This()) !void {
        if (self.pending_synced) return;
        if (self.guard.directory.createDir(self.guard.io, pending_directory, .default_dir)) |_| {} else |err| {
            switch (err) {
                error.PathAlreadyExists => {},
                else => return err,
            }
        }
        const dir = try self.guard.directory.openDir(self.guard.io, pending_directory, .{ .follow_symlinks = false });
        defer dir.close(self.guard.io);
        const name = std.fmt.bytesToHex(self.namespace, .lower);
        const permissions: std.Io.File.Permissions = if (comptime @import("builtin").os.tag == .windows) .default_file else @fromBackingInt(0o600);
        if (dir.createFile(self.guard.io, &name, .{ .exclusive = true, .permissions = permissions })) |file| {
            defer file.close(self.guard.io);
            try file.sync(self.guard.io);
        } else |err| switch (err) {
            error.PathAlreadyExists => {
                const file = try dir.openFile(self.guard.io, &name, .{ .follow_symlinks = false });
                defer file.close(self.guard.io);
                const stat = try file.stat(self.guard.io);
                if (stat.kind != .file or stat.size != 0) return error.InvalidGenerationPublication;
                if (comptime @import("builtin").os.tag != .windows) if ((stat.permissions.toMode() & 0o077) != 0) return error.InvalidGenerationPublication;
                try file.sync(self.guard.io);
            },
            else => return err,
        }
        // Complete mkdir/file publication before committing an active record.
        // A crash may leave an extra hint, never an undiscoverable obligation.
        try syncDirectory(self.guard.io, dir);
        try syncDirectory(self.guard.io, self.guard.directory);
        self.pending_synced = true;
    }
    fn unindexPending(self: *@This()) !void {
        const dir = self.guard.directory.openDir(self.guard.io, pending_directory, .{ .follow_symlinks = false }) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer dir.close(self.guard.io);
        const name = std.fmt.bytesToHex(self.namespace, .lower);
        dir.deleteFile(self.guard.io, &name) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
        try syncDirectory(self.guard.io, dir);
        self.pending_synced = false;
    }
    fn save(self: *@This(), value: Record) !void {
        try self.ensureActive();
        try value.validate(self.guard.alloc);
        const json = try std.json.Stringify.valueAlloc(self.guard.alloc, value, .{});
        defer self.guard.alloc.free(json);
        if (json.len > max_bytes - 76) return error.InvalidGenerationPublication;
        const bytes = try self.guard.alloc.alloc(u8, json.len + 76);
        defer self.guard.alloc.free(bytes);
        @memcpy(bytes[0..8], magic);
        std.mem.writeInt(u32, bytes[8..12], @intCast(json.len), .little);
        @memcpy(bytes[12..][0..json.len], json);
        @memcpy(bytes[bytes.len - 64 ..], &try self.guard.root.sign(&message(bytes[0 .. bytes.len - 64])));
        // Own the successor before persistence: post-rename OOM cannot be
        // mistaken for an unadmitted operation. Any uncertain IO poisons this
        // instance; only reopening the actual signed record can recover it.
        var successor = try self.decode(bytes);
        errdefer successor.deinit();
        if (value.phase != .retired) try self.indexPending();
        checkpoints.write(&self.guard, bytes, errors) catch |err| {
            self.poisoned = true;
            return err;
        };
        if (value.phase == .retired) self.unindexPending() catch |err| {
            self.poisoned = true;
            return err;
        };
        if (self.parsed) |*parsed| parsed.deinit();
        self.parsed = successor;
    }
    fn next(self: *@This()) !Record {
        try self.ensureActive();
        var result = (self.record() orelse return error.SchemaValidationJobRequired).*;
        result.serial = std.math.add(u64, result.serial, 1) catch return error.InvalidGenerationPublication;
        return result;
    }
    /// Persist BEFORE retaining any image or invoking the source certifier.
    pub fn reserve(self: *@This(), intent: Intent) !void {
        try self.ensureActive();
        try intent.validate();
        if (!std.mem.eql(u8, &intent.namespace(), &self.namespace)) return error.SchemaValidationJobChanged;
        if (self.record()) |previous| {
            if (previous.intent.eql(intent)) return;
            if (previous.phase != .retired or std.mem.eql(u8, &previous.intent.image.reference, &intent.image.reference)) return error.SchemaValidationRuntimeRecoveryRequired;
        }
        const serial = if (self.record()) |previous| std.math.add(u64, previous.serial, 1) catch return error.InvalidGenerationPublication else 1;
        try self.save(.{ .signing_root = self.guard.root.root_incarnation, .serial = serial, .intent = intent });
    }
    /// No command/grant exists in CAPTURED, so live source scope can be
    /// refreshed after election/restart without recapturing or relabeling bytes.
    /// Once prepared, even a grant that has not been sent remains immutable.
    pub fn refreshCaptured(self: *@This(), intent: Intent) !void {
        try self.ensureActive();
        try intent.validate();
        const previous = self.record() orelse return error.SchemaValidationJobRequired;
        if (previous.phase != .captured) return error.SchemaValidationRuntimeRecoveryRequired;
        if (previous.intent.eql(intent)) return;
        const old = previous.intent;
        if (!std.meta.eql(old.lookup, intent.lookup) or old.ordinal != intent.ordinal or old.root_epoch != intent.root_epoch or
            !std.meta.eql(old.binding, intent.binding) or !std.meta.eql(old.image, intent.image) or
            !std.meta.eql(old.scope.identity, intent.scope.identity) or old.scope.replica_id != intent.scope.replica_id or
            old.scope.physical_root_incarnation != intent.scope.physical_root_incarnation or intent.revision < old.revision or
            intent.scope.reported_term < old.scope.reported_term or intent.scope.reported_applied_index < old.scope.reported_applied_index)
            return error.SchemaValidationRuntimeRecoveryRequired;
        try previous.image.?.require(intent);
        var value = try self.next();
        value.intent = intent;
        try self.save(value);
    }

    pub fn capture(self: *@This(), retained: retention.RetainedSnapshot) !void {
        var value = try self.next();
        if ((value.phase != .reserved and value.phase != .captured) or retained.identity.index != value.intent.image.index or retained.identity.term != value.intent.image.term or
            !std.mem.eql(u8, &retained.reference, &value.intent.image.reference) or retained.application_manifest.len > 64 * 1024) return error.SchemaValidationJobChanged;
        var parsed = try std.json.parseFromSlice(wire.MaterializationWitness, self.guard.alloc, retained.application_manifest, .{ .allocate = .alloc_always, .ignore_unknown_fields = false, .max_value_len = 64 * 1024 });
        defer parsed.deinit();
        const image: Image = .{ .payload_len = retained.payload_len, .payload_digest = retained.digest, .witness = parsed.value };
        try image.require(value.intent);
        if (value.phase == .captured) {
            if (!std.meta.eql(image, value.image.?)) return error.SchemaValidationRuntimeRecoveryRequired;
            return;
        }
        value.image = image;
        value.phase = .captured;
        try self.save(value);
    }
    pub fn prepare(self: *@This(), request: parent.Request, grant: []const u8) !void {
        const command = try std.json.Stringify.valueAlloc(self.guard.alloc, @import("schema_validation_ledger.zig").Command{ .authorize_parent = request }, .{});
        defer self.guard.alloc.free(command);
        return self.prepareWithCommand(request, command, grant);
    }
    pub fn prepareWithCommand(self: *@This(), request: parent.Request, command: []const u8, grant: []const u8) !void {
        var value = try self.next();
        if (value.phase != .captured) return error.SchemaValidationNotReady;
        value.admission = .{ .request = request, .command = command, .grant = grant };
        value.phase = .prepared;
        try self.save(value);
    }
    /// The caller owns the returned bytes, closes this guard, then performs
    /// ONE transport attempt. Reopening observing permits receipt reads only.
    pub fn beginSubmission(self: *@This()) !Submission {
        var value = try self.next();
        if (value.phase != .prepared) return error.SchemaValidationRuntimeRecoveryRequired;
        const command = try self.guard.alloc.dupe(u8, value.admission.?.command);
        errdefer self.guard.alloc.free(command);
        const grant = try self.guard.alloc.dupe(u8, value.admission.?.grant);
        errdefer self.guard.alloc.free(grant);
        const expected = try value.receipt();
        value.phase = .observing;
        try self.save(value);
        return .{ .alloc = self.guard.alloc, .serial = value.serial, .command = command, .grant = grant, .expected = expected };
    }
    pub fn observeAcceptance(self: *@This(), observed: receipts.Observation) !bool {
        var value = try self.next();
        if (value.phase != .observing and value.phase != .authorized) return error.SchemaValidationNotReady;
        if (!try observed.isAcknowledged(try value.receipt())) return false;
        if (value.phase == .authorized) return true;
        value.phase = .authorized;
        try self.save(value);
        return true;
    }
    // Only the live one-attempt dispatcher may return an attempt to prepared,
    // and only with positive transport proof that it did not send. A fresh
    // tracker or a negative metadata read cannot call this recovery transition.
    fn returnNotSent(self: *@This(), submission: Submission) !void {
        try self.ensureActive();
        const current = self.record() orelse return error.SchemaValidationJobRequired;
        if (current.phase != .observing or current.serial != submission.serial or
            !std.meta.eql(try current.receipt(), submission.expected)) return error.SchemaValidationRuntimeRecoveryRequired;
        var value = try self.next();
        value.phase = .prepared;
        try self.save(value);
    }
    /// An unsubmitted intent can be abandoned; an accepted parent must have
    /// its exact linked root adoption acknowledged by authenticated metadata.
    pub fn beginRetirement(self: *@This(), adoption: ?roots.Request, observed: ?receipts.Observation) !void {
        var value = try self.next();
        switch (value.phase) {
            .reserved, .captured, .prepared => if (adoption != null or observed != null) return error.InvalidGenerationPublication,
            .authorized => {
                const request = adoption orelse return error.SchemaValidationNotReady;
                try request.validateShape();
                const expected_parent = (try value.receipt()).expected.parent_authorization;
                if (!std.meta.eql(request.lookup, value.intent.lookup) or request.ordinal != value.intent.ordinal or
                    request.expected_epoch != value.intent.root_epoch or request.lineage != .retained_parent or
                    !std.mem.eql(u8, &request.lineage.retained_parent, &expected_parent.grant)) return error.SchemaValidationJobChanged;
                if (!try (observed orelse return error.SchemaValidationNotReady).isAcknowledged(.{ .lookup = request.lookup, .expected = .{ .root_adoption = .{ .ordinal = request.ordinal, .epoch = request.expected_epoch + 1, .request_digest = try request.digest() } } })) return error.SchemaValidationNotReady;
            },
            .retiring, .retired => return,
            .observing => return error.SchemaValidationRuntimeRecoveryRequired,
        }
        value.phase = .retiring;
        try self.save(value);
    }
    /// A fresh authenticated receipt read may discharge even an uncertain
    /// parent attempt once the exact job is terminal. Terminal means ALL owners
    /// have durably released/canceled; the metadata writer also removes its
    /// lifecycle lock atomically, preventing any later new parent admission.
    /// A cancel decision or partial cohort is NOT sufficient.
    pub fn beginTerminalRetirement(self: *@This(), observed: receipts.Observation) !void {
        var value = try self.next();
        if (value.phase == .retiring or value.phase == .retired) return;
        if (value.phase != .observing and value.phase != .authorized) return error.SchemaValidationNotReady;
        _ = try observed.isAcknowledged(try value.receipt());
        const head = observed.head orelse return error.SchemaValidationNotReady;
        if (head.revision < value.intent.revision or value.intent.ordinal >= head.owner_count) return error.SchemaValidationJobChanged;
        if (head.phase != .succeeded and head.phase != .canceled) return error.SchemaValidationNotReady;
        value.phase = .retiring;
        try self.save(value);
    }
    pub fn retirement(self: *@This()) !?Retirement {
        try self.ensureActive();
        const value = self.record() orelse return error.SchemaValidationJobRequired;
        if (value.phase == .retired) return null;
        if (value.phase != .retiring) return error.SchemaValidationNotReady;
        var intent = value.intent;
        intent.snapshot = try value.intent.snapshot.clone(self.guard.alloc);
        return .{ .alloc = self.guard.alloc, .serial = value.serial, .intent = intent, .image = value.image };
    }
    /// Called only AFTER the detached retention owner confirms unlink AND
    /// directory sync. On any uncertain release/checkpoint result the durable
    /// retiring record survives, and an exact callback can retry idempotently.
    pub fn completeRetirement(self: *@This(), ticket: Retirement) !void {
        try self.ensureActive();
        try ticket.intent.validate();
        const current = self.record() orelse return error.SchemaValidationJobRequired;
        if (current.phase != .retiring or current.serial != ticket.serial or
            !current.intent.eql(ticket.intent) or !std.meta.eql(current.image, ticket.image)) return error.SchemaValidationRuntimeRecoveryRequired;
        var value = try self.next();
        value.phase = .retired;
        try self.save(value);
    }
};

/// The caller keeps the signing-root lifecycle lease throughout each operation.
/// Checkpoint guards are opened only for local transitions, never across RPCs.
pub const Location = struct {
    alloc: A,
    io: std.Io,
    root_dir: []const u8,
    root_incarnation: u128,
    namespace: [24]u8,
    context: local.api_operation.RequestContext = .{},

    fn openStore(self: @This()) !Store {
        return open(self.alloc, self.io, self.root_dir, self.root_incarnation, self.namespace, self.context);
    }
    fn finishStore(self: @This()) !Store {
        // The caller still holds the root lifecycle lease. Complete a known
        // local durability obligation after cancellation, with an independent
        // bounded budget; this grants no new metadata mutation or RPC.
        const deadline = @import("antfly_platform").time.monotonicNs() +| std.time.ns_per_s;
        return open(self.alloc, self.io, self.root_dir, self.root_incarnation, self.namespace, .{ .deadline_ns = deadline });
    }
    fn timeout(self: @This(), maximum_ms: u32) !u32 {
        try self.context.ensureActive();
        if (maximum_ms == 0) return error.Timeout;
        return @min(maximum_ms, try self.context.remainingTimeoutMs() orelse maximum_ms);
    }
};

/// Grant preparation is pure local cryptography. Save the exact source command
/// and body-bound grant before permitting submission; never refresh a prepared
/// grant merely because the caller retries or its original timeout expired.
pub fn prepareParentAdmission(location: Location, request: parent.Request, client: anytype) !void {
    var store = try location.openStore();
    defer store.deinit();
    const record = store.record() orelse return error.SchemaValidationJobRequired;
    if (record.phase != .captured) return error.SchemaValidationNotReady;
    const command = try std.json.Stringify.valueAlloc(location.alloc, @import("schema_validation_ledger.zig").Command{ .authorize_parent = request }, .{});
    defer location.alloc.free(command);
    const grant = try client.prepareSchemaValidationGrant(command, false);
    defer client.alloc.free(grant);
    try store.prepareWithCommand(request, command, grant);
}

pub const AdmissionProgress = enum { pending, authorized };

/// A retention callback may acquire its own owner/replica serialization. Never
/// hold the checkpoint guard across that callback, even for local-only IO.
pub fn finishSourceRetirement(location: Location, ctx: *anyopaque, release: *const fn (*anyopaque, Intent, ?Image) anyerror!void) !void {
    var ticket = blk: {
        var store = try location.openStore();
        defer store.deinit();
        break :blk try store.retirement() orelse return;
    };
    defer ticket.deinit();
    try release(ctx, ticket.intent, ticket.image);
    var store = try location.finishStore();
    defer store.deinit();
    try store.completeRetirement(ticket);
}

/// Shared retention adapter; it cannot skip the signed outbox's retirement
/// ticket or complete its receipt before the actual unlink/sync has succeeded.
/// File guards are released before entering the source callback, including when
/// the old leader's group and provider have already disappeared.
pub fn finishSourceRetirementWithRetention(location: Location, source: retention.Source) !void {
    const Release = struct {
        location: Location,
        source: retention.Source,
        fn run(raw: *anyopaque, intent: Intent, image: ?Image) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const expected: ?retention.CapturedPayload = if (image) |value| .{ .payload_len = value.payload_len, .digest = value.payload_digest } else null;
            try self.source.releaseRetained(self.location.alloc, intent.image, expected, self.location.context);
        }
    };
    var release: Release = .{ .location = location, .source = source };
    return finishSourceRetirement(location, &release, Release.run);
}

/// Transport adapter to the existing authenticated/read-safe metadata path.
/// Returned parsed data is owned by the caller; no outbox guard spans the call.
pub const ReceiptReader = struct {
    ptr: *anyopaque,
    read: *const fn (*anyopaque, A, local.api_operation.RequestContext, receipts.Request) anyerror!std.json.Parsed(receipts.Observation),
};

/// Recover one pending namespace. Unknown sends only read their exact receipt;
/// this path never submits a command, renews a grant or recaptures CURRENT.
/// Terminal metadata is required before an uncertain/accepted parent retires.
/// Unsubmitted work belongs to the live producer and is left intact.
pub fn recoverRetirement(location: Location, reader: ReceiptReader, source: retention.Source) !void {
    const pending: ?struct { serial: u64, expected: receipts.Request } = blk: {
        var store = try location.openStore();
        defer store.deinit();
        const record = store.record() orelse return error.SchemaValidationRuntimeRecoveryRequired;
        switch (record.phase) {
            .reserved, .captured, .prepared, .retired => return,
            .retiring => break :blk null,
            .observing, .authorized => break :blk .{ .serial = record.serial, .expected = try record.receipt() },
        }
    };
    if (pending) |attempt| {
        var observed = try reader.read(reader.ptr, location.alloc, location.context, attempt.expected);
        defer observed.deinit();
        _ = try observed.value.isAcknowledged(attempt.expected);
        const head = observed.value.head orelse return;
        if (head.phase != .succeeded and head.phase != .canceled) return;
        var store = try location.finishStore();
        defer store.deinit();
        const record = store.record() orelse return error.SchemaValidationRuntimeRecoveryRequired;
        // Never apply an old terminal response to a replacement obligation.
        if (record.serial != attempt.serial or !std.meta.eql(try record.receipt(), attempt.expected)) return error.SchemaValidationRuntimeRecoveryRequired;
        try store.beginTerminalRetirement(observed.value);
    }
    try finishSourceRetirementWithRetention(location, source);
}

/// Check the exact parent's receipt through the existing authenticated/read-safe
/// client. Negative acceptance is not proof of non-send; only the complete
/// terminal cohort permits retirement without a linked adoption receipt.
pub fn observeTerminalRetirement(location: Location, client: anytype, base_uri: []const u8, maximum_ms: u32, cancellation: ?*const http.RequestCancellation) !bool {
    const expected = blk: {
        var store = try location.openStore();
        defer store.deinit();
        const record = store.record() orelse return error.SchemaValidationJobRequired;
        if (record.phase == .retiring or record.phase == .retired) return true;
        if (record.phase != .observing and record.phase != .authorized) return error.SchemaValidationNotReady;
        break :blk try record.receipt();
    };
    var observed = try client.readSchemaValidationReceipt(base_uri, expected, try location.timeout(maximum_ms), cancellation);
    defer observed.deinit();
    _ = try observed.value.isAcknowledged(expected);
    const head = observed.value.head orelse return false;
    if (head.phase != .succeeded and head.phase != .canceled) return false;
    var store = try location.finishStore();
    defer store.deinit();
    const current = store.record() orelse return error.SchemaValidationJobRequired;
    if (!std.meta.eql(try current.receipt(), expected)) return error.SchemaValidationRuntimeRecoveryRequired;
    try store.beginTerminalRetirement(observed.value);
    return true;
}

/// One bounded operation: submit a PREPARED record once, or read the exact
/// receipt of an OBSERVING record. Neither uncertainty nor absent receipts
/// permit replay, grant renewal, recapture or retirement. The transport and
/// owner lookup still perform their normal authentication/read-safety checks.
pub fn advanceParentAdmission(location: Location, client: anytype, base_uri: []const u8, maximum_ms: u32, cancellation: ?*const http.RequestCancellation) !AdmissionProgress {
    if (cancellation) |signal| if (signal.isCancelled()) return error.Cancelled;
    var submission: ?Submission = null;
    defer if (submission) |*value| value.deinit();
    var expected: receipts.Request = undefined;
    var serial: u64 = undefined;
    {
        var store = try location.openStore();
        defer store.deinit();
        const record = store.record() orelse return error.SchemaValidationJobRequired;
        switch (record.phase) {
            .prepared => {
                // Check the live budget BEFORE changing local state; once an
                // attempt begins, only its actual delivery provenance can rearm.
                _ = try location.timeout(maximum_ms);
                submission = try store.beginSubmission();
                expected = submission.?.expected;
                serial = submission.?.serial;
            },
            .observing => {
                expected = try record.receipt();
                serial = record.serial;
            },
            .authorized => return .authorized,
            else => return error.SchemaValidationNotReady,
        }
    }
    var observed = if (submission) |value| blk: {
        // Start UNKNOWN so no caller-created zero/default tracker can be used
        // as proof. The real client records local rejection or transport facts.
        var delivery: http.RequestDeliveryTracker = .{};
        delivery.markUnknown();
        const remaining = location.timeout(maximum_ms) catch |err| {
            // No client invocation occurred. This control-flow fact, unlike a
            // newly initialized tracker after restart, is positive non-send.
            var store = try location.finishStore();
            defer store.deinit();
            try store.returnNotSent(value);
            return err;
        };
        const response = client.submitSchemaValidationWithGrantControlled(base_uri, value.command, value.grant, remaining, false, cancellation, &delivery) catch |err| {
            if (delivery.load() == .not_sent) {
                var store = try location.finishStore();
                defer store.deinit();
                try store.returnNotSent(value);
            }
            return err;
        };
        defer client.alloc.free(response);
        break :blk try std.json.parseFromSlice(receipts.Observation, location.alloc, response, .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
    } else try client.readSchemaValidationReceipt(base_uri, expected, try location.timeout(maximum_ms), cancellation);
    defer observed.deinit();
    var store = try location.finishStore();
    defer store.deinit();
    const record = store.record() orelse return error.SchemaValidationJobRequired;
    if (record.serial != serial or !std.meta.eql(try record.receipt(), expected)) return error.SchemaValidationRuntimeRecoveryRequired;
    return if (try store.observeAcceptance(observed.value)) .authorized else .pending;
}

/// Connect the durable producer intent to the compiled owner's actual capture
/// and quorum certifier. The caller holds that owner's lifecycle lease and the
/// signing root for the whole call. Every checkpoint guard is closed before
/// owner/Raft IO: neither a quorum wait nor later RPC can hold the file lock.
/// Prepared/observing records must use their saved admission; they cannot enter
/// this path and re-sign a different command after an uncertain submission.
pub fn captureParent(comptime Session: type, session: *Session, alloc: A, io: std.Io, root_dir: []const u8, intent: Intent, root: signing.State, context: local.api_operation.RequestContext) !Session.SnapshotParent {
    try context.ensureActive();
    try intent.validate();
    if (root.root_incarnation != intent.scope.identity.root_incarnation or
        !std.mem.eql(u8, &root.public_key, &intent.scope.identity.public_key)) return error.SchemaValidationRuntimeRecoveryRequired;
    var captured: ?retention.CapturedPayload = null;
    {
        var store = try open(alloc, io, root_dir, root.root_incarnation, intent.namespace(), context);
        defer store.deinit();
        if (store.record()) |existing| if (existing.phase == .captured) try store.refreshCaptured(intent);
        try store.reserve(intent);
        const record = store.record().?;
        captured = switch (record.phase) {
            .reserved => null,
            .captured => .{ .payload_len = record.image.?.payload_len, .digest = record.image.?.payload_digest },
            else => return error.SchemaValidationRuntimeRecoveryRequired,
        };
    }
    const Checkpoint = struct {
        alloc: A,
        io: std.Io,
        root_dir: []const u8,
        root_incarnation: u128,
        intent: Intent,
        context: local.api_operation.RequestContext,
        fn record(raw: *anyopaque, retained: retention.RetainedSnapshot) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var store = try open(self.alloc, self.io, self.root_dir, self.root_incarnation, self.intent.namespace(), self.context);
            defer store.deinit();
            const value = store.record() orelse return error.SchemaValidationJobRequired;
            if (!value.intent.eql(self.intent)) return error.SchemaValidationJobChanged;
            try store.capture(retained);
        }
    };
    var checkpoint: Checkpoint = .{ .alloc = alloc, .io = io, .root_dir = root_dir, .root_incarnation = root.root_incarnation, .intent = intent, .context = context };
    return session.captureSnapshotParent(alloc, intent.image, intent.scope, root, captured, .{ .ptr = &checkpoint, .record = Checkpoint.record });
}

pub fn open(alloc: A, io: std.Io, root_dir: []const u8, root_incarnation: u128, namespace: [24]u8, context: local.api_operation.RequestContext) !Store {
    var result: Store = .{ .guard = try checkpoints.open(alloc, io, root_dir, root_incarnation, namespace, directory_name, context, errors), .namespace = namespace };
    errdefer result.deinit();
    if (try checkpoints.read(&result.guard, max_bytes, errors)) |bytes| {
        defer alloc.free(bytes);
        result.parsed = try result.decode(bytes);
        // A surviving rename is not proof of directory durability. Resync
        // the selected checkpoint and its parent before enabling side effects.
        try checkpoints.write(&result.guard, bytes, errors);
        // Repair only from the authenticated current slot under its lock.
        // A stale scanner can never delete a successor's pending obligation.
        if (result.record().?.phase == .retired) try result.unindexPending() else try result.indexPending();
    }
    return result;
}

const TestFixture = struct {
    root: signing.State,
    intent: Intent,
    image: Image,
    request: parent.Request,

    fn init(alloc: A, root_dir: []const u8) !@This() {
        const root = try signing.loadOrCreate(alloc, std.testing.io, root_dir);
        const binding: contract.Binding = .{
            .id = @splat(1),
            .plan_digest = @splat(2),
            .owner = @splat(3),
            .fence = .{ .transition_id = 4, .attempt = 1, .admission_epoch = 1, .owner_group_id = 72, .peer_group_id = 72, .namespace = .{ .table_id = 7, .shard_id = 172, .range_id = 272 }, .catalog_digest = @splat(5), .role = .schema_validation },
            .authority = .raft,
            .before_version = 1,
            .after_version = 2,
            .columns = 1,
            .before_schema = @splat(6),
            .after_schema = @splat(7),
            .before_public = @splat(8),
            .after_public = @splat(9),
            .after_catalog = @splat(10),
            .physical_catalog = @splat(11),
            .inputs = @splat(12),
            .projection_identity = @splat(13),
        };
        const attestation = @import("schema_validation_attestation.zig");
        const snapshot: @import("../raft/storage/snapshot_metadata.zig").Metadata = .{ .index = 5, .term = 1, .conf_state = .{ .voters = @constCast(&[_]u64{ 5, 8 }), .voters_outgoing = @constCast(&[_]u64{ 5, 9 }), .learners_next = @constCast(&[_]u64{9}), .auto_leave = true } };
        const intent: Intent = .{
            .lookup = .{ .group = 21, .table = 7, .id = binding.id, .plan_digest = binding.plan_digest, .metadata_incarnation = "0123456789abcdef0123456789abcdef".* },
            .ordinal = 0,
            .revision = 1,
            .root_epoch = 0,
            .owner_digest = @splat(14),
            .binding = binding,
            .scope = .{
                .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
                .binding_digest = try attestation.bindingDigest(binding),
                .identity = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 2, .store_id = 3, .root_incarnation = root.root_incarnation, .public_key = root.public_key },
                .reporter_incarnation = 4,
                .replica_id = 5,
                .group_id = 72,
                .root_generation = 1,
                .physical_root_incarnation = 17,
                .reported_term = 2,
                .reported_applied_index = 10,
            },
            .snapshot = snapshot,
            .image = .{ .group_id = 72, .index = 5, .term = 1, .reference = @splat(15), .metadata_digest = try @import("../raft/storage/snapshot_metadata.zig").digest(72, snapshot) },
        };
        const cut = local.storage_db_receipt_position.Position{ .raft = .{ .term = 1, .index = 5 } };
        const header: contract.Header = .{ .binding = binding, .phase = .fencing, .admitted = cut, .updated = cut, .cursor_digest = contract.hash(""), .chain = @splat(16), .last_effect = @splat(17) };
        const header_bytes = try header.encode(alloc);
        defer alloc.free(header_bytes);
        // Checkpoint protocol fixture, not a claim of actual NRSP2 bytes or
        // metadata admission. Native stream and authority gates cover those.
        const image: Image = .{ .payload_len = 128, .payload_digest = @splat(18), .witness = .{
            .native_offset = 0,
            .native_size = 128,
            .stream = .{
                .identity = .{ .group_id = 72, .namespace = intent.namespace(), .through_index = 5, .native_term = 1, .native_index = 5, .schema_header = header, .origin = .{ .physical_root_incarnation = 17, .namespace = binding.fence.namespace, .authority = .{ .kind = .raft, .namespace = intent.namespace() }, .applied = .{ .term = 1, .index = 5 }, .schema_header = contract.hash(header_bytes), .transaction_directory = null, .schema_outcomes = null } },
                .footer_digest = @splat(19),
            },
        } };
        const signed = try attestation.SignedSnapshotParent.sign(.{
            .binding = binding,
            .origin = image.witness.stream.identity.origin.?,
            .stream_digest = image.witness.stream.footer_digest,
            .through_index = 5,
            .completed = .{ .incarnation = 1, .term = 2, .applied_index = 11 },
        }, intent.scope.identity, 4, 5, 1, root);
        return .{ .root = root, .intent = intent, .image = image, .request = .{ .lookup = intent.lookup, .ordinal = 0, .revision = 1, .owner_digest = intent.owner_digest, .signed = signed, .source_header = header } };
    }
    fn capture(self: @This(), store: *Store) !void {
        const manifest = try std.json.Stringify.valueAlloc(store.guard.alloc, self.image.witness, .{});
        defer store.guard.alloc.free(manifest);
        try store.capture(.{ .reference = self.intent.image.reference, .identity = .{ .index = 5, .term = 1 }, .payload_len = self.image.payload_len, .digest = self.image.payload_digest, .application_manifest = manifest });
    }
};

test "schema validation source outbox cold recovery owns original snapshot membership and rejects relabeling" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root_dir = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(root_dir);
    const fixture = try TestFixture.init(a, root_dir);
    // Protocol fixtures only: these lists are not quorum certification. The
    // saved copy must outlive and ignore changes to the caller's configuration.
    var voters = [_]u64{ 5, 8 };
    var intent = fixture.intent;
    intent.snapshot.conf_state.voters = &voters;
    {
        var store = try open(a, std.testing.io, root_dir, fixture.root.root_incarnation, intent.namespace(), .{});
        defer store.deinit();
        try store.reserve(intent);
        voters[1] = 99;
        try std.testing.expectEqualSlices(u64, &.{ 5, 8 }, store.record().?.intent.snapshot.conf_state.voters);
    }
    var store = try open(a, std.testing.io, root_dir, fixture.root.root_incarnation, intent.namespace(), .{});
    defer store.deinit();
    try std.testing.expectEqualDeep(fixture.intent.snapshot, store.record().?.intent.snapshot);
    try store.reserve(fixture.intent); // Compare values, never slice addresses.
    try std.testing.expectError(error.InvalidSnapshotRetention, store.reserve(intent));
    intent.image.metadata_digest = try @import("../raft/storage/snapshot_metadata.zig").digest(intent.image.group_id, intent.snapshot);
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, store.reserve(intent));
    intent = fixture.intent;
    intent.snapshot.term += 1;
    try std.testing.expectError(error.InvalidSnapshotRetention, intent.validate());
    try std.testing.expectEqualDeep(fixture.intent.snapshot, store.record().?.intent.snapshot);
}

test "schema validation source outbox captured scope refresh preserves bytes and seals on preparation" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root_dir = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/source-refresh", .{tmp.sub_path});
    defer a.free(root_dir);
    const fixture = try TestFixture.init(a, root_dir);
    var next = fixture.intent;
    next.revision += 1;
    next.owner_digest = @splat(29);
    next.scope.reporter_incarnation += 1;
    next.scope.root_generation += 1;
    next.scope.reported_term += 1;
    next.scope.reported_applied_index += 1;
    var serial: u64 = undefined;
    {
        var store = try open(a, std.testing.io, root_dir, fixture.root.root_incarnation, fixture.intent.namespace(), .{});
        defer store.deinit();
        try store.reserve(fixture.intent);
        try fixture.capture(&store);
        serial = store.record().?.serial;
        try store.refreshCaptured(next);
        try std.testing.expectEqual(serial + 1, store.record().?.serial);
        try std.testing.expectEqualDeep(fixture.image, store.record().?.image.?);
        try std.testing.expectEqual(Phase.captured, store.record().?.phase);
    }
    var store = try open(a, std.testing.io, root_dir, fixture.root.root_incarnation, fixture.intent.namespace(), .{});
    defer store.deinit();
    try store.reserve(next);
    try std.testing.expectEqualDeep(next, store.record().?.intent);
    try std.testing.expectEqualDeep(fixture.image, store.record().?.image.?);
    var substituted = next;
    substituted.image.reference[0] ^= 1;
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, store.refreshCaptured(substituted));
    substituted = next;
    substituted.scope.physical_root_incarnation += 1;
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, store.refreshCaptured(substituted));
    substituted = next;
    substituted.scope.identity.root_incarnation += 1;
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, store.refreshCaptured(substituted));
    substituted = next;
    substituted.scope.reported_term -= 1;
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, store.refreshCaptured(substituted));
    var request = fixture.request;
    request.revision = next.revision;
    request.owner_digest = next.owner_digest;
    var signed_value = request.signed.value;
    signed_value.completed.term = next.scope.reported_term;
    signed_value.completed.applied_index = next.scope.reported_applied_index;
    request.signed = try @import("schema_validation_attestation.zig").SignedSnapshotParent.sign(signed_value, next.scope.identity, next.scope.reporter_incarnation, next.scope.replica_id, next.scope.root_generation, fixture.root);
    try store.prepare(request, "fixture-body-bound-grant");
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, store.refreshCaptured(next));
    var submission = try store.beginSubmission();
    defer submission.deinit();
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, store.refreshCaptured(next));
    try std.testing.expectEqualDeep(fixture.image, store.record().?.image.?);
    try std.testing.expectEqual(serial + 3, store.record().?.serial);
}

test "schema validation source outbox cold recovery preserves exact grant and never replays uncertainty" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root_dir = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/source-outbox", .{tmp.sub_path});
    defer a.free(root_dir);
    const fixture = try TestFixture.init(a, root_dir);
    var store = try open(a, std.testing.io, root_dir, fixture.root.root_incarnation, fixture.intent.namespace(), .{});
    try store.reserve(fixture.intent);
    try std.testing.expectEqual(Phase.reserved, store.record().?.phase);
    store.deinit();
    store = try open(a, std.testing.io, root_dir, fixture.root.root_incarnation, fixture.intent.namespace(), .{});
    defer store.deinit();
    var substitute = fixture.intent;
    substitute.image.reference[0] ^= 1;
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, store.reserve(substitute));
    try fixture.capture(&store);
    try std.testing.expectEqual(Phase.captured, store.record().?.phase);
    var wrong = fixture.request;
    wrong.owner_digest[0] ^= 1;
    try std.testing.expectError(error.SchemaValidationJobChanged, store.prepare(wrong, "exact-persisted-grant"));
    const exact_command = try std.json.Stringify.valueAlloc(a, @import("schema_validation_ledger.zig").Command{ .authorize_parent = fixture.request }, .{ .whitespace = .indent_2 });
    defer a.free(exact_command);
    try std.testing.expectError(error.SchemaValidationJobChanged, store.prepareWithCommand(wrong, exact_command, "exact-persisted-grant"));
    try store.prepareWithCommand(fixture.request, exact_command, "exact-persisted-grant");
    var submission = try store.beginSubmission();
    defer submission.deinit();
    const serial = submission.serial;
    store.deinit();
    store = try open(a, std.testing.io, root_dir, fixture.root.root_incarnation, fixture.intent.namespace(), .{});
    try std.testing.expectEqual(Phase.observing, store.record().?.phase);
    try std.testing.expectEqual(serial, store.record().?.serial);
    try std.testing.expectEqualStrings("exact-persisted-grant", submission.grant);
    try std.testing.expectEqualStrings(exact_command, submission.command);
    try std.testing.expectEqualStrings(exact_command, store.record().?.admission.?.command);
    try std.testing.expectEqualStrings(submission.grant, store.record().?.admission.?.grant);
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, store.beginSubmission());
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, store.beginRetirement(null, null));
    try std.testing.expect(!try store.observeAcceptance(.{ .lookup = fixture.intent.lookup }));
    const head: @import("schema_validation_publication.zig").Head = .{ .metadata_group_id = 21, .metadata_incarnation = fixture.intent.lookup.metadata_incarnation, .id = fixture.intent.lookup.id, .plan_digest = fixture.intent.lookup.plan_digest, .table_id = 7, .owner_count = 1 };
    var observed: receipts.Observation = .{ .lookup = fixture.intent.lookup, .head = head, .observed = submission.expected.expected };
    observed.observed.?.parent_authorization.grant[0] ^= 1;
    try std.testing.expect(!try store.observeAcceptance(observed));
    observed.observed = submission.expected.expected;
    try std.testing.expect(try store.observeAcceptance(observed));
    try std.testing.expectEqual(Phase.authorized, store.record().?.phase);
    try std.testing.expectError(error.SchemaValidationNotReady, store.beginRetirement(null, null));
    store.deinit();
    store = try open(a, std.testing.io, root_dir, fixture.root.root_incarnation, fixture.intent.namespace(), .{});
    try std.testing.expectEqual(Phase.authorized, store.record().?.phase);
    try std.testing.expect(try store.observeAcceptance(observed));
}

test "schema validation source outbox real client persists grants rearms proven nonsends and only reads uncertainty" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root_dir = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/source-dispatch", .{tmp.sub_path});
    defer a.free(root_dir);
    const fixture = try TestFixture.init(a, root_dir);
    const location: Location = .{ .alloc = a, .io = std.testing.io, .root_dir = root_dir, .root_incarnation = fixture.root.root_incarnation, .namespace = fixture.intent.namespace() };
    {
        var store = try location.openStore();
        defer store.deinit();
        try store.reserve(fixture.intent);
        try fixture.capture(&store);
    }
    const transport = @import("http_routes.zig");
    const authority = @import("../system_catalog/setting_authority.zig");
    const ledger = @import("schema_validation_ledger.zig");
    const Executor = struct {
        location: Location,
        mode: enum { not_sent, lost_after, absent, wrong, malformed, accept } = .not_sent,
        expected_command: []const u8 = "",
        expected_grant: []const u8 = "",
        writes: usize = 0,
        reads: usize = 0,
        accepted: bool = false,
        terminal: bool = false,
        cancel_after: ?*std.atomic.Value(bool) = null,
        fn execute(raw: *anyopaque, alloc: A, req: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // Independently acquire the actual file guard during transport.
            // This also proves observing was durable before any network call.
            var store = try self.location.openStore();
            defer store.deinit();
            try std.testing.expect(store.record().?.phase == .observing or store.record().?.phase == .authorized);
            const wanted = try store.record().?.receipt();
            try authority.verify("producer-secret", "producer-test", .read, req.body, @intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s)), req.header(authority.header_name).?);
            if (std.mem.endsWith(u8, req.uri, transport.Routes.internal_schema_validation)) {
                self.writes += 1;
                try std.testing.expectEqualStrings(self.expected_command, req.body);
                try std.testing.expectEqualStrings(self.expected_grant, req.header(authority.header_name).?);
                if (self.mode == .not_sent) {
                    req.delivery_tracker.?.markNotSent();
                    if (self.cancel_after) |signal| signal.store(true, .release);
                    return error.ConnectionResetByPeer;
                }
                self.accepted = true;
                if (self.mode == .lost_after) return error.ConnectionResetByPeer;
            } else {
                self.reads += 1;
                try std.testing.expect(std.mem.endsWith(u8, req.uri, "/internal/v1/system-catalog"));
                var parsed = try std.json.parseFromSlice(@import("../system_catalog/server_call.zig").Call, alloc, req.body, .{});
                defer parsed.deinit();
                try std.testing.expectEqualDeep(wanted, parsed.value.schema_validation_receipt_read);
            }
            // Transport fixture only: metadata ledger/enrollment acceptance has
            // its own real-store gate. This test must not claim quorum recovery.
            var head: @import("schema_validation_publication.zig").Head = .{ .metadata_group_id = wanted.lookup.group, .metadata_incarnation = wanted.lookup.metadata_incarnation, .id = wanted.lookup.id, .plan_digest = wanted.lookup.plan_digest, .table_id = wanted.lookup.table, .owner_count = 1 };
            if (self.terminal) {
                head = try head.cancel(head.revision, .requested);
                head.phase = .canceled;
                head.canceled = head.owner_count;
                head.terminal_chain = @splat(41);
                head.revision += 1;
                try head.validate();
            }
            var observed: receipts.Observation = .{ .lookup = wanted.lookup, .head = head, .observed = if (self.accepted and self.mode != .absent) wanted.expected else null };
            if (self.mode == .wrong) observed.observed.?.parent_authorization.grant[0] ^= 1;
            const body = if (self.mode == .malformed) try alloc.dupe(u8, "{") else try std.json.Stringify.valueAlloc(alloc, observed, .{});
            errdefer alloc.free(body);
            const headers = try alloc.alloc(http.Header, 4);
            var initialized: usize = 0;
            errdefer {
                for (headers[0..initialized]) |header| {
                    alloc.free(header.name);
                    alloc.free(header.value);
                }
                alloc.free(headers);
            }
            var group_buffer: [20]u8 = undefined;
            for (headers, [_][]const u8{ transport.Routes.raft_mutation_outcome_header, "content-type", "x-antfly-catalog-metadata-group", "x-antfly-catalog-metadata-incarnation" }, [_][]const u8{ transport.Routes.raft_mutation_outcome_committed, "application/json", try std.fmt.bufPrint(&group_buffer, "{d}", .{wanted.lookup.group}), &wanted.lookup.metadata_incarnation }) |*header, name, value| {
                const owned_name = try alloc.dupe(u8, name);
                errdefer alloc.free(owned_name);
                header.* = .{ .name = owned_name, .value = try alloc.dupe(u8, value) };
                initialized += 1;
            }
            if (self.cancel_after) |signal| signal.store(true, .release);
            return .{ .status = 200, .body = body, .headers = headers };
        }
    };
    var executor: Executor = .{ .location = location };
    const Client = @import("http_client.zig").MetadataHttpClient;
    var client = Client.init(a, .{ .ptr = &executor, .vtable = &.{ .execute = Executor.execute } });
    _ = client.withInternalServiceAuth("0123456789abcdef0123456789abcdef", "producer-test");
    _ = client.withSettingAuthority("producer-secret", "producer-test");
    try prepareParentAdmission(location, fixture.request, &client);
    const command = try std.json.Stringify.valueAlloc(a, ledger.Command{ .authorize_parent = fixture.request }, .{});
    defer a.free(command);
    const grant = blk: {
        var store = try location.openStore();
        defer store.deinit();
        try std.testing.expectEqual(Phase.prepared, store.record().?.phase);
        break :blk try a.dupe(u8, store.record().?.admission.?.grant);
    };
    defer a.free(grant);
    executor.expected_command = command;
    executor.expected_grant = grant;
    try std.testing.expectEqual(@as(usize, 0), executor.writes);
    try std.testing.expectError(error.Timeout, advanceParentAdmission(location, &client, "http://metadata.invalid", 0, null));
    var canceled: http.RequestCancellation = .{};
    canceled.cancel();
    try std.testing.expectError(error.Cancelled, advanceParentAdmission(location, &client, "http://metadata.invalid", 100, &canceled));
    try std.testing.expectEqual(@as(usize, 0), executor.writes);
    try std.testing.expectError(error.RaftMutationRequestNotSent, advanceParentAdmission(location, &client, "http://metadata.invalid", 100, null));
    {
        var store = try location.openStore();
        defer store.deinit();
        try std.testing.expectEqual(Phase.prepared, store.record().?.phase);
        try std.testing.expectEqualStrings(grant, store.record().?.admission.?.grant);
    }
    executor.mode = .lost_after;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, advanceParentAdmission(location, &client, "http://metadata.invalid", 100, null));
    try std.testing.expectEqual(@as(usize, 2), executor.writes);
    {
        var store = try location.openStore();
        defer store.deinit();
        try std.testing.expectEqual(Phase.observing, store.record().?.phase);
        try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, store.beginSubmission());
    }
    // Fresh client, cold file guard, and no retained delivery tracker. Missing,
    // malformed and mismatched receipts never authorize another POST mutation.
    client = Client.init(a, .{ .ptr = &executor, .vtable = &.{ .execute = Executor.execute } });
    _ = client.withInternalServiceAuth("0123456789abcdef0123456789abcdef", "producer-test");
    _ = client.withSettingAuthority("producer-secret", "producer-test");
    executor.mode = .absent;
    try std.testing.expectEqual(.pending, try advanceParentAdmission(location, &client, "http://metadata.invalid", 100, null));
    executor.mode = .wrong;
    try std.testing.expectEqual(.pending, try advanceParentAdmission(location, &client, "http://metadata.invalid", 100, null));
    executor.mode = .malformed;
    try std.testing.expectError(error.InvalidGenerationPublication, advanceParentAdmission(location, &client, "http://metadata.invalid", 100, null));
    executor.mode = .accept;
    try std.testing.expectEqual(.authorized, try advanceParentAdmission(location, &client, "http://metadata.invalid", 100, null));
    try std.testing.expectEqual(.authorized, try advanceParentAdmission(location, &client, "http://metadata.invalid", 100, null));
    try std.testing.expectEqual(@as(usize, 4), executor.reads);
    try std.testing.expectEqual(@as(usize, 2), executor.writes);
    executor.terminal = true;
    try std.testing.expect(try observeTerminalRetirement(location, &client, "http://metadata.invalid", 100, null));
    try std.testing.expect(try observeTerminalRetirement(location, &client, "http://metadata.invalid", 100, null));
    try std.testing.expectEqual(@as(usize, 5), executor.reads);
    try std.testing.expectEqual(@as(usize, 2), executor.writes);
    for ([_]@FieldType(Executor, "mode"){ .accept, .not_sent }) |mode| {
        const direct_dir = try std.fmt.allocPrint(a, "{s}/{s}", .{ root_dir, @tagName(mode) });
        defer a.free(direct_dir);
        const direct = try TestFixture.init(a, direct_dir);
        const direct_location: Location = .{ .alloc = a, .io = std.testing.io, .root_dir = direct_dir, .root_incarnation = direct.root.root_incarnation, .namespace = direct.intent.namespace() };
        {
            var store = try direct_location.openStore();
            defer store.deinit();
            try store.reserve(direct.intent);
            try direct.capture(&store);
        }
        var signal = std.atomic.Value(bool).init(false);
        var direct_executor: Executor = .{ .location = direct_location, .mode = mode, .cancel_after = &signal };
        var direct_client = Client.init(a, .{ .ptr = &direct_executor, .vtable = &.{ .execute = Executor.execute } });
        _ = direct_client.withInternalServiceAuth("0123456789abcdef0123456789abcdef", "producer-test");
        _ = direct_client.withSettingAuthority("producer-secret", "producer-test");
        try prepareParentAdmission(direct_location, direct.request, &direct_client);
        const saved = blk: {
            var store = try direct_location.openStore();
            defer store.deinit();
            break :blk .{ try a.dupe(u8, store.record().?.admission.?.command), try a.dupe(u8, store.record().?.admission.?.grant) };
        };
        defer a.free(saved[0]);
        defer a.free(saved[1]);
        direct_executor.expected_command = saved[0];
        direct_executor.expected_grant = saved[1];
        var request_location = direct_location;
        request_location.context.cancellation = local.api_operation.CancellationToken.fromAtomic(&signal);
        if (mode == .accept) {
            try std.testing.expectEqual(.authorized, try advanceParentAdmission(request_location, &direct_client, "http://metadata.invalid", 100, null));
        } else try std.testing.expectError(error.RaftMutationRequestNotSent, advanceParentAdmission(request_location, &direct_client, "http://metadata.invalid", 100, null));
        try std.testing.expect(signal.load(.acquire));
        var store = try direct_location.openStore();
        defer store.deinit();
        try std.testing.expectEqual(if (mode == .accept) Phase.authorized else .prepared, store.record().?.phase);
        try std.testing.expectEqual(@as(usize, 1), direct_executor.writes);
        try std.testing.expectEqual(@as(usize, 0), direct_executor.reads);
    }
}

test "schema validation source outbox terminal cohorts discharge uncertainty and unlink outside guards across restart" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root_dir = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/terminal-source", .{tmp.sub_path});
    defer a.free(root_dir);
    const fixture = try TestFixture.init(a, root_dir);
    const location: Location = .{ .alloc = a, .io = io, .root_dir = root_dir, .root_incarnation = fixture.root.root_incarnation, .namespace = fixture.intent.namespace() };
    {
        var store = try location.openStore();
        defer store.deinit();
        try store.reserve(fixture.intent);
    }
    const payloads = @import("../raft/storage/snapshot_payload_store.zig");
    const snapshot_dir = try std.fmt.allocPrint(a, "{s}/snapshots", .{root_dir});
    defer a.free(snapshot_dir);
    const manifest = try std.json.Stringify.valueAlloc(a, fixture.image.witness, .{});
    defer a.free(manifest);
    const Artifact = struct {
        manifest: []const u8,
        pub fn len(_: @This()) u64 {
            return 128;
        }
        pub fn applicationManifest(self: @This()) []const u8 {
            return self.manifest;
        }
        pub fn writeTo(_: @This(), writer: *std.Io.Writer) !void {
            try writer.splatByteAll('a', 128);
        }
    };
    // Real envelope/link/unlink durability with an explicitly opaque fixture
    // payload, NOT a fabricated native NRSP2 stream or metadata acceptance.
    const receipt = try payloads.writeArtifactWithReceipt(a, io, snapshot_dir, fixture.intent.image.index, fixture.intent.image.term, Artifact{ .manifest = manifest });
    var retained = try payloads.retainPublished(a, io, snapshot_dir, receipt, fixture.intent.image.reference);
    defer retained.deinit(a);
    {
        var store = try location.openStore();
        defer store.deinit();
        try store.capture(retained);
        try store.prepare(fixture.request, "unsubmitted-transport-fixture");
        var submission = try store.beginSubmission();
        defer submission.deinit();
    }
    const barriers = @import("schema_validation_publication.zig");
    var head: barriers.Head = .{ .metadata_group_id = fixture.intent.lookup.group, .metadata_incarnation = fixture.intent.lookup.metadata_incarnation, .table_id = fixture.intent.lookup.table, .id = fixture.intent.lookup.id, .plan_digest = fixture.intent.lookup.plan_digest, .owner_count = 1 };
    var observed: receipts.Observation = .{ .lookup = fixture.intent.lookup };
    {
        var store = try location.openStore();
        defer store.deinit();
        try std.testing.expectError(error.SchemaValidationNotReady, store.beginTerminalRetirement(observed));
        head = try head.cancel(head.revision, .requested);
        observed.head = head;
        try std.testing.expectError(error.SchemaValidationNotReady, store.beginTerminalRetirement(observed));
        head.phase = .canceled;
        observed.head = head;
        try std.testing.expectError(error.InvalidGenerationPublication, store.beginTerminalRetirement(observed)); // Partial cohort is not terminal.
        head.canceled = head.owner_count;
        head.terminal_chain = @splat(42);
        head.revision += 1;
        observed.head = head;
        var foreign = observed;
        foreign.lookup.id[0] ^= 1;
        try std.testing.expectError(error.GenerationPublicationChanged, store.beginTerminalRetirement(foreign));
        try std.testing.expectEqual(Phase.observing, store.record().?.phase);
        try store.beginTerminalRetirement(observed); // No acceptance inferred from absence.
    }
    const Release = struct {
        location: Location,
        directory: []const u8,
        retained: payloads.RetainedSnapshot,
        fail_after_unlink: bool = true,
        calls: usize = 0,
        fn run(raw: *anyopaque, alloc: A, request: retention.Request, expected: ?retention.CapturedPayload, context: local.api_operation.RequestContext) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var store = try self.location.openStore();
            defer store.deinit();
            try std.testing.expectEqual(Phase.retiring, store.record().?.phase);
            try context.ensureActive();
            try std.testing.expectEqualDeep(self.retained.reference, request.reference);
            try std.testing.expectEqualDeep(self.retained.digest, expected.?.digest);
            self.calls += 1;
            try payloads.releaseReference(alloc, self.location.io, self.directory, request.reference, .{ .index = request.index, .term = request.term }, expected);
            if (self.fail_after_unlink) return error.WriteFailed;
        }
    };
    var release: Release = .{ .location = location, .directory = snapshot_dir, .retained = retained };
    const NoCapture = struct {
        fn run(_: *anyopaque, _: A, _: retention.Request, _: local.api_operation.RequestContext) !retention.RetainedSnapshot {
            return error.TestUnexpectedCapture;
        }
    };
    const source: retention.Source = .{ .ptr = &release, .retain = NoCapture.run, .release = Release.run };
    try std.testing.expectError(error.WriteFailed, finishSourceRetirementWithRetention(location, source));
    try std.testing.expectError(error.FileNotFound, payloads.readRetained(a, io, snapshot_dir, retained.reference, retained.identity));
    var stale = blk: {
        var store = try location.openStore();
        defer store.deinit();
        try std.testing.expectEqual(Phase.retiring, store.record().?.phase);
        break :blk (try store.retirement()).?;
    };
    defer stale.deinit();
    release.fail_after_unlink = false;
    try finishSourceRetirementWithRetention(location, source);
    try finishSourceRetirementWithRetention(location, source);
    try std.testing.expectEqual(@as(usize, 2), release.calls);
    var store = try location.openStore();
    defer store.deinit();
    try std.testing.expectEqual(Phase.retired, store.record().?.phase);
    var successor = fixture.intent;
    successor.image.reference[0] ^= 1;
    try store.reserve(successor);
    try store.beginRetirement(null, null);
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, store.completeRetirement(stale));
}

test "schema validation source outbox retirement stays durable until release succeeds and bounds churn" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root_dir = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/source-retirement", .{tmp.sub_path});
    defer a.free(root_dir);
    const fixture = try TestFixture.init(a, root_dir);
    const location: Location = .{ .alloc = a, .io = std.testing.io, .root_dir = root_dir, .root_incarnation = fixture.root.root_incarnation, .namespace = fixture.intent.namespace() };
    const Release = struct {
        location: Location,
        failed: bool = true,
        calls: usize = 0,
        fn run(raw: *anyopaque, intent: Intent, image: ?Image) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try intent.validate();
            try std.testing.expect(image == null);
            var store = try self.location.openStore();
            defer store.deinit();
            try std.testing.expectEqual(Phase.retiring, store.record().?.phase);
            self.calls += 1;
            if (self.failed) return error.WriteFailed;
        }
    };
    var release: Release = .{ .location = location };
    var intent = fixture.intent;
    for (0..16) |i| {
        intent.image.reference[0] = @intCast(i + 1);
        {
            var store = try location.openStore();
            defer store.deinit();
            try store.reserve(intent);
            try store.beginRetirement(null, null);
        }
        try std.testing.expectError(error.WriteFailed, finishSourceRetirement(location, &release, Release.run));
        release.failed = false;
        try finishSourceRetirement(location, &release, Release.run);
        const calls = release.calls;
        try finishSourceRetirement(location, &release, Release.run);
        try std.testing.expectEqual(calls, release.calls);
        var store = try location.openStore();
        defer store.deinit();
        try store.reserve(intent); // Exact terminal intent never starts again.
        try std.testing.expectEqual(Phase.retired, store.record().?.phase);
        release.failed = true;
    }
    var store = try location.openStore();
    defer store.deinit();
    var iterator = store.guard.directory.iterate();
    var files: usize = 0;
    while (try iterator.next(std.testing.io)) |_| files += 1;
    try std.testing.expectEqual(@as(usize, 3), files); // One slot, its lock, and the empty pending directory.
}

test "schema validation source outbox pending discovery survives cold recovery and bounds historical churn" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(path);
    const fixture = try TestFixture.init(a, path);
    const location: Location = .{ .alloc = a, .io = std.testing.io, .root_dir = path, .root_incarnation = fixture.root.root_incarnation, .namespace = fixture.intent.namespace() };
    {
        var store = try location.openStore();
        defer store.deinit();
        try store.reserve(fixture.intent);
    }
    {
        var cursor = (try RecoveryCursor.init(a, std.testing.io, path)).?;
        defer cursor.deinit();
        try std.testing.expectEqualSlices(u8, &location.namespace, &(try cursor.next(.{})).?);
        try std.testing.expect(try cursor.next(.{}) == null);
    }
    const pending_path = try std.fs.path.join(a, &.{ path, directory_name, pending_directory });
    defer a.free(pending_path);
    const dir = try std.Io.Dir.cwd().openDir(std.testing.io, pending_path, .{});
    defer dir.close(std.testing.io);
    const name = std.fmt.bytesToHex(location.namespace, .lower);
    try dir.deleteFile(std.testing.io, &name);
    {
        // Repair a lost locator from authenticated current state, under lock.
        var store = try location.openStore();
        defer store.deinit();
        try std.testing.expectEqual(Phase.reserved, store.record().?.phase);
        var cursor = (try RecoveryCursor.init(a, std.testing.io, path)).?;
        defer cursor.deinit();
        try std.testing.expectEqualSlices(u8, &location.namespace, &(try cursor.next(.{})).?);
        try store.beginRetirement(null, null);
        var ticket = (try store.retirement()).?;
        defer ticket.deinit();
        // No image was captured; real missing-file release and sync are
        // separately fault-qualified by the retention adapter campaign.
        try store.completeRetirement(ticket);
    }
    {
        var cursor = (try RecoveryCursor.init(a, std.testing.io, path)).?;
        defer cursor.deinit();
        try std.testing.expect(try cursor.next(.{}) == null);
    }
    // Crash after the terminal rename but before hint cleanup.
    const permissions: std.Io.File.Permissions = if (comptime @import("builtin").os.tag == .windows) .default_file else @fromBackingInt(0o600);
    const stale = try dir.createFile(std.testing.io, &name, .{ .exclusive = true, .permissions = permissions });
    stale.close(std.testing.io);
    {
        var store = try location.openStore();
        defer store.deinit();
        try std.testing.expectEqual(Phase.retired, store.record().?.phase);
    }
    var cursor = (try RecoveryCursor.init(a, std.testing.io, path)).?;
    defer cursor.deinit();
    try std.testing.expect(try cursor.next(.{}) == null);
}

test "schema validation source outbox pending recovery only observes uncertainty and retries terminal release without RPC" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(path);
    const fixture = try TestFixture.init(a, path);
    const location: Location = .{ .alloc = a, .io = std.testing.io, .root_dir = path, .root_incarnation = fixture.root.root_incarnation, .namespace = fixture.intent.namespace() };
    const State = struct {
        location: Location,
        observation: receipts.Observation,
        reads: usize = 0,
        releases: usize = 0,
        lose_release: bool = true,
        fn read(raw: *anyopaque, alloc: A, _: local.api_operation.RequestContext, expected: receipts.Request) !std.json.Parsed(receipts.Observation) {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // Reopening proves the caller holds no checkpoint lock over RPC.
            var store = try self.location.openStore();
            defer store.deinit();
            try std.testing.expectEqual(Phase.observing, store.record().?.phase);
            try std.testing.expectEqualDeep(try store.record().?.receipt(), expected);
            self.reads += 1;
            const bytes = try std.json.Stringify.valueAlloc(alloc, self.observation, .{});
            defer alloc.free(bytes);
            return std.json.parseFromSlice(receipts.Observation, alloc, bytes, .{ .allocate = .alloc_always });
        }
        fn retain(_: *anyopaque, _: A, _: retention.Request, _: local.api_operation.RequestContext) !retention.RetainedSnapshot {
            return error.TestUnexpectedRetention;
        }
        fn release(raw: *anyopaque, _: A, request: retention.Request, image: ?retention.CapturedPayload, _: local.api_operation.RequestContext) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var store = try self.location.openStore();
            defer store.deinit();
            try std.testing.expectEqual(Phase.retiring, store.record().?.phase);
            try std.testing.expectEqualDeep(store.record().?.intent.image, request);
            try std.testing.expectEqualDeep(store.record().?.image.?.payload_digest, image.?.digest);
            self.releases += 1;
            if (self.lose_release) return error.WriteFailed;
        }
    };
    var state: State = .{ .location = location, .observation = .{ .lookup = fixture.intent.lookup } };
    const reader: ReceiptReader = .{ .ptr = &state, .read = State.read };
    const source: retention.Source = .{ .ptr = &state, .retain = State.retain, .release = State.release };
    {
        var store = try location.openStore();
        defer store.deinit();
        try store.reserve(fixture.intent);
        try fixture.capture(&store);
        try store.prepare(fixture.request, "saved-exact-grant");
        var attempt = try store.beginSubmission();
        attempt.deinit();
    }
    try recoverRetirement(location, reader, source);
    try std.testing.expectEqual(@as(usize, 1), state.reads);
    try std.testing.expectEqual(@as(usize, 0), state.releases);
    state.observation.lookup.id[0] ^= 1;
    try std.testing.expectError(error.GenerationPublicationChanged, recoverRetirement(location, reader, source));
    state.observation.lookup = fixture.intent.lookup;
    state.observation.head = .{ .metadata_group_id = fixture.intent.lookup.group, .metadata_incarnation = fixture.intent.lookup.metadata_incarnation, .table_id = fixture.intent.lookup.table, .id = fixture.intent.lookup.id, .plan_digest = fixture.intent.lookup.plan_digest, .owner_count = 2, .phase = .canceling, .canceled = 1, .cancellation_revision = 1, .failure = .requested, .terminal_chain = @splat(42) };
    try recoverRetirement(location, reader, source);
    try std.testing.expectEqual(@as(usize, 0), state.releases);
    state.observation.head.?.phase = .canceled;
    state.observation.head.?.canceled = 2;
    try std.testing.expectError(error.WriteFailed, recoverRetirement(location, reader, source));
    const reads = state.reads;
    {
        var store = try location.openStore();
        defer store.deinit();
        try std.testing.expectEqual(Phase.retiring, store.record().?.phase);
    }
    state.lose_release = false;
    try recoverRetirement(location, reader, source);
    try recoverRetirement(location, reader, source);
    try std.testing.expectEqual(reads, state.reads);
    try std.testing.expectEqual(@as(usize, 2), state.releases);
    var cursor = (try RecoveryCursor.init(a, std.testing.io, path)).?;
    defer cursor.deinit();
    try std.testing.expect(try cursor.next(.{}) == null);
}
