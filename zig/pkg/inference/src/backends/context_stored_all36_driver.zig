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

//! Test-only immutable-file provider for the all-36 stored publication seam.
//! Files are opened before fresh held generation and the same descriptors are
//! read for upload and rehashed immediately before the native Batch commit.
const std = @import("std");
const snapshot = @import("../runtime/file_snapshot.zig");
const owner_alloc = @import("../runtime/kv/host_allocation_tracker.zig");
const packed_mod = @import("packed_context_session.zig");
const storage = @import("packed_context_storage.zig");
const coordinator = @import("compaction_coordinator.zig");

const max_artifacts = 36;
const max_evidence = 32;

pub const FileSpec = struct {
    path: []const u8,
    size_bytes: u64,
    sha256: [32]u8,
};

pub const ArtifactSpec = struct {
    coordinate: coordinator.StoredDiagnosticCoordinate,
    keys: FileSpec,
    values: FileSpec,
    bias: FileSpec,
};

pub const Config = struct {
    artifact_dir: std.Io.Dir,
    evidence_dir: std.Io.Dir,
    ledger_dir: std.Io.Dir,
    reservation_name: []const u8,
    artifacts: []const ArtifactSpec,
    evidence: []const FileSpec,
    reservation: coordinator.StoredDiagnosticReservation,
    generator_sha256: [32]u8,
    corpus_sha256: [32]u8,
    ordered_rows_sha256: [32]u8,
    partition_sha256: [32]u8,
    fault: ?Fault = null,
};

pub const Fault = enum { tuple_write, ledger_write, content_write };

const Opened = struct {
    file: std.Io.File,
    initial: std.Io.File.Stat,
    spec: FileSpec,
};

const ArtifactLease = struct { keys: Opened, values: Opened, bias: Opened };

pub const Driver = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    config: Config,
    artifact_leases: [max_artifacts]ArtifactLease = undefined,
    evidence_leases: [max_evidence]Opened = undefined,
    artifact_count: usize = 0,
    evidence_count: usize = 0,
    ledger: ?std.Io.File = null,
    tuple_claim: ?std.Io.File = null,
    content_claim: ?std.Io.File = null,
    materialized: bool = false,
    materialization_tracker: ?*owner_alloc.HostAllocationTracker = null,
    materialization_before: owner_alloc.Snapshot = .{},
    materialization_tracked_bytes: u64 = 0,
    artifacts: [max_artifacts]packed_mod.StoredArtifact = undefined,
    keys_bytes: [max_artifacts]?[]u8 = @splat(null),
    values_bytes: [max_artifacts]?[]u8 = @splat(null),
    bias_bytes: [max_artifacts]?[]u8 = @splat(null),
    active_identity: ?packed_mod.StoredCampaignIdentity = null,
    active_derived_manifest: [32]u8 = @splat(0),
    reservation_open: bool = false,
    terminal: bool = false,

    fn digest(bytes: []const u8) [32]u8 {
        var result: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
        return result;
    }

    fn sameStat(a: std.Io.File.Stat, b: std.Io.File.Stat) bool {
        return a.kind == .file and b.kind == .file and a.size == b.size and a.inode == b.inode and
            std.meta.eql(a.mtime, b.mtime) and std.meta.eql(a.ctime, b.ctime);
    }

    fn openOne(io: std.Io, dir: std.Io.Dir, spec: FileSpec) !Opened {
        if (spec.size_bytes == 0 or std.mem.allEqual(u8, &spec.sha256, 0)) return error.InvalidPayloadProvenance;
        const file = try snapshot.openRegular(io, dir, spec.path, null);
        errdefer file.close(io);
        const initial = try file.stat(io);
        if (initial.size != spec.size_bytes) return error.InvalidPayloadProvenance;
        return .{ .file = file, .initial = initial, .spec = spec };
    }

    fn closeOpened(self: *Driver) void {
        for (self.artifact_leases[0..self.artifact_count]) |lease| {
            lease.keys.file.close(self.io);
            lease.values.file.close(self.io);
            lease.bias.file.close(self.io);
        }
        for (self.evidence_leases[0..self.evidence_count]) |lease| lease.file.close(self.io);
        self.artifact_count = 0;
        self.evidence_count = 0;
    }

    pub fn init(allocator: std.mem.Allocator, io: std.Io, config: Config) !Driver {
        if (!@import("builtin").is_test) return error.DiagnosticOnly;
        if (config.artifacts.len != max_artifacts or config.evidence.len == 0 or config.evidence.len > max_evidence or
            config.reservation.coordinates.len != max_artifacts or config.reservation.artifact_count != max_artifacts or
            config.reservation_name.len == 0 or std.mem.indexOfScalar(u8, config.reservation_name, '/') != null)
            return error.InvalidPayloadProvenance;
        if (std.mem.allEqual(u8, &config.generator_sha256, 0) or
            std.mem.allEqual(u8, &config.corpus_sha256, 0) or
            std.mem.allEqual(u8, &config.ordered_rows_sha256, 0) or
            std.mem.allEqual(u8, &config.partition_sha256, 0))
            return error.InvalidPayloadProvenance;
        if (try materializationUpper(config) != config.reservation.materialization_host_bytes)
            return error.InvalidPayloadProvenance;
        var self = Driver{ .allocator = allocator, .io = io, .config = config };
        errdefer self.deinit();
        for (config.artifacts, 0..) |spec, index| {
            if (!std.meta.eql(spec.coordinate, config.reservation.coordinates[index])) return error.InvalidPayloadProvenance;
            const keys = try openOne(io, config.artifact_dir, spec.keys);
            errdefer keys.file.close(io);
            const values = try openOne(io, config.artifact_dir, spec.values);
            errdefer values.file.close(io);
            const bias = try openOne(io, config.artifact_dir, spec.bias);
            self.artifact_leases[index] = .{ .keys = keys, .values = values, .bias = bias };
            self.artifact_count += 1;
        }
        for (config.evidence, 0..) |spec, index| {
            self.evidence_leases[index] = try openOne(io, config.evidence_dir, spec);
            self.evidence_count += 1;
        }
        return self;
    }

    pub fn deinit(self: *Driver) void {
        _ = releaseMaterialization(self) catch {};
        if (self.content_claim) |file| file.close(self.io);
        if (self.tuple_claim) |file| file.close(self.io);
        if (self.ledger) |file| file.close(self.io);
        self.closeOpened();
        self.* = undefined;
    }

    fn readAndVerify(self: *Driver, allocator: std.mem.Allocator, opened: Opened) ![]u8 {
        const size = std.math.cast(usize, opened.spec.size_bytes) orelse return error.InvalidPayloadProvenance;
        const bytes = try snapshot.readOpened(allocator, self.io, opened.file, size, null);
        errdefer allocator.free(bytes);
        if (!sameStat(opened.initial, try opened.file.stat(self.io)) or !std.meta.eql(digest(bytes), opened.spec.sha256))
            return error.InvalidPayloadProvenance;
        return bytes;
    }

    fn verifyOpened(self: *Driver, opened: Opened) !void {
        const initial = try opened.file.stat(self.io);
        if (!sameStat(opened.initial, initial) or initial.size != opened.spec.size_bytes)
            return error.InvalidPayloadProvenance;
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        var buffer: [64 * 1024]u8 = undefined;
        var offset: u64 = 0;
        while (offset < initial.size) {
            const count: usize = @intCast(@min(initial.size - offset, buffer.len));
            if (try opened.file.readPositionalAll(self.io, buffer[0..count], offset) != count)
                return error.InvalidPayloadProvenance;
            hash.update(buffer[0..count]);
            offset += count;
        }
        if (try opened.file.readPositionalAll(self.io, buffer[0..1], offset) != 0 or
            !sameStat(initial, try opened.file.stat(self.io)) or
            !std.meta.eql(hash.finalResult(), opened.spec.sha256))
            return error.InvalidPayloadProvenance;
    }

    fn rounded(value: usize) !usize {
        return (try std.math.add(usize, value, std.heap.pageSize() - 1)) & ~(std.heap.pageSize() - 1);
    }

    fn materializationUpper(config: Config) !usize {
        var total: usize = 0;
        for (config.artifacts) |artifact| {
            total = try std.math.add(usize, total, try rounded(std.math.cast(usize, artifact.keys.size_bytes) orelse return error.InvalidPayloadProvenance));
            total = try std.math.add(usize, total, try rounded(std.math.cast(usize, artifact.values.size_bytes) orelse return error.InvalidPayloadProvenance));
            total = try std.math.add(usize, total, try rounded(std.math.cast(usize, artifact.bias.size_bytes) orelse return error.InvalidPayloadProvenance));
        }
        return total;
    }

    fn fingerprintU32(hash: *std.crypto.hash.sha2.Sha256, value: u32) void {
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &bytes, value, .little);
        hash.update(&bytes);
    }

    fn fingerprintU64(hash: *std.crypto.hash.sha2.Sha256, value: u64) void {
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, value, .little);
        hash.update(&bytes);
    }

    fn fingerprintCoordinate(hash: *std.crypto.hash.sha2.Sha256, value: coordinator.StoredDiagnosticCoordinate) void {
        fingerprintU32(hash, value.owner_layer);
        fingerprintU32(hash, value.head);
        fingerprintU32(hash, value.span);
        fingerprintU32(hash, value.logical_start);
        fingerprintU32(hash, value.logical_count);
        fingerprintU32(hash, value.retained_rows);
        fingerprintU32(hash, value.head_dim);
    }

    fn rawManifest(self: *Driver) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly.context.stored-raw-manifest.v1\x00");
        for (self.config.artifacts) |spec| {
            fingerprintCoordinate(&hash, spec.coordinate);
            hash.update(&spec.keys.sha256);
            hash.update(&spec.values.sha256);
            hash.update(&spec.bias.sha256);
        }
        return hash.finalResult();
    }

    fn reservationDigest(self: *Driver, state: u8, extra: []const u8) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly.context.stored-ledger.v1\x00");
        hash.update(&.{state});
        fingerprintU64(&hash, self.config.reservation.reservation_identity);
        hash.update(&self.config.generator_sha256);
        hash.update(&self.config.corpus_sha256);
        hash.update(&self.config.ordered_rows_sha256);
        hash.update(&self.config.partition_sha256);
        hash.update(&self.config.reservation.artifact_manifest_sha256);
        hash.update(&self.config.reservation.profile_sha256);
        hash.update(extra);
        return hash.finalResult();
    }

    fn tupleDigest(self: *Driver) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly.context.stored-held-tuple.v1\x00");
        hash.update(&self.config.generator_sha256);
        hash.update(&self.config.corpus_sha256);
        hash.update(&self.config.ordered_rows_sha256);
        hash.update(&self.config.partition_sha256);
        return hash.finalResult();
    }

    fn tupleClaimName(self: *Driver, buffer: *[80]u8) ![]const u8 {
        const encoded = std.fmt.bytesToHex(self.tupleDigest(), .lower);
        return std.fmt.bufPrint(buffer, "tuple-{s}.claim", .{&encoded});
    }

    fn writeLedger(self: *Driver, state: u8, extra: []const u8, offset: u64) ![32]u8 {
        const evidence = self.reservationDigest(state, extra);
        const file = self.ledger orelse return error.InvalidContextTransaction;
        var record: [33]u8 = undefined;
        record[0] = state;
        @memcpy(record[1..], &evidence);
        try file.writePositionalAll(self.io, &record, offset);
        try file.sync(self.io);
        return evidence;
    }

    fn reserve(ctx: *anyopaque) !coordinator.StoredDiagnosticReservation {
        const self: *Driver = @ptrCast(@alignCast(ctx));
        if (self.reservation_open or self.terminal or self.tuple_claim != null or self.ledger != null or
            !std.meta.eql(self.rawManifest(), self.config.reservation.artifact_manifest_sha256))
            return error.InvalidPayloadProvenance;
        var tuple_name_buffer: [80]u8 = undefined;
        const tuple_name = try self.tupleClaimName(&tuple_name_buffer);
        const tuple_claim = try self.config.ledger_dir.createFile(self.io, tuple_name, .{ .exclusive = true, .truncate = false });
        self.tuple_claim = tuple_claim;
        if (self.config.fault == .tuple_write) return error.InjectedStoredDriverFailure;
        const tuple_digest = self.tupleDigest();
        try tuple_claim.writePositionalAll(self.io, &tuple_digest, 0);
        try tuple_claim.sync(self.io);
        // The file is the durable nonreuse claim. Once created it remains on
        // every later error; the Driver owns only its open handle.
        const ledger = try self.config.ledger_dir.createFile(self.io, self.config.reservation_name, .{ .exclusive = true, .truncate = false });
        self.ledger = ledger;
        if (self.config.fault == .ledger_write) return error.InjectedStoredDriverFailure;
        _ = try self.writeLedger(1, &.{}, 0);
        self.reservation_open = true;
        return self.config.reservation;
    }

    fn releaseMaterialization(ctx: *anyopaque) !void {
        const self: *Driver = @ptrCast(@alignCast(ctx));
        if (!self.materialized) return;
        const tracker = self.materialization_tracker orelse return error.ContextPhysicalAccountingUnavailable;
        const allocator = tracker.allocator();
        const before_release = tracker.snapshot();
        for (0..self.artifact_count) |index| {
            if (self.keys_bytes[index]) |bytes| allocator.free(bytes);
            if (self.values_bytes[index]) |bytes| allocator.free(bytes);
            if (self.bias_bytes[index]) |bytes| allocator.free(bytes);
            self.keys_bytes[index] = null;
            self.values_bytes[index] = null;
            self.bias_bytes[index] = null;
        }
        self.materialized = false;
        self.materialization_tracker = null;
        const after = tracker.snapshot();
        if (!before_release.tracked_combined_physical_available or !after.tracked_combined_physical_available or
            before_release.owner_identity != self.materialization_before.owner_identity or
            after.owner_identity != self.materialization_before.owner_identity or
            before_release.tracked_combined_live_bytes < after.tracked_combined_live_bytes or
            before_release.tracked_combined_live_bytes - after.tracked_combined_live_bytes != self.materialization_tracked_bytes)
            return error.ContextPhysicalAccountingUnavailable;
        self.materialization_before = .{};
        self.materialization_tracked_bytes = 0;
    }

    fn materialize(ctx: *anyopaque, identity: packed_mod.StoredCampaignIdentity, tracker: *owner_alloc.HostAllocationTracker) !coordinator.StoredDiagnosticMaterialization {
        const self: *Driver = @ptrCast(@alignCast(ctx));
        if (!self.reservation_open or self.terminal or self.materialized or self.active_identity != null or
            std.mem.allEqual(u8, &identity.query_content_sha256, 0))
            return error.InvalidContextTransaction;
        const before = tracker.snapshot();
        if (!before.tracked_combined_physical_available) return error.ContextPhysicalAccountingUnavailable;
        self.materialized = true;
        self.materialization_tracker = tracker;
        self.materialization_before = before;
        errdefer {
            releaseMaterialization(self) catch {};
            self.active_identity = null;
            self.active_derived_manifest = @splat(0);
        }
        const allocator = tracker.allocator();
        for (self.config.artifacts, self.artifact_leases[0..self.artifact_count], 0..) |spec, lease, index| {
            const keys = try self.readAndVerify(allocator, lease.keys);
            self.keys_bytes[index] = keys;
            const values = try self.readAndVerify(allocator, lease.values);
            self.values_bytes[index] = values;
            const bias = try self.readAndVerify(allocator, lease.bias);
            self.bias_bytes[index] = bias;
            self.artifacts[index] = .{
                .owner_layer = spec.coordinate.owner_layer,
                .head = spec.coordinate.head,
                .span = spec.coordinate.span,
                .logical_start = spec.coordinate.logical_start,
                .logical_count = spec.coordinate.logical_count,
                .retained_rows = spec.coordinate.retained_rows,
                .head_dim = spec.coordinate.head_dim,
                .keys_f16 = keys,
                .values_f16 = values,
                .bias_f32 = bias,
                .sha256 = undefined,
            };
            self.artifacts[index].sha256 = try packed_mod.storedArtifactDiagnosticDigest(identity, self.artifacts[index]);
        }
        var manifest = std.crypto.hash.sha2.Sha256.init(.{});
        manifest.update("antfly.context.stored-loader.v1\x00");
        var count_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &count_bytes, @intCast(self.artifact_count), .little);
        manifest.update(&count_bytes);
        for (self.artifacts[0..self.artifact_count]) |artifact| manifest.update(&artifact.sha256);
        self.active_derived_manifest = manifest.finalResult();
        self.active_identity = identity;
        const after = tracker.snapshot();
        if (!after.tracked_combined_physical_available or after.owner_identity != before.owner_identity or
            after.tracked_combined_live_bytes <= before.tracked_combined_live_bytes)
            return error.ContextPhysicalAccountingUnavailable;
        self.materialization_tracked_bytes = after.tracked_combined_live_bytes - before.tracked_combined_live_bytes;
        if (self.materialization_tracked_bytes > @as(u64, @intCast(self.config.reservation.materialization_host_bytes)))
            return error.ContextStagingBudgetExceeded;
        return .{
            .artifacts = self.artifacts[0..self.artifact_count],
            .derived_manifest_sha256 = self.active_derived_manifest,
            .tracked_host_bytes = self.materialization_tracked_bytes,
            .ctx = self,
            .deinit_fn = releaseMaterialization,
        };
    }

    fn contentClaimName(identity: packed_mod.StoredCampaignIdentity, buffer: *[80]u8) ![]const u8 {
        const encoded = std.fmt.bytesToHex(identity.query_content_sha256, .lower);
        return std.fmt.bufPrint(buffer, "held-{s}.claim", .{&encoded});
    }

    fn precommit(ctx: *anyopaque, identity: packed_mod.StoredCampaignIdentity, derived_manifest: [32]u8) !void {
        const self: *Driver = @ptrCast(@alignCast(ctx));
        if (!self.reservation_open or self.terminal or self.materialized or self.active_identity == null or
            !std.meta.eql(self.active_identity.?, identity) or !std.meta.eql(self.active_derived_manifest, derived_manifest))
            return error.InvalidContextTransaction;
        for (self.artifact_leases[0..self.artifact_count]) |lease| {
            try self.verifyOpened(lease.keys);
            try self.verifyOpened(lease.values);
            try self.verifyOpened(lease.bias);
        }
        for (self.evidence_leases[0..self.evidence_count]) |lease| try self.verifyOpened(lease);
        if (!std.meta.eql(self.rawManifest(), self.config.reservation.artifact_manifest_sha256))
            return error.InvalidPayloadProvenance;
        var name_buffer: [80]u8 = undefined;
        const name = try contentClaimName(identity, &name_buffer);
        const claim = try self.config.ledger_dir.createFile(self.io, name, .{ .exclusive = true, .truncate = false });
        self.content_claim = claim;
        if (self.config.fault == .content_write) return error.InjectedStoredDriverFailure;
        try claim.writePositionalAll(self.io, &identity.query_content_sha256, 0);
        try claim.sync(self.io);
        _ = try self.writeLedger(2, &identity.query_content_sha256, 64);
    }

    fn commit(ctx: *anyopaque, reservation_identity: u64, receipt: packed_mod.StoredPublicationReceipt) !void {
        const self: *Driver = @ptrCast(@alignCast(ctx));
        if (!self.reservation_open or self.terminal or self.active_identity == null or reservation_identity != self.config.reservation.reservation_identity or
            !std.meta.eql(receipt.loader_manifest_sha256, self.active_derived_manifest) or
            !std.meta.eql(receipt.query_content_sha256, self.active_identity.?.query_content_sha256))
            return error.InvalidContextTransaction;
        var extra: [72]u8 = undefined;
        std.mem.writeInt(u64, extra[0..8], receipt.publication_epoch_after, .little);
        @memcpy(extra[8..40], &receipt.loader_manifest_sha256);
        @memcpy(extra[40..72], &receipt.query_content_sha256);
        _ = try self.writeLedger(3, &extra, 128);
        self.terminal = true;
        self.active_identity = null;
        self.active_derived_manifest = @splat(0);
    }

    fn abort(ctx: *anyopaque, reservation_identity: u64) !coordinator.StoredReservationFailureReceipt {
        const self: *Driver = @ptrCast(@alignCast(ctx));
        if (!self.reservation_open or self.terminal or reservation_identity != self.config.reservation.reservation_identity)
            return error.InvalidContextTransaction;
        const evidence = try self.writeLedger(4, &.{}, 128);
        self.terminal = true;
        self.active_identity = null;
        self.active_derived_manifest = @splat(0);
        return .{ .reservation_identity = reservation_identity, .durable = true, .nonreuse_committed = true, .evidence_sha256 = evidence };
    }

    pub fn provider(self: *Driver) coordinator.StoredDiagnosticProvider {
        return .{
            .ctx = self,
            .reserve_fn = reserve,
            .materialize_fn = materialize,
            .precommit_fn = precommit,
            .commit_reservation_fn = commit,
            .abort_reservation_fn = abort,
        };
    }
};

pub fn materializationUpperDiagnostic(config: Config) !usize {
    if (!@import("builtin").is_test) return error.DiagnosticOnly;
    return Driver.materializationUpper(config);
}

pub const LoaderGeometryBudget = packed_mod.StoredLoaderGeometryBudget;
pub const StoredGeometryLayout = packed_mod.StoredGeometryLayout;

fn copyCoordinateGeometries(coordinates: []const coordinator.StoredDiagnosticCoordinate) ![max_artifacts]packed_mod.StoredArtifactGeometry {
    if (coordinates.len != max_artifacts) return error.InvalidPayloadProvenance;
    var geometries: [max_artifacts]packed_mod.StoredArtifactGeometry = undefined;
    for (&geometries, coordinates) |*geometry, coordinate| geometry.* = .{
        .owner_layer = coordinate.owner_layer,
        .head = coordinate.head,
        .span = coordinate.span,
        .logical_start = coordinate.logical_start,
        .logical_count = coordinate.logical_count,
        .retained_rows = coordinate.retained_rows,
        .head_dim = coordinate.head_dim,
    };
    return geometries;
}

/// Test-binary-only bridge from the authority protocol's coordinates to the
/// byte-free geometry consumed by the real packed loader budget calculator.
pub fn loaderBudgetForCoordinatesDiagnostic(coordinates: []const coordinator.StoredDiagnosticCoordinate) !LoaderGeometryBudget {
    if (!@import("builtin").is_test) return error.DiagnosticOnly;
    const geometries = try copyCoordinateGeometries(coordinates);
    return packed_mod.storedLoaderBudgetFromGeometryDiagnostic(&geometries);
}

pub fn layoutForCoordinatesDiagnostic(coordinates: []const coordinator.StoredDiagnosticCoordinate, parts: []const []const storage.Segment) !StoredGeometryLayout {
    if (!@import("builtin").is_test) return error.DiagnosticOnly;
    const geometries = try copyCoordinateGeometries(coordinates);
    return packed_mod.storedGeometryLayoutDiagnostic(&geometries, parts);
}

pub fn rawManifestDiagnostic(config: Config) ![32]u8 {
    if (!@import("builtin").is_test) return error.DiagnosticOnly;
    var probe = Driver{ .allocator = undefined, .io = undefined, .config = config };
    return probe.rawManifest();
}

test "context stored all36 driver uses tracked materialization and durable nonreuse ledger" {
    const io = std.testing.io;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    var coordinates: [max_artifacts]coordinator.StoredDiagnosticCoordinate = undefined;
    var specs: [max_artifacts]ArtifactSpec = undefined;
    var key_names: [max_artifacts][32]u8 = undefined;
    var value_names: [max_artifacts][32]u8 = undefined;
    var bias_names: [max_artifacts][32]u8 = undefined;
    const key_bytes = [_]u8{ 0, 0 };
    const value_bytes = [_]u8{ 0, 0 };
    const bias_bytes = [_]u8{ 0, 0, 0, 0 };
    const key_sha = Driver.digest(&key_bytes);
    const value_sha = Driver.digest(&value_bytes);
    const bias_sha = Driver.digest(&bias_bytes);
    for (0..max_artifacts) |index| {
        const owner: u32 = @intCast(index / 12);
        const span: u32 = @intCast(index % 12);
        coordinates[index] = .{ .owner_layer = owner + 1, .head = 0, .span = span, .logical_start = span * 2, .logical_count = 2, .retained_rows = 1, .head_dim = 1 };
        const key_name = try std.fmt.bufPrint(&key_names[index], "k-{d}", .{index});
        const value_name = try std.fmt.bufPrint(&value_names[index], "v-{d}", .{index});
        const bias_name = try std.fmt.bufPrint(&bias_names[index], "b-{d}", .{index});
        inline for (.{ .{ key_name, &key_bytes }, .{ value_name, &value_bytes }, .{ bias_name, &bias_bytes } }) |item| {
            const file = try temporary.dir.createFile(io, item[0], .{ .exclusive = true });
            try file.writeStreamingAll(io, item[1]);
            try file.sync(io);
            file.close(io);
        }
        specs[index] = .{
            .coordinate = coordinates[index],
            .keys = .{ .path = key_name, .size_bytes = key_bytes.len, .sha256 = key_sha },
            .values = .{ .path = value_name, .size_bytes = value_bytes.len, .sha256 = value_sha },
            .bias = .{ .path = bias_name, .size_bytes = bias_bytes.len, .sha256 = bias_sha },
        };
    }
    const evidence_file = try temporary.dir.createFile(io, "evidence", .{ .exclusive = true });
    try evidence_file.writeStreamingAll(io, "frozen");
    try evidence_file.sync(io);
    evidence_file.close(io);
    const evidence_bytes = "frozen";
    var evidence = [_]FileSpec{.{ .path = "evidence", .size_bytes = evidence_bytes.len, .sha256 = Driver.digest(evidence_bytes) }};
    var config = Config{
        .artifact_dir = temporary.dir,
        .evidence_dir = temporary.dir,
        .ledger_dir = temporary.dir,
        .reservation_name = "reservation",
        .artifacts = &specs,
        .evidence = &evidence,
        .reservation = .{
            .reservation_identity = 7,
            .algorithm_sha256 = @splat(1),
            .build_sha256 = @splat(2),
            .artifact_manifest_sha256 = @splat(0),
            .profile_sha256 = @splat(3),
            .coordinates = &coordinates,
            .source_descriptor_sha256 = @splat(4),
            .source_payload_sha256 = @splat(5),
            .loader_budget_bytes = 1,
            .materialization_host_bytes = 0,
            .artifact_count = max_artifacts,
            .expected_baseline_context_bytes = 1,
            .expected_owner_count = 3,
            .expected_total_owner_count = 36,
        },
        .generator_sha256 = @splat(6),
        .corpus_sha256 = @splat(7),
        .ordered_rows_sha256 = @splat(8),
        .partition_sha256 = @splat(9),
    };
    config.reservation.materialization_host_bytes = try Driver.materializationUpper(config);
    var probe = Driver{ .allocator = std.testing.allocator, .io = io, .config = config };
    config.reservation.artifact_manifest_sha256 = probe.rawManifest();
    var driver = try Driver.init(std.testing.allocator, io, config);
    var driver_active = true;
    defer if (driver_active) driver.deinit();
    const provider = driver.provider();
    _ = try provider.reserve_fn(provider.ctx);
    try std.testing.expectError(error.InvalidPayloadProvenance, provider.reserve_fn(provider.ctx));
    var tracker = owner_alloc.HostAllocationTracker.init(null, 0);
    const identity = packed_mod.StoredCampaignIdentity{
        .load_generation = 1,
        .session_identity = 2,
        .publication_epoch = 3,
        .query_campaign_identity = 4,
        .query_campaign_sha256 = @splat(10),
        .query_payload_sha256 = @splat(11),
        .query_content_sha256 = @splat(12),
        .query_payload_readback_bytes = 1,
        .source_descriptor_sha256 = @splat(4),
        .source_payload_sha256 = @splat(5),
    };
    const before = tracker.snapshot();
    const materialization = try provider.materialize_fn(provider.ctx, identity, &tracker);
    try std.testing.expectEqual(@as(usize, max_artifacts), materialization.artifacts.len);
    try std.testing.expect(materialization.tracked_host_bytes > 0);
    try std.testing.expectEqual(before.tracked_combined_live_bytes + materialization.tracked_host_bytes, tracker.snapshot().tracked_combined_live_bytes);
    const unrelated = try tracker.allocator().alloc(u8, 4097);
    const before_release = tracker.snapshot();
    try materialization.deinit_fn(materialization.ctx);
    const after_release = tracker.snapshot();
    try std.testing.expectEqual(materialization.tracked_host_bytes, before_release.tracked_combined_live_bytes - after_release.tracked_combined_live_bytes);
    try std.testing.expect(after_release.tracked_combined_live_bytes > before.tracked_combined_live_bytes);
    tracker.allocator().free(unrelated);
    try std.testing.expectEqual(before.tracked_combined_live_bytes, tracker.snapshot().tracked_combined_live_bytes);
    try provider.precommit_fn(provider.ctx, identity, materialization.derived_manifest_sha256);
    try provider.commit_reservation_fn(provider.ctx, 7, .{
        .load_generation = identity.load_generation,
        .session_identity = identity.session_identity,
        .publication_epoch_before = identity.publication_epoch,
        .publication_epoch_after = identity.publication_epoch + 1,
        .query_campaign_identity = identity.query_campaign_identity,
        .query_campaign_sha256 = identity.query_campaign_sha256,
        .query_payload_sha256 = identity.query_payload_sha256,
        .query_content_sha256 = identity.query_content_sha256,
        .query_payload_readback_bytes = identity.query_payload_readback_bytes,
        .source_descriptor_sha256 = identity.source_descriptor_sha256,
        .source_payload_sha256 = identity.source_payload_sha256,
        .artifact_count = max_artifacts,
        .loader_physical_bytes = 1,
        .loader_tracker_owner_identity = tracker.owner_identity,
        .loader_entries_physical_bytes = 1,
        .loader_buffer_shared_bytes = 0,
        .loader_manifest_sha256 = materialization.derived_manifest_sha256,
        .validation_status_readback_bytes = 1,
    });
    try std.testing.expectError(error.InvalidContextTransaction, provider.abort_reservation_fn(provider.ctx, 7));
    driver.deinit();
    driver_active = false;

    // A different reservation filename cannot reuse the frozen generator,
    // corpus, ordered-row and partition tuple before held generation.
    var reused_tuple_config = config;
    reused_tuple_config.reservation_name = "reservation-reused-tuple";
    reused_tuple_config.reservation.reservation_identity = 70;
    var reused_tuple_driver = try Driver.init(std.testing.allocator, io, reused_tuple_config);
    const reused_tuple_provider = reused_tuple_driver.provider();
    try std.testing.expectError(error.PathAlreadyExists, reused_tuple_provider.reserve_fn(reused_tuple_provider.ctx));
    reused_tuple_driver.deinit();

    // A genuinely different frozen tuple still cannot reuse identical held
    // content merely by changing campaign/session nonces.
    var config2 = config;
    config2.reservation_name = "reservation-2";
    config2.reservation.reservation_identity = 8;
    config2.partition_sha256 = @splat(10);
    var driver2 = try Driver.init(std.testing.allocator, io, config2);
    var driver2_active = true;
    defer if (driver2_active) driver2.deinit();
    const provider2 = driver2.provider();
    _ = try provider2.reserve_fn(provider2.ctx);
    var identity2 = identity;
    identity2.session_identity = 20;
    identity2.query_campaign_identity = 40;
    identity2.query_campaign_sha256 = @splat(20);
    identity2.query_payload_sha256 = @splat(21);
    const materialization2 = try provider2.materialize_fn(provider2.ctx, identity2, &tracker);
    try materialization2.deinit_fn(materialization2.ctx);
    try std.testing.expectError(error.PathAlreadyExists, provider2.precommit_fn(provider2.ctx, identity2, materialization2.derived_manifest_sha256));
    const failed2 = try provider2.abort_reservation_fn(provider2.ctx, 8);
    try std.testing.expect(failed2.durable and failed2.nonreuse_committed);
    driver2.deinit();
    driver2_active = false;

    // A same-descriptor evidence mutation is rejected before any commit.
    var config3 = config;
    config3.reservation_name = "reservation-3";
    config3.reservation.reservation_identity = 9;
    config3.partition_sha256 = @splat(11);
    var driver3 = try Driver.init(std.testing.allocator, io, config3);
    var driver3_active = true;
    defer if (driver3_active) driver3.deinit();
    const provider3 = driver3.provider();
    _ = try provider3.reserve_fn(provider3.ctx);
    var identity3 = identity;
    identity3.session_identity = 30;
    identity3.query_campaign_identity = 50;
    identity3.query_campaign_sha256 = @splat(30);
    identity3.query_payload_sha256 = @splat(31);
    identity3.query_content_sha256 = @splat(32);
    const materialization3 = try provider3.materialize_fn(provider3.ctx, identity3, &tracker);
    try materialization3.deinit_fn(materialization3.ctx);
    const changed = try temporary.dir.createFile(io, "evidence", .{ .truncate = true });
    try changed.writeStreamingAll(io, "changed");
    try changed.sync(io);
    changed.close(io);
    try std.testing.expectError(error.InvalidPayloadProvenance, provider3.precommit_fn(provider3.ctx, identity3, materialization3.derived_manifest_sha256));
    const failed3 = try provider3.abort_reservation_fn(provider3.ctx, 9);
    try std.testing.expect(failed3.durable and failed3.nonreuse_committed);
    driver3.deinit();
    driver3_active = false;
    const restored = try temporary.dir.createFile(io, "evidence", .{ .truncate = true });
    try restored.writeStreamingAll(io, "frozen");
    try restored.sync(io);
    restored.close(io);

    // Ownership transfers immediately after each exclusive create. Injected
    // write failures therefore leave one live owner for deinit, never a stale
    // closed handle or a second errdefer close.
    var tuple_fault_config = config;
    tuple_fault_config.reservation_name = "reservation-tuple-write-fault";
    tuple_fault_config.reservation.reservation_identity = 10;
    tuple_fault_config.partition_sha256 = @splat(12);
    tuple_fault_config.fault = .tuple_write;
    var tuple_fault_driver = try Driver.init(std.testing.allocator, io, tuple_fault_config);
    const tuple_fault_provider = tuple_fault_driver.provider();
    try std.testing.expectError(error.InjectedStoredDriverFailure, tuple_fault_provider.reserve_fn(tuple_fault_provider.ctx));
    tuple_fault_driver.deinit();

    var ledger_fault_config = config;
    ledger_fault_config.reservation_name = "reservation-ledger-write-fault";
    ledger_fault_config.reservation.reservation_identity = 11;
    ledger_fault_config.partition_sha256 = @splat(13);
    ledger_fault_config.fault = .ledger_write;
    var ledger_fault_driver = try Driver.init(std.testing.allocator, io, ledger_fault_config);
    const ledger_fault_provider = ledger_fault_driver.provider();
    try std.testing.expectError(error.InjectedStoredDriverFailure, ledger_fault_provider.reserve_fn(ledger_fault_provider.ctx));
    ledger_fault_driver.deinit();

    var content_fault_config = config;
    content_fault_config.reservation_name = "reservation-content-write-fault";
    content_fault_config.reservation.reservation_identity = 12;
    content_fault_config.partition_sha256 = @splat(14);
    content_fault_config.fault = .content_write;
    var content_fault_driver = try Driver.init(std.testing.allocator, io, content_fault_config);
    var content_fault_active = true;
    defer if (content_fault_active) content_fault_driver.deinit();
    const content_fault_provider = content_fault_driver.provider();
    _ = try content_fault_provider.reserve_fn(content_fault_provider.ctx);
    var identity4 = identity;
    identity4.session_identity = 40;
    identity4.query_campaign_identity = 60;
    identity4.query_campaign_sha256 = @splat(40);
    identity4.query_payload_sha256 = @splat(41);
    identity4.query_content_sha256 = @splat(42);
    const materialization4 = try content_fault_provider.materialize_fn(content_fault_provider.ctx, identity4, &tracker);
    try materialization4.deinit_fn(materialization4.ctx);
    try std.testing.expectError(error.InjectedStoredDriverFailure, content_fault_provider.precommit_fn(content_fault_provider.ctx, identity4, materialization4.derived_manifest_sha256));
    _ = try content_fault_provider.abort_reservation_fn(content_fault_provider.ctx, 12);
    content_fault_driver.deinit();
    content_fault_active = false;
}
