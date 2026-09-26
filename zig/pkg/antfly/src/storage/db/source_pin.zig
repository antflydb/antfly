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

//! Source-primary pin owner. Admission's prepared record is a durable short
//! mutation fence; no live recapture is permitted after its pinned receipt.
//! Artifact serialization and hashing run only against the durable seal.
const std = @import("std");
const ledger = @import("online_source.zig");
const seal = @import("native_backup_seal.zig");
const backup = @import("native_backup.zig");
const fs = @import("../../common/fs_paths.zig");
const state = @import("../source_pin_state.zig");
const snapshot = @import("../source_snapshot.zig");
const portable = @import("../portable_backup.zig");
const DB = @import("antfly_source_root").antfly_sources.physical_db.DB;
const Allocator = std.mem.Allocator;
const Cancellation = @import("types.zig").CancellationToken;
const gc = @import("source_pin_gc.zig");
pub const CleanupBudget = gc.Budget;
pub const CleanupWork = gc.Work;

const CleanupCursor = struct {
    const max_encoded = gc.max_path + 96;
    const Mode = enum(u8) { prepared_staging = 1, released_all = 2 };
    const Tree = enum(u8) { root = 1, staging = 2, done = 3 };
    namespace: [24]u8,
    pin: [32]u8,
    mode: Mode,
    tree: Tree,
    directory: gc.Cursor = .{},

    fn encode(self: *const CleanupCursor, buffer: *[max_encoded]u8) []const u8 {
        @memcpy(buffer[0..4], "SGC1");
        buffer[4..28].* = self.namespace;
        buffer[28..60].* = self.pin;
        buffer[60] = @intFromEnum(self.mode);
        buffer[61] = @intFromEnum(self.tree);
        std.mem.writeInt(u16, buffer[62..64], @intCast(self.directory.len), .little);
        @memcpy(buffer[64..][0..self.directory.len], self.directory.bytes());
        const body_len = 64 + self.directory.len;
        std.crypto.hash.sha2.Sha256.hash(buffer[0..body_len], buffer[body_len..][0..32], .{});
        return buffer[0 .. body_len + 32];
    }
    fn decode(raw: []const u8) !CleanupCursor {
        if (raw.len < 96 or raw.len > max_encoded or !std.mem.eql(u8, raw[0..4], "SGC1")) return error.OnlineSourceCorrupt;
        const len = std.mem.readInt(u16, raw[62..64], .little);
        if (raw.len != 96 + @as(usize, len)) return error.OnlineSourceCorrupt;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw[0 .. 64 + len], &digest, .{});
        if (!std.mem.eql(u8, &digest, raw[64 + @as(usize, len) ..])) return error.OnlineSourceCorrupt;
        var result: CleanupCursor = .{
            .namespace = raw[4..28].*,
            .pin = raw[28..60].*,
            .mode = std.enums.fromInt(Mode, raw[60]) orelse return error.OnlineSourceCorrupt,
            .tree = std.enums.fromInt(Tree, raw[61]) orelse return error.OnlineSourceCorrupt,
        };
        try gc.Cursor.validate(raw[64..][0..len]);
        @memcpy(result.directory.path[0..len], raw[64..][0..len]);
        result.directory.len = len;
        if (result.tree == .done and len != 0) return error.OnlineSourceCorrupt;
        return result;
    }
};

/// Local durability receipt, never part of the transferable certificate. The
/// complete artifact is verified before this checksum-protected record is
/// atomically renamed. Retries stat the immutable file instead of rehashing it.
const PublicationReceipt = struct {
    const certificate_size = snapshot.encoded_size;
    const body_size = certificate_size + 64;
    const encoded_size = body_size + 32;
    certificate: snapshot.Certificate,
    seal_digest: [32]u8,
    inode: u64,
    size: u64,
    mtime_ns: i128,
    fn encode(self: PublicationReceipt) ![encoded_size]u8 {
        var bytes: [encoded_size]u8 = undefined;
        bytes[0..certificate_size].* = try self.certificate.encode();
        bytes[certificate_size..][0..32].* = self.seal_digest;
        std.mem.writeInt(u64, bytes[certificate_size + 32 ..][0..8], self.inode, .little);
        std.mem.writeInt(u64, bytes[certificate_size + 40 ..][0..8], self.size, .little);
        std.mem.writeInt(i128, bytes[certificate_size + 48 ..][0..16], self.mtime_ns, .little);
        std.crypto.hash.sha2.Sha256.hash(bytes[0..body_size], bytes[body_size..][0..32], .{});
        return bytes;
    }
    fn decode(bytes: []const u8) !PublicationReceipt {
        if (bytes.len != encoded_size) return error.InvalidSourceSnapshot;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes[0..body_size], &digest, .{});
        if (!std.mem.eql(u8, &digest, bytes[body_size..])) return error.InvalidSourceSnapshot;
        return .{ .certificate = try snapshot.Certificate.decode(bytes[0..certificate_size]), .seal_digest = bytes[certificate_size..][0..32].*, .inode = std.mem.readInt(u64, bytes[certificate_size + 32 ..][0..8], .little), .size = std.mem.readInt(u64, bytes[certificate_size + 40 ..][0..8], .little), .mtime_ns = std.mem.readInt(i128, bytes[certificate_size + 48 ..][0..16], .little) };
    }
    fn verifyFile(self: PublicationReceipt, io: std.Io, path: []const u8, digest: [32]u8) !void {
        const stat = try backup.statRegularFile(io, path);
        if (!std.mem.eql(u8, &self.seal_digest, &digest) or self.inode != stat.inode or self.size != stat.size or self.mtime_ns != stat.mtime.toNanoseconds()) return error.BackupSealSourceChanged;
    }
};

pub const FailurePoint = enum { none, after_prepare, after_seal, before_cleanup };
pub var test_failure: FailurePoint = .none;
pub fn afterPrepare() !void {
    if (@import("builtin").is_test and test_failure == .after_prepare) return error.InjectedSourcePinFailure;
}
pub fn locate(db: *DB, scope: ledger.Scope) !ledger.Located {
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    return ledger.locate(&read, scope);
}
pub fn lockPath(alloc: Allocator, path: []const u8, slot: usize) ![]u8 {
    return std.fmt.allocPrint(alloc, "{s}.online-slot-{d}", .{ path, slot });
}
pub fn pathAlloc(alloc: Allocator, db_path: []const u8, scope: ledger.Scope) ![]u8 {
    try scope.validate();
    return pathForPin(alloc, db_path, scope.pin());
}
fn pathForPin(alloc: Allocator, db_path: []const u8, pin: [32]u8) ![]u8 {
    return std.fmt.allocPrint(alloc, "{s}.online.backup-pins/source-{s}", .{ db_path, std.fmt.bytesToHex(pin, .lower) });
}
fn exists(io: std.Io, path: []const u8) !bool {
    std.Io.Dir.cwd().access(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}
fn verifyInventory(alloc: Allocator, io: std.Io, root: []const u8, handle: seal.Handle, applied_index: u64, cancellation: Cancellation) !void {
    var opened = try seal.open(alloc, io, root, handle);
    defer opened.deinit();
    if (!std.mem.eql(u8, opened.parsed.value.primary.artifact_format, "antfly-lsm-checkpoint") or opened.parsed.value.projections.len != 0) return error.InvalidBackupSeal;
    if (opened.parsed.value.sequence != applied_index) return error.SourceSnapshotCutMismatch;
    for (opened.parsed.value.files) |file| {
        try cancellation.check();
        const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root, file.path });
        defer alloc.free(path);
        const stat = try backup.statRegularFile(io, path);
        if (stat.inode != file.inode or stat.size != file.size or stat.mtime.toNanoseconds() != file.mtime_ns) return error.BackupSealSourceChanged;
    }
}

/// Caller holds the DB apply lock. The normal path never exposes prepared
/// admission to another writer. Restart retries complete this same frozen cut.
pub fn ensureAssumeApply(db: *DB, scope: ledger.Scope) !void {
    const alloc = db.alloc;
    const io = db.backend_runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable;
    const progress = try db.onlineSourceStatus(scope);
    if (progress.phase == .released) return error.OnlineSourceScopeChanged;
    // A durable receipt is the admission ACK. Artifact validation belongs to
    // export; replay must not wait behind its full-table serialization lock.
    if (progress.snapshot_phase != .prepared) return;
    const lock_path = try lockPath(alloc, db.core.path, (try locate(db, scope)).slot);
    defer alloc.free(lock_path);
    var held = try seal.StoreLock.acquire(alloc, io, lock_path, .none);
    defer held.deinit();
    const root = try pathAlloc(alloc, db.core.path, scope);
    defer alloc.free(root);
    var handle: seal.Handle = undefined;
    if (try exists(io, root)) {
        handle = try seal.readHandle(alloc, io, root, scope.fence);
        if (progress.snapshot_phase != .prepared and !std.mem.eql(u8, &handle.digest, &progress.local_seal_digest)) return error.SourceSnapshotCutMismatch;
        try verifyInventory(alloc, io, root, handle, progress.admitted_applied_index, .none);
    } else {
        if (progress.snapshot_phase != .prepared) return error.OnlineSourcePinMissing;
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            const pending = try state.load(&read) orelse return error.OnlineSourceCorrupt;
            const retained = try @import("../retained_effects.zig").load(&read) orelse return error.OnlineSourceCorrupt;
            if (!std.mem.eql(u8, &pending.namespace, &scope.namespace()) or !std.mem.eql(u8, &pending.pin, &scope.pin()) or
                pending.applied_index != progress.admitted_applied_index or pending.retained_start != progress.start or retained.latest != progress.start) return error.SourceSnapshotCutMismatch;
        }
        const staging = try std.fmt.allocPrint(alloc, "{s}.staging", .{root});
        defer alloc.free(staging);
        // Abandoned work is paged even on writer-open. The durable prepared
        // fence remains in force and no marker is acknowledged while cleanup
        // yields. Never recursively delete a large tree under the apply lock.
        if (try exists(io, staging)) {
            var work = CleanupWork.init(io, .{});
            if (!try cleanupTrees(db, try locate(db, scope), .prepared_staging, &work)) return error.OnlineSourcePinPending;
        }
        try fs.createDirPathPortable(io, staging);
        // A failed seal leaves one exact attempt-owned staging tree, resumed
        // through the same bounded cleanup cursor on its next retry.
        try db.core.syncStore(true);
        var primary = try db.core.pinNativeSnapshot();
        defer primary.deinit();
        const primary_root = try std.fmt.allocPrint(alloc, "{s}/primary-lsm", .{staging});
        defer alloc.free(primary_root);
        switch (primary) {
            .lsm => |*checkpoint| _ = try checkpoint.seal(io, primary_root, .none),
            .logical => return error.BackupSealBackendUnsupported,
        }
        handle = try seal.finish(alloc, io, staging, scope.fence, progress.admitted_applied_index, .{ .artifact_format = primary.artifactFormat(), .artifact_version = primary.artifactVersion(), .source_backend = "lsm" }, &.{}, .none);
        try std.Io.Dir.rename(.cwd(), staging, .cwd(), root, io);
        try fs.syncDirPortable(io, std.fs.path.dirname(root).?);
    }
    if (@import("builtin").is_test and test_failure == .after_seal) return error.InjectedSourcePinFailure;
    // Reconfirm the complete publication path even on an after-rename retry.
    // The source-pin parent is a sibling of the DB and may itself be new.
    const pin_parent = std.fs.path.dirname(root).?;
    try fs.syncDirPortable(io, pin_parent);
    try fs.syncDirPortable(io, std.fs.path.dirname(pin_parent) orelse ".");
    var txn = try db.core.store.beginWriteTxn();
    var open = true;
    defer if (open) txn.abort();
    try ledger.stagePinned(&txn, scope, handle.digest);
    try txn.commit();
    open = false;
    try db.core.store.sync(true);
}

/// Serialize once, verify the complete artifact, fsync it, and only then return
/// a certificate suitable for the replicated publish_certificate command.
/// The caller keeps the owner leased; normal primary writes remain admitted.
/// Compact observation only: never exports, hashes the corpus, or recaptures
/// live rows. A busy exporter is pending, not a reason to block status polls.
pub fn publicationCertificateIfPresent(db: *DB, scope: ledger.Scope, cancellation: Cancellation) !?snapshot.Certificate {
    try cancellation.check();
    const found = try locate(db, scope);
    if (found.progress.phase == .released or found.progress.snapshot_phase == .prepared) return null;
    const alloc = db.alloc;
    const io = db.backend_runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable;
    const lock_path = try lockPath(alloc, db.core.path, found.slot);
    defer alloc.free(lock_path);
    var held = try tryCleanupLock(alloc, io, lock_path) orelse return null;
    defer held.deinit();
    const progress = try db.onlineSourceStatus(scope);
    if (progress.phase == .released or progress.snapshot_phase == .prepared) return null;
    const root = try pathAlloc(alloc, db.core.path, scope);
    defer alloc.free(root);
    if (try @import("source_artifact_transfer.zig").transferredCertificate(db, scope, root)) |certificate| return certificate;
    const receipt_path = try std.fmt.allocPrint(alloc, "{s}/source.certificate", .{root});
    defer alloc.free(receipt_path);
    if (!try exists(io, receipt_path)) return null;
    const raw = try backup.readFileAlloc(alloc, io, receipt_path, PublicationReceipt.encoded_size + 1);
    defer alloc.free(raw);
    const receipt = try PublicationReceipt.decode(raw);
    try verifyCut(scope, progress, receipt.certificate);
    const artifact_path = try std.fmt.allocPrint(alloc, "{s}/source.afb2", .{root});
    defer alloc.free(artifact_path);
    try receipt.verifyFile(io, artifact_path, progress.local_seal_digest);
    try cancellation.check();
    if ((try db.onlineSourceStatus(scope)).phase == .released) return null;
    return receipt.certificate;
}

pub fn preparePublication(db: *DB, scope: ledger.Scope, cancellation: Cancellation) !snapshot.Certificate {
    const alloc = db.alloc;
    const io = db.backend_runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable;
    try cancellation.check();
    const lock_path = try lockPath(alloc, db.core.path, (try locate(db, scope)).slot);
    defer alloc.free(lock_path);
    var held = try seal.StoreLock.acquire(alloc, io, lock_path, cancellation);
    defer {
        held.deinit();
        // Release may have committed while this immutable export ran. It never
        // waits for us in the Raft applier; finish its retained cleanup receipt
        // now, or leave the durable bounded job for writer-open reconciliation.
        reclaimReleased(db, scope) catch {};
    }
    const progress = try db.onlineSourceStatus(scope);
    if (progress.phase == .released) return error.OnlineSourceScopeChanged;
    if (progress.snapshot_phase == .prepared) return error.OnlineSourcePinPending;
    const root = try pathAlloc(alloc, db.core.path, scope);
    defer alloc.free(root);
    // A transferred artifact carries its own replica-local durability receipt.
    // Its authority is the replicated logical certificate, not another
    // replica's inode inventory or a recapture of this owner's current rows.
    if (try @import("source_artifact_transfer.zig").transferredCertificate(db, scope, root)) |certificate| return certificate;
    const handle: seal.Handle = .{ .fence = scope.fence, .digest = progress.local_seal_digest };
    const artifact_path = try std.fmt.allocPrint(alloc, "{s}/source.afb2", .{root});
    defer alloc.free(artifact_path);
    const receipt_path = try std.fmt.allocPrint(alloc, "{s}/source.certificate", .{root});
    defer alloc.free(receipt_path);
    if (try exists(io, receipt_path)) {
        const receipt = try backup.readFileAlloc(alloc, io, receipt_path, PublicationReceipt.encoded_size + 1);
        defer alloc.free(receipt);
        const publication = try PublicationReceipt.decode(receipt);
        try verifyCut(scope, progress, publication.certificate);
        try publication.verifyFile(io, artifact_path, progress.local_seal_digest);
        try cancellation.check();
        if ((try db.onlineSourceStatus(scope)).phase == .released) return error.OnlineSourceScopeChanged;
        return publication.certificate;
    }
    if (progress.snapshot_phase == .published) return error.OnlineSourcePinMissing;
    try verifyInventory(alloc, io, root, handle, progress.admitted_applied_index, cancellation);
    const primary_path = try std.fmt.allocPrint(alloc, "{s}/primary-lsm", .{root});
    defer alloc.free(primary_path);
    var backend = try @import("../lsm_backend.zig").Backend.open(alloc, primary_path, .{ .backend = .{ .read_only = true }, .read_runtime = @import("../lsm_backend/storage_io.zig").ReadRuntime.init(io) });
    defer backend.close();
    var store = try @import("../docstore.zig").DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{ .name = "docs" }));
    defer store.close();
    const file = try std.Io.Dir.cwd().createFile(io, artifact_path, .{ .read = true, .truncate = true });
    defer file.close(io);
    var buffer: [256 * 1024]u8 = undefined;
    var writer = file.writer(io, &buffer);
    var certificate: ?snapshot.Certificate = null;
    try portable.exportPortableToWriterWithOptions(alloc, &store, &writer.interface, .{ .cancellation = cancellation, .source_copy = .{ .scope = scope, .applied_index = progress.admitted_applied_index, .retained_start = progress.start }, .source_certificate = .{ .cut = .{ .namespace = scope.fence.namespace, .applied_index = progress.admitted_applied_index, .retained_start = progress.start }, .output = &certificate } });
    try writer.end();
    try file.sync(io);
    const result = certificate orelse return error.InvalidSourceSnapshot;
    try verifyCut(scope, progress, result);
    try portable.verifySourceCertificateFile(alloc, io, file, (try file.stat(io)).size, result, cancellation);
    try cancellation.check();
    const receipt_staging = try std.fmt.allocPrint(alloc, "{s}.staging", .{receipt_path});
    defer alloc.free(receipt_staging);
    const artifact_stat = try file.stat(io);
    const publication: PublicationReceipt = .{ .certificate = result, .seal_digest = progress.local_seal_digest, .inode = artifact_stat.inode, .size = artifact_stat.size, .mtime_ns = artifact_stat.mtime.toNanoseconds() };
    _ = try backup.writeFileDurable(io, receipt_staging, &try publication.encode());
    try std.Io.Dir.rename(.cwd(), receipt_staging, .cwd(), receipt_path, io);
    try fs.syncDirPortable(io, root);
    if ((try db.onlineSourceStatus(scope)).phase == .released) return error.OnlineSourceScopeChanged;
    return result;
}
fn verifyCut(scope: ledger.Scope, progress: ledger.Progress, certificate: snapshot.Certificate) !void {
    if (!certificate.cut.namespace.eql(scope.fence.namespace) or certificate.cut.applied_index != progress.admitted_applied_index or certificate.cut.retained_start != progress.start) return error.SourceSnapshotCutMismatch;
    if (progress.snapshot_phase == .published and !std.mem.eql(u8, &progress.snapshot_certificate, &try certificate.digest())) return error.SourceSnapshotCutMismatch;
}

/// Run outside the DB apply fence. A concurrent artifact export may finish
/// before cleanup acquires its file lock, but cannot publish after release's
/// durable source-ledger decision. Its slot remains reserved until local
/// cleanup is durable, so crashes cannot orphan a pin through slot reuse.
pub fn reclaimReleased(db: *DB, scope: ledger.Scope) !void {
    const found = locate(db, scope) catch |err| switch (err) {
        error.OnlineSourceScopeChanged => return,
        else => return err,
    };
    if (found.progress.phase != .released) return error.OnlineSourceScopeChanged;
    if (found.progress.local_cleanup_complete) return;
    _ = db.source_pin_gc_epoch.fetchAdd(1, .acq_rel);
    cleanupLocated(db, found) catch |err| {
        db.recordSourcePinCleanupFailure(err);
        return err;
    };
}

/// Startup and admission each perform one time-sliced page, never drain a tree.
pub fn reconcileReleased(db: *DB) !void {
    _ = reconcileReleasedWithBudget(db, .{}) catch |err| {
        db.recordSourcePinCleanupFailure(err);
        return;
    };
}

pub fn reconcileReleasedWithBudget(db: *DB, budget: CleanupBudget) !CleanupWork {
    const io = db.backend_runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable;
    var work = CleanupWork.init(io, budget);
    const epoch = db.source_pin_gc_epoch.load(.acquire);
    if (epoch == 0) return work;
    // Preserve useful work even when a different slot reports an error: the
    // shared maintenance scheduler must not back off healthy cleanup pages.
    defer if (work.completed_units != 0) {
        _ = db.source_pin_gc_work_units.fetchAdd(@intCast(work.completed_units), .acq_rel);
    };
    // The common document-only owner pays one bounded read-only startup scan,
    // not directory creation, locking, cursor writes or per-mutation probes.
    if (budget.max_entries >= 16 and budget.max_metadata_bytes >= 16 * 512) {
        var read = try db.core.store.beginReadTxn();
        var pending = false;
        for (0..16) |slot| {
            if (!work.charge(io, 512)) {
                pending = true;
                break;
            }
            const candidate = ledger.pendingCleanupAt(&read, slot) catch {
                pending = true; // The fair pass records and isolates errors.
                break;
            };
            if (candidate != null) {
                pending = true;
                break;
            }
        }
        read.abort();
        if (!pending) {
            clearCleanupDebt(db, epoch);
            return work;
        }
    }
    const scheduler_path = try std.fmt.allocPrint(db.alloc, "{s}.online-gc", .{db.core.path});
    defer db.alloc.free(scheduler_path);
    var scheduling = try tryCleanupLock(db.alloc, io, scheduler_path) orelse return work;
    defer scheduling.deinit();
    const cursor_path = try std.fmt.allocPrint(db.alloc, "{s}.backup-pins/.next", .{scheduler_path});
    defer db.alloc.free(cursor_path);
    const raw = backup.readFileAlloc(db.alloc, io, cursor_path, 34) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
    defer if (raw) |bytes| db.alloc.free(bytes);
    var start: usize = 0;
    if (raw) |bytes| {
        var digest: [32]u8 = undefined;
        if (bytes.len != 33 or bytes[0] >= 16) return error.OnlineSourceCorrupt;
        std.crypto.hash.sha2.Sha256.hash(bytes[0..1], &digest, .{});
        if (!std.mem.eql(u8, &digest, bytes[1..])) return error.OnlineSourceCorrupt;
        start = bytes[0];
    }
    var visited: usize = 0;
    var pending_seen = false;
    var first_error: ?anyerror = null;
    for (0..@import("../retained_effects.zig").max_consumers) |offset| {
        if (!work.charge(io, 512)) break;
        const slot = (start + offset) % @import("../retained_effects.zig").max_consumers;
        visited += 1;
        const pending = blk: {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            break :blk ledger.pendingCleanupAt(&read, slot) catch |err| {
                pending_seen = true;
                if (first_error == null) first_error = err;
                continue;
            };
        } orelse continue;
        pending_seen = true;
        cleanupLocatedWithBudget(db, pending, &work) catch |err| {
            if (first_error == null) first_error = err;
        };
    }
    if (visited != 0 and (visited != 16 or pending_seen)) {
        var next: [33]u8 = undefined;
        next[0] = @intCast((start + visited) % @import("../retained_effects.zig").max_consumers);
        std.crypto.hash.sha2.Sha256.hash(next[0..1], next[1..33], .{});
        try writeAtomicReceipt(db.alloc, io, cursor_path, &next);
    }
    if (visited == 16 and !pending_seen) clearCleanupDebt(db, epoch);
    if (first_error) |err| return err;
    return work;
}

fn clearCleanupDebt(db: *DB, epoch: u64) void {
    if (db.source_pin_gc_epoch.cmpxchgStrong(epoch, 0, .acq_rel, .acquire) != null) return;
    db.source_pin_gc_error.store(0, .release);
    db.source_pin_gc_failure_streak.store(0, .release);
    db.source_pin_gc_next_ns.store(0, .release);
}

fn cleanupLocated(db: *DB, found: ledger.Located) !void {
    var work = CleanupWork.init(db.backend_runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable, .{});
    try cleanupLocatedWithBudget(db, found, &work);
}

fn cleanupLocatedWithBudget(db: *DB, found: ledger.Located, work: *CleanupWork) !void {
    if (@import("builtin").is_test and test_failure == .before_cleanup) return error.InjectedSourcePinFailure;
    const alloc = db.alloc;
    const io = db.backend_runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable;
    const lock_path = try lockPath(alloc, db.core.path, found.slot);
    defer alloc.free(lock_path);
    var held = try tryCleanupLock(alloc, io, lock_path) orelse return;
    defer held.deinit();
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const current = try ledger.pendingCleanupAt(&read, found.slot) orelse return;
        if (!std.mem.eql(u8, &current.progress.pin, &found.progress.pin) or !std.mem.eql(u8, &current.progress.namespace, &found.progress.namespace)) return;
    }
    if (!try cleanupTrees(db, found, .released_all, work)) return;
    var txn = try db.core.store.beginWriteTxn();
    var open = true;
    defer if (open) txn.abort();
    try ledger.stageCleanupComplete(&txn, found);
    try txn.commit();
    open = false;
    try db.core.store.sync(true);
}

fn writeAtomicReceipt(alloc: Allocator, io: std.Io, path: []const u8, bytes: []const u8) !void {
    const staging = try std.fmt.allocPrint(alloc, "{s}.staging", .{path});
    defer alloc.free(staging);
    _ = try backup.writeFileDurable(io, staging, bytes);
    try std.Io.Dir.rename(.cwd(), staging, .cwd(), path, io);
    try fs.syncDirPortable(io, std.fs.path.dirname(path).?);
}

/// Caller holds the exact slot lock. Cursor files are bounded one-per-slot,
/// outside the primary DB, so abandoned prepared cuts need not advance any
/// applied marker merely to persist local filesystem cleanup progress.
fn cleanupTrees(db: *DB, found: ledger.Located, mode: CleanupCursor.Mode, work: *CleanupWork) !bool {
    const alloc = db.alloc;
    const io = db.backend_runtime.filesystemIo().?;
    // Reserve the maximum cursor-write/durability epilogue before touching
    // files. It is required even when the elapsed budget expires mid-page.
    if (!work.charge(io, CleanupCursor.max_encoded * 2 + 1024)) return false;
    const lock_path = try lockPath(alloc, db.core.path, found.slot);
    defer alloc.free(lock_path);
    const receipt = try std.fmt.allocPrint(alloc, "{s}.backup-pins/.gc-progress", .{lock_path});
    defer alloc.free(receipt);
    var cursor: CleanupCursor = .{ .namespace = found.progress.namespace, .pin = found.progress.pin, .mode = mode, .tree = if (mode == .prepared_staging) .staging else .root };
    const raw = backup.readFileAlloc(alloc, io, receipt, CleanupCursor.max_encoded + 1) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
    defer if (raw) |bytes| alloc.free(bytes);
    if (raw) |bytes| {
        const prior = try CleanupCursor.decode(bytes);
        if (!std.mem.eql(u8, &prior.namespace, &cursor.namespace) or !std.mem.eql(u8, &prior.pin, &cursor.pin)) return error.OnlineSourceCorrupt;
        if (prior.mode == mode) cursor = prior;
    }
    const root = try pathForPin(alloc, db.core.path, found.progress.pin);
    defer alloc.free(root);
    while (cursor.tree != .done and !work.exhausted(io)) {
        const path = if (cursor.tree == .root) try alloc.dupe(u8, root) else try std.fmt.allocPrint(alloc, "{s}.staging", .{root});
        defer alloc.free(path);
        if (!try gc.advance(alloc, io, path, &cursor.directory, work)) break;
        cursor.directory = .{};
        cursor.tree = if (cursor.tree == .root) .staging else .done;
    }
    if (cursor.tree == .done) {
        std.Io.Dir.cwd().deleteFile(io, receipt) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
        const receipt_staging = try std.fmt.allocPrint(alloc, "{s}.staging", .{receipt});
        defer alloc.free(receipt_staging);
        std.Io.Dir.cwd().deleteFile(io, receipt_staging) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
        try fs.syncDirPortable(io, std.fs.path.dirname(receipt).?);
        return true;
    }
    var buffer: [CleanupCursor.max_encoded]u8 = undefined;
    try writeAtomicReceipt(alloc, io, receipt, cursor.encode(&buffer));
    return false;
}

fn tryCleanupLock(alloc: Allocator, io: std.Io, lock_path: []const u8) !?seal.StoreLock {
    const parent = try std.fmt.allocPrint(alloc, "{s}.backup-pins", .{lock_path});
    defer alloc.free(parent);
    try fs.createDirPathPortable(io, parent);
    const path = try std.fmt.allocPrint(alloc, "{s}/.lock", .{parent});
    defer alloc.free(path);
    const file = try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = false });
    errdefer file.close(io);
    if (!try file.tryLock(io, .exclusive)) {
        file.close(io);
        return null;
    }
    return .{ .file = file, .io = io };
}

test "relational index system source pin cancellation releases a prepared cut before files exist" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/cancel-source", .{tmp.sub_path});
    defer alloc.free(path);
    const options: @import("antfly_source_root").antfly_sources.physical_db.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var db = try DB.open(alloc, path, options);
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    const owner = try db.relationalTopologyIdentity();
    const scope: ledger.Scope = .{ .fence = .{ .role = .merge_source, .transition_id = 78, .attempt = 1, .admission_epoch = owner.next_epoch, .peer_group_id = 3, .owner_group_id = 2, .namespace = owner.namespace, .catalog_digest = owner.catalog_digest }, .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 }, .consumer_epoch = 1, .copy_attempt = .{ .donor_term = 1, .sequence = 1 } };
    test_failure = .after_prepare;
    defer test_failure = .none;
    try std.testing.expectError(error.InjectedSourcePinFailure, db.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 1, .index = 1 }));
    test_failure = .none;
    try db.batchRaftReplicatedApply(.{ .online_source = .{ .release = scope } }, .{ .term = 1, .index = 2 });
    try std.testing.expectEqual(.released, (try db.onlineSourceStatus(scope)).phase);
    try db.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 1, .index = 1 });
    try db.batchRaftReplicatedApply(.{ .timestamp_ns = 1, .writes = &.{.{ .key = "a", .value = "{\"ok\":true}" }} }, .{ .term = 1, .index = 3 });
    db.close();
    db = try DB.open(alloc, path, options);
    try std.testing.expectEqual(.released, (try db.onlineSourceStatus(scope)).phase);
}

test "relational index system source pin interrupted cleanup reserves slot across later admission and restart" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/cleanup-source", .{tmp.sub_path});
    defer alloc.free(path);
    const options: @import("antfly_source_root").antfly_sources.physical_db.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var db = try DB.open(alloc, path, options);
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    const owner = try db.relationalTopologyIdentity();
    const scope: ledger.Scope = .{ .fence = .{ .role = .merge_source, .transition_id = 79, .attempt = 1, .admission_epoch = owner.next_epoch, .peer_group_id = 3, .owner_group_id = 2, .namespace = owner.namespace, .catalog_digest = owner.catalog_digest }, .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 }, .consumer_epoch = 1, .copy_attempt = .{ .donor_term = 1, .sequence = 1 } };
    try db.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 1, .index = 1 });
    const old_location = try locate(&db, scope);
    const old_slot = old_location.slot;
    const old_root = try pathAlloc(alloc, path, scope);
    defer alloc.free(old_root);
    const garbage = try std.fmt.allocPrint(alloc, "{s}/abandoned/deep", .{old_root});
    defer alloc.free(garbage);
    try fs.createDirPathPortable(std.testing.io, garbage);
    var garbage_dir = try std.Io.Dir.cwd().openDir(std.testing.io, garbage, .{});
    for (0..500) |i| {
        var name: [32]u8 = undefined;
        const file = try garbage_dir.createFile(std.testing.io, try std.fmt.bufPrint(&name, "{d}.part", .{i}), .{});
        file.close(std.testing.io);
    }
    garbage_dir.close(std.testing.io);
    test_failure = .before_cleanup;
    defer test_failure = .none;
    try std.testing.expectError(error.InjectedSourcePinFailure, db.batchRaftReplicatedApply(.{ .online_source = .{ .release = scope } }, .{ .term = 1, .index = 2 }));
    try std.testing.expect(try exists(std.testing.io, old_root));
    var next = scope;
    next.consumer_epoch = 2;
    next.copy_attempt.sequence = 2;
    test_failure = .none;
    {
        const export_lock_path = try lockPath(alloc, path, old_slot);
        defer alloc.free(export_lock_path);
        var exporting = try seal.StoreLock.acquire(alloc, std.testing.io, export_lock_path, .none);
        defer exporting.deinit();
        // Admission's fair cleanup pass skips an exporting slot without
        // blocking the apply fence, and cannot erase its cleanup ownership.
        try db.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = next } } }, .{ .term = 1, .index = 3 });
        try std.testing.expect(old_slot != (try locate(&db, next)).slot);
        try std.testing.expect(try exists(std.testing.io, old_root));
        const next_slot = (try locate(&db, next)).slot;
        test_failure = .before_cleanup;
        try std.testing.expectError(error.InjectedSourcePinFailure, db.batchRaftReplicatedApply(.{ .online_source = .{ .release = next } }, .{ .term = 1, .index = 4 }));
        test_failure = .none;
        try drainCleanupForTest(&db, next);
        var third = scope;
        third.consumer_epoch = 3;
        third.copy_attempt.sequence = 3;
        try db.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = third } } }, .{ .term = 1, .index = 5 });
        // Busy first slot cannot starve a later ready cleanup.
        try std.testing.expectEqual(next_slot, (try locate(&db, third)).slot);
    }
    const old_lock_path = try lockPath(alloc, path, old_slot);
    defer alloc.free(old_lock_path);
    const durable_cursor = try std.fmt.allocPrint(alloc, "{s}.backup-pins/.gc-progress", .{old_lock_path});
    defer alloc.free(durable_cursor);
    for (0..16) |_| {
        _ = try reconcileReleasedWithBudget(&db, .{ .max_entries = 12, .max_metadata_bytes = 64 * 1024, .max_duration_ns = std.time.ns_per_s });
        if (try exists(std.testing.io, durable_cursor)) break;
    }
    try std.testing.expect(try exists(std.testing.io, durable_cursor));
    try std.testing.expect(!(try db.onlineSourceStatus(scope)).local_cleanup_complete);
    const valid_cursor = try backup.readFileAlloc(alloc, std.testing.io, durable_cursor, CleanupCursor.max_encoded);
    defer alloc.free(valid_cursor);
    _ = try backup.writeFileDurable(std.testing.io, durable_cursor, "corrupt cursor");
    var healthy = scope;
    healthy.consumer_epoch = 3;
    healthy.copy_attempt.sequence = 3;
    const healthy_root = try pathAlloc(alloc, path, healthy);
    defer alloc.free(healthy_root);
    var healthy_dir = try std.Io.Dir.cwd().openDir(std.testing.io, healthy_root, .{});
    for (0..200) |i| {
        var name: [32]u8 = undefined;
        const file = try healthy_dir.createFile(std.testing.io, try std.fmt.bufPrint(&name, "{d}.pending", .{i}), .{});
        file.close(std.testing.io);
    }
    healthy_dir.close(std.testing.io);
    test_failure = .before_cleanup;
    try std.testing.expectError(error.InjectedSourcePinFailure, db.batchRaftReplicatedApply(.{ .online_source = .{ .release = healthy } }, .{ .term = 1, .index = 6 }));
    test_failure = .none;
    db.close();
    db = try DB.open(alloc, path, options);
    // A released cursor failure is visible maintenance debt, not an outage
    // for unrelated documents, and cannot starve later healthy slots.
    for (0..16) |_| {
        try reconcileReleased(&db);
        if (db.sourcePinCleanupStatus().failures != 0) break;
    }
    try std.testing.expectEqualStrings("OnlineSourceCorrupt", db.sourcePinCleanupStatus().last_error.?);
    db.source_pin_gc_next_ns.store(0, .release);
    const units_before = db.source_pin_gc_work_units.load(.acquire);
    try std.testing.expect(try db.runSourcePinCleanupStep());
    try std.testing.expect(db.source_pin_gc_work_units.load(.acquire) > units_before);
    // Useful later-slot work keeps the next pass prompt despite the earlier
    // damaged cursor, while zero-progress errors retain exponential backoff.
    try std.testing.expect(db.sourcePinCleanupStatus().next_attempt_ns <= @import("antfly_platform").time.monotonicNs() + 2 * std.time.ns_per_ms);
    try db.batchRaftReplicatedApply(.{ .timestamp_ns = 7, .writes = &.{.{ .key = "healthy", .value = "{\"ok\":true}" }} }, .{ .term = 1, .index = 7 });
    for (0..100) |_| {
        attempt: {
            _ = reconcileReleasedWithBudget(&db, .{ .max_entries = 128, .max_metadata_bytes = 128 * 1024, .max_duration_ns = std.time.ns_per_s }) catch |err| {
                try std.testing.expectEqual(error.OnlineSourceCorrupt, err);
                break :attempt;
            };
        }
        if ((try db.onlineSourceStatus(healthy)).local_cleanup_complete) break;
    }
    try std.testing.expect((try db.onlineSourceStatus(healthy)).local_cleanup_complete);
    try std.testing.expect(!(try db.onlineSourceStatus(scope)).local_cleanup_complete);
    _ = try backup.writeFileDurable(std.testing.io, durable_cursor, valid_cursor);
    try drainCleanupForTest(&db, scope);
    try std.testing.expect(!try exists(std.testing.io, old_root));
    try std.testing.expect((try db.onlineSourceStatus(scope)).local_cleanup_complete);
    try std.testing.expect(!try exists(std.testing.io, durable_cursor));
    var last = scope;
    last.consumer_epoch = 4;
    last.copy_attempt.sequence = 4;
    try db.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = last } } }, .{ .term = 1, .index = 8 });
    try std.testing.expectEqual(old_slot, (try locate(&db, last)).slot);
    try cleanupLocated(&db, old_location);
    try db.batchRaftReplicatedApply(.{ .online_source = .{ .release = scope } }, .{ .term = 1, .index = 2 });
    try std.testing.expectEqual(.pinned, (try db.onlineSourceStatus(last)).snapshot_phase);
}

fn drainCleanupForTest(db: *DB, scope: ledger.Scope) !void {
    for (0..1000) |_| {
        if ((try db.onlineSourceStatus(scope)).local_cleanup_complete) return;
        _ = try reconcileReleasedWithBudget(db, .{ .max_entries = 128, .max_metadata_bytes = 128 * 1024, .max_duration_ns = std.time.ns_per_s });
    }
    return error.TestUnexpectedResult;
}

test "relational index system source pin GC cursor binds scope and validates restart paths" {
    var cursor: CleanupCursor = .{ .namespace = @splat(1), .pin = @splat(2), .mode = .released_all, .tree = .staging };
    const nested = "primary-lsm/runs";
    @memcpy(cursor.directory.path[0..nested.len], nested);
    cursor.directory.len = nested.len;
    var buffer: [CleanupCursor.max_encoded]u8 = undefined;
    const encoded = cursor.encode(&buffer);
    const decoded = try CleanupCursor.decode(encoded);
    try std.testing.expectEqualStrings(nested, decoded.directory.bytes());
    try std.testing.expectEqualSlices(u8, &cursor.namespace, &decoded.namespace);
    try std.testing.expectEqualSlices(u8, &cursor.pin, &decoded.pin);
    for (0..encoded.len) |i| {
        buffer[i] ^= 1;
        try std.testing.expectError(error.OnlineSourceCorrupt, CleanupCursor.decode(encoded));
        buffer[i] ^= 1;
    }
    @memcpy(cursor.directory.path[0..3], "../");
    cursor.directory.len = 3;
    try std.testing.expectError(error.OnlineSourceCorrupt, CleanupCursor.decode(cursor.encode(&buffer)));
}

test "relational index system source pin prepared reopen pages abandoned staging before acknowledging cut" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/prepared-gc", .{tmp.sub_path});
    defer alloc.free(path);
    const options: @import("antfly_source_root").antfly_sources.physical_db.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var scope: ledger.Scope = undefined;
    {
        var db = try DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc, "{}");
        const owner = try db.relationalTopologyIdentity();
        scope = .{ .fence = .{ .role = .merge_source, .transition_id = 89, .attempt = 1, .admission_epoch = owner.next_epoch, .peer_group_id = 3, .owner_group_id = 2, .namespace = owner.namespace, .catalog_digest = owner.catalog_digest }, .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 }, .consumer_epoch = 1, .copy_attempt = .{ .donor_term = 1, .sequence = 1 } };
        test_failure = .after_prepare;
        defer test_failure = .none;
        try std.testing.expectError(error.InjectedSourcePinFailure, db.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 1, .index = 1 }));
        const root = try pathAlloc(alloc, path, scope);
        defer alloc.free(root);
        const abandoned = try std.fmt.allocPrint(alloc, "{s}.staging/abandoned", .{root});
        defer alloc.free(abandoned);
        try fs.createDirPathPortable(std.testing.io, abandoned);
        var dir = try std.Io.Dir.cwd().openDir(std.testing.io, abandoned, .{});
        defer dir.close(std.testing.io);
        for (0..200) |i| {
            var name: [32]u8 = undefined;
            const file = try dir.createFile(std.testing.io, try std.fmt.bufPrint(&name, "{d}.part", .{i}), .{});
            file.close(std.testing.io);
        }
    }
    var retries: usize = 0;
    var db = while (retries < 1000) {
        const opened = DB.open(alloc, path, options) catch |err| switch (err) {
            error.OnlineSourcePinPending => {
                retries += 1;
                continue;
            },
            else => return err,
        };
        break opened;
    } else return error.TestUnexpectedResult;
    defer db.close();
    try std.testing.expect(retries > 1);
    const progress = try db.onlineSourceStatus(scope);
    try std.testing.expectEqual(.pinned, progress.snapshot_phase);
    try std.testing.expectEqual(@as(u64, 1), progress.admitted_applied_index);
    try db.batchRaftReplicatedApply(.{ .timestamp_ns = 2, .writes = &.{.{ .key = "after", .value = "{\"ok\":true}" }} }, .{ .term = 1, .index = 2 });
}

test "relational index system source pin prepared crash blocks markers then reopens exact immutable artifact" {
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const schema = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}";
    const changed = "{\"version\":2,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}";
    for ([_]FailurePoint{ .after_prepare, .after_seal }, 0..) |point, trial| {
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/source-{d}", .{ tmp.sub_path, trial });
        defer alloc.free(path);
        const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
        var scope: ledger.Scope = undefined;
        {
            var db = try db_mod.DB.open(alloc, path, options);
            defer db.close();
            try db.setSchemaJson(alloc, schema);
            try db.batchRaftReplicatedApply(.{ .timestamp_ns = 111, .writes = &.{.{ .key = "a", .value = "{\"id\":1}" }} }, .{ .term = 1, .index = 1 });
            const owner = try db.relationalTopologyIdentity();
            scope = .{ .fence = .{ .role = .merge_source, .transition_id = 77, .attempt = 1, .admission_epoch = owner.next_epoch, .peer_group_id = 3, .owner_group_id = 2, .namespace = owner.namespace, .catalog_digest = owner.catalog_digest }, .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 }, .consumer_epoch = 1, .copy_attempt = .{ .donor_term = 1, .sequence = 1 } };
            test_failure = point;
            defer test_failure = .none;
            try std.testing.expectError(error.InjectedSourcePinFailure, db.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 1, .index = 2 }));
            try std.testing.expectEqual(.prepared, (try db.onlineSourceStatus(scope)).snapshot_phase);
            try std.testing.expectError(error.OnlineSourcePinPending, db.batchRaftReplicatedApply(.{ .transaction = .{ .prepare = .{ .txn_id = @splat(4), .topology_epoch = 1 } } }, .{ .term = 1, .index = 3 }));
            try std.testing.expectError(error.OnlineSourcePinPending, db.batchRaftReplicatedApply(.{}, .{ .term = 1, .index = 3 }));
            // Even direct transaction metadata cannot bypass the prepared gate.
            {
                var txn = try db.core.store.beginWriteTxn();
                defer txn.abort();
                try std.testing.expectError(error.OnlineSourcePinPending, txn.put("\x00\x00__metadata__:unrelated", "metadata"));
            }
            try db.setSchemaJson(alloc, schema);
            try std.testing.expectError(error.OnlineSourcePinPending, db.setSchemaJson(alloc, changed));
        }
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try std.testing.expectEqual(.pinned, (try db.onlineSourceStatus(scope)).snapshot_phase);
        try db.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 1, .index = 2 });
        try db.setSchemaJson(alloc, schema);
        try std.testing.expectError(error.IntegrityTopologyBusy, db.setSchemaJson(alloc, changed));
        try db.batchRaftReplicatedApply(.{ .timestamp_ns = 222, .writes = &.{.{ .key = "a", .value = "{\"id\":2}" }} }, .{ .term = 1, .index = 3 });
        var canceled = std.atomic.Value(bool).init(true);
        try std.testing.expectError(error.Canceled, db.prepareOnlineSourcePublication(scope, Cancellation.fromAtomic(&canceled)));
        const root = try pathAlloc(alloc, path, scope);
        defer alloc.free(root);
        const receipt_path = try std.fmt.allocPrint(alloc, "{s}/source.certificate", .{root});
        defer alloc.free(receipt_path);
        try std.testing.expect(!try exists(std.testing.io, receipt_path));
        try std.testing.expect(try publicationCertificateIfPresent(&db, scope, .none) == null);
        // A crash during receipt creation leaves only unpublished staging;
        // the next attempt safely reserializes the same immutable cut.
        const partial_receipt = try std.fmt.allocPrint(alloc, "{s}.staging", .{receipt_path});
        defer alloc.free(partial_receipt);
        _ = try backup.writeFileDurable(std.testing.io, partial_receipt, "partial");
        const certificate = try db.prepareOnlineSourcePublication(scope, .none);
        try std.testing.expect(certificate.eql((try publicationCertificateIfPresent(&db, scope, .none)).?));
        {
            const observing_lock = try lockPath(alloc, path, (try locate(&db, scope)).slot);
            defer alloc.free(observing_lock);
            var exporting = try seal.StoreLock.acquire(alloc, std.testing.io, observing_lock, .none);
            defer exporting.deinit();
            try std.testing.expect(try publicationCertificateIfPresent(&db, scope, .none) == null);
        }
        try std.testing.expectEqual(@as(u64, 2), certificate.cut.applied_index);
        const proof: portable.SourceCopyProof = .{ .scope = scope, .applied_index = 2, .retained_start = certificate.cut.retained_start };
        const artifact = try std.fmt.allocPrint(alloc, "{s}/source.afb2", .{root});
        defer alloc.free(artifact);
        const bytes = try backup.readFileAlloc(alloc, std.testing.io, artifact, 8 * 1024 * 1024);
        defer alloc.free(bytes);
        const decoder_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/decoder-{d}", .{ tmp.sub_path, trial });
        defer alloc.free(decoder_path);
        var decoder = try db_mod.DB.open(alloc, decoder_path, .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
        defer decoder.close();
        try std.testing.expectError(error.SourceCopyRestoreUnsupported, portable.importPortableWithOptions(alloc, decoder.core.store, bytes, .{}));
        try portable.importPortableWithOptions(alloc, decoder.core.store, bytes, .{ .source_copy = proof, .unpublished_staging = true });
        try portable.validateCompleteSourceCopyImage(alloc, decoder.core.store, proof);
        try std.testing.expectError(error.SourceCopyRestoreUnsupported, portable.validateCompleteDatabaseImageAlloc(alloc, decoder.core.store));
        const primary_key = try @import("../internal_keys.zig").relationalRowKeyAlloc(alloc, "a");
        defer alloc.free(primary_key);
        const pinned_primary = try decoder.core.store.get(alloc, primary_key);
        defer alloc.free(pinned_primary);
        const live_primary = try db.core.store.get(alloc, primary_key);
        defer alloc.free(live_primary);
        try std.testing.expect(!std.mem.eql(u8, pinned_primary, live_primary));
        // Distinct later live writes cannot alter the certified artifact.
        try std.testing.expect(certificate.eql(try db.prepareOnlineSourcePublication(scope, .none)));
        try db.batchRaftReplicatedApply(.{ .online_source = .{ .publish_certificate = .{ .scope = scope, .certificate = certificate } } }, .{ .term = 1, .index = 4 });
        try std.testing.expectEqual(.published, (try db.onlineSourceStatus(scope)).snapshot_phase);
        _ = try backup.writeFileDurable(std.testing.io, artifact, "corrupt");
        try std.testing.expectError(error.BackupSealSourceChanged, publicationCertificateIfPresent(&db, scope, .none));
        try std.testing.expectError(error.BackupSealSourceChanged, db.prepareOnlineSourcePublication(scope, .none));
        try std.Io.Dir.cwd().deleteFile(std.testing.io, artifact);
        try std.testing.expectError(error.FileNotFound, db.prepareOnlineSourcePublication(scope, .none));
        // A missing published artifact is not recaptured from the later row.
        try std.testing.expect(!try exists(std.testing.io, artifact));
        try db.batchRaftReplicatedApply(.{ .online_source = .{ .release = scope } }, .{ .term = 1, .index = 5 });
        // An old applied admission never resurrects a released pin.
        try db.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 1, .index = 2 });
        try db.setSchemaJson(alloc, changed);
        try std.testing.expectError(error.OnlineSourceScopeChanged, db.prepareOnlineSourcePublication(scope, .none));
    }
}
