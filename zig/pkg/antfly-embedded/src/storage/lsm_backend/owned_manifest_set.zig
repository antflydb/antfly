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

//! Durable checkpoint, successor journal and descriptor on one retained grant.
const std = @import("std");
const checkpoint = @import("manifest_checkpoint.zig");
const manifests = @import("manifest_set.zig");
const storage_io = @import("storage_io.zig");
const File = @import("owned_atomic_file.zig").File;
const admission = @import("../../common/read_admission.zig");

/// The caller prepays memoryBound(), fences unrelated publication and pins the
/// source through retirement. A caller that already rotated the live journal
/// sets preserve_journal before driving; that suffix must never be overwritten. Existing parent directories and valid output metadata are
/// required. No original execution or allocator callback is retained.
///
/// Keep this owner address stable while physical IO borrows its command. A
/// successful resume is the receipt authorizing backend generation installation.
/// On any failure retain all attempted final paths for cleanup, and fence the
/// backend for recovery if the descriptor may have been published. Abort never
/// deletes a final path or invents a new completion budget.
pub fn Publication(comptime Source: type) type {
    return struct {
        pub const Phase = enum { checkpoint, segment, descriptor, complete };
        file: File,
        encoder: checkpoint.Encoder(Source),
        paths: [3][std.fs.max_path_bytes]u8,
        path_lens: [3]usize,
        segment: [manifests.header_len]u8,
        descriptor: [36]u8,
        phase: Phase = .checkpoint,
        fixed_written: bool = false,
        preserve_journal: bool = false,
        attempted: [3]bool = @splat(false),
        published: [3]bool = @splat(false),
        aborting: bool = false,
        retired: bool = false,

        const Self = @This();
        pub fn memoryBound(storage: storage_io.Storage) !u64 {
            return std.math.add(u64, @sizeOf(Self), try storage.appendResourceBytes(0)) catch error.ResourceBudgetExceeded;
        }
        /// Transfers the provider grant only on success. No IO or allocation.
        pub fn initInPlace(self: *Self, resources: storage_io.DurabilityResources, root: []const u8, id: u64, sequence: u64, next_run_id: u64, source: Source, limit: usize) !void {
            if (id == 0) return error.InvalidManifest;
            const first_sequence = std.math.add(u64, sequence, 1) catch return error.InvalidManifest;
            const encoder = try checkpoint.Encoder(Source).init(source, sequence, next_run_id, limit);
            var paths: [3][std.fs.max_path_bytes]u8 = undefined;
            var lens: [3]usize = undefined;
            lens[0] = (std.fmt.bufPrint(&paths[0], "{s}/manifest-{d}.checkpoint", .{ root, id }) catch return error.NameTooLong).len;
            lens[1] = (std.fmt.bufPrint(&paths[1], "{s}/manifest-{d}.journal", .{ root, id }) catch return error.NameTooLong).len;
            lens[2] = (std.fmt.bufPrint(&paths[2], "{s}/manifest.bin", .{root}) catch return error.NameTooLong).len;
            // Every later File.init must fit even with a 20-digit nonce. All
            // validation precedes consuming the grant or starting publication.
            for (lens) |len| if (len + 32 >= std.fs.max_path_bytes) return error.NameTooLong;
            const file = try File.init(resources, paths[0][0..lens[0]]);
            self.* = .{ .file = file, .encoder = encoder, .paths = paths, .path_lens = lens, .segment = manifests.segmentHeader(id, first_sequence), .descriptor = (manifests.Descriptor{ .checkpoint = id, .first_segment = id, .sequence = sequence }).encode() };
        }

        fn finishCurrent(self: *Self, original: admission.Port) !void {
            const index = @backingInt(self.phase);
            self.file.finishWithAdmission(original) catch |cause| {
                self.attempted[index] = self.file.publication_attempted;
                return cause;
            };
            self.attempted[index] = true;
            self.published[index] = true;
        }
        fn advanceFile(self: *Self, phase: Phase) void {
            std.debug.assert(self.file.phase == .published and self.file.pending == null);
            const resources = self.file.takeResources() catch unreachable;
            const index = @backingInt(phase);
            // Paths and capabilities were validated before grant consumption.
            self.file = File.init(resources, self.paths[index][0..self.path_lens[index]]) catch unreachable;
            self.fixed_written = false;
            self.phase = phase;
        }
        pub fn resumeWithAdmission(self: *Self, original: admission.Port) !void {
            return self.drive(original, false);
        }
        /// Stop before even opening the descriptor temporary. The backend can
        /// validate its captured lineage under its mutex before authorizing IO.
        pub fn prepareDescriptorWithAdmission(self: *Self, original: admission.Port) !void {
            return self.drive(original, true);
        }
        fn drive(self: *Self, original: admission.Port, before_descriptor: bool) !void {
            if (self.aborting or self.retired) return error.InvalidArgument;
            if (self.phase == .complete) return original.checkpoint();
            while (self.phase != .complete) {
                if (before_descriptor and self.phase == .descriptor) return original.checkpoint();
                if (self.file.phase == .initial or self.file.phase == .opening) try self.file.openWithAdmission(original);
                if (self.file.phase == .failed) return self.file.failure.?;
                switch (self.phase) {
                    .checkpoint => {
                        if (!self.encoder.retiring and !self.encoder.retired) while (try self.encoder.writeNextWithAdmission(&self.file, original)) {};
                        try self.encoder.retireWithAdmission(original);
                        try self.finishCurrent(original);
                        self.advanceFile(if (self.preserve_journal) .descriptor else .segment);
                    },
                    .segment, .descriptor => {
                        if (!self.fixed_written) {
                            const bytes: []const u8 = if (self.phase == .segment) &self.segment else &self.descriptor;
                            try self.file.writeWithAdmission(0, bytes, original);
                            self.fixed_written = true;
                        }
                        try self.finishCurrent(original);
                        if (self.phase == .segment) self.advanceFile(.descriptor) else self.phase = .complete;
                    },
                    .complete => unreachable,
                }
            }
        }
        pub fn takeResources(self: *Self) !storage_io.DurabilityResources {
            if (self.phase != .complete or self.aborting or self.retired) return error.InvalidArgument;
            return self.file.takeResources();
        }
        pub fn retireWithAdmission(self: *Self, original: admission.Port) !void {
            if (self.retired) return;
            if (self.phase != .complete or self.aborting) return error.InvalidArgument;
            try self.file.retireWithAdmission(original);
            self.retired = true;
        }
        pub fn abortWithAdmission(self: *Self, original: admission.Port) !void {
            if (self.retired) return;
            self.aborting = true;
            try self.file.abortWithAdmission(original);
            const index = @min(@backingInt(self.phase), 2);
            self.attempted[index] = self.attempted[index] or self.file.publication_attempted;
            self.published[index] = self.published[index] or self.file.phase == .published;
            try self.encoder.retireWithAdmission(original);
            try self.file.retireWithAdmission(original);
            self.retired = true;
        }
        /// Explicit quiesced fallback. Caller retains final-path/recovery debt.
        pub fn deinit(self: *Self) void {
            self.file.deinit();
            self.encoder.retireWithAdmission(null) catch unreachable;
        }
    };
}

const manifest = @import("../lsm/manifest.zig");
const TestSource = struct {
    runs: []const manifest.RunMeta = &.{},
    paths: []const manifest.ObsoletePathMeta = &.{},
    run_index: usize = 0,
    path_index: usize = 0,
    pub fn runCount(self: *@This()) usize {
        return self.runs.len;
    }
    pub fn obsoleteCount(self: *@This()) usize {
        return self.paths.len;
    }
    pub fn nextRun(self: *@This()) ?manifest.RunMeta {
        if (self.run_index == self.runs.len) return null;
        defer self.run_index += 1;
        return self.runs[self.run_index];
    }
    pub fn nextObsolete(self: *@This()) ?manifest.ObsoletePathMeta {
        if (self.path_index == self.paths.len) return null;
        defer self.path_index += 1;
        return self.paths[self.path_index];
    }
};
const TestPublication = Publication(*TestSource);
const cpu = @import("../../common/runnable_scheduler.zig");
const TestControl = struct {
    owner: *TestPublication,
    enabled: bool = false,
    seen: [11]bool = @splat(false),
    failure: anyerror = error.Canceled,
    physical_failure_phase: ?TestPublication.Phase = null,
    physical_failure_injected: bool = false,
    execution: ?*cpu.Execution = null,
    scheduler: ?*cpu.Scheduler = null,
    waits: usize = 0,
    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.execution) |execution| {
            try execution.checkLifetime();
            try std.testing.expectEqual(cpu.State.running, self.scheduler.?.state(&execution.entry.?));
        }
        if (!self.enabled) return;
        const owner = self.owner;
        if (!self.physical_failure_injected and self.physical_failure_phase != null and owner.phase == self.physical_failure_phase.? and owner.file.phase == .ready and
            (if (owner.phase == .checkpoint) owner.encoder.retired else owner.fixed_written))
        {
            self.physical_failure_injected = true;
            storage_io.NativeSyncTestControl.control_failure_after_rename.store(true, .release);
        }
        if (owner.phase != .complete) {
            const operation: ?usize = switch (owner.file.phase) {
                .opening => 0,
                .writing => 1,
                .publishing => 2,
                else => null,
            };
            if (operation) |offset| {
                const index = 3 * @as(usize, @backingInt(owner.phase)) + offset;
                if (!self.seen[index]) {
                    self.seen[index] = true;
                    return self.failure;
                }
            }
            if (owner.phase == .checkpoint and owner.file.phase == .ready and owner.encoder.phase == .placeholder and !self.seen[9]) {
                self.seen[9] = true;
                return self.failure;
            }
            if (owner.encoder.retiring and !owner.encoder.retired and !self.seen[10]) {
                self.seen[10] = true;
                return self.failure;
            }
        }
    }
    fn lifetime(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.execution) |execution| try execution.checkLifetime();
    }
    fn deadline(raw: *anyopaque) !?u64 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return if (self.execution) |execution| execution.entry.?.options.deadline_ns else std.math.maxInt(u64);
    }
    fn begin(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.execution) |execution| {
            try execution.park(.resource_blocked);
            var control: cpu.Execution = .{};
            defer control.finish();
            try control.start(self.scheduler.?, .{ .io = std.testing.io, .native_now_ns = @import("antfly_platform").time.monotonicNs }, .control);
        }
        self.waits += 1;
        storage_io.NativeSyncTestControl.release.store(true, .release);
    }
    fn end(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        storage_io.NativeSyncTestControl.release.store(false, .release);
        if (self.execution) |execution| try execution.enter(.write);
    }
    fn payload(self: *@This()) admission.Payload {
        return .{ .ptr = self, .vtable = &.{ .checkpoint = check, .check_lifetime = lifetime, .deadline_ns = deadline }, .execution_wait = .{ .ptr = self, .begin = begin, .end = end } };
    }
};

test "controlled physical sync SST and manifest set reuse one prepaid grant across every original rejection and preserve control progress" {
    if (comptime @import("builtin").os.tag == .freestanding or @import("builtin").os.tag == .windows or @import("builtin").os.tag == .wasi) return error.SkipZigTest;
    const resources = @import("../resource_manager.zig");
    const run_writer = @import("owned_run_writer.zig");
    const a = std.testing.allocator;
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.FileNotFound }) |failure| {
        var manager = resources.ResourceManager.init(.{ .identity_allocator = a, .memory_budget = .{ .hard_limit_bytes = 16 * 1024 * 1024 } });
        defer manager.deinit(a);
        var pool = storage_io.NativeStoragePool.initWithCapacityForTest(a, 16);
        defer pool.deinit();
        var native = try storage_io.NativeStorage.initWithPool(a, .threaded, &pool);
        defer native.deinit();
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
        defer a.free(root);
        const table_path = try std.fmt.allocPrint(a, "{s}/table.sst", .{root});
        defer a.free(table_path);
        var scheduler = try cpu.Scheduler.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1 });
        defer scheduler.close();
        var execution: cpu.Execution = .{};
        defer execution.finish();
        try execution.reserveCleanupRoot(&scheduler, .{ .io = std.testing.io, .native_now_ns = @import("antfly_platform").time.monotonicNs, .allow_wait = true }, 30 * std.time.ns_per_s);
        try execution.activateCleanup();
        var query: cpu.Execution = .{};
        defer query.finish();
        try query.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = @import("antfly_platform").time.monotonicNs }, .query);
        var fixed = try manager.reserve(.lsm_table_builder_working_set, @sizeOf(run_writer.Writer) + @sizeOf(run_writer.Output) + try TestPublication.memoryBound(native.storage()));
        defer fixed.release();
        var owner: TestPublication = undefined;
        var control: TestControl = .{ .owner = &owner, .failure = failure, .execution = &execution, .scheduler = &scheduler };
        const payload = control.payload();
        const original = admission.Port.init(&payload);
        var reserved: usize = 0;
        while (pool.tryReserveDescriptorsForTest(std.testing.io, 1)) reserved += 1;
        defer pool.releaseDescriptorsForTest(std.testing.io, reserved);
        try std.testing.expect(reserved > 0);
        pool.releaseDescriptorsForTest(std.testing.io, 1);
        reserved -= 1;
        var writer: run_writer.Writer = undefined;
        try writer.initInPlace(a, &manager, (try native.storage().tryPrepareDurabilityWithAdmission(original)).?, table_path, 3, 512 * 1024, .{});
        defer writer.deinit();
        var output: run_writer.Output = .{ .allocator = a, .manager = &manager };
        defer output.deinit();
        storage_io.NativeSyncTestControl.pause.store(true, .release);
        storage_io.NativeSyncTestControl.release.store(false, .release);
        defer storage_io.NativeSyncTestControl.pause.store(false, .release);
        defer storage_io.NativeSyncTestControl.release.store(true, .release);
        try writer.openWithAdmission(original);
        for ([_][]const u8{ "a", "b", "z" }) |key| try writer.appendEntryWithAdmission(.{ .namespace_name = "docs", .key = key, .value = "value" }, original);
        try writer.finishWithAdmission(original);
        try writer.takeResultInto(&output, original);
        const held = pool.snapshotStats().fd_admitted_descriptors;
        const runs = [_]manifest.RunMeta{.{ .id = 7, .level = 0, .size_bytes = output.result.?.size_bytes, .compression_stats = output.result.?.compression_stats, .path = "table.sst", .smallest_namespace_name = "docs", .smallest_key = "a", .largest_namespace_name = "docs", .largest_key = "z", .entry_count = 3 }};
        const paths = [_]manifest.ObsoletePathMeta{.{ .path = "old.sst", .delete_after_ns = 123 }};
        var source: TestSource = .{ .runs = &runs, .paths = &paths };
        try owner.initInPlace(try writer.takeResources(), root, 9, 11, 10, &source, 512 * 1024);
        defer owner.deinit();
        try writer.retireWithAdmission(original);
        control.enabled = true;
        while (true) {
            owner.resumeWithAdmission(original) catch |cause| {
                try std.testing.expectEqual(failure, cause);
                try std.testing.expect(owner.file.resources != null);
                try std.testing.expectEqual(held, pool.snapshotStats().fd_admitted_descriptors);
                continue;
            };
            break;
        }
        for (control.seen) |seen| try std.testing.expect(seen);
        for (owner.published) |published| try std.testing.expect(published);
        for (owner.attempted) |attempted| try std.testing.expect(attempted);
        try std.testing.expectEqual(held, pool.snapshotStats().fd_admitted_descriptors);
        try std.testing.expect(control.waits > 10);
        try std.testing.expectEqual(cpu.State.running, scheduler.state(&query.entry.?));
        // The same grant remains reusable after the descriptor is durable.
        const next_path = try std.fmt.allocPrint(a, "{s}/next-output", .{root});
        defer a.free(next_path);
        var next = try File.init(try owner.takeResources(), next_path);
        defer next.deinit();
        try next.openWithAdmission(original);
        try next.writeWithAdmission(0, "next output", original);
        try next.finishWithAdmission(original);
        try next.retireWithAdmission(original);
        try owner.retireWithAdmission(original);
        try output.retireWithAdmission(original);
        storage_io.NativeSyncTestControl.pause.store(false, .release);
        const descriptor_bytes = try native.storage().readFileAlloc(a, owner.paths[2][0..owner.path_lens[2]], 37);
        defer a.free(descriptor_bytes);
        const descriptor = try manifests.Descriptor.decode(descriptor_bytes);
        try std.testing.expectEqual(@as(u64, 9), descriptor.checkpoint);
        try std.testing.expectEqual(@as(u64, 11), descriptor.sequence);
        const loaded = try manifests.load(a, native.storage(), root, descriptor, 512 * 1024);
        defer a.free(loaded.bytes);
        var decoded = try manifest.decodeAlloc(a, loaded.bytes);
        defer decoded.deinit(a);
        try std.testing.expectEqual(@as(usize, 1), decoded.runs.len);
        try std.testing.expectEqualStrings("table.sst", decoded.runs[0].path);
        try std.testing.expectEqual(@as(u32, 3), decoded.runs[0].entry_count);
        try std.testing.expectEqual(@as(u64, 123), decoded.obsolete_paths[0].delete_after_ns);
        // Existing production serialization shares this exact wire producer.
        var legacy_source: TestSource = .{ .runs = &runs, .paths = &paths };
        _ = try manifests.writeCheckpoint(a, native.storage(), root, 12, 11, 10, &legacy_source, 512 * 1024);
        const legacy_path = try manifests.pathAlloc(a, root, 12, .checkpoint);
        defer a.free(legacy_path);
        const legacy = try native.storage().readFileAlloc(a, legacy_path, 512 * 1024);
        defer a.free(legacy);
        try std.testing.expectEqualSlices(u8, loaded.bytes, legacy);
        fixed.release();
        try resources.qualifyNoLiveReservations(&manager);
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
    }
}

test "controlled physical sync manifest abandonment retains final path and recovery obligations after accepted publication" {
    if (comptime @import("builtin").os.tag == .freestanding or @import("builtin").os.tag == .windows or @import("builtin").os.tag == .wasi) return error.SkipZigTest;
    const a = std.testing.allocator;
    for (0..3) |phase| for ([_]usize{ 1, 2 }) |operation| {
        var pool = storage_io.NativeStoragePool.initWithCapacityForTest(a, 16);
        defer pool.deinit();
        var native = try storage_io.NativeStorage.initWithPool(a, .threaded, &pool);
        defer native.deinit();
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
        defer a.free(root);
        var owner: TestPublication = undefined;
        var control: TestControl = .{ .owner = &owner, .seen = @splat(true) };
        control.seen[3 * phase + operation] = false;
        const payload = control.payload();
        const original = admission.Port.init(&payload);
        var source: TestSource = .{};
        try owner.initInPlace((try native.storage().tryPrepareDurabilityWithAdmission(original)).?, root, 9, 0, 10, &source, 4096);
        defer owner.deinit();
        control.enabled = true;
        try std.testing.expectError(error.Canceled, owner.resumeWithAdmission(original));
        try std.testing.expectEqual(phase, @backingInt(owner.phase));
        try std.testing.expect(owner.file.pending != null);
        control.enabled = false;
        try owner.abortWithAdmission(original);
        try std.testing.expect(owner.retired and owner.file.resources == null);
        for (0..3) |index| {
            const present = index < phase or (index == phase and operation == 2);
            try std.testing.expectEqual(present, owner.attempted[index]);
            try std.testing.expectEqual(present, owner.published[index]);
            const read = native.storage().readFileAlloc(a, owner.paths[index][0..owner.path_lens[index]], 4096);
            if (present) a.free(try read) else try std.testing.expectError(error.FileNotFound, read);
        }
        // Descriptor attempt is the exact point requiring backend recovery.
        try std.testing.expectEqual(phase == 2 and operation == 2, owner.attempted[2]);
    };
}

test "controlled physical sync manifest physical failure after rename retains uncertainty without replay or deleting final files" {
    if (comptime @import("builtin").os.tag == .freestanding or @import("builtin").os.tag == .windows or @import("builtin").os.tag == .wasi) return error.SkipZigTest;
    const a = std.testing.allocator;
    for (0..3) |phase| {
        var pool = storage_io.NativeStoragePool.initWithCapacityForTest(a, 16);
        defer pool.deinit();
        var native = try storage_io.NativeStorage.initWithPool(a, .threaded, &pool);
        defer native.deinit();
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
        defer a.free(root);
        var owner: TestPublication = undefined;
        var control: TestControl = .{ .owner = &owner, .seen = @splat(true), .physical_failure_phase = @fromBackingInt(@intCast(phase)) };
        const payload = control.payload();
        const original = admission.Port.init(&payload);
        var source: TestSource = .{};
        try owner.initInPlace((try native.storage().tryPrepareDurabilityWithAdmission(original)).?, root, 9, 0, 10, &source, 4096);
        defer owner.deinit();
        defer storage_io.NativeSyncTestControl.control_failure_after_rename.store(false, .release);
        control.enabled = true;
        try std.testing.expectError(error.InputOutput, owner.resumeWithAdmission(original));
        try std.testing.expect(control.physical_failure_injected and owner.attempted[phase] and !owner.published[phase]);
        try std.testing.expect(owner.file.phase == .failed and owner.file.pending == null);
        const calls = storage_io.NativeSyncTestControl.metadata.load(.acquire);
        try std.testing.expectError(error.InputOutput, owner.resumeWithAdmission(original));
        try std.testing.expectEqual(calls, storage_io.NativeSyncTestControl.metadata.load(.acquire));
        storage_io.NativeSyncTestControl.control_failure_after_rename.store(false, .release);
        try owner.abortWithAdmission(original);
        try std.testing.expect(owner.retired and owner.file.resources == null);
        for (0..3) |index| {
            try std.testing.expectEqual(index <= phase, owner.attempted[index]);
            try std.testing.expectEqual(index < phase, owner.published[index]);
            const read = native.storage().readFileAlloc(a, owner.paths[index][0..owner.path_lens[index]], 4096);
            if (index <= phase) a.free(try read) else try std.testing.expectError(error.FileNotFound, read);
        }
        try std.testing.expectEqual(phase == 2, owner.attempted[2]);
    }
}
