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

//! Prepaid retained table construction on an owning durability grant.
const std = @import("std");
const table = @import("../lsm/table_file.zig");
const resource = @import("../resource_manager.zig");
const admission = @import("../../common/read_admission.zig");
const storage_io = @import("storage_io.zig");
const File = @import("owned_atomic_file.zig").File;
const Encoder = table.StreamingEncoder;
const slice: resource.Slice = .lsm_table_builder_working_set;

/// Stable caller-owned output. The manifest/obsolete-output owner retains the
/// path ticket even after filter retirement; this owner never deletes a final
/// file. Keep its address and allocator/manager authority alive through cleanup.
/// A registered output's final anchor. First acquisition is serialized with
/// creator close; an existing producer binding survives close across refusal.
pub const OutputAnchor = struct {
    output: *Output,
    registration: *resource.PayloadRetirement,
    refs: std.atomic.Value(usize) = .init(1),
    closing: bool = false,
    claimed: bool = false,
    pub fn acquire(self: *@This(), original: admission.Port) !void {
        try original.checkpoint();
        if (self.closing or self.claimed or self.registration.credit == null or self.registration.credit.?.manager != self.output.manager) return error.InvalidArgument;
        _ = self.refs.fetchAdd(1, .monotonic);
    }
    pub fn release(self: *@This()) void {
        const before = self.refs.fetchSub(1, .acq_rel);
        std.debug.assert(before > 1);
    }
};

pub const Output = struct {
    allocator: std.mem.Allocator,
    manager: *resource.ResourceManager,
    tracked_bytes: u64 = 0,
    result: ?table.StreamingEncoderResult = null,
    bounds: ?table.OwnedRunBounds = null,
    registered: ?*OutputAnchor = null,
    path: [std.fs.max_path_bytes]u8 = undefined,
    path_len: usize = 0,
    retirement_offset: usize = 0,
    retiring: bool = false,
    taken: bool = false,
    retired: bool = false,

    /// One physical quantum; full resident charge remains until all bytes are
    /// gone and observer retirement succeeds. Registered owners claim first.
    pub fn retireStep(self: *Output, scope: ?admission.Port) !bool {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope);
        if (original) |port| try port.checkpoint();
        if (self.registered) |anchor| if (!anchor.claimed) return error.InvalidArgument;
        if (self.retired) return true;
        self.retiring = true;
        if (self.bounds) |*bounds| {
            if (try bounds.retireStep(self.allocator, original)) self.bounds = null;
            return false;
        }
        if (self.result) |*result| {
            const bytes = result.filter.bytes;
            const end = @min(bytes.len, self.retirement_offset + 16 * 1024);
            @memset(bytes[self.retirement_offset..end], undefined);
            self.retirement_offset = end;
            if (end == bytes.len) {
                if (bytes.len != 0) self.allocator.rawFree(bytes, .of(u8), @returnAddress());
                self.result = null;
                self.retirement_offset = 0;
            }
            return false;
        }
        if (original) |port| try self.manager.adjustUsageWithAdmission(slice, &self.tracked_bytes, 0, port) else try self.manager.adjustUsage(slice, &self.tracked_bytes, 0);
        self.retired = true;
        return true;
    }
    pub fn retireWithAdmission(self: *Output, original: admission.Port) !void {
        while (!(try self.retireStep(original))) {}
    }
    /// Explicit joined cleanup; a registered header belongs to its final anchor.
    pub fn deinit(self: *Output) void {
        if (self.registered != null) @panic("registered output requires owner retirement");
        while (!(self.retireStep(null) catch unreachable)) {}
    }
};

/// Address-stable aggregate. Caller prepays this fixed header and the File
/// grant, retains allocator/manager/storage/executor authority, and supplies an
/// existing parent directory. No original application port is retained.
///
/// A rejected append retains the input borrow until that same command succeeds
/// or abort joins it. Final-file cleanup/manifest publication remain the
/// caller's responsibility, including ambiguous physical publication failure.
pub const Writer = struct {
    allocator: std.mem.Allocator,
    manager: *resource.ResourceManager,
    file: File,
    sink: table.TableSink,
    initialization: ?Encoder.Initialization,
    encoder: Encoder = undefined,
    have_encoder: bool = false,
    entry: ?Encoder.EntryPreparation = null,
    block: ?Encoder.Block = null,
    metadata: ?Encoder.Metadata = null,
    encoder_retirement: ?Encoder.Retirement = null,
    result: ?table.StreamingEncoderResult = null,
    bounds: ?table.OwnedRunBounds = null,
    result_retirement_offset: usize = 0,
    tracked_bytes: u64 = 0,
    credit: ?resource.ConstructionCredit = null,
    pending_input: ?table.Entry = null,
    output_target: ?*Output = null,
    initialization_bound: usize,
    max_file_bytes: usize,
    ready: bool = false,
    finishing: bool = false,
    published: bool = false,
    result_taken: bool = false,
    aborting: bool = false,
    retired: bool = false,

    /// Consumes the grant only on success; performs no IO or heap allocation.
    pub fn initInPlace(self: *Writer, allocator: std.mem.Allocator, manager: *resource.ResourceManager, resources: storage_io.DurabilityResources, path: []const u8, expected_entries: usize, max_file_bytes: usize, options: table.StreamingEncoderOptions) !void {
        const bound = try Encoder.Initialization.memoryBound(expected_entries, options);
        if (max_file_bytes < table.header_len + table.footer_len) return error.TableFileTooLarge;
        const initialization = try Encoder.Initialization.init(allocator, expected_entries, options, 0);
        const file = try File.init(resources, path);
        self.* = .{ .allocator = allocator, .manager = manager, .file = file, .sink = .{ .ptr = self, .vtable = &sink_vtable }, .initialization = initialization, .initialization_bound = bound, .max_file_bytes = max_file_bytes };
    }

    // Owned encoding never calls the legacy sink. Refuse synchronous fallback.
    const sink_vtable: table.TableSink.VTable = .{ .len = sinkLen, .append_slice = sinkAppend, .append_byte = sinkByte, .write_at = sinkWrite };
    fn sinkLen(_: *anyopaque) usize {
        return 0;
    }
    fn sinkAppend(_: *anyopaque, _: []const u8) !void {
        return error.ExecutionWaitUnsupported;
    }
    fn sinkByte(_: *anyopaque, _: u8) !void {
        return error.ExecutionWaitUnsupported;
    }
    fn sinkWrite(_: *anyopaque, _: usize, _: []const u8) !void {
        return error.ExecutionWaitUnsupported;
    }

    fn reserve(self: *Writer, bytes: usize, original: admission.Port) !void {
        if (self.credit != null) return;
        try self.manager.prepareUsageWithAdmission(slice, &self.tracked_bytes, original);
        self.credit = try self.manager.reserveConstructionWithAdmission(slice, bytes, original);
    }

    /// Private operation memory stays covered by construction credit until its
    /// admitted retirement finishes. Surviving allocations then adopt that
    /// credit; unused credit retires only after physical release.
    fn settle(self: *Writer, next: u64, original: admission.Port) !void {
        if (next > self.tracked_bytes) {
            try self.credit.?.adoptUsageWithAdmission(slice, &self.tracked_bytes, next - self.tracked_bytes, original);
        } else {
            try self.manager.adjustUsageWithAdmission(slice, &self.tracked_bytes, next, original);
        }
        if (self.credit) |*credit| credit.retire();
        self.credit = null;
    }

    fn resident(self: *const Writer) u64 {
        var bytes: u64 = if (self.have_encoder) self.encoder.workingSetBytes() else 0;
        if (self.result) |result| bytes += result.filter.bytes.len;
        if (self.bounds) |*bounds| bytes += bounds.memoryBytes();
        return bytes;
    }

    pub fn openWithAdmission(self: *Writer, original: admission.Port) !void {
        if (self.aborting or self.retired or self.finishing) return error.InvalidArgument;
        if (self.ready) return original.checkpoint();
        // Do not reopen a file whose header operation is retained by its producer.
        if (self.file.phase == .initial or self.file.phase == .opening) try self.file.openWithAdmission(original);
        if (!self.have_encoder) {
            try self.reserve(self.initialization_bound, original);
            while (try self.initialization.?.writeNextWithAdmission(&self.file, original)) {}
            self.encoder = try self.initialization.?.takeEncoder(&self.sink);
            self.have_encoder = true;
        }
        try self.initialization.?.retireWithAdmission(original);
        try self.settle(self.resident(), original);
        self.initialization = null;
        self.ready = true;
    }

    fn sameBytes(a: []const u8, b: []const u8) bool {
        return a.ptr == b.ptr and a.len == b.len;
    }
    fn sameInput(a: table.Entry, b: table.Entry) bool {
        if ((a.namespace_name == null) != (b.namespace_name == null)) return false;
        if (a.namespace_name) |name| if (!sameBytes(name, b.namespace_name.?)) return false;
        return sameBytes(a.key, b.key) and sameBytes(a.value, b.value) and a.tombstone == b.tombstone;
    }

    pub fn canAppendEntry(self: *const Writer, input: table.Entry) !bool {
        if (!self.ready or self.finishing or self.aborting or self.pending_input != null) return error.InvalidArgument;
        return try self.encoder.encodedSizeUpperBoundAfterEntry(input) <= self.max_file_bytes;
    }

    pub fn appendEntryWithAdmission(self: *Writer, input: table.Entry, original: admission.Port) !void {
        if (!self.ready or self.finishing or self.aborting or self.retired) return error.InvalidArgument;
        if (self.pending_input) |pending| {
            if (!sameInput(pending, input)) return error.InvalidArgument;
        } else {
            if (!try self.canAppendEntry(input)) return error.TableFileTooLarge;
            self.pending_input = input;
        }
        if (self.entry == null) {
            if (self.block != null or try self.encoder.needsBlockBeforeEntry(input)) try self.flushBlock(original);
            try self.reserve(try Encoder.EntryPreparation.memoryBound(&self.encoder, input), original);
            self.entry = try self.encoder.prepareEntry(input);
        }
        const entry = &self.entry.?;
        if (!entry.retiring and entry.phase != .adopted and entry.phase != .retired) {
            while (!(try entry.step(original))) {}
            entry.adopt(); // Linearized row receipt precedes retirement/accounting.
        }
        try entry.retireWithAdmission(original);
        try self.settle(self.resident(), original);
        self.entry = null;
        self.pending_input = null;
    }

    fn flushBlock(self: *Writer, original: admission.Port) !void {
        if (self.block == null) {
            if (self.encoder.block_entry_count == 0) return;
            try self.reserve(try Encoder.Block.memoryBound(&self.encoder), original);
            self.block = try self.encoder.prepareBlock();
        }
        const block = &self.block.?;
        if (!block.published and !block.retiring) while (try block.writeNextWithAdmission(&self.file, original)) {};
        try block.retireWithAdmission(original);
        try self.settle(self.resident(), original);
        self.block = null;
    }

    pub fn finishWithAdmission(self: *Writer, original: admission.Port) !void {
        if (!self.ready or self.aborting or self.retired or self.pending_input != null) return error.InvalidArgument;
        self.finishing = true;
        if (self.published) return original.checkpoint();
        if (self.have_encoder) {
            // Metadata and encoder retirement retry after the last block is gone.
            if (self.metadata == null and self.result == null) {
                try self.flushBlock(original);
                try self.reserve(try Encoder.Metadata.memoryBound(&self.encoder), original);
                self.metadata = try self.encoder.prepareMetadata();
            }
            if (self.metadata) |*metadata| {
                if (self.result == null) {
                    while (try metadata.writeNextWithAdmission(&self.file, original)) {}
                    const owned = try metadata.takeOwnedResult();
                    self.result = owned.result;
                    self.bounds = owned.bounds;
                }
                try metadata.retireWithAdmission(original);
                try self.settle(self.resident(), original);
                self.metadata = null;
            }
            if (self.encoder_retirement == null) self.encoder_retirement = try self.encoder.prepareRetirement();
            while (!(try self.encoder_retirement.?.step(original))) {}
            try self.settle(self.resident(), original);
            self.encoder.deinit();
            self.have_encoder = false;
            self.encoder_retirement = null;
        }
        if (self.result.?.size_bytes > self.max_file_bytes) return error.TableFileTooLarge;
        try self.file.finishWithAdmission(original);
        self.published = true;
    }

    /// Prepare destination accounting before transferring an unchanged charge.
    /// A successful ledger handoff and physical receipt have no fallible gap.
    pub fn takeResultInto(self: *Writer, output: *Output, scope: admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        if (!self.published or self.result_taken or self.aborting or self.retired or output.taken or output.retired or output.retiring or output.result != null or output.tracked_bytes != 0 or output.manager != self.manager or !std.meta.eql(output.allocator, self.allocator)) return error.InvalidArgument;
        if (self.output_target) |target| {
            if (target != output) return error.InvalidArgument;
        } else {
            if (output.registered) |anchor| {
                if (anchor.output != output) return error.InvalidArgument;
                try anchor.acquire(original);
            }
            self.output_target = output;
        }
        try self.manager.prepareUsageWithAdmission(slice, &output.tracked_bytes, original);
        try self.manager.transferPreparedUsageWithAdmission(slice, &self.tracked_bytes, &output.tracked_bytes, self.tracked_bytes, original);
        output.result = self.result;
        output.bounds = self.bounds;
        self.bounds = null;
        output.path_len = self.file.final_len;
        @memcpy(output.path[0..output.path_len], self.file.final[0..output.path_len]);
        output.taken = true;
        self.result = null;
        self.result_taken = true;
        self.releaseOutputTarget();
    }

    fn releaseOutputTarget(self: *Writer) void {
        const target = self.output_target orelse return;
        const anchor = target.registered;
        self.output_target = null;
        if (anchor) |held| held.release(); // Final access to the output header.
    }

    pub fn takeResources(self: *Writer) !storage_io.DurabilityResources {
        if (!self.published or !self.result_taken or self.aborting) return error.InvalidArgument;
        return self.file.takeResources();
    }

    /// Join every accepted borrow before retiring producers. Final publication
    /// debt is retained in file.publication_attempted and the caller's ticket.
    pub fn abortWithAdmission(self: *Writer, original: admission.Port) !void {
        if (self.retired) return;
        self.aborting = true;
        try self.file.abortWithAdmission(original);
        if (self.entry) |*entry| try entry.retireWithAdmission(original);
        self.entry = null;
        self.pending_input = null;
        if (self.block) |*block| try block.retireWithAdmission(original);
        self.block = null;
        if (self.metadata) |*metadata| try metadata.retireWithAdmission(original);
        self.metadata = null;
        if (self.initialization) |*initialization| try initialization.retireWithAdmission(original);
        self.initialization = null;
        if (self.have_encoder) {
            if (self.encoder_retirement == null) self.encoder_retirement = try self.encoder.prepareRetirement();
            while (!(try self.encoder_retirement.?.step(original))) {}
            self.encoder.deinit();
            self.have_encoder = false;
            self.encoder_retirement = null;
        }
        if (self.bounds) |*bounds| {
            while (!(try bounds.retireStep(self.allocator, original))) {}
            self.bounds = null;
        }
        if (self.result) |*result| {
            try retireFilter(self.allocator, result, &self.result_retirement_offset, original);
            self.result = null;
        }
        try self.settle(0, original);
        try self.file.retireWithAdmission(original);
        self.retired = true;
        self.releaseOutputTarget();
    }

    pub fn retireWithAdmission(self: *Writer, original: admission.Port) !void {
        if (self.retired) return;
        if (!self.published or !self.result_taken or self.aborting) return error.InvalidArgument;
        try self.manager.adjustUsageWithAdmission(slice, &self.tracked_bytes, 0, original);
        try self.file.retireWithAdmission(original);
        self.retired = true;
    }

    /// Explicit quiesced fallback. Join IO before releasing producer buffers,
    /// then release accounting after all covered memory has physically gone.
    pub fn deinit(self: *Writer) void {
        self.file.deinit();
        if (self.entry) |*entry| entry.deinit();
        if (self.block) |*block| block.deinit();
        if (self.metadata) |*metadata| metadata.deinit();
        if (self.initialization) |*initialization| initialization.deinit();
        if (self.have_encoder) self.encoder.deinit();
        if (self.bounds) |*bounds| bounds.deinit(self.allocator);
        if (self.result) |*result| result.filter.deinit(self.allocator);
        self.manager.observeUsage(slice, &self.tracked_bytes, 0);
        if (self.credit) |*credit| credit.retire();
        self.releaseOutputTarget();
    }
};

fn retireFilter(allocator: std.mem.Allocator, result: *table.StreamingEncoderResult, offset: *usize, original: admission.Port) !void {
    const bytes = result.filter.bytes;
    while (offset.* < bytes.len) {
        try original.checkpoint();
        const n = @min(bytes.len - offset.*, 16 * 1024);
        @memset(bytes[offset.*..][0..n], undefined);
        offset.* += n;
    }
    if (bytes.len != 0) allocator.rawFree(bytes, .of(u8), @returnAddress());
    result.filter.bytes = &.{};
    offset.* = 0;
}

// Check the ledger independently at every physical allocation and release.
const FundedAllocator = struct {
    writer: *Writer,
    manager: *resource.ResourceManager,
    fixed_bytes: u64,
    live: usize = 0,
    allocations: usize = 0,

    fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        std.debug.assert(self.writer.credit != null);
        std.debug.assert(self.manager.sliceStats(slice).used_bytes >= self.fixed_bytes + self.live + len);
        const ptr = std.testing.allocator.rawAlloc(len, alignment, ra) orelse return null;
        self.live += len;
        self.allocations += 1;
        return ptr;
    }
    fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }
    fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        std.debug.assert(self.manager.sliceStats(slice).used_bytes >= self.fixed_bytes + self.live);
        std.testing.allocator.rawFree(bytes, alignment, ra);
        self.live -= bytes.len;
    }
};

const TestControl = struct {
    writer: *Writer,
    output: ?*Output = null,
    failure: anyerror = error.Canceled,
    seen: [11]bool = @splat(false),
    enabled: bool = true,
    execution: ?*@import("../../common/runnable_scheduler.zig").Execution = null,
    scheduler: ?*@import("../../common/runnable_scheduler.zig").Scheduler = null,
    waits: usize = 0,

    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.execution) |execution| {
            try execution.checkLifetime();
            try std.testing.expectEqual(@import("../../common/runnable_scheduler.zig").State.running, self.scheduler.?.state(&execution.entry.?));
        }
        if (!self.enabled) return;
        const w = self.writer;
        const points = [_]bool{
            w.file.phase == .opening,
            w.initialization != null and w.file.phase == .writing,
            if (w.entry) |entry| entry.phase == .prefix_scan else false,
            if (w.entry) |entry| entry.phase == .adopted else false,
            w.block != null and w.file.phase == .writing,
            if (w.block) |block| block.retiring else false,
            w.metadata != null and w.file.phase == .writing,
            if (w.metadata) |metadata| metadata.retiring else false,
            if (w.encoder_retirement) |retirement| !retirement.done else false,
            w.output_target != null and !w.result_taken,
            if (self.output) |output| output.retiring else false,
        };
        for (points, 0..) |active, i| {
            if (active and !self.seen[i]) {
                self.seen[i] = true;
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
            var control: @import("../../common/runnable_scheduler.zig").Execution = .{};
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

test "controlled physical sync prepaid run writer retains rejected rows IO retirement and output credit with sole FD headroom" {
    if (comptime @import("builtin").os.tag == .freestanding or @import("builtin").os.tag == .windows or @import("builtin").os.tag == .wasi) return error.SkipZigTest;
    const cpu = @import("../../common/runnable_scheduler.zig");
    const a = std.testing.allocator;
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.FileNotFound }) |failure| {
        var manager = resource.ResourceManager.init(.{ .identity_allocator = a, .memory_budget = .{ .hard_limit_bytes = 16 * 1024 * 1024 } });
        defer manager.deinit(a);
        var pool = storage_io.NativeStoragePool.initWithCapacityForTest(a, 16);
        defer pool.deinit();
        var native = try storage_io.NativeStorage.initWithPool(a, .threaded, &pool);
        defer native.deinit();
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/run", .{tmp.sub_path});
        defer a.free(path);
        var scheduler = try cpu.Scheduler.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1 });
        defer scheduler.close();
        var execution: cpu.Execution = .{};
        defer execution.finish();
        try execution.reserveCleanupRoot(&scheduler, .{ .io = std.testing.io, .native_now_ns = @import("antfly_platform").time.monotonicNs, .allow_wait = true }, 30 * std.time.ns_per_s);
        try execution.activateCleanup();
        var query: cpu.Execution = .{};
        defer query.finish();
        try query.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = @import("antfly_platform").time.monotonicNs }, .query);
        // The fixed aggregate, output, and durability provider are prepaid by
        // their caller; the aggregate reserves every additional heap peak.
        const fixed_bytes = @sizeOf(Writer) + @sizeOf(Output) + try File.memoryBound(native.storage());
        var fixed = try manager.reserve(slice, fixed_bytes);
        defer fixed.release();
        var writer: Writer = undefined;
        var witness: FundedAllocator = .{ .writer = &writer, .manager = &manager, .fixed_bytes = fixed_bytes };
        defer std.debug.assert(witness.live == 0);
        var output: Output = .{ .allocator = witness.allocator(), .manager = &manager };
        defer output.deinit();
        var control: TestControl = .{ .writer = &writer, .output = &output, .failure = failure, .execution = &execution, .scheduler = &scheduler };
        const payload = control.payload();
        const original = admission.Port.init(&payload);
        var reserved: usize = 0;
        while (pool.tryReserveDescriptorsForTest(std.testing.io, 1)) reserved += 1;
        defer pool.releaseDescriptorsForTest(std.testing.io, reserved);
        try std.testing.expect(reserved > 0);
        pool.releaseDescriptorsForTest(std.testing.io, 1);
        reserved -= 1;
        // Acquiring the provider may checkpoint before writer initialization.
        control.enabled = false;
        const grant = (try native.storage().tryPrepareDurabilityWithAdmission(original)).?;
        try writer.initInPlace(witness.allocator(), &manager, grant, path, 192, 512 * 1024, .{});
        defer writer.deinit();
        control.enabled = true;
        storage_io.NativeSyncTestControl.pause.store(true, .release);
        storage_io.NativeSyncTestControl.release.store(false, .release);
        defer storage_io.NativeSyncTestControl.pause.store(false, .release);
        defer storage_io.NativeSyncTestControl.release.store(true, .release);
        while (true) {
            writer.openWithAdmission(original) catch |cause| {
                try std.testing.expectEqual(failure, cause);
                continue;
            };
            break;
        }
        try std.testing.expect(writer.credit == null);
        try std.testing.expectEqual(writer.encoder.workingSetBytes(), writer.tracked_bytes);
        const value: [1024]u8 = @splat('v');
        for (0..192) |i| {
            var key: [32]u8 = undefined;
            const input: table.Entry = .{ .namespace_name = "docs", .key = try std.fmt.bufPrint(&key, "doc:{d:0>8}", .{i}), .value = &value };
            while (true) {
                writer.appendEntryWithAdmission(input, original) catch |cause| {
                    try std.testing.expectEqual(failure, cause);
                    // An observed row or block cannot be replaced on retry.
                    try std.testing.expectError(error.InvalidArgument, writer.appendEntryWithAdmission(.{ .key = "different", .value = "command" }, original));
                    try std.testing.expect(writer.pending_input != null);
                    continue;
                };
                break;
            }
            try std.testing.expectEqual(i + 1, writer.encoder.entry_count);
            try std.testing.expectEqual(writer.encoder.workingSetBytes(), writer.tracked_bytes);
            try std.testing.expect(writer.credit == null and writer.pending_input == null);
        }
        while (true) {
            writer.finishWithAdmission(original) catch |cause| {
                try std.testing.expectEqual(failure, cause);
                continue;
            };
            break;
        }
        try std.testing.expect(writer.published and !writer.have_encoder and writer.credit == null);
        try std.testing.expectEqual(@as(u64, writer.result.?.filter.bytes.len) + writer.bounds.?.memoryBytes(), writer.tracked_bytes);
        // Metadata is prepared before measuring the unchanged-charge handoff.
        control.enabled = false;
        try manager.prepareUsageWithAdmission(slice, &output.tracked_bytes, original);
        const before = manager.snapshot();
        control.enabled = true;
        while (true) {
            writer.takeResultInto(&output, original) catch |cause| {
                try std.testing.expectEqual(failure, cause);
                try std.testing.expectEqual(@as(u64, 0), output.tracked_bytes);
                continue;
            };
            break;
        }
        try std.testing.expectEqual(before.memory.used_bytes, manager.snapshot().memory.used_bytes);
        try std.testing.expectEqual(before.memory.peak_bytes, manager.snapshot().memory.peak_bytes);
        try std.testing.expectEqual(@as(u64, 0), writer.tracked_bytes);
        try std.testing.expectEqual(@as(u64, output.result.?.filter.bytes.len) + output.bounds.?.memoryBytes(), output.tracked_bytes);
        try std.testing.expectEqualStrings("docs", output.bounds.?.fields[0].?);
        try std.testing.expectEqualStrings("doc:00000000", output.bounds.?.fields[1].?);
        try std.testing.expectEqualStrings("docs", output.bounds.?.fields[2].?);
        try std.testing.expectEqualStrings("doc:00000191", output.bounds.?.fields[3].?);
        try std.testing.expectEqualStrings(path, output.path[0..output.path_len]);
        try std.testing.expectError(error.InvalidArgument, writer.takeResultInto(&output, original));
        const size = output.result.?.size_bytes;
        const held = pool.snapshotStats().fd_admitted_descriptors;
        const next_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/next", .{tmp.sub_path});
        defer a.free(next_path);
        var next = try File.init(try writer.takeResources(), next_path);
        defer next.deinit();
        try std.testing.expectEqual(held, pool.snapshotStats().fd_admitted_descriptors);
        try next.openWithAdmission(original);
        try next.writeWithAdmission(0, "reused completion grant", original);
        try next.finishWithAdmission(original);
        try next.retireWithAdmission(original);
        try writer.retireWithAdmission(original);
        while (true) {
            output.retireWithAdmission(original) catch |cause| {
                try std.testing.expectEqual(failure, cause);
                try std.testing.expect(output.tracked_bytes != 0);
                continue;
            };
            break;
        }
        for (control.seen) |seen| try std.testing.expect(seen);
        try std.testing.expect(control.waits > 5);
        try std.testing.expectEqual(@as(u64, 0), output.tracked_bytes);
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
        try std.testing.expectEqual(cpu.State.running, scheduler.state(&query.entry.?));
        const raw = try native.storage().readFileAlloc(a, path, size + 1);
        defer a.free(raw);
        var decoded = try table.decodeAlloc(a, raw);
        defer decoded.deinit(a);
        try std.testing.expectEqual(@as(usize, 192), decoded.entries.len);
        for (decoded.entries, 0..) |entry, i| {
            var key: [32]u8 = undefined;
            try std.testing.expectEqualStrings(try std.fmt.bufPrint(&key, "doc:{d:0>8}", .{i}), entry.key);
            try std.testing.expectEqualStrings("docs", entry.namespace_name.?);
            try std.testing.expectEqualSlices(u8, &value, entry.value);
        }
        try std.testing.expect(witness.allocations > 10);
        try std.testing.expectEqual(@as(usize, 0), witness.live);
        fixed.release();
        try resource.qualifyNoLiveReservations(&manager);
    }
}

test "controlled physical sync prepaid run writer refuses unfunded construction and aborts retained producer borrows" {
    if (comptime @import("builtin").os.tag == .freestanding or @import("builtin").os.tag == .windows or @import("builtin").os.tag == .wasi) return error.SkipZigTest;
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |deny| {
        var budgets = resource.Options.defaultBudgets();
        budgets[@backingInt(slice)] = .{ .hard_limit_bytes = if (deny) 1 else 16 * 1024 * 1024 };
        var manager = resource.ResourceManager.init(.{ .identity_allocator = a, .budgets = budgets, .memory_budget = .{ .hard_limit_bytes = 16 * 1024 * 1024 } });
        defer manager.deinit(a);
        var pool = storage_io.NativeStoragePool.initWithCapacityForTest(a, 16);
        defer pool.deinit();
        var native = try storage_io.NativeStorage.initWithPool(a, .threaded, &pool);
        defer native.deinit();
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/aborted", .{tmp.sub_path});
        defer a.free(path);
        var writer: Writer = undefined;
        var control: TestControl = .{ .writer = &writer, .enabled = false };
        const payload = control.payload();
        const original = admission.Port.init(&payload);
        try writer.initInPlace(a, &manager, (try native.storage().tryPrepareDurabilityWithAdmission(original)).?, path, 192, 512 * 1024, .{});
        defer writer.deinit();
        if (deny) {
            try std.testing.expectError(error.ResourceBudgetExceeded, writer.openWithAdmission(original));
            try std.testing.expect(writer.initialization.?.builder == null and writer.credit == null and !writer.have_encoder);
            try std.testing.expectEqual(@as(u64, 0), writer.file.logical_len);
        } else {
            control.enabled = true;
            control.seen[0] = true; // Reject the accepted header, then abandon it.
            try std.testing.expectError(error.Canceled, writer.openWithAdmission(original));
            try std.testing.expect(writer.file.pending != null and writer.credit != null);
            try std.testing.expect(writer.initialization.?.builder != null);
            control.enabled = false;
        }
        try writer.abortWithAdmission(original);
        try std.testing.expect(writer.retired and writer.file.resources == null and writer.credit == null);
        try std.testing.expectEqual(@as(u64, 0), writer.tracked_bytes);
        try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(slice).used_bytes);
        try resource.qualifyNoLiveReservations(&manager);
        try std.testing.expectError(error.FileNotFound, native.storage().readFileAlloc(a, path, 512 * 1024));
    }
}

test "controlled physical sync prepared output charge transfer needs no parent headroom and preserves rejected ownership" {
    const a = std.testing.allocator;
    var parent = try resource.metadata_fixture.init(.{ .identity_allocator = a });
    defer parent.deinit(a);
    var manager = try resource.metadata_fixture.init(.{ .identity_allocator = a });
    defer manager.deinit(a);
    try resource.metadata_fixture.installParent(&manager, &parent);
    const Control = struct {
        reject: bool = false,
        fn check(_: *anyopaque) !void {}
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.reject) return error.Canceled;
            return error.UnexpectedWait;
        }
        fn end(_: *anyopaque) !void {}
    };
    var control: Control = .{};
    const payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check, .wait_for_progress = Control.begin } };
    const original = admission.Port.init(&payload);
    var source: u64 = 0;
    var target: u64 = 0;
    var unprepared: u64 = 0;
    defer manager.observeUsage(slice, &source, 0);
    defer manager.observeUsage(slice, &target, 0);
    try manager.prepareUsageWithAdmission(slice, &source, original);
    try manager.prepareUsageWithAdmission(slice, &target, original);
    try manager.adjustUsageWithAdmission(slice, &source, 80, original);
    const before = manager.snapshot();
    const parent_before = parent.snapshot();
    // Fill the parent exactly, then hold its ledger. The handoff is entirely
    // local and must not ask it for either release or growth.
    parent.memory.budget.hard_limit_bytes = parent_before.memory.used_bytes;
    try std.testing.expect(parent.mutex.tryLock());
    manager.transferPreparedUsageWithAdmission(slice, &source, &target, 30, original) catch |cause| {
        parent.mutex.unlock();
        return cause;
    };
    parent.mutex.unlock();
    try std.testing.expectEqual(@as(u64, 50), source);
    try std.testing.expectEqual(@as(u64, 30), target);
    try std.testing.expectEqual(before.memory.used_bytes, manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(parent_before.memory.used_bytes, parent.snapshot().memory.used_bytes);
    try std.testing.expectEqual(before.memory.peak_bytes, manager.snapshot().memory.peak_bytes);
    try std.testing.expectError(error.ResourceAccountingMismatch, manager.transferPreparedUsageWithAdmission(slice, &source, &unprepared, 1, original));
    try std.testing.expectError(error.ResourceAccountingMismatch, manager.transferPreparedUsageWithAdmission(slice, &source, &target, 51, original));
    try std.testing.expectError(error.InvalidArgument, manager.transferPreparedUsageWithAdmission(slice, &source, &source, 1, original));
    // Rejected lock waiting retains both owners and the complete parent charge.
    control.reject = true;
    try std.testing.expect(manager.mutex.tryLock());
    const rejected = manager.transferPreparedUsageWithAdmission(slice, &source, &target, 50, original);
    manager.mutex.unlock();
    try std.testing.expectError(error.Canceled, rejected);
    try std.testing.expectEqual(@as(u64, 50), source);
    try std.testing.expectEqual(@as(u64, 30), target);
    control.reject = false;
    try manager.transferPreparedUsageWithAdmission(slice, &source, &target, 50, original);
    try std.testing.expectEqual(@as(u64, 0), source);
    try std.testing.expectEqual(@as(u64, 80), target);
    // Zero source identity survives the handoff and can receive credit again
    // without observer allocation or parent admission.
    try manager.transferPreparedUsageWithAdmission(slice, &target, &source, 80, original);
    try std.testing.expectEqual(@as(u64, 80), source);
    try std.testing.expectEqual(@as(u64, 0), target);
    try std.testing.expectEqual(parent_before.memory.used_bytes, parent.snapshot().memory.used_bytes);
}

test "controlled physical sync prepaid run abandonment retires adopted rows blocks metadata and encoder under original rejection" {
    if (comptime @import("builtin").os.tag == .freestanding or @import("builtin").os.tag == .windows or @import("builtin").os.tag == .wasi) return error.SkipZigTest;
    const a = std.testing.allocator;
    // Reject after row adoption, accepted block/metadata IO, and at encoder
    // retirement. Each abandoned state must retain its own funded memory and
    // settle accepted borrows before releasing producers or their credit.
    for ([_]usize{ 3, 4, 6, 8 }) |point| {
        var manager = resource.ResourceManager.init(.{ .identity_allocator = a, .memory_budget = .{ .hard_limit_bytes = 16 * 1024 * 1024 } });
        defer manager.deinit(a);
        var pool = storage_io.NativeStoragePool.initWithCapacityForTest(a, 16);
        defer pool.deinit();
        var native = try storage_io.NativeStorage.initWithPool(a, .threaded, &pool);
        defer native.deinit();
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/abandon", .{tmp.sub_path});
        defer a.free(path);
        const fixed_bytes = @sizeOf(Writer) + try File.memoryBound(native.storage());
        var fixed = try manager.reserve(slice, fixed_bytes);
        defer fixed.release();
        var writer: Writer = undefined;
        var witness: FundedAllocator = .{ .writer = &writer, .manager = &manager, .fixed_bytes = fixed_bytes };
        defer std.debug.assert(witness.live == 0);
        var control: TestControl = .{ .writer = &writer, .enabled = false, .seen = @splat(true) };
        control.seen[point] = false;
        const payload = control.payload();
        const original = admission.Port.init(&payload);
        try writer.initInPlace(witness.allocator(), &manager, (try native.storage().tryPrepareDurabilityWithAdmission(original)).?, path, 192, 512 * 1024, .{});
        defer writer.deinit();
        try writer.openWithAdmission(original);
        control.enabled = true;
        const value: [1024]u8 = @splat('v');
        var keys: [192][32]u8 = undefined;
        for (0..192) |i| {
            writer.appendEntryWithAdmission(.{ .namespace_name = "docs", .key = try std.fmt.bufPrint(&keys[i], "doc:{d:0>8}", .{i}), .value = &value }, original) catch |cause| {
                try std.testing.expectEqual(error.Canceled, cause);
                try std.testing.expect(control.seen[point]);
                break;
            };
        }
        if (!control.seen[point]) try std.testing.expectError(error.Canceled, writer.finishWithAdmission(original));
        try std.testing.expect(control.seen[point]);
        try std.testing.expect(writer.tracked_bytes != 0 or writer.credit != null);
        // Abort itself may reject at each still-unretired CPU owner. Keep the
        // complete ledger charge until the final physical release and settle.
        control.seen[3] = false;
        control.seen[5] = false;
        control.seen[7] = false;
        control.seen[8] = false;
        while (true) {
            writer.abortWithAdmission(original) catch |cause| {
                try std.testing.expectEqual(error.Canceled, cause);
                try std.testing.expect(writer.tracked_bytes != 0 or writer.credit != null);
                continue;
            };
            break;
        }
        try std.testing.expectEqual(@as(usize, 0), witness.live);
        try std.testing.expectEqual(@as(u64, 0), writer.tracked_bytes);
        try std.testing.expect(writer.retired and writer.credit == null and writer.file.resources == null);
        try std.testing.expectEqual(fixed_bytes, manager.sliceStats(slice).used_bytes);
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
        fixed.release();
        try resource.qualifyNoLiveReservations(&manager);
        try std.testing.expectError(error.FileNotFound, native.storage().readFileAlloc(a, path, 512 * 1024));
    }
}
