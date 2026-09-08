// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared forward-pass batching. Pipelines retain their task semantics; only
//! explicitly row-independent stages may install this dispatcher. Dynamic
//! leading input/output axes and identical non-batch geometry are required.
const std = @import("std");
const platform = @import("antfly_platform");
const session_mod = @import("../backends/session.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const micro = @import("executor_microbatch.zig");

pub const Dispatch = struct {
    ptr: *anyopaque,
    task: micro.Task,
    run_fn: *const fn (*anyopaque, micro.Task, std.mem.Allocator, session_mod.Session, ?*session_mod.RunPermit, ?*std.atomic.Mutex, []const Tensor, ?Control) anyerror![]Tensor,

    pub fn run(self: Dispatch, allocator: std.mem.Allocator, session: session_mod.Session, permit: ?*session_mod.RunPermit, gate: ?*std.atomic.Mutex, inputs: []const Tensor, control: ?Control) ![]Tensor {
        return self.run_fn(self.ptr, self.task, allocator, session, permit, gate, inputs, control);
    }
};

const Ticket = struct {
    session: session_mod.Session,
    permit: ?*session_mod.RunPermit,
    gate: *std.atomic.Mutex,
    inputs: []const Tensor,
    control: ?Control,
    rows: usize,
};

test "tensor microbatch retained cross output does not pin obsolete columns" {
    const memory = @import("../runtime/tier/memory.zig");
    const alloc = std.testing.allocator;
    var controller = memory.AdmissionController{};
    const session = session_mod.Session{ .ptr = &controller, .vtable = undefined, .run_admission = .{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = .{},
        .static_workspace_bytes = 1,
        .check_live_memory = false,
    } };
    const outputs = try alloc.alloc(Tensor, 2);
    outputs[0] = try Tensor.initFloat32(alloc, "logits", &.{ 2, 2 }, &.{ 1, 2, 3, 4 });
    outputs[1] = try Tensor.initFloat32(alloc, "present.0.encoder.key", &.{ 2, 2 }, &.{ 5, 6, 7, 8 });
    const retained = try session_mod.retainedOutputAmounts(outputs, false);
    const owner = try alloc.create(SharedOutputs);
    const columns = try alloc.alloc(SharedOutputs.Column, 2);
    owner.* = .{ .allocator = alloc, .outputs = outputs, .columns = columns, .remaining = .init(2), .permit = try session.admitHostPreprocess(retained.host_scratch_bytes) };
    owner.credits = try session_mod.OutputCredits.init(alloc, outputs, false, true);
    for (columns, 0..) |*column, index| column.* = .{ .owner = owner, .index = index };
    const first = try owner.rows(0, 1, 2);
    const second = try owner.rows(1, 1, 2);
    defer alloc.free(first);
    defer alloc.free(second);
    SharedOutputs.release(owner);
    first[0].deinit();
    second[0].deinit();
    try std.testing.expectEqual(retained.host_scratch_bytes / 2, controller.snapshot().host_scratch_bytes);
    try std.testing.expect(!try session.compactExclusiveRows(alloc, second[1..], 0, null));
    first[1].deinit();
    try std.testing.expectEqual(retained.host_scratch_bytes / 2, controller.snapshot().host_scratch_bytes);
    try std.testing.expect(try session.compactExclusiveRows(alloc, second[1..], 0, null));
    try std.testing.expectEqual(@as(usize, 24), controller.snapshot().hostTotalBytes());
    try std.testing.expectEqual(second[1].data.len, second[1].shared_storage.?.len);
    try std.testing.expectEqualSlices(f32, &.{ 7, 8 }, second[1].asFloat32());
    second[1].deinit();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

/// Names and data remain valid through the last consumer, not merely the last
/// submitter. Slices share a batch allocation but never share mutable rows.
const SharedOutputs = struct {
    allocator: std.mem.Allocator,
    outputs: []Tensor,
    permit: ?session_mod.RunPermit,
    columns: []Column,
    remaining: std.atomic.Value(usize),
    credits: ?session_mod.OutputCredits = null,
    mutex: std.atomic.Mutex = .unlocked,

    const Column = struct {
        owner: *SharedOutputs,
        index: usize,
        refs: std.atomic.Value(usize) = .init(1),

        fn isExclusive(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // The producer's root reference prevents exclusivity until all
            // views have been published. No new views can be created afterward.
            return self.refs.load(.acquire) == 1;
        }

        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.refs.fetchSub(1, .acq_rel) != 1) return;
            const owner = self.owner;
            // A persistent cross-cache must not pin obsolete logits/self-cache.
            platform.sync.lockYielding(&owner.mutex);
            const output = &owner.outputs[self.index];
            if (owner.permit) |*permit| if (permit.lease) |*lease| {
                output.deinit();
                owner.credits.?.release(self.index, lease);
            } else output.deinit() else output.deinit();
            owner.mutex.unlock();
            if (owner.remaining.fetchSub(1, .acq_rel) != 1) return;
            if (owner.permit) |*permit| permit.deinit();
            if (owner.credits) |*credits| credits.deinit(owner.allocator);
            owner.allocator.free(owner.outputs);
            owner.allocator.free(owner.columns);
            owner.allocator.destroy(owner);
        }
    };

    fn release(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const columns = self.columns;
        for (columns) |*column| Column.release(column);
    }

    fn rows(self: *@This(), start: usize, count: usize, total: usize) ![]Tensor {
        const output = try self.allocator.alloc(Tensor, self.outputs.len);
        var initialized: usize = 0;
        errdefer {
            for (output[0..initialized]) |*tensor| tensor.deinit();
            self.allocator.free(output);
        }
        for (self.outputs, output, self.columns) |tensor, *view, *column| {
            const shape = try self.allocator.dupe(i64, tensor.shape);
            if (shape[0] != 0) shape[0] = @intCast(count);
            const stride = tensor.data.len / total;
            view.* = .{
                .data = tensor.data[start * stride ..][0 .. count * stride],
                .dtype = tensor.dtype,
                .shape = shape,
                .name = tensor.name,
                .allocator = self.allocator,
                .owns_data = false,
                .owns_shape = true,
                .lifetime = .{ .context = column, .release = Column.release, .is_exclusive = Column.isExclusive },
                .shared_storage = tensor.data,
                .admitted_storage_domain = if (self.permit) |permit| if (permit.lease) |lease| lease.controller else null else null,
            };
            _ = column.refs.fetchAdd(1, .monotonic);
            initialized += 1;
        }
        return output;
    }
};

fn destroy(allocator: std.mem.Allocator, tensors: []Tensor) void {
    for (tensors) |*tensor| tensor.deinit();
    allocator.free(tensors);
}

fn validTensor(tensor: Tensor) bool {
    var elements: usize = 1;
    for (tensor.shape) |dim| {
        if (dim < 0) return false;
        elements = std.math.mul(usize, elements, @intCast(dim)) catch return false;
    }
    return (std.math.mul(usize, elements, tensor.dtype.byteSize()) catch return false) == tensor.data.len;
}

/// Dynamic shape metadata alone does not establish semantic independence.
/// Installation by a row-independent pipeline stage is the explicit opt-in.
pub fn eligible(session: session_mod.Session, inputs: []const Tensor) bool {
    if (inputs.len == 0) return false;
    if (inputs[0].shape.len == 0 or inputs[0].shape[0] <= 0) return false;
    for (inputs, 0..) |input, index| {
        if (!validTensor(input)) return false;
        if (session.isBroadcastInput(input.name)) {
            if (input.data.len == 0 or input.data.len > 8) return false;
        } else if (input.shape.len == 0 or input.shape[0] != inputs[0].shape[0]) return false;
        for (inputs[0..index]) |previous| if (std.mem.eql(u8, previous.name, input.name)) return false;
    }
    if (session.vtable.independentBatchRows) |qualified| return qualified(session.ptr, inputs);
    const declared = session.inputInfo();
    const outputs = session.outputInfo();
    if (declared.len != inputs.len or outputs.len == 0) return false;
    for (inputs) |input| {
        var matched = false;
        for (declared) |info| {
            if (!std.mem.eql(u8, info.name, input.name)) continue;
            if (info.dtype != input.dtype or info.shape.len != input.shape.len) return false;
            if (session.isBroadcastInput(input.name)) {
                if (!std.mem.eql(i64, info.shape, input.shape)) return false;
            } else {
                if (info.shape[0] > 0) return false;
                for (info.shape[1..], input.shape[1..]) |expected, actual| if (expected > 0 and expected != actual) return false;
            }
            matched = true;
            break;
        }
        if (!matched) return false;
    }
    for (outputs) |info| {
        if (info.shape.len == 0 or info.shape[0] > 0) return false;
        if (session.vtable.runGeometry == null and session.output_geometry == null and session.cached_decoder_geometry == null) {
            // The legacy text-stage opt-in preserves a 2-D token axis only.
            // Audio/image transforms and unresolved feature widths require a
            // concrete stage contract before their outputs may be fused.
            for (info.shape[1..], 1..) |dim, axis| {
                if (dim <= 0 and !(axis == 1 and inputs[0].shape.len == 2)) return false;
            }
        }
    }
    return true;
}

pub fn run(broker: *micro.Broker, allocator: std.mem.Allocator, io: std.Io, task: micro.Task, session: session_mod.Session, permit: ?*session_mod.RunPermit, gate: *std.atomic.Mutex, inputs: []const Tensor, control: ?Control, deadline: ?std.Io.Clock.Timestamp, wait_us: u64) ![]Tensor {
    if (control) |active| try active.check();
    if (!eligible(session, inputs)) return direct(allocator, .{ .session = session, .permit = permit, .gate = gate, .inputs = inputs, .control = control, .rows = 1 });
    const alloc = std.heap.smp_allocator;
    var signature = std.Io.Writer.Allocating.init(allocator);
    defer signature.deinit();
    try signature.writer.print("{d}:{d};", .{ @intFromPtr(session.vtable), @intFromPtr(gate) });
    try std.json.Stringify.value(session.output_geometry, .{}, &signature.writer);
    try std.json.Stringify.value(session.cached_decoder_geometry, .{}, &signature.writer);
    try std.json.Stringify.value(session.broadcast_inputs, .{}, &signature.writer);
    // A fused native call must stay inside the same supervising boundary.
    // Never inherit just one caller's deadline/cancellation for its peers.
    if (control) |active| if (active.hard_cancellation) |boundary| {
        try signature.writer.print("hard:{d}:{d}:{d};", .{ @intFromPtr(boundary.ptr), @intFromPtr(boundary.arm_fn), @intFromPtr(boundary.disarm_fn) });
    };
    if (session.run_admission) |admission| try std.json.Stringify.value(.{
        .controller = @intFromPtr(admission.controller),
        .backend = admission.backend_class,
        .limits = admission.limits,
        .workspace = admission.static_workspace_bytes,
        .workspace_reserved = admission.backend_workspace_reserved,
        .profile = admission.model_profile,
        .check_live_memory = admission.check_live_memory,
    }, .{}, &signature.writer);
    var bytes: usize = 0;
    for (inputs) |tensor| {
        bytes = try std.math.add(usize, bytes, tensor.data.len);
        const broadcast = session.isBroadcastInput(tensor.name);
        try std.json.Stringify.value(.{ .name = tensor.name, .dtype = tensor.dtype, .shape = if (broadcast) tensor.shape else tensor.shape[1..], .value = if (broadcast) tensor.data else &.{} }, .{}, &signature.writer);
    }
    const ticket = Ticket{ .session = session, .permit = permit, .gate = gate, .inputs = inputs, .control = control, .rows = @intCast(inputs[0].shape[0]) };
    // Requests already larger than the coalescing window keep their existing
    // native batch. Do not turn a valid caller batch into a broker size error.
    if (ticket.rows > 64 or bytes > 64 * 1024 * 1024) return direct(allocator, ticket);
    if (permit) |borrowed| {
        if (!try borrowed.yieldExecution()) return direct(allocator, ticket);
    }
    const Probe = struct {
        fn canceled(raw: *const anyopaque) bool {
            const active: *const Control = @ptrCast(@alignCast(raw));
            active.check() catch return true;
            return false;
        }
    };
    const results = try broker.submitBatchControlled(Ticket, []Tensor, io, alloc, .{
        .model = "tensor-forward",
        .generation = @intFromPtr(session.ptr),
        .task = task,
        .schema = signature.written(),
        .resource_class = if (session.backend().usesGpuHostedSession()) .gpu else .cpu,
    }, .{ .mode = .native, .preferred_items = 8, .max_items = 8, .max_tokens = 64, .max_bytes = 64 * 1024 * 1024, .max_wait_us = wait_us }, &.{.{ .bytes = bytes, .tokens = ticket.rows }}, &.{.{}}, deadline, if (control) |*active| .{ .ptr = active, .is_cancelled_fn = Probe.canceled } else .{}, &.{ticket}, broker, execute);
    defer {
        for (results) |result| switch (result.result) {
            .value => |value| destroy(alloc, value),
            .item_error => {},
        };
        alloc.free(results);
    }
    switch (results[0].result) {
        .item_error => |err| return err.cause,
        .value => |value| {
            if (control) |active| try active.check();
            // Move tensor ownership after joining; only the small container
            // uses the caller allocator. No copies of encoder hidden states.
            const output = try allocator.dupe(Tensor, value);
            alloc.free(value);
            results[0].result = .{ .value = &.{} };
            return output;
        },
    }
}

fn direct(allocator: std.mem.Allocator, ticket: Ticket) ![]Tensor {
    if (ticket.control) |control| try control.lock(ticket.gate) else platform.sync.lockYielding(ticket.gate);
    defer ticket.gate.unlock();
    if (ticket.permit) |permit| return permit.runWithControl(ticket.inputs, allocator, ticket.control);
    return ticket.session.runWithControl(ticket.inputs, allocator, ticket.control);
}

fn execute(_: *anyopaque, items: []const micro.ExecuteItem) void {
    // Regroup rows from the same immutable encoder allocation in source order.
    // Result slots travel with tickets, so request arrival order is irrelevant.
    var ordered: [8]micro.ExecuteItem = undefined;
    std.debug.assert(items.len <= ordered.len);
    @memcpy(ordered[0..items.len], items);
    std.mem.sort(micro.ExecuteItem, ordered[0..items.len], {}, struct {
        fn key(item: micro.ExecuteItem) usize {
            var largest: usize = 0;
            var address: usize = 0;
            for (item.payloadAs(Ticket).inputs) |tensor| if (tensor.shared_storage != null and tensor.data.len > largest) {
                largest = tensor.data.len;
                address = @intFromPtr(tensor.data.ptr);
            };
            return address;
        }
        fn less(_: void, a: micro.ExecuteItem, b: micro.ExecuteItem) bool {
            return key(a) < key(b);
        }
    }.less);
    executeGroup(ordered[0..items.len]) catch |err| {
        for (items) |item| if (!item.slot.completed) item.slot.fail(err);
    };
}

/// Only explicit allocation provenance allows joining rows. Pointer adjacency
/// alone is insufficient: unrelated allocations may happen to be neighbors.
fn sharedColumn(items: []const micro.ExecuteItem, column: usize) ?[]u8 {
    const first = items[0].payloadAs(Ticket).inputs[column];
    const storage = first.shared_storage orelse return null;
    const base = @intFromPtr(storage.ptr);
    const start = @intFromPtr(first.data.ptr);
    if (start < base or start - base > storage.len) return null;
    var offset = start - base;
    for (items) |item| {
        const tensor = item.payloadAs(Ticket).inputs[column];
        const backing = tensor.shared_storage orelse return null;
        if (backing.ptr != storage.ptr or backing.len != storage.len or @intFromPtr(tensor.data.ptr) != base + offset) return null;
        if (tensor.data.len > storage.len - offset) return null;
        offset += tensor.data.len;
    }
    return storage[start - base .. offset];
}

fn serial(items: []const micro.ExecuteItem) void {
    for (items) |item| {
        item.control.check() catch |err| {
            item.slot.fail(err);
            continue;
        };
        const output = direct(std.heap.smp_allocator, item.payloadAs(Ticket).*) catch |err| {
            item.slot.fail(err);
            continue;
        };
        item.slot.setValue([]Tensor, output, .fallback);
    }
}

/// Capacity subdivision only happens before a forward. Already completed
/// subgroups are never replayed if a later subgroup fails.
fn subdivide(items: []const micro.ExecuteItem) void {
    std.debug.assert(items.len > 1);
    const split = items.len / 2;
    for ([_][]const micro.ExecuteItem{ items[0..split], items[split..] }) |part| {
        executeGroup(part) catch |err| {
            for (part) |item| if (!item.slot.completed) item.slot.fail(err);
        };
    }
}

fn executeGroup(items: []const micro.ExecuteItem) !void {
    if (items.len == 0) return;
    if (items.len == 1) {
        const output = try direct(std.heap.smp_allocator, items[0].payloadAs(Ticket).*);
        items[0].slot.physical_group_key = @intFromPtr(items[0].slot);
        items[0].slot.setValue([]Tensor, output, if (items[0].payloadAs(Ticket).rows > 1) .native_batch else .serial);
        return;
    }
    const alloc = std.heap.smp_allocator;
    const first = items[0].payloadAs(Ticket);
    var rows: usize = 0;
    var bytes: usize = 0;
    var unadmitted_input_bytes: usize = 0;
    for (items) |item| {
        rows += item.payloadAs(Ticket).rows;
        for (item.payloadAs(Ticket).inputs) |tensor| {
            bytes = try std.math.add(usize, bytes, tensor.data.len);
        }
    }
    var request = try first.session.planRun(first.inputs, rows);
    request.input_bytes = bytes;
    request.pre_admitted_host_bytes = 0;
    for (first.inputs, 0..) |_, column| {
        const borrowing = sharedColumn(items, column) != null;
        for (items) |item| {
            const ticket = item.payloadAs(Ticket);
            const input = ticket.inputs[column];
            const covered = if (ticket.permit != null) input.data.len else try first.session.inputResidencyCredit(&.{input});
            if (borrowing) {
                // The logical model input is existing storage, not a new packed
                // allocation. Uncovered bytes are charged by input_bytes itself.
                request.pre_admitted_host_bytes = try std.math.add(usize, request.pre_admitted_host_bytes, covered);
            } else {
                unadmitted_input_bytes = try std.math.add(usize, unadmitted_input_bytes, input.data.len - covered);
            }
        }
    }
    // input_bytes covers the new packed allocation. Originals with a live
    // caller permit are already charged; retain only the uncovered originals.
    request.host_preprocess_bytes = unadmitted_input_bytes;
    if (first.session.run_admission) |admission| {
        if (!try (try admission.estimateRequest(request, first.session.outputInfo())).fitsLimits(admission.limits)) return subdivide(items);
    }
    var group = micro.ExecutionControl{ .items = items };
    const control = Control{
        .ptr = &group,
        .check_fn = micro.ExecutionControl.check,
        .io = items[0].control.io,
        .hard_cancellation = if (first.control) |active| active.hard_cancellation else null,
    };
    try control.lock(first.gate);
    var locked = true;
    defer if (locked) first.gate.unlock();
    // Caller permits now retain only input residency. Reserve one physical
    // forward, without charging each queued caller's idle compute workspace.
    var permit = first.session.admit(request) catch |err| switch (err) {
        error.ResourceLimitExceeded, error.ResourceTemporarilyUnavailable => {
            first.gate.unlock();
            locked = false;
            return subdivide(items);
        },
    };
    var owns_permit = true;
    defer if (owns_permit) permit.deinit();
    const batch_inputs = try alloc.alloc(Tensor, first.inputs.len);
    var owns_inputs = true;
    var initialized: usize = 0;
    defer if (owns_inputs) {
        for (batch_inputs[0..initialized]) |*tensor| tensor.deinit();
        alloc.free(batch_inputs);
    };
    for (first.inputs, 0..) |input, column| {
        if (first.session.isBroadcastInput(input.name)) {
            batch_inputs[column] = input.borrowedView(input.name);
            initialized += 1;
            continue;
        }
        const shape = try alloc.dupe(i64, input.shape);
        errdefer alloc.free(shape);
        shape[0] = @intCast(rows);
        const shared = sharedColumn(items, column);
        const data = shared orelse try alloc.alloc(u8, input.data.len / first.rows * rows);
        var offset: usize = 0;
        if (shared == null) for (items) |item| {
            const source = item.payloadAs(Ticket).inputs[column].data;
            @memcpy(data[offset..][0..source.len], source);
            offset += source.len;
        };
        batch_inputs[column] = .{ .data = data, .shape = shape, .dtype = input.dtype, .name = input.name, .allocator = alloc, .owns_data = shared == null, .owns_shape = true };
        initialized += 1;
    }
    const outputs = try permit.runWithControl(batch_inputs, alloc, control);
    var owns_outputs = true;
    defer if (owns_outputs) destroy(alloc, outputs);
    if (outputs.len == 0 or outputs.len != first.session.outputInfo().len) return error.InvalidFusedOutputShape;
    for (outputs, first.session.outputInfo()) |tensor, info| {
        const empty_cross_cache = first.session.cached_decoder_geometry != null and
            std.mem.startsWith(u8, tensor.name, "present.") and std.mem.indexOf(u8, tensor.name, ".encoder.") != null and
            tensor.data.len == 0 and tensor.shape.len == 4 and tensor.shape[0] == 0;
        if (!validTensor(tensor) or tensor.dtype != info.dtype or tensor.shape.len == 0 or (tensor.shape[0] != rows and !empty_cross_cache)) return error.InvalidFusedOutputShape;
        // Native multi-entry sessions qualify concrete stages independently
        // of their summary metadata. Graph contracts must match exactly.
        if (first.session.vtable.independentBatchRows == null) {
            if (tensor.shape.len != info.shape.len) return error.InvalidFusedOutputShape;
            if (!empty_cross_cache) for (tensor.shape[1..], info.shape[1..]) |actual, declared| if (declared > 0 and actual != declared) return error.InvalidFusedOutputShape;
        }
    }
    // No backend scratch or input packing survives this invocation. Keep only
    // the actual materialized outputs admitted across downstream decode steps.
    for (batch_inputs) |*tensor| tensor.deinit();
    alloc.free(batch_inputs);
    owns_inputs = false;
    try permit.retainOutputs(outputs);
    first.gate.unlock();
    locked = false;
    const owner = try alloc.create(SharedOutputs);
    errdefer alloc.destroy(owner);
    const columns = try alloc.alloc(SharedOutputs.Column, outputs.len);
    errdefer alloc.free(columns);
    const credits = if (permit.lease) |lease| try session_mod.OutputCredits.init(alloc, outputs, lease.amounts.host_kv_bytes > 0, true) else null;
    owner.* = .{ .allocator = alloc, .outputs = outputs, .permit = permit, .columns = columns, .remaining = .init(outputs.len), .credits = credits };
    for (columns, 0..) |*column, index| column.* = .{ .owner = owner, .index = index };
    owns_outputs = false;
    owns_permit = false;
    defer SharedOutputs.release(owner);
    var offset: usize = 0;
    for (items) |item| {
        const count = item.payloadAs(Ticket).rows;
        const start = offset;
        offset += count;
        item.control.check() catch |err| {
            item.slot.fail(err);
            continue;
        };
        const views = owner.rows(start, count, rows) catch |err| {
            item.slot.fail(err);
            continue;
        };
        item.slot.physical_group_key = @intFromPtr(items[0].slot);
        item.slot.setValue([]Tensor, views, .native_batch);
    }
}

const TestSession = struct {
    expanded: bool = false,
    last_input: usize = 0,
    calls: std.atomic.Value(usize) = .init(0),
    invalid_output: bool = false,
    cancel_after_forward: ?*std.atomic.Value(bool) = null,
    const Info = @import("../backends/tensor.zig").TensorInfo;
    const vtable = session_mod.Session.VTable{
        .run = forward,
        .runWithControl = controlled,
        .inputInfo = info,
        .outputInfo = info,
        .backend = backend,
        .close = close,
    };
    fn info(_: *anyopaque) []const Info {
        return &.{.{ .name = "values", .dtype = .f32, .shape = &.{ -1, -1 } }};
    }
    fn backend(_: *anyopaque) @import("../backends/backends.zig").BackendType {
        return .native;
    }
    fn close(_: *anyopaque) void {}
    fn controlled(raw: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator, control: Control) ![]Tensor {
        try control.check();
        return forward(raw, inputs, allocator);
    }
    fn forward(raw: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) ![]Tensor {
        const self: *@This() = @ptrCast(@alignCast(raw));
        _ = self.calls.fetchAdd(1, .monotonic);
        self.last_input = @intFromPtr(inputs[0].data.ptr);
        if (self.expanded) {
            const batch: usize = @intCast(inputs[0].shape[0]);
            const values = try allocator.alloc(f32, batch * 64);
            defer allocator.free(values);
            @memset(values, 2);
            const outputs = try allocator.alloc(Tensor, 1);
            errdefer allocator.free(outputs);
            outputs[0] = try Tensor.initFloat32(allocator, "values", &.{ @intCast(batch), 64 }, values);
            return outputs;
        }
        const values = try allocator.dupe(f32, inputs[0].asFloat32());
        defer allocator.free(values);
        for (values) |*value| value.* *= 2;
        var tensor = try Tensor.initFloat32(allocator, "values", if (self.invalid_output) &.{ 1, 1 } else inputs[0].shape, values);
        errdefer tensor.deinit();
        const outputs = try allocator.alloc(Tensor, 1);
        outputs[0] = tensor;
        if (self.cancel_after_forward) |flag| flag.store(true, .release);
        return outputs;
    }
};

const TestSubmit = struct {
    broker: *micro.Broker,
    session: session_mod.Session,
    gate: *std.atomic.Mutex,
    inputs: []const Tensor,
    task: micro.Task = .rerank,
    control: ?Control = null,
    output: ?[]Tensor = null,
    err: ?anyerror = null,
    permit: ?*session_mod.RunPermit = null,

    fn submit(self: *@This()) std.Io.Cancelable!void {
        self.output = run(self.broker, std.testing.allocator, std.testing.io, self.task, self.session, self.permit, self.gate, self.inputs, self.control, null, 500_000) catch |err| {
            self.err = err;
            return;
        };
    }
    fn deinit(self: *@This()) void {
        if (self.output) |output| destroy(std.testing.allocator, output);
        self.output = null;
    }
};

test "tensor microbatch fuses forwards and retains zero-copy outputs through last consumer" {
    const memory = @import("../runtime/tier/memory.zig");
    var controller = memory.AdmissionController{};
    var fake = TestSession{};
    const session = session_mod.Session{ .ptr = &fake, .vtable = &TestSession.vtable, .run_admission = .{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = .{},
        .static_workspace_bytes = 1,
        .check_live_memory = false,
    } };
    var broker = micro.Broker.init(std.testing.allocator);
    defer broker.deinit();
    var gate: std.atomic.Mutex = .unlocked;
    var input = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 2, 2 }, &.{ 1, 2, 3, 4 });
    defer input.deinit();
    var callers: [8]TestSubmit = undefined;
    for (&callers) |*caller| caller.* = .{ .broker = &broker, .session = session, .gate = &gate, .inputs = &.{input} };
    defer for (&callers) |*caller| caller.deinit();
    var group = std.Io.Group.init;
    defer group.cancel(std.testing.io);
    for (&callers) |*caller| try group.concurrent(std.testing.io, TestSubmit.submit, .{caller});
    try group.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 1), fake.calls.load(.monotonic));
    for (&callers) |*caller| {
        if (caller.err) |err| return err;
        try std.testing.expectEqualSlices(f32, &.{ 2, 4, 6, 8 }, caller.output.?[0].asFloat32());
        try std.testing.expectEqualSlices(i64, &.{ 2, 2 }, caller.output.?[0].shape);
        try std.testing.expectEqual(callers[0].output.?[0].lifetime.?.context, caller.output.?[0].lifetime.?.context);
    }
    for (callers[0..7]) |*caller| caller.deinit();
    try std.testing.expectEqual(@as(usize, 8 * input.data.len + 2 * @sizeOf(i64)), controller.snapshot().host_scratch_bytes);
    try std.testing.expectEqualSlices(f32, &.{ 2, 4, 6, 8 }, callers[7].output.?[0].asFloat32());
    callers[7].deinit();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
    try std.testing.expectEqual(@as(u64, 8), broker.snapshot(std.testing.io).native_items);
}

test "tensor microbatch broadcasts scalar controls and separates unequal values" {
    const Probe = struct {
        fn info(_: *anyopaque) []const TestSession.Info {
            return &.{
                .{ .name = "values", .dtype = .f32, .shape = &.{ -1, -1 } },
                .{ .name = "branch", .dtype = .bool_, .shape = &.{} },
            };
        }
        fn forward(raw: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) ![]Tensor {
            try std.testing.expectEqual(@as(usize, 0), inputs[1].shape.len);
            try std.testing.expectEqual(@as(usize, 1), inputs[1].data.len);
            return TestSession.forward(raw, inputs, allocator);
        }
    };
    var fake = TestSession{};
    var vtable = TestSession.vtable;
    vtable.inputInfo = Probe.info;
    vtable.run = Probe.forward;
    const session = session_mod.Session{ .ptr = &fake, .vtable = &vtable, .broadcast_inputs = &.{"branch"} };
    var broker = micro.Broker.init(std.testing.allocator);
    defer broker.deinit();
    var gate = std.atomic.Mutex.unlocked;
    var input = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 2, 2 }, &.{ 1, 2, 3, 4 });
    defer input.deinit();
    var branch_data = [_]u8{ 0, 1 };
    var inputs: [2][2]Tensor = undefined;
    for (&inputs, 0..) |*values, i| values.* = .{ input, .{ .name = "branch", .dtype = .bool_, .shape = &.{}, .data = branch_data[i..][0..1], .allocator = std.testing.allocator, .owns_shape = false, .owns_data = false } };
    var callers: [4]TestSubmit = undefined;
    for (&callers, 0..) |*caller, i| caller.* = .{ .broker = &broker, .session = session, .gate = &gate, .inputs = &inputs[i % 2] };
    defer for (&callers) |*caller| caller.deinit();
    var group = std.Io.Group.init;
    defer group.cancel(std.testing.io);
    for (&callers) |*caller| try group.concurrent(std.testing.io, TestSubmit.submit, .{caller});
    try group.await(std.testing.io);
    for (&callers) |*caller| {
        if (caller.err) |err| return err;
        try std.testing.expectEqualSlices(f32, &.{ 2, 4, 6, 8 }, caller.output.?[0].asFloat32());
    }
    try std.testing.expectEqual(@as(usize, 2), fake.calls.load(.monotonic));
}

test "tensor microbatch rejoins shared encoder rows without repacking and restores request order" {
    const memory = @import("../runtime/tier/memory.zig");
    var controller = memory.AdmissionController{};
    var fake = TestSession{};
    var session = session_mod.Session{ .ptr = &fake, .vtable = &TestSession.vtable, .run_admission = .{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = .{ .host_limit_bytes = 2000 },
        .static_workspace_bytes = 1,
        .check_live_memory = false,
    } };
    var broker = micro.Broker.init(std.testing.allocator);
    defer broker.deinit();
    var gate: std.atomic.Mutex = .unlocked;
    var inputs: [8]Tensor = undefined;
    var first: [8]TestSubmit = undefined;
    for (&inputs, &first, 0..) |*input, *caller, i| {
        input.* = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 1, 2 }, &.{ @floatFromInt(i + 1), 1 });
        caller.* = .{ .broker = &broker, .session = session, .gate = &gate, .inputs = input[0..1] };
    }
    defer for (&inputs) |*input| input.deinit();
    defer for (&first) |*caller| caller.deinit();
    var group = std.Io.Group.init;
    defer group.cancel(std.testing.io);
    for (&first) |*caller| try group.concurrent(std.testing.io, TestSubmit.submit, .{caller});
    try group.await(std.testing.io);
    for (first) |caller| if (caller.err) |err| return err;
    const backing = first[0].output.?[0].shared_storage.?;
    // Retained source (80) + physical execution (896) fits; fictitious
    // duplicate originals/packing would exceed 1000 and force singleton calls.
    session.run_admission.?.limits.host_limit_bytes = 1000;
    try std.testing.expectEqual(@as(usize, 8), try session.inputResidencyCredit(first[0].output.?));
    var other_controller = memory.AdmissionController{};
    var other_session = session;
    other_session.run_admission.?.controller = &other_controller;
    try std.testing.expectEqual(@as(usize, 0), try other_session.inputResidencyCredit(first[0].output.?));
    var renamed: [8]Tensor = undefined;
    var second: [8]TestSubmit = undefined;
    for (&renamed, &second, 0..) |*input, *caller, i| {
        input.* = first[7 - i].output.?[0].borrowedView("values");
        caller.* = .{ .broker = &broker, .session = session, .gate = &gate, .inputs = input[0..1] };
    }
    defer for (&second) |*caller| caller.deinit();
    for (&second) |*caller| try group.concurrent(std.testing.io, TestSubmit.submit, .{caller});
    try group.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 2), fake.calls.load(.monotonic));
    try std.testing.expectEqual(@intFromPtr(backing.ptr), fake.last_input);
    for (second, 0..) |caller, i| {
        if (caller.err) |err| return err;
        try std.testing.expectEqualSlices(f32, &.{ @floatFromInt(4 * (8 - i)), 4 }, caller.output.?[0].asFloat32());
    }
    for (&second) |*caller| caller.deinit();
    for (&first) |*caller| caller.deinit();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "tensor microbatch yields idle workspace and permits remain reusable" {
    const memory = @import("../runtime/tier/memory.zig");
    var controller = memory.AdmissionController{};
    var fake = TestSession{};
    const session = session_mod.Session{ .ptr = &fake, .vtable = &TestSession.vtable, .run_admission = .{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = .{ .host_limit_bytes = 8500 },
        .static_workspace_bytes = 1024,
        .check_live_memory = false,
    } };
    var broker = micro.Broker.init(std.testing.allocator);
    defer broker.deinit();
    var gate: std.atomic.Mutex = .unlocked;
    var input = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 1, 2 }, &.{ 1, 2 });
    defer input.deinit();
    var permits: [8]session_mod.RunPermit = undefined;
    var count: usize = 0;
    defer for (permits[0..count]) |*permit| permit.deinit();
    for (&permits) |*permit| {
        permit.* = try session.admit(try session_mod.RunRequest.fromTensors(&.{input}));
        count += 1;
    }
    var callers: [8]TestSubmit = undefined;
    for (&callers, &permits) |*caller, *permit| caller.* = .{ .broker = &broker, .session = session, .gate = &gate, .inputs = &.{input}, .permit = permit };
    defer for (&callers) |*caller| caller.deinit();
    var group = std.Io.Group.init;
    defer group.cancel(std.testing.io);
    for (&callers) |*caller| try group.concurrent(std.testing.io, TestSubmit.submit, .{caller});
    try group.await(std.testing.io);
    for (callers) |caller| if (caller.err) |err| return err;
    try std.testing.expectEqual(@as(usize, 1), fake.calls.load(.monotonic));
    for (&callers) |*caller| caller.deinit();
    try std.testing.expectEqual(@as(usize, 8 * input.data.len), controller.snapshot().host_scratch_bytes);
    const reused = try permits[0].run(&.{input}, std.testing.allocator);
    destroy(std.testing.allocator, reused);
    try std.testing.expectEqual(@as(usize, 8 * input.data.len), controller.snapshot().host_scratch_bytes);
    for (&permits) |*permit| permit.deinit();
    count = 0;
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "tensor microbatch admits expanded GPU outputs before forwarding" {
    const memory = @import("../runtime/tier/memory.zig");
    const Geometry = struct {
        fn plan(_: *anyopaque, _: session_mod.ShapeInputs, batch: usize) !?session_mod.RunGeometry {
            return .{ .sequence = 2, .output_bytes = batch * 64 * 4 + 16 };
        }
    };
    var controller = memory.AdmissionController{};
    var fake = TestSession{ .expanded = true };
    var vtable = TestSession.vtable;
    vtable.runGeometry = Geometry.plan;
    const session = session_mod.Session{ .ptr = &fake, .vtable = &vtable, .run_admission = .{
        .controller = &controller,
        .backend_class = .gpu,
        .limits = .{ .host_limit_bytes = 5000 },
        .static_workspace_bytes = 1,
        .check_live_memory = false,
    } };
    var broker = micro.Broker.init(std.testing.allocator);
    defer broker.deinit();
    var gate: std.atomic.Mutex = .unlocked;
    var input = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 1, 2 }, &.{ 1, 2 });
    defer input.deinit();
    var callers: [8]TestSubmit = undefined;
    for (&callers) |*caller| caller.* = .{ .broker = &broker, .session = session, .gate = &gate, .inputs = &.{input} };
    defer for (&callers) |*caller| caller.deinit();
    var group = std.Io.Group.init;
    defer group.cancel(std.testing.io);
    for (&callers) |*caller| try group.concurrent(std.testing.io, TestSubmit.submit, .{caller});
    try group.await(std.testing.io);
    for (&callers) |*caller| {
        if (caller.err) |err| return err;
        try std.testing.expectEqual(@as(usize, 64), caller.output.?[0].asFloat32().len);
    }
    try std.testing.expectEqual(@as(usize, 1), fake.calls.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 8 * 64 * 4 + 16), controller.snapshot().host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
    for (&callers) |*caller| caller.deinit();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "tensor microbatch reusable permit expands native output geometry" {
    const memory = @import("../runtime/tier/memory.zig");
    const Geometry = struct {
        fn plan(_: *anyopaque, _: session_mod.ShapeInputs, batch: usize) !?session_mod.RunGeometry {
            return .{ .sequence = 2, .output_bytes = batch * 64 * 4 + 16 };
        }
    };
    var controller = memory.AdmissionController{};
    var fake = TestSession{ .expanded = true };
    var vtable = TestSession.vtable;
    vtable.runGeometry = Geometry.plan;
    const session = session_mod.Session{ .ptr = &fake, .vtable = &vtable, .run_admission = .{
        .controller = &controller,
        .backend_class = .gpu,
        .limits = .{ .host_limit_bytes = 5000 },
        .static_workspace_bytes = 1,
        .check_live_memory = false,
    } };
    var input = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 1, 2 }, &.{ 1, 2 });
    defer input.deinit();
    var larger = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 2, 2 }, &.{ 1, 2, 3, 4 });
    defer larger.deinit();
    var permit = try session.admit(try session_mod.RunRequest.fromTensors(&.{larger}));
    defer permit.deinit();
    const small_outputs = try permit.run(&.{input}, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 64 * 4 + 16), permit.request.?.output_bytes.?);
    destroy(std.testing.allocator, small_outputs);
    const large_outputs = try permit.run(&.{larger}, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2 * 64 * 4 + 16), permit.request.?.output_bytes.?);
    try std.testing.expectEqual(@as(usize, 128), large_outputs[0].asFloat32().len);
    try std.testing.expectEqual(@as(usize, larger.data.len + 2 * 64 * 4 + 16), controller.snapshot().host_scratch_bytes);
    destroy(std.testing.allocator, large_outputs);
    permit.deinit();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "tensor microbatch rejects ambiguous input layouts and fixed batch graphs" {
    var fake = TestSession{};
    var vtable = TestSession.vtable;
    const session = session_mod.Session{ .ptr = &fake, .vtable = &vtable };
    var input = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 1, 2 }, &.{ 1, 2 });
    defer input.deinit();
    try std.testing.expect(eligible(session, &.{input}));
    try std.testing.expect(!eligible(session, &.{ input, input }));
    var bad = input.borrowedView("values");
    bad.shape = &.{ 2, 2 };
    try std.testing.expect(!eligible(session, &.{bad}));
    const Fixed = struct {
        fn info(_: *anyopaque) []const TestSession.Info {
            return &.{.{ .name = "values", .dtype = .f32, .shape = &.{ 1, 2 } }};
        }
        fn veto(_: *anyopaque, _: []const Tensor) bool {
            return false;
        }
    };
    vtable.outputInfo = Fixed.info;
    try std.testing.expect(!eligible(session, &.{input}));
    vtable.outputInfo = TestSession.info;
    vtable.independentBatchRows = Fixed.veto;
    try std.testing.expect(!eligible(session, &.{input}));
}

test "tensor microbatch isolates tasks and fails malformed fused output without replay" {
    for ([_]bool{ false, true }) |malformed| {
        var fake = TestSession{ .invalid_output = malformed };
        const session = session_mod.Session{ .ptr = &fake, .vtable = &TestSession.vtable };
        var broker = micro.Broker.init(std.testing.allocator);
        defer broker.deinit();
        var gate: std.atomic.Mutex = .unlocked;
        var input = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 1, 2 }, &.{ 1, 2 });
        defer input.deinit();
        var callers: [8]TestSubmit = undefined;
        for (&callers, 0..) |*caller, i| caller.* = .{ .broker = &broker, .session = session, .gate = &gate, .inputs = &.{input}, .task = if (!malformed and i >= 4) .extract else .rerank };
        defer for (&callers) |*caller| caller.deinit();
        var group = std.Io.Group.init;
        defer group.cancel(std.testing.io);
        for (&callers) |*caller| try group.concurrent(std.testing.io, TestSubmit.submit, .{caller});
        try group.await(std.testing.io);
        try std.testing.expectEqual(@as(usize, if (malformed) 1 else 2), fake.calls.load(.monotonic));
        for (callers) |caller| {
            if (malformed) try std.testing.expectEqual(error.InvalidFusedOutputShape, caller.err.?) else if (caller.err) |err| return err;
        }
    }
}

test "tensor microbatch row-view ownership unwinds every allocation failure" {
    const Probe = struct {
        fn create(allocator: std.mem.Allocator) !*SharedOutputs {
            const owner = try allocator.create(SharedOutputs);
            errdefer allocator.destroy(owner);
            const outputs = try allocator.alloc(Tensor, 2);
            errdefer allocator.free(outputs);
            outputs[0] = try Tensor.initFloat32(allocator, "a", &.{ 2, 1 }, &.{ 1, 2 });
            errdefer outputs[0].deinit();
            outputs[1] = try Tensor.initFloat32(allocator, "b", &.{ 2, 1 }, &.{ 3, 4 });
            errdefer outputs[1].deinit();
            const columns = try allocator.alloc(SharedOutputs.Column, outputs.len);
            owner.* = .{ .allocator = allocator, .outputs = outputs, .permit = null, .columns = columns, .remaining = .init(outputs.len) };
            for (columns, 0..) |*column, index| column.* = .{ .owner = owner, .index = index };
            return owner;
        }
        fn check(allocator: std.mem.Allocator) !void {
            const owner = try create(allocator);
            defer SharedOutputs.release(owner);
            const view = try owner.rows(1, 1, 2);
            defer destroy(owner.allocator, view);
            std.debug.assert(view[0].asFloat32()[0] == 2);
            std.debug.assert(view[1].asFloat32()[0] == 4);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.check, .{});
}

test "tensor microbatch admission subdivision happens before any fused forward" {
    for ([_]bool{ false, true }) |permanent| {
        const memory = @import("../runtime/tier/memory.zig");
        var controller = memory.AdmissionController{};
        if (!permanent) controller.configureForcedRunDenialsForTesting(1);
        var fake = TestSession{};
        const session = session_mod.Session{ .ptr = &fake, .vtable = &TestSession.vtable, .run_admission = .{
            .controller = &controller,
            .backend_class = .cpu,
            .limits = if (permanent) .{ .host_limit_bytes = 800 } else .{},
            .static_workspace_bytes = 1,
            .check_live_memory = false,
        } };
        var broker = micro.Broker.init(std.testing.allocator);
        defer broker.deinit();
        var gate: std.atomic.Mutex = .unlocked;
        var input = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 1, 2 }, &.{ 1, 2 });
        defer input.deinit();
        var callers: [8]TestSubmit = undefined;
        for (&callers) |*caller| caller.* = .{ .broker = &broker, .session = session, .gate = &gate, .inputs = &.{input} };
        defer for (&callers) |*caller| caller.deinit();
        var group = std.Io.Group.init;
        defer group.cancel(std.testing.io);
        for (&callers) |*caller| try group.concurrent(std.testing.io, TestSubmit.submit, .{caller});
        try group.await(std.testing.io);
        try std.testing.expectEqual(@as(usize, 2), fake.calls.load(.monotonic));
        for (&callers) |*caller| {
            if (caller.err) |err| return err;
            try std.testing.expectEqualSlices(f32, &.{ 2, 4 }, caller.output.?[0].asFloat32());
            caller.deinit();
        }
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
        try std.testing.expectEqual(@as(u64, 8), broker.snapshot(std.testing.io).native_items);
        try std.testing.expectEqual(@as(u64, 2), broker.snapshot(std.testing.io).native_batches);
    }
}

test "tensor microbatch canceled consumer does not discard healthy fused rows" {
    var canceled = std.atomic.Value(bool).init(false);
    const Probe = struct {
        fn check(raw: ?*anyopaque) bool {
            const flag: *std.atomic.Value(bool) = @ptrCast(@alignCast(raw.?));
            return flag.load(.acquire);
        }
    };
    var fake = TestSession{ .cancel_after_forward = &canceled };
    const session = session_mod.Session{ .ptr = &fake, .vtable = &TestSession.vtable };
    var broker = micro.Broker.init(std.testing.allocator);
    defer broker.deinit();
    var gate: std.atomic.Mutex = .unlocked;
    var input = try Tensor.initFloat32(std.testing.allocator, "values", &.{ 1, 2 }, &.{ 1, 2 });
    defer input.deinit();
    var callers: [8]TestSubmit = undefined;
    for (&callers) |*caller| caller.* = .{ .broker = &broker, .session = session, .gate = &gate, .inputs = &.{input} };
    callers[0].control = .{ .cancellation = .{ .ptr = &canceled, .is_cancelled_fn = Probe.check } };
    defer for (&callers) |*caller| caller.deinit();
    var group = std.Io.Group.init;
    defer group.cancel(std.testing.io);
    for (&callers) |*caller| try group.concurrent(std.testing.io, TestSubmit.submit, .{caller});
    try group.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 1), fake.calls.load(.monotonic));
    const canceled_error = callers[0].err orelse return error.ExpectedCancellation;
    try std.testing.expect(canceled_error == error.Canceled or canceled_error == error.Cancelled);
    for (callers[1..]) |caller| {
        if (caller.err) |err| return err;
        try std.testing.expectEqualSlices(f32, &.{ 2, 4 }, caller.output.?[0].asFloat32());
    }
}
