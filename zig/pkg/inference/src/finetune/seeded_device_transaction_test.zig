// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const tx = @import("seeded_device_transaction.zig");
const ops = @import("../ops/ops.zig");
const ml = @import("ml").graph;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;
const CT = ops.CT;

/// Test-only executable backend contract. Every mutating call rejects a
/// protected source tensor; failures can occur after AdamW has changed private
/// buffers. Unused generic hooks remain undefined so fallback is never hidden.
const Fake = struct {
    allocator: Allocator,
    vtable: ops.ComputeBackend.VTable = undefined,
    kind: ops.BackendKind = .metal,
    live: usize = 0,
    calls: usize = 0,
    fail_at: ?usize = null,
    fail_after_adam: bool = false,
    adam_calls: usize = 0,
    largest_batch: usize = 0,
    external_frame: bool = false,
    downloads: usize = 0,
    const Tensor = struct { owner: *Fake, values: []f32, shape: ml.Shape, protected: bool };

    fn init(a: Allocator) Fake {
        var result = Fake{ .allocator = a };
        result.vtable.backendKind = backendKind;
        result.vtable.freeTensor = free;
        result.vtable.residentTrainingPrimitive = primitive;
        result.vtable.residentTrainingInstruction = instruction;
        result.vtable.residentTrainingNorm = norm;
        result.vtable.trainingAdamWManyF32 = adam;
        result.vtable.trainingSynchronize = synchronize;
        return result;
    }
    fn cb(self: *Fake) ops.ComputeBackend {
        return .{ .ptr = self, .vtable = &self.vtable };
    }
    fn deinit(self: *Fake) void {
        std.debug.assert(self.live == 0);
    }
    fn from(raw: *anyopaque) *Fake {
        return @ptrCast(@alignCast(raw));
    }
    fn get(self: *Fake, value: CT) !*Tensor {
        const tensor: *Tensor = @ptrCast(@alignCast(value));
        if (tensor.owner != self) return error.ForeignResidentTrainingTensor;
        return tensor;
    }
    fn tick(self: *Fake) !void {
        self.calls += 1;
        if (self.fail_at == self.calls) return error.InjectedDeviceFailure;
        if (self.external_frame) return error.ResidentTrainingExternalFrame;
    }
    fn make(self: *Fake, values: []const f32, shape: ml.Shape, protected: bool) !CT {
        const owned = try self.allocator.dupe(f32, values);
        errdefer self.allocator.free(owned);
        const tensor = try self.allocator.create(Tensor);
        tensor.* = .{ .owner = self, .values = owned, .shape = shape, .protected = protected };
        self.live += 1;
        return tensor;
    }
    fn backendKind(raw: *anyopaque) ops.BackendKind {
        return from(raw).kind;
    }
    fn free(raw: *anyopaque, value: CT) void {
        const self = from(raw);
        const tensor = self.get(value) catch unreachable;
        self.allocator.free(tensor.values);
        self.allocator.destroy(tensor);
        self.live -= 1;
    }
    fn synchronize(_: *anyopaque) anyerror!void {
        return error.UnqualifiedSynchronizeWasUsed;
    }
    fn primitive(raw: *anyopaque, request: *const ops.resident_training.Request, limits: ops.resident_training.Limits, _: ?Control) anyerror!CT {
        const self = from(raw);
        try self.tick();
        return switch (request.*) {
            .upload_f32 => |value| blk: {
                const count = try ops.resident_training.shapeElements(i32, value.shape, limits);
                if (count != value.values.len) return error.InvalidResidentTrainingShape;
                var shape = ml.Shape{ .dtype = .f32, .rank_ = @intCast(value.shape.len) };
                for (value.shape, 0..) |dim, i| shape.dims[i] = dim;
                break :blk self.make(value.values, shape, false);
            },
            .snapshot => |value| blk: {
                const tensor = try self.get(value.input);
                const count = try ops.resident_training.shapeElements(i32, value.shape, limits);
                if (count != tensor.values.len) return error.InvalidResidentTrainingShape;
                var shape = ml.Shape{ .dtype = .f32, .rank_ = @intCast(value.shape.len) };
                for (value.shape, 0..) |dim, i| shape.dims[i] = dim;
                break :blk self.make(tensor.values, shape, false);
            },
            else => error.UnexpectedDeviceTransactionPrimitive,
        };
    }
    fn instruction(raw: *anyopaque, inst: *const ops.resident_program.Instruction, inputs: []const CT, limits: ops.resident_program.Limits, _: ?Control) anyerror!CT {
        const self = from(raw);
        try self.tick();
        const geometry = try inst.validate(limits);
        if (inst.num_inputs != inputs.len) return error.InvalidResidentProgramShape;
        var tensors: [3]*Tensor = undefined;
        for (inputs, 0..) |value, i| {
            tensors[i] = try self.get(value);
            if (!tensors[i].shape.eq(inst.inputs[i])) return error.InvalidResidentProgramShape;
        }
        const values = try self.allocator.alloc(f32, geometry.output_elements);
        defer self.allocator.free(values);
        switch (inst.op) {
            .reshape => @memcpy(values, tensors[0].values),
            .broadcast_in_dim => @memset(values, tensors[0].values[0]),
            .add, .mul, .div => for (values, 0..) |*out, i| {
                const lhs = tensors[0].values[if (tensors[0].values.len == 1) 0 else i];
                const rhs = tensors[1].values[if (tensors[1].values.len == 1) 0 else i];
                out.* = switch (inst.op) {
                    .add => lhs + rhs,
                    .mul => lhs * rhs,
                    .div => lhs / rhs,
                    else => unreachable,
                };
            },
            else => return error.UnexpectedDeviceTransactionInstruction,
        }
        return self.make(values, inst.output, false);
    }
    fn norm(raw: *anyopaque, inputs: []const ops.resident_training.NormInput, limits: ops.resident_training.NormLimits, _: ?Control) anyerror!ops.resident_training.NormSummary {
        const self = from(raw);
        try self.tick();
        if (inputs.len > limits.max_tensors) return error.ResourceLimitExceeded;
        var squared: f64 = 0;
        var finite = true;
        var partials: usize = 0;
        var total: usize = 0;
        for (inputs) |value| {
            const tensor = try self.get(value.tensor);
            if (tensor.values.len != value.elem_count) return error.InvalidResidentTrainingShape;
            total += value.elem_count;
            partials += try std.math.divCeil(usize, value.elem_count, 1024) * 12;
            for (tensor.values) |number| {
                finite = finite and std.math.isFinite(number);
                squared += @as(f64, number) * number;
            }
        }
        if (total > limits.max_total_elements or partials > limits.max_partial_bytes) return error.ResourceLimitExceeded;
        self.downloads += inputs.len * 12;
        return .{ .sum_squares = squared, .norm = @sqrt(squared), .finite = finite, .tensor_count = inputs.len, .partial_bytes = partials, .download_bytes = inputs.len * 12 };
    }
    fn adam(raw: *anyopaque, inputs: []const ops.TrainingAdamWBatchInput, options: ops.TrainingAdamWBatchOptions) anyerror!void {
        const self = from(raw);
        try self.tick();
        self.adam_calls += 1;
        self.largest_batch = @max(self.largest_batch, inputs.len);
        for (inputs) |value| {
            const weight = try self.get(value.weight);
            const gradient = try self.get(value.grad);
            const m = try self.get(value.m);
            const v = try self.get(value.v);
            if (weight.protected or gradient.protected or m.protected or v.protected) return error.BorrowedDeviceOptimizerStateMutated;
            for (weight.values, gradient.values, m.values, v.values) |*w, *g, *first, *second| {
                const scaled = g.* * options.grad_scale;
                first.* = options.beta1 * first.* + (1 - options.beta1) * scaled;
                second.* = options.beta2 * second.* + (1 - options.beta2) * scaled * scaled;
                const m_hat = first.* / value.bias_correction1;
                const v_hat = second.* / value.bias_correction2;
                w.* -= options.lr * (m_hat / (@sqrt(v_hat) + options.eps) + options.weight_decay * w.*);
                g.* = 0;
            }
            if (self.fail_after_adam) return error.InjectedDeviceFailure;
        }
        try self.tick();
    }
};

const StateOwner = struct {
    allocator: Allocator,
    cb: ops.ComputeBackend,
    slots: []tx.Slot,
    initialized: usize = 0,
    state: tx.State,

    fn init(a: Allocator, cb: ops.ComputeBackend, count: usize, configured: []const tx.Group) !StateOwner {
        const slots = try a.alloc(tx.Slot, count);
        var owner = StateOwner{ .allocator = a, .cb = cb, .slots = slots, .state = .{ .identity = .{ .optimizer_step = 0, .microbatch_step = 0 }, .accumulated_microbatches = 0, .grad_accum_steps = 2, .slots = slots, .groups = configured } };
        errdefer owner.deinit();
        for (slots) |*slot| {
            const weight = try upload(&cb, &.{ 2, -1 }, &.{2});
            errdefer cb.free(weight);
            const accum = try upload(&cb, &.{ 0, 0 }, &.{2});
            errdefer cb.free(accum);
            const m = try upload(&cb, &.{ 0, 0 }, &.{2});
            errdefer cb.free(m);
            const v = try upload(&cb, &.{ 0, 0 }, &.{2});
            slot.* = .{ .weight = weight, .grad_accum = accum, .m = m, .v = v, .shape = &.{2}, .group = 0, .adam_step = 0, .present = false };
            owner.initialized += 1;
        }
        return owner;
    }
    fn deinit(self: *StateOwner) void {
        for (self.slots[0..self.initialized]) |slot| for ([_]CT{ slot.weight, slot.grad_accum, slot.m, slot.v }) |tensor| self.cb.free(tensor);
        self.allocator.free(self.slots);
    }
    fn commit(self: *StateOwner, pending: *tx.Pending) void {
        for (0..pending.replacements.len) |i| {
            const next = pending.take(i);
            const slot = &self.slots[next.slot];
            self.cb.free(slot.grad_accum);
            slot.grad_accum = next.grad_accum.?;
            if (next.weight) |weight| {
                self.cb.free(slot.weight);
                self.cb.free(slot.m);
                self.cb.free(slot.v);
                slot.weight = weight;
                slot.m = next.m.?;
                slot.v = next.v.?;
            }
            slot.adam_step = next.adam_step;
            slot.present = next.present;
        }
        self.state.identity = pending.receipt.identity;
        self.state.accumulated_microbatches = pending.receipt.accumulated_microbatches;
    }
};
fn upload(cb: *const ops.ComputeBackend, values: []const f32, shape: []const i32) !CT {
    return cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = values, .shape = shape } }, .{});
}
const groups = [_]tx.Group{.{ .optimizer = .{ .weight_decay = 0.1 }, .schedule = .{ .constant = 0.05 } }};
fn protect(fake: *Fake, owner: *StateOwner) !void {
    for (owner.slots) |slot| {
        for ([_]CT{ slot.weight, slot.grad_accum, slot.m, slot.v }) |tensor| {
            (try fake.get(tensor)).protected = true;
        }
    }
}
fn unchanged(fake: *Fake, owner: *const StateOwner) !void {
    try std.testing.expectEqual(tx.Identity{ .optimizer_step = 0, .microbatch_step = 0 }, owner.state.identity);
    for (owner.slots) |slot| {
        try std.testing.expectEqualSlices(f32, &.{ 2, -1 }, (try fake.get(slot.weight)).values);
        for ([_]CT{ slot.grad_accum, slot.m, slot.v }) |tensor| try std.testing.expectEqualSlices(f32, &.{ 0, 0 }, (try fake.get(tensor)).values);
        try std.testing.expect(!slot.present and slot.adam_step == 0);
    }
}

test "seeded device transaction admission rejects shapes identities limits and duplicate gradients before work" {
    const a = std.testing.allocator;
    var fake = Fake.init(a);
    defer fake.deinit();
    const cb = fake.cb();
    var owner = try StateOwner.init(a, cb, 2, &groups);
    defer owner.deinit();
    const request = tx.Request{ .expected = owner.state.identity, .action = .submit, .loss = 1, .gradients = &.{.{ .slot = 0, .value = .zero }} };
    const plan = try tx.estimate(owner.state, request, .{});
    try std.testing.expectEqual(@as(usize, 1), plan.selected_slots);
    try std.testing.expect(!plan.optimizer_stepped);
    const before = fake.calls;
    try std.testing.expectError(error.TrainingOptimizerLimitExceeded, tx.prepare(a, &cb, owner.state, request, .{ .max_device_bytes = 1 }, null));
    try std.testing.expectError(error.TrainingOptimizerLimitExceeded, tx.prepare(a, &cb, owner.state, request, .{ .max_host_metadata_bytes = 4096 }, null));
    try std.testing.expectError(error.TrainingOptimizerLimitExceeded, tx.prepare(a, &cb, owner.state, request, .{ .max_scalar_transfer_bytes = 1 }, null));
    var invalid = request;
    invalid.expected.microbatch_step = 1;
    try std.testing.expectError(error.TrainingTapeIdentityMismatch, tx.prepare(a, &cb, owner.state, invalid, .{}, null));
    invalid = request;
    invalid.gradients = &.{ .{ .slot = 0, .value = .zero }, .{ .slot = 0, .value = .zero } };
    try std.testing.expectError(error.DuplicateTrainingGradient, tx.prepare(a, &cb, owner.state, invalid, .{}, null));
    invalid = request;
    invalid.loss = std.math.nan(f32);
    try std.testing.expectError(error.NonFiniteTrainingUpdate, tx.prepare(a, &cb, owner.state, invalid, .{}, null));
    owner.slots[0].shape = &.{ -1, 2 };
    try std.testing.expectError(error.InvalidResidentTrainingShape, tx.prepare(a, &cb, owner.state, request, .{}, null));
    owner.slots[0].shape = &.{2};
    try std.testing.expectEqual(before, fake.calls);
    fake.kind = .native;
    try std.testing.expectError(error.UnsupportedDeviceOptimizerBackend, tx.prepare(a, &cb, owner.state, request, .{}, null));
    try unchanged(&fake, &owner);
}

test "seeded device transaction preserves absent versus zero partial windows and exact counters" {
    const a = std.testing.allocator;
    var fake = Fake.init(a);
    defer fake.deinit();
    const cb = fake.cb();
    var owner = try StateOwner.init(a, cb, 3, &groups);
    defer owner.deinit();
    owner.state.grad_accum_steps = 3;
    owner.state.max_grad_norm = 0;
    const gradient = try upload(&cb, &.{ 3, -6 }, &.{2});
    defer cb.free(gradient);
    const inputs = [_]tx.Gradient{ .{ .slot = 0, .value = .{ .tensor = gradient } }, .{ .slot = 1, .value = .zero } };
    try protect(&fake, &owner);
    const first = try tx.prepare(a, &cb, owner.state, .{ .expected = owner.state.identity, .action = .submit, .loss = 4, .gradients = &inputs }, .{}, null);
    defer first.deinit();
    try unchanged(&fake, &owner);
    try std.testing.expectEqual(@as(usize, 2), first.replacements.len);
    try std.testing.expectEqualSlices(f32, &.{ 1, -2 }, (try fake.get(first.replacements[0].grad_accum.?)).values);
    owner.commit(first);
    try protect(&fake, &owner);
    const second = try tx.prepare(a, &cb, owner.state, .{ .expected = owner.state.identity, .action = .submit, .loss = 2 }, .{}, null);
    defer second.deinit();
    owner.commit(second);
    try protect(&fake, &owner);
    const final = try tx.prepare(a, &cb, owner.state, .{ .expected = owner.state.identity, .action = .flush }, .{}, null);
    defer final.deinit();
    try std.testing.expectApproxEqAbs(@sqrt(@as(f64, 11.25)), final.receipt.grad_norm, 1e-8);
    try std.testing.expectEqual(@as(usize, 2), final.replacements.len);
    owner.commit(final);
    try std.testing.expectEqual(tx.Identity{ .optimizer_step = 1, .microbatch_step = 2 }, owner.state.identity);
    try std.testing.expectEqual(@as(u32, 1), owner.slots[0].adam_step);
    try std.testing.expectEqual(@as(u32, 1), owner.slots[1].adam_step);
    try std.testing.expectEqual(@as(u32, 0), owner.slots[2].adam_step);
    try std.testing.expectEqualSlices(f32, &.{ 2, -1 }, (try fake.get(owner.slots[2].weight)).values);
    try std.testing.expectApproxEqAbs(@as(f32, 1.99), (try fake.get(owner.slots[1].weight)).values[0], 1e-7);
    for (owner.slots) |slot| try std.testing.expectEqualSlices(f32, &.{ 0, 0 }, (try fake.get(slot.grad_accum)).values);
    owner.state.identity = .{ .optimizer_step = 16_777_217, .microbatch_step = 16_777_219 };
    owner.state.grad_accum_steps = 1;
    owner.slots[1].adam_step = 16_777_217;
    const high = try tx.prepare(a, &cb, owner.state, .{ .expected = owner.state.identity, .action = .submit, .loss = 0, .gradients = &.{.{ .slot = 1, .value = .zero }} }, .{}, null);
    defer high.deinit();
    try std.testing.expectEqual(@as(u64, 16_777_218), high.receipt.identity.optimizer_step);
    try std.testing.expectEqual(@as(u64, 16_777_220), high.receipt.identity.microbatch_step);
    try std.testing.expectEqual(@as(u32, 16_777_218), high.replacements[0].adam_step);
}

fn exercise(a: Allocator, fail_at: ?usize, after_adam: bool, control: ?Control) !usize {
    var fake = Fake.init(a);
    defer fake.deinit();
    const cb = fake.cb();
    var owner = try StateOwner.init(a, cb, 2, &groups);
    defer owner.deinit();
    owner.state.grad_accum_steps = 1;
    try protect(&fake, &owner);
    const gradient = try upload(&cb, &.{ 2, -4 }, &.{2});
    defer cb.free(gradient);
    (try fake.get(gradient)).protected = true;
    fake.calls = 0;
    fake.fail_at = fail_at;
    fake.fail_after_adam = after_adam;
    const result = tx.prepare(a, &cb, owner.state, .{ .expected = owner.state.identity, .action = .submit, .loss = 7, .gradients = &.{ .{ .slot = 0, .value = .{ .tensor = gradient } }, .{ .slot = 1, .value = .zero } } }, .{}, control);
    // Check originals before propagating every injected failure.
    try unchanged(&fake, &owner);
    const pending = try result;
    defer pending.deinit();
    try std.testing.expect(pending.receipt.optimizer_stepped);
    try std.testing.expectEqual(@as(usize, 2), pending.replacements.len);
    try std.testing.expectEqual(@as(usize, 1), fake.adam_calls);
    return fake.calls;
}
fn exerciseAllocation(a: Allocator) !void {
    _ = try exercise(a, null, false, null);
}

test "seeded device transaction failures after mutation and every dispatch preserve the old epoch" {
    const count = try exercise(std.testing.allocator, null, false, null);
    for (1..count + 1) |index| try std.testing.expectError(error.InjectedDeviceFailure, exercise(std.testing.allocator, index, false, null));
    try std.testing.expectError(error.InjectedDeviceFailure, exercise(std.testing.allocator, null, true, null));
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseAllocation, .{});
}

test "seeded device transaction cancellation checks both controls and releases all private buffers" {
    const Canceller = struct {
        calls: usize = 0,
        at: ?usize = null,
        fn check(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == self.at) return error.Cancelled;
        }
    };
    var baseline = Canceller{};
    _ = try exercise(std.testing.allocator, null, false, .{ .ptr = &baseline, .check_fn = Canceller.check });
    for (1..baseline.calls + 1) |i| {
        var cancelled = Canceller{ .at = i };
        try std.testing.expectError(error.Cancelled, exercise(std.testing.allocator, null, false, .{ .ptr = &cancelled, .check_fn = Canceller.check }));
    }
    const a = std.testing.allocator;
    var fake = Fake.init(a);
    defer fake.deinit();
    var cb = fake.cb();
    var owner = try StateOwner.init(a, cb, 1, &groups);
    defer owner.deinit();
    var original = Canceller{ .at = 3 };
    var request = Canceller{};
    cb.execution_control = .{ .ptr = &original, .check_fn = Canceller.check };
    try std.testing.expectError(error.Cancelled, tx.prepare(a, &cb, owner.state, .{ .expected = owner.state.identity, .action = .submit, .loss = 1, .gradients = &.{.{ .slot = 0, .value = .zero }} }, .{}, .{ .ptr = &request, .check_fn = Canceller.check }));
    try std.testing.expect(request.calls > 0);
    try unchanged(&fake, &owner);
}

test "seeded device transaction rejects foreign physical shapes nonfinite values and dirty absent accumulators" {
    const a = std.testing.allocator;
    var fake = Fake.init(a);
    defer fake.deinit();
    const cb = fake.cb();
    var owner = try StateOwner.init(a, cb, 1, &groups);
    defer owner.deinit();
    const request = tx.Request{ .expected = owner.state.identity, .action = .submit, .loss = 1, .gradients = &.{.{ .slot = 0, .value = .zero }} };
    owner.slots[0].shape = &.{ 1, 2 };
    try std.testing.expectError(error.InvalidResidentProgramShape, tx.prepare(a, &cb, owner.state, request, .{}, null));
    owner.slots[0].shape = &.{2};
    (try fake.get(owner.slots[0].grad_accum)).values[0] = 1;
    try std.testing.expectError(error.InvalidDeviceOptimizerState, tx.prepare(a, &cb, owner.state, request, .{}, null));
    (try fake.get(owner.slots[0].grad_accum)).values[0] = 0;
    (try fake.get(owner.slots[0].weight)).values[0] = std.math.nan(f32);
    try std.testing.expectError(error.NonFiniteTrainingUpdate, tx.prepare(a, &cb, owner.state, request, .{}, null));
    (try fake.get(owner.slots[0].weight)).values[0] = 2;
    fake.external_frame = true;
    try std.testing.expectError(error.ResidentTrainingExternalFrame, tx.prepare(a, &cb, owner.state, request, .{}, null));
    fake.external_frame = false;
    var foreign = Fake.init(a);
    defer foreign.deinit();
    const foreign_cb = foreign.cb();
    const grad = try upload(&foreign_cb, &.{ 1, 2 }, &.{2});
    defer foreign_cb.free(grad);
    var invalid = request;
    invalid.gradients = &.{.{ .slot = 0, .value = .{ .tensor = grad } }};
    try std.testing.expectError(error.ForeignResidentTrainingTensor, tx.prepare(a, &cb, owner.state, invalid, .{}, null));
    try unchanged(&fake, &owner);
}

test "seeded device transaction caps mutating batches and leaves all-absent slots untouched" {
    const a = std.testing.allocator;
    var fake = Fake.init(a);
    defer fake.deinit();
    const cb = fake.cb();
    var owner = try StateOwner.init(a, cb, 260, &groups);
    defer owner.deinit();
    owner.state.grad_accum_steps = 1;
    try protect(&fake, &owner);
    const gradients = try a.alloc(tx.Gradient, owner.slots.len);
    defer a.free(gradients);
    for (gradients, 0..) |*gradient, i| gradient.* = .{ .slot = i, .value = .zero };
    const pending = try tx.prepare(a, &cb, owner.state, .{ .expected = owner.state.identity, .action = .submit, .loss = 0, .gradients = gradients }, .{}, null);
    defer pending.deinit();
    try std.testing.expectEqual(@as(usize, 2), fake.adam_calls);
    try std.testing.expectEqual(@as(usize, 256), fake.largest_batch);
    try std.testing.expect(pending.receipt.scalar_download_bytes <= pending.estimate.scalar_transfer_upper_bound_bytes);
    try unchanged(&fake, &owner);
    const absent = try tx.prepare(a, &cb, owner.state, .{ .expected = owner.state.identity, .action = .submit, .loss = 0 }, .{}, null);
    defer absent.deinit();
    try std.testing.expectEqual(@as(usize, 0), absent.replacements.len);
    try std.testing.expectEqual(tx.Identity{ .optimizer_step = 1, .microbatch_step = 1 }, absent.receipt.identity);
    try std.testing.expectEqual(@as(usize, 0), absent.receipt.scalar_download_bytes);
    try std.testing.expectEqual(@as(usize, 2), fake.adam_calls);
    owner.commit(absent);
    const empty_flush = try tx.prepare(a, &cb, owner.state, .{ .expected = owner.state.identity, .action = .flush }, .{}, null);
    defer empty_flush.deinit();
    try std.testing.expectEqual(owner.state.identity, empty_flush.receipt.identity);
    try std.testing.expect(!empty_flush.receipt.optimizer_stepped);
}

const Oracle = struct {
    optimizer: struct { betas: [2]f32, eps: f32, gradient_accumulation_steps: u32, max_grad_norm: f32 },
    parameters: []const struct { name: []const u8, shape: []const i32, initial: []const f32, lr: f32, weight_decay: f32 },
    microbatches: []const struct { gradients: std.json.ArrayHashMap(?[]const f32), flush: bool },
    flushes: []const struct {
        after_microbatch: u64,
        grad_norm: f64,
        parameters: std.json.ArrayHashMap(struct { weight: []const f32, step: u32, exp_avg: []const f32, exp_avg_sq: []const f32 }),
    },
};
fn diagnostic(a: Allocator, cb: *const ops.ComputeBackend, value: CT, expected: []const f32, fake: ?*Fake, tolerance: f32) !void {
    const values = if (fake) |backend|
        try a.dupe(f32, (try backend.get(value)).values)
    else blk: {
        const output = try a.alloc(f32, expected.len);
        errdefer a.free(output);
        try cb.glinerBoundaryDownload(value, output);
        break :blk output;
    };
    defer a.free(values);
    try std.testing.expectEqual(expected.len, values.len);
    for (expected, values) |want, got| try std.testing.expectApproxEqAbs(want, got, tolerance + @abs(want) * 2e-5);
}
fn measuredPrepare(a: Allocator, cb: *const ops.ComputeBackend, state: tx.State, request: tx.Request, fake: ?*Fake) !*tx.Pending {
    const mt = @import("../backends/metal_tensor.zig");
    const before = if (fake == null) mt.memoryStatsSnapshot() else null;
    const result = try tx.prepare(a, cb, state, request, .{}, null);
    errdefer result.deinit();
    if (before) |prior| {
        const after = mt.memoryStatsSnapshot();
        try std.testing.expectEqual(prior.to_host_device_calls, after.to_host_device_calls);
        try std.testing.expectEqual(prior.host_mirror_download_bytes, after.host_mirror_download_bytes);
    }
    return result;
}
fn oracle(cb: *const ops.ComputeBackend, fake: ?*Fake) !void {
    const a = std.testing.allocator;
    const bytes = try @import("../architectures/gliner_boundary_parity_test.zig").fixtureBytes(a, "training_adamw.json");
    defer a.free(bytes);
    const parsed = try std.json.parseFromSlice(Oracle, a, bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const f = parsed.value;
    const configured_groups = try a.alloc(tx.Group, f.parameters.len);
    defer a.free(configured_groups);
    for (configured_groups, f.parameters) |*group, parameter| group.* = .{
        .optimizer = .{ .beta1 = f.optimizer.betas[0], .beta2 = f.optimizer.betas[1], .eps = f.optimizer.eps, .weight_decay = parameter.weight_decay },
        .schedule = .{ .constant = parameter.lr },
    };
    var owner = try StateOwner.init(a, cb.*, f.parameters.len, configured_groups);
    defer owner.deinit();
    owner.state.grad_accum_steps = f.optimizer.gradient_accumulation_steps;
    owner.state.max_grad_norm = f.optimizer.max_grad_norm;
    for (owner.slots, f.parameters, 0..) |*slot, parameter, i| {
        const zeros = try a.alloc(f32, parameter.initial.len);
        defer a.free(zeros);
        @memset(zeros, 0);
        const weight = try upload(cb, parameter.initial, parameter.shape);
        errdefer cb.free(weight);
        const accum = try upload(cb, zeros, parameter.shape);
        errdefer cb.free(accum);
        const m = try upload(cb, zeros, parameter.shape);
        errdefer cb.free(m);
        const v = try upload(cb, zeros, parameter.shape);
        for ([_]CT{ slot.weight, slot.grad_accum, slot.m, slot.v }) |value| cb.free(value);
        slot.* = .{ .weight = weight, .grad_accum = accum, .m = m, .v = v, .shape = parameter.shape, .group = i, .adam_step = 0, .present = false };
    }
    var flush_index: usize = 0;
    for (f.microbatches) |microbatch| {
        if (fake) |backend| try protect(backend, &owner);
        var incoming = std.ArrayListUnmanaged(tx.Gradient).empty;
        defer {
            for (incoming.items) |gradient| if (gradient.value == .tensor) cb.free(gradient.value.tensor);
            incoming.deinit(a);
        }
        for (f.parameters, 0..) |parameter, index| {
            const expected = microbatch.gradients.map.get(parameter.name) orelse return error.InvalidFixture;
            if (expected) |values| {
                if (std.mem.allEqual(f32, values, 0)) {
                    try incoming.append(a, .{ .slot = index, .value = .zero });
                } else {
                    const gradient = try upload(cb, values, parameter.shape);
                    errdefer cb.free(gradient);
                    try incoming.append(a, .{ .slot = index, .value = .{ .tensor = gradient } });
                }
            }
        }
        const pending = try measuredPrepare(a, cb, owner.state, .{ .expected = owner.state.identity, .action = .submit, .loss = 1, .gradients = incoming.items }, fake);
        var receipt = pending.receipt;
        owner.commit(pending);
        pending.deinit();
        if (microbatch.flush and !receipt.optimizer_stepped) {
            if (fake) |backend| try protect(backend, &owner);
            const partial = try measuredPrepare(a, cb, owner.state, .{ .expected = owner.state.identity, .action = .flush }, fake);
            receipt = partial.receipt;
            owner.commit(partial);
            partial.deinit();
        }
        if (!microbatch.flush) {
            try std.testing.expect(!receipt.optimizer_stepped);
            continue;
        }
        const expected = f.flushes[flush_index];
        flush_index += 1;
        try std.testing.expect(receipt.optimizer_stepped);
        try std.testing.expectEqual(expected.after_microbatch, receipt.identity.microbatch_step);
        try std.testing.expectApproxEqAbs(expected.grad_norm, receipt.grad_norm, 1e-6);
        for (owner.slots, f.parameters) |slot, parameter| {
            const want = expected.parameters.map.get(parameter.name).?;
            errdefer std.debug.print("device AdamW parameter {s}, flush {d}\n", .{ parameter.name, flush_index });
            try std.testing.expectEqual(want.step, slot.adam_step);
            try std.testing.expect(!slot.present);
            try diagnostic(a, cb, slot.weight, want.weight, fake, 4e-7);
            try diagnostic(a, cb, slot.m, want.exp_avg, fake, 1e-7);
            try diagnostic(a, cb, slot.v, want.exp_avg_sq, fake, 1e-10);
            const zeros = try a.alloc(f32, parameter.initial.len);
            defer a.free(zeros);
            @memset(zeros, 0);
            try diagnostic(a, cb, slot.grad_accum, zeros, fake, 0);
        }
    }
    try std.testing.expectEqual(f.flushes.len, flush_index);
}

test "seeded device transaction contract matches pinned Torch groups clipping and partial flush" {
    var fake = Fake.init(std.testing.allocator);
    defer fake.deinit();
    const cb = fake.cb();
    try oracle(&cb, &fake);
}

test "seeded device transaction actual Metal matches pinned Torch with only scalar training readbacks" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    var device = try @import("../graph/resident_training_fixture.zig").Device.init(std.testing.allocator);
    defer device.deinit();
    const cb = device.backend.computeBackend();
    try oracle(&cb, null);
}
