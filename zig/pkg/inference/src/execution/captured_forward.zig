// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Model-owned capture/compile cache. Immutable programs and compiled operands
//! are shared; every concurrent invocation borrows a separate mutable frame.
const std = @import("std");
const platform = @import("antfly_platform");
const ops = @import("../ops/ops.zig");
const capture = @import("capture.zig");
const executable = @import("executable.zig");
const interpreter = @import("../graph/interpreter.zig");
const compiled = @import("../graph/compiled_backend.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Key = [32]u8;

/// Factory context belongs to the loaded model. A frame backend must own its
/// mutable state and use the supplied model allocator, never a request arena.
pub const Provider = struct {
    context: *anyopaque,
    create: *const fn (*anyopaque, std.mem.Allocator) anyerror!ops.ComputeBackend,
    import: *const fn (*anyopaque, *const ops.ComputeBackend, *const Tensor, std.mem.Allocator) anyerror!ops.CT,
    compiled_context: ?compiled.AttachContext = null,
};

const Frame = struct {
    backend: ops.ComputeBackend,
    executable: executable.Executable,
    busy: bool = false,
};

const Entry = struct {
    allocator: std.mem.Allocator,
    key: Key,
    provider: Provider,
    program: @import("program.zig").ModelProgram,
    references: std.atomic.Value(usize) = .init(1),
    mutex: std.atomic.Mutex = .unlocked,
    frames: std.ArrayListUnmanaged(*Frame) = .empty,
    creating: usize = 0,
    const max_frames = 8;

    fn keyOf(self: *Entry) Key {
        return self.key;
    }
    fn equal(a: Key, b: Key) bool {
        return std.mem.eql(u8, &a, &b);
    }
    fn retain(self: *Entry) *Entry {
        _ = self.references.fetchAdd(1, .monotonic);
        return self;
    }
    fn release(self: *Entry) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        std.debug.assert(self.creating == 0);
        // Borrowed compiled partitions retire before the owning first frame.
        var index = self.frames.items.len;
        while (index != 0) {
            index -= 1;
            const frame = self.frames.items[index];
            std.debug.assert(!frame.busy);
            frame.executable.deinit();
            frame.backend.deinit();
            self.allocator.destroy(frame);
        }
        self.frames.deinit(self.allocator);
        self.program.deinit();
        self.allocator.destroy(self);
    }
    fn weightShape(context: ?*anyopaque, allocator: std.mem.Allocator, name: []const u8) anyerror!?@import("ml").graph.Shape {
        const backend: *const ops.ComputeBackend = @ptrCast(@alignCast(context.?));
        const weight = try backend.getWeight(name);
        defer backend.free(weight);
        const shape = try backend.tensorShape(weight, allocator);
        defer allocator.free(shape);
        if (shape.len > 8) return error.UnsupportedShape;
        const dtype = try backend.tensorDType(weight);
        return @import("ml").graph.Shape.init(switch (dtype) {
            inline else => |tag| @field(@import("ml").graph.DType, @tagName(tag)),
        }, shape);
    }
    fn create(allocator: std.mem.Allocator, provider: Provider, forward: capture.Forward, inputs: []const capture.Input, key: Key) !*Entry {
        const frame = try allocator.create(Frame);
        errdefer allocator.destroy(frame);
        frame.backend = try provider.create(provider.context, allocator);
        errdefer frame.backend.deinit();
        var program = try capture.trace(allocator, forward, inputs, .{ .context = @constCast(&frame.backend), .resolve = weightShape });
        errdefer program.deinit();
        const entry = try allocator.create(Entry);
        errdefer allocator.destroy(entry);
        entry.* = .{ .allocator = allocator, .provider = provider, .key = key, .program = program };
        errdefer entry.frames.deinit(allocator);
        try entry.frames.ensureUnusedCapacity(allocator, 1);
        var options = executable.PrepareOptions{ .compiled = provider.compiled_context };
        if (options.compiled) |*context| context.cb = &frame.backend;
        frame.executable = try executable.prepareWithOptions(allocator, &entry.program, &frame.backend, if (provider.compiled_context != null) .compiled_required else .compiled_preferred, options);
        frame.busy = false;
        entry.frames.appendAssumeCapacity(frame);
        return entry;
    }
    fn acquireFrame(self: *Entry) !*Frame {
        platform.sync.lockYielding(&self.mutex);
        for (self.frames.items) |frame| if (!frame.busy) {
            frame.busy = true;
            self.mutex.unlock();
            return frame;
        };
        if (self.frames.items.len + self.creating == max_frames) {
            self.mutex.unlock();
            return error.QueueFull;
        }
        // Reserve publication metadata before unlocking. Another constructor
        // cannot consume this reservation: capacity covers every creator.
        self.frames.ensureTotalCapacity(self.allocator, self.frames.items.len + self.creating + 1) catch |err| {
            self.mutex.unlock();
            return err;
        };
        self.creating += 1;
        const source = &self.frames.items[0].executable.runtime;
        self.mutex.unlock();
        errdefer {
            platform.sync.lockYielding(&self.mutex);
            self.creating -= 1;
            self.mutex.unlock();
        }
        const frame = try self.allocator.create(Frame);
        errdefer self.allocator.destroy(frame);
        frame.backend = try self.provider.create(self.provider.context, self.allocator);
        errdefer frame.backend.deinit();
        var options = executable.PrepareOptions{ .compiled = self.provider.compiled_context, .compiled_source = if (self.provider.compiled_context != null) source else null };
        if (options.compiled) |*context| context.cb = &frame.backend;
        frame.executable = try executable.prepareWithOptions(self.allocator, &self.program, &frame.backend, if (self.provider.compiled_context != null) .compiled_required else .compiled_preferred, options);
        frame.busy = true;
        platform.sync.lockYielding(&self.mutex);
        self.creating -= 1;
        self.frames.appendAssumeCapacity(frame);
        self.mutex.unlock();
        return frame;
    }
    fn returnFrame(self: *Entry, frame: *Frame) void {
        platform.sync.lockYielding(&self.mutex);
        std.debug.assert(frame.busy);
        frame.busy = false;
        self.mutex.unlock();
    }
};

const Entries = @import("specialization_cache.zig").Cache(Key, *Entry, Entry.keyOf, Entry.equal);

pub const Cache = struct {
    mutex: std.atomic.Mutex = .unlocked,
    entries: Entries = Entries.init(4),
    parameters: ?*@import("../graph/pjrt_parameters.zig").Store = null,
    identity: ?Provider = null,

    fn sameProvider(a: Provider, b: Provider) bool {
        if (a.context != b.context or a.create != b.create or a.import != b.import or (a.compiled_context != null) != (b.compiled_context != null)) return false;
        if (a.compiled_context) |left| {
            const right = b.compiled_context.?;
            return left.requested_backend == right.requested_backend and left.pjrt_client == right.pjrt_client and left.attachment_target == right.attachment_target;
        }
        return true;
    }

    /// Caller holds the loaded model through every run; close follows quiesce.
    pub fn deinit(self: *Cache, allocator: std.mem.Allocator) void {
        for (self.entries.entries.items) |entry| entry.release();
        self.entries.deinit(allocator);
        if (self.parameters) |parameters| {
            parameters.deinit();
            allocator.destroy(parameters);
        }
    }
    fn acquire(self: *Cache, allocator: std.mem.Allocator, provider: Provider, forward: capture.Forward, inputs: []const capture.Input) !*Entry {
        const key = try capture.specializationKey(forward, inputs);
        var resolved_provider = provider;
        platform.sync.lockYielding(&self.mutex);
        if (self.identity) |identity| {
            if (!sameProvider(identity, provider)) {
                self.mutex.unlock();
                return error.ForeignModelProgramProvider;
            }
        } else self.identity = provider;
        if (resolved_provider.compiled_context) |*context| if (context.requested_backend == .pjrt) {
            const client: *@import("pjrt").pjrt.Client = @ptrCast(@alignCast(context.pjrt_client orelse {
                self.mutex.unlock();
                return error.MissingPjrtClient;
            }));
            if (self.parameters == null) {
                const parameters = allocator.create(@import("../graph/pjrt_parameters.zig").Store) catch |err| {
                    self.mutex.unlock();
                    return err;
                };
                parameters.* = .init(allocator, client);
                self.parameters = parameters;
            }
            if (self.parameters.?.client != client) {
                self.mutex.unlock();
                return error.ForeignCompiledProvider;
            }
            context.pjrt_parameter_store = @ptrCast(self.parameters.?);
        };
        if (self.entries.get(key)) |hit| {
            _ = hit.retain();
            self.mutex.unlock();
            return hit;
        }
        self.mutex.unlock();
        const built = try Entry.create(allocator, resolved_provider, forward, inputs, key);
        errdefer built.release();
        // Compilation/provider calls never run under the cache metadata lock.
        platform.sync.lockYielding(&self.mutex);
        if (self.entries.peek(key)) |hit| {
            _ = hit.retain();
            self.mutex.unlock();
            built.release();
            return hit;
        }
        self.entries.reserve(allocator) catch |err| {
            self.mutex.unlock();
            return err;
        };
        const victim = self.entries.insert(built.retain());
        self.mutex.unlock();
        if (victim) |old| old.release();
        return built;
    }
    pub fn run(self: *Cache, model_allocator: std.mem.Allocator, provider: Provider, forward: capture.Forward, inputs: []const capture.Input, allocator: std.mem.Allocator, control: ?Control) ![]Tensor {
        if (control) |active| try active.check();
        if (forward.validate) |validate| try validate(forward.context, inputs);
        const entry = try self.acquire(model_allocator, provider, forward, inputs);
        defer entry.release();
        const frame = try entry.acquireFrame();
        defer entry.returnFrame(frame);
        frame.backend.execution_control = control;
        defer frame.backend.execution_control = null;
        var runtime_inputs: std.ArrayList(interpreter.RuntimeInput) = .empty;
        defer runtime_inputs.deinit(allocator);
        defer for (runtime_inputs.items) |input| frame.backend.free(input.value);
        for (inputs) |input| {
            var node: ?@import("ml").graph.NodeId = null;
            for (entry.program.bindings) |binding| {
                if ((binding.kind == .input or binding.kind == .state_input) and std.mem.eql(u8, binding.name, input.tensor.name)) {
                    node = binding.node_id;
                    break;
                }
            }
            const id = node orelse return error.MissingProgramInput;
            if (id == @import("ml").graph.null_node) continue;
            const value = try provider.import(provider.context, &frame.backend, &input.tensor, allocator);
            runtime_inputs.append(allocator, .{ .node_id = id, .value = value }) catch |err| {
                frame.backend.free(value);
                return err;
            };
        }
        var result = try frame.executable.run(allocator, .{ .runtime_inputs = runtime_inputs.items, .execution_control = control, .output_placement = .producer, .strict_integer_constants = true });
        defer result.deinit(&frame.executable.runtime);
        if (control) |active| try active.check();
        const outputs = try allocator.alloc(Tensor, result.outputs.len);
        var count: usize = 0;
        errdefer {
            for (outputs[0..count]) |*output| output.deinit();
            allocator.free(outputs);
        }
        if (forward.output_names.len != result.outputs.len) return error.MissingProgramOutputABI;
        for (result.outputs, entry.program.graph.outputs.items, 0..) |value, node, i| {
            const shape = entry.program.graph.node(node).output_shape;
            if (shape.dtype != .f32) return error.UnsupportedProgramOutputType;

            const owner = result.backendFor(&frame.executable.runtime, i);
            const data = try owner.toFloat32(value, allocator);
            defer allocator.free(data);
            outputs[i] = try Tensor.initFloat32(allocator, forward.output_names[i], shape.dims[0..shape.rank()], data);
            count += 1;
        }
        return outputs;
    }
};

const TestProvider = struct {
    weights: @import("../ops/native_compute.zig").WeightStore,
    fn create(ptr: *anyopaque, allocator: std.mem.Allocator) anyerror!ops.ComputeBackend {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return @import("../backends/compute_factory.zig").createNative(allocator, &self.weights, null, null, .{});
    }
    fn import(_: *anyopaque, cb: *const ops.ComputeBackend, input: *const Tensor, _: std.mem.Allocator) anyerror!ops.CT {
        const native: *@import("../ops/native_compute.zig").NativeCompute = @ptrCast(@alignCast(cb.ptr));
        return native.importHostTensor(input);
    }
    fn forward(_: *anyopaque, cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, inputs: []const capture.Input) anyerror![]capture.Output {
        const x = try cb.fromFloat32Shape(inputs[0].tensor.asFloat32(), &.{ 2, 3 });
        defer cb.free(x);
        const weight = try cb.getWeight("scale");
        defer cb.free(weight);
        const product = try cb.multiply(x, weight);
        defer cb.free(product);
        const ids = (try cb.fromConstantBytes(inputs[1].tensor.data, .i64, &.{ 2, 3 })).?;
        defer cb.free(ids);
        const threshold: [6]i64 = @splat(16777218);
        const limit = (try cb.fromConstantBytes(std.mem.sliceAsBytes(&threshold), .i64, &.{ 2, 3 })).?;
        defer cb.free(limit);
        const less = try cb.primLessThan(ids, limit);
        defer cb.free(less);
        const negative = try cb.primNegate(product);
        defer cb.free(negative);
        const selected = try cb.primWhereSelect(less, product, negative);
        errdefer cb.free(selected);
        const outputs = try allocator.alloc(capture.Output, 1);
        outputs[0] = .{ .name = "product", .value = selected };
        return outputs;
    }
    fn qualify(client: ?*anyopaque) !void {
        const a = std.testing.allocator;
        var source = TestProvider{ .weights = .{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} } };
        defer source.weights.deinitOwned();
        var weight = try Tensor.initFloat32(a, "scale", &.{ 2, 3 }, &.{ 2, 2, 2, 2, 2, 2 });
        var owns_weight = true;
        defer if (owns_weight) weight.deinit();
        const key = try a.dupe(u8, "scale");
        var owns_key = true;
        defer if (owns_key) a.free(key);
        try source.weights.resident_weights.put(a, key, .{ .tensor = weight, .quantized = false, .quantized_storage = null });
        owns_weight = false;
        owns_key = false;
        const provider = Provider{ .context = &source, .create = create, .import = import, .compiled_context = if (client) |actual| .{ .cb = undefined, .requested_backend = .pjrt, .pjrt_client = actual } else null };
        const frontend = capture.Forward{ .context = &source, .fingerprint = 0, .output_names = &.{"product"}, .run = forward };
        var cache = Cache{};
        defer cache.deinit(a);
        var input = try Tensor.initFloat32(a, "x", &.{ 2, 3 }, &.{ 1, 2, 3, 4, 5, 6 });
        defer input.deinit();
        var ids = try Tensor.initInt64(a, "ids", &.{ 2, 3 }, &.{ 16777217, 16777219, 16777217, 16777219, 16777217, 16777219 });
        defer ids.deinit();
        const declared = [_]capture.Input{ .{ .tensor = input }, .{ .tensor = ids } };
        for (0..2) |iteration| {
            if (iteration == 1) {
                @memset(input.asFloat32Mut(), 7);
                @memset(std.mem.bytesAsSlice(i64, @as([]align(8) u8, @alignCast(ids.data))), 16777219);
            }
            const outputs = try cache.run(a, provider, frontend, &declared, a, null);
            defer a.free(outputs);
            defer for (outputs) |*output| output.deinit();
            const expected: []const f32 = if (iteration == 0) &.{ 2, -4, 6, -8, 10, -12 } else &.{ -14, -14, -14, -14, -14, -14 };
            try std.testing.expectEqualSlices(f32, expected, outputs[0].asFloat32());
            try std.testing.expectEqualSlices(i64, &.{ 2, 3 }, outputs[0].shape);
        }
        try std.testing.expectEqual(@as(u64, 1), cache.entries.stats.builds);
        var foreign = provider;
        foreign.context = @ptrFromInt(1);
        try std.testing.expectError(error.ForeignModelProgramProvider, cache.run(a, foreign, frontend, &declared, a, null));
        if (client != null) {
            var second_program = frontend;
            second_program.fingerprint = 1;
            const outputs = try cache.run(a, provider, second_program, &declared, a, null);
            defer a.free(outputs);
            defer for (outputs) |*output| output.deinit();
            try std.testing.expectEqual(@as(u64, 2), cache.entries.stats.builds);
            try std.testing.expectEqual(@as(usize, 1), cache.parameters.?.uploads);
        }
        const entry = cache.entries.entries.items[0];
        const first = try entry.acquireFrame();
        defer entry.returnFrame(first);
        const second = try entry.acquireFrame();
        defer entry.returnFrame(second);
        try std.testing.expect(first.backend.ptr != second.backend.ptr);
        if (client != null) {
            const original = first.executable.runtime.plan.?;
            const receiving = second.executable.runtime.plan.?;
            var compiled_count: usize = 0;
            for (original.base.partitions, receiving.base.partitions) |left, right| {
                if (left.executor) |executor| {
                    try std.testing.expectEqual(ops.BackendKind.pjrt, left.backend);
                    try std.testing.expectEqual(executor, right.executor.?);
                    compiled_count += 1;
                }
            }
            try std.testing.expect(compiled_count > 0);
        }
    }
};

test "captured forward cache reuses immutable programs with independent CPU frames" {
    try TestProvider.qualify(null);
}

test "captured forward cache executes through real PJRT and shares compiled operands across frames" {
    if (comptime !@import("build_options").enable_pjrt) return error.SkipZigTest;
    var client = @import("pjrt").pjrt.Client.initFromEnv(std.testing.allocator) catch |err| switch (err) {
        error.PjrtPluginNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer client.deinit();
    try TestProvider.qualify(@ptrCast(&client));
}

const TestRotary = struct {
    consecutive: bool,
    rotary_dim: usize,
    fn forward(ptr: *anyopaque, cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, inputs: []const capture.Input) anyerror![]capture.Output {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const x = try cb.fromFloat32Shape(inputs[0].tensor.asFloat32(), &.{ 2, 8 });
        defer cb.free(x);
        const output = try cb.rope(x, 2, 4, self.rotary_dim, 10000, 1, 7, self.consecutive);
        errdefer cb.free(output);
        const result = try allocator.alloc(capture.Output, 1);
        result[0] = .{ .name = "rotary", .value = output };
        return result;
    }
};

test "captured forward PJRT rotary preserves both pair layouts partial rotation and offsets" {
    if (comptime !@import("build_options").enable_pjrt) return error.SkipZigTest;
    const a = std.testing.allocator;
    var client = @import("pjrt").pjrt.Client.initFromEnv(a) catch |err| switch (err) {
        error.PjrtPluginNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer client.deinit();
    var source = TestProvider{ .weights = .{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} } };
    defer source.weights.deinitOwned();
    const provider = Provider{ .context = &source, .create = TestProvider.create, .import = TestProvider.import, .compiled_context = .{ .cb = undefined, .requested_backend = .pjrt, .pjrt_client = @ptrCast(&client) } };
    var input = try Tensor.initFloat32(a, "x", &.{ 2, 8 }, &.{ -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7 });
    defer input.deinit();
    const declared = [_]capture.Input{.{ .tensor = input }};
    var reference = try TestProvider.create(&source, a);
    defer reference.deinit();
    for ([_]bool{ false, true }) |consecutive| for ([_]usize{ 2, 4 }) |rotary_dim| {
        var mode = TestRotary{ .consecutive = consecutive, .rotary_dim = rotary_dim };
        const forward = capture.Forward{ .context = &mode, .fingerprint = rotary_dim * 2 + @intFromBool(consecutive), .output_names = &.{"rotary"}, .run = TestRotary.forward };
        const expected = try forward.run(forward.context, &reference, a, &declared);
        defer a.free(expected);
        defer for (expected) |output| reference.free(output.value);
        const values = try reference.toFloat32(expected[0].value, a);
        defer a.free(values);
        var cache = Cache{};
        defer cache.deinit(a);
        const actual = try cache.run(a, provider, forward, &declared, a, null);
        defer a.free(actual);
        defer for (actual) |*output| output.deinit();
        try std.testing.expectEqual(values.len, actual[0].asFloat32().len);
        for (values, actual[0].asFloat32()) |want, got| try std.testing.expectApproxEqAbs(want, got, 1e-6);
    };
}

const TestPooling = struct {
    fn forward(_: *anyopaque, cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, inputs: []const capture.Input) anyerror![]capture.Output {
        const x = try cb.fromFloat32Shape(inputs[0].tensor.asFloat32(), &.{ 2, 2, 4 });
        defer cb.free(x);
        const result = (try cb.vtable.maskedMeanBatch.?(cb.ptr, x, inputs[1].tensor.asInt64(), 2, 2, 4, 2, true)).?;
        errdefer cb.free(result);
        const outputs = try allocator.alloc(capture.Output, 1);
        outputs[0] = .{ .name = "embedding", .value = result };
        return outputs;
    }
    fn qualify(client: ?*anyopaque) !void {
        const a = std.testing.allocator;
        var source = TestProvider{ .weights = .{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} } };
        defer source.weights.deinitOwned();
        const nan = std.math.nan(f32);
        var input = try Tensor.initFloat32(a, "x", &.{ 2, 2, 4 }, &.{ 1, 2, 3, 4, nan, nan, nan, nan, 1, 2, 3, 4, 3, 4, 5, 6 });
        defer input.deinit();
        var mask = try Tensor.initInt64(a, "attention_mask", &.{ 2, 2 }, &.{ 1, 0, 1, 1 });
        defer mask.deinit();
        const provider = Provider{ .context = &source, .create = TestProvider.create, .import = TestProvider.import, .compiled_context = if (client) |actual| .{ .cb = undefined, .requested_backend = .pjrt, .pjrt_client = actual } else null };
        var cache = Cache{};
        defer cache.deinit(a);
        const outputs = try cache.run(a, provider, .{ .context = &source, .fingerprint = 0, .output_names = &.{"embedding"}, .run = forward }, &.{ .{ .tensor = input }, .{ .tensor = mask, .specialize = true } }, a, null);
        defer a.free(outputs);
        defer for (outputs) |*output| output.deinit();
        const want = [_]f32{ 1 / @sqrt(@as(f32, 5)), 2 / @sqrt(@as(f32, 5)), 2 / @sqrt(@as(f32, 13)), 3 / @sqrt(@as(f32, 13)) };
        try std.testing.expectEqualSlices(i64, &.{ 2, 2 }, outputs[0].shape);
        for (want, outputs[0].asFloat32()) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 1e-6);
    }
};

test "captured forward CPU pooling excludes nonfinite padding and normalizes truncated rows" {
    try TestPooling.qualify(null);
}

test "captured forward PJRT pooling excludes nonfinite padding and normalizes truncated rows" {
    if (comptime !@import("build_options").enable_pjrt) return error.SkipZigTest;
    var client = @import("pjrt").pjrt.Client.initFromEnv(std.testing.allocator) catch |err| switch (err) {
        error.PjrtPluginNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer client.deinit();
    try TestPooling.qualify(@ptrCast(&client));
}
