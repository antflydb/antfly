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

const PjrtExecutor = if (@import("build_options").enable_pjrt) @import("../graph/pjrt_executor.zig").PjrtExecutor else opaque {};
const ParameterStore = if (@import("build_options").enable_pjrt) @import("../graph/pjrt_parameters.zig").Store else opaque {};

const Frame = struct {
    backend: ops.ComputeBackend,
    executable: ?executable.Executable = null,
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
    resident: ?*PjrtExecutor = null,
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
        if (comptime @import("build_options").enable_pjrt) if (self.resident) |resident| resident.partitionExecutor().deinitExecutor();
        // Borrowed compiled partitions retire before the owning first frame.
        var index = self.frames.items.len;
        while (index != 0) {
            index -= 1;
            const frame = self.frames.items[index];
            std.debug.assert(!frame.busy);
            if (frame.executable) |*value| value.deinit();
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
    fn weightValues(context: ?*anyopaque, allocator: std.mem.Allocator, name: []const u8) anyerror!?[]f32 {
        const backend: *const ops.ComputeBackend = @ptrCast(@alignCast(context.?));
        const weight = try backend.getWeight(name);
        defer backend.free(weight);
        const shape = try backend.tensorShape(weight, allocator);
        defer allocator.free(shape);
        var count: usize = 1;
        for (shape) |dim| {
            if (dim < 0 or dim > 4096) return error.HostReadbackDuringCapture;
            count = try std.math.mul(usize, count, @intCast(dim));
        }
        if (count > 4096) return error.HostReadbackDuringCapture;
        return try backend.toFloat32(weight, allocator);
    }
    fn create(allocator: std.mem.Allocator, provider: Provider, forward: capture.Forward, inputs: []const capture.Input, key: Key) !*Entry {
        const frame = try allocator.create(Frame);
        errdefer allocator.destroy(frame);
        frame.backend = try provider.create(provider.context, allocator);
        errdefer frame.backend.deinit();
        var program = try capture.trace(allocator, forward, inputs, .{ .context = @constCast(&frame.backend), .resolve = weightShape, .values = weightValues });
        errdefer program.deinit();
        const entry = try allocator.create(Entry);
        errdefer allocator.destroy(entry);
        entry.* = .{ .allocator = allocator, .provider = provider, .key = key, .program = program };
        errdefer entry.frames.deinit(allocator);
        try entry.frames.ensureUnusedCapacity(allocator, 1);
        var options = executable.PrepareOptions{ .compiled = provider.compiled_context };
        if (options.compiled) |*context| context.cb = &frame.backend;
        frame.executable = null;
        if (forward.resident_capacity != 0) {
            if (comptime @import("build_options").enable_pjrt) {
                const context = options.compiled orelse return error.MissingPjrtClient;
                const client: *@import("pjrt").pjrt.Client = @ptrCast(@alignCast(context.pjrt_client orelse return error.MissingPjrtClient));
                const count = entry.program.graph.nodeCount();
                const nodes = try allocator.alloc(@import("ml").graph.NodeId, count);
                defer allocator.free(nodes);
                for (nodes, 0..) |*node, i| node.* = @intCast(i);
                const part = @import("../graph/partition.zig").Partition{ .backend = .pjrt, .node_ids = nodes, .external_inputs = &.{} };
                const visibility: ?[]const u8 = for (inputs) |input| {
                    if (std.mem.eql(u8, input.tensor.name, "attention_or_mask")) break input.tensor.data;
                } else null;
                entry.resident = try @import("../graph/pjrt_executor.zig").createExecutorWithOptions(allocator, &entry.program.graph, &part, &frame.backend, &frame.backend, client, .{ .resident_capacity = forward.resident_capacity, .resident_visibility = visibility, .precision = forward.precision, .mutable_parameters = entry.program.mutable_parameters }, if (context.pjrt_parameter_store) |store| @ptrCast(@alignCast(store)) else null);
            } else return error.PjrtNotEnabled;
        } else {
            frame.executable = try executable.prepareWithOptions(allocator, &entry.program, &frame.backend, if (provider.compiled_context != null) .compiled_required else .compiled_preferred, options);
        }
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
        const source = if (self.frames.items[0].executable) |*value| &value.runtime else null;
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
        frame.executable = if (self.resident != null) null else try executable.prepareWithOptions(self.allocator, &self.program, &frame.backend, if (self.provider.compiled_context != null) .compiled_required else .compiled_preferred, options);
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
    // Use the common bounded shape working set. A four-entry cache evicted
    // ordinary encoder/decoder prefixes before a repeated task could reuse them.
    entries: Entries = Entries.init(@import("../graph/cache.zig").max_entries),
    parameters: ?*ParameterStore = null,
    identity: ?Provider = null,

    pub fn specializationCount(self: *Cache) usize {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        return self.entries.entries.items.len;
    }

    fn sameProvider(a: Provider, b: Provider) bool {
        if (a.context != b.context or a.create != b.create or a.import != b.import or (a.compiled_context != null) != (b.compiled_context != null)) return false;
        if (a.compiled_context) |left| {
            const right = b.compiled_context.?;
            return left.requested_backend == right.requested_backend and left.pjrt_client == right.pjrt_client and left.pjrt_parameter_store == right.pjrt_parameter_store and left.attachment_target == right.attachment_target;
        }
        return true;
    }

    /// Caller holds the loaded model through every run; close follows quiesce.
    pub fn deinit(self: *Cache, allocator: std.mem.Allocator) void {
        for (self.entries.entries.items) |entry| entry.release();
        self.entries.deinit(allocator);
        if (comptime @import("build_options").enable_pjrt) if (self.parameters) |parameters| {
            parameters.deinit();
            allocator.destroy(parameters);
        };
    }
    fn acquire(self: *Cache, allocator: std.mem.Allocator, provider: Provider, forward: capture.Forward, inputs: []const capture.Input) !*Entry {
        // Precision belongs to the program specialization, not model/provider
        // identity: F32 task heads can follow a BF16 encoder on one client.
        if (provider.compiled_context) |context| if (context.requested_backend == .pjrt and context.pjrt_precision != forward.precision) return error.ModelProgramPrecisionMismatch;
        const key = try capture.specializationKey(forward, inputs);
        var resolved_provider = provider;
        platform.sync.lockYielding(&self.mutex);
        if (self.identity) |identity| {
            if (!sameProvider(identity, provider)) {
                self.mutex.unlock();
                return error.ForeignModelProgramProvider;
            }
        } else self.identity = provider;
        if (resolved_provider.compiled_context) |*context| if (context.requested_backend == .pjrt and context.pjrt_parameter_store == null) {
            if (comptime @import("build_options").enable_pjrt) {
                const client: *@import("pjrt").pjrt.Client = @ptrCast(@alignCast(context.pjrt_client orelse {
                    self.mutex.unlock();
                    return error.MissingPjrtClient;
                }));
                if (self.parameters == null) {
                    const parameters = allocator.create(ParameterStore) catch |err| {
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
            } else {
                self.mutex.unlock();
                return error.PjrtNotEnabled;
            }
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
    pub fn runResident(self: *Cache, model_allocator: std.mem.Allocator, provider: Provider, forward: capture.Forward, inputs: []const capture.Input, state: *@import("../graph/pjrt_decoder_state.zig").State, allocator: std.mem.Allocator, control: ?Control) ![]Tensor {
        if (comptime !@import("build_options").enable_pjrt) return error.PjrtNotEnabled;
        if (forward.resident_capacity == 0) return error.NotResidentDecoder;
        if (control) |active| try active.check();
        for (inputs) |input| _ = try capture.inputShape(input.tensor);
        if (forward.validate) |validate| try validate(forward.context, inputs);
        const entry = try self.acquire(model_allocator, provider, forward, inputs);
        defer entry.release();
        const frame = try entry.acquireFrame();
        defer entry.returnFrame(frame);
        frame.backend.execution_control = control;
        defer frame.backend.execution_control = null;
        const values = try allocator.alloc(?ops.CT, entry.program.graph.nodeCount());
        defer allocator.free(values);
        @memset(values, null);
        defer for (values) |value| if (value) |tensor| frame.backend.free(tensor);
        for (inputs) |input| {
            for (entry.program.bindings) |binding| {
                if (binding.kind != .input or !std.mem.eql(u8, binding.name, input.tensor.name) or binding.node_id == @import("ml").graph.null_node) continue;
                values[binding.node_id] = try provider.import(provider.context, &frame.backend, &input.tensor, allocator);
            }
        }
        var rows: usize = 0;
        for (inputs) |input| if (std.mem.eql(u8, input.tensor.name, "input_ids")) {
            rows = @intCast(input.tensor.shape[1]);
            break;
        };
        var valid_rows = rows;
        for (inputs) |input| if (std.mem.eql(u8, input.tensor.name, "logits_row")) {
            valid_rows = @intCast(input.tensor.asInt64()[0] + 1);
            break;
        };
        // Allocate the entire public result before committing request state.
        const outputs = try allocator.alloc(Tensor, 1);
        errdefer allocator.free(outputs);
        const shape = try allocator.dupe(i64, entry.resident.?.output_shapes[0]);
        errdefer allocator.free(shape);
        const data = try state.run(entry.resident.?, &frame.backend, values, rows, valid_rows, allocator, control);
        outputs[0] = .{ .data = std.mem.sliceAsBytes(data), .dtype = .f32, .shape = shape, .name = "logits", .allocator = allocator, .owns_data = true, .owns_shape = true, .data_alignment = .of(f32) };
        return outputs;
    }

    pub fn run(self: *Cache, model_allocator: std.mem.Allocator, provider: Provider, forward: capture.Forward, inputs: []const capture.Input, allocator: std.mem.Allocator, control: ?Control) ![]Tensor {
        if (control) |active| try active.check();
        for (inputs) |input| _ = try capture.inputShape(input.tensor);
        if (forward.validate) |validate| try validate(forward.context, inputs);
        const entry = try self.acquire(model_allocator, provider, forward, inputs);
        defer entry.release();
        const frame = try entry.acquireFrame();
        defer entry.returnFrame(frame);
        const frame_executable = if (frame.executable) |*value| value else return error.ResidentDecoderRequiresState;
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
        var result = try frame_executable.run(allocator, .{ .runtime_inputs = runtime_inputs.items, .execution_control = control, .output_placement = .producer, .strict_integer_constants = true });
        defer result.deinit(&frame_executable.runtime);
        if (control) |active| try active.check();
        const outputs = try allocator.alloc(Tensor, result.outputs.len);
        var count: usize = 0;
        errdefer {
            for (outputs[0..count]) |*output| output.deinit();
            allocator.free(outputs);
        }
        const output_names = if (forward.output_abi) |resolve| try resolve(forward.context, inputs) else forward.output_names;
        if (output_names.len != result.outputs.len) return error.MissingProgramOutputABI;
        for (result.outputs, entry.program.graph.outputs.items, 0..) |value, node, i| {
            const shape = entry.program.graph.node(node).output_shape;
            if (shape.dtype != .f32) return error.UnsupportedProgramOutputType;

            const owner = result.backendFor(&frame_executable.runtime, i);
            const data = try owner.toFloat32(value, allocator);
            outputs[i] = Tensor.initFloat32Owned(allocator, output_names[i], shape.dims[0..shape.rank()], data) catch |err| {
                allocator.free(data);
                return err;
            };
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
            var bf16_forward = frontend;
            bf16_forward.precision = .bf16;
            var bf16_provider = provider;
            bf16_provider.compiled_context.?.pjrt_precision = .bf16;
            const mixed = try cache.run(a, bf16_provider, bf16_forward, &declared, a, null);
            defer a.free(mixed);
            defer for (mixed) |*output| output.deinit();
            try std.testing.expectEqualSlices(f32, &.{ -14, -14, -14, -14, -14, -14 }, mixed[0].asFloat32());
            try std.testing.expectEqual(@as(u64, 3), cache.entries.stats.builds);
            try std.testing.expectEqual(@as(usize, 2), cache.parameters.?.uploads);
            try std.testing.expectError(error.ModelProgramPrecisionMismatch, cache.run(a, bf16_provider, frontend, &declared, a, null));
        }
        const entry = cache.entries.entries.items[0];
        const first = try entry.acquireFrame();
        defer entry.returnFrame(first);
        const second = try entry.acquireFrame();
        defer entry.returnFrame(second);
        try std.testing.expect(first.backend.ptr != second.backend.ptr);
        if (client != null) {
            const original = first.executable.?.runtime.plan.?;
            const receiving = second.executable.?.runtime.plan.?;
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
        return qualifyPrecision(client, .f32);
    }
    fn qualifyPrecision(client: ?*anyopaque, precision: @import("precision.zig").Policy) !void {
        const a = std.testing.allocator;
        var source = TestProvider{ .weights = .{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} } };
        defer source.weights.deinitOwned();
        const nan = std.math.nan(f32);
        var input = try Tensor.initFloat32(a, "x", &.{ 2, 2, 4 }, &.{ 1, 2, 3, 4, nan, nan, nan, nan, 1, 2, 3, 4, 3, 4, 5, 6 });
        defer input.deinit();
        var mask = try Tensor.initInt64(a, "attention_mask", &.{ 2, 2 }, &.{ 1, 0, 1, 1 });
        defer mask.deinit();
        const provider = Provider{ .context = &source, .create = TestProvider.create, .import = TestProvider.import, .compiled_context = if (client) |actual| .{ .cb = undefined, .requested_backend = .pjrt, .pjrt_client = actual, .pjrt_precision = precision } else null };
        var cache = Cache{};
        defer cache.deinit(a);
        const outputs = try cache.run(a, provider, .{ .context = &source, .fingerprint = 0, .precision = precision, .output_names = &.{"embedding"}, .run = forward }, &.{ .{ .tensor = input }, .{ .tensor = mask } }, a, null);
        defer a.free(outputs);
        defer for (outputs) |*output| output.deinit();
        const want = [_]f32{ 1 / @sqrt(@as(f32, 5)), 2 / @sqrt(@as(f32, 5)), 2 / @sqrt(@as(f32, 13)), 3 / @sqrt(@as(f32, 13)) };
        try std.testing.expectEqualSlices(i64, &.{ 2, 2 }, outputs[0].shape);
        for (want, outputs[0].asFloat32()) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 1e-6);
        const bits: []i64 = @alignCast(std.mem.bytesAsSlice(i64, mask.data));
        bits[2] = 0;
        const changed = try cache.run(a, provider, .{ .context = &source, .fingerprint = 0, .precision = precision, .output_names = &.{"embedding"}, .run = forward }, &.{ .{ .tensor = input }, .{ .tensor = mask } }, a, null);
        defer a.free(changed);
        defer for (changed) |*output| output.deinit();
        const changed_want = [_]f32{ want[0], want[1], 0.6, 0.8 };
        for (changed_want, changed[0].asFloat32()) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 1e-6);
        try std.testing.expectEqual(@as(u64, 1), cache.entries.stats.builds);
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

test "captured forward BF16 storage preserves F32 pooling and output normalization" {
    if (comptime !@import("build_options").enable_pjrt) return error.SkipZigTest;
    var client = try @import("pjrt").pjrt.Client.initFromEnvWithMemoryPolicy(std.testing.allocator, .cpu);
    defer client.deinit();
    try TestPooling.qualifyPrecision(@ptrCast(&client), .bf16);
}

test "captured forward BF16 rotary preserves coefficient product and partial-layout rounding" {
    if (comptime !@import("build_options").enable_pjrt) return error.SkipZigTest;
    const a = std.testing.allocator;
    const round = @import("antfly_tile").fexpr.roundBf16Value;
    var client = try @import("pjrt").pjrt.Client.initFromEnvWithMemoryPolicy(a, .cpu);
    defer client.deinit();
    var source = TestProvider{ .weights = .{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} } };
    defer source.weights.deinitOwned();
    var input = try Tensor.initFloat32(a, "x", &.{ 2, 8 }, &.{ 0.1, -0.2, 3.14, -4.23, 0.875, 0.125, -2.3, 7.1, -0.2, 0.3, -0.4, 0.5, 8.01, -6.12, 4.23, -2.34 });
    defer input.deinit();
    for ([_]bool{ false, true }) |consecutive| for ([_]usize{ 2, 4 }) |rotary_dim| {
        var mode = TestRotary{ .consecutive = consecutive, .rotary_dim = rotary_dim };
        var expected: [16]f32 = undefined;
        for (input.asFloat32(), &expected) |value, *out| out.* = round(value);
        for (0..2) |row| for (0..2) |head| for (0..rotary_dim / 2) |pair| {
            const angle = @as(f32, @floatFromInt(row + 7)) / std.math.pow(f32, 10000, @as(f32, @floatFromInt(2 * pair)) / @as(f32, @floatFromInt(rotary_dim)));
            const cosine = round(@cos(angle));
            const sine = round(@sin(angle));
            const lo = row * 8 + head * 4 + (if (consecutive) 2 * pair else pair);
            const hi = if (consecutive) lo + 1 else lo + 2;
            const x0 = round(input.asFloat32()[lo]);
            const x1 = round(input.asFloat32()[hi]);
            expected[lo] = round(round(x0 * cosine) - round(x1 * sine));
            expected[hi] = round(round(x1 * cosine) + round(x0 * sine));
        };
        var cache = Cache{};
        defer cache.deinit(a);
        const outputs = try cache.run(a, .{ .context = &source, .create = TestProvider.create, .import = TestProvider.import, .compiled_context = .{ .cb = undefined, .requested_backend = .pjrt, .pjrt_client = @ptrCast(&client), .pjrt_precision = .bf16 } }, .{ .context = &mode, .fingerprint = 0, .precision = .bf16, .output_names = &.{"rotary"}, .run = TestRotary.forward }, &.{.{ .tensor = input }}, a, null);
        defer a.free(outputs);
        defer for (outputs) |*output| output.deinit();
        try std.testing.expectEqualSlices(f32, &expected, outputs[0].asFloat32());
    };
}

const TestTemporal = struct {
    fn client(a: std.mem.Allocator) !@import("pjrt").pjrt.Client {
        const Client = @import("pjrt").pjrt.Client;
        // Standalone CUDA qualification needs an explicit, small pool; the
        // native CUDA backend is deliberately unnecessary for these tests.
        const policy: Client.MemoryPolicy = if (platform.env.getenv("ANTFLY_PJRT_TEST_CUDA_DEVICE_BYTES")) |bytes|
            .{ .cuda = .{ .capacity_bytes = 256 * 1024 * 1024, .device_bytes = try std.fmt.parseInt(usize, bytes, 10) } }
        else
            .cpu;
        return Client.initFromEnvWithMemoryPolicy(a, policy);
    }
    const params = ops.Gemma4AudioLocalAttentionParams{ .rows = 5, .hidden = 4, .heads = 2, .head_dim = 2, .chunk = 2, .context_left = 3, .context = 4, .k_scale = 0.7, .logit_cap = 50, .invalid_value = -1e9, .exclude_farthest_key = true };
    const rel = [_]f32{ 0.1, -0.2, 0.3, 0.4, -0.3, 0.2, -0.1, 0.1, 0.2, -0.4, 0.3, 0.1 };
    const scales = [_]f32{ 0.7, 0.9 };
    const weights = [_]f32{ 0.1, 0.2, 0.3, 0.4, 0.4, 0.3, 0.2, 0.1, -0.2, 0.5, -0.1, 0.2 };
    const valid = [_]bool{ true, true, false, true, true };
    fn forward(_: *anyopaque, cb: *const ops.ComputeBackend, a: std.mem.Allocator, inputs: []const capture.Input) anyerror![]capture.Output {
        return forwardWithParams(cb, a, inputs, params);
    }
    fn independent(_: *anyopaque, cb: *const ops.ComputeBackend, a: std.mem.Allocator, inputs: []const capture.Input) anyerror![]capture.Output {
        var p = params;
        p.arithmetic = .independent_f32;
        p.query_scale = 1.3;
        p.publication = .bf16_values_f32;
        return forwardWithParams(cb, a, inputs, p);
    }
    fn forwardWithParams(cb: *const ops.ComputeBackend, a: std.mem.Allocator, inputs: []const capture.Input, p: ops.Gemma4AudioLocalAttentionParams) anyerror![]capture.Output {
        const x = try cb.fromFloat32Shape(inputs[0].tensor.asFloat32(), &.{ 5, 4 });
        defer cb.free(x);
        const table = try cb.fromFloat32Shape(&rel, &.{ 1, 3, 4 });
        defer cb.free(table);
        const relative = try cb.primReshape(table, &.{ 3, 4 });
        defer cb.free(relative);
        const gathered = try cb.embeddingLookup(table, &.{ 2, 0 }, 2, 4);
        errdefer cb.free(gathered);
        const scale = try cb.fromFloat32Shape(&scales, &.{2});
        defer cb.free(scale);
        const mask = try cb.fromFloat32Shape(&.{ 1, 1, 0, 1, 1 }, &.{5});
        defer cb.free(mask);
        const kernel = try cb.fromFloat32Shape(&weights, &.{ 3, 4 });
        defer cb.free(kernel);
        const attention = (try cb.gemma4AudioLocalAttention(x, x, x, relative, scale, mask, p)) orelse return error.UnsupportedPrimitiveOp;
        errdefer cb.free(attention);
        const convolution = (try cb.depthwiseCausalConv1d(x, kernel, 5, 4, 3)) orelse return error.UnsupportedPrimitiveOp;
        errdefer cb.free(convolution);
        const outputs = try a.alloc(capture.Output, 3);
        outputs[0] = .{ .name = "attention", .value = attention };
        outputs[1] = .{ .name = "convolution", .value = convolution };
        outputs[2] = .{ .name = "positions", .value = gathered };
        return outputs;
    }
};

test "captured temporal PJRT local relative attention and causal convolution preserve masks and runtime values" {
    if (comptime !@import("build_options").enable_pjrt) return error.SkipZigTest;
    const a = std.testing.allocator;
    var client = try TestTemporal.client(a);
    defer client.deinit();
    var source = TestProvider{ .weights = .{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} } };
    defer source.weights.deinitOwned();
    var input = try Tensor.initFloat32(a, "x", &.{ 5, 4 }, &.{ 0.1, -0.2, 0.3, -0.4, 0.5, 0.7, -0.1, 0.2, -0.3, 0.4, 0.9, 0.8, 0.2, -0.6, 0.1, 0.7, -0.5, 0.4, 0.3, -0.2 });
    defer input.deinit();
    var cache = Cache{};
    defer cache.deinit(a);
    for (0..2) |iteration| {
        input.asFloat32Mut()[0] += @as(f32, @floatFromInt(iteration)) * 0.3;
        const expected = try @import("../architectures/gemma4_projector.zig").audioLocalAttentionReference(a, TestTemporal.params, input.asFloat32(), input.asFloat32(), input.asFloat32(), &TestTemporal.rel, &TestTemporal.scales, &TestTemporal.valid);
        defer a.free(expected);
        const outputs = try cache.run(a, .{ .context = &source, .create = TestProvider.create, .import = TestProvider.import, .compiled_context = .{ .cb = undefined, .requested_backend = .pjrt, .pjrt_client = @ptrCast(&client) } }, .{ .context = &source, .fingerprint = 1, .output_names = &.{ "attention", "convolution", "positions" }, .run = TestTemporal.forward }, &.{.{ .tensor = input }}, a, null);
        defer a.free(outputs);
        defer for (outputs) |*output| output.deinit();
        try std.testing.expectEqualSlices(f32, TestTemporal.rel[8..12], outputs[2].asFloat32()[0..4]);
        try std.testing.expectEqualSlices(f32, TestTemporal.rel[0..4], outputs[2].asFloat32()[4..8]);
        for (expected, outputs[0].asFloat32()) |want, got| try std.testing.expectApproxEqAbs(want, got, 2e-6);
        for (0..5) |row| for (0..4) |channel| {
            var want: f32 = 0;
            for (0..3) |tap| if (row + tap >= 2) {
                want += input.asFloat32()[(row + tap - 2) * 4 + channel] * TestTemporal.weights[tap * 4 + channel];
            };
            try std.testing.expectApproxEqAbs(want, outputs[1].asFloat32()[row * 4 + channel], 1e-6);
        };
    }
    try std.testing.expectEqual(@as(usize, 1), cache.entries.entries.items.len);
}

test "captured temporal PJRT independent reductions publish BF16 values after F32 probabilities" {
    if (comptime !@import("build_options").enable_pjrt) return error.SkipZigTest;
    const a = std.testing.allocator;
    var client = try TestTemporal.client(a);
    defer client.deinit();
    var source = TestProvider{ .weights = .{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} } };
    defer source.weights.deinitOwned();
    var input = try Tensor.initFloat32(a, "x", &.{ 5, 4 }, &.{ 0.1, -0.2, 0.3, -0.4, 0.5, 0.7, -0.1, 0.2, -0.3, 0.4, 0.9, 0.8, 0.2, -0.6, 0.1, 0.7, -0.5, 0.4, 0.3, -0.2 });
    defer input.deinit();
    var cache = Cache{};
    defer cache.deinit(a);
    const outputs = try cache.run(a, .{ .context = &source, .create = TestProvider.create, .import = TestProvider.import, .compiled_context = .{ .cb = undefined, .requested_backend = .pjrt, .pjrt_client = @ptrCast(&client) } }, .{ .context = &source, .fingerprint = 2, .output_names = &.{ "attention", "convolution", "positions" }, .run = TestTemporal.independent }, &.{.{ .tensor = input }}, a, null);
    defer a.free(outputs);
    defer for (outputs) |*output| output.deinit();
    // Frozen scalar reference: distinct content/relative F32 dot reductions,
    // F32 softmax/value contraction, then one BF16 publication per coordinate.
    const expected = [_]f32{ 0.10009765625, -0.2001953125, 0.30078125, -0.400390625, 0.337890625, 0.333984375, 0.09130859375, -0.0869140625, 0, 0, 0, 0, 0.2001953125, -0.6015625, 0.10009765625, 0.69921875, -0.1572265625, -0.08935546875, 0.21484375, 0.1845703125 };
    try std.testing.expectEqualSlices(f32, &expected, outputs[0].asFloat32());
}

// One captured graph must use the current donor values, retain them after the
// producer retires, and preserve its local causal window on every replay.
test "forward KV reuse captured PJRT preserves donor values and local windows" {
    if (comptime !@import("build_options").enable_pjrt) return error.SkipZigTest;
    const a = std.testing.allocator;
    var client = try @import("pjrt").pjrt.Client.initFromEnvWithMemoryPolicy(a, .cpu);
    defer client.deinit();
    var source = TestProvider{ .weights = .{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} } };
    defer source.weights.deinitOwned();
    const Forward = struct {
        fn run(_: *anyopaque, cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, inputs: []const capture.Input) anyerror![]capture.Output {
            var scope = try @import("forward_kv_reuse.zig").Scope.init(allocator, cb, 1, null);
            defer scope.deinit();
            const q = (try cb.zeroTensor(3, 2)) orelse return error.ZeroTensorUnsupported;
            defer cb.free(q);
            var attention = ops.AttentionContext{ .total_sequence_len = 3, .query_sequence_len = 3, .kv_sequence_len = 3, .sliding_window = 2, .forward_kv_reuse = scope.context() };
            {
                const values = try cb.fromFloat32Shape(inputs[0].tensor.asFloat32(), &.{ 3, 2 });
                defer cb.free(values);
                const donor = try cb.gqaPagedAttention(q, q, values, null, attention, 1, 1, 1, 2);
                cb.free(donor);
            }
            attention.skip_kv_write = true;
            const output = try cb.gqaPagedAttention(q, q, q, null, attention, 1, 1, 1, 2);
            errdefer cb.free(output);
            const outputs = try allocator.alloc(capture.Output, 1);
            outputs[0] = .{ .name = "output", .value = output };
            return outputs;
        }
    };
    const provider = Provider{ .context = &source, .create = TestProvider.create, .import = TestProvider.import, .compiled_context = .{ .cb = undefined, .requested_backend = .pjrt, .pjrt_client = @ptrCast(&client) } };
    var cache = Cache{};
    defer cache.deinit(a);
    for ([_]f32{ 1, 3 }) |factor| {
        var values = try Tensor.initFloat32(a, "v", &.{ 3, 2 }, &.{ 2 * factor, 4 * factor, 6 * factor, 8 * factor, 10 * factor, 12 * factor });
        defer values.deinit();
        const outputs = try cache.run(a, provider, .{ .context = &source, .fingerprint = 0, .output_names = &.{"output"}, .run = Forward.run }, &.{.{ .tensor = values }}, a, null);
        defer a.free(outputs);
        defer for (outputs) |*output| output.deinit();
        for ([_]f32{ 2, 4, 4, 6, 8, 10 }, outputs[0].asFloat32()) |want, got| try std.testing.expectApproxEqAbs(want * factor, got, 2e-6);
    }
    try std.testing.expectEqual(@as(u64, 1), cache.entries.stats.builds);
}

test "captured prefix working set reuses executables across repeated tasks" {
    const a = std.testing.allocator;
    var source = TestProvider{ .weights = .{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} } };
    defer source.weights.deinitOwned();
    const Forward = struct {
        fn run(_: *anyopaque, cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, inputs: []const capture.Input) anyerror![]capture.Output {
            const input = inputs[0].tensor;
            const value = try cb.fromFloat32Shape(input.asFloat32(), &.{ @intCast(input.shape[0]), 1 });
            defer cb.free(value);
            const output = try cb.primNegate(value);
            errdefer cb.free(output);
            const outputs = try allocator.alloc(capture.Output, 1);
            outputs[0] = .{ .name = "output", .value = output };
            return outputs;
        }
    };
    var cache = Cache{};
    defer cache.deinit(a);
    const provider = Provider{ .context = &source, .create = TestProvider.create, .import = TestProvider.import };
    for (0..2) |_| for (1..14) |rows| {
        const values: [13]f32 = @splat(2);
        var input = try Tensor.initFloat32(a, "x", &.{ @intCast(rows), 1 }, values[0..rows]);
        defer input.deinit();
        const outputs = try cache.run(a, provider, .{ .context = &source, .fingerprint = 0, .output_names = &.{"output"}, .run = Forward.run }, &.{.{ .tensor = input }}, a, null);
        defer a.free(outputs);
        defer for (outputs) |*output| output.deinit();
        for (outputs[0].asFloat32()) |value| try std.testing.expectEqual(@as(f32, -2), value);
    };
    try std.testing.expectEqual(@as(u64, 13), cache.entries.stats.builds);
    try std.testing.expectEqual(@as(u64, 13), cache.entries.stats.hits);
    try std.testing.expectEqual(@as(u64, 0), cache.entries.stats.evictions);
}
