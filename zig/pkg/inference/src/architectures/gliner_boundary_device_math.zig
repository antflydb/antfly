// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Request-owned strict resident FP32 math shared by the boundary encoder and
//! learned task heads. Transfers use caller-allocated storage, never mirrors.
const std = @import("std");
const compute = @import("../ops/ops.zig");
const device = compute.gliner_boundary_device;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const CT = compute.CT;

/// The reference policy preserves the original synchronous request lifetime.
/// Optimized inference requires a separately admitted immutable model owner.
pub const ExecutionPolicy = enum { reference_v1, optimized_v2 };

pub const Limits = struct {
    max_batch: usize = 8,
    max_text_words: usize = 8192,
    max_queries: usize = 256,
    max_device_bytes: usize = 256 * 1024 * 1024,
    max_proposal_download_bytes: usize = 64 * 1024 * 1024,
    max_result_download_bytes: usize = 64 * 1024 * 1024,
    max_pool_pair_elements: usize = 1024 * 1024,
    query_chunk: usize = 32,
    max_explicit_candidates: usize = 4096,
    max_explicit_elements: usize = 262144,
    explicit_chunk: usize = 256,
};

/// Counters belong to one request, never to a shared backend/global snapshot.
/// Weight bytes are an admission charge: already-resident weights can alias
/// storage and therefore need no physical upload. Peaks include conservative
/// upload staging allowances; they do not measure process/unified-memory RSS.
pub const Stats = struct {
    peak_device_bytes: usize = 0,
    charged_weight_bytes: usize = 0,
    metadata_upload_bytes: usize = 0,
    proposal_download_bytes: usize = 0,
    result_download_bytes: usize = 0,
    proposal_download_calls: usize = 0,
    result_download_calls: usize = 0,
    device_dispatches: usize = 0,
};

fn count(values: []const usize) !usize {
    var n: usize = 1;
    for (values) |v| n = try std.math.mul(usize, n, v);
    if (n > std.math.maxInt(i32)) return error.ResourceLimitExceeded;
    return n;
}

fn bytes(elements: usize) !usize {
    return std.math.mul(usize, elements, 4);
}

fn dim(n: usize) !u32 {
    return std.math.cast(u32, n) orelse error.InvalidInputShape;
}

const Entry = struct { tensor: CT, bytes: usize };

pub const Context = struct {
    allocator: std.mem.Allocator,
    cb: compute.ComputeBackend,
    limits: Limits,
    control: ?Control = null,
    encoder_precision: device.WeightPrecision = .f32,
    stats: Stats = .{},
    entries: std.ArrayList(Entry) = .empty,
    weights: std.StringHashMapUnmanaged(CT) = .{},
    current_bytes: usize = 0,
    resident_weights: bool = false,
    scoped_commands: bool = false,
    scope_generation: ?u64 = null,
    scope_start_dispatches: u64 = 0,
    const max_segment_dispatches: usize = 64;

    pub fn create(allocator: std.mem.Allocator, cb: *const compute.ComputeBackend, limits: Limits, control: ?Control) !*Context {
        if (cb.decoderRuntimeHasActiveFrame()) return error.GlinerBoundaryExternalFrame;
        const self = try allocator.create(Context);
        self.* = .{ .allocator = allocator, .cb = cb.*, .limits = limits, .control = control };
        return self;
    }

    pub fn destroy(self: *Context) void {
        // Pending physical owners must be retired before request wrappers.
        // The backend's managed hard-cancellation guard remains held here.
        if (self.scope_generation) |generation| {
            _ = self.cb.glinerBoundaryScope(&.{ .cancel = .{ .generation = generation } }) catch {};
        }
        for (self.entries.items) |entry| self.cb.free(entry.tensor);
        self.entries.deinit(self.allocator);
        var it = self.weights.keyIterator();
        while (it.next()) |key| self.allocator.free(key.*);
        self.weights.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    pub fn check(self: *const Context) !void {
        // Standalone command completion is part of this owner's lifetime and
        // memory contract. Joining a caller's frame would defer both cleanup
        // and readback, invalidating request-local bounds.
        if (self.cb.decoderRuntimeHasActiveFrame()) {
            if (!self.scoped_commands) return error.GlinerBoundaryExternalFrame;
            const scope = try self.cb.glinerBoundaryScope(&.snapshot);
            if (!scope.active) return error.GlinerBoundaryExternalFrame;
        }
        try self.cb.checkExecutionControl();
        if (self.control) |control| try control.check();
    }

    pub fn configure(self: *Context, policy: ExecutionPolicy) void {
        self.resident_weights = policy == .optimized_v2;
        self.scoped_commands = policy == .optimized_v2;
    }

    pub fn finishSegment(self: *Context) !void {
        if (self.scope_generation) |generation| {
            _ = try self.cb.glinerBoundaryScope(&.{ .finish = .{ .generation = generation } });
            self.scope_generation = null;
        }
        try self.check();
    }

    fn ensureSegment(self: *Context) !void {
        if (!self.scoped_commands) return;
        var snapshot = try self.cb.glinerBoundaryScope(&.snapshot);
        if (self.scope_generation) |generation| {
            if (snapshot.active and snapshot.generation != generation) return error.GlinerBoundaryExternalFrame;
            if (!snapshot.active or snapshot.dispatches - self.scope_start_dispatches >= max_segment_dispatches) {
                try self.finishSegment();
                snapshot = try self.cb.glinerBoundaryScope(&.snapshot);
            }
        }
        if (self.scope_generation == null) {
            if (snapshot.active) return error.GlinerBoundaryExternalFrame;
            const scope = try self.cb.glinerBoundaryScope(&.{ .begin = .{
                .max_pending_device_bytes = self.limits.max_device_bytes,
                .max_dispatches = max_segment_dispatches,
            } });
            self.scope_generation = scope.generation;
            self.scope_start_dispatches = scope.dispatches;
        }
    }

    pub fn reserve(self: *Context, additional: usize) !void {
        var peak = try std.math.add(usize, self.current_bytes, additional);
        if (self.scoped_commands and self.scope_generation != null) {
            const scope = try self.cb.glinerBoundaryScope(&.snapshot);
            // Conservatively retain every pending allocation's charge, even
            // when some of it also has a live CT in this owner's entries.
            const pending_peak = try std.math.add(usize, peak, scope.pending_device_bytes);
            if (pending_peak > self.limits.max_device_bytes) {
                try self.finishSegment();
            } else peak = pending_peak;
        }
        if (peak > self.limits.max_device_bytes) return error.ResourceLimitExceeded;
        self.stats.peak_device_bytes = @max(self.stats.peak_device_bytes, peak);
    }

    pub fn execute(self: *Context, request: device.Request, elements: usize) !CT {
        return self.executeStorage(request, try bytes(elements));
    }

    fn executeStorage(self: *Context, request: device.Request, allocation_bytes: usize) !CT {
        try self.check();
        const staging_bytes = switch (request) {
            .upload_f32, .upload_i32 => allocation_bytes,
            .resident_f32 => |r| if (r.allow_host_weight) allocation_bytes else 0,
            .linear_reduced => |r| try std.math.add(usize, try r.precision.byteLen(r.out_dim, r.in_dim), try bytes(r.out_dim)),
            .embedding_reduced => |r| try std.math.add(usize, try r.precision.byteLen(r.vocabulary, r.width), try bytes(r.ids.len)),
            else => 0,
        };
        try self.reserve(try std.math.add(usize, allocation_bytes, staging_bytes));
        try self.ensureSegment();
        const output = try self.cb.glinerBoundaryDevice(&request);
        errdefer self.cb.free(output);
        try self.check();
        try self.entries.append(self.allocator, .{ .tensor = output, .bytes = allocation_bytes });
        self.current_bytes += allocation_bytes;
        self.stats.device_dispatches += 1;
        return output;
    }

    pub fn drop(self: *Context, tensor: CT) void {
        for (self.entries.items, 0..) |entry, i| if (entry.tensor == tensor) {
            self.current_bytes -= entry.bytes;
            _ = self.entries.swapRemove(i);
            self.cb.free(tensor);
            return;
        };
        unreachable;
    }

    pub fn kernel(self: *Context, kind: device.Kind, dimensions: []const usize, inputs: []const CT, scalar: f32) !CT {
        if (dimensions.len > 8 or inputs.len > device.max_inputs) return error.InvalidInputShape;
        var request = device.Kernel{ .kind = kind };
        for (dimensions, 0..) |n, i| request.dims[i] = try dim(n);
        for (inputs, 0..) |tensor, i| request.inputs[i] = tensor;
        request.scalars[0] = scalar;
        const layout = try request.layout();
        return self.execute(.{ .kernel = request }, layout.output_elements);
    }

    pub fn uploadIntegers(self: *Context, values: []const i32) !CT {
        const output = try self.execute(.{ .upload_i32 = .{ .values = values, .shape = &.{@intCast(values.len)} } }, values.len);
        self.stats.metadata_upload_bytes += try bytes(values.len);
        return output;
    }

    pub fn uploadMask(self: *Context, values: []const bool) !CT {
        const ints = try self.allocator.alloc(i32, values.len);
        defer self.allocator.free(ints);
        for (values, ints) |v, *target| target.* = @intFromBool(v);
        return self.uploadIntegers(ints);
    }

    pub fn weight(self: *Context, name: []const u8, shape: []const i64) !CT {
        if (self.weights.get(name)) |tensor| return tensor;
        try self.check();
        var elements: usize = 1;
        for (shape) |n| {
            if (n <= 0) return error.InvalidGlinerBoundaryWeightShape;
            elements = try std.math.mul(usize, elements, @intCast(n));
        }
        const tensor = if (self.resident_weights)
            // Its physical payload is covered by the model's retained lease.
            // Only the fresh request wrapper belongs to the request heap.
            try self.executeStorage(.{ .load_f32_weight = .{ .name = name, .shape = shape } }, 0)
        else blk: {
            const original = try self.cb.getWeight(name);
            defer self.cb.free(original);
            const actual = try self.cb.tensorShape(original, self.allocator);
            defer self.allocator.free(actual);
            if (!std.mem.eql(i64, actual, shape)) return error.InvalidGlinerBoundaryWeightShape;
            break :blk try self.execute(.{ .resident_f32 = .{ .input = original, .allow_host_weight = true } }, elements);
        };
        errdefer self.drop(tensor);
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        try self.weights.put(self.allocator, key, tensor);
        self.stats.charged_weight_bytes += try bytes(elements);
        return tensor;
    }

    pub fn derived(self: *Context, key: device.DerivedKey, shape: []const i64) !CT {
        if (!self.resident_weights) return error.InvalidBoundaryDeviceState;
        var elements: usize = 1;
        for (shape) |n| {
            if (n <= 0) return error.InvalidGlinerBoundaryWeightShape;
            elements = try std.math.mul(usize, elements, @intCast(n));
        }
        _ = try bytes(elements);
        return self.executeStorage(.{ .load_f32_derived = .{ .key = key, .shape = shape } }, 0);
    }

    pub fn publishDerived(self: *Context, key: device.DerivedKey, shape: []const i64, input: CT) !void {
        // Preparation publishes only synchronously completed physical tensors.
        try self.finishSegment();
        const retained = try self.cb.glinerBoundaryDevice(&.{ .publish_f32_derived = .{ .key = key, .shape = shape, .input = input } });
        self.cb.free(retained);
    }

    pub fn linear(self: *Context, input: CT, rows: usize, in_dim: usize, out_dim: usize, prefix: []const u8) !CT {
        var name: [256]u8 = undefined;
        const reduced = self.encoder_precision != .f32 and std.mem.startsWith(u8, prefix, "encoder.layer.");
        const weight_name = try std.fmt.bufPrint(&name, "{s}.weight", .{prefix});
        const w = if (reduced) try self.encoderMatrix(weight_name, out_dim, in_dim, true) else try self.weight(weight_name, &.{ @intCast(out_dim), @intCast(in_dim) });
        const b = try self.weight(try std.fmt.bufPrint(&name, "{s}.bias", .{prefix}), &.{@intCast(out_dim)});
        const elements = try count(&.{ rows, out_dim });
        // The strict linear owns one GEMM product while adding its bias.
        try self.reserve(try std.math.mul(usize, try bytes(elements), 2));
        if (reduced) return self.execute(.{ .linear_reduced = .{ .input = input, .weight = w, .bias = b, .rows = rows, .in_dim = in_dim, .out_dim = out_dim, .precision = self.encoder_precision } }, elements);
        return self.execute(.{ .linear = .{ .input = input, .weight = w, .bias = b, .rows = rows, .in_dim = in_dim, .out_dim = out_dim } }, elements);
    }

    fn encoderMatrix(self: *Context, name: []const u8, rows: usize, columns: usize, linear_slot: bool) !CT {
        if (self.weights.get(name)) |tensor| return tensor;
        if (self.encoder_precision == .f32) return error.InvalidBoundaryDeviceState;
        const raw_bytes = try self.encoder_precision.byteLen(rows, columns);
        // Prepared linear slots own an FP32 zero bias as well as the exact
        // native weight bytes. The actual checkpoint bias is charged by weight().
        const charged_bytes = try std.math.add(usize, raw_bytes, if (linear_slot) try bytes(rows) else 0);
        const tensor = try self.executeStorage(.{ .load_matrix = .{ .name = name, .rows = rows, .columns = columns, .precision = self.encoder_precision } }, charged_bytes);
        errdefer self.drop(tensor);
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        try self.weights.put(self.allocator, key, tensor);
        self.stats.charged_weight_bytes += charged_bytes;
        return tensor;
    }

    pub fn embedding(self: *Context, ids: []const i64, vocabulary: usize, width: usize) !CT {
        if (self.encoder_precision == .f32) return error.InvalidBoundaryDeviceState;
        const weight_tensor = try self.encoderMatrix("embeddings.word_embeddings.weight", vocabulary, width, false);
        const output = try self.execute(.{ .embedding_reduced = .{ .weight = weight_tensor, .ids = ids, .vocabulary = vocabulary, .width = width, .precision = self.encoder_precision } }, try count(&.{ ids.len, width }));
        self.stats.metadata_upload_bytes += try bytes(ids.len);
        return output;
    }

    pub fn norm(self: *Context, input: CT, rows: usize, width: usize, prefix: []const u8) !CT {
        return self.normEps(input, rows, width, prefix, 1e-5);
    }

    pub fn normEps(self: *Context, input: CT, rows: usize, width: usize, prefix: []const u8, eps: f32) !CT {
        var name: [256]u8 = undefined;
        const w = try self.weight(try std.fmt.bufPrint(&name, "{s}.weight", .{prefix}), &.{@intCast(width)});
        const b = try self.weight(try std.fmt.bufPrint(&name, "{s}.bias", .{prefix}), &.{@intCast(width)});
        return self.kernel(.norm, &.{ rows, width }, &.{ input, w, b }, eps);
    }

    fn validateDownload(self: *Context, tensor: CT, elements: usize, proposal: bool) !usize {
        try self.check();
        const nbytes = try bytes(elements);
        var owned = false;
        for (self.entries.items) |entry| if (entry.tensor == tensor) {
            if (entry.bytes != nbytes) return error.InvalidBoundaryDeviceShape;
            owned = true;
            break;
        };
        if (!owned) return error.InvalidBoundaryDeviceState;
        const current = if (proposal) self.stats.proposal_download_bytes else self.stats.result_download_bytes;
        const limit = if (proposal) self.limits.max_proposal_download_bytes else self.limits.max_result_download_bytes;
        if (try std.math.add(usize, current, nbytes) > limit) return error.ResourceLimitExceeded;
        return nbytes;
    }

    pub fn downloadInto(self: *Context, tensor: CT, output: []f32, proposal: bool) !void {
        try self.finishSegment();
        const nbytes = try self.validateDownload(tensor, output.len, proposal);
        try self.cb.glinerBoundaryDownload(tensor, output);
        if (proposal) {
            self.stats.proposal_download_bytes += nbytes;
            self.stats.proposal_download_calls += 1;
        } else {
            self.stats.result_download_bytes += nbytes;
            self.stats.result_download_calls += 1;
        }
        for (output) |v| if (!std.math.isFinite(v)) return error.NonFiniteBoundaryScore;
        try self.check();
    }

    pub fn downloadTo(self: *Context, allocator: std.mem.Allocator, tensor: CT, elements: usize, proposal: bool) ![]f32 {
        _ = try self.validateDownload(tensor, elements, proposal);
        const output = try allocator.alloc(f32, elements);
        errdefer allocator.free(output);
        try self.downloadInto(tensor, output, proposal);
        return output;
    }

    pub fn download(self: *Context, tensor: CT, elements: usize, proposal: bool) ![]f32 {
        return self.downloadTo(self.allocator, tensor, elements, proposal);
    }
};
