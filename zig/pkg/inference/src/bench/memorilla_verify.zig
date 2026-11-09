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

//! Independent module-boundary parity gate. It never treats missing CUDA or
//! absent reference tensors as a skipped success; decoder qualification is separate.
const std = @import("std");
const internal = @import("inference_internal");
const options = @import("build_options");
const model = internal.models.memorilla;
const pipeline = internal.pipelines.memorilla;
const safe = internal.models.safetensors;
const ops = internal.ops;
const native = internal.native_compute.native;
const A = std.mem.Allocator;

fn emit(a: A, io: std.Io, value: anytype) !void {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    try std.Io.File.stdout().writeStreamingAll(io, bytes);
    try std.Io.File.stdout().writeStreamingAll(io, "\n");
}

fn expectHash(actual: []const u8, expected: []const u8) !void {
    if (actual.len != 64 or !std.mem.eql(u8, actual, expected)) return error.ReferenceIdentityMismatch;
}

const Receipt = struct {
    schema_version: u32,
    kind: []const u8,
    release_bundle: bool,
    bundle_manifest_sha256: []const u8,
    bundle_weights_sha256: []const u8,
    execution: struct { device: []const u8, dtype: []const u8 },
    cases: []const []const u8,
    tensors: struct { path: []const u8, sha256: []const u8 },
};

const PrecisionProfile = enum {
    strict_f32_v1,
    trained_f32_v1,
    custom_f32_diagnostic,

    fn label(self: PrecisionProfile) []const u8 {
        return switch (self) {
            .strict_f32_v1 => "strict-f32-v1",
            .trained_f32_v1 => "trained-f32-v1",
            .custom_f32_diagnostic => "custom-f32-diagnostic",
        };
    }
};

const Sink = struct {
    a: A,
    io: std.Io,
    reader: *safe.MMapReader,
    case: []const u8,
    atol: f64,
    memory_atol: f64,
    rtol: f64,
    relative_l2_limit: ?f64,
    boundaries: usize = 0,
    mismatches: usize = 0,
    failed_boundaries: usize = 0,
    declared_readback_bytes: usize = 0,
    maximum_difference: f64 = 0,
    memory_maximum_difference: f64 = 0,
    memory_squared_error: f64 = 0,
    memory_squared_reference: f64 = 0,

    fn record(ptr: *anyopaque, cb: *const ops.ComputeBackend, name: []const u8, tensor: ops.CT) anyerror!void {
        const self: *Sink = @ptrCast(@alignCast(ptr));
        const key = try std.fmt.allocPrint(self.a, "{s}.{s}", .{ self.case, name });
        defer self.a.free(key);
        var expected = try self.reader.readTensor(key);
        defer expected.deinit();
        if (expected.dtype != .f32) return error.ReferenceRequiresFloat32;
        const actual = try cb.toFloat32(tensor, self.a);
        defer self.a.free(actual);
        const values = expected.asFloat32();
        if (actual.len != values.len) return error.ReferenceShapeMismatch;
        self.declared_readback_bytes += actual.len * @sizeOf(f32);
        var maximum: f64 = 0;
        var mismatches: usize = 0;
        var squared_error: f64 = 0;
        var squared_reference: f64 = 0;
        const atol = if (std.mem.eql(u8, name, "memory_tokens")) self.memory_atol else self.atol;
        for (actual, values) |got, want| {
            if (!std.math.isFinite(got) or !std.math.isFinite(want)) return error.NonfiniteMemorillaOutput;
            const delta = @abs(@as(f64, got) - @as(f64, want));
            maximum = @max(maximum, delta);
            squared_error += delta * delta;
            squared_reference += @as(f64, want) * @as(f64, want);
            if (delta > atol + self.rtol * @abs(@as(f64, want))) mismatches += 1;
        }
        const relative_l2: ?f64 = if (squared_reference > 0) @sqrt(squared_error / squared_reference) else null;
        const normwise_passed = if (self.relative_l2_limit) |limit|
            if (relative_l2) |relative| relative <= limit else maximum <= atol
        else
            true;
        const passed = mismatches == 0 and normwise_passed;
        self.boundaries += 1;
        self.mismatches += mismatches;
        self.failed_boundaries += @intFromBool(!passed);
        self.maximum_difference = @max(self.maximum_difference, maximum);
        if (std.mem.eql(u8, name, "memory_tokens")) {
            self.memory_maximum_difference = @max(self.memory_maximum_difference, maximum);
            self.memory_squared_error += squared_error;
            self.memory_squared_reference += squared_reference;
        }
        try emit(self.a, self.io, .{ .event = "boundary", .case = self.case, .name = name, .elements = values.len, .max_absolute_difference = maximum, .mismatches = mismatches, .atol = atol, .rtol = self.rtol, .relative_l2_limit = self.relative_l2_limit, .l2_error = @sqrt(squared_error), .reference_l2 = @sqrt(squared_reference), .relative_l2_error = relative_l2, .normwise_passed = normwise_passed, .passed = passed });
    }
};

const Args = struct {
    bundle: []const u8,
    reference: []const u8,
    backend: enum { native, cuda } = .native,
    allow_synthetic: bool = false,
    precision_profile: PrecisionProfile = .strict_f32_v1,
    atol: f64 = 1e-5,
    memory_atol: f64 = 1e-5,
    rtol: f64 = 1e-5,
    relative_l2_limit: ?f64 = null,
    max_documents: usize = 4096,
};

fn parseArgs(init: std.process.Init) !Args {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    var bundle: ?[]const u8 = null;
    var reference: ?[]const u8 = null;
    var result = Args{ .bundle = undefined, .reference = undefined };
    var profile_seen = false;
    var numeric_tolerance_seen = false;
    while (args.next()) |key| {
        if (std.mem.eql(u8, key, "--allow-synthetic")) {
            result.allow_synthetic = true;
            continue;
        }
        const value = args.next() orelse return error.MissingArgument;
        if (std.mem.eql(u8, key, "--bundle") and bundle == null) bundle = value else if (std.mem.eql(u8, key, "--reference") and reference == null) reference = value else if (std.mem.eql(u8, key, "--backend")) {
            result.backend = std.meta.stringToEnum(@TypeOf(result.backend), value) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, key, "--precision-profile") and !profile_seen) {
            result.precision_profile = if (std.mem.eql(u8, value, "strict-f32-v1")) .strict_f32_v1 else if (std.mem.eql(u8, value, "trained-f32-v1")) .trained_f32_v1 else return error.InvalidPrecisionProfile;
            profile_seen = true;
        } else if (std.mem.eql(u8, key, "--atol")) {
            result.atol = try std.fmt.parseFloat(f64, value);
            numeric_tolerance_seen = true;
        } else if (std.mem.eql(u8, key, "--rtol")) {
            result.rtol = try std.fmt.parseFloat(f64, value);
            numeric_tolerance_seen = true;
        } else if (std.mem.eql(u8, key, "--max-documents")) result.max_documents = try std.fmt.parseInt(usize, value, 10) else return error.UnknownArgument;
    }
    result.bundle = bundle orelse return error.MissingBundle;
    result.reference = reference orelse return error.MissingReference;
    if (profile_seen and numeric_tolerance_seen) return error.NamedPrecisionProfileCannotBeOverridden;
    if (numeric_tolerance_seen) {
        result.precision_profile = .custom_f32_diagnostic;
        result.memory_atol = result.atol;
    } else if (result.precision_profile == .trained_f32_v1) {
        // Frozen after Stage 1 F32/F64 analysis, before held-out Stage 3 runs.
        // See scripts/memorilla/PRECISION.md. Do not retune per checkpoint.
        result.atol = 2e-4;
        result.memory_atol = 5e-5;
        result.rtol = 1e-5;
        result.relative_l2_limit = 5e-6;
    }
    if (!std.math.isFinite(result.atol) or !std.math.isFinite(result.rtol) or result.atol < 0 or result.rtol < 0)
        return error.InvalidTolerance;
    if (result.max_documents == 0 or result.max_documents > 65536) return error.InvalidDocumentLimit;
    return result;
}

fn run(cb: *const ops.ComputeBackend, a: A, io: std.Io, args: Args, bundle: *model.Bundle, reader: *safe.MMapReader, receipt: Receipt) !Sink {
    var sink = Sink{ .a = a, .io = io, .reader = reader, .case = "", .atol = args.atol, .memory_atol = args.memory_atol, .rtol = args.rtol, .relative_l2_limit = args.relative_l2_limit };
    for (receipt.cases) |case| {
        sink.case = case;
        var buf: [192]u8 = undefined;
        var documents = try reader.readTensor(try std.fmt.bufPrint(&buf, "{s}.input.documents", .{case}));
        defer documents.deinit();
        var question = try reader.readTensor(try std.fmt.bufPrint(&buf, "{s}.input.question", .{case}));
        defer question.deinit();
        var padding = try reader.readTensor(try std.fmt.bufPrint(&buf, "{s}.input.padding_mask", .{case}));
        defer padding.deinit();
        // The shared SafeTensors reader represents both BOOL and U8 as u8.
        // Require binary values below before constructing the padding mask.
        if (documents.dtype != .f32 or question.dtype != .f32 or (padding.dtype != .bool_ and padding.dtype != .u8) or
            documents.shape.len != 3 or documents.shape[0] <= 0 or documents.shape[1] <= 0 or
            documents.shape[2] != bundle.config.embedding_dim) return error.InvalidReferenceInput;
        const batch: usize = @intCast(documents.shape[0]);
        const count: usize = @intCast(documents.shape[1]);
        if (question.shape.len != 2 or question.shape[0] != batch or question.shape[1] != bundle.config.embedding_dim or
            padding.shape.len != 2 or padding.shape[0] != batch or padding.shape[1] != count)
            return error.InvalidReferenceInput;
        const mask = try a.alloc(bool, padding.data.len);
        defer a.free(mask);
        for (padding.data, mask) |raw, *flag| {
            if (raw > 1) return error.InvalidReferenceInput;
            flag.* = raw == 1;
        }
        for (documents.asFloat32()) |x| if (!std.math.isFinite(x)) return error.InvalidReferenceInput;
        for (question.asFloat32()) |x| if (!std.math.isFinite(x)) return error.InvalidReferenceInput;
        const doc_input = try cb.fromFloat32Shape(documents.asFloat32(), &.{ @intCast(batch * count), @intCast(bundle.config.embedding_dim) });
        defer cb.free(doc_input);
        const question_input = try cb.fromFloat32Shape(question.asFloat32(), &.{ @intCast(batch), @intCast(bundle.config.embedding_dim) });
        defer cb.free(question_input);
        const output = try pipeline.forward(cb, a, bundle.config, .{
            .documents = doc_input,
            .question = question_input,
            .padding_mask = mask,
            .batch = batch,
            .num_documents = count,
            .max_document_rows = args.max_documents,
        }, .{ .context = &sink, .record = Sink.record });
        cb.free(output);
    }
    const expected_boundaries = receipt.cases.len * (6 + 8 * bundle.config.num_cross_attn_layers);
    if (sink.boundaries != expected_boundaries) return error.IncompleteReferenceComparison;
    return sink;
}

pub fn main(init: std.process.Init) !void {
    const a = std.heap.c_allocator;
    const args = try parseArgs(init);
    var bundle = try model.Bundle.open(a, args.bundle, args.allow_synthetic);
    defer bundle.deinit();
    const receipt_path = try std.fs.path.join(a, &.{ args.reference, "reference.json" });
    defer a.free(receipt_path);
    const bytes = try internal.util.c_file.readFileMax(a, receipt_path, 4 * 1024 * 1024);
    defer a.free(bytes);
    var receipt = try std.json.parseFromSlice(Receipt, a, bytes, .{ .ignore_unknown_fields = true });
    defer receipt.deinit();
    const source = receipt.value;
    if (source.schema_version != 1 or !std.mem.eql(u8, source.kind, "memorilla_module_reference") or
        !std.mem.eql(u8, source.execution.device, "cpu") or !std.mem.eql(u8, source.execution.dtype, "float32") or
        source.release_bundle != bundle.release or source.cases.len == 0 or source.cases.len > 64 or
        !std.mem.eql(u8, source.tensors.path, "reference.safetensors")) return error.InvalidReferenceReceipt;
    for (source.cases, 0..) |case, i| {
        if (case.len == 0 or case.len > 100 or std.mem.indexOfScalar(u8, case, '.') != null) return error.InvalidReferenceCase;
        for (source.cases[0..i]) |previous| if (std.mem.eql(u8, case, previous)) return error.DuplicateReferenceCase;
    }
    try expectHash(&bundle.manifest_sha256, source.bundle_manifest_sha256);
    try expectHash(&bundle.weights_sha256, source.bundle_weights_sha256);
    const tensor_path = try std.fs.path.join(a, &.{ args.reference, source.tensors.path });
    defer a.free(tensor_path);
    var reader = try safe.MMapReader.openFileAbsoluteLimited(a, tensor_path, 512 * 1024 * 1024, 4 * 1024 * 1024);
    defer reader.deinit();
    try safe.validateReader(a, &reader);
    const tensor_sha = model.digest(reader.file_bytes);
    try expectHash(&tensor_sha, source.tensors.sha256);
    var store = try bundle.loadFloat32Store();
    defer store.deinitOwned();
    try emit(a, init.io, .{ .event = "ready", .scope = "memorilla_module_fixture_parity", .backend = args.backend, .release_bundle = bundle.release, .arithmetic = "source_weights_widened_f32_activations_f32", .precision_profile = args.precision_profile.label(), .intermediate_atol = args.atol, .memory_tokens_atol = args.memory_atol, .rtol = args.rtol, .relative_l2_limit = args.relative_l2_limit, .maximum_document_rows = args.max_documents, .bundle_manifest_sha256 = bundle.manifest_sha256[0..], .reference_sha256 = tensor_sha[0..], .decoder_qualified = false });
    const sink = switch (args.backend) {
        .native => blk: {
            var compute = native.NativeCompute.initWithIo(a, &store, null, init.io);
            defer compute.deinit();
            const cb = compute.computeBackend();
            break :blk try run(&cb, a, init.io, args, &bundle, &reader, source);
        },
        .cuda => blk: {
            if (comptime !options.enable_cuda) return error.CudaNotEnabled;
            var compute = try internal.native_compute.cuda.CudaCompute.init(a);
            defer compute.deinit();
            compute.strict_f32_weights = true;
            // The existing upload helper adopts its key at final insertion.
            // Reserve the validated inventory first so insertion cannot fail
            // allocating a map entry after that helper has adopted/freed it.
            try compute.resident_weights.ensureUnusedCapacity(a, store.resident_weights.count());
            var iter = store.resident_weights.iterator();
            while (iter.next()) |entry| {
                const key = try a.dupe(u8, entry.key_ptr.*);
                errdefer a.free(key);
                try compute.insertWeightFromLoaded(key, entry.value_ptr);
            }
            const cb = compute.computeBackend();
            if (cb.kind() != .cuda) return error.RequiredCudaBackendUnavailable;
            const result = try run(&cb, a, init.io, args, &bundle, &reader, source);
            const stats = compute.snapshotStats();
            if (stats.launch_linear == 0 or stats.launch_attention == 0 or stats.launch_norm_layer == 0 or
                stats.resident_weight_bytes == 0 or stats.cross_backend_sync_fallbacks != 0 or
                stats.d2h_bytes != result.declared_readback_bytes)
                return error.CudaExecutionEvidenceMissing;
            try emit(a, init.io, .{ .event = "cuda_execution", .device_name = compute.ctx.info.nameSlice(), .compute_major = compute.ctx.info.compute_major, .compute_minor = compute.ctx.info.compute_minor, .kernel_launches = compute.ctx.stats.kernel_launches, .linear_launches = stats.launch_linear, .attention_launches = stats.launch_attention, .layer_norm_launches = stats.launch_norm_layer, .resident_weight_bytes = stats.resident_weight_bytes, .download_bytes = stats.d2h_bytes, .declared_trace_readback_bytes = result.declared_readback_bytes, .cross_backend_sync_fallbacks = stats.cross_backend_sync_fallbacks });
            break :blk result;
        },
    };
    try emit(a, init.io, .{ .event = "complete", .backend = args.backend, .precision_profile = args.precision_profile.label(), .cases = source.cases.len, .boundaries = sink.boundaries, .failed_boundaries = sink.failed_boundaries, .mismatches = sink.mismatches, .max_absolute_difference = sink.maximum_difference, .memory_tokens_max_absolute_difference = sink.memory_maximum_difference, .memory_tokens_l2_error = @sqrt(sink.memory_squared_error), .memory_tokens_relative_l2_error = if (sink.memory_squared_reference > 0) @sqrt(sink.memory_squared_error / sink.memory_squared_reference) else null, .module_parity_passed = sink.failed_boundaries == 0, .decoder_qualified = false });
    if (sink.failed_boundaries != 0) return error.MemorillaReferenceMismatch;
}
