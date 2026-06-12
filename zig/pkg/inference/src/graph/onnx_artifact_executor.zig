// Copyright 2026 Antfly, Inc.
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

const std = @import("std");

const backends = @import("../backends/backends.zig");
const compiled_artifact = @import("../compiled_artifact.zig");
const cache_mod = @import("cache.zig");
const model_runtime = @import("model_runtime.zig");
const onnx_kv_cache = @import("onnx_kv_cache.zig");

pub const InputMaterialization = enum {
    /// Current compiled graph artifacts expose explicit inputs for host-computed
    /// values when the runtime cannot own them yet.
    host_assisted_explicit_kv,
    /// Decoder-style ONNX ABI. The runtime owns host-side KV tensors and feeds
    /// them back into later ORT calls.
    runtime_owned_host_cache,
    /// Decoder-style ONNX ABI using ORT IO binding. Antfly inference keeps opaque
    /// OrtValue handles for present KV tensors and feeds them back as past KV.
    backend_owned_kv,
};

pub const Executor = struct {
    allocator: std.mem.Allocator,
    prefill_manifest: std.json.Parsed(compiled_artifact.Manifest),
    decode_manifest: ?std.json.Parsed(compiled_artifact.Manifest) = null,
    fallback_vocab_size: usize,
    input_materialization: InputMaterialization = .host_assisted_explicit_kv,

    const executor_vtable = model_runtime.ModelExecutor.VTable{
        .create_runtime = createRuntime,
        .deinit = deinit,
    };

    pub fn modelExecutor(self: *@This()) model_runtime.ModelExecutor {
        return .{ .ptr = self, .vtable = &executor_vtable };
    }

    fn createRuntime(ctx: *anyopaque, allocator: std.mem.Allocator) !model_runtime.ModelRuntime {
        const self: *Executor = @ptrCast(@alignCast(ctx));
        const prefill_artifact = self.prefill_manifest.value;
        const prefill_session = try backends.onnx.createSessionWithOptions(
            allocator,
            prefill_artifact.artifact_path,
            artifactSessionOptions(prefill_artifact),
        );
        errdefer prefill_session.close();
        const prefill_input_materialization = detectInputMaterialization(
            self.input_materialization,
            prefill_session.inputInfo(),
            prefill_session.outputInfo(),
        );

        var decode_session: ?backends.Session = null;
        errdefer if (decode_session) |session| session.close();
        var decode_input_materialization: InputMaterialization = .host_assisted_explicit_kv;
        if (self.decode_manifest) |decode_manifest| {
            const decode_artifact = decode_manifest.value;
            decode_session = try backends.onnx.createSessionWithOptions(
                allocator,
                decode_artifact.artifact_path,
                artifactSessionOptions(decode_artifact),
            );
            decode_input_materialization = detectInputMaterialization(
                self.input_materialization,
                decode_session.?.inputInfo(),
                decode_session.?.outputInfo(),
            );
        }

        const runtime_ctx = try allocator.create(Runtime);
        runtime_ctx.* = .{
            .allocator = allocator,
            .prefill_artifact = &self.prefill_manifest.value,
            .decode_artifact = if (self.decode_manifest) |*manifest| &manifest.value else null,
            .fallback_vocab_size = self.fallback_vocab_size,
            .prefill_input_materialization = prefill_input_materialization,
            .decode_input_materialization = decode_input_materialization,
            .prefill_session = prefill_session,
            .decode_session = decode_session,
            .kv_cache = onnx_kv_cache.KvCache.init(allocator),
            .ort_kv_cache = backends.onnx.RetainedValueCache.init(allocator),
        };
        return .{ .ptr = runtime_ctx, .vtable = &Runtime.runtime_vtable };
    }

    fn deinit(ctx: *anyopaque) void {
        const self: *Executor = @ptrCast(@alignCast(ctx));
        const allocator = self.allocator;
        self.prefill_manifest.deinit();
        if (self.decode_manifest) |*manifest| manifest.deinit();
        allocator.destroy(self);
    }
};

pub fn createModelExecutorFromManifestPath(
    allocator: std.mem.Allocator,
    io: std.Io,
    manifest_path: []const u8,
    fallback_vocab_size: usize,
) !*Executor {
    return createModelExecutorFromManifestPaths(allocator, io, manifest_path, null, fallback_vocab_size);
}

pub fn createModelExecutorFromManifestPaths(
    allocator: std.mem.Allocator,
    io: std.Io,
    prefill_manifest_path: []const u8,
    decode_manifest_path: ?[]const u8,
    fallback_vocab_size: usize,
) !*Executor {
    var prefill_manifest = try compiled_artifact.readManifest(allocator, io, prefill_manifest_path);
    errdefer prefill_manifest.deinit();
    var decode_manifest: ?std.json.Parsed(compiled_artifact.Manifest) = null;
    errdefer if (decode_manifest) |*manifest| manifest.deinit();
    if (decode_manifest_path) |path| {
        decode_manifest = try compiled_artifact.readManifest(allocator, io, path);
    }

    const executor = try allocator.create(Executor);
    executor.* = .{
        .allocator = allocator,
        .prefill_manifest = prefill_manifest,
        .decode_manifest = decode_manifest,
        .fallback_vocab_size = fallback_vocab_size,
    };
    return executor;
}

pub fn createModelExecutorFromPackageManifestPath(
    allocator: std.mem.Allocator,
    io: std.Io,
    package_manifest_path: []const u8,
    prefill_seq_len: usize,
    prefill_query_seq_len: usize,
    prefill_attention_mode: []const u8,
    fallback_vocab_size: usize,
) !*Executor {
    var package = try compiled_artifact.readPackageManifest(allocator, io, package_manifest_path);
    defer package.deinit();

    try compiled_artifact.validatePackageManifest(package.value, "onnx", package.value.model_dir, "onnx_graph", null);

    const prefill_entry = try compiled_artifact.findUniqueMatchingPackageEntry(package.value, .{
        .artifact_role = compiled_artifact.artifact_role_prefill,
        .seq_len = prefill_seq_len,
        .query_seq_len = prefill_query_seq_len,
        .attention_mode = prefill_attention_mode,
    });
    const prefill_manifest_path = if (prefill_entry) |entry| entry.manifest_path else return error.ArtifactShapeMismatch;

    const decode_entry = try compiled_artifact.findUniqueMatchingPackageEntry(package.value, .{
        .artifact_role = compiled_artifact.artifact_role_decode,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
    });
    const decode_manifest_path = if (decode_entry) |entry| entry.manifest_path else null;
    return createModelExecutorFromManifestPaths(
        allocator,
        io,
        prefill_manifest_path,
        decode_manifest_path,
        fallback_vocab_size,
    );
}

pub fn deinitModelExecutorPtr(ctx: *anyopaque) void {
    Executor.deinit(ctx);
}

const Runtime = struct {
    allocator: std.mem.Allocator,
    prefill_artifact: *const compiled_artifact.Manifest,
    decode_artifact: ?*const compiled_artifact.Manifest,
    fallback_vocab_size: usize,
    prefill_input_materialization: InputMaterialization,
    decode_input_materialization: InputMaterialization,
    prefill_session: backends.Session,
    decode_session: ?backends.Session,
    kv_cache: onnx_kv_cache.KvCache,
    ort_kv_cache: backends.onnx.RetainedValueCache,

    const runtime_vtable = model_runtime.ModelRuntime.VTable{
        .capabilities = capabilities,
        .prefill = prefill,
        .decode = decode,
        .deinit = deinit,
        .reset = reset,
    };

    fn capabilities(ctx: *anyopaque) model_runtime.RuntimeCapabilities {
        const self: *Runtime = @ptrCast(@alignCast(ctx));
        const supports_decode = self.decode_session != null and switch (self.prefill_input_materialization) {
            .runtime_owned_host_cache => self.decode_input_materialization == .runtime_owned_host_cache,
            .backend_owned_kv => self.decode_input_materialization == .backend_owned_kv,
            .host_assisted_explicit_kv => false,
        };
        return .{
            .supports_decode = supports_decode,
            .state_ownership = switch (if (supports_decode) self.decode_input_materialization else self.prefill_input_materialization) {
                .host_assisted_explicit_kv => .host_assisted_inputs,
                .runtime_owned_host_cache => .runtime_owned_host_cache,
                .backend_owned_kv => .backend_owned,
            },
        };
    }

    fn prefill(
        ctx: *anyopaque,
        allocator: std.mem.Allocator,
        request: model_runtime.PrefillRequest,
    ) !model_runtime.ModelOutput {
        const self: *Runtime = @ptrCast(@alignCast(ctx));
        if (!std.mem.eql(u8, self.prefill_artifact.attention_mode, attentionModeName(request.attention_mode))) {
            return error.ArtifactShapeMismatch;
        }
        if (request.seq_len != self.prefill_artifact.seq_len or request.query_seq_len != self.prefill_artifact.query_seq_len) {
            return error.ArtifactShapeMismatch;
        }
        if (request.input_ids.len != self.prefill_artifact.query_seq_len) {
            return error.ArtifactShapeMismatch;
        }

        const last_logits = switch (self.prefill_input_materialization) {
            .host_assisted_explicit_kv => try runFullArtifactLastLogitsWithSession(
                allocator,
                self.prefill_artifact.*,
                self.prefill_session,
                request.input_ids,
                self.fallback_vocab_size,
                request.query_seq_len,
            ),
            .runtime_owned_host_cache => try runDecoderArtifactWithSession(
                allocator,
                self.prefill_session,
                &self.kv_cache,
                request.input_ids,
                0,
                request.input_ids.len,
                false,
                self.fallback_vocab_size,
            ),
            .backend_owned_kv => try runBackendOwnedDecoderArtifactWithSession(
                allocator,
                self.prefill_session,
                &self.ort_kv_cache,
                request.input_ids,
                0,
                request.input_ids.len,
                false,
                self.decode_session != null,
                self.fallback_vocab_size,
            ),
        };
        return .{ .logits = last_logits };
    }

    fn decode(
        ctx: *anyopaque,
        allocator: std.mem.Allocator,
        request: model_runtime.DecodeRequest,
    ) !model_runtime.ModelOutput {
        const self: *Runtime = @ptrCast(@alignCast(ctx));
        const decode_session = self.decode_session orelse return error.UnsupportedDecode;
        const decode_artifact = self.decode_artifact orelse return error.UnsupportedDecode;
        const materialization = self.prefill_input_materialization;
        if (materialization != self.decode_input_materialization) return error.UnsupportedDecode;
        if (materialization != .runtime_owned_host_cache and materialization != .backend_owned_kv) return error.UnsupportedDecode;
        if (request.attention_mode != .paged_decode) return error.UnsupportedDecode;
        if (!std.mem.eql(u8, decode_artifact.attention_mode, "paged_decode")) return error.ArtifactShapeMismatch;
        const input_ids = [_]i64{request.token_id};
        const last_logits = switch (materialization) {
            .runtime_owned_host_cache => blk: {
                if (self.kv_cache.tensors.len == 0) return error.MissingPastKeyValue;
                break :blk try runDecoderArtifactWithSession(
                    allocator,
                    decode_session,
                    &self.kv_cache,
                    &input_ids,
                    request.position,
                    request.position + 1,
                    true,
                    self.fallback_vocab_size,
                );
            },
            .backend_owned_kv => blk: {
                if (self.ort_kv_cache.values.len == 0) return error.MissingPastKeyValue;
                break :blk try runBackendOwnedDecoderArtifactWithSession(
                    allocator,
                    decode_session,
                    &self.ort_kv_cache,
                    &input_ids,
                    request.position,
                    request.position + 1,
                    true,
                    true,
                    self.fallback_vocab_size,
                );
            },
            .host_assisted_explicit_kv => unreachable,
        };
        return .{ .logits = last_logits };
    }

    fn reset(ctx: *anyopaque) !void {
        const self: *Runtime = @ptrCast(@alignCast(ctx));
        self.kv_cache.deinit();
        self.kv_cache = onnx_kv_cache.KvCache.init(self.allocator);
        self.ort_kv_cache.deinit();
        self.ort_kv_cache = backends.onnx.RetainedValueCache.init(self.allocator);
    }

    fn deinit(ctx: *anyopaque) void {
        const self: *Runtime = @ptrCast(@alignCast(ctx));
        const allocator = self.allocator;
        self.kv_cache.deinit();
        self.ort_kv_cache.deinit();
        if (self.decode_session) |session| session.close();
        self.prefill_session.close();
        allocator.destroy(self);
    }
};

pub fn detectInputMaterialization(
    configured: InputMaterialization,
    input_info: []const backends.TensorInfo,
    output_info: []const backends.TensorInfo,
) InputMaterialization {
    if (configured != .host_assisted_explicit_kv) return configured;
    if (!hasTensorInfoNamed(input_info, "input_ids")) return configured;
    if (onnx_kv_cache.supportsPastPresentIo(input_info, output_info)) {
        return .backend_owned_kv;
    }
    return configured;
}

pub fn stateOwnershipName(input_materialization: InputMaterialization) []const u8 {
    return switch (input_materialization) {
        .host_assisted_explicit_kv => "host_assisted_inputs",
        .runtime_owned_host_cache => "runtime_owned_host_cache",
        .backend_owned_kv => "backend_owned",
    };
}

fn artifactSessionOptions(artifact: compiled_artifact.Manifest) backends.onnx.SessionOptions {
    const debug_outputs_enabled = artifact.onnx_output_node_ids.len > 1;
    return if (debug_outputs_enabled or artifact.onnx_input_node_ids.len > 0)
        .{ .low_memory = true }
    else
        .{};
}

fn runFullArtifactLastLogitsWithSession(
    allocator: std.mem.Allocator,
    artifact: compiled_artifact.Manifest,
    session: backends.Session,
    input_ids: []const i64,
    fallback_vocab_size: usize,
    query_seq_len: usize,
) ![]f32 {
    const input_info = session.inputInfo();
    if (input_info.len == 0) return error.UnsupportedArtifactInputs;

    const full_inputs = try buildFullInputs(allocator, artifact, input_info, input_ids);
    var full_inputs_owned = true;
    errdefer if (full_inputs_owned) deinitTensorSlice(allocator, full_inputs);

    const outputs = try session.run(full_inputs, allocator);
    var outputs_owned = true;
    errdefer if (outputs_owned) deinitTensorSlice(allocator, outputs);
    deinitTensorSlice(allocator, full_inputs);
    full_inputs_owned = false;

    if (outputs.len == 0) return error.MissingValue;
    const logits = try tensorToOwnedF32(allocator, &outputs[0]);
    defer allocator.free(logits);
    const output_logit_width = try inferOutputLogitWidth(
        outputs[0].shape,
        logits.len,
        query_seq_len,
        fallback_vocab_size,
    );
    const last_logits = try allocator.dupe(f32, logits[logits.len - output_logit_width ..]);
    deinitTensorSlice(allocator, outputs);
    outputs_owned = false;
    return last_logits;
}

fn buildFullInputs(
    allocator: std.mem.Allocator,
    artifact: compiled_artifact.Manifest,
    input_info: []const backends.TensorInfo,
    input_ids: []const i64,
) ![]backends.Tensor {
    if (input_info.len == 1 and artifact.onnx_input_node_ids.len == 0) {
        const inputs = try allocator.alloc(backends.Tensor, 1);
        errdefer allocator.free(inputs);
        const shape = if (input_info[0].shape.len == 2)
            &[_]i64{ resolvePositiveDimOrDefault(input_info[0].shape[0], 1), @intCast(input_ids.len) }
        else
            &[_]i64{@intCast(input_ids.len)};
        inputs[0] = try initIntTensorForDType(allocator, input_info[0].name, shape, input_ids, input_info[0].dtype);
        return inputs;
    }
    return error.UnsupportedArtifactInputs;
}

fn runDecoderArtifactWithSession(
    allocator: std.mem.Allocator,
    session: backends.Session,
    kv_cache: *onnx_kv_cache.KvCache,
    input_ids: []const i64,
    position_start: usize,
    total_sequence_len: usize,
    use_cached_past: bool,
    fallback_vocab_size: usize,
) ![]f32 {
    const input_info = session.inputInfo();
    const output_info = session.outputInfo();

    var inputs = std.ArrayListUnmanaged(backends.Tensor).empty;
    defer {
        for (inputs.items) |*tensor| tensor.deinit();
        inputs.deinit(allocator);
    }

    try appendDecoderInputIds(allocator, input_info, input_ids, &inputs);
    try appendDecoderAttentionMask(allocator, input_info, total_sequence_len, &inputs);
    try appendDecoderPositionIds(allocator, input_info, position_start, input_ids.len, &inputs);
    try appendDecoderUseCacheBranch(allocator, input_info, use_cached_past, &inputs);
    try appendDecoderNumLogitsToKeep(allocator, input_info, &inputs);
    if (use_cached_past) {
        try onnx_kv_cache.appendPastInputs(allocator, input_info, kv_cache, &inputs);
    } else {
        try appendDecoderEmptyPastInputs(allocator, input_info, &inputs);
    }

    const outputs = try session.run(inputs.items, allocator);
    var outputs_owned = true;
    errdefer if (outputs_owned) deinitTensorSlice(allocator, outputs);

    const logits_index = findDecoderLogitsOutputIndex(outputs) orelse return error.InvalidArtifactOutput;
    const logits_tensor = outputs[logits_index];
    const logits = try tensorToOwnedF32(allocator, &logits_tensor);
    defer allocator.free(logits);
    const output_logit_width = try inferOutputLogitWidth(
        logits_tensor.shape,
        logits.len,
        input_ids.len,
        fallback_vocab_size,
    );
    const last_logits = try allocator.dupe(f32, logits[logits.len - output_logit_width ..]);
    try kv_cache.replace(allocator, output_info, outputs);
    outputs_owned = false;
    return last_logits;
}

fn runBackendOwnedDecoderArtifactWithSession(
    allocator: std.mem.Allocator,
    session: backends.Session,
    kv_cache: *backends.onnx.RetainedValueCache,
    input_ids: []const i64,
    position_start: usize,
    total_sequence_len: usize,
    use_cached_past: bool,
    retain_cache_outputs: bool,
    fallback_vocab_size: usize,
) ![]f32 {
    const input_info = session.inputInfo();
    const output_info = session.outputInfo();

    var tensor_inputs = std.ArrayListUnmanaged(backends.Tensor).empty;
    defer {
        for (tensor_inputs.items) |*tensor| tensor.deinit();
        tensor_inputs.deinit(allocator);
    }

    var retained_inputs = std.ArrayListUnmanaged(backends.onnx.RetainedInput).empty;
    defer retained_inputs.deinit(allocator);

    var retain_output_names = std.ArrayListUnmanaged([]const u8).empty;
    defer retain_output_names.deinit(allocator);

    try appendDecoderInputIds(allocator, input_info, input_ids, &tensor_inputs);
    try appendDecoderAttentionMask(allocator, input_info, total_sequence_len, &tensor_inputs);
    try appendDecoderPositionIds(allocator, input_info, position_start, input_ids.len, &tensor_inputs);
    try appendDecoderUseCacheBranch(allocator, input_info, use_cached_past, &tensor_inputs);
    try appendDecoderNumLogitsToKeep(allocator, input_info, &tensor_inputs);
    if (use_cached_past) {
        try appendRetainedPastInputs(allocator, input_info, kv_cache, &retained_inputs);
    } else {
        try appendDecoderEmptyPastInputs(allocator, input_info, &tensor_inputs);
    }
    if (retain_cache_outputs) {
        try appendPresentOutputNames(allocator, output_info, &retain_output_names);
    }

    var run_result = try backends.onnx.runWithBoundValues(
        session,
        tensor_inputs.items,
        retained_inputs.items,
        retain_output_names.items,
        allocator,
    );
    defer run_result.deinit();

    const logits_index = findDecoderLogitsOutputIndex(run_result.tensors) orelse return error.InvalidArtifactOutput;
    const logits_tensor = run_result.tensors[logits_index];
    const logits = try tensorToOwnedF32(allocator, &logits_tensor);
    defer allocator.free(logits);
    const output_logit_width = try inferOutputLogitWidth(
        logits_tensor.shape,
        logits.len,
        input_ids.len,
        fallback_vocab_size,
    );
    const last_logits = try allocator.dupe(f32, logits[logits.len - output_logit_width ..]);

    if (retain_cache_outputs) {
        const retained_outputs = run_result.retained_outputs;
        run_result.retained_outputs = &.{};
        kv_cache.replace(retained_outputs);
    }
    return last_logits;
}

fn appendDecoderInputIds(
    allocator: std.mem.Allocator,
    input_info: []const backends.TensorInfo,
    input_ids: []const i64,
    inputs: *std.ArrayListUnmanaged(backends.Tensor),
) !void {
    const info = tensorInfoByName(input_info, "input_ids") orelse return error.UnsupportedArtifactInputs;
    if (info.shape.len == 1) {
        const shape = [_]i64{@intCast(input_ids.len)};
        try inputs.append(allocator, try initIntTensorForDType(allocator, info.name, &shape, input_ids, info.dtype));
        return;
    }
    if (info.shape.len == 2) {
        const shape = [_]i64{ resolvePositiveDimOrDefault(info.shape[0], 1), @intCast(input_ids.len) };
        try inputs.append(allocator, try initIntTensorForDType(allocator, info.name, &shape, input_ids, info.dtype));
        return;
    }
    return error.UnsupportedArtifactInputs;
}

fn appendDecoderAttentionMask(
    allocator: std.mem.Allocator,
    input_info: []const backends.TensorInfo,
    total_sequence_len: usize,
    inputs: *std.ArrayListUnmanaged(backends.Tensor),
) !void {
    const info = tensorInfoByName(input_info, "attention_mask") orelse return;
    const values = try allocator.alloc(i64, total_sequence_len);
    defer allocator.free(values);
    @memset(values, 1);
    if (info.shape.len == 1) {
        const shape = [_]i64{@intCast(total_sequence_len)};
        try inputs.append(allocator, try initIntTensorForDType(allocator, info.name, &shape, values, info.dtype));
        return;
    }
    if (info.shape.len == 2) {
        const shape = [_]i64{ resolvePositiveDimOrDefault(info.shape[0], 1), @intCast(total_sequence_len) };
        try inputs.append(allocator, try initIntTensorForDType(allocator, info.name, &shape, values, info.dtype));
        return;
    }
    return error.UnsupportedArtifactInputs;
}

fn appendDecoderPositionIds(
    allocator: std.mem.Allocator,
    input_info: []const backends.TensorInfo,
    position_start: usize,
    seq_len: usize,
    inputs: *std.ArrayListUnmanaged(backends.Tensor),
) !void {
    const info = tensorInfoByName(input_info, "position_ids") orelse return;
    const values = try allocator.alloc(i64, seq_len);
    defer allocator.free(values);
    for (values, 0..) |*value, idx| value.* = @intCast(position_start + idx);
    if (info.shape.len == 1) {
        const shape = [_]i64{@intCast(seq_len)};
        try inputs.append(allocator, try initIntTensorForDType(allocator, info.name, &shape, values, info.dtype));
        return;
    }
    if (info.shape.len == 2) {
        const shape = [_]i64{ resolvePositiveDimOrDefault(info.shape[0], 1), @intCast(seq_len) };
        try inputs.append(allocator, try initIntTensorForDType(allocator, info.name, &shape, values, info.dtype));
        return;
    }
    return error.UnsupportedArtifactInputs;
}

fn appendDecoderUseCacheBranch(
    allocator: std.mem.Allocator,
    input_info: []const backends.TensorInfo,
    enabled: bool,
    inputs: *std.ArrayListUnmanaged(backends.Tensor),
) !void {
    const info = tensorInfoByName(input_info, "use_cache_branch") orelse return;
    const scalar_shape = if (info.shape.len == 0) &[_]i64{} else &[_]i64{1};
    switch (info.dtype) {
        .bool_ => {
            const value = [_]u8{if (enabled) 1 else 0};
            try inputs.append(allocator, try backends.Tensor.initBool(allocator, info.name, scalar_shape, &value));
        },
        .f32 => {
            const value = [_]f32{if (enabled) 1.0 else 0.0};
            try inputs.append(allocator, try backends.Tensor.initFloat32(allocator, info.name, scalar_shape, &value));
        },
        .i64, .i32 => {
            const value = [_]i64{if (enabled) 1 else 0};
            try inputs.append(allocator, try initIntTensorForDType(allocator, info.name, scalar_shape, &value, info.dtype));
        },
        else => return error.UnsupportedArtifactInputs,
    }
}

fn appendDecoderNumLogitsToKeep(
    allocator: std.mem.Allocator,
    input_info: []const backends.TensorInfo,
    inputs: *std.ArrayListUnmanaged(backends.Tensor),
) !void {
    const info = tensorInfoByName(input_info, "num_logits_to_keep") orelse return;
    const scalar_shape = if (info.shape.len == 0) &[_]i64{} else &[_]i64{1};
    const one = [_]i64{1};
    try inputs.append(allocator, try initIntTensorForDType(allocator, info.name, scalar_shape, &one, info.dtype));
}

fn appendDecoderEmptyPastInputs(
    allocator: std.mem.Allocator,
    input_info: []const backends.TensorInfo,
    inputs: *std.ArrayListUnmanaged(backends.Tensor),
) !void {
    for (input_info) |info| {
        if (onnx_kv_cache.pastInputSuffix(info.name) == null) continue;
        if (info.shape.len != 4) return error.UnsupportedArtifactInputs;
        if (info.shape[1] <= 0 or info.shape[3] <= 0) return error.UnsupportedShape;
        const shape = [_]i64{
            resolvePositiveDimOrDefault(info.shape[0], 1),
            info.shape[1],
            0,
            info.shape[3],
        };
        try inputs.append(allocator, try initEmptyTensorForDType(allocator, info.name, &shape, info.dtype));
    }
}

fn appendRetainedPastInputs(
    allocator: std.mem.Allocator,
    input_info: []const backends.TensorInfo,
    cache: *const backends.onnx.RetainedValueCache,
    inputs: *std.ArrayListUnmanaged(backends.onnx.RetainedInput),
) !void {
    for (input_info) |info| {
        if (onnx_kv_cache.pastInputSuffix(info.name) == null) continue;
        const present_name = try onnx_kv_cache.presentNameForPastInput(allocator, info.name);
        defer allocator.free(present_name);
        const cached = cache.find(present_name) orelse return error.MissingPastKeyValue;
        try inputs.append(allocator, .{
            .name = info.name,
            .value = cached.value,
        });
    }
}

fn appendPresentOutputNames(
    allocator: std.mem.Allocator,
    output_info: []const backends.TensorInfo,
    names: *std.ArrayListUnmanaged([]const u8),
) !void {
    for (output_info) |info| {
        if (onnx_kv_cache.presentOutputSuffix(info.name) == null) continue;
        try names.append(allocator, info.name);
    }
}

fn findDecoderLogitsOutputIndex(outputs: []const backends.Tensor) ?usize {
    for (outputs, 0..) |tensor, idx| {
        if (!std.mem.startsWith(u8, tensor.name, "present.")) return idx;
    }
    return null;
}

fn inferOutputLogitWidth(
    output_shape: []const i64,
    logits_len: usize,
    query_seq_len: usize,
    fallback_vocab_size: usize,
) !usize {
    if (output_shape.len > 0 and output_shape[output_shape.len - 1] > 0) {
        const width: usize = @intCast(output_shape[output_shape.len - 1]);
        if (width > 0 and logits_len >= width) return width;
    }
    if (query_seq_len > 0) {
        const width = logits_len / query_seq_len;
        if (width > 0 and logits_len >= width) return width;
    }
    if (fallback_vocab_size > 0 and logits_len >= fallback_vocab_size) return fallback_vocab_size;
    return error.InvalidArtifactOutput;
}

fn tensorToOwnedF32(allocator: std.mem.Allocator, tensor: *const backends.Tensor) ![]f32 {
    return switch (tensor.dtype) {
        .f32 => allocator.dupe(f32, tensor.asFloat32()),
        .f16 => convertF16ToF32(allocator, tensor.data),
        .bf16 => convertBf16ToF32(allocator, tensor.data),
        else => error.UnsupportedTensorType,
    };
}

fn convertF16ToF32(allocator: std.mem.Allocator, data: []const u8) ![]f32 {
    const count = data.len / 2;
    const out = try allocator.alloc(f32, count);
    const aligned: []align(@alignOf(u16)) const u8 = @alignCast(data);
    const src = std.mem.bytesAsSlice(u16, aligned);
    for (src, 0..) |bits, idx| out[idx] = @floatCast(@as(f16, @bitCast(bits)));
    return out;
}

fn convertBf16ToF32(allocator: std.mem.Allocator, data: []const u8) ![]f32 {
    const count = data.len / 2;
    const out = try allocator.alloc(f32, count);
    const aligned: []align(@alignOf(u16)) const u8 = @alignCast(data);
    const src = std.mem.bytesAsSlice(u16, aligned);
    for (src, 0..) |bits, idx| {
        const wide: u32 = @as(u32, bits) << 16;
        out[idx] = @bitCast(wide);
    }
    return out;
}

fn initEmptyTensorForDType(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    dtype: backends.DType,
) !backends.Tensor {
    return .{
        .data = try allocator.alloc(u8, 0),
        .dtype = dtype,
        .shape = try allocator.dupe(i64, shape),
        .name = name,
        .allocator = allocator,
        .owns_data = true,
        .owns_shape = true,
    };
}

fn initIntTensorForDType(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    data: []const i64,
    dtype: backends.DType,
) !backends.Tensor {
    return switch (dtype) {
        .i64 => backends.Tensor.initInt64(allocator, name, shape, data),
        .i32 => initInt32Tensor(allocator, name, shape, data),
        else => error.UnsupportedArtifactInputs,
    };
}

fn initInt32Tensor(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    data: []const i64,
) !backends.Tensor {
    const owned_data = try allocator.alloc(u8, data.len * @sizeOf(i32));
    errdefer allocator.free(owned_data);
    const aligned: []align(@alignOf(i32)) u8 = @alignCast(owned_data);
    const dst = std.mem.bytesAsSlice(i32, aligned);
    for (data, 0..) |value, idx| dst[idx] = @intCast(value);
    return .{
        .data = owned_data,
        .dtype = .i32,
        .shape = try allocator.dupe(i64, shape),
        .name = name,
        .allocator = allocator,
        .owns_data = true,
        .owns_shape = true,
    };
}

fn tensorInfoByName(info_list: []const backends.TensorInfo, name: []const u8) ?backends.TensorInfo {
    for (info_list) |info| {
        if (std.mem.eql(u8, info.name, name)) return info;
    }
    return null;
}

fn hasTensorInfoNamed(info_list: []const backends.TensorInfo, name: []const u8) bool {
    return tensorInfoByName(info_list, name) != null;
}

fn resolvePositiveDimOrDefault(dim: i64, default_value: i64) i64 {
    return if (dim > 0) dim else default_value;
}

fn deinitTensorSlice(allocator: std.mem.Allocator, tensors: []backends.Tensor) void {
    for (tensors) |*tensor| tensor.deinit();
    allocator.free(tensors);
}

fn attentionModeName(mode: cache_mod.AttentionMode) []const u8 {
    return switch (mode) {
        .full_recompute => "full_recompute",
        .paged_prefill => "paged_prefill",
        .paged_decode => "paged_decode",
    };
}

test "decoder past/present ONNX ABI defaults to backend-owned KV" {
    const inputs = [_]backends.TensorInfo{
        .{ .name = "input_ids", .dtype = .i64, .shape = &.{ 1, 1 } },
        .{ .name = "past_key_values.0.key", .dtype = .f32, .shape = &.{ 1, 1, 1, 2 } },
    };
    const outputs = [_]backends.TensorInfo{
        .{ .name = "logits", .dtype = .f32, .shape = &.{ 1, 1, 8 } },
        .{ .name = "present.0.key", .dtype = .f32, .shape = &.{ 1, 1, 2, 2 } },
    };

    try std.testing.expectEqual(
        InputMaterialization.backend_owned_kv,
        detectInputMaterialization(.host_assisted_explicit_kv, &inputs, &outputs),
    );
}

test "ONNX package selectors resolve unique prefill and decode manifests" {
    const package: compiled_artifact.PackageManifest = .{
        .backend = "onnx",
        .model_dir = "/tmp/model",
        .kind = "onnx_graph",
        .artifacts = &.{
            .{
                .manifest_path = "/tmp/prefill.inference.json",
                .artifact_path = "/tmp/prefill.onnx",
                .artifact_role = compiled_artifact.artifact_role_prefill,
                .seq_len = 2,
                .query_seq_len = 2,
                .attention_mode = "paged_prefill",
            },
            .{
                .manifest_path = "/tmp/decode.inference.json",
                .artifact_path = "/tmp/decode.onnx",
                .artifact_role = compiled_artifact.artifact_role_decode,
                .seq_len = 2,
                .query_seq_len = 1,
                .attention_mode = "paged_decode",
            },
        },
    };

    const prefill_entry = (try compiled_artifact.findUniqueMatchingPackageEntry(package, .{
        .artifact_role = compiled_artifact.artifact_role_prefill,
        .seq_len = 2,
        .query_seq_len = 2,
        .attention_mode = "paged_prefill",
    })).?;
    try std.testing.expectEqualStrings(
        "/tmp/prefill.inference.json",
        prefill_entry.manifest_path,
    );
    const decode_entry = (try compiled_artifact.findUniqueMatchingPackageEntry(package, .{
        .artifact_role = compiled_artifact.artifact_role_decode,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
    })).?;
    try std.testing.expectEqualStrings(
        "/tmp/decode.inference.json",
        decode_entry.manifest_path,
    );
}
