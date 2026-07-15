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
const build_options = @import("build_options");
const manifest_mod = @import("../models/manifest.zig");
const c_file = @import("../util/c_file.zig");
const graph_runtime_mod = @import("../graph/runtime.zig");

pub const Session = @import("session.zig").Session;
pub const Tensor = @import("tensor.zig").Tensor;
pub const TensorInfo = @import("tensor.zig").TensorInfo;
pub const DType = @import("tensor.zig").DType;
pub const native = @import("native.zig");
pub const activations = @import("activations.zig");

pub const session_pool = @import("session_pool.zig");
pub const SessionPool = session_pool.SessionPool;

pub const onnx = if (build_options.enable_onnx) @import("onnx.zig") else struct {};
pub const ortgenai = if (build_options.enable_onnx) @import("ortgenai.zig") else struct {};
pub const imported_onnx_session = @import("imported_onnx_session.zig");
pub const metal_kv_storage = if (build_options.enable_metal) @import("metal_kv_storage.zig") else struct {};

const session_factory = @import("../architectures/session_factory.zig");

pub const BackendType = enum {
    native,
    onnx,
    metal,
    cuda,
    pjrt,
    wasm,

    pub fn available(self: BackendType) bool {
        return switch (self) {
            .native => build_options.enable_native,
            .onnx => build_options.enable_onnx,
            .metal => build_options.enable_metal,
            .cuda => build_options.enable_cuda,
            .pjrt => build_options.enable_pjrt,
            .wasm => build_options.enable_wasm,
        };
    }

    pub fn priority(self: BackendType) u8 {
        return switch (self) {
            .onnx => 10,
            .metal => 15,
            .cuda => 25,
            .pjrt => 35,
            .wasm => 50,
            .native => 100,
        };
    }

    pub fn usesGpuHostedSession(self: BackendType) bool {
        return switch (self) {
            .metal, .cuda => true,
            else => false,
        };
    }

    /// Whether SessionManager.loadModel can create a Session directly for this backend.
    pub fn supportsDirectSessionLoad(self: BackendType) bool {
        return switch (self) {
            .native, .onnx, .metal, .cuda, .wasm => true,
            .pjrt => false,
        };
    }
};

const backend_order_capacity = std.meta.fields(BackendType).len;

/// SessionManager selects the best available backend and creates sessions.
pub const SessionManager = struct {
    allocator: std.mem.Allocator,
    preferred_backends: []const BackendType,
    graph_runtime_strategy: ?graph_runtime_mod.Strategy = null,
    /// Maximum overlapping compute backends per model session. Metal uses
    /// this to bound its warm provider pool and backpressure excess work.
    pool_size: usize = 1,
    /// Optional Io runtime threaded into compute backends so parallel GEMM
    /// dispatch goes through the caller's thread pool (linalg.sgemm*Io).
    /// Null means backends use the process-wide futex pool inside lib/linalg.
    io: ?std.Io = null,

    pub fn init(allocator: std.mem.Allocator) SessionManager {
        return .{
            .allocator = allocator,
            .preferred_backends = configuredPreferredBackends(),
        };
    }

    pub fn initWithIo(allocator: std.mem.Allocator, io: std.Io) SessionManager {
        return .{
            .allocator = allocator,
            .preferred_backends = configuredPreferredBackends(),
            .io = io,
        };
    }

    pub fn initWithBackendPriority(
        allocator: std.mem.Allocator,
        backend_priority: ?[]const BackendType,
    ) SessionManager {
        return .{
            .allocator = allocator,
            .preferred_backends = backend_priority orelse configuredPreferredBackends(),
        };
    }

    pub fn setPoolSize(self: *SessionManager, pool_size: usize) void {
        self.pool_size = @max(pool_size, 1);
    }

    pub fn loadModel(self: *SessionManager, model_path: []const u8) !Session {
        return self.loadModelWithImportedOnnxContext(model_path, null);
    }

    pub fn loadModelWithImportedOnnxContext(
        self: *SessionManager,
        model_path: []const u8,
        shared_backend_ctx: ?*imported_onnx_session.SharedBackendContext,
    ) !Session {
        var manifest = manifest_mod.loadFromDir(self.allocator, model_path) catch null;
        defer if (manifest) |*m| m.deinit();
        var effective_buf: [backend_order_capacity]BackendType = undefined;
        const effective_backends = effectiveBackendOrder(self.allocator, &effective_buf, self.preferred_backends, if (manifest) |m| m else null);

        for (effective_backends) |backend| {
            if (!backend.available()) continue;
            if (!backend.supportsDirectSessionLoad()) {
                std.log.err(
                    "backend {s} is available but does not support direct model inference yet",
                    .{@tagName(backend)},
                );
                continue;
            }
            std.log.info("trying backend {s} for {s}", .{ @tagName(backend), model_path });
            const effective_model_path = switch (backend) {
                .onnx, .wasm => if (manifest) |m| m.onnx_path orelse model_path else model_path,
                else => model_path,
            };

            const session = switch (backend) {
                .onnx => if (comptime build_options.enable_onnx) blk: {
                    if (!isOnnxFilePath(effective_model_path)) continue;
                    break :blk onnx.createSession(self.allocator, effective_model_path) catch |err| {
                        std.log.err("onnx runtime session create failed for {s}: {s}", .{ effective_model_path, @errorName(err) });
                        continue;
                    };
                } else continue,
                .metal => if (self.shouldUseImportedOnnxGraphRuntime(effective_model_path))
                    self.createImportedOnnxSession(effective_model_path, .metal, shared_backend_ctx) catch |err| {
                        std.log.err("imported onnx metal session create failed for {s}: {s}", .{ effective_model_path, @errorName(err) });
                        continue;
                    }
                else if (build_options.enable_metal)
                    session_factory.createMetalSession(self.allocator, model_path) catch |err| {
                        std.log.err("Metal session create failed for {s}: {s}", .{ model_path, @errorName(err) });
                        continue;
                    }
                else
                    continue,
                .cuda => if (self.shouldUseImportedOnnxGraphRuntime(effective_model_path))
                    self.createImportedOnnxSession(effective_model_path, .cuda, shared_backend_ctx) catch |err| {
                        std.log.err("imported onnx CUDA session create failed for {s}: {s}", .{ effective_model_path, @errorName(err) });
                        continue;
                    }
                else if (build_options.enable_cuda)
                    session_factory.createCudaSession(self.allocator, model_path) catch |err| {
                        std.log.err("CUDA session create failed for {s}: {s}", .{ model_path, @errorName(err) });
                        continue;
                    }
                else
                    continue,
                .native => if (self.shouldUseImportedOnnxGraphRuntime(effective_model_path))
                    self.createImportedOnnxSession(effective_model_path, .native, shared_backend_ctx) catch |err| {
                        std.log.err("imported onnx native session create failed for {s}: {s}", .{ effective_model_path, @errorName(err) });
                        continue;
                    }
                else
                    session_factory.createNativeSession(self.allocator, model_path) catch |err| {
                        std.log.err("native session create failed for {s}: {s}", .{ model_path, @errorName(err) });
                        continue;
                    },
                .wasm => if (self.shouldUseImportedOnnxGraphRuntime(effective_model_path))
                    self.createImportedOnnxSession(effective_model_path, .wasm, shared_backend_ctx) catch |err| {
                        std.log.err("imported onnx wasm session create failed for {s}: {s}", .{ effective_model_path, @errorName(err) });
                        continue;
                    }
                else
                    continue,
                .pjrt => continue,
            };
            // For sessions produced by session_factory (native/Metal
            // direct loads -- not the imported_onnx path which already
            // received options.io), attach the SessionManager's Io now so
            // matmul work composes with the caller's runtime.  attachIo
            // is a no-op on Sessions whose vtable isn't arch_vtable.
            session_factory.attachMetalProviderPoolSize(session, self.pool_size);
            if (self.io) |io_handle| session_factory.attachIo(session, io_handle);
            // Same lifecycle for graph-runtime strategy: today only the
            // gliner branch consults it (other architectures don't have
            // graph paths wired into runArch), but plumbing it through
            // SessionManager keeps the seam consistent for when they do.
            if (self.graph_runtime_strategy) |strategy| {
                session_factory.attachGraphRuntimeStrategy(session, strategy);
            }
            std.log.info("selected backend {s} for {s}", .{ @tagName(backend), model_path });
            return session;
        }
        return error.NoBackendAvailable;
    }

    fn createImportedOnnxSession(
        self: *SessionManager,
        model_path: []const u8,
        backend: BackendType,
        shared_backend_ctx: ?*imported_onnx_session.SharedBackendContext,
    ) !Session {
        return imported_onnx_session.createSessionWithOptions(self.allocator, model_path, backend, .{
            .graph_runtime_strategy = self.graph_runtime_strategy,
            .shared_backend_ctx = shared_backend_ctx,
            .io = self.io,
        });
    }

    fn shouldUseImportedOnnxGraphRuntime(self: *const SessionManager, model_path: []const u8) bool {
        _ = self;
        return isOnnxFilePath(model_path);
    }

    fn shouldUseExternalOnnxRuntime(self: *const SessionManager, model_path: []const u8) bool {
        _ = self;
        return build_options.enable_onnx and isOnnxFilePath(model_path);
    }

    pub fn bestAvailable(self: *const SessionManager) ?BackendType {
        for (self.preferred_backends) |backend| {
            if (backend.available() and backend.supportsDirectSessionLoad()) return backend;
        }
        return null;
    }
};

fn configuredPreferredBackends() []const BackendType {
    if (build_options.enable_wasm) return &.{.wasm};
    if (preferredBackendOverride()) |backend| {
        return preferredBackendsForOverride(backend);
    }
    return &.{ .metal, .native };
}

/// Validate an explicit backend order before serving. At least one configured
/// backend must be usable by this build; a single-entry list therefore acts as
/// a strict backend requirement without relying on process environment.
pub fn validateBackendPriority(order: []const BackendType) !void {
    if (order.len == 0) return error.InvalidBackendPriority;
    for (order) |backend| {
        if (backend.available() and backend.supportsDirectSessionLoad()) return;
    }
    return error.ConfiguredBackendUnavailable;
}

fn preferredBackendsForOverride(backend: BackendType) []const BackendType {
    return switch (backend) {
        .onnx => &.{ .onnx, .metal, .native },
        .metal => if (build_options.enable_metal) &.{ .metal, .onnx, .native } else &.{ .native, .onnx },
        .cuda => if (build_options.enable_cuda) &.{ .cuda, .onnx, .metal, .native } else &.{ .native, .onnx, .metal },
        .pjrt => if (build_options.enable_pjrt) &.{ .pjrt, .onnx, .metal, .native } else &.{ .onnx, .metal, .native },
        .native => &.{ .native, .onnx, .metal },
        .wasm => &.{ .onnx, .metal, .native },
    };
}

fn defaultImportedOnnxBackend() BackendType {
    return if (build_options.enable_wasm) .wasm else .native;
}

fn isOnnxFilePath(path: []const u8) bool {
    return std.mem.endsWith(u8, path, ".onnx");
}

test "onnx artifact routes graph execution for direct compute backends" {
    try std.testing.expect(isOnnxFilePath("model.onnx"));
    try std.testing.expect(!isOnnxFilePath("model.gguf"));
}

test "onnx backend availability follows linked onnx runtime" {
    try std.testing.expectEqual(build_options.enable_onnx, BackendType.onnx.available());
    try std.testing.expect(BackendType.onnx.supportsDirectSessionLoad());
    if (build_options.enable_wasm) {
        try std.testing.expectEqual(BackendType.wasm, configuredPreferredBackends()[0]);
        try std.testing.expectEqual(BackendType.wasm, defaultImportedOnnxBackend());
        try std.testing.expect(BackendType.wasm.supportsDirectSessionLoad());
    } else {
        try std.testing.expectEqual(BackendType.native, defaultImportedOnnxBackend());
    }
}

test "preferred backend override keeps fallback backends" {
    try std.testing.expectEqualSlices(BackendType, &.{ .onnx, .metal, .native }, preferredBackendsForOverride(.onnx));
    try std.testing.expectEqualSlices(BackendType, &.{ .native, .onnx, .metal }, preferredBackendsForOverride(.native));
}

test "configured backend priority validates available fallbacks" {
    try std.testing.expectError(error.InvalidBackendPriority, validateBackendPriority(&.{}));
    if (build_options.enable_cuda)
        try validateBackendPriority(&.{.cuda})
    else
        try std.testing.expectError(error.ConfiguredBackendUnavailable, validateBackendPriority(&.{.cuda}));
    try std.testing.expectError(error.ConfiguredBackendUnavailable, validateBackendPriority(&.{.pjrt}));
    if (build_options.enable_native) try validateBackendPriority(&.{ .pjrt, .native });
}

test "session manager normalizes provider pool size" {
    var manager = SessionManager.init(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), manager.pool_size);
    manager.setPoolSize(0);
    try std.testing.expectEqual(@as(usize, 1), manager.pool_size);
    manager.setPoolSize(4);
    try std.testing.expectEqual(@as(usize, 4), manager.pool_size);
}

test "explicit graph runtime is independent from onnx runtime backend availability" {
    var manager = SessionManager.init(std.testing.allocator);
    try std.testing.expect(manager.shouldUseImportedOnnxGraphRuntime("model.onnx"));
    try std.testing.expectEqual(build_options.enable_onnx, manager.shouldUseExternalOnnxRuntime("model.onnx"));
    manager.graph_runtime_strategy = .partitioned;
    try std.testing.expect(manager.shouldUseImportedOnnxGraphRuntime("model.onnx"));
    try std.testing.expectEqual(build_options.enable_onnx, manager.shouldUseExternalOnnxRuntime("model.onnx"));
    try std.testing.expect(!manager.shouldUseImportedOnnxGraphRuntime("model.gguf"));
    try std.testing.expect(!manager.shouldUseExternalOnnxRuntime("model.gguf"));
}

test "auto backend order keeps external onnx runtime opt-in" {
    if (!build_options.enable_wasm) {
        try std.testing.expectEqualSlices(BackendType, &.{ .metal, .native }, configuredPreferredBackends());
    }
    try std.testing.expectEqualSlices(BackendType, &.{ .onnx, .metal, .native }, preferredBackendsForOverride(.onnx));
}

fn preferredBackendOverride() ?BackendType {
    if (build_options.enable_wasm or !build_options.link_libc) return null;
    const value = std.c.getenv("ANTFLY_INFERENCE_PREFERRED_BACKEND") orelse
        std.c.getenv("TERMITE_PREFERRED_BACKEND") orelse
        return null;
    return parseBackendOverride(std.mem.span(value));
}

fn parseBackendOverride(slice: []const u8) ?BackendType {
    if (std.ascii.eqlIgnoreCase(slice, "auto")) return null;
    if (std.ascii.eqlIgnoreCase(slice, "onnx")) return .onnx;
    if (std.ascii.eqlIgnoreCase(slice, "metal")) return .metal;
    if (std.ascii.eqlIgnoreCase(slice, "pjrt")) return .pjrt;
    if (std.ascii.eqlIgnoreCase(slice, "cuda")) return .cuda;
    if (std.ascii.eqlIgnoreCase(slice, "native")) return .native;
    return null;
}

fn gpuEagerDenseMaxBytes() u64 {
    if (build_options.enable_wasm or !build_options.link_libc) return 1024 * 1024 * 1024;
    const value = std.c.getenv("TERMITE_GPU_EAGER_DENSE_MAX_MB") orelse return 1024 * 1024 * 1024;
    const slice = std.mem.span(value);
    const mb = std.fmt.parseInt(u64, slice, 10) catch return 1024 * 1024 * 1024;
    return mb * 1024 * 1024;
}

fn shouldPreferBlasBeforeGpuForBytes(total_bytes: u64, max_eager_dense_bytes: u64) bool {
    return total_bytes == 0 or total_bytes > max_eager_dense_bytes;
}

fn shouldPreferBlasBeforeGpu(allocator: std.mem.Allocator, manifest: ?manifest_mod.ModelManifest) bool {
    if (build_options.enable_wasm) return false;
    const man = manifest orelse return false;
    if (man.model_type == .generator) return false;
    const gguf_path = man.gguf_path orelse return false;
    const total_bytes = c_file.fileSize(allocator, gguf_path) catch return true;
    return shouldPreferBlasBeforeGpuForBytes(total_bytes, gpuEagerDenseMaxBytes());
}

fn shouldPreferNativeTextEncoder(man: manifest_mod.ModelManifest) bool {
    if (man.model_type != .embedder) return false;
    if (man.safetensors_path == null and man.safetensors_index_path == null) return false;
    return man.onnx_path != null and
        man.visual_model_path == null and
        man.audio_model_path == null and
        man.text_projection_path == null and
        man.visual_projection_path == null and
        man.audio_projection_path == null;
}

fn effectiveBackendOrder(
    allocator: std.mem.Allocator,
    scratch: *[backend_order_capacity]BackendType,
    preferred: []const BackendType,
    manifest: ?manifest_mod.ModelManifest,
) []const BackendType {
    const prefer_blas_before_gpu = shouldPreferBlasBeforeGpu(allocator, manifest);
    if (manifest) |man| {
        if (shouldPreferNativeTextEncoder(man)) {
            return reorderNativeAheadOfOnnx(scratch, preferred, true);
        }
        if (man.native_arch_hint == .layoutlmv3 and man.safetensors_path != null) {
            return reorderNativeAheadOfOnnx(scratch, preferred, prefer_blas_before_gpu);
        }
    }
    return effectiveBackendOrderForPreference(scratch, preferred, prefer_blas_before_gpu);
}

fn effectiveBackendOrderForPreference(
    scratch: *[backend_order_capacity]BackendType,
    preferred: []const BackendType,
    prefer_blas_before_gpu: bool,
) []const BackendType {
    if (!prefer_blas_before_gpu) return preferred;

    var has_gpu = false;
    var has_blas = false;
    for (preferred) |backend| {
        has_gpu = has_gpu or backend.usesGpuHostedSession();
        has_blas = has_blas or backend == .native;
    }
    if (!has_gpu or !has_blas) return preferred;

    var idx: usize = 0;

    for (preferred) |backend| {
        if (backend.usesGpuHostedSession() or backend == .native) continue;
        scratch[idx] = backend;
        idx += 1;
    }
    for (preferred) |backend| {
        if (!backend.usesGpuHostedSession()) continue;
        scratch[idx] = backend;
        idx += 1;
    }
    scratch[idx] = .native;
    idx += 1;
    return scratch[0..idx];
}

fn reorderNativeAheadOfOnnx(
    scratch: *[backend_order_capacity]BackendType,
    preferred: []const BackendType,
    prefer_blas_before_gpu: bool,
) []const BackendType {
    var idx: usize = 0;
    if (prefer_blas_before_gpu) {
        for (preferred) |backend| {
            if (backend == .native) {
                scratch[idx] = backend;
                idx += 1;
            }
        }
        for (preferred) |backend| {
            if (backend.usesGpuHostedSession()) {
                scratch[idx] = backend;
                idx += 1;
            }
        }
    } else {
        for (preferred) |backend| {
            if (backend.usesGpuHostedSession() or backend == .native) {
                scratch[idx] = backend;
                idx += 1;
            }
        }
    }
    for (preferred) |backend| {
        if (backend.usesGpuHostedSession() or backend == .native) continue;
        scratch[idx] = backend;
        idx += 1;
    }
    return scratch[0..idx];
}
test {
    _ = @import("tensor.zig");
    _ = @import("session.zig");
    _ = session_pool;
    _ = native;
    _ = activations;
    _ = session_factory;
    _ = imported_onnx_session;
}

test "shouldPreferBlasBeforeGpuForBytes prefers native only above eager dense threshold" {
    try std.testing.expect(shouldPreferBlasBeforeGpuForBytes(2 * 1024 * 1024 * 1024, 1024 * 1024 * 1024));
    try std.testing.expect(!shouldPreferBlasBeforeGpuForBytes(256 * 1024 * 1024, 1024 * 1024 * 1024));
}

test "effective backend order keeps gpu before native for large gguf generators" {
    const preferred = [_]BackendType{ .onnx, .metal, .native };
    var scratch: [backend_order_capacity]BackendType = undefined;
    const manifest: manifest_mod.ModelManifest = .{
        .allocator = std.testing.allocator,
        .model_type = .generator,
        .gguf_path = "missing-large-generator.gguf",
    };
    const effective = effectiveBackendOrder(std.testing.allocator, &scratch, &preferred, manifest);
    try std.testing.expectEqualSlices(BackendType, &preferred, effective);
}

test "effective backend order handles four backend preference lists" {
    const preferred = [_]BackendType{ .cuda, .onnx, .metal, .native };
    var scratch: [backend_order_capacity]BackendType = undefined;
    const effective = effectiveBackendOrderForPreference(&scratch, &preferred, true);
    try std.testing.expectEqualSlices(BackendType, &.{ .onnx, .cuda, .metal, .native }, effective);
}

test "effective backend order preserves gpu preference for small gguf generators" {
    const preferred = [_]BackendType{ .onnx, .metal, .native };
    var scratch: [backend_order_capacity]BackendType = undefined;
    const effective = effectiveBackendOrderForPreference(&scratch, &preferred, false);
    try std.testing.expectEqualSlices(BackendType, &preferred, effective);
}

test "effective backend order preserves order for non-gguf models" {
    const preferred = [_]BackendType{ .onnx, .metal, .native };
    var scratch: [backend_order_capacity]BackendType = undefined;
    const manifest: manifest_mod.ModelManifest = .{
        .allocator = std.testing.allocator,
        .model_type = .generator,
        .safetensors_path = "dummy",
    };
    const effective = effectiveBackendOrder(std.testing.allocator, &scratch, &preferred, manifest);
    try std.testing.expectEqualSlices(BackendType, &preferred, effective);
}

test "effective backend order prefers native layoutlmv3 before onnx" {
    const preferred = [_]BackendType{ .onnx, .metal, .native };
    var scratch: [backend_order_capacity]BackendType = undefined;
    const manifest: manifest_mod.ModelManifest = .{
        .allocator = std.testing.allocator,
        .native_arch_hint = .layoutlmv3,
        .safetensors_path = "dummy",
    };
    const effective = effectiveBackendOrder(std.testing.allocator, &scratch, &preferred, manifest);
    try std.testing.expectEqualSlices(BackendType, &.{ .metal, .native, .onnx }, effective);
}
