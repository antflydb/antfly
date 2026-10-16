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

const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const manifest_mod = @import("../models/manifest.zig");
const c_file = @import("../util/c_file.zig");
const kernel_jit_mod = @import("../graph/kernel_jit.zig");
const backend_contracts = @import("../graph/backend_contracts.zig");
const graph_runtime_mod = @import("../graph/runtime.zig");
const execution_control = @import("../execution_control.zig");
const Interruption = execution_control.Interruption;

pub const Session = @import("session.zig").Session;
pub const Tensor = @import("tensor.zig").Tensor;
pub const TensorInfo = @import("tensor.zig").TensorInfo;
pub const DType = @import("tensor.zig").DType;
pub const native = @import("native.zig");
pub const activations = @import("activations.zig");

pub const session_pool = @import("session_pool.zig");
pub const SessionPool = session_pool.SessionPool;

pub const imported_onnx_session = @import("imported_onnx_session.zig");
pub const metal_kv_storage = if (build_options.enable_metal) @import("metal_kv_storage.zig") else struct {};

const session_factory = @import("../architectures/session_factory.zig");

pub const BackendType = enum {
    native,
    metal,
    cuda,
    pjrt,
    wasm,

    pub fn parse(value: []const u8) ?BackendType {
        if (std.mem.eql(u8, value, "native")) return .native;
        if (std.mem.eql(u8, value, "metal")) return .metal;
        if (std.mem.eql(u8, value, "cuda")) return .cuda;
        if (std.mem.eql(u8, value, "xla") or std.mem.eql(u8, value, "pjrt")) return .pjrt;
        if (std.mem.eql(u8, value, "wasm") or std.mem.eql(u8, value, "webgpu")) return .wasm;
        return null;
    }

    pub fn parseOptional(value: ?[]const u8) !?BackendType {
        const raw = value orelse return null;
        if (std.mem.eql(u8, raw, "auto")) return null;
        return parse(raw) orelse error.InvalidArguments;
    }

    pub fn available(self: BackendType) bool {
        return switch (self) {
            .native => build_options.enable_native,
            .metal => build_options.enable_metal,
            .cuda => build_options.enable_cuda,
            .pjrt => build_options.enable_pjrt,
            .wasm => build_options.enable_wasm,
        };
    }

    pub fn priority(self: BackendType) u8 {
        return switch (self) {
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

    pub fn supportsKernelJitSession(self: BackendType) bool {
        return switch (self) {
            .metal, .cuda => true,
            else => false,
        };
    }

    pub fn supportsA4bSession(self: BackendType) bool {
        return self == .metal or self == .cuda;
    }

    /// Strongest cancellation mechanism available once execution has crossed
    /// into the backend. GPU driver and PJRT calls have no portable per-call
    /// abort primitive. A supervised worker can enforce hard deadlines; embedded
    /// runtimes explicitly accept waiting for these calls to return.
    pub fn executionInterruption(self: BackendType) Interruption {
        return switch (self) {
            .native, .wasm => .cooperative,
            .metal, .cuda, .pjrt => .uninterruptible,
        };
    }

    /// Model/session constructors may enter a device driver or native runtime
    /// which cannot observe cooperative cancellation.
    pub fn loadInterruption(self: BackendType) Interruption {
        return switch (self) {
            .native, .wasm => .cooperative,
            .metal, .cuda, .pjrt => .uninterruptible,
        };
    }

    /// Whether SessionManager.loadModel can create a Session directly for this backend.
    pub fn supportsDirectSessionLoad(self: BackendType) bool {
        return switch (self) {
            .native, .metal, .cuda, .wasm => true,
            .pjrt => false,
        };
    }
};

/// The backend that determines a session's resource domain and interruption
/// policy.
pub const BackendRuntime = struct {
    backend: BackendType,

    pub fn hasUninterruptibleCalls(self: BackendRuntime) bool {
        return self.loadInterruption() == .uninterruptible or
            self.executionInterruption() == .uninterruptible;
    }

    /// Placement must cover backends selected lazily after server startup.
    pub fn availableHasUninterruptibleCalls() bool {
        inline for (std.meta.tags(BackendType)) |backend| {
            if (backend.available() and (BackendRuntime{ .backend = backend }).hasUninterruptibleCalls()) return true;
        }
        return false;
    }

    pub fn usesGpuHostedSession(self: BackendRuntime) bool {
        return switch (self.backend) {
            .metal, .cuda => true,
            else => false,
        };
    }

    pub fn executionInterruption(self: BackendRuntime) Interruption {
        return self.backend.executionInterruption();
    }

    pub fn loadInterruption(self: BackendRuntime) Interruption {
        return self.backend.loadInterruption();
    }
};

test "backend runtime process placement covers loading and execution" {
    try std.testing.expect(!(BackendRuntime{ .backend = .native }).hasUninterruptibleCalls());
    try std.testing.expect(!(BackendRuntime{ .backend = .wasm }).hasUninterruptibleCalls());
    inline for (.{ BackendType.metal, BackendType.cuda, BackendType.pjrt }) |backend|
        try std.testing.expect((BackendRuntime{ .backend = backend }).hasUninterruptibleCalls());
}

test "backend interruption policy isolates unabortable driver calls" {
    try std.testing.expectEqual(Interruption.cooperative, BackendType.native.executionInterruption());
    try std.testing.expectEqual(Interruption.uninterruptible, BackendType.metal.executionInterruption());
    try std.testing.expectEqual(Interruption.uninterruptible, BackendType.cuda.executionInterruption());
    try std.testing.expectEqual(Interruption.uninterruptible, BackendType.pjrt.executionInterruption());
}

test "backend load policy isolates native runtime construction" {
    try std.testing.expectEqual(Interruption.cooperative, BackendType.native.loadInterruption());
    try std.testing.expectEqual(Interruption.uninterruptible, BackendType.metal.loadInterruption());
    try std.testing.expectEqual(Interruption.uninterruptible, BackendType.cuda.loadInterruption());
}

test "embedded session policy fails before selecting an uninterruptible backend" {
    if (!BackendType.metal.available()) return error.SkipZigTest;
    var manager = SessionManager.init(std.testing.allocator);
    manager.required_backend = .metal;
    manager.required_backend_invalid = false;
    manager.process_termination_available = false;
    try std.testing.expectError(
        error.ProcessIsolationRequired,
        manager.validateRequiredBackendPolicy(),
    );
}

const backend_order_capacity = std.meta.fieldNames(BackendType).len;

const RequiredBackendConfig = struct {
    backend: ?BackendType = null,
    invalid: bool = false,
};

/// SessionManager selects the best available backend and creates sessions.
pub const SessionManager = struct {
    allocator: std.mem.Allocator,
    preferred_backends: []const BackendType,
    required_backend: ?BackendType = null,
    required_backend_invalid: bool = false,
    graph_runtime_strategy: ?graph_runtime_mod.Strategy = null,
    kernel_jit: kernel_jit_mod.Config = .{},
    kernel_jit_load_context: kernel_jit_mod.LoadContext = .dynamic,
    /// Load-time A4B policy. It is copied into the created session and is
    /// never consulted as mutable process-global state during inference.
    a4b_inference_request: ?backend_contracts.A4bInferenceRequest = null,
    gliner_cuda_precision: ?session_factory.GlinerCudaPrecision = null,
    /// Per-manager qualification override, absent from production builds.
    test_allow_unqualified_gliner_cuda_precision: if (builtin.is_test) bool else void = if (builtin.is_test) false else {},
    /// Optional Io runtime threaded into compute backends so parallel GEMM
    /// dispatch goes through the caller's thread pool (linalg.sgemm*Io).
    /// Null means backends use the process-wide futex pool inside lib/linalg.
    io: ?std.Io = null,
    /// True for disposable CLI tools and supervised inference workers. An
    /// embedded runtime keeps this false and explicitly accepts in-process
    /// native calls through its local native_call_policy.
    process_termination_available: bool = true,
    native_call_policy: execution_control.NativeCallPolicy = .require_supervision,

    /// Backends whose execution can only be stopped by killing the process run
    /// in a supervised worker, or in-process when the host accepted that they
    /// cannot be interrupted (see `native_call_policy`).
    pub fn uninterruptibleBackendsAllowed(self: *const SessionManager) bool {
        return self.process_termination_available or self.native_call_policy == .in_process;
    }

    pub fn init(allocator: std.mem.Allocator) SessionManager {
        const required = requiredBackendFromEnv();
        return .{
            .allocator = allocator,
            .preferred_backends = configuredPreferredBackends(),
            .required_backend = required.backend,
            .required_backend_invalid = required.invalid,
        };
    }

    pub fn initWithIo(allocator: std.mem.Allocator, io: std.Io) SessionManager {
        const required = requiredBackendFromEnv();
        return .{
            .allocator = allocator,
            .preferred_backends = configuredPreferredBackends(),
            .required_backend = required.backend,
            .required_backend_invalid = required.invalid,
            .io = io,
        };
    }

    pub fn withPreferredBackends(
        self: SessionManager,
        allocator: std.mem.Allocator,
        preferred_backends: []const BackendType,
    ) SessionManager {
        var copy = self;
        copy.allocator = allocator;
        copy.preferred_backends = preferred_backends;
        return copy;
    }

    /// Validate the process-level required backend before a server publishes
    /// readiness or an artifact path begins execution. Required backends must
    /// be compiled into this binary and support the direct session-loading
    /// contract used by the inference runtime. PJRT is intentionally rejected
    /// until the runtime has a consistent compiled-only loading path.
    pub fn validateRequiredBackendPolicy(self: *const SessionManager) !void {
        try validateBackendEnvironment();
        if (self.required_backend_invalid) return error.InvalidRequiredBackend;
        const backend = self.required_backend orelse return;
        if (!backend.available() or !backend.supportsDirectSessionLoad())
            return error.RequiredBackendUnavailable;
        const backend_runtime = try self.resolveBackendRuntime(backend);
        if (!self.uninterruptibleBackendsAllowed() and backend_runtime.executionInterruption() == .uninterruptible)
            return error.ProcessIsolationRequired;
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
        const fallback_backends = effectiveBackendOrder(
            self.allocator,
            &effective_buf,
            self.preferred_backends,
            if (manifest) |m| m else null,
        );
        var required_buf: [1]BackendType = undefined;
        const effective_backends = try self.requiredBackendCandidates(
            fallback_backends,
            &required_buf,
        );
        const fail_closed_cuda_a4b = self.a4b_inference_request == null and
            backendOrderSelectsCudaBeforeCpu(effective_backends) and
            if (manifest) |m|
                ((session_factory.resolveCudaA4bInferenceConfigForModelListing(
                    self.allocator,
                    model_path,
                    m,
                    null,
                ) catch |err| blk: {
                    std.log.warn(
                        "CUDA A4B auto-policy inspection failed for {s}: {s}",
                        .{ model_path, @errorName(err) },
                    );
                    break :blk null;
                }) != null)
            else
                false;

        // Every backend below logs its failure and moves on, so without this the caller
        // only ever sees a blanket NoBackendAvailable. The actionable cause -- a GGUF
        // whose tensors could not be resolved fails with MissingRequiredWeights -- was
        // reaching the log but never the API response.
        var first_err: ?anyerror = null;

        if (self.gliner_cuda_precision != null and self.a4b_inference_request != null) return error.IncompatibleCudaLoadPolicies;
        for (effective_backends) |backend| {
            if (self.gliner_cuda_precision != null and backend != .cuda) continue;
            if (fail_closed_cuda_a4b and !backend.supportsA4bSession()) {
                std.log.err(
                    "qualified CUDA A4B model {s} rejected CPU fallback after GPU admission failure",
                    .{model_path},
                );
                return first_err orelse error.A4bCudaAutoFallbackForbidden;
            }
            if (!backendAcceptsA4bRequest(backend, self.a4b_inference_request)) {
                first_err = first_err orelse error.A4bRequiresGpu;
                continue;
            }
            // The imported-ONNX graph runtime has no A4B residency contract.
            // Reject the artifact at the route boundary instead of silently
            // dropping the caller's memory envelope. Metal uses model_path
            // directly, so this check does not depend on manifest fallback.
            if (self.a4b_inference_request != null and
                backend == .metal and
                self.shouldUseImportedOnnxGraphRuntime(model_path))
            {
                return error.A4bUnsupportedArtifact;
            }
            if (!backend.available()) continue;
            if (!backend.supportsDirectSessionLoad()) {
                std.log.err(
                    "backend {s} is available but does not support direct model inference yet",
                    .{@tagName(backend)},
                );
                continue;
            }
            const backend_runtime = self.resolveBackendRuntime(backend) catch |err| {
                std.log.err(
                    "backend {s} runtime resolution failed: {s}",
                    .{ @tagName(backend), @errorName(err) },
                );
                first_err = first_err orelse err;
                continue;
            };
            if (!self.uninterruptibleBackendsAllowed() and backend_runtime.executionInterruption() == .uninterruptible) {
                first_err = first_err orelse error.ProcessIsolationRequired;
                continue;
            }
            const effective_model_path = switch (backend) {
                .wasm => if (manifest) |m| m.onnx_path orelse model_path else model_path,
                else => model_path,
            };
            if (self.kernel_jit.qualified_profile_path != null and
                !backendAcceptsQualifiedProfile(backend, effective_model_path)) continue;
            const jit_capable_session = backend.supportsKernelJitSession() and
                !isOnnxFilePath(effective_model_path);
            if (self.kernel_jit.mode.failClosed() and !jit_capable_session) continue;
            std.log.info("trying backend {s} for {s}", .{ @tagName(backend), model_path });

            const session = switch (backend) {
                .metal => if (self.shouldUseImportedOnnxGraphRuntime(effective_model_path))
                    self.createImportedOnnxSession(effective_model_path, .metal, shared_backend_ctx) catch |err| {
                        std.log.err("imported onnx metal session create failed for {s}: {s}", .{ effective_model_path, @errorName(err) });
                        first_err = first_err orelse err;
                        continue;
                    }
                else if (build_options.enable_metal)
                    session_factory.createMetalSessionWithKernelJitAndLoadContextAndA4bRequest(
                        self.allocator,
                        model_path,
                        self.kernel_jit,
                        self.kernel_jit_load_context,
                        self.a4b_inference_request,
                    ) catch |err| {
                        std.log.err("Metal session create failed for {s}: {s}", .{ model_path, @errorName(err) });
                        if (self.kernel_jit.qualified_profile_path != null or self.kernel_jit.profile_capture_only) return err;
                        if (kernel_jit_mod.isRequiredFailure(self.kernel_jit.mode, err)) return err;
                        first_err = first_err orelse err;
                        continue;
                    }
                else
                    continue,
                .cuda => if (self.shouldUseImportedOnnxGraphRuntime(effective_model_path))
                    self.createImportedOnnxSession(effective_model_path, .cuda, shared_backend_ctx) catch |err| {
                        std.log.err("imported onnx CUDA session create failed for {s}: {s}", .{ effective_model_path, @errorName(err) });
                        first_err = first_err orelse err;
                        continue;
                    }
                else if (build_options.enable_cuda)
                    (if (self.gliner_cuda_precision) |precision|
                        (if (if (builtin.is_test) self.test_allow_unqualified_gliner_cuda_precision else false)
                            session_factory.createCudaSessionWithGlinerPrecisionForTest(self.allocator, model_path, self.kernel_jit, self.kernel_jit_load_context, precision)
                        else
                            session_factory.createCudaSessionWithGlinerPrecision(self.allocator, model_path, self.kernel_jit, self.kernel_jit_load_context, precision))
                    else
                        session_factory.createCudaSessionWithKernelJitAndLoadContextAndA4bRequest(
                            self.allocator,
                            model_path,
                            self.kernel_jit,
                            self.kernel_jit_load_context,
                            self.a4b_inference_request,
                        )) catch |err| {
                        std.log.err("CUDA session create failed for {s}: {s}", .{ model_path, @errorName(err) });
                        if (kernel_jit_mod.isRequiredFailure(self.kernel_jit.mode, err)) return err;
                        first_err = first_err orelse err;
                        continue;
                    }
                else
                    continue,
                .native => if (self.shouldUseImportedOnnxGraphRuntime(effective_model_path))
                    self.createImportedOnnxSession(effective_model_path, .native, shared_backend_ctx) catch |err| {
                        std.log.err("imported onnx native session create failed for {s}: {s}", .{ effective_model_path, @errorName(err) });
                        first_err = first_err orelse err;
                        continue;
                    }
                else
                    session_factory.createNativeSession(self.allocator, model_path) catch |err| {
                        std.log.err("native session create failed for {s}: {s}", .{ model_path, @errorName(err) });
                        first_err = first_err orelse err;
                        continue;
                    },
                .wasm => if (self.shouldUseImportedOnnxGraphRuntime(effective_model_path))
                    self.createImportedOnnxSession(effective_model_path, .wasm, shared_backend_ctx) catch |err| {
                        std.log.err("imported onnx wasm session create failed for {s}: {s}", .{ effective_model_path, @errorName(err) });
                        first_err = first_err orelse err;
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
            if (self.io) |io_handle| session_factory.attachIo(session, io_handle);
            // Same lifecycle for graph-runtime strategy: today only the
            // gliner branch consults it (other architectures don't have
            // graph paths wired into runArch), but plumbing it through
            // SessionManager keeps the seam consistent for when they do.
            if (self.graph_runtime_strategy) |strategy| {
                session_factory.attachGraphRuntimeStrategy(session, strategy);
            }
            if (self.kernel_jit.mode.compiles() and !jit_capable_session) {
                std.log.warn(
                    "kernel_jit_skipped mode={s} selected_backend={s} scoped_routes=0 reason=backend_not_jit_capable",
                    .{ @tagName(self.kernel_jit.mode), @tagName(backend) },
                );
            }
            std.log.info("selected backend {s} for {s}", .{ @tagName(backend), model_path });
            return session;
        }
        if (self.required_backend != null) return backendLoadFailure(self.required_backend, first_err);
        if (self.kernel_jit.qualified_profile_path != null or self.kernel_jit.profile_capture_only) return error.KernelJitProfileRequiresMetalBackend;
        if (self.kernel_jit.mode.failClosed()) return error.KernelJitRequiredBackendUnavailable;
        // NoBackendAvailable only when no backend produced a real error.
        return backendLoadFailure(null, first_err);
    }

    /// Apply the process-level fail-closed backend policy before callers make
    /// backend-specific artifact, compatibility, or resource-admission choices.
    pub fn requiredBackendCandidates(
        self: *const SessionManager,
        fallback_backends: []const BackendType,
        required_buf: *[1]BackendType,
    ) ![]const BackendType {
        if (self.required_backend_invalid) return error.InvalidRequiredBackend;
        if (self.required_backend) |backend| {
            if (!backend.supportsDirectSessionLoad())
                return error.RequiredBackendUnavailable;
        }
        return enforceRequiredBackend(self.required_backend, fallback_backends, required_buf);
    }

    /// Whether a backend-specific fast path may bypass normal session loading.
    /// Invalid or unsupported required policies still fail closed, while a
    /// different valid required backend makes the fast path ineligible.
    pub fn allowsDirectBackend(self: *const SessionManager, backend: BackendType) !bool {
        if (self.required_backend_invalid) return error.InvalidRequiredBackend;
        if (self.required_backend) |required| {
            if (!required.supportsDirectSessionLoad()) return error.RequiredBackendUnavailable;
        }
        return self.allowsBackend(backend);
    }

    /// Whether a backend-specific execution path is compatible with the
    /// process-level required backend, including compiled artifact runtimes.
    pub fn allowsBackend(self: *const SessionManager, backend: BackendType) !bool {
        if (self.required_backend_invalid) return error.InvalidRequiredBackend;
        const required = self.required_backend orelse return true;
        return required == backend;
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

    pub fn bestAvailable(self: *const SessionManager) ?BackendType {
        if (self.required_backend_invalid) return null;
        if (self.required_backend) |backend| {
            return if (backend.available() and backend.supportsDirectSessionLoad()) backend else null;
        }
        if (self.kernel_jit.mode.failClosed()) return self.bestKernelJitBackend();
        for (self.preferred_backends) |backend| {
            if (backend.available() and backend.supportsDirectSessionLoad()) return backend;
        }
        return null;
    }

    pub fn bestKernelJitBackend(self: *const SessionManager) ?BackendType {
        if (self.required_backend_invalid) return null;
        if (self.required_backend) |backend| {
            return if (backend.supportsKernelJitSession() and backend.available() and
                backend.supportsDirectSessionLoad()) backend else null;
        }
        for (self.preferred_backends) |backend| {
            if (backend.supportsKernelJitSession() and backend.available() and
                backend.supportsDirectSessionLoad()) return backend;
        }
        return null;
    }

    pub fn resolveBackendRuntime(
        self: *const SessionManager,
        backend: BackendType,
    ) !BackendRuntime {
        _ = self;
        return .{ .backend = backend };
    }
};

fn configuredPreferredBackends() []const BackendType {
    if (build_options.enable_wasm) return &.{.wasm};
    if (preferredBackendOverride()) |backend| {
        return preferredBackendsForOverride(backend);
    }
    return &.{ .metal, .native };
}

fn preferredBackendsForOverride(backend: BackendType) []const BackendType {
    return switch (backend) {
        .metal => if (build_options.enable_metal) &.{ .metal, .native } else &.{.native},
        .cuda => if (build_options.enable_cuda) &.{ .cuda, .metal, .native } else &.{ .native, .metal },
        .pjrt => if (build_options.enable_pjrt) &.{ .pjrt, .metal, .native } else &.{ .metal, .native },
        .native => &.{ .native, .metal },
        .wasm => &.{ .metal, .native },
    };
}

fn defaultImportedOnnxBackend() BackendType {
    return if (build_options.enable_wasm) .wasm else .native;
}

fn isOnnxFilePath(path: []const u8) bool {
    return std.mem.endsWith(u8, path, ".onnx");
}

fn backendAcceptsQualifiedProfile(backend: BackendType, model_path: []const u8) bool {
    return backend == .metal and !isOnnxFilePath(model_path);
}

fn backendAcceptsA4bRequest(
    backend: BackendType,
    request: ?backend_contracts.A4bInferenceRequest,
) bool {
    return request == null or backend == .metal or backend == .cuda;
}

pub fn backendOrderSelectsCudaBeforeCpu(order: []const BackendType) bool {
    var saw_cuda = false;
    for (order) |backend| {
        if (backend == .cuda) saw_cuda = true;
        if (!backend.supportsA4bSession()) return saw_cuda;
    }
    return false;
}

test "onnx artifact routes graph execution for direct compute backends" {
    try std.testing.expect(isOnnxFilePath("model.onnx"));
    try std.testing.expect(!isOnnxFilePath("model.gguf"));
}

test "qualified workload profiles select direct Metal sessions only" {
    try std.testing.expect(backendAcceptsQualifiedProfile(.metal, "model.gguf"));
    try std.testing.expect(!backendAcceptsQualifiedProfile(.metal, "model.onnx"));
    try std.testing.expect(!backendAcceptsQualifiedProfile(.cuda, "model.gguf"));
}

test "explicit A4B requests select GPU backends without generic fallback" {
    try std.testing.expect(backendAcceptsA4bRequest(.metal, .{}));
    try std.testing.expect(backendAcceptsA4bRequest(.cuda, .{}));
    try std.testing.expect(!backendAcceptsA4bRequest(.native, .{}));
    try std.testing.expect(backendAcceptsA4bRequest(.native, null));
}

test "CUDA-first automatic backend order is fail-closed before CPU" {
    try std.testing.expect(backendOrderSelectsCudaBeforeCpu(&.{ .cuda, .native }));
    try std.testing.expect(backendOrderSelectsCudaBeforeCpu(&.{ .cuda, .metal, .native }));
    try std.testing.expect(!backendOrderSelectsCudaBeforeCpu(&.{ .native, .cuda }));
    try std.testing.expect(!backendOrderSelectsCudaBeforeCpu(&.{ .metal, .native }));
}

test "imported ONNX graphs default to the native or wasm backend" {
    if (build_options.enable_wasm) {
        try std.testing.expectEqual(BackendType.wasm, configuredPreferredBackends()[0]);
        try std.testing.expectEqual(BackendType.wasm, defaultImportedOnnxBackend());
        try std.testing.expect(BackendType.wasm.supportsDirectSessionLoad());
    } else {
        try std.testing.expectEqual(BackendType.native, defaultImportedOnnxBackend());
    }
}

test "preferred backend override keeps fallback backends" {
    try std.testing.expectEqualSlices(BackendType, &.{ .native, .metal }, preferredBackendsForOverride(.native));
}

test "ONNX files always use the imported graph runtime" {
    var manager = SessionManager.init(std.testing.allocator);
    try std.testing.expect(manager.shouldUseImportedOnnxGraphRuntime("model.onnx"));
    manager.graph_runtime_strategy = .partitioned;
    try std.testing.expect(manager.shouldUseImportedOnnxGraphRuntime("model.onnx"));
    try std.testing.expect(!manager.shouldUseImportedOnnxGraphRuntime("model.gguf"));
}

test "A4B request rejects imported ONNX Metal artifact before session creation" {
    var manager = SessionManager.init(std.testing.allocator);
    manager.preferred_backends = &.{.metal};
    manager.a4b_inference_request = .{ .residency_mode = .streamed, .memory_budget_mb = 2048 };
    try std.testing.expectError(error.A4bUnsupportedArtifact, manager.loadModel("model.onnx"));
}

test "session manager defaults runtime kernel JIT off" {
    const manager = SessionManager.init(std.testing.allocator);
    try std.testing.expectEqual(kernel_jit_mod.Mode.off, manager.kernel_jit.mode);
}

test "required runtime kernel JIT accepts only direct GPU session backends" {
    try std.testing.expect(BackendType.metal.supportsKernelJitSession());
    try std.testing.expect(BackendType.cuda.supportsKernelJitSession());
    try std.testing.expect(!BackendType.native.supportsKernelJitSession());
    try std.testing.expect(!BackendType.wasm.supportsKernelJitSession());

    var manager = SessionManager.init(std.testing.allocator);
    manager.kernel_jit.mode = .required;
    manager.preferred_backends = &.{.native};
    try std.testing.expectEqual(@as(?BackendType, null), manager.bestAvailable());
}

test "session manager preferred-backend clone preserves runtime kernel JIT" {
    var source = SessionManager.init(std.testing.allocator);
    source.graph_runtime_strategy = .partitioned;
    source.kernel_jit = .{
        .mode = .shadow,
        .cache_dir = "/tmp/antfly-jit",
        .qualified_profile_path = "/tmp/antfly-jit/profile.json",
        .max_cache_bytes_mb = 256,
        .preload_budget_ms = 120_000,
    };
    source.kernel_jit_load_context = .startup_preload;
    source.a4b_inference_request = .{ .residency_mode = .streamed, .memory_budget_mb = 4096 };
    const preferred = [_]BackendType{.native};

    const cloned = source.withPreferredBackends(std.testing.allocator, &preferred);
    try std.testing.expectEqual(source.graph_runtime_strategy, cloned.graph_runtime_strategy);
    try std.testing.expectEqual(source.kernel_jit.mode, cloned.kernel_jit.mode);
    try std.testing.expectEqualStrings(source.kernel_jit.cache_dir.?, cloned.kernel_jit.cache_dir.?);
    try std.testing.expectEqualStrings(source.kernel_jit.qualified_profile_path.?, cloned.kernel_jit.qualified_profile_path.?);
    try std.testing.expectEqual(source.kernel_jit.max_cache_bytes_mb, cloned.kernel_jit.max_cache_bytes_mb);
    try std.testing.expectEqual(source.kernel_jit.preload_budget_ms, cloned.kernel_jit.preload_budget_ms);
    try std.testing.expectEqual(source.kernel_jit_load_context, cloned.kernel_jit_load_context);
    try std.testing.expectEqual(source.a4b_inference_request, cloned.a4b_inference_request);
    try std.testing.expectEqualSlices(BackendType, &preferred, cloned.preferred_backends);
}

test "auto backend order prefers Metal then native" {
    if (!build_options.enable_wasm) {
        try std.testing.expectEqualSlices(BackendType, &.{ .metal, .native }, configuredPreferredBackends());
    }
}

/// The ONNX Runtime backend was removed. `onnx` is rejected by name so a stale
/// configuration fails with an explanation instead of silently falling back.
pub const removed_onnx_backend_message =
    "the onnx backend (ONNX Runtime) has been removed; ONNX models run on Antfly's native ONNX implementation, so use auto, native, metal, or cuda";

pub fn isRemovedOnnxBackendName(value: []const u8) bool {
    return std.ascii.eqlIgnoreCase(value, "onnx");
}

const backend_environment_variables = [_][:0]const u8{
    "ANTFLY_INFERENCE_PREFERRED_BACKEND",
    "TERMITE_PREFERRED_BACKEND",
    "ANTFLY_INFERENCE_REQUIRED_BACKEND",
};

/// Reject backend environment settings that name the removed `onnx` backend.
pub fn validateBackendEnvironment() !void {
    if (build_options.enable_wasm or !build_options.link_libc) return;
    for (backend_environment_variables) |name| {
        const raw = std.c.getenv(name) orelse continue;
        try validateBackendEnvironmentValue(name, std.mem.span(raw));
    }
}

fn validateBackendEnvironmentValue(name: []const u8, value: []const u8) !void {
    if (!isRemovedOnnxBackendName(value)) return;
    if (!builtin.is_test) std.log.err("{s}={s}: {s}", .{ name, value, removed_onnx_backend_message });
    return error.OnnxBackendRemoved;
}

test "backend environment rejects the removed onnx backend" {
    try std.testing.expectError(
        error.OnnxBackendRemoved,
        validateBackendEnvironmentValue("ANTFLY_INFERENCE_PREFERRED_BACKEND", "onnx"),
    );
    try std.testing.expectError(
        error.OnnxBackendRemoved,
        validateBackendEnvironmentValue("TERMITE_PREFERRED_BACKEND", "ONNX"),
    );
    try validateBackendEnvironmentValue("ANTFLY_INFERENCE_PREFERRED_BACKEND", "native");
    try std.testing.expectEqual(@as(?BackendType, null), parseBackendName("onnx", true));
    try std.testing.expect(requiredBackendConfigForValue("onnx").invalid);
}

fn preferredBackendOverride() ?BackendType {
    if (build_options.enable_wasm or !build_options.link_libc) return null;
    const value = std.c.getenv("ANTFLY_INFERENCE_PREFERRED_BACKEND") orelse
        std.c.getenv("TERMITE_PREFERRED_BACKEND") orelse
        return null;
    const backend = parseBackendName(std.mem.span(value), true) orelse return null;
    return if (backend == .wasm) null else backend;
}

fn requiredBackendFromEnv() RequiredBackendConfig {
    if (build_options.enable_wasm or !build_options.link_libc) return .{};
    const value = std.c.getenv("ANTFLY_INFERENCE_REQUIRED_BACKEND") orelse return .{};
    return requiredBackendConfigForValue(std.mem.span(value));
}

fn requiredBackendConfigForValue(value: []const u8) RequiredBackendConfig {
    return if (parseBackendName(value, false)) |backend|
        .{ .backend = backend }
    else
        .{ .invalid = true };
}

fn parseBackendName(value: []const u8, allow_auto: bool) ?BackendType {
    if (allow_auto and std.ascii.eqlIgnoreCase(value, "auto")) return null;
    if (std.ascii.eqlIgnoreCase(value, "metal")) return .metal;
    if (std.ascii.eqlIgnoreCase(value, "pjrt")) return .pjrt;
    if (std.ascii.eqlIgnoreCase(value, "cuda")) return .cuda;
    if (std.ascii.eqlIgnoreCase(value, "native")) return .native;
    if (std.ascii.eqlIgnoreCase(value, "wasm")) return .wasm;
    return null;
}

fn enforceRequiredBackend(
    required_backend: ?BackendType,
    fallback_backends: []const BackendType,
    required_buf: *[1]BackendType,
) []const BackendType {
    const backend = required_backend orelse return fallback_backends;
    required_buf[0] = backend;
    return required_buf;
}

fn backendLoadFailure(required_backend: ?BackendType, first_err: ?anyerror) anyerror {
    if (required_backend != null) return first_err orelse error.RequiredBackendUnavailable;
    return first_err orelse error.NoBackendAvailable;
}

test "required backend parser rejects auto and invalid values" {
    try std.testing.expectEqual(BackendType.cuda, requiredBackendConfigForValue("cuda").backend.?);
    try std.testing.expectEqual(BackendType.pjrt, requiredBackendConfigForValue("pjrt").backend.?);
    try std.testing.expectEqual(BackendType.wasm, requiredBackendConfigForValue("wasm").backend.?);
    try std.testing.expect(requiredBackendConfigForValue("auto").invalid);
    try std.testing.expect(requiredBackendConfigForValue("bogus").invalid);
}

test "session manager reads required backend policy from environment" {
    const expected = requiredBackendFromEnv();
    const manager = SessionManager.init(std.testing.allocator);
    try std.testing.expectEqual(expected.backend, manager.required_backend);
    try std.testing.expectEqual(expected.invalid, manager.required_backend_invalid);
}

test "required backend replaces every fallback candidate" {
    var manager = SessionManager.init(std.testing.allocator);
    manager.required_backend = .cuda;
    manager.required_backend_invalid = false;
    var required_buf: [1]BackendType = undefined;
    const fallback = [_]BackendType{ .native, .metal };
    try std.testing.expectEqualSlices(
        BackendType,
        &.{.cuda},
        try manager.requiredBackendCandidates(&fallback, &required_buf),
    );
    manager.required_backend = null;
    try std.testing.expectEqualSlices(
        BackendType,
        &fallback,
        try manager.requiredBackendCandidates(&fallback, &required_buf),
    );
}

test "required backend gates backend-specific fast paths" {
    var manager = SessionManager.init(std.testing.allocator);
    manager.required_backend = .cuda;
    manager.required_backend_invalid = false;
    try std.testing.expect(try manager.allowsDirectBackend(.cuda));
    try std.testing.expect(!try manager.allowsDirectBackend(.native));
    try std.testing.expect(try manager.allowsBackend(.cuda));
    try std.testing.expect(!try manager.allowsBackend(.native));

    manager.required_backend = null;
    try std.testing.expect(try manager.allowsDirectBackend(.native));

    manager.required_backend_invalid = true;
    try std.testing.expectError(error.InvalidRequiredBackend, manager.allowsDirectBackend(.native));
}

test "required backend policy rejects unavailable and compiled-only backends" {
    var manager = SessionManager.init(std.testing.allocator);
    manager.required_backend = null;
    manager.required_backend_invalid = false;
    try manager.validateRequiredBackendPolicy();

    manager.required_backend_invalid = true;
    try std.testing.expectError(
        error.InvalidRequiredBackend,
        manager.validateRequiredBackendPolicy(),
    );

    manager.required_backend_invalid = false;
    manager.required_backend = .pjrt;
    try std.testing.expectError(
        error.RequiredBackendUnavailable,
        manager.validateRequiredBackendPolicy(),
    );

    manager.required_backend = .native;
    if (BackendType.native.available()) {
        try manager.validateRequiredBackendPolicy();
    } else {
        try std.testing.expectError(
            error.RequiredBackendUnavailable,
            manager.validateRequiredBackendPolicy(),
        );
    }
}

test "invalid required backend fails before model loading" {
    var manager = SessionManager.init(std.testing.allocator);
    manager.required_backend = null;
    manager.required_backend_invalid = true;
    try std.testing.expectError(error.InvalidRequiredBackend, manager.loadModel("/nonexistent/model"));
}

test "unavailable required backend does not fall back" {
    var manager = SessionManager.init(std.testing.allocator);
    manager.required_backend = .pjrt;
    manager.required_backend_invalid = false;
    manager.preferred_backends = &.{.native};
    try std.testing.expectError(error.RequiredBackendUnavailable, manager.loadModel("/nonexistent/model"));
}

test "failing required backend preserves its load error" {
    try std.testing.expectEqual(
        error.NoTensorStoreFound,
        backendLoadFailure(.native, error.NoTensorStoreFound),
    );
    try std.testing.expectEqual(
        error.RequiredBackendUnavailable,
        backendLoadFailure(.cuda, null),
    );
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
    if (!man.usesGgufWeights()) return false;
    const gguf_path = man.gguf_path.?;
    const total_bytes = c_file.fileSize(allocator, gguf_path) catch return true;
    return shouldPreferBlasBeforeGpuForBytes(total_bytes, gpuEagerDenseMaxBytes());
}

fn shouldPreferNativeTextEncoder(man: manifest_mod.ModelManifest) bool {
    if (man.model_type != .embedder) return false;
    const artifact = man.nativeWeightArtifactKind() orelse return false;
    if (artifact != .safetensors and artifact != .sharded_safetensors) return false;
    return man.onnx_path != null and
        man.visual_model_path == null and
        man.audio_model_path == null and
        man.text_projection_path == null and
        man.visual_projection_path == null and
        man.audio_projection_path == null;
}

pub const bge_m3_metal_min_managed_embeddings_per_second: f64 = 1.0;

fn bgeM3MetalThroughputQualifies(raw: []const u8) bool {
    const measured = std.fmt.parseFloat(f64, raw) catch return false;
    return std.math.isFinite(measured) and
        measured >= bge_m3_metal_min_managed_embeddings_per_second;
}

fn bgeM3MetalQualified() bool {
    if (build_options.enable_wasm or !build_options.link_libc) return false;
    const value = std.c.getenv("ANTFLY_BGE_M3_METAL_MANAGED_EMBEDDINGS_PER_SECOND") orelse return false;
    return bgeM3MetalThroughputQualifies(std.mem.span(value));
}

test "BGE-M3 Metal qualification enforces measured managed throughput" {
    try std.testing.expect(!bgeM3MetalThroughputQualifies("true"));
    try std.testing.expect(!bgeM3MetalThroughputQualifies("0.999"));
    try std.testing.expect(bgeM3MetalThroughputQualifies("1.0"));
    try std.testing.expect(bgeM3MetalThroughputQualifies("4.25"));
}

fn shouldPreferNativeBgeM3(man: manifest_mod.ModelManifest) bool {
    return !bgeM3MetalQualified() and man.model_type == .embedder and
        std.mem.eql(u8, man.config_model_arch, "xlm-roberta") and
        man.bert_model_type == .roberta and
        man.bert_vocab_size == 250002 and man.hidden_size == 1024 and
        man.num_hidden_layers == 24 and man.num_attention_heads == 16 and
        man.intermediate_size == 4096 and man.max_position_embeddings == 8194;
}

fn effectiveBackendOrder(
    allocator: std.mem.Allocator,
    scratch: *[backend_order_capacity]BackendType,
    preferred: []const BackendType,
    manifest: ?manifest_mod.ModelManifest,
) []const BackendType {
    const prefer_blas_before_gpu = shouldPreferBlasBeforeGpu(allocator, manifest);
    if (manifest) |man| {
        if (shouldPreferNativeBgeM3(man)) {
            return reorderNativeAheadOfOtherBackends(scratch, preferred, true);
        }
        if (shouldPreferNativeTextEncoder(man)) {
            return reorderNativeAheadOfOtherBackends(scratch, preferred, true);
        }
        if (man.native_arch_hint == .layoutlmv3 and man.safetensors_path != null) {
            return reorderNativeAheadOfOtherBackends(scratch, preferred, prefer_blas_before_gpu);
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

fn reorderNativeAheadOfOtherBackends(
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
    _ = metal_kv_storage;
    _ = session_factory;
    _ = imported_onnx_session;
}

test "shouldPreferBlasBeforeGpuForBytes prefers native only above eager dense threshold" {
    try std.testing.expect(shouldPreferBlasBeforeGpuForBytes(2 * 1024 * 1024 * 1024, 1024 * 1024 * 1024));
    try std.testing.expect(!shouldPreferBlasBeforeGpuForBytes(256 * 1024 * 1024, 1024 * 1024 * 1024));
}

test "effective backend order keeps gpu before native for large gguf generators" {
    const preferred = [_]BackendType{ .pjrt, .metal, .native };
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
    const preferred = [_]BackendType{ .cuda, .pjrt, .metal, .native };
    var scratch: [backend_order_capacity]BackendType = undefined;
    const effective = effectiveBackendOrderForPreference(&scratch, &preferred, true);
    try std.testing.expectEqualSlices(BackendType, &.{ .pjrt, .cuda, .metal, .native }, effective);
}

test "effective backend order preserves gpu preference for small gguf generators" {
    const preferred = [_]BackendType{ .pjrt, .metal, .native };
    var scratch: [backend_order_capacity]BackendType = undefined;
    const effective = effectiveBackendOrderForPreference(&scratch, &preferred, false);
    try std.testing.expectEqualSlices(BackendType, &preferred, effective);
}

test "effective backend order preserves order for non-gguf models" {
    const preferred = [_]BackendType{ .pjrt, .metal, .native };
    var scratch: [backend_order_capacity]BackendType = undefined;
    const manifest: manifest_mod.ModelManifest = .{
        .allocator = std.testing.allocator,
        .model_type = .generator,
        .safetensors_path = "dummy",
    };
    const effective = effectiveBackendOrder(std.testing.allocator, &scratch, &preferred, manifest);
    try std.testing.expectEqualSlices(BackendType, &preferred, effective);
}

test "unqualified BGE-M3 prefers native ahead of Metal" {
    const preferred = [_]BackendType{ .metal, .native };
    var scratch: [backend_order_capacity]BackendType = undefined;
    const manifest: manifest_mod.ModelManifest = .{
        .allocator = std.testing.allocator,
        .model_type = .embedder,
        .config_model_arch = "xlm-roberta",
        .bert_model_type = .roberta,
        .bert_vocab_size = 250002,
        .hidden_size = 1024,
        .num_hidden_layers = 24,
        .num_attention_heads = 16,
        .intermediate_size = 4096,
        .max_position_embeddings = 8194,
        .safetensors_path = "model.safetensors",
    };
    const effective = effectiveBackendOrder(std.testing.allocator, &scratch, &preferred, manifest);
    try std.testing.expectEqualSlices(BackendType, &.{ .native, .metal }, effective);
}

test "effective backend order prefers native layoutlmv3 before other backends" {
    const preferred = [_]BackendType{ .pjrt, .metal, .native };
    var scratch: [backend_order_capacity]BackendType = undefined;
    const manifest: manifest_mod.ModelManifest = .{
        .allocator = std.testing.allocator,
        .native_arch_hint = .layoutlmv3,
        .safetensors_path = "dummy",
    };
    const effective = effectiveBackendOrder(std.testing.allocator, &scratch, &preferred, manifest);
    try std.testing.expectEqualSlices(BackendType, &.{ .metal, .native, .pjrt }, effective);
}

test "backend names parse canonical values aliases and optional auto" {
    inline for (.{ "native", "metal", "cuda", "pjrt", "wasm" }) |name|
        try std.testing.expectEqual(std.meta.stringToEnum(BackendType, name).?, BackendType.parse(name).?);
    try std.testing.expectEqual(BackendType.pjrt, BackendType.parse("xla").?);
    try std.testing.expectEqual(BackendType.wasm, BackendType.parse("webgpu").?);
    try std.testing.expect(BackendType.parse("unknown") == null);
    try std.testing.expect(BackendType.parse("onnx") == null);
    try std.testing.expect(try BackendType.parseOptional(null) == null);
    try std.testing.expect(try BackendType.parseOptional("auto") == null);
    try std.testing.expectError(error.InvalidArguments, BackendType.parseOptional("unknown"));
}
