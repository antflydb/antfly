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

//! Backend-attempt construction and rollback, independent of serving cache and
//! publication. The host supplies resource admission and lifetime protection;
//! this owner selects artifacts, constructs/prepares a session, and transfers
//! its retained lease only after all post-construction checks have succeeded.
const std = @import("std");
const backends = @import("../backends/backends.zig");
pub const ModelSource = @import("model_source.zig").ModelSource;
pub const PublishedArtifacts = @import("../registry/published_artifacts.zig").PublishedArtifacts;
pub const ResolvedModel = @import("../registry/model_selection.zig").ResolvedModel;
const safetensors_mod = @import("safetensors.zig");
const manifest_mod = @import("manifest.zig");
const session_factory = @import("../architectures/session_factory.zig");
const load_plan = @import("load_plan.zig");
const runtime = @import("../runtime/root.zig");
const execution_control_mod = @import("../execution_control.zig");
const InferenceExecutionControl = execution_control_mod.InferenceExecutionControl;
fn manifestHasNativeAssets(man: manifest_mod.ModelManifest) bool {
    return man.nativeWeightArtifactKind() != null;
}

pub const Reservation = struct {
    lease: runtime.tier.memory.AdmissionLease,
    resident: runtime.tier.memory.AdmissionAmounts,
    serving_floor: ?runtime.tier.memory.Limits,
};

/// Borrowed for this synchronous load only. No service/context escapes into the
/// published session. Admission policy is snapshotted for this attempt; no callback can disable it halfway through.
pub const ModelLoadAdmission = enum { preflight_checked, managed };
pub const ModelLoadPreflight = struct {
    context: *anyopaque,
    check: *const fn (*anyopaque, []const u8, *const manifest_mod.ModelManifest, backends.BackendType, ?@import("../graph/backend_contracts.zig").A4bInferenceRequest) anyerror!ModelLoadAdmission,
};
pub const Host = struct {
    verification_cache: ?*@import("../registry/published_artifacts.zig").VerificationCache = null,
    preflight: ?ModelLoadPreflight = null,
    context: *anyopaque,
    admission_enabled: bool,
    unhealthy_fn: *const fn (*anyopaque, []const u8, backends.BackendType) bool,
    limits_fn: *const fn (*anyopaque, ModelSource, backends.BackendRuntime, runtime.tier.memory.Limits) anyerror!runtime.tier.memory.Limits,
    reserve_fn: *const fn (*anyopaque, []const u8, backends.BackendRuntime, runtime.tier.memory.Limits, load_plan.ModelLoadAdmissionPlan) anyerror!Reservation,
    protect_close_fn: *const fn (*anyopaque, backends.BackendRuntime) anyerror!?backends.Session.CloseProtection,
    attach_admission_fn: *const fn (*anyopaque, *backends.Session, backends.BackendRuntime, runtime.tier.memory.AdmissionAmounts, ?*const manifest_mod.ModelManifest) anyerror!void,
};
pub const ManagedSession = struct {
    session: backends.Session,
    resource_lease: ?runtime.tier.memory.AdmissionLease = null,
    owns_session: bool = true,

    pub fn deinit(self: *ManagedSession) void {
        if (self.owns_session) self.session.closeRetainingResources(&self.resource_lease);
        if (self.resource_lease) |*lease| lease.release();
        self.resource_lease = null;
        self.owns_session = false;
    }

    /// Move the session and its admission together. A deferred deinit remains
    /// safe on the emptied source, including across fallible publication work.
    pub fn take(self: *ManagedSession) ManagedSession {
        const owned = self.*;
        self.resource_lease = null;
        self.owns_session = false;
        return owned;
    }

    /// Transfer session ownership while leaving admission accounting with the
    /// caller. Useful for pipelines that already own and close their sessions.
    pub fn disownSession(self: *ManagedSession) backends.Session {
        self.owns_session = false;
        return self.session;
    }
};

/// Session-only generator assets used while materializing offline artifacts.
/// Close the session before its borrowed prefetch/cache state, and keep its
/// admission reservation until all retained model resources are destroyed.
pub const ArtifactSession = struct {
    allocator: std.mem.Allocator,
    managed: ManagedSession,
    session: backends.Session,
    shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache = null,
    shared_prefetch: ?*runtime.tier.shared.SharedPrefetchState = null,

    pub fn deinit(self: *@This()) void {
        var close_scope = self.session.beginClose();
        defer close_scope.deinit();
        const lifetime = if (self.session.model_lifetime) |owner| owner.retain() else null;
        defer if (lifetime) |owner| owner.release();
        self.session.closeRetainingResources(&self.managed.resource_lease);
        self.managed.owns_session = false;
        if (self.shared_moe_cache) |cache| cache.releaseOwned();
        if (self.shared_prefetch) |state| {
            state.deinit();
            self.allocator.destroy(state);
        }
        self.managed.deinit();
    }
};

pub fn preferredModelPathForBackend(
    model_dir: []const u8,
    man: manifest_mod.ModelManifest,
    backend: backends.BackendType,
) ?[]const u8 {
    return switch (backend) {
        .native, .metal, .cuda, .wasm => if (!manifestHasNativeAssets(man) and man.onnx_path != null)
            man.onnx_path.?
        else
            model_dir,
        .pjrt => null,
    };
}

pub fn enterSessionConstruction(
    control: ?InferenceExecutionControl,
    backend_runtime: backends.BackendRuntime,
) !execution_control_mod.UninterruptibleGuard {
    const active = control orelse return .{};
    try active.check();
    return active.enterUninterruptible(backend_runtime.loadInterruption());
}

fn loadErrorPriority(err: anyerror) u2 {
    return switch (err) {
        // Admission pressure is actionable at the HTTP boundary: callers can
        // retry it after the current model request or eviction completes.
        error.ResourceTemporarilyUnavailable => 3,
        // A stable capacity-policy rejection is more useful than a backend
        // probe miss, but must not replace a retryable alternative.
        error.ResourceLimitExceeded => 2,
        // These are expected while walking a preferred-backend list and carry
        // less information than an actual import or model-contract failure.
        error.NoBackendAvailable, error.UnsupportedBackend => 0,
        else => 1,
    };
}

pub fn rememberPreferredLoadError(selected: *?anyerror, candidate: anyerror) void {
    const current = selected.* orelse {
        selected.* = candidate;
        return;
    };
    if (loadErrorPriority(candidate) > loadErrorPriority(current)) {
        selected.* = candidate;
    }
}

pub fn loadPreferred(
    allocator: std.mem.Allocator,
    host: Host,
    preferred_backends: []const backends.BackendType,
    model_dir: []const u8,
    man: manifest_mod.ModelManifest,
    source_session_manager: *const backends.SessionManager,
    control: ?InferenceExecutionControl,
) !ManagedSession {
    return loadPreferredWithArtifacts(allocator, host, preferred_backends, model_dir, man, source_session_manager, control, null);
}

pub fn loadPreferredWithArtifacts(
    allocator: std.mem.Allocator,
    host: Host,
    preferred_backends: []const backends.BackendType,
    model_dir: []const u8,
    provided_man: manifest_mod.ModelManifest,
    source_session_manager: *const backends.SessionManager,
    control: ?InferenceExecutionControl,
    borrowed: ?*PublishedArtifacts,
) !ManagedSession {
    if (control) |active| try active.check();
    var proof = try ComponentConstruction.init(
        allocator,
        if (control) |active| active.io orelse source_session_manager.io orelse std.Options.debug_io else source_session_manager.io orelse std.Options.debug_io,
        .{ .path = model_dir, .publication = provided_man.managed_publication },
        control,
        borrowed,
    );
    defer proof.deinit();
    if (proof.owned != null) proof.artifacts().verification_cache = host.verification_cache;
    const construction_root = try proof.artifacts().constructionPath(model_dir);
    defer proof.artifacts().allocator.free(construction_root);
    // Bind explicit selections once, before any backend policy or admission.
    // Every phase sees the same metadata; legacy callers never rediscover defaults.
    var bound = try BoundModelSource.init(&proof, model_dir, ModelSource.kindForPath(model_dir), &provided_man, null);
    defer bound.deinit();
    const man = bound.view().manifest.?.*;
    var onnx_admission_bytes: ?usize = null;
    var verified_onnx_path: ?[]const u8 = null;
    var device_order_scratch: backends.BackendOrderBuffer = undefined;
    const available_backends = try source_session_manager.executionBackendOrder(&device_order_scratch, preferred_backends, control);
    var required_backend_scratch: [1]backends.BackendType = undefined;
    const policy_backends = try source_session_manager.requiredBackendCandidates(
        available_backends,
        &required_backend_scratch,
    );
    const effective_backends = policy_backends;
    const fail_closed_cuda_a4b = source_session_manager.a4b_inference_request == null and
        backends.backendOrderSelectsCudaBeforeCpu(effective_backends) and
        (try session_factory.resolveCudaA4bInferenceConfigForModelListing(
            allocator,
            construction_root,
            man,
            null,
        )) != null;
    // Keep the first real failure. Reporting a blanket NoModelFileFound hides the
    // actionable cause: a GGUF whose tensors could not be resolved fails with
    // MissingRequiredWeights, and callers were being told the file did not exist.
    var first_err: ?anyerror = null;
    var laya_resident_attempted = false;
    const laya_packed = man.hasCapability("typed_decisions") and session_factory.isPackedLayaModel(allocator, construction_root);
    for (effective_backends) |backend| {
        if (source_session_manager.gliner_cuda_precision != null and backend != .cuda) continue;
        // Once opted-in Metal residency is attempted, preserve its actionable
        // admission/load error rather than silently publishing a CPU session.
        if (laya_resident_attempted) return first_err orelse error.UnsupportedLayaArtifact;
        if (backend == .metal and man.hasCapability("typed_decisions") and !laya_packed and @import("../ops/laya_metal.zig").enabled()) laya_resident_attempted = true;
        if (control) |active| try active.check();
        if (host.unhealthy_fn(host.context, model_dir, backend)) {
            rememberPreferredLoadError(&first_err, error.ModelBackendUnhealthy);
            continue;
        }
        if (fail_closed_cuda_a4b and !backend.supportsA4bSession()) {
            std.log.err(
                "loadModel({s}) qualified CUDA A4B artifact rejected CPU fallback after GPU admission failure",
                .{model_dir},
            );
            return first_err orelse error.A4bCudaAutoFallbackForbidden;
        }
        if (!backend.available() or !backend.supportsDirectSessionLoad()) continue;
        if (source_session_manager.a4b_inference_request != null and
            backend != .metal and backend != .cuda)
        {
            rememberPreferredLoadError(&first_err, error.A4bRequiresGpu);
            continue;
        }
        if (source_session_manager.kernel_jit.mode.failClosed() and
            !backend.supportsKernelJitSession()) continue;
        const source = (try bound.forBackend(&proof, backend)) orelse continue;
        const selected_man = source.manifest orelse &man;
        // Reuse the manifest and checksum proofs already acquired by the
        // primary loader. Graph inspection is shared across backend retries.
        if (source.kind == .onnx) {
            if (verified_onnx_path == null or !std.mem.eql(u8, verified_onnx_path.?, source.path)) {
                try bound.verify(&proof, backend);
                verified_onnx_path = source.path;
            }
        } else {
            try bound.verify(&proof, backend);
        }
        if (control) |active| try active.updateDetail(
            .preparing_weights,
            0,
            1,
            model_dir,
            @tagName(backend),
        );
        if (source_session_manager.kernel_jit.mode.failClosed() and
            source.kind == .onnx) continue;
        const load_admission: ModelLoadAdmission = if (host.preflight) |preflight|
            preflight.check(preflight.context, construction_root, selected_man, backend, source_session_manager.a4b_inference_request) catch |err| {
                rememberPreferredLoadError(&first_err, err);
                continue;
            }
        else
            .preflight_checked;
        const managed_admission = host.admission_enabled or load_admission == .managed;
        var single_backend = [_]backends.BackendType{backend};
        var backend_session_manager = source_session_manager.withPreferredBackends(allocator, single_backend[0..]);
        const backend_runtime = backend_session_manager.resolveBackendRuntime(backend) catch |err| {
            rememberPreferredLoadError(&first_err, err);
            continue;
        };
        validateSessionConstruction(control, backend_runtime) catch |err| {
            if (err == error.ProcessIsolationRequired) {
                rememberPreferredLoadError(&first_err, err);
                continue;
            }
            return err;
        };
        var resource_lease: ?runtime.tier.memory.AdmissionLease = null;
        // This attempt owns admission until the successful result explicitly
        // takes it. Includes cancellation, guard-arm failure, and fallback.
        defer if (resource_lease) |*lease| lease.release();
        var resident_amounts = runtime.tier.memory.AdmissionAmounts{};
        var admission_limits = runtime.tier.memory.Limits{};
        var serving_floor: ?runtime.tier.memory.Limits = null;
        if (managed_admission) {
            const admission_result = if (source.kind == .onnx) graph_plan: {
                if (onnx_admission_bytes == null) {
                    var artifacts = backends.imported_onnx_session.inspectArtifactSetWithBase(allocator, source.path, source.external_base_dir) catch |err| {
                        rememberPreferredLoadError(&first_err, err);
                        continue;
                    };
                    defer artifacts.deinit();
                    onnx_admission_bytes = artifacts.encoded_bytes;
                }
                break :graph_plan load_plan.onnxModelLoadAdmission(onnx_admission_bytes.?, backend_runtime);
            } else load_plan.estimateModelLoadAdmission(
                source.path,
                selected_man.*,
                backend_runtime,
                source_session_manager.a4b_inference_request,
                laya_packed,
            );
            const admission_plan = admission_result catch |err| {
                rememberPreferredLoadError(&first_err, err);
                continue;
            };
            resident_amounts = admission_plan.resident;
            serving_floor = admission_plan.serving_floor;
            admission_limits = host.limits_fn(
                host.context,
                source,
                backend_runtime,
                admission_plan.limit_floor,
            ) catch |err| {
                rememberPreferredLoadError(&first_err, err);
                continue;
            };
            if (control) |active| try active.check();
            const reservation = host.reserve_fn(host.context, model_dir, backend_runtime, admission_limits, admission_plan) catch |err| {
                rememberPreferredLoadError(&first_err, err);
                continue;
            };
            resource_lease = reservation.lease;
        }
        var hard_cancellation = enterSessionConstruction(control, backend_runtime) catch |err| {
            if (err == error.ProcessIsolationRequired) {
                rememberPreferredLoadError(&first_err, err);
                continue;
            }
            return err;
        };
        defer hard_cancellation.deinit();
        var close_protection = host.protect_close_fn(host.context, backend_runtime) catch |err| {
            if (err == error.ProcessIsolationRequired) {
                rememberPreferredLoadError(&first_err, err);
                continue;
            }
            return err;
        };
        defer if (close_protection) |protection| protection.release();
        if (backend_session_manager.loadModelSource(source, null)) |loaded_session| {
            var loaded = ManagedSession{ .session = loaded_session, .resource_lease = resource_lease };
            std.debug.assert(loaded.session.close_protection == null);
            loaded.session.installBorrowedCloseProtection(close_protection);
            close_protection = null;
            resource_lease = null;
            defer loaded.deinit();
            if (control) |active| try active.check();
            if (managed_admission) {
                if (loaded.session.executionCapabilities().measured_resident_weights) |weight_bytes| {
                    const construction_bytes = resident_amounts.backend_weight_bytes;
                    resident_amounts.backend_weight_bytes = try session_factory.retainedCudaMirrorWeightBytes(construction_bytes, weight_bytes);
                    std.log.info("cuda_eager_weight_residency construction_bytes={d} measured_weight_bytes={d} retained_weight_bytes={d}", .{
                        construction_bytes, weight_bytes, resident_amounts.backend_weight_bytes,
                    });
                }
                if (serving_floor) |floor| session_factory.configureReservedGenerationWorkspace(
                    &loaded.session,
                    floor,
                    resident_amounts,
                );
                // Admitted standalone/component receivers need the SAME actual
                // Model owner/gate before policy can authorize Native cold work.
                try loaded.session.installOwnedExecutionGate(allocator);
                try host.attach_admission_fn(host.context, &loaded.session, backend_runtime, resident_amounts, selected_man);
            }
            // Resident preparation can populate the shared weight cache.
            // Install its owner and hard limits while it is still empty,
            // keeping the construction peak leased until preparation drains.
            try loaded.session.prepareResident(control);
            try bound.finish(&proof);
            if (loaded.resource_lease) |*lease| try lease.retain(resident_amounts);
            return loaded.take();
        } else |err| {
            std.log.warn("loadModel({s}) backend {s} failed: {s}", .{ model_dir, @tagName(backend), @errorName(err) });
            rememberPreferredLoadError(&first_err, err);
        }
    }

    if (laya_resident_attempted) return first_err orelse error.UnsupportedLayaArtifact;
    // Missing isolation and caller budget caps are expected policy decisions,
    // rather than corrupt artifacts or session import failures.
    if (first_err) |err| if (err == error.ProcessIsolationRequired or err == error.MemoryBudgetExceeded or
        err == error.ResourceLimitExceeded or err == error.ResourceTemporarilyUnavailable) return err;
    std.log.err("loadModel({s}) failed: no backend accepted model", .{model_dir});
    std.log.err("manifest paths onnx={?s} visual={?s} audio={?s} text_projection={?s} visual_projection={?s} audio_projection={?s}", .{
        man.onnx_path,
        man.visual_model_path,
        man.audio_model_path,
        man.text_projection_path,
        man.visual_projection_path,
        man.audio_projection_path,
    });
    // NoModelFileFound only when nothing was even attempted.
    if (first_err) |err| return err;
    return error.NoModelFileFound;
}

pub fn loadComponent(
    allocator: std.mem.Allocator,
    host: Host,
    model_path: []const u8,
    preferred_backends: []const backends.BackendType,
    shared_backend_ctx: ?*backends.imported_onnx_session.SharedBackendContext,
    source_session_manager: *const backends.SessionManager,
    control: ?InferenceExecutionControl,
    selected: ResolvedModel,
    borrowed: ?*PublishedArtifacts,
) !ManagedSession {
    if (control) |active| try active.update(.loading_model, 0, 1);
    const io = if (control) |active| active.io orelse source_session_manager.io orelse std.Options.debug_io else source_session_manager.io orelse std.Options.debug_io;
    var construction_proof = try ComponentConstruction.init(allocator, io, selected, control, borrowed);
    defer construction_proof.deinit();
    if (construction_proof.owned != null) construction_proof.artifacts().verification_cache = host.verification_cache;
    var bound = try BoundModelSource.init(&construction_proof, model_path, ModelSource.kindForPath(model_path), null, null);
    defer bound.deinit();
    var device_order_scratch: backends.BackendOrderBuffer = undefined;
    const available_backends = try source_session_manager.executionBackendOrder(&device_order_scratch, preferred_backends, control);
    var required_backend_scratch: [1]backends.BackendType = undefined;
    const effective_backends = try source_session_manager.requiredBackendCandidates(
        available_backends,
        &required_backend_scratch,
    );
    var first_err: ?anyerror = null;
    var artifact_estimates: [2]?load_plan.ComponentArtifactEstimate = @splat(null);
    defer for (&artifact_estimates) |*estimate| if (estimate.*) |*value| value.deinit();

    for (effective_backends) |backend| {
        if (control) |active| try active.check();
        if (!backend.supportsDirectSessionLoad()) continue;
        const source = (try bound.forBackend(&construction_proof, backend)) orelse continue;
        try bound.verify(&construction_proof, backend);
        if (shared_backend_ctx) |shared| {
            if (shared.backendType() != backend) continue;
        }

        var single_backend = [_]backends.BackendType{backend};
        var session_manager = source_session_manager.withPreferredBackends(allocator, single_backend[0..]);
        const backend_runtime = session_manager.resolveBackendRuntime(backend) catch |err| {
            rememberPreferredLoadError(&first_err, err);
            continue;
        };

        validateSessionConstruction(control, backend_runtime) catch |err| {
            if (err == error.ProcessIsolationRequired) {
                rememberPreferredLoadError(&first_err, err);
                continue;
            }
            return err;
        };

        var resource_lease: ?runtime.tier.memory.AdmissionLease = null;
        defer if (resource_lease) |*lease| lease.release();
        var resident_amounts = runtime.tier.memory.AdmissionAmounts{};
        var admission_limits = runtime.tier.memory.Limits{};
        if (host.admission_enabled) {
            const slot = &artifact_estimates[@intFromBool(source.kind == .onnx)];
            if (slot.* == null) slot.* = try load_plan.ComponentArtifactEstimate.init(allocator, source);
            const artifact_estimate = &slot.*.?;
            const artifact_bytes = artifact_estimate.encodedBytes() catch |err| {
                rememberPreferredLoadError(&first_err, err);
                continue;
            };
            const plan = if (artifact_estimate.* == .onnx)
                load_plan.onnxModelLoadAdmission(artifact_bytes, backend_runtime)
            else
                load_plan.nativeModelLoadAdmission(
                    artifact_bytes,
                    backend_runtime.backend,
                    artifact_estimate.extraBackendResidentAmounts(backend_runtime.backend),
                );
            const base_plan = plan catch |err| {
                rememberPreferredLoadError(&first_err, err);
                continue;
            };
            const admission_plan = (if (source.kind == .onnx) base_plan else load_plan.withJinaAdapterAdmission(source.manifest.?.*, backend, base_plan)) catch |err| {
                rememberPreferredLoadError(&first_err, err);
                continue;
            };
            resident_amounts = admission_plan.resident;
            admission_limits = host.limits_fn(
                host.context,
                source,
                backend_runtime,
                admission_plan.limit_floor,
            ) catch |err| {
                rememberPreferredLoadError(&first_err, err);
                continue;
            };
            const reservation = host.reserve_fn(host.context, model_path, backend_runtime, admission_limits, admission_plan) catch |err| {
                rememberPreferredLoadError(&first_err, err);
                continue;
            };
            resource_lease = reservation.lease;
        }

        var construction = enterSessionConstruction(control, backend_runtime) catch |err| {
            if (err == error.ProcessIsolationRequired) {
                // An in-process caller can still use a later cooperative
                // backend. A required backend has a single candidate and
                // therefore remains fail-closed.
                rememberPreferredLoadError(&first_err, err);
                continue;
            }
            return err;
        };
        defer construction.deinit();
        var close_protection = host.protect_close_fn(host.context, backend_runtime) catch |err| {
            if (err == error.ProcessIsolationRequired) {
                rememberPreferredLoadError(&first_err, err);
                continue;
            }
            return err;
        };
        defer if (close_protection) |protection| protection.release();
        if (session_manager.loadModelSource(
            source,
            shared_backend_ctx,
        )) |loaded_session| {
            var loaded = ManagedSession{ .session = loaded_session, .resource_lease = resource_lease };
            std.debug.assert(loaded.session.close_protection == null);
            loaded.session.installBorrowedCloseProtection(close_protection);
            close_protection = null;
            resource_lease = null;
            defer loaded.deinit();
            if (control) |active| try active.check();
            if (loaded.resource_lease) |*lease| try lease.retain(resident_amounts);
            if (host.admission_enabled) {
                try loaded.session.installOwnedExecutionGate(allocator);
                try host.attach_admission_fn(host.context, &loaded.session, backend_runtime, resident_amounts, source.manifest);
            }
            try bound.finish(&construction_proof);
            return loaded.take();
        } else |err| {
            rememberPreferredLoadError(&first_err, err);
        }
    }
    return first_err orelse error.NoBackendAvailable;
}
/// One cold construction owns its publication fence. Bundle callers can lend
/// their verifier to share checksum proofs across graphs and backend retries.
pub const ComponentConstruction = struct {
    io: std.Io,
    lease: @import("../registry/publication_lease.zig").Lease,
    owned: ?PublishedArtifacts,
    borrowed: ?*PublishedArtifacts,

    pub fn init(a: std.mem.Allocator, io: std.Io, selected: ResolvedModel, control: ?InferenceExecutionControl, borrowed: ?*PublishedArtifacts) !ComponentConstruction {
        if (control) |active| {
            try active.check();
            if (active.model_selection) |expected| {
                if (!expected.matches(selected.path) or !std.meta.eql(expected.publication, selected.publication)) return error.ModelArtifactsChanging;
            }
        }
        var lease = try @import("../registry/publication_lease.zig").Lease.shared(a, io, selected.path);
        errdefer lease.deinit(io);
        if (borrowed) |provided| {
            if (!std.mem.eql(u8, provided.root, selected.path) or !std.meta.eql(provided.publication, selected.publication)) return error.ModelArtifactsChanging;
            try selected.verify(a, io);
            return .{ .io = io, .lease = lease, .owned = null, .borrowed = provided };
        }
        return .{ .io = io, .lease = lease, .owned = try PublishedArtifacts.init(a, io, selected.path, .{ .publication = selected.publication }), .borrowed = null };
    }
    /// Reuse an externally owned publication lease and artifact catalog during
    /// synchronous preparation. This view never releases the caller's fence.
    pub fn borrowingUnderLease(io: std.Io, borrowed_artifacts: *PublishedArtifacts) ComponentConstruction {
        return .{ .io = io, .lease = .{}, .owned = null, .borrowed = borrowed_artifacts };
    }
    pub fn artifacts(self: *ComponentConstruction) *PublishedArtifacts {
        return self.borrowed orelse &self.owned.?;
    }
    pub fn verifyPath(self: *ComponentConstruction, path: []const u8) !void {
        if (std.mem.endsWith(u8, path, ".onnx")) {
            try self.artifacts().verifyOnnx(path);
        } else if (std.mem.endsWith(u8, path, ".gguf") or std.mem.endsWith(u8, path, ".safetensors")) {
            try self.artifacts().verifyPath(path);
        }
    }
    pub fn verifyDirectory(self: *ComponentConstruction, path: []const u8, backend: backends.BackendType) !void {
        if (self.artifacts().receipt == null or (try std.Io.Dir.cwd().statFile(self.io, path, .{})).kind != .directory) return;
        // Directory components may select an ONNX primary too. Preserve
        // native-format preference rather than authenticating an unused graph.
        var man = try manifest_mod.loadFromDir(self.artifacts().allocator, path);
        defer man.deinit();
        try self.verifyManifest(man, backend);
    }
    pub fn verifyManifest(self: *ComponentConstruction, man: manifest_mod.ModelManifest, backend: backends.BackendType) !void {
        if (self.artifacts().receipt == null) return;
        if (backend == .wasm or !manifestHasNativeAssets(man)) {
            if (man.onnx_path) |onnx| try self.artifacts().verifyOnnxWithBase(onnx, if (man.onnx_logical_path) |path| std.fs.path.dirname(path) else null);
            return;
        }
        if (man.config_path) |path| try self.artifacts().verifyPath(path);
        const adapter = if (session_factory.usesJinaRetrievalAdapter(man, backend)) try man.jinaRetrievalAdapter() else null;
        if (adapter) |selected| try self.artifacts().verifyPath(selected.config);
        switch (man.nativeWeightArtifactKind().?) {
            .gguf => try self.artifacts().verifyPath(man.gguf_path.?),
            .safetensors, .sharded_safetensors => {
                // Authenticate the index before interpreting shard names, and
                // retain proof for every shard the native importer can open.
                if (man.safetensors_path == null) try self.artifacts().verifyPath(man.safetensors_index_path.?);
                var dependencies = try safetensors_mod.inspectArtifactDependencies(self.artifacts().allocator, man.safetensors_path, man.safetensors_index_path);
                defer dependencies.deinit();
                for (dependencies.paths) |dependency| try self.artifacts().verifyPath(dependency);
            },
        }
        if (man.nativeCompanionPath()) |path| try self.artifacts().verifyPath(path);
        if (adapter) |selected| try self.artifacts().verifyPath(selected.weights);
    }
    pub fn finish(self: *ComponentConstruction) !void {
        try self.artifacts().verify();
    }
    pub fn deinit(self: *ComponentConstruction) void {
        if (self.owned) |*owned| owned.deinit();
        self.lease.deinit(self.io);
    }
};

/// Keep supplied execution metadata borrowed while owning only rebound paths.
/// Provenance-free callers select artifacts explicitly; discovering defaults
/// would change the model. Ordinary snapshots need no path copies or parsing.
pub const BoundDirectoryManifest = struct {
    allocator: std.mem.Allocator,
    manifest: manifest_mod.ModelManifest,
    owns_manifest: bool = false,
    owned_paths: std.ArrayListUnmanaged([]u8) = .empty,

    pub fn init(proof: *ComponentConstruction, physical_root: []const u8, requested_root: []const u8, provided: ?*const manifest_mod.ModelManifest) !BoundDirectoryManifest {
        const a = proof.artifacts().allocator;
        if (provided == null) return .{ .allocator = a, .manifest = try manifest_mod.loadFromDirectorySource(a, physical_root, requested_root), .owns_manifest = true };
        var result = BoundDirectoryManifest{ .allocator = a, .manifest = provided.?.* };
        result.manifest.allocator = a;
        errdefer result.deinit();
        if (result.manifest.source_root != null) {
            try result.manifest.verifyBoundRoot(proof.io, physical_root);
            return result;
        }
        const stat = try std.Io.Dir.cwd().statFile(proof.io, physical_root, .{});
        if (stat.kind != .directory) return error.NotDir;
        result.manifest.source_root = try result.own(try a.dupe(u8, physical_root));
        result.manifest.source_root_inode = stat.inode;
        // Namespace rebasing must not open or require unused alternatives.
        if (provided.?.onnx_logical_path orelse provided.?.onnx_path) |logical| {
            result.manifest.onnx_logical_path = try result.own(try proof.artifacts().namespacePath(logical));
        }
        inline for (@typeInfo(manifest_mod.ModelManifest).@"struct".field_names) |field| {
            if (comptime std.mem.endsWith(u8, field, "_path") and
                !std.mem.eql(u8, field, "onnx_logical_path") and
                @TypeOf(@field(result.manifest, field)) == ?[]const u8)
            {
                if (@field(result.manifest, field)) |path| {
                    @field(result.manifest, field) = try result.own(try proof.artifacts().namespacePath(path));
                }
            }
        }
        try result.manifest.verifyBoundRoot(proof.io, physical_root);
        return result;
    }

    /// Pin only the native importer's dependencies; keep alternate exports and
    /// independent lazy components in the namespace for their own acquisitions.
    pub fn nativeSnapshot(self: *const BoundDirectoryManifest, proof: *ComponentConstruction) !BoundDirectoryManifest {
        var result = BoundDirectoryManifest{ .allocator = self.allocator, .manifest = self.manifest };
        errdefer result.deinit();
        switch (self.manifest.nativeWeightArtifactKind() orelse return error.NoModelFileFound) {
            .gguf => try result.capture(proof, "gguf_path"),
            .safetensors => try result.capture(proof, "safetensors_path"),
            .sharded_safetensors => try result.capture(proof, "safetensors_index_path"),
        }
        switch (self.manifest.nativeCompanion()) {
            .none => {},
            .gliner_gguf => try result.capture(proof, "gliner_head_gguf_path"),
            .gliner_safetensors => try result.capture(proof, "gliner_head_safetensors_path"),
            .projector => try result.capture(proof, "gguf_projector_path"),
        }
        if (result.manifest.config_path != null) {
            try result.capture(proof, "config_path");
        } else if (self.manifest.source_root) |root| {
            // Partial legacy manifests still allow root architecture metadata,
            // but an unlisted default must not override embedded GGUF metadata.
            const requested = try std.fs.path.join(self.allocator, &.{ root, "config.json" });
            defer self.allocator.free(requested);
            const config = try proof.artifacts().optionalConstructionPath(requested);
            if (config) |path| {
                const kind = (std.Io.Dir.cwd().statFile(proof.io, path, .{}) catch |err| {
                    self.allocator.free(path);
                    return err;
                }).kind;
                if (kind == .file) {
                    result.manifest.config_path = try result.own(path);
                } else self.allocator.free(path);
            }
        }
        return result;
    }

    pub fn withJinaAdapter(self: *const BoundDirectoryManifest, proof: *ComponentConstruction) !BoundDirectoryManifest {
        var result = BoundDirectoryManifest{ .allocator = self.allocator, .manifest = self.manifest };
        errdefer result.deinit();
        if (!result.manifest.jina_adapter_selection_captured and result.manifest.jina_adapter_config_path == null and result.manifest.jina_adapter_weights_path == null) {
            if (result.manifest.source_root) |root| {
                inline for (.{ .{ "jina_adapter_config_path", "adapters/retrieval/adapter_config.json" }, .{ "jina_adapter_weights_path", "adapters/retrieval/adapter_model.safetensors" } }) |entry| {
                    const path = try std.fs.path.join(self.allocator, &.{ root, entry[1] });
                    defer self.allocator.free(path);
                    if (try proof.artifacts().optionalConstructionPath(path)) |selected| @field(result.manifest, entry[0]) = try result.own(selected);
                }
            }
        }
        result.manifest.jina_adapter_selection_captured = true;
        if (try result.manifest.jinaRetrievalAdapter()) |_| {
            try result.capture(proof, "jina_adapter_config_path");
            try result.capture(proof, "jina_adapter_weights_path");
        }
        return result;
    }

    pub fn capture(self: *BoundDirectoryManifest, proof: *ComponentConstruction, comptime field: []const u8) !void {
        if (@field(self.manifest, field)) |path| {
            const physical = try proof.artifacts().constructionPath(path);
            if (std.mem.eql(u8, physical, path)) {
                self.allocator.free(physical);
            } else @field(self.manifest, field) = try self.own(physical);
        }
    }

    pub fn own(self: *BoundDirectoryManifest, path: []u8) ![]u8 {
        self.owned_paths.append(self.allocator, path) catch |err| {
            self.allocator.free(path);
            return err;
        };
        return path;
    }

    pub fn deinit(self: *BoundDirectoryManifest) void {
        if (self.owns_manifest) self.manifest.deinit();
        for (self.owned_paths.items) |path| self.allocator.free(path);
        self.owned_paths.deinit(self.allocator);
    }
};

/// Acquisition-owned storage for the borrowed backend source descriptor.
pub const BoundModelSource = struct {
    allocator: std.mem.Allocator,
    path: []u8,
    kind: ModelSource.Kind,
    logical_path: []const u8,
    external_base_dir: ?[]u8 = null,
    owned_manifest: ?manifest_mod.ModelManifest = null,
    borrowed_manifest: ?*const manifest_mod.ModelManifest = null,
    directory_manifest: ?BoundDirectoryManifest = null,
    native_manifest: ?BoundDirectoryManifest = null,
    native_adapter_manifest: ?BoundDirectoryManifest = null,
    onnx_binding: ?struct {
        path: []u8,
        external_base_dir: []u8,
        manifest: manifest_mod.ModelManifest,
    } = null,

    pub fn init(proof: *ComponentConstruction, requested: []const u8, kind: ModelSource.Kind, manifest: ?*const manifest_mod.ModelManifest, external_base: ?[]const u8) !BoundModelSource {
        const artifacts = proof.artifacts();
        var result = BoundModelSource{ .allocator = artifacts.allocator, .path = try artifacts.constructionPath(requested), .kind = kind, .logical_path = if (kind == .onnx and manifest != null) manifest.?.onnx_logical_path orelse requested else requested, .borrowed_manifest = manifest };
        errdefer result.deinit();
        if (kind == .onnx) {
            result.external_base_dir = try artifacts.constructionPath(external_base orelse std.fs.path.dirname(requested) orelse ".");
        } else {
            if (kind == .directory) {
                result.borrowed_manifest = null;
                result.directory_manifest = try BoundDirectoryManifest.init(proof, result.path, requested, manifest);
            } else if (manifest == null) result.owned_manifest = try manifest_mod.loadFromDir(result.allocator, requested);
            if (kind == .gguf) {
                const selected = result.view().manifest.?;
                if (selected.source_root) |root| {
                    try selected.verifyBoundRoot(proof.io, root);
                }
                if (selected.nativeWeightArtifactKind() != .gguf) return error.ModelArtifactsChanging;
                const selected_path = try artifacts.constructionPath(selected.gguf_path orelse return error.NoModelFileFound);
                defer result.allocator.free(selected_path);
                if (!std.mem.eql(u8, selected_path, result.path)) return error.ModelArtifactsChanging;
                if (selected.source_root) |root| try artifacts.bindDirectoryScope(root);
            }
        }
        return result;
    }
    pub fn view(self: *const BoundModelSource) ModelSource {
        return .{ .path = self.path, .kind = self.kind, .logical_path = self.logical_path, .external_base_dir = self.external_base_dir, .manifest = if (self.directory_manifest) |*directory| &directory.manifest else self.borrowed_manifest orelse if (self.owned_manifest) |*manifest| manifest else null };
    }
    /// Resolve the backend's actual source once and reuse it for proof,
    /// preflight, admission, and construction, including subsequent retries.
    pub fn forBackend(self: *BoundModelSource, proof: *ComponentConstruction, backend: backends.BackendType) !?ModelSource {
        if (self.kind == .gguf) {
            if (backend == .wasm) return null;
            if (self.native_manifest == null) {
                const direct = BoundDirectoryManifest{ .allocator = self.allocator, .manifest = self.view().manifest.?.* };
                self.native_manifest = try direct.nativeSnapshot(proof);
            }
            var source = self.view();
            source.manifest = try self.nativeManifestForBackend(proof, backend);
            return source;
        }
        if (self.kind != .directory) return self.view();
        const directory = &self.directory_manifest.?;
        const man = &directory.manifest;
        if (backend == .wasm or !manifestHasNativeAssets(man.*)) {
            if (man.onnx_path) |selected| {
                if (self.onnx_binding == null) {
                    const path = try proof.artifacts().constructionPath(selected);
                    errdefer self.allocator.free(path);
                    const logical = man.onnx_logical_path orelse selected;
                    const base = try proof.artifacts().constructionPath(std.fs.path.dirname(logical) orelse ".");
                    var graph_manifest = man.*;
                    graph_manifest.onnx_path = path;
                    graph_manifest.gguf_path = null;
                    graph_manifest.safetensors_path = null;
                    graph_manifest.safetensors_index_path = null;
                    graph_manifest.gguf_projector_path = null;
                    graph_manifest.gliner_head_gguf_path = null;
                    graph_manifest.gliner_head_safetensors_path = null;
                    graph_manifest.jina_adapter_config_path = null;
                    graph_manifest.jina_adapter_weights_path = null;
                    graph_manifest.jina_adapter_selection_captured = true;
                    self.onnx_binding = .{ .path = path, .external_base_dir = base, .manifest = graph_manifest };
                }
                const graph = &self.onnx_binding.?;
                return .{ .path = graph.path, .kind = .onnx, .manifest = &graph.manifest, .logical_path = man.onnx_logical_path orelse selected, .external_base_dir = graph.external_base_dir };
            }
            if (backend == .wasm) return null;
            return self.view();
        }
        if (self.native_manifest == null) self.native_manifest = try directory.nativeSnapshot(proof);
        var source = self.view();
        source.manifest = try self.nativeManifestForBackend(proof, backend);
        return source;
    }

    pub fn nativeManifestForBackend(self: *BoundModelSource, proof: *ComponentConstruction, backend: backends.BackendType) !*const manifest_mod.ModelManifest {
        const native = &self.native_manifest.?;
        if (!session_factory.usesJinaRetrievalAdapter(native.manifest, backend)) return &native.manifest;
        if (self.native_adapter_manifest == null) self.native_adapter_manifest = try native.withJinaAdapter(proof);
        return &self.native_adapter_manifest.?.manifest;
    }

    pub fn verify(self: *BoundModelSource, proof: *ComponentConstruction, backend: backends.BackendType) !void {
        const source = (try self.forBackend(proof, backend)) orelse return error.NoModelFileFound;
        switch (source.kind) {
            .onnx => try proof.artifacts().verifyOnnxWithBase(source.path, source.external_base_dir),
            .gguf, .directory => try proof.verifyManifest(source.manifest.?.*, backend),
        }
    }
    pub fn finish(self: *const BoundModelSource, proof: *ComponentConstruction) !void {
        if (self.kind == .directory) {
            try self.directory_manifest.?.manifest.verifyBoundRoot(proof.io, self.path);
        } else if (self.borrowed_manifest) |man| {
            if (man.source_root) |root| try man.verifyBoundRoot(proof.io, root);
        }
        try proof.finish();
    }
    pub fn deinit(self: *BoundModelSource) void {
        if (self.onnx_binding) |graph| {
            self.allocator.free(graph.path);
            self.allocator.free(graph.external_base_dir);
        }
        if (self.native_adapter_manifest) |*adapter| adapter.deinit();
        if (self.native_manifest) |*native| native.deinit();
        if (self.directory_manifest) |*directory| directory.deinit();
        if (self.owned_manifest) |*manifest| manifest.deinit();
        if (self.external_base_dir) |path| self.allocator.free(path);
        self.allocator.free(self.path);
    }
};

pub fn validateSessionConstruction(control: ?InferenceExecutionControl, backend_runtime: backends.BackendRuntime) !void {
    if (control) |active| {
        try active.check();
        try active.validateUninterruptible(backend_runtime.loadInterruption());
    }
}
