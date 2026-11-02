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
pub const Host = struct {
    context: *anyopaque,
    admission_enabled: bool,
    unhealthy_fn: *const fn (*anyopaque, []const u8, backends.BackendType) bool,
    limits_fn: *const fn (*anyopaque, []const u8, backends.BackendRuntime, runtime.tier.memory.Limits) anyerror!runtime.tier.memory.Limits,
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
    if (control) |active| try active.check();
    var required_backend_scratch: [1]backends.BackendType = undefined;
    const policy_backends = try source_session_manager.requiredBackendCandidates(
        preferred_backends,
        &required_backend_scratch,
    );
    const effective_backends = policy_backends;
    const fail_closed_cuda_a4b = source_session_manager.a4b_inference_request == null and
        backends.backendOrderSelectsCudaBeforeCpu(effective_backends) and
        (try session_factory.resolveCudaA4bInferenceConfigForModelListing(
            allocator,
            model_dir,
            man,
            null,
        )) != null;
    // Keep the first real failure. Reporting a blanket NoModelFileFound hides the
    // actionable cause: a GGUF whose tensors could not be resolved fails with
    // MissingRequiredWeights, and callers were being told the file did not exist.
    var first_err: ?anyerror = null;
    var laya_resident_attempted = false;
    const laya_packed = man.hasCapability("typed_decisions") and session_factory.isPackedLayaModel(allocator, model_dir);
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
        if (!backend.supportsDirectSessionLoad()) continue;
        if (source_session_manager.a4b_inference_request != null and
            backend != .metal and backend != .cuda)
        {
            rememberPreferredLoadError(&first_err, error.A4bRequiresGpu);
            continue;
        }
        if (source_session_manager.kernel_jit.mode.failClosed() and
            !backend.supportsKernelJitSession()) continue;
        const candidate_path = preferredModelPathForBackend(model_dir, man, backend) orelse continue;
        if (control) |active| try active.updateDetail(
            .preparing_weights,
            0,
            1,
            model_dir,
            @tagName(backend),
        );
        if (source_session_manager.kernel_jit.mode.failClosed() and
            std.mem.endsWith(u8, candidate_path, ".onnx")) continue;
        var single_backend = [_]backends.BackendType{backend};
        var backend_session_manager = source_session_manager.withPreferredBackends(allocator, single_backend[0..]);
        const backend_runtime = backend_session_manager.resolveBackendRuntime(backend) catch |err| {
            rememberPreferredLoadError(&first_err, err);
            continue;
        };
        var hard_cancellation = enterSessionConstruction(control, backend_runtime) catch |err| {
            if (err == error.ProcessIsolationRequired) {
                rememberPreferredLoadError(&first_err, err);
                continue;
            }
            return err;
        };
        defer hard_cancellation.deinit();
        var resource_lease: ?runtime.tier.memory.AdmissionLease = null;
        // This attempt owns admission until the successful result explicitly
        // takes it. Includes cancellation, guard-arm failure, and fallback.
        defer if (resource_lease) |*lease| lease.release();
        const reservation: ?Reservation = if (host.admission_enabled) reserved: {
            const plan = load_plan.estimateModelLoadAdmission(model_dir, man, backend_runtime, source_session_manager.a4b_inference_request, laya_packed) catch |err| {
                rememberPreferredLoadError(&first_err, err);
                continue;
            };
            const limits = host.limits_fn(host.context, model_dir, backend_runtime, plan.limit_floor) catch |err| {
                rememberPreferredLoadError(&first_err, err);
                continue;
            };
            if (control) |active| try active.check();
            break :reserved host.reserve_fn(host.context, model_dir, backend_runtime, limits, plan) catch |err| {
                rememberPreferredLoadError(&first_err, err);
                continue;
            };
        } else null;
        const resident_amounts = if (reservation) |reserved| reserved.resident else runtime.tier.memory.AdmissionAmounts{};
        resource_lease = if (reservation) |reserved| reserved.lease else null;
        var close_protection = host.protect_close_fn(host.context, backend_runtime) catch |err| {
            if (err == error.ProcessIsolationRequired) {
                rememberPreferredLoadError(&first_err, err);
                continue;
            }
            return err;
        };
        defer if (close_protection) |protection| protection.release();
        if (backend_session_manager.loadModel(candidate_path)) |loaded_session| {
            var loaded = ManagedSession{ .session = loaded_session, .resource_lease = resource_lease };
            std.debug.assert(loaded.session.close_protection == null);
            loaded.session.close_protection = close_protection;
            close_protection = null;
            resource_lease = null;
            defer loaded.deinit();
            if (control) |active| try active.check();
            if (reservation) |reserved| {
                if (reserved.serving_floor) |floor| session_factory.configureReservedGenerationWorkspace(
                    &loaded.session,
                    floor,
                    resident_amounts,
                );
                try host.attach_admission_fn(host.context, &loaded.session, backend_runtime, resident_amounts, &man);
            }
            // Resident preparation can populate the shared weight cache.
            // Install its owner and hard limits while it is still empty,
            // keeping the construction peak leased until preparation drains.
            try loaded.session.prepareResident(control);
            if (loaded.resource_lease) |*lease| try lease.retain(resident_amounts);
            return loaded.take();
        } else |err| {
            std.log.warn("loadModel({s}) backend {s} failed: {s}", .{ model_dir, @tagName(backend), @errorName(err) });
            rememberPreferredLoadError(&first_err, err);
        }
    }

    if (laya_resident_attempted) return first_err orelse error.UnsupportedLayaArtifact;
    // A missing process boundary is an expected fail-closed policy decision,
    // not an artifact/import failure.
    if (first_err) |err| if (err == error.ProcessIsolationRequired) return err;
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
) !ManagedSession {
    if (control) |active| try active.update(.loading_model, 0, 1);
    var required_backend_scratch: [1]backends.BackendType = undefined;
    const effective_backends = try source_session_manager.requiredBackendCandidates(
        preferred_backends,
        &required_backend_scratch,
    );
    var first_err: ?anyerror = null;
    var artifact_estimate = if (host.admission_enabled)
        try load_plan.ComponentArtifactEstimate.init(allocator, model_path)
    else
        load_plan.ComponentArtifactEstimate.disabled;
    defer artifact_estimate.deinit();

    for (effective_backends) |backend| {
        if (control) |active| try active.check();
        if (!backend.supportsDirectSessionLoad()) continue;
        if (shared_backend_ctx) |shared| {
            if (shared.backendType() != backend) continue;
        }

        var single_backend = [_]backends.BackendType{backend};
        var session_manager = source_session_manager.withPreferredBackends(allocator, single_backend[0..]);
        const backend_runtime = session_manager.resolveBackendRuntime(backend) catch |err| {
            rememberPreferredLoadError(&first_err, err);
            continue;
        };

        var resource_lease: ?runtime.tier.memory.AdmissionLease = null;
        defer if (resource_lease) |*lease| lease.release();
        var resident_amounts = runtime.tier.memory.AdmissionAmounts{};
        if (host.admission_enabled) {
            const artifact_bytes = artifact_estimate.encodedBytes() catch |err| {
                rememberPreferredLoadError(&first_err, err);
                continue;
            };
            const plan = if (artifact_estimate == .onnx)
                load_plan.onnxModelLoadAdmission(artifact_bytes, backend_runtime)
            else
                load_plan.nativeModelLoadAdmission(
                    artifact_bytes,
                    backend_runtime.backend,
                    artifact_estimate.extraBackendResidentAmounts(backend_runtime.backend),
                );
            const admission_plan = plan catch |err| {
                rememberPreferredLoadError(&first_err, err);
                continue;
            };
            resident_amounts = admission_plan.resident;
            // Component construction historically admits before arming its
            // driver guard. Preserve that order and its cancellation boundary.
            const limits = host.limits_fn(host.context, model_path, backend_runtime, admission_plan.limit_floor) catch |err| {
                rememberPreferredLoadError(&first_err, err);
                continue;
            };
            const reservation = host.reserve_fn(host.context, model_path, backend_runtime, limits, admission_plan) catch |err| {
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
        if (session_manager.loadModelWithImportedOnnxContext(
            model_path,
            shared_backend_ctx,
        )) |loaded_session| {
            var loaded = ManagedSession{ .session = loaded_session, .resource_lease = resource_lease };
            std.debug.assert(loaded.session.close_protection == null);
            loaded.session.close_protection = close_protection;
            close_protection = null;
            resource_lease = null;
            defer loaded.deinit();
            if (control) |active| try active.check();
            if (loaded.resource_lease) |*lease| try lease.retain(resident_amounts);
            if (host.admission_enabled) {
                try host.attach_admission_fn(host.context, &loaded.session, backend_runtime, resident_amounts, null);
            }
            return loaded.take();
        } else |err| {
            rememberPreferredLoadError(&first_err, err);
        }
    }
    return first_err orelse error.NoBackendAvailable;
}
