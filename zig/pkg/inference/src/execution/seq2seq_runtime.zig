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

//! Request-owned incremental execution for the explicit merged seq2seq ABI.
//! Reuses the graph KV owner, never a process-global prompt/context cache.
const std = @import("std");
const backends = @import("../backends/backends.zig");
const kv = @import("../graph/onnx_kv_cache.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;

const sessions = @import("../backends/session.zig");
const tensor_dispatch = @import("tensor_dispatch.zig");
const Suffix = @import("decoder_position.zig").Suffix;

pub fn qualified(session: backends.Session, vocabulary: usize) bool {
    const context = session.shared_compute_context orelse return false;
    if (session.vtable.runResidentInputs == null or session.vtable.runResidentInputsWithControl == null or
        context.stableBackend().vtable.sliceRows2D == null or
        !@import("../backends/imported_onnx_session.zig").supportsMergedCacheBranch(session)) return false;
    return qualifiedSignature(session.inputInfo(), session.outputInfo(), vocabulary);
}

/// Shared by metadata-only cold planning and the loaded-session ABI check.
pub fn qualifiedSignature(inputs: []const backends.TensorInfo, outputs: []const backends.TensorInfo, vocabulary: usize) bool {
    const vocab = std.math.cast(i64, vocabulary) orelse return false;
    if (vocab == 0) return false;
    var ids = false;
    var encoder = false;
    var branch = false;
    var past_count: usize = 0;
    for (inputs, 0..) |info, index| {
        for (inputs[0..index]) |prior| if (std.mem.eql(u8, prior.name, info.name)) return false;
        if (std.mem.eql(u8, info.name, "input_ids")) {
            if (info.dtype != .i64 or info.shape.len != 2 or info.shape[0] == 0 or info.shape[0] > 1 or info.shape[1] >= 0) return false;
            ids = true;
        } else if (std.mem.eql(u8, info.name, "encoder_hidden_states")) {
            if (info.dtype != .f32 or info.shape.len != 3 or info.shape[0] == 0 or info.shape[0] > 1) return false;
            encoder = true;
        } else if (std.mem.eql(u8, info.name, "encoder_attention_mask")) {
            if (info.dtype != .i64 or info.shape.len != 2 or info.shape[0] == 0 or info.shape[0] > 1) return false;
        } else if (std.mem.eql(u8, info.name, "use_cache_branch")) {
            if (info.dtype != .bool_ or info.shape.len > 1 or (info.shape.len == 1 and info.shape[0] != 1)) return false;
            branch = true;
        } else if (kv.pastInputSuffix(info.name)) |suffix| {
            if (!validCacheSuffix(suffix) or info.dtype != .f32 or info.shape.len != 4 or info.shape[0] == 0 or info.shape[0] > 1 or info.shape[1] <= 0 or info.shape[2] >= 0 or info.shape[3] <= 0) return false;
            const present = for (outputs) |output| {
                if (kv.presentNameMatchesPastInput(info.name, output.name)) break output;
            } else return false;
            if (present.shape.len != 4 or
                (present.shape[1] >= 0 and present.shape[1] != info.shape[1]) or
                (present.shape[3] >= 0 and present.shape[3] != info.shape[3])) return false;
            const cross = std.mem.indexOf(u8, suffix, ".encoder.") != null;
            if (!cross and present.shape[2] >= 0) return false;
            past_count += 1;
        } else return false;
    }
    var present_count: usize = 0;
    var logits = false;
    for (outputs, 0..) |info, index| {
        for (outputs[0..index]) |prior| if (std.mem.eql(u8, prior.name, info.name)) return false;
        if (std.mem.eql(u8, info.name, "logits")) {
            if (info.dtype != .f32 or info.shape.len != 3 or info.shape[0] == 0 or info.shape[0] > 1 or
                info.shape[1] >= 0 or (info.shape[2] >= 0 and info.shape[2] != vocab)) return false;
            logits = true;
        } else if (kv.presentOutputSuffix(info.name)) |suffix| {
            if (!validCacheSuffix(suffix) or info.dtype != .f32 or info.shape.len != 4 or info.shape[0] == 0 or info.shape[0] > 1) return false;
            present_count += 1;
        } else return false;
    }
    return ids and encoder and branch and logits and past_count > 0 and past_count == present_count;
}

fn validCacheSuffix(suffix: []const u8) bool {
    const dot = std.mem.indexOfScalar(u8, suffix, '.') orelse return false;
    _ = std.fmt.parseInt(usize, suffix[0..dot], 10) catch return false;
    for ([_][]const u8{ ".decoder.key", ".decoder.value", ".encoder.key", ".encoder.value" }) |ending|
        if (std.mem.eql(u8, suffix[dot..], ending)) return true;
    return false;
}

fn cacheBound(session: backends.Session, batch: usize, tokens: usize, context: usize) !usize {
    var total: usize = 0;
    for (session.inputInfo()) |info| {
        const suffix = kv.pastInputSuffix(info.name) orelse continue;
        const time = if (std.mem.indexOf(u8, suffix, ".encoder.") != null) context else tokens;
        const rows = try std.math.mul(usize, batch, time);
        const features = try std.math.mul(usize, @intCast(info.shape[1]), @intCast(info.shape[3]));
        const bytes = try std.math.mul(usize, try std.math.mul(usize, rows, features), info.dtype.byteSize());
        total = try std.math.add(usize, total, try std.math.add(usize, bytes, 4 * @sizeOf(i64)));
    }
    return total;
}

/// Request admission accounts for both previous and replacement K/V. Move
/// the retained inputs to the KV category without charging the same host
/// payload again as temporary ingestion storage.
pub fn estimateAdmission(session: backends.Session, admission: @import("../backends/session.zig").RunAdmission, base: @import("../backends/session.zig").RunRequest, batch: usize, tokens: usize, context: usize, vocabulary: usize) !@import("../runtime/tier/memory.zig").AdmissionAmounts {
    if (!qualified(session, vocabulary)) return admission.estimateRequest(base, session.outputInfo());
    const cache_bytes = try cacheBound(session, batch, tokens, context);
    const logit_bytes = try std.math.mul(usize, vocabulary, @sizeOf(f32));
    const per_item = try std.math.add(usize, logit_bytes, 3 * @sizeOf(i64));
    var request = base;
    request.input_bytes = try std.math.add(usize, request.input_bytes, cache_bytes);
    request.output_kv_bytes = cache_bytes;
    request.output_bytes = try std.math.add(usize, cache_bytes, try std.math.mul(usize, batch, per_item));
    var peak = try admission.estimateRequest(request, session.outputInfo());
    peak.host_scratch_bytes = std.math.sub(usize, peak.host_scratch_bytes, cache_bytes) catch return error.InvalidSeq2SeqAdmission;
    peak.host_kv_bytes = try std.math.add(usize, peak.host_kv_bytes, cache_bytes);
    return peak;
}

const InputRole = union(enum) {
    tokens,
    encoder,
    mask,
    cache_branch,
    past: struct { output_index: usize, empty_shape: [4]i64, cross: bool },
};

const ResidentSlot = struct {
    pin: sessions.Session.ModelLifetime.ResourcePin,
    input: sessions.ResidentInput,
    shape: [4]i64,
    independent: bool = false,
};

pub const State = struct {
    allocator: std.mem.Allocator,
    session: backends.Session,
    roles: []InputRole,
    resident_slots: ?[]?ResidentSlot = null,
    stage_inputs: []sessions.StageInput = &.{},
    logits_index: usize,
    /// Encoder buffers and mask are pinned by the enclosing generation request.
    encoder: backends.Tensor,
    mask: []const i64,
    position: usize = 0,
    vocab_size: usize,

    pub fn init(allocator: std.mem.Allocator, session: backends.Session, encoder: backends.Tensor, mask: []const i64, vocab_size: usize) !?State {
        if (!qualified(session, vocab_size)) return null;
        const input_info = session.inputInfo();
        const output_info = session.outputInfo();
        const roles = try allocator.alloc(InputRole, input_info.len);
        errdefer allocator.free(roles);
        for (input_info, roles) |info, *role| {
            role.* = if (kv.pastInputSuffix(info.name)) |suffix| cache: {
                for (output_info, 0..) |output, index| {
                    if (kv.presentNameMatchesPastInput(info.name, output.name)) break :cache .{ .past = .{
                        .output_index = index,
                        .empty_shape = .{ 1, info.shape[1], 0, info.shape[3] },
                        .cross = std.mem.indexOf(u8, suffix, ".encoder.") != null,
                    } };
                }
                unreachable; // The immutable qualified signature has an exact match.
            } else if (std.mem.eql(u8, info.name, "input_ids")) .tokens else if (std.mem.eql(u8, info.name, "encoder_hidden_states")) .encoder else if (std.mem.eql(u8, info.name, "encoder_attention_mask")) .mask else .cache_branch;
        }
        var logits_index: usize = 0;
        for (output_info, 0..) |output, index| if (std.mem.eql(u8, output.name, "logits")) {
            logits_index = index;
            break;
        };
        var planned = session;
        planned.cached_decoder_geometry = .{ .vocab_size = vocab_size };
        planned.broadcast_inputs = &.{"use_cache_branch"};
        return .{
            .allocator = allocator,
            .session = planned,
            .roles = roles,
            .logits_index = logits_index,
            .encoder = encoder.borrowedView("encoder_hidden_states"),
            .mask = mask,
            .vocab_size = vocab_size,
        };
    }

    pub fn deinit(self: *State) void {
        if (self.resident_slots) |slots| {
            for (slots) |slot| if (slot) |owned| owned.pin.release();
            self.allocator.free(slots);
            self.allocator.free(self.stage_inputs);
        }
        self.allocator.free(self.roles);
    }

    fn borrow(self: *const State, info: backends.TensorInfo, shape: []const i64, data: []const u8) backends.Tensor {
        return .{ .data = @constCast(data), .shape = shape, .dtype = info.dtype, .name = info.name, .allocator = self.allocator, .owns_data = false, .owns_shape = false };
    }

    /// Each cache binding retains its producer independently. Omitted cross
    /// outputs keep their previous pin without retaining a chain of results.
    pub fn stepResident(self: *State, suffix: Suffix, control: ?Control) ![]f32 {
        return self.stepResidentDispatched(suffix, control, null);
    }

    pub fn stepResidentDispatched(self: *State, suffix: Suffix, control: ?Control, dispatch: ?tensor_dispatch.Dispatch) ![]f32 {
        const end = try suffix.endAt(self.position);
        const fresh = self.position == 0;
        if (self.resident_slots == null) {
            const slots = try self.allocator.alloc(?ResidentSlot, try std.math.mul(usize, 2, self.roles.len));
            errdefer self.allocator.free(slots);
            self.stage_inputs = try self.allocator.alloc(sessions.StageInput, self.roles.len);
            @memset(slots, null);
            self.resident_slots = slots;
        }
        const storage = self.resident_slots.?;
        const slots = storage[0..self.roles.len];
        const updated = storage[self.roles.len..];
        const token_shape = [_]i64{ 1, @intCast(suffix.tokens.len) };
        const mask_shape = [_]i64{ 1, @intCast(self.mask.len) };
        const branch = [_]u8{@intFromBool(!fresh)};
        for (self.session.inputInfo(), self.roles, slots, self.stage_inputs) |info, *role, *slot, *binding| {
            binding.* = switch (role.*) {
                .tokens => .{ .host = self.borrow(info, &token_shape, std.mem.sliceAsBytes(suffix.tokens)) },
                .encoder => .{ .host = self.encoder },
                .mask => .{ .host = self.borrow(info, &mask_shape, std.mem.sliceAsBytes(self.mask)) },
                .cache_branch => .{ .host = self.borrow(info, info.shape, &branch) },
                .past => |*past| if (slot.*) |*owned| resident: {
                    var input = owned.input;
                    input.shape = &owned.shape;
                    break :resident .{ .resident = input };
                } else .{ .host = self.borrow(info, &past.empty_shape, &.{}) },
            };
        }
        var outputs = if (dispatch) |selected|
            try selected.run_stage_fn.?(selected.ptr, selected.task, self.allocator, self.session, self.stage_inputs, control)
        else
            (try self.session.runStageInputsWithControl(self.stage_inputs, self.allocator, control)) orelse return error.UnexpectedResidentStageDecline;
        defer outputs.deinit();
        if (outputs.outputs.len != self.session.outputInfo().len) return error.InvalidDecoderOutput;
        defer @memset(updated, null);
        errdefer for (updated) |slot| if (slot) |owned| owned.pin.release();
        for (self.roles, updated) |role, *slot| switch (role) {
            .past => |past| {
                const value = outputs.outputs[past.output_index];
                if (try outputs.backend.tensorDType(value) != .f32) return error.InvalidDecoderOutput;
                const shape = try outputs.backend.tensorShape(value, self.allocator);
                defer self.allocator.free(shape);
                if (shape.len != 4) return error.InvalidDecoderOutput;
                const elements = try cacheElements(shape);
                if (!fresh and past.cross and elements == 0) continue;
                const want = [_]i64{ 1, past.empty_shape[1], @intCast(if (past.cross) self.mask.len else end), past.empty_shape[3] };
                if (!std.mem.eql(i64, shape, &want)) return error.InvalidDecoderOutput;
                const bytes = try std.math.mul(usize, elements, @sizeOf(f32));
                slot.* = .{
                    .pin = try outputs.retainResources(self.allocator),
                    .input = .{ .value = value, .backend = outputs.backend, .estimated_bytes = bytes },
                    .shape = want,
                };
            },
            else => {},
        };
        const row = try @import("encoder_decoder_runtime.zig").residentVocabularyRow(self.allocator, self.session, &outputs, self.logits_index, suffix.tokens.len, self.vocab_size, control);
        errdefer self.allocator.free(row);
        for (self.roles, updated) |role, *slot| switch (role) {
            .past => |past| if (!past.cross) {
                const provisional = slot.* orelse return error.InvalidDecoderOutput;
                var aliases: usize = 0;
                for (outputs.outputs) |value| {
                    if (value == provisional.input.value) aliases += 1;
                }
                if (aliases != 1) continue;
                const adopted = self.adoptSelfCache(provisional) catch |err| switch (err) {
                    error.ResourceLimitExceeded, error.ResourceTemporarilyUnavailable => null,
                    else => return err,
                };
                if (adopted) |independent| {
                    // The new lease owns this backing. Keep the output vector
                    // in signature order but remove this CT from the original
                    // invocation's retirement inventory. Its destruction
                    // already deduplicates repeated handles.
                    outputs.outputs[past.output_index] = outputs.outputs[self.logits_index];
                    outputs.backend.free(provisional.input.value);
                    provisional.pin.release();
                    slot.* = independent;
                }
            },
            else => {},
        };
        // The logits readback has finished the invocation's numerical use.
        // An omitted cross column can now be copied outside that frame.
        if (!fresh) for (self.roles, slots, updated) |role, old, *slot| switch (role) {
            .past => |past| if (past.cross and slot.* == null) {
                const previous = old orelse return error.InvalidDecoderOutput;
                if (!previous.independent) slot.* = self.copyCrossCache(previous, control) catch |err| switch (err) {
                    error.ResourceLimitExceeded, error.ResourceTemporarilyUnavailable, error.QueueFull, error.InvalidPhysicalTransferSize => null,
                    else => return err,
                };
            },
            else => {},
        };
        if (control) |active| try active.check();
        for (slots, updated) |*old, new| if (new) |value| {
            if (old.*) |previous| previous.pin.release();
            old.* = value;
        };
        self.position = end;
        return row;
    }

    fn adoptSelfCache(self: *State, source: ResidentSlot) !?ResidentSlot {
        const shared = self.session.shared_compute_context orelse return null;
        const backend = shared.stableBackend();
        if (self.session.model_lifetime == null or source.input.backend.ptr != backend.ptr or source.input.backend.vtable != backend.vtable) return null;
        const kernels = backend.vtable.kernels orelse return null;
        if (!kernels.accepts(backend.vtable)) return null;
        const retain = kernels.retainImmutableTensor orelse return null;
        const pooled = if (kernels.pooledOutputStorage) |storage| storage(backend.ptr, source.input.value) else null;
        var admission = if (pooled) |lease| blk: {
            if (lease.backend.ptr != backend.ptr or lease.backend.vtable != backend.vtable or
                lease.usage != .ordinary or lease.accounting_class != .activation or lease.isReadOnly() or
                lease.byte_size < source.input.estimated_bytes) return null;
            const backing_bytes = lease.backing_byte_size orelse lease.byte_size;
            if (lease.admission) |charged| {
                const expected = self.session.run_admission orelse return null;
                if (!charged.identity.eql(.{ .controller = expected.controller, .backend_class = expected.backend_class }) or
                    lease.admittedBytes() < backing_bytes) return null;
                break :blk null;
            }
            // Raw CPU sessions can publish an uncharged frame view. The
            // retained-state lease covers the whole synchronous CPU backing.
            if (backend.kind() != .native or lease.isQuarantined()) return null;
            break :blk try self.session.admitRetainedState(0, backing_bytes);
        } else blk: {
            const measure = kernels.standaloneOutputBytes orelse return null;
            const bytes = measure(backend.ptr, source.input.value) orelse return null;
            if (bytes < source.input.estimated_bytes) return null;
            break :blk try self.session.admitRetainedState(0, bytes);
        };
        defer if (admission) |*reservation| reservation.deinit();
        const handles = try self.allocator.alloc(@import("../ops/ops.zig").CT, 1);
        var populated = false;
        var owned: sessions.ResidentOutputs = .{ .outputs = handles, .backend = backend, .allocator = self.allocator };
        defer if (populated) owned.deinit() else self.allocator.free(handles);
        handles[0] = (try retain(backend.ptr, source.input.value)) orelse return null;
        populated = true;
        self.session.pinResident(&owned);
        owned.physical_backing_requires_epoch = if (pooled) |lease| lease.retirement_owner != null else false;
        // Physical banks carry their own charge through their final retained
        // view. Standalone allocations need a separate retained-state charge.
        if (admission) |*reservation| {
            owned.resource_lease = reservation.lease;
            reservation.lease = null;
        }
        var result = source;
        result.pin = try owned.retainResources(self.allocator);
        result.input.value = handles[0];
        result.input.backend = backend;
        result.independent = true;
        return result;
    }

    /// Move a cross cache off its original invocation once omission shows
    /// that it could keep that invocation alive for the rest of the request.
    fn copyCrossCache(self: *State, previous: ResidentSlot, control: ?Control) !?ResidentSlot {
        const storage = @import("../ops/physical_storage.zig");
        const operation = @import("backend_operation.zig");
        const shared = self.session.shared_compute_context orelse return null;
        const backend = shared.stableBackend();
        if (self.session.model_lifetime == null or previous.input.backend.ptr != backend.ptr or previous.input.backend.vtable != backend.vtable) return null;
        const kernels = backend.vtable.kernels orelse return null;
        if (!kernels.accepts(backend.vtable) or kernels.physical_storage_policy == null) return null;
        const view = kernels.physicalTensorView orelse return null;
        const copy = kernels.copyPhysicalInput orelse return null;
        if (backend.kind() != .native and backend.kind() != .metal and kernels.createPhysicalCompletion == null) return null;
        if (control) |active| try active.check();
        const bytes = previous.input.estimated_bytes;
        var dimensions: [4]i32 = undefined;
        for (&dimensions, previous.shape) |*dim, axis| dim.* = std.math.cast(i32, axis) orelse return null;
        const bound = try storage.reservationBytes(backend.*, .{ .byte_size = bytes });
        var admission = try self.session.admitRetainedState(0, bound);
        defer admission.deinit();
        var protection = if (control) |active| try active.enterUninterruptible(self.session.interruption()) else null;
        defer if (protection) |*guard| guard.deinit();

        const handles = try self.allocator.alloc(@import("../ops/ops.zig").CT, 1);
        var populated = false;
        var copied: sessions.ResidentOutputs = .{ .outputs = handles, .backend = backend, .allocator = self.allocator };
        defer if (populated) copied.deinit() else self.allocator.free(handles);
        {
            var scope = try operation.enter(backend.*, null, backend.operationToken(), .legacy_unknown_frame);
            defer scope.deinitChecked() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            // A standalone Metal blit completes synchronously. An open
            // frame would require that frame's submission and retirement.
            if (backend.kind() == .metal and (backend.decoderRuntimeHasActiveFrame() or backend.decoderRuntimeHasSubmittedFrame())) return null;
            const completion = if (kernels.createPhysicalCompletion) |create| try create(backend.ptr) else null;
            defer if (completion) |event| event.deinit();
            const allocation = (try storage.Lease.create(self.allocator, backend.*, bytes)) orelse return null;
            defer allocation.release();
            if ((allocation.backing_byte_size orelse return null) > bound) return error.PhysicalStorageExceededReservation;
            handles[0] = (try view(backend.ptr, .{ .storage = allocation, .shape = &dimensions })) orelse return null;
            populated = true;
            self.session.pinResident(&copied);
            copied.resource_lease = admission.lease;
            admission.lease = null;
            // A callback error can occur after an asynchronous submission.
            // Complete the copy before releasing either source or destination.
            {
                defer if (completion) |event| {
                    event.record() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
                    event.wait() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
                };
                try copy(backend.ptr, handles[0], previous.input.value);
            }
            try scope.finish();
        }
        if (control) |active| try active.check();
        return .{
            .pin = try copied.retainResources(self.allocator),
            .input = .{ .value = handles[0], .backend = backend, .estimated_bytes = bytes },
            .shape = previous.shape,
            .independent = true,
        };
    }

    fn cacheElements(shape: []const i64) !usize {
        var count: usize = 1;
        for (shape) |dim| {
            if (dim < 0) return error.InvalidDecoderOutput;
            count = try std.math.mul(usize, count, @intCast(dim));
        }
        return count;
    }
};

const fixture_inputs = [_]backends.TensorInfo{
    .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
    .{ .name = "encoder_hidden_states", .dtype = .f32, .shape = &.{ -1, -1, 2 } },
    .{ .name = "use_cache_branch", .dtype = .bool_, .shape = &.{1} },
    .{ .name = "past_key_values.0.decoder.key", .dtype = .f32, .shape = &.{ -1, 1, -1, 2 } },
    .{ .name = "past_key_values.0.decoder.value", .dtype = .f32, .shape = &.{ -1, 1, -1, 2 } },
    .{ .name = "past_key_values.0.encoder.key", .dtype = .f32, .shape = &.{ -1, 1, -1, 2 } },
    .{ .name = "past_key_values.0.encoder.value", .dtype = .f32, .shape = &.{ -1, 1, -1, 2 } },
};
const fixture_outputs = [_]backends.TensorInfo{
    .{ .name = "logits", .dtype = .f32, .shape = &.{ -1, -1, 4 } },
    .{ .name = "present.0.decoder.key", .dtype = .f32, .shape = &.{ -1, 1, -1, 2 } },
    .{ .name = "present.0.decoder.value", .dtype = .f32, .shape = &.{ -1, 1, -1, 2 } },
    .{ .name = "present.0.encoder.key", .dtype = .f32, .shape = &.{ -1, 1, -1, 2 } },
    .{ .name = "present.0.encoder.value", .dtype = .f32, .shape = &.{ -1, 1, -1, 2 } },
};

test "incremental seq2seq executes imported ONNX with empty and populated cache lengths" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try checkImportedCacheHandoff(.native, false);
}

test "incremental seq2seq imported ONNX Metal hands off resident cache" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    try checkImportedCacheHandoff(.metal, false);
    try checkImportedCacheHandoff(.metal, true);
}

test "incremental seq2seq imported ONNX CUDA hands off resident cache" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    checkImportedCacheHandoff(.cuda, false) catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
}

fn checkImportedCacheHandoff(backend: backends.BackendType, require_independent: bool) !void {
    const a = std.testing.allocator;
    const ml = @import("ml");
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const ids = try b.parameter("input_ids", ml.graph.Shape.init(.i64, &.{ 1, -1 }));
    const encoder = try b.parameter("encoder_hidden_states", ml.graph.Shape.init(.f32, &.{ 1, -1, 4 }));
    const branch = try b.parameter("use_cache_branch", ml.graph.Shape.init(.bool_, &.{1}));
    const past = try b.parameter("past_key_values.0.decoder.key", ml.graph.Shape.init(.f32, &.{ 1, 1, -1, 4 }));
    const token = try b.reduceSum(try b.convertDtype(ids, .f32), &.{ 0, 1 });
    const cached = try b.reduceSum(try b.convertDtype(branch, .f32), &.{0});
    const logits = try b.add(encoder, try b.add(token, cached));
    const current = try b.reshape(logits, ml.graph.Shape.init(.f32, &.{ 1, 1, 1, 4 }));
    const present = try b.concat(past, current, 2);
    // The fixture exports a growing cache axis. The generic builder adds
    // dimension sentinels arithmetically and would otherwise declare zero.
    graph.nodes.items[present].output_shape.dims[2] = -1;
    try graph.markOutput(logits);
    try graph.markOutput(present);
    const model = try @import("onnx_graph").exportGraph(a, &graph, .{ .node_name_overrides = &.{
        .{ .node_id = logits, .name = "logits" },
        .{ .node_id = present, .name = "present.0.decoder.key" },
    } });
    defer a.free(model);
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    try directory.dir.writeFile(std.testing.io, .{ .sub_path = "merged.onnx", .data = model });
    const path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", directory.sub_path[0..], "merged.onnx" });
    defer a.free(path);
    const memory = @import("../runtime/tier/memory.zig");
    var controller = memory.AdmissionController{};
    defer std.debug.assert(std.meta.eql(memory.AdmissionAmounts{}, controller.snapshot()));
    var session = try @import("../backends/imported_onnx_session.zig").createSessionWithOptions(a, path, backend, .{
        .graph_runtime_strategy = if (require_independent) .interpreter else null,
    });
    session.run_admission = .{ .controller = &controller, .backend_class = if (backend == .native) .cpu else .gpu, .limits = .{}, .opaque_workspace_bytes = if (backend == .native) 4096 else 0, .static_workspace_bytes = 1, .check_live_memory = false };
    var diagnostics = @import("diagnostics.zig").Diagnostics{};
    diagnostics.configure(true);
    session.invocation_diagnostics = .{ .diagnostics = &diagnostics };
    defer session.close();
    var hidden = try backends.Tensor.initFloat32(a, "encoder_hidden_states", &.{ 1, 1, 4 }, &.{ 1, 2, 3, 4 });
    defer hidden.deinit();
    var values = [_]backends.Tensor{hidden};
    var encoded = @import("encoder_decoder_runtime.zig").EncoderOutputs{ .host = &values };
    var decoder = try @import("decoder_stage.zig").Decoder.init(a, session, &encoded, 1, &.{1}, 4, .{ .native_call_policy = .in_process });
    defer decoder.deinit();
    const route = diagnostics.snapshot().?.report.?.decoder_route.?;
    try std.testing.expectEqual(.merged_incremental, route.selected);
    try std.testing.expectEqual(.merged_resident, route.cache_custody);
    if (backend == .native) {
        const state = if (decoder.incremental) |*value| value else return error.InvalidMergedDecoderFixture;
        var restricted = state.*;
        restricted.session.run_admission.?.limits.kv_limit_bytes = 1;
        defer if (restricted.resident_slots) |slots| {
            a.free(slots);
            a.free(restricted.stage_inputs);
        };
        try std.testing.expectError(error.ResourceLimitExceeded, restricted.stepResident(.{ .position = 0, .tokens = &.{2} }, .{ .native_call_policy = .in_process }));
        try std.testing.expectEqual(@as(usize, 0), restricted.position);
    }
    var first = try decoder.step(&.{2}, 0, null, null);
    defer first.deinit();
    try std.testing.expectEqualSlices(f32, &.{ 3, 4, 5, 6 }, first.logits);
    try std.testing.expectError(error.InvalidDecoderPosition, decoder.stepSuffix(.{ .position = 0, .tokens = &.{5} }, null, null));
    if (backend == .native) {
        const Cancel = struct {
            canceled: bool = false,
            fn check(raw: ?*anyopaque) bool {
                const self: *@This() = @ptrCast(@alignCast(raw.?));
                return self.canceled;
            }
            fn run(raw: *anyopaque, _: tensor_dispatch.Task, allocator: std.mem.Allocator, target: backends.Session, inputs: []const sessions.StageInput, control: ?Control) !sessions.ResidentOutputs {
                const self: *@This() = @ptrCast(@alignCast(raw));
                const output = (try target.runStageInputsWithControl(inputs, allocator, control)) orelse return error.MissingResidentResult;
                self.canceled = true;
                return output;
            }
            fn unexpected(_: *anyopaque, _: tensor_dispatch.Task, _: std.mem.Allocator, _: backends.Session, _: ?*sessions.RunPermit, _: ?*std.atomic.Mutex, _: []const backends.Tensor, _: ?Control) ![]backends.Tensor {
                return error.UnexpectedHostDecoderCall;
            }
        };
        var probe: Cancel = .{};
        const state = if (decoder.incremental) |*value| value else return error.InvalidMergedDecoderFixture;
        try std.testing.expectError(error.Cancelled, state.stepResidentDispatched(.{ .position = 1, .tokens = &.{5} }, .{ .native_call_policy = .in_process, .cancellation = .{ .ptr = &probe, .is_cancelled_fn = Cancel.check } }, .{ .ptr = &probe, .task = .rewrite, .run_fn = Cancel.unexpected, .run_stage_fn = Cancel.run }));
        try std.testing.expectEqual(@as(usize, 1), state.position);
    }
    var next = try decoder.stepSuffix(.{ .position = 1, .tokens = &.{5} }, null, null);
    defer next.deinit();
    try std.testing.expectEqualSlices(f32, &.{ 7, 8, 9, 10 }, next.logits);
    const state = if (decoder.incremental) |*value| value else return error.InvalidMergedDecoderFixture;
    const expected_cache: []const f32 = &.{ 3, 4, 5, 6, 7, 8, 9, 10 };
    if (backend == .native) {
        const slots = state.resident_slots orelse return error.MissingResidentResult;
        const cpu_cache = for (slots[0..state.roles.len]) |slot| {
            if (slot) |value| break value;
        } else return error.MissingResidentResult;
        if (!cpu_cache.independent) return error.MissingIndependentCpuCache;
        const actual = try cpu_cache.input.backend.toFloat32(cpu_cache.input.value, a);
        defer a.free(actual);
        try std.testing.expectEqualSlices(f32, expected_cache, actual);
        const kernels = cpu_cache.input.backend.vtable.kernels.?;
        const pooled = kernels.pooledOutputStorage.?(cpu_cache.input.backend.ptr, cpu_cache.input.value);
        const accounting = controller.snapshot();
        if (pooled) |lease| {
            if (lease.admission != null) {
                try std.testing.expect(lease.admittedBytes() >= cpu_cache.input.estimated_bytes);
                try std.testing.expect(accounting.host_scratch_bytes >= lease.admittedBytes());
            } else try std.testing.expect(accounting.host_kv_bytes >= cpu_cache.input.estimated_bytes);
        } else try std.testing.expect(accounting.host_kv_bytes >= cpu_cache.input.estimated_bytes);
    } else {
        try std.testing.expect(controller.snapshot().backendTotalBytes() > 0);
        const slots = state.resident_slots orelse return error.MissingResidentResult;
        for (slots[0..state.roles.len]) |*slot| {
            if (slot.*) |owned| {
                if (require_independent) {
                    try std.testing.expect(owned.independent);
                    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
                }
                const hooks = owned.input.backend.vtable.kernels.?;
                if (hooks.pooledOutputStorage) |query| {
                    if (query(owned.input.backend.ptr, owned.input.value)) |lease| {
                        if (lease.admission != null) {
                            try std.testing.expect(owned.independent);
                            try std.testing.expect(lease.admittedBytes() >= owned.input.estimated_bytes);
                        }
                    }
                }
                const cache = try owned.input.backend.toFloat32(owned.input.value, a);
                defer a.free(cache);
                try std.testing.expectEqualSlices(f32, expected_cache, cache);
                // The graph may publish a frame view. Exercise standalone
                // adoption separately with a provider-owned allocation.
                const shared = state.session.shared_compute_context.?;
                var standalone = try backends.Tensor.initFloat32(a, "present.0.decoder.key", &owned.shape, expected_cache);
                defer standalone.deinit();
                const original = try shared.importHostTensor(a, &standalone);
                var original_live = true;
                defer if (original_live) shared.stableBackend().free(original);
                var standalone_input = owned;
                standalone_input.input.value = original;
                standalone_input.input.backend = shared.stableBackend();
                const before_adopt = controller.snapshot();
                const adopted = (try state.adoptSelfCache(standalone_input)) orelse return error.MissingIndependentSelfCache;
                defer adopted.pin.release();
                try std.testing.expect(adopted.independent);
                try std.testing.expect(controller.snapshot().backend_kv_bytes >= before_adopt.backend_kv_bytes + owned.input.estimated_bytes);
                shared.stableBackend().free(original);
                original_live = false;
                const adopted_cache = try adopted.input.backend.toFloat32(adopted.input.value, a);
                defer a.free(adopted_cache);
                try std.testing.expectEqualSlices(f32, expected_cache, adopted_cache);
                try checkPooledSelfAdoption(state, owned, &controller, expected_cache);
                const bound = try @import("../ops/physical_storage.zig").reservationBytes(owned.input.backend.*, .{ .byte_size = owned.input.estimated_bytes });
                var below = state.*;
                below.session.run_admission.?.limits.kv_limit_bytes = bound - 1;
                const before_denial = controller.snapshot();
                const copy_control: Control = .{ .native_call_policy = .in_process };
                try std.testing.expectError(error.ResourceLimitExceeded, below.copyCrossCache(owned, copy_control));
                try std.testing.expectEqualDeep(before_denial, controller.snapshot());
                const detached = (try state.copyCrossCache(owned, copy_control)) orelse return error.MissingIndependentResidentCache;
                defer detached.pin.release();
                try std.testing.expect(detached.independent);
                try std.testing.expect(controller.snapshot().backend_kv_bytes >= owned.input.estimated_bytes);
                slot.* = null;
                owned.pin.release();
                try std.testing.expect(controller.snapshot().backend_kv_bytes >= bound + owned.input.estimated_bytes);
                const independent_cache = try detached.input.backend.toFloat32(detached.input.value, a);
                defer a.free(independent_cache);
                try std.testing.expectEqualSlices(f32, expected_cache, independent_cache);
                break;
            }
        } else return error.MissingResidentResult;
    }

    // Exercise the same imported graph through mixed resident inputs. Keep
    // both output owners live across the second invocation and read only
    // logits until the final cache comparison.
    const stages = @import("encoder_decoder_runtime.zig");
    const test_control: Control = .{ .native_call_policy = .in_process };
    const prefill_ids = [_]i64{2};
    const decode_ids = [_]i64{5};
    const prefill_branch = [_]u8{0};
    const decode_branch = [_]u8{1};
    const token_shape = [_]i64{ 1, 1 };
    const cache_shape = [_]i64{ 1, 1, 1, 4 };
    var bindings: [4]sessions.StageInput = undefined;
    for (state.session.inputInfo(), state.roles, &bindings) |info, *role, *binding| {
        binding.* = .{ .host = switch (role.*) {
            .tokens => state.borrow(info, &token_shape, std.mem.sliceAsBytes(&prefill_ids)),
            .encoder => hidden,
            .cache_branch => state.borrow(info, info.shape, &prefill_branch),
            .past => |*cache| state.borrow(info, &cache.empty_shape, &.{}),
            .mask => return error.InvalidMergedDecoderFixture,
        } };
    }
    var prefill = (try state.session.runStageInputsWithControl(&bindings, a, test_control)) orelse return error.MissingResidentResult;
    defer prefill.deinit();
    const prefill_logits = try stages.residentVocabularyRow(a, state.session, &prefill, state.logits_index, 1, 4, test_control);
    defer a.free(prefill_logits);
    try std.testing.expectEqualSlices(f32, first.logits, prefill_logits);
    for (state.session.inputInfo(), state.roles, &bindings) |info, role, *binding| switch (role) {
        .tokens => binding.* = .{ .host = state.borrow(info, &token_shape, std.mem.sliceAsBytes(&decode_ids)) },
        .cache_branch => binding.* = .{ .host = state.borrow(info, info.shape, &decode_branch) },
        .past => |cache| binding.* = .{ .resident = .{ .value = prefill.outputs[cache.output_index], .backend = prefill.backend, .shape = &cache_shape, .estimated_bytes = 4 * @sizeOf(f32) } },
        else => {},
    };
    var decoded = (try state.session.runStageInputsWithControl(&bindings, a, test_control)) orelse return error.MissingResidentResult;
    defer decoded.deinit();
    const decode_logits = try stages.residentVocabularyRow(a, state.session, &decoded, state.logits_index, 1, 4, test_control);
    defer a.free(decode_logits);
    try std.testing.expectEqualSlices(f32, next.logits, decode_logits);
    const final_cache = try decoded.backend.toFloat32(decoded.outputs[1], a);
    defer a.free(final_cache);
    try std.testing.expectEqualSlices(f32, expected_cache, final_cache);
}

fn checkPooledSelfAdoption(state: *State, previous: ResidentSlot, controller: *@import("../runtime/tier/memory.zig").AdmissionController, expected: []const f32) !void {
    const storage = @import("../ops/physical_storage.zig");
    const admission = @import("storage_admission.zig");
    const memory = @import("../runtime/tier/memory.zig");
    const Reserve = struct {
        controller: *memory.AdmissionController,
        run: memory.AdmissionLease,

        fn reserve(raw: *anyopaque, allocator: std.mem.Allocator, kind: @import("../graph/backend_contracts.zig").BackendKind, class: @import("storage_accounting.zig").Class, bytes: usize) !admission.Reservation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return admission.fromRunLease(allocator, &self.run, .{
                .controller = self.controller,
                .backend_class = .gpu,
                .limits = .{},
                .check_live_memory = false,
            }, kind, class, bytes);
        }
    };
    var reserve: Reserve = .{ .controller = controller, .run = try controller.tryAcquire(.gpu, .{}, .{}, false) };
    defer reserve.run.release();
    const backend = previous.input.backend;
    const kernels = backend.vtable.kernels.?;
    const bytes = previous.input.estimated_bytes;
    const before = controller.snapshot();
    const adopted = blk: {
        const lease = (try storage.Lease.createAdmitted(state.allocator, backend.*, bytes, null, .activation, .{
            .identity = .{ .controller = controller, .backend_class = .gpu },
            .context = &reserve,
            .services = &.{ .reserve = Reserve.reserve },
        })) orelse return error.MissingPooledCacheBank;
        defer lease.release();
        var shape: [4]i32 = undefined;
        for (&shape, previous.shape) |*dim, size| dim.* = @intCast(size);
        const value = (try kernels.physicalTensorView.?(backend.ptr, .{ .storage = lease, .shape = &shape })) orelse return error.MissingPooledCacheView;
        defer backend.free(value);
        try kernels.writePhysicalFloat32.?(backend.ptr, value, expected);
        const charged = controller.snapshot();
        var source = previous;
        source.input.value = value;
        const result = (try state.adoptSelfCache(source)) orelse return error.MissingIndependentPooledCache;
        errdefer result.pin.release();
        try std.testing.expect(result.independent);
        try std.testing.expectEqualDeep(charged, controller.snapshot());
        break :blk result;
    };
    {
        defer adopted.pin.release();
        try std.testing.expect(controller.snapshot().backend_scratch_bytes > before.backend_scratch_bytes);
        const actual = try backend.toFloat32(adopted.input.value, state.allocator);
        defer state.allocator.free(actual);
        try std.testing.expectEqualSlices(f32, expected, actual);
    }
    try std.testing.expectEqualDeep(before, controller.snapshot());
}

test "incremental seq2seq requires explicit merged ABI" {
    try std.testing.expect(qualifiedSignature(&fixture_inputs, &fixture_outputs, 4));
    try std.testing.expect(!qualifiedSignature(fixture_inputs[3..], &fixture_outputs, 4));
}
