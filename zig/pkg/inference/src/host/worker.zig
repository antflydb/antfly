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

//! Host-owned replaceable inference process. Only wire values cross the pipes.
const std = @import("std");
const builtin = @import("builtin");
const rpc = @import("antfly_inference_worker_rpc");
pub const wire = @import("antfly_inference_worker_wire");
const bridge = @import("antfly_inference_bridge");
const http = @import("antfly_runtime_abi").http_abi;
const host = @import("host.zig");
const platform = @import("antfly_platform");
const worker_lifetime = @import("worker_lifetime.zig");
const worker_heap = @import("worker_heap.zig");
pub const BootstrapOwner = worker_heap.Bootstrap;
pub const heap_module = worker_heap;
const httpx = @import("httpx");
const execution_control = @import("inference_server").execution_control;
const Output = @import("worker_output_error.zig").Output;
const HttpControl = @import("worker_http_control.zig");
const Progress = rpc.Progress;
const ExecutionPhases = @import("worker_execution_phase.zig").For(execution_control, rpc);
pub const EnvironmentEntry = struct { name: []const u8, value: []const u8 };

const ForwardControl = struct {
    alloc: std.mem.Allocator,
    cancellation: http.CancellationView,
    deadline_ns: ?u64 = null,
    progress: bridge.ProgressView = .{},
    stream: http.StreamSink = .{},
    stream_started: bool = false,
    body_source: http.RequestBodySource = .{},
    output: Output = .{},
    phase_handoff: bool = false,
    output_waits_owned: bool = false,
    execution_phase: ExecutionPhases.Parent = .{},

    fn phase(raw: ?*anyopaque, value: rpc.Phase) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        if (!self.phase_handoff) return error.UnsupportedWorkerPhase;
        if (value == .resource_wait) try check(self);
        try self.execution_phase.transition(value);
        if (value == .runnable) try check(self);
    }

    fn finishResourceWait(self: *@This()) !void {
        try self.execution_phase.finish();
    }

    fn beginOutputWait(raw: ?*anyopaque) callconv(.c) http.CallbackStatus {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        phase(raw, .resource_wait) catch |err| return self.output.record(err);
        return .ok;
    }
    fn endOutputWait(raw: ?*anyopaque) callconv(.c) http.CallbackStatus {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        phase(raw, .runnable) catch |err| return self.output.record(err);
        return .ok;
    }

    fn check(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        // Poll original cancellation/deadline only. A scheduler checkpoint can
        // yield this loan while the child is still computing. The child sends
        // acknowledged checkpoints at its own safe boundaries instead.
        try self.execution_phase.poll(self.cancellation, self.deadline_ns, platform.time.monotonicNs());
    }

    fn event(raw: ?*anyopaque, payload: rpc.Payload) !void {
        return eventControlled(raw, payload, .{});
    }
    fn eventControlled(raw: ?*anyopaque, payload: rpc.Payload, event_control: rpc.EventControl) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        var parsed = try std.json.parseFromSlice(wire.Event, self.alloc, payload.metadata, .{});
        defer parsed.deinit();
        const value = parsed.value;
        var control: HttpControl.Parent = .{ .original_ptr = self, .original_check = check, .original_deadline_ns = self.deadline_ns, .event_deadline_ns = value.deadline_ns, .event = event_control };
        const view_control = control.view();
        try control.poll();
        switch (value.kind) {
            .progress => self.progress.update(value.phase, value.completed, value.total, value.model, value.backend),
            .stream_start => {
                const headers = try self.alloc.alloc(http.HeaderView, value.headers.len);
                defer self.alloc.free(headers);
                for (value.headers, headers) |header, *header_view| header_view.* = .{ .name = .init(header.name), .value = .init(header.value) };
                if (control.active()) try self.output.startControlled(self.stream, value.status, .init(payload.body), .{ .ptr = headers.ptr, .len = headers.len }, &view_control) else try self.output.start(self.stream, value.status, .init(payload.body), .{ .ptr = headers.ptr, .len = headers.len });
            },
            .stream_write => {
                if (control.active()) try self.output.writeControlled(self.stream, .init(payload.body), &view_control) else try self.output.write(self.stream, .init(payload.body));
            },
            .stream_close => if (control.active()) try self.output.closeControlled(self.stream, &view_control) else try self.output.close(self.stream),
        }
    }

    fn readBody(raw: ?*anyopaque, dest: []u8) !usize {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        try check(raw);
        var count: usize = 0;
        var control: HttpControl.Parent = .{ .original_ptr = self, .original_check = check, .original_deadline_ns = self.deadline_ns, .event_deadline_ns = null, .event = .{} };
        const view_control = control.view();
        const status = if (self.body_source.read_controlled) |read|
            read(self.body_source.context, dest.ptr, dest.len, &count, &view_control)
        else if (!control.active())
            (self.body_source.read orelse return error.BodySourceUnavailable)(self.body_source.context, dest.ptr, dest.len, &count)
        else
            return error.StreamingUnavailable;
        try callbackResult(status);
        try check(raw);
        if (count > dest.len) return error.BodyReadFailed;
        return count;
    }
    fn view(self: *@This()) rpc.Control {
        return .{ .ptr = self, .check = check, .event = event, .read_body = if (self.body_source.read != null) readBody else null, .event_controlled = eventControlled, .phase = if (self.phase_handoff) phase else null, .fence_on_error = self.phase_handoff, .event_owns_waits = self.output_waits_owned };
    }
};

fn callbackResult(status: http.CallbackStatus) !void {
    const output: Output = .{};
    try output.result(status);
}

pub const ProviderResult = union(enum) {
    json: struct { bytes: []u8, alloc: std.mem.Allocator },
    numeric: struct { rows: [][]f32, kind: @FieldType(bridge.NumericResult, "kind"), alloc: std.mem.Allocator },

    pub fn allocator(self: ProviderResult) std.mem.Allocator {
        return switch (self) {
            .json => |value| value.alloc,
            .numeric => |value| value.alloc,
        };
    }
    pub fn deinit(self: ProviderResult) void {
        switch (self) {
            .json => |value| value.alloc.free(value.bytes),
            .numeric => |value| {
                for (value.rows) |row| value.alloc.free(row);
                value.alloc.free(value.rows);
            },
        }
    }
};

pub fn invokeProvider(client: *Client, context: *const bridge.ProviderInvokeContext) !ProviderResult {
    const result_alloc = try client.resultAllocator();
    var arena = std.heap.ArenaAllocator.init(result_alloc);
    defer arena.deinit();
    var control = ForwardControl{
        .alloc = result_alloc,
        .cancellation = context.cancellation,
        .deadline_ns = if (context.has_deadline != 0) context.deadline_ns else null,
        .progress = context.progress,
        .phase_handoff = context.runnable != null,
    };
    try ForwardControl.check(&control);
    const input = try wire.ProviderInput.init(arena.allocator(), context);
    const data = try std.json.Stringify.valueAlloc(arena.allocator(), input.options, .{});
    const response = client.invokePayload(.provider, data, .{ .body_segments = input.body.segments }, control.view()) catch |err| {
        // A canceled or disconnected child can stop while parked. Reacquire
        // (or preserve its authoritative rejection) before leaving the owning
        // thread-local scope, even when no response was produced.
        try control.finishResourceWait();
        return err;
    };
    defer response.deinit();
    if (control.execution_phase.wait != null) {
        try control.finishResourceWait();
        return error.InvalidWorkerPhase;
    }
    try ForwardControl.check(&control);
    const reply_options = try std.json.parseFromSliceLeaky(wire.Reply, arena.allocator(), response.view().metadata, .{});
    const output = try std.json.parseFromSliceLeaky(wire.ProviderOutput, arena.allocator(), reply_options.options, .{});
    if (output.kind != .absent) {
        if (!input.options.numeric) return error.InvalidInferenceNumericResult;
        return .{ .numeric = .{ .rows = try output.decode(result_alloc, response.view().body), .kind = output.kind, .alloc = result_alloc } };
    }
    if (input.options.numeric) return error.InvalidInferenceNumericResult;
    return .{ .json = .{ .bytes = try result_alloc.dupe(u8, response.view().body), .alloc = result_alloc } };
}

const HttpPayloadOwner = struct {
    payload: ?rpc.OwnedPayload = null,
    alloc: std.mem.Allocator,
    fn release(raw: *anyopaque) void {
        const self: *HttpPayloadOwner = @ptrCast(@alignCast(raw));
        const alloc = self.alloc;
        if (self.payload) |payload| payload.deinit();
        alloc.destroy(self);
    }
};

fn initHttpResponse(alloc: std.mem.Allocator, status: u16) !httpx.Response {
    var response = httpx.Response.init(alloc, status);
    // This allocation retains the budget even if headers are cleared and the
    // body is static. Its allocator remains usable until response destruction.
    const owner = try alloc.create(HttpPayloadOwner);
    owner.* = .{ .alloc = alloc };
    response.retirement = .{ .ptr = owner, .release = HttpPayloadOwner.release };
    return response;
}

/// Consumes a detached transport payload only on success. Its original slab and
/// the response's independent allocator owner survive through physical output.
fn attachHttpPayload(response: *httpx.Response, payload: rpc.OwnedPayload) !void {
    if (payload.endpoint != null or response.body != null) return error.InvalidWorkerPayloadOwnership;
    const owner = if (response.retirement) |retirement| blk: {
        if (retirement.release != HttpPayloadOwner.release) return error.InvalidWorkerPayloadOwnership;
        const current: *HttpPayloadOwner = @ptrCast(@alignCast(retirement.ptr));
        if (current.payload != null) return error.InvalidWorkerPayloadOwnership;
        break :blk current;
    } else blk: {
        const current = try response.allocator.create(HttpPayloadOwner);
        current.* = .{ .alloc = response.allocator };
        response.retirement = .{ .ptr = current, .release = HttpPayloadOwner.release };
        break :blk current;
    };
    owner.payload = payload;
    response.body = payload.view().body;
}

pub fn invokeHttp(client: *Client, route: []const u8, context: *const bridge.HttpHandleContext) !httpx.Response {
    return invokeHttpOwned(client, route, context) catch |err| return if (err == error.OutOfMemory) error.ResourceTemporarilyUnavailable else err;
}

fn invokeHttpOwned(client: *Client, route: []const u8, context: *const bridge.HttpHandleContext) !httpx.Response {
    const result_alloc = try client.resultAllocator();
    var arena = std.heap.ArenaAllocator.init(result_alloc);
    defer arena.deinit();
    const alloc = arena.allocator();
    // Events can be numerous on a long stream: use a freeing allocator rather
    // than retaining every decoded frame in the request arena.
    const cpu_slots = try wire.admittedCpuSlots(context);
    var control = ForwardControl{
        .alloc = result_alloc,
        .cancellation = context.cancellation,
        .stream = context.stream,
        .body_source = context.body_source,
        .deadline_ns = if (context.has_deadline != 0) context.deadline_ns else null,
        .phase_handoff = context.runnable != null,
    };
    defer control.finishResourceWait() catch {};
    try ForwardControl.check(&control);
    if (control.phase_handoff) if (control.stream.set_execution_wait) |install| {
        callbackResult(install(control.stream.context, &.{ .context = &control, .begin = ForwardControl.beginOutputWait, .end = ForwardControl.endOutputWait })) catch |err| {
            // A rejecting sink may have copied part of the borrowed view.
            // Clear it before this stack-owned parent control retires.
            _ = install(control.stream.context, null);
            return err;
        };
        control.output_waits_owned = true;
    };
    defer if (control.output_waits_owned) {
        _ = control.stream.set_execution_wait.?(control.stream.context, null);
    };
    var body = context.request.body;
    const streaming_body = context.body_source.streaming != 0 and context.body_source.read != null and
        std.mem.endsWith(u8, context.request.path.slice(), "/stream") and
        std.mem.indexOf(u8, context.request.path.slice(), "/transcription/sessions/") != null;
    if (!streaming_body and body.slice() == null) if (context.body_source.read_all) |read| {
        if (control.phase_handoff and context.body_source.streaming != 0 and !control.output_waits_owned) return error.UnsupportedWorkerPhase;
        var lifetime: HttpControl.Parent = .{ .original_ptr = &control, .original_check = ForwardControl.check, .original_deadline_ns = control.deadline_ns, .event_deadline_ns = null, .event = .{} };
        const view_control = lifetime.view();
        const status = if (context.body_source.read_all_controlled) |read_controlled|
            read_controlled(context.body_source.context, &body, &view_control)
        else if (control.deadline_ns == null and context.cancellation.is_cancelled == null and !control.phase_handoff)
            read(context.body_source.context, &body)
        else
            return error.StreamingUnavailable;
        try control.output.result(status);
    };
    const headers = try alloc.alloc(wire.Header, context.request.headers_len);
    const source_headers = if (context.request.headers_ptr) |ptr| ptr[0..context.request.headers_len] else &.{};
    for (source_headers, headers) |header, *out| out.* = .{ .name = header.name.slice(), .value = header.value.slice() };
    const params = try alloc.alloc(wire.Header, context.request.params_len);
    const source_params = if (context.request.params_ptr) |ptr| ptr[0..context.request.params_len] else &.{};
    for (source_params, params) |param, *out| out.* = .{ .name = param.name.slice(), .value = param.value.slice() };
    const data = try std.json.Stringify.valueAlloc(alloc, wire.Http{
        .route = route,
        .method = context.request.method,
        .path = context.request.path.slice(),
        .query = context.request.query.slice(),
        .headers = headers,
        .params = params,
        .has_body = body.slice() != null,
        .deadline_ns = control.deadline_ns,
        .admitted_cpu_slots = cpu_slots,
        .phase_handoff = control.phase_handoff,
        .streaming_body = streaming_body,
    }, .{});
    const result = client.invoke(.http, data, body.slice() orelse "", control.view()) catch |err| {
        if (control.output.started) return err;
        if (err == error.BodyTooLarge) {
            var response = try initHttpResponse(result_alloc, 413);
            errdefer response.deinit();
            try response.headers.append("content-type", "application/json");
            response.body = "{\"error\":\"inference request too large\"}";
            return response;
        }
        if (err == error.InferenceWorkerExecutableNotConfigured) {
            var response = try initHttpResponse(result_alloc, 503);
            errdefer response.deinit();
            try response.headers.append("content-type", "application/json");
            response.body = "{\"error\":\"inference worker executable not configured; set ANTFLY_INFERENCE_WORKER to an antfly executable\",\"code\":\"InferenceWorkerExecutableNotConfigured\"}";
            return response;
        }
        if (err == error.InferenceWorkerUnavailable or err == error.ResourceTemporarilyUnavailable or err == error.QueueFull) {
            var response = try initHttpResponse(result_alloc, 503);
            errdefer response.deinit();
            try response.headers.append("content-type", "application/json");
            try response.headers.append("retry-after", "1");
            response.body = if (err == error.InferenceWorkerUnavailable)
                "{\"error\":\"inference worker stopped or its transport failed; retry the request and check worker stderr if this persists\",\"code\":\"InferenceWorkerUnavailable\"}"
            else
                "{\"error\":\"inference capacity temporarily unavailable; retry the request\",\"code\":\"ResourceTemporarilyUnavailable\"}";
            return response;
        }
        return err;
    };
    var result_owned = true;
    defer if (result_owned) result.deinit();
    const result_header = try std.json.parseFromSliceLeaky(wire.Reply, alloc, result.view().metadata, .{});
    const value = try std.json.parseFromSliceLeaky(wire.HttpResponse, alloc, result_header.options, .{});
    var response = try initHttpResponse(result_alloc, value.status);
    errdefer response.deinit();
    for (value.headers) |header| try response.headers.append(header.name, header.value);
    try attachHttpPayload(&response, result);
    result_owned = false;
    return response;
}

pub const Client = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    create_json: []u8,
    environment: std.process.Environ.Map,
    data_dir: ?[]u8 = null,
    payload_budget: ?*rpc.PayloadBudget = null,
    mutex: std.Io.Mutex = .init,
    worker: ?*Worker = null,
    active: usize = 0,
    generation: u64 = 0,
    budget: ?bridge.ResourceBudget = null,
    worker_baseline: u64 = 0,
    configuration_allocator: ?std.mem.Allocator = null,
    bootstrap_allocator: ?std.mem.Allocator = null,

    pub fn create(alloc: std.mem.Allocator, io: std.Io, context: *const bridge.CreateContext) !*Client {
        const parent = context.bootstrap_memory_parent orelse return error.ResourceOwnerNotConfigured;
        const bootstrap = try worker_heap.Bootstrap.create(alloc, parent);
        defer bootstrap.release();
        const capture = bootstrap.allocator();
        var arena = std.heap.ArenaAllocator.init(capture);
        defer arena.deinit();
        const json = try std.json.Stringify.valueAlloc(capture, try wire.Create.fromContext(arena.allocator(), context), .{});
        errdefer capture.free(json);
        // The external node budget arrives in configure. Keep only the launch
        // capsule here; parse/copy its environment after admission is available.
        var environment = std.process.Environ.Map.init(capture);
        errdefer environment.deinit();
        const data_dir = try capture.dupe(u8, context.data_dir_ptr[0..context.data_dir_len]);
        errdefer capture.free(data_dir);
        if (data_dir.len == 0) return error.InvalidArguments;
        const payload_budget = try rpc.PayloadBudget.createWithObjectAllocator(alloc, capture, .{});
        errdefer payload_budget.releaseOwner();
        const self = try capture.create(Client);
        self.* = .{ .alloc = alloc, .io = io, .create_json = json, .environment = environment, .data_dir = data_dir, .payload_budget = payload_budget, .bootstrap_allocator = capture };
        return self;
    }

    pub fn configure(self: *Client, budget: bridge.ResourceBudget) !void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        try self.configureMemory(budget);
        _ = try self.ensureWorker(.{});
    }

    /// Caller serializes configuration. A failed publication remains unable to
    /// start a worker, while deinit still owns every bootstrap/context edge.
    fn configureMemory(self: *Client, budget: bridge.ResourceBudget) !void {
        if (self.budget != null) return error.ExternalResourceBudgetsAlreadyConfigured;
        const adjust = budget.adjust_transient_host orelse return error.UnsupportedVersion;
        const payload_budget = self.payload_budget orelse return error.ResourceOwnerNotConfigured;
        if (budget.retain_context(budget.context) == 0) return error.ResourceOwnerShuttingDown;
        var retained = true;
        defer if (retained) budget.release_context(budget.context);
        try payload_budget.configureExternal(.{ .context = budget.context, .retain = budget.retain_context, .release = budget.release_context, .adjust = adjust });
        self.budget = budget;
        retained = false;
        try self.prepareConfiguration();
    }

    fn prepareConfiguration(self: *Client) !void {
        if (self.configuration_allocator != null) return error.InvalidArgument;
        const alloc = try self.resultAllocator();
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const capsule = try std.json.parseFromSliceLeaky(struct { runtime_config_json: []const u8 }, arena.allocator(), self.create_json, .{ .ignore_unknown_fields = true });
        const settings = try std.json.parseFromSliceLeaky(struct { worker_environment: []const EnvironmentEntry = &.{} }, arena.allocator(), capsule.runtime_config_json, .{ .ignore_unknown_fields = true });
        const json = try alloc.dupe(u8, self.create_json);
        errdefer alloc.free(json);
        const data_dir = try alloc.dupe(u8, self.data_dir orelse return error.InvalidArguments);
        errdefer alloc.free(data_dir);
        var environment = std.process.Environ.Map.init(alloc);
        errdefer environment.deinit();
        // RPC is the child's lifeline; it must not inherit a competing reader.
        for (settings.worker_environment) |entry| {
            if (std.mem.eql(u8, entry.name, platform.inference_process_supervisor.worker_env) or
                std.mem.eql(u8, entry.name, "ANTFLY_INFERENCE_SUPERVISOR_LIFELINE")) continue;
            try environment.put(entry.name, entry.value);
        }
        // Publish only after every fallible allocation. Retire bootstrap copies
        // physically; the configured views use the admitted allocator henceforth.
        const bootstrap_allocator = self.bootstrap_allocator orelse self.alloc;
        bootstrap_allocator.free(self.create_json);
        self.environment.deinit();
        if (self.data_dir) |path| bootstrap_allocator.free(path);
        self.create_json = json;
        self.data_dir = data_dir;
        self.environment = environment;
        self.configuration_allocator = alloc;
    }

    pub fn deinit(self: *Client) void {
        std.debug.assert(self.active == 0);
        if (self.worker) |worker| worker.destroy();
        const configuration_allocator = self.configuration_allocator orelse self.bootstrap_allocator orelse self.alloc;
        configuration_allocator.free(self.create_json);
        self.environment.deinit();
        if (self.data_dir) |path| configuration_allocator.free(path);
        if (self.payload_budget) |budget| budget.releaseOwner();
        const budget = self.budget;
        const object_allocator = self.bootstrap_allocator orelse self.alloc;
        object_allocator.destroy(self);
        if (budget) |owner| owner.release_context(owner.context);
    }

    fn ensureWorker(self: *Client, control: rpc.Control) !*Worker {
        if (control.check) |check| try check(control.ptr);
        if (self.worker) |worker| {
            if (!worker.endpoint.closed.load(.acquire)) return worker;
            if (self.active != 0) return error.InferenceWorkerUnavailable;
            worker.destroy();
            self.worker = null;
        }
        if (self.budget == null or self.configuration_allocator == null) return error.ResourceOwnerNotConfigured;
        try self.reserveWorkerBaseline();
        var baseline_owned = true;
        defer if (baseline_owned) self.releaseWorkerBaseline();
        const worker = try self.alloc.create(Worker);
        errdefer self.alloc.destroy(worker);
        const metadata_budget = try Worker.createMetadataBudget(self.alloc, self.budget.?);
        errdefer metadata_budget.releaseOwner();
        const transport_budget = try Worker.createMetadataBudget(self.alloc, self.budget.?);
        errdefer transport_budget.releaseOwner();
        const executable = try resolveWorkerExecutable(self.alloc, self.io);
        defer self.alloc.free(executable);
        var fence: ?platform.process_generation_fence.Lease = if (platform.process_generation_fence.supported) blk: {
            const path = self.data_dir orelse return error.ResourceOwnerNotConfigured;
            const dir = try std.Io.Dir.cwd().createDirPathOpen(self.io, path, .{});
            defer dir.close(self.io);
            break :blk platform.process_generation_fence.Lease.acquire(self.io, dir, "inference-worker-generation.lock") catch |err| switch (err) {
                error.ProcessGenerationBusy => return error.InferenceWorkerUnavailable,
                else => return err,
            };
        } else null;
        var fence_owned = true;
        defer if (fence_owned) if (fence) |*held| held.close(self.io);
        const inherited = [_]std.Io.File{if (fence) |*held| held.inheritedFile() else undefined};
        const child = std.process.spawn(self.io, .{
            .argv = &.{ executable, "inference", "_worker" },
            .stdin = .pipe,
            .stdout = .pipe,
            .stderr = .inherit,
            .environ_map = &self.environment,
            .inherit_files = if (fence != null) &inherited else &.{},
        }) catch |err| {
            std.log.warn("could not start inference worker executable={s} err={s}; set {s} to an executable antfly binary", .{ executable, @errorName(err), worker_executable_env });
            return switch (err) {
                error.FileNotFound, error.AccessDenied, error.InvalidExe => error.InferenceWorkerExecutableNotConfigured,
                else => err,
            };
        };
        worker.* = .{
            .owner = self,
            .child = child,
            .fence = fence,
            .metadata_budget = metadata_budget,
            .transport_budget = transport_budget,
            .endpoint = .{
                .alloc = self.alloc,
                .io = self.io,
                .input = child.stdout.?,
                .output = child.stdin.?,
                .context = worker,
                .handler = Worker.rejectRequest,
                .resource_handler = Worker.resourceRequest,
                .on_closed = Worker.closed,
                .next_id = 1,
                .payload_budget = self.payload_budget,
                .metadata_allocator = transport_budget.retainedAllocator(),
                .peer_payload_budget = self.payload_budget,
                .compute_allocator = if (self.payload_budget) |budget| budget.retainedAllocator() else null,
            },
        };
        fence_owned = false;
        errdefer worker.releaseFence();
        // The worker owns the child even during partial startup. start() can
        // already reap it through on_closed before returning an error; kill
        // must inspect that same handle, whose ID is then null.
        errdefer worker.child.kill(self.io);
        try worker.endpoint.start();
        errdefer worker.endpoint.deinit();
        const initialized = try call(&worker.endpoint, .initialize, self.create_json, control);
        self.alloc.free(initialized);
        const configured = try call(&worker.endpoint, .configure, "", control);
        self.alloc.free(configured);
        self.generation += 1;
        self.worker = worker;
        baseline_owned = false;
        std.log.info("standalone inference worker ready generation={d}", .{self.generation});
        return worker;
    }

    // Includes both fixed RPC control tables and the serialized parent parser
    // scratch. Dynamic maps, runtime stacks and backend heaps have other owners.
    const worker_fixed_bytes = @sizeOf(Worker) + @sizeOf(Child) + 4 * @sizeOf(rpc.PayloadBudget) + rpc.child_metadata_limit;
    fn reserveWorkerBaseline(self: *Client) !void {
        if (self.worker_baseline != 0) @panic("worker baseline reused before physical destruction");
        const budget = self.budget orelse return error.ResourceOwnerNotConfigured;
        try worker_heap.adjustParent(&self.worker_baseline, &budget, .{ .key = 1, .previous = 0, .next = worker_fixed_bytes });
    }
    fn releaseWorkerBaseline(self: *Client) void {
        const budget = self.budget orelse @panic("worker baseline has no node owner");
        worker_heap.retireParentAfterJoin(&self.worker_baseline, &budget);
    }

    pub fn resultAllocator(self: *Client) !std.mem.Allocator {
        if (self.budget == null) return error.ResourceOwnerNotConfigured;
        return (self.payload_budget orelse return error.ResourceOwnerNotConfigured).retainedAllocator();
    }

    pub fn invoke(self: *Client, operation: wire.Operation, options: []const u8, data: []const u8, control: rpc.Control) !rpc.OwnedPayload {
        return self.invokePayload(operation, options, .{ .body = data }, control);
    }

    fn invokePayload(self: *Client, operation: wire.Operation, options: []const u8, payload: rpc.Payload, control: rpc.Control) !rpc.OwnedPayload {
        // Startup/replacement contention precedes this caller's physical RPC.
        // Park its borrowed grant while queued, then restore it before worker
        // startup or dispatch; retain that grant throughout active child work.
        const execution = execution_control.InferenceExecutionControl{
            .io = self.io,
            .ptr = control.ptr,
            .check_fn = control.check,
        };
        try execution.lockIo(self.io, &self.mutex);
        const worker = self.ensureWorker(control) catch |err| {
            self.mutex.unlock(self.io);
            return err;
        };
        self.active += 1;
        self.mutex.unlock(self.io);
        defer {
            self.mutex.lockUncancelable(self.io);
            self.active -= 1;
            self.mutex.unlock(self.io);
        }
        const result = try callSegments(&worker.endpoint, operation, options, payload, control);
        // Hand off an independent response before releasing this worker lease.
        return result.detach();
    }
};

fn callPayload(endpoint: *rpc.Endpoint, operation: wire.Operation, options: []const u8, data: []const u8, control: rpc.Control) !rpc.OwnedPayload {
    return callSegments(endpoint, operation, options, .{ .body = data }, control);
}

fn callSegments(endpoint: *rpc.Endpoint, operation: wire.Operation, options: []const u8, payload: rpc.Payload, control: rpc.Control) !rpc.OwnedPayload {
    const alloc = endpoint.computeAllocator();
    if (options.len > rpc.max_metadata_bytes) return error.BodyTooLarge;
    const header = try std.json.Stringify.valueAlloc(alloc, wire.Request{ .operation = operation, .options = options }, .{});
    defer alloc.free(header);
    var framed = payload;
    framed.metadata = header;
    const response = endpoint.call(framed, control) catch |err| switch (err) {
        error.InferenceWorkerUnavailable, error.InferenceWorkerRequestFailed, error.BrokenPipe => return error.InferenceWorkerUnavailable,
        else => return err,
    };
    errdefer response.deinit();
    var parsed = try std.json.parseFromSlice(wire.Reply, alloc, response.view().metadata, .{});
    defer parsed.deinit();
    if (!parsed.value.status.isOk()) return bridge.errorFromStatus(parsed.value.status);
    return response;
}

/// Explicit override for the worker executable. Takes priority over every
/// other resolution step; see `resolveWorkerExecutable`.
const worker_executable_env = "ANTFLY_INFERENCE_WORKER";
const worker_binary_names = [_][]const u8{if (builtin.target.os.tag == .windows) "antfly.exe" else "antfly"};

/// Layout shared by glibc, musl, and Darwin's libc; sufficient to recover the
/// path of the image an address was loaded from.
const DlInfo = extern struct {
    fname: [*:0]const u8,
    fbase: ?*anyopaque,
    sname: ?[*:0]const u8,
    saddr: ?*anyopaque,
};
extern "c" fn dladdr(addr: ?*const anyopaque, info: *DlInfo) c_int;

fn isSharedLibraryName(name: []const u8) bool {
    return std.mem.endsWith(u8, name, ".dylib") or
        std.mem.endsWith(u8, name, ".so") or
        std.mem.indexOf(u8, name, ".so.") != null or
        std.mem.endsWith(u8, name, ".dll");
}

fn fileExists(io: std.Io, path: []const u8) bool {
    std.Io.Dir.cwd().access(io, path, .{}) catch return false;
    return true;
}

/// Path of the image (shared library or main executable) this code was
/// loaded from, independent of argv[0] -- which belongs to whatever process
/// linked libantfly and is meaningless as a worker executable candidate.
fn selfImagePathAlloc(alloc: std.mem.Allocator) ![]u8 {
    if (comptime !builtin.link_libc) return error.Unsupported;
    var info: DlInfo = undefined;
    const anchor: *const anyopaque = @ptrCast(&selfImagePathAlloc);
    if (dladdr(anchor, &info) == 0) return error.Unsupported;
    return alloc.dupe(u8, std.mem.sliceTo(info.fname, 0));
}

fn findOnPathAlloc(alloc: std.mem.Allocator, io: std.Io, name: []const u8) !?[]u8 {
    const path_var = platform.env.getenvSlice("PATH") orelse return null;
    const sep: u8 = if (builtin.target.os.tag == .windows) ';' else ':';
    var it = std.mem.tokenizeScalar(u8, path_var, sep);
    while (it.next()) |dir| {
        const candidate = try std.fs.path.join(alloc, &.{ dir, name });
        if (fileExists(io, candidate)) return candidate;
        alloc.free(candidate);
    }
    return null;
}

/// Resolves the executable used to spawn the sandboxed inference worker
/// (`<exe> inference _worker`, host-owned and replaceable so a crashing GPU
/// driver call or unabortable native session load cannot take the caller
/// down with it -- see `BackendRuntime.hasUninterruptibleCalls`). The
/// `antfly` CLI binary re-execs itself, which is correct: `argv[0]` names the
/// `antfly` binary the user launched. A library host (libantfly linked into a
/// Go test binary, `examples/dogfood`, or any other consumer) has no such
/// self: its `argv[0]` is the host binary, not an `antfly` executable that
/// understands `inference _worker`. Resolution order:
///
///  1. `ANTFLY_INFERENCE_WORKER`, an explicit override naming the worker
///     executable directly.
///  2. The image this code was loaded from, via `dladdr`: for the statically
///     linked `antfly` binary this resolves to itself (unchanged behavior);
///     for a shared `libantfly`, it resolves to the `.dylib`/`.so`, next to
///     which we look for a sibling antfly binary.
///  3. An antfly binary on `PATH`.
fn resolveWorkerExecutable(alloc: std.mem.Allocator, io: std.Io) ![]u8 {
    if (platform.env.getenvSlice(worker_executable_env)) |override| {
        if (override.len == 0) return error.InferenceWorkerExecutableNotConfigured;
        return alloc.dupe(u8, override);
    }
    if (selfImagePathAlloc(alloc)) |self_path| {
        defer alloc.free(self_path);
        const basename = std.fs.path.basename(self_path);
        if (!isSharedLibraryName(basename)) return alloc.dupe(u8, self_path);
        const dir = std.fs.path.dirname(self_path) orelse ".";
        for (worker_binary_names) |name| {
            const candidate = try std.fs.path.join(alloc, &.{ dir, name });
            if (fileExists(io, candidate)) return candidate;
            alloc.free(candidate);
        }
    } else |_| {}
    for (worker_binary_names) |name| {
        if (try findOnPathAlloc(alloc, io, name)) |path| return path;
    }
    std.log.err(
        "no inference worker executable found for the embedded runtime; set {s} to the path " ++
            "of an `antfly` executable next to the loaded libantfly or on PATH",
        .{worker_executable_env},
    );
    return error.InferenceWorkerExecutableNotConfigured;
}

fn call(endpoint: *rpc.Endpoint, operation: wire.Operation, data: []const u8, control: rpc.Control) ![]u8 {
    const response = try callPayload(endpoint, operation, "", data, control);
    defer response.deinit();
    return endpoint.alloc.dupe(u8, response.view().body);
}

fn reply(endpoint: *rpc.Endpoint, status: bridge.Status, options: []const u8, body: []const u8) !rpc.OwnedPayload {
    const alloc = endpoint.computeAllocator();
    const header = try std.json.Stringify.valueAlloc(alloc, wire.Reply{ .status = status, .options = options }, .{});
    defer alloc.free(header);
    return endpoint.copyPayload(.{ .metadata = header, .body = body });
}

fn requestEnvelope(arena: std.mem.Allocator, request: *rpc.Request) !wire.Envelope {
    const payload = request.payload.view();
    const header = try std.json.parseFromSliceLeaky(wire.Request, arena, payload.metadata, .{});
    return .{ .operation = header.operation, .options = header.options, .data = payload.body };
}

const Observation = struct { key: u64, bytes: u64 = 0, tokenizer: bool };
const Worker = struct {
    owner: *Client,
    child: std.process.Child,
    fence: ?platform.process_generation_fence.Lease = null,
    endpoint: rpc.Endpoint,
    resources: std.Io.Mutex = .init,
    leases: std.AutoHashMapUnmanaged(usize, void) = .empty,
    observations: std.ArrayListUnmanaged(*Observation) = .empty,
    child_transient: u64 = 0,
    resource_scratch: [8 * rpc.max_resource_bytes]u8 = undefined,
    metadata_budget: ?*rpc.PayloadBudget = null,
    transport_budget: ?*rpc.PayloadBudget = null,
    const metadata_limit = 1024 * 1024;

    fn createMetadataBudget(alloc: std.mem.Allocator, budget: bridge.ResourceBudget) !*rpc.PayloadBudget {
        const metadata = try rpc.PayloadBudget.create(alloc, .{ .retained = metadata_limit, .control = 0 });
        errdefer metadata.releaseOwner();
        const adjust = budget.adjust_transient_host orelse return error.ResourceOwnerNotConfigured;
        try metadata.configureExternal(.{ .context = budget.context, .retain = budget.retain_context, .release = budget.release_context, .adjust = adjust });
        return metadata;
    }
    fn metadataAllocator(self: *Worker) !std.mem.Allocator {
        return (self.metadata_budget orelse return error.ResourceOwnerNotConfigured).retainedAllocator();
    }

    fn destroy(self: *Worker) void {
        self.endpoint.deinit();
        const owner = self.owner;
        if (self.metadata_budget) |metadata| metadata.releaseOwner();
        if (self.transport_budget) |transport| transport.releaseOwner();
        owner.alloc.destroy(self);
        owner.releaseWorkerBaseline();
    }

    fn closed(raw: *anyopaque) void {
        const self: *Worker = @ptrCast(@alignCast(raw));
        // Reaping is the fence: device memory and borrowed reservations cannot
        // be reused just because the transport disconnected.
        self.child.kill(self.owner.io);
        self.releaseResources();
        self.endpoint.releasePeerAfterPhysicalJoin();
        self.releaseFence();
    }

    fn releaseFence(self: *Worker) void {
        if (self.fence) |*held| held.close(self.owner.io);
        self.fence = null;
    }

    fn releaseResources(self: *Worker) void {
        self.resources.lockUncancelable(self.owner.io);
        defer self.resources.unlock(self.owner.io);
        const budget = self.owner.budget.?;
        worker_heap.retireParentAfterJoin(&self.child_transient, &budget);
        if (self.metadata_budget) |metadata| {
            const alloc = metadata.retainedAllocator();
            var leases = self.leases.keyIterator();
            while (leases.next()) |lease| budget.release_admission(budget.context, lease.*);
            self.leases.deinit(alloc);
            self.leases = .empty;
            for (self.observations.items) |observation| {
                const callback = if (observation.tokenizer) budget.observe_tokenizer_cache else budget.observe_prompt_cache;
                _ = callback(budget.context, @intFromPtr(observation), observation.bytes, 0);
                alloc.destroy(observation);
            }
            self.observations.deinit(alloc);
            self.observations = .empty;
        } else if (self.leases.capacity() != 0 or self.observations.capacity != 0) {
            @panic("worker resource metadata has no allocation owner");
        }
    }

    fn rejectRequest(_: *anyopaque, _: *rpc.Request) !rpc.OwnedPayload {
        return error.UnsupportedOperation;
    }

    fn resourceRequest(raw: *anyopaque, bytes: []const u8) !rpc.ResourceReply {
        const self: *Worker = @ptrCast(@alignCast(raw));
        self.resources.lockUncancelable(self.owner.io);
        defer self.resources.unlock(self.owner.io);
        var arena = std.heap.FixedBufferAllocator.init(&self.resource_scratch);
        const incoming = try std.json.parseFromSliceLeaky(wire.ResourceRequest, arena.allocator(), bytes, .{});
        const result = self.resourceOperation(arena.allocator(), .{ .operation = incoming.operation, .data = incoming.data }) catch |err| {
            // Only an explicit budget denial is recoverable. Other errors can
            // mean ownership diverged; the RPC lifecycle will reap the worker.
            if (err != error.ResourceLimitExceeded and err != error.ResourceTemporarilyUnavailable) return err;
            const status = bridge.statusFromError(err);
            return .{ .code = status.code, .detail = status.detail };
        };
        return .{ .value = result };
    }

    fn resourceOperation(self: *Worker, arena: std.mem.Allocator, envelope: wire.Envelope) !u64 {
        if (self.endpoint.closed.load(.acquire)) return error.ResourceOwnerShuttingDown;
        const budget = self.owner.budget.?;
        switch (envelope.operation) {
            .transient_host => {
                const value = try std.json.parseFromSliceLeaky(wire.Observation, arena, envelope.data, .{});
                try worker_heap.adjustParent(&self.child_transient, &budget, value);
            },
            .reserve => {
                const reservation = try std.json.parseFromSliceLeaky(wire.Reservation, arena, envelope.data, .{});
                // No allocation may fail after a successful reserve before it
                // is recorded for reaped cleanup.
                const alloc = try self.metadataAllocator();
                self.leases.ensureUnusedCapacity(alloc, 1) catch return error.ResourceTemporarilyUnavailable;
                var lease: usize = 0;
                const status = budget.reserve_admission(budget.context, &reservation.amounts, &lease);
                if (!status.isOk()) return bridge.errorFromStatus(status);
                if (lease == 0 or self.leases.contains(lease)) return error.InvalidGenerationAdmission;
                self.leases.putAssumeCapacity(lease, {});
                return lease;
            },
            .retain => {
                const reservation = try std.json.parseFromSliceLeaky(wire.Reservation, arena, envelope.data, .{});
                if (!self.leases.contains(reservation.lease)) return error.InvalidGenerationAdmission;
                const status = budget.retain_admission(budget.context, reservation.lease, &reservation.amounts);
                if (!status.isOk()) return bridge.errorFromStatus(status);
            },
            .transfer => {
                const value = try std.json.parseFromSliceLeaky(wire.Transfer, arena, envelope.data, .{});
                if (value.source == value.destination or !self.leases.contains(value.source) or
                    !self.leases.contains(value.destination)) return error.InvalidGenerationAdmission;
                const transfer = budget.transfer_admission orelse return error.ResourceLimitExceeded;
                // Both tokens are tracked before mutation. No allocation or
                // bookkeeping publication can fail after the owner's handoff;
                // uncertain replies fence the child and reap both live tokens.
                const status = transfer(budget.context, value.source, value.destination, &value.amounts);
                if (!status.isOk()) return bridge.errorFromStatus(status);
            },
            .release => {
                const lease = try std.json.parseFromSliceLeaky(usize, arena, envelope.data, .{});
                if (!self.leases.remove(lease)) return error.InvalidGenerationAdmission;
                budget.release_admission(budget.context, lease);
            },
            .prompt_cache, .tokenizer_cache => {
                const value = try std.json.parseFromSliceLeaky(wire.Observation, arena, envelope.data, .{});
                const tokenizer = envelope.operation == .tokenizer_cache;
                const alloc = try self.metadataAllocator();
                const existing = found: {
                    for (self.observations.items) |item| if (item.key == value.key and item.tokenizer == tokenizer) break :found item;
                    break :found null;
                };
                if (existing == null and value.previous != 0) return error.InvalidGenerationAdmission;
                if (existing == null and value.next == 0) return 0;
                const observation = existing orelse blk: {
                    self.observations.ensureUnusedCapacity(alloc, 1) catch return error.ResourceTemporarilyUnavailable;
                    const item = alloc.create(Observation) catch return error.ResourceTemporarilyUnavailable;
                    item.* = .{ .key = value.key, .tokenizer = tokenizer };
                    break :blk item;
                };
                errdefer if (existing == null) alloc.destroy(observation);
                if (observation.bytes != value.previous) return error.InvalidGenerationAdmission;
                const callback = if (tokenizer) budget.observe_tokenizer_cache else budget.observe_prompt_cache;
                if (callback(budget.context, @intFromPtr(observation), value.previous, value.next) == 0) return error.ResourceLimitExceeded;
                observation.bytes = value.next;
                if (existing == null) self.observations.appendAssumeCapacity(observation);
                if (value.next == 0) {
                    for (self.observations.items, 0..) |item, i| {
                        if (item == observation) {
                            _ = self.observations.swapRemove(i);
                            alloc.destroy(item);
                            break;
                        }
                    }
                }
            },
            else => return error.UnsupportedOperation,
        }
        return 0;
    }
};

const Child = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    endpoint: rpc.Endpoint,
    state: ?*host.LinkedInferenceState = null,
    create_arena: std.heap.ArenaAllocator,
    heap: worker_heap.Heap = undefined,
    configured: bool = false,
    startup_mutex: std.Io.Mutex = .init,

    const ProviderDiagnostic = struct {
        operation: c_int = 0,
        request_json: []const u8 = "",
        has_deadline: bool = false,
    };

    fn dispatch(raw: *anyopaque, request: *rpc.Request) !rpc.OwnedPayload {
        const self: *Child = @ptrCast(@alignCast(raw));
        var arena = std.heap.ArenaAllocator.init(self.heap.allocator());
        defer arena.deinit();
        const envelope = try requestEnvelope(arena.allocator(), request);
        var diagnostic: ProviderDiagnostic = .{};
        return self.execute(arena.allocator(), request, envelope, &diagnostic) catch |err| {
            // Diagnose at the error owner, including envelope decoding and
            // response serialization, before the first lossy RPC status hop.
            const status = if (envelope.operation == .provider)
                @import("antfly_inference_provider_failure").status(diagnostic.operation, diagnostic.request_json, diagnostic.has_deadline, err)
            else
                bridge.statusFromError(err);
            return reply(&self.endpoint, status, "", "");
        };
    }

    fn execute(self: *Child, arena: std.mem.Allocator, request: *rpc.Request, envelope: wire.Envelope, diagnostic: *ProviderDiagnostic) !rpc.OwnedPayload {
        if (envelope.operation == .initialize) {
            try self.startup_mutex.lock(self.io);
            defer self.startup_mutex.unlock(self.io);
            if (self.state != null) return error.AlreadyExists;
            const create_arena = self.create_arena.allocator();
            defer _ = self.create_arena.reset(.free_all);
            const create = try std.json.parseFromSliceLeaky(wire.Create, create_arena, envelope.data, .{ .allocate = .alloc_always });
            var out: ?*anyopaque = null;
            const context = try create.toContext(create_arena, &self.io, &out);
            self.state = @ptrCast(@alignCast(try host.linkedInferenceCreateSupervised(&context, self.heap.allocator())));
            self.state.?.http_allocator = self.heap.allocator();
            return reply(&self.endpoint, .ok, "", "");
        }
        try self.startup_mutex.lock(self.io);
        const state = self.state orelse {
            self.startup_mutex.unlock(self.io);
            return error.ResourceOwnerNotConfigured;
        };
        if (envelope.operation == .configure) {
            defer self.startup_mutex.unlock(self.io);
            if (self.configured) return error.AlreadyExists;
            const budget = self.resourceBudget();
            try host.linkedInferenceConfigure(&.{ .abi_version = bridge.abi_version, .handle = state, .resource_budget = &budget });
            var entries: ?[*]const bridge.RouteManifestEntry = null;
            var length: usize = 0;
            try host.linkedInferenceRouteManifest(&.{ .abi_version = bridge.abi_version, .handle = state, .out_entries = &entries, .out_len = &length });
            self.configured = true;
            return reply(&self.endpoint, .ok, "", "");
        }
        const configured = self.configured;
        self.startup_mutex.unlock(self.io);
        if (!configured) return error.ResourceOwnerNotConfigured;
        switch (envelope.operation) {
            .provider => {
                const provider = try std.json.parseFromSliceLeaky(wire.Provider, arena, envelope.options, .{});
                try provider.validate();
                diagnostic.operation = provider.operation;
                diagnostic.has_deadline = provider.deadline_ns != null;
                var limits = wire.provider_attachment_limits;
                // Text-only calls keep the existing logical JSON body ceiling.
                limits.max_metadata_bytes = rpc.max_body_bytes;
                var media = try wire.attachments.parseAlloc(arena, envelope.data, limits);
                defer media.deinit();
                // Metadata borrows the request payload, not the attachment
                // descriptor allocation freed by media.deinit().
                diagnostic.request_json = media.metadata;
                if (media.attachments.len > 0 and media.metadata.len > wire.provider_attachment_limits.max_metadata_bytes) return error.BodyTooLarge;
                const payloads = try arena.alloc(bridge.ProviderBinaryPayload, media.attachments.len);
                for (payloads, media.attachments) |*payload, attachment| payload.* = .{
                    .bytes = .init(attachment.data),
                    .content_type = .init(attachment.mime_type),
                };
                const refs = try arena.alloc(bridge.ProviderAttachmentRef, provider.attachment_refs.len);
                for (refs, provider.attachment_refs) |*ref, source| {
                    if (source.attachment_index >= payloads.len) return error.InvalidInput;
                    ref.* = .{ .attachment_index = source.attachment_index, .item_index = source.item_index, .item_id = .init(source.item_id), .source_fingerprint = .init(source.source_fingerprint), .page_number = source.page_number orelse 0, .has_page_number = @intFromBool(source.page_number != null) };
                }
                var response_handle: ?*anyopaque = null;
                var response: bridge.String = undefined;
                var numeric: bridge.NumericResult = .{};
                defer if (response_handle) |handle| host.linkedInferenceDestroyProviderResponse(handle);
                var child_phase = ChildPhase{ .request = request };
                var phase_scope: ?execution_control.CheckpointScope = if (provider.phase_handoff)
                    execution_control.CheckpointScope.init(.{ .context = &child_phase, .check_fn = ChildPhase.check, .wait_begin_fn = ChildPhase.park, .wait_end_fn = ChildPhase.reenter })
                else
                    null;
                defer if (phase_scope) |*scope| scope.deinit();
                try host.linkedInferenceInvokeProvider(&.{
                    .abi_version = bridge.abi_version,
                    .handle = state,
                    .operation = provider.operation,
                    .admitted_cpu_slots = provider.admitted_cpu_slots,
                    .request_json = .init(media.metadata),
                    .deadline_ns = provider.deadline_ns orelse 0,
                    .has_deadline = @intFromBool(provider.deadline_ns != null),
                    .out_response_handle = &response_handle,
                    .out_response_json = &response,
                    .out_numeric_result = if (provider.numeric) &numeric else null,
                    .binary_payloads = payloads.ptr,
                    .binary_payloads_len = payloads.len,
                    .attachment_refs = refs.ptr,
                    .attachment_refs_len = refs.len,
                    .cancellation = .{ .context = request, .is_cancelled = cancelled },
                    .progress = .{ .context = request, .update_progress = progress },
                });
                // A backend may catch a lock/resume error. It must never turn
                // a denied physical grant into a successful published result.
                if (child_phase.failure) |err| return err;
                if (numeric.kind != .absent) {
                    const output = try wire.ProviderOutput.encode(arena, numeric);
                    return reply(&self.endpoint, .ok, try std.json.Stringify.valueAlloc(arena, output.options, .{}), output.body);
                }
                return reply(&self.endpoint, .ok, "{}", response.slice());
            },
            .http => {
                const incoming = try std.json.parseFromSliceLeaky(wire.Http, arena, envelope.options, .{});
                try incoming.validate();
                // Route identity, not an ordinal: replacing the executable
                // must never redirect a request to a different handler.
                const route_handle = found: {
                    for (state.route_manifest.items) |entry| {
                        if (entry.method == incoming.method and std.mem.eql(u8, entry.path.slice(), incoming.route))
                            break :found entry.route_handle;
                    }
                    return error.UnsupportedOperation;
                };
                const headers = try arena.alloc(http.HeaderView, incoming.headers.len);
                for (incoming.headers, headers) |header, *out| out.* = .{ .name = .init(header.name), .value = .init(header.value) };
                const params = try arena.alloc(http.RouteParamView, incoming.params.len);
                for (incoming.params, params) |param, *out| out.* = .{ .name = .init(param.name), .value = .init(param.value) };
                const body: ?[]const u8 = if (incoming.has_body) envelope.data else null;
                const view = http.HttpRequestView{
                    .method = incoming.method,
                    .path = .init(incoming.path),
                    .query = .init(incoming.query),
                    .headers_ptr = headers.ptr,
                    .headers_len = headers.len,
                    .params_ptr = params.ptr,
                    .params_len = params.len,
                    .body = .init(body),
                };
                var response_handle: ?*anyopaque = null;
                var response: http.HttpResponseView = undefined;
                defer if (response_handle) |handle| host.linkedInferenceDestroyHttpResponse(handle);
                var child_phase = ChildPhase{ .request = request };
                var phase_scope: ?execution_control.CheckpointScope = if (incoming.phase_handoff)
                    execution_control.CheckpointScope.init(.{ .context = &child_phase, .check_fn = ChildPhase.check, .wait_begin_fn = ChildPhase.park, .wait_end_fn = ChildPhase.reenter })
                else
                    null;
                defer if (phase_scope) |*scope| scope.deinit();
                try host.linkedInferenceHandleHttp(&.{
                    .abi_version = bridge.abi_version,
                    .route_handle = route_handle,
                    .request = &view,
                    .cancellation = .{ .context = request, .is_cancelled = cancelled },
                    .body_source = if (incoming.streaming_body) .{ .context = request, .streaming = 1, .read = readBody, .read_controlled = readBodyControlled, .read_all = readAllBody } else .{},
                    .stream = .{ .context = request, .start = streamStart, .write = streamWrite, .close = streamClose, .start_controlled = streamStartControlled, .write_controlled = streamWriteControlled, .close_controlled = streamCloseControlled },
                    .out_response_handle = &response_handle,
                    .out_response = &response,
                    .deadline_ns = incoming.deadline_ns orelse 0,
                    .has_deadline = @intFromBool(incoming.deadline_ns != null),
                    .admitted_cpu_slots = incoming.admitted_cpu_slots,
                });
                if (child_phase.failure) |err| return err;
                const output_headers = try arena.alloc(wire.Header, response.headers_len);
                const source_headers = if (response.headers_ptr) |ptr| ptr[0..response.headers_len] else &.{};
                for (source_headers, output_headers) |header, *out| out.* = .{ .name = header.name.slice(), .value = header.value.slice() };
                const response_options = try std.json.Stringify.valueAlloc(arena, wire.HttpResponse{
                    .status = response.status,
                    .headers = output_headers,
                }, .{});
                return reply(&self.endpoint, .ok, response_options, response.body.slice());
            },
            else => return error.UnsupportedOperation,
        }
    }

    fn readBody(raw: ?*anyopaque, dest: [*]u8, capacity: usize, out: *usize) callconv(.c) http.CallbackStatus {
        const request: *rpc.Request = @ptrCast(@alignCast(raw orelse return .failed));
        out.* = request.readBody(dest[0..capacity]) catch |err| return switch (err) {
            error.Canceled, error.Cancelled => .canceled,
            else => .failed,
        };
        return .ok;
    }
    fn readBodyControlled(raw: ?*anyopaque, dest: [*]u8, capacity: usize, out: *usize, incoming: *const http.RequestControl) callconv(.c) http.CallbackStatus {
        const request: *rpc.Request = @ptrCast(@alignCast(raw orelse return .failed));
        var control: HttpControl.Child = .{ .incoming = incoming };
        out.* = request.readBodyWithControl(dest[0..capacity], control.view()) catch |err| return HttpControl.status(err);
        return .ok;
    }
    fn readAllBody(_: ?*anyopaque, _: *http.OptionalBytes) callconv(.c) http.CallbackStatus {
        return .failed;
    }

    fn cancelled(raw: ?*const anyopaque) callconv(.c) u8 {
        const request: *const rpc.Request = @ptrCast(@alignCast(raw.?));
        return @intFromBool(request.cancelled.load(.acquire));
    }

    const ChildPhase = ExecutionPhases.Child;

    fn emitAcknowledged(request: *rpc.Request, event: wire.Event, body: []const u8) !void {
        const alloc = request.endpoint.computeAllocator();
        const data = try std.json.Stringify.valueAlloc(alloc, event, .{});
        defer alloc.free(data);
        try request.eventAcknowledged(.{ .metadata = data, .body = body });
    }

    fn emitControlled(request: *rpc.Request, event: wire.Event, body: []const u8, incoming: *const http.RequestControl) !void {
        var control: HttpControl.Child = .{ .incoming = incoming };
        var value = event;
        value.deadline_ns = try control.deadline();
        const alloc = request.endpoint.computeAllocator();
        const data = try std.json.Stringify.valueAlloc(alloc, value, .{});
        defer alloc.free(data);
        try request.eventAcknowledgedControlled(.{ .metadata = data, .body = body }, control.view());
    }
    fn streamStartControlled(raw: ?*anyopaque, status: u16, content_type: http.Bytes, headers: http.HeaderList, incoming: *const http.RequestControl) callconv(.c) http.CallbackStatus {
        const request: *rpc.Request = @ptrCast(@alignCast(raw.?));
        const alloc = request.endpoint.computeAllocator();
        const values = alloc.alloc(wire.Header, headers.len) catch return .failed;
        defer alloc.free(values);
        for (headers.slice(), values) |header, *value| value.* = .{ .name = header.name.slice(), .value = header.value.slice() };
        emitControlled(request, .{ .kind = .stream_start, .status = status, .headers = values }, content_type.slice(), incoming) catch |err| return HttpControl.status(err);
        return .ok;
    }
    fn streamWriteControlled(raw: ?*anyopaque, bytes: http.Bytes, incoming: *const http.RequestControl) callconv(.c) http.CallbackStatus {
        const request: *rpc.Request = @ptrCast(@alignCast(raw.?));
        emitControlled(request, .{ .kind = .stream_write }, bytes.slice(), incoming) catch |err| return HttpControl.status(err);
        return .ok;
    }
    fn streamCloseControlled(raw: ?*anyopaque, incoming: *const http.RequestControl) callconv(.c) http.CallbackStatus {
        const request: *rpc.Request = @ptrCast(@alignCast(raw.?));
        emitControlled(request, .{ .kind = .stream_close }, "", incoming) catch |err| return HttpControl.status(err);
        return .ok;
    }

    fn progress(raw: ?*anyopaque, phase: u8, completed: u64, total: u64, model: bridge.String, backend: bridge.String) callconv(.c) void {
        const request: *rpc.Request = @ptrCast(@alignCast(raw.?));
        if (!request.progress_gate.enter(platform.time.monotonicNs())) return;
        defer request.progress_gate.leave();
        var storage: [Progress.encoded_capacity]u8 = undefined;
        const data = Progress.encode(&storage, .{ .phase = phase, .completed = completed, .total = total, .model = model.slice(), .backend = backend.slice() }) catch {
            request.cancelled.store(true, .release);
            return;
        };
        request.event(.{ .metadata = data }) catch
            request.cancelled.store(true, .release);
    }
    fn streamStart(raw: ?*anyopaque, status: u16, content_type: http.Bytes, headers: http.HeaderList) callconv(.c) http.CallbackStatus {
        const request: *rpc.Request = @ptrCast(@alignCast(raw.?));
        const alloc = request.endpoint.computeAllocator();
        const values = alloc.alloc(wire.Header, headers.len) catch return .failed;
        defer alloc.free(values);
        for (headers.slice(), values) |header, *value| value.* = .{ .name = header.name.slice(), .value = header.value.slice() };
        // The start event's body carries the content type; subsequent chunks
        // keep their existing envelope without an extra JSON field per event.
        emitAcknowledged(request, .{ .kind = .stream_start, .status = status, .headers = values }, content_type.slice()) catch return .canceled;
        return .ok;
    }
    fn streamWrite(raw: ?*anyopaque, bytes: http.Bytes) callconv(.c) http.CallbackStatus {
        const request: *rpc.Request = @ptrCast(@alignCast(raw.?));
        emitAcknowledged(request, .{ .kind = .stream_write }, bytes.slice()) catch return .canceled;
        return .ok;
    }
    fn streamClose(raw: ?*anyopaque) callconv(.c) http.CallbackStatus {
        const request: *rpc.Request = @ptrCast(@alignCast(raw.?));
        emitAcknowledged(request, .{ .kind = .stream_close }, "") catch return .canceled;
        return .ok;
    }

    fn resourceBudget(self: *Child) bridge.ResourceBudget {
        return .{
            .abi_version = bridge.abi_version,
            .context = self,
            .retain_context = retainContext,
            .release_context = releaseContext,
            .reserve_admission = reserve,
            .retain_admission = retain,
            .transfer_admission = transfer,
            .release_admission = release,
            .observe_prompt_cache = observePrompt,
            .observe_tokenizer_cache = observeTokenizer,
        };
    }
    fn retainContext(_: *anyopaque) callconv(.c) u8 {
        return 1;
    }
    fn releaseContext(_: *anyopaque) callconv(.c) void {}
    fn resourceFailure(_: *Child) noreturn {
        worker_lifetime.resourceOwnerLost();
    }

    fn resourceCall(self: *Child, operation: wire.Operation, value: anytype) rpc.ResourceReply {
        var value_buffer: [rpc.max_resource_bytes]u8 = undefined;
        var value_writer = std.Io.Writer.fixed(&value_buffer);
        std.json.Stringify.value(value, .{}, &value_writer) catch self.resourceFailure();
        var request_buffer: [rpc.max_resource_bytes]u8 = undefined;
        var request_writer = std.Io.Writer.fixed(&request_buffer);
        std.json.Stringify.value(wire.ResourceRequest{ .operation = operation, .data = value_writer.buffered() }, .{}, &request_writer) catch self.resourceFailure();
        const response = self.endpoint.callResource(request_writer.buffered()) catch self.resourceFailure();
        validateResourceReply(operation, response) catch self.resourceFailure();
        return response;
    }
    fn reserve(raw: *anyopaque, amounts: *const bridge.AdmissionAmounts, lease: *usize) callconv(.c) bridge.Status {
        const self: *Child = @ptrCast(@alignCast(raw));
        const response = self.resourceCall(.reserve, wire.Reservation{ .amounts = amounts.* });
        const status: bridge.Status = .{ .code = response.code, .detail = response.detail };
        if (status.isOk()) lease.* = @intCast(response.value);
        return status;
    }
    fn retain(raw: *anyopaque, lease: usize, amounts: *const bridge.AdmissionAmounts) callconv(.c) bridge.Status {
        const self: *Child = @ptrCast(@alignCast(raw));
        const response = self.resourceCall(.retain, wire.Reservation{ .lease = lease, .amounts = amounts.* });
        return .{ .code = response.code, .detail = response.detail };
    }
    fn transfer(raw: *anyopaque, source: usize, destination: usize, amounts: *const bridge.AdmissionAmounts) callconv(.c) bridge.Status {
        const self: *Child = @ptrCast(@alignCast(raw));
        const response = self.resourceCall(.transfer, wire.Transfer{ .source = source, .destination = destination, .amounts = amounts.* });
        return .{ .code = response.code, .detail = response.detail };
    }
    fn release(raw: *anyopaque, lease: usize) callconv(.c) void {
        const self: *Child = @ptrCast(@alignCast(raw));
        _ = self.resourceCall(.release, lease);
    }
    fn observe(self: *Child, operation: wire.Operation, key: usize, previous: u64, next: u64) u8 {
        const response = self.resourceCall(operation, wire.Observation{ .key = key, .previous = previous, .next = next });
        const status: bridge.Status = .{ .code = response.code, .detail = response.detail };
        // Prompt cache reports already-retained bytes through a void observer;
        // it cannot reject growth or retry a lost physical retirement. Tokenizer
        // admission can refuse growth before publication, but not retirement.
        return worker_lifetime.cacheObservationReply(operation == .prompt_cache, previous, next, status.isOk());
    }
    fn observePrompt(raw: *anyopaque, key: usize, previous: u64, next: u64) callconv(.c) u8 {
        const self: *Child = @ptrCast(@alignCast(raw));
        return self.observe(.prompt_cache, key, previous, next);
    }
    fn observeTokenizer(raw: *anyopaque, key: usize, previous: u64, next: u64) callconv(.c) u8 {
        const self: *Child = @ptrCast(@alignCast(raw));
        return self.observe(.tokenizer_cache, key, previous, next);
    }
};

fn validateResourceReply(operation: wire.Operation, reply_value: rpc.ResourceReply) !void {
    if (reply_value.code == 0 and reply_value.detail == 0) {
        if (operation == .reserve) {
            if (reply_value.value == 0 or reply_value.value > std.math.maxInt(usize)) return error.InvalidResourceReply;
        } else if (reply_value.value != 0) return error.InvalidResourceReply;
        return;
    }
    if (operation != .release and reply_value.value == 0) {
        for ([_]anyerror{ error.ResourceLimitExceeded, error.ResourceTemporarilyUnavailable }) |err| {
            const denial = bridge.statusFromError(err);
            if (reply_value.code == denial.code and reply_value.detail == denial.detail) return;
        }
    }
    return error.InvalidResourceReply;
}

pub fn runChild(alloc: std.mem.Allocator, io: std.Io) !void {
    var child: Child = .{
        .alloc = alloc,
        .io = io,
        .create_arena = std.heap.ArenaAllocator.init(alloc),
        .endpoint = undefined,
    };
    child.endpoint = .{
        .alloc = alloc,
        .io = io,
        .input = .stdin(),
        .output = .stdout(),
        .context = &child,
        .handler = Child.dispatch,
        .on_closed = worker_lifetime.ownerLost,
        .next_id = 2,
        .report_retirement = true,
    };
    const transport = try rpc.PayloadBudget.create(alloc, .{ .retained = rpc.child_metadata_limit, .control = 0 });
    defer transport.releaseOwner();
    child.endpoint.metadata_allocator = transport.retainedAllocator();
    try child.heap.init(alloc, &child.endpoint);
    defer child.heap.deinit();
    child.endpoint.compute_allocator = child.heap.allocator();
    child.create_arena = std.heap.ArenaAllocator.init(child.heap.allocator());
    try child.endpoint.start();
    defer child.endpoint.deinit();
    var lifetime: std.Io.Event = .unset;
    try lifetime.wait(io);
}

test "inference worker streaming events preserve content type and callback ABI" {
    const Capture = struct {
        status: u16 = 0,
        bytes: [128]u8 = undefined,
        len: usize = 0,
        saw_headers: bool = false,
        fn start(raw: ?*anyopaque, status: u16, content_type: http.Bytes, headers: http.HeaderList) callconv(.c) http.CallbackStatus {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.saw_headers = headers.len == 1 and std.mem.eql(u8, headers.slice()[0].name.slice(), "X-Test-Policy") and std.mem.eql(u8, headers.slice()[0].value.slice(), "preserved");
            const value = content_type.slice();
            if (value.len > self.bytes.len) return .failed;
            @memcpy(self.bytes[0..value.len], value);
            self.len = value.len;
            self.status = status;
            return .ok;
        }
    };
    // Type-check the child side as well as exercising parent-side wire decode.
    const child_sink: http.StreamSink = .{ .start = Child.streamStart };
    try std.testing.expect(child_sink.start != null);
    for ([_][]const u8{ "text/event-stream", "application/x-ndjson", "application/octet-stream" }) |content_type| {
        var capture: Capture = .{};
        var control: ForwardControl = .{ .alloc = std.testing.allocator, .cancellation = .{}, .stream = .{ .context = &capture, .start = Capture.start } };
        const encoded = try std.json.Stringify.valueAlloc(std.testing.allocator, wire.Event{ .kind = .stream_start, .status = 202, .headers = &.{.{ .name = "X-Test-Policy", .value = "preserved" }} }, .{});
        defer std.testing.allocator.free(encoded);
        try ForwardControl.event(&control, .{ .metadata = encoded, .body = content_type });
        try std.testing.expect(control.output.started);
        try std.testing.expect(capture.saw_headers);
        try std.testing.expectEqual(@as(u16, 202), capture.status);
        try std.testing.expectEqualStrings(content_type, capture.bytes[0..capture.len]);
    }
}

test "inference worker retains reservations until reaped cleanup and owns partial startup" {
    const Budget = struct {
        const Charge = struct { id: usize, bytes: u64 };
        leased: bool = false,
        cached: u64 = 0,
        transient: u64 = 0,
        charges: [8]?Charge = @splat(null),
        fn adjust(raw: *anyopaque, id: usize, previous: u64, next: u64) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var vacant: ?usize = null;
            for (&self.charges, 0..) |*slot, i| {
                if (slot.*) |charge| {
                    if (charge.id != id) continue;
                    if (charge.bytes != previous) return 0;
                    self.transient = self.transient - previous + next;
                    slot.* = if (next == 0) null else .{ .id = id, .bytes = next };
                    return 1;
                } else vacant = i;
            }
            if (previous != 0) return 0;
            if (next == 0) return 1;
            const index = vacant orelse return 0;
            self.charges[index] = .{ .id = id, .bytes = next };
            self.transient += next;
            return 1;
        }
        fn retainContext(_: *anyopaque) callconv(.c) u8 {
            return 1;
        }
        fn releaseContext(_: *anyopaque) callconv(.c) void {}
        fn reserve(raw: *anyopaque, _: *const bridge.AdmissionAmounts, out: *usize) callconv(.c) bridge.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.leased) return bridge.statusFromError(error.ResourceLimitExceeded);
            self.leased = true;
            out.* = 7;
            return .ok;
        }
        fn retain(_: *anyopaque, _: usize, _: *const bridge.AdmissionAmounts) callconv(.c) bridge.Status {
            return .ok;
        }
        fn release(raw: *anyopaque, lease: usize) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(lease == 7 and self.leased);
            self.leased = false;
        }
        fn observe(raw: *anyopaque, _: usize, previous: u64, next: u64) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.cached == previous);
            self.cached = next;
            return 1;
        }
    };
    const alloc = std.testing.allocator;
    var budget: Budget = .{};
    var client: Client = .{
        .alloc = alloc,
        .io = std.testing.io,
        .create_json = undefined,
        .environment = undefined,
        .budget = .{
            .abi_version = bridge.abi_version,
            .context = &budget,
            .retain_context = Budget.retainContext,
            .release_context = Budget.releaseContext,
            .reserve_admission = Budget.reserve,
            .retain_admission = Budget.retain,
            .release_admission = Budget.release,
            .observe_prompt_cache = Budget.observe,
            .observe_tokenizer_cache = Budget.observe,
            .adjust_transient_host = Budget.adjust,
        },
    };
    client.payload_budget = try rpc.PayloadBudget.create(alloc, .{});
    defer client.payload_budget.?.releaseOwner();
    var worker: Worker = .{
        .owner = &client,
        .child = undefined,
        .endpoint = .{ .alloc = alloc, .io = std.testing.io, .input = undefined, .output = undefined, .context = undefined, .handler = Worker.rejectRequest, .resource_handler = Worker.resourceRequest, .on_closed = Worker.closed, .next_id = 1 },
    };
    worker.metadata_budget = try Worker.createMetadataBudget(alloc, client.budget.?);
    defer worker.metadata_budget.?.releaseOwner();
    defer worker.releaseResources();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const reserve_json = try std.json.Stringify.valueAlloc(arena.allocator(), wire.Reservation{ .amounts = .{
        .host_weight_bytes = 100,
        .backend_weight_bytes = 50,
        .host_kv_bytes = 0,
        .backend_kv_bytes = 0,
        .host_scratch_bytes = 0,
        .backend_scratch_bytes = 0,
    } }, .{});
    // Regression: ordinary reply credit used to fail after reserve succeeded,
    // leaving a live lease whose ID the child never received.
    worker.endpoint.control_bytes = 4 * 1024 * 1024;
    worker.endpoint.retained_bytes = 256 * 1024 * 1024;
    defer {
        worker.endpoint.control_bytes = 0;
        worker.endpoint.retained_bytes = 0;
    }
    const resource_json = try std.json.Stringify.valueAlloc(arena.allocator(), wire.ResourceRequest{ .operation = .reserve, .data = reserve_json }, .{});
    const reserved = try Worker.resourceRequest(&worker, resource_json);
    try validateResourceReply(.reserve, reserved);
    try std.testing.expectEqual(@as(u64, 7), reserved.value);
    const denied = try Worker.resourceRequest(&worker, resource_json);
    try validateResourceReply(.reserve, denied);
    try std.testing.expectEqual(bridge.statusFromError(error.ResourceLimitExceeded).detail, denied.detail);
    try std.testing.expect(!worker.endpoint.closed.load(.acquire));
    const release_json = try std.json.Stringify.valueAlloc(arena.allocator(), wire.ResourceRequest{ .operation = .release, .data = "7" }, .{});
    try validateResourceReply(.release, try Worker.resourceRequest(&worker, release_json));
    try std.testing.expect(!budget.leased);
    try std.testing.expectEqual(@as(usize, 0), worker.leases.count());
    _ = try Worker.resourceRequest(&worker, resource_json);
    _ = try worker.resourceOperation(arena.allocator(), .{ .operation = .prompt_cache, .data = "{\"key\":9,\"previous\":0,\"next\":64}" });
    try std.testing.expectError(error.InvalidGenerationAdmission, worker.resourceOperation(arena.allocator(), .{ .operation = .release, .data = "8" }));
    worker.endpoint.closed.store(true, .release);
    try std.testing.expect(budget.leased);
    try std.testing.expectEqual(@as(u64, 64), budget.cached);
    try std.testing.expectError(error.ResourceOwnerShuttingDown, worker.resourceOperation(arena.allocator(), .{ .operation = .reserve, .data = reserve_json }));
    worker.releaseResources();
    try std.testing.expect(!budget.leased);
    try std.testing.expectEqual(@as(u64, 0), budget.cached);

    if (@import("builtin").os.tag == .windows) return; // POSIX child fixture.
    var fence_dir = std.testing.tmpDir(.{});
    defer fence_dir.cleanup();
    const test_data_dir = try fence_dir.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(test_data_dir);
    client.data_dir = test_data_dir;
    client.create_json = try alloc.dupe(u8, "{}");
    defer alloc.free(client.create_json);
    client.environment = std.process.Environ.Map.init(alloc);
    defer client.environment.deinit();
    // This partial-startup fixture constructs its prepared launch state directly.
    client.configuration_allocator = alloc;
    const Fault = struct {
        var fail_at: usize = 0;
        var groups: usize = 0;
        var kills: usize = 0;
        var spawns: usize = 0;
        var missing_executable: bool = false;
        var owner: *Client = undefined;
        var baseline_at_kill: bool = false;
        fn concurrent(raw: ?*anyopaque, group: *std.Io.Group, context: []const u8, alignment: std.mem.Alignment, start: *const fn (*const anyopaque) void) std.Io.ConcurrentError!void {
            groups += 1;
            if (groups == fail_at) return error.ConcurrencyUnavailable;
            return std.testing.io.vtable.groupConcurrent(raw, group, context, alignment, start);
        }
        fn spawn(raw: ?*anyopaque, options: std.process.SpawnOptions) std.process.SpawnError!std.process.Child {
            spawns += 1;
            if (missing_executable) return error.FileNotFound;
            var modified = options;
            modified.argv = &.{"/bin/cat"};
            modified.environ_map = null;
            if (platform.process_generation_fence.supported and modified.inherit_files.len != 1) return error.Unexpected;
            return std.testing.io.vtable.processSpawn(raw, modified);
        }
        fn kill(raw: ?*anyopaque, child: *std.process.Child) void {
            baseline_at_kill = owner.worker_baseline == Client.worker_fixed_bytes;
            kills += 1;
            if (kills == 1) {
                std.testing.io.vtable.childKill(raw, child);
            } else {
                // Count a duplicate cleanup without signaling a reused PID or
                // closing unrelated descriptors if this regression returns.
                child.id = null;
                child.stdin = null;
                child.stdout = null;
                child.stderr = null;
            }
        }
    };
    var vtable = std.testing.io.vtable.*;
    vtable.groupConcurrent = Fault.concurrent;
    vtable.processSpawn = Fault.spawn;
    vtable.childKill = Fault.kill;
    Fault.owner = &client;
    client.io = .{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    // A missing override/sibling must stay actionable across the ABI and
    // must not enter the transient worker restart/capacity retry path.
    Fault.missing_executable = true;
    Fault.kills = 0;
    try std.testing.expectError(error.InferenceWorkerExecutableNotConfigured, client.ensureWorker(.{}));
    try std.testing.expectEqual(@as(usize, 0), Fault.kills);
    try std.testing.expect(client.worker == null);
    Fault.missing_executable = false;
    if (platform.process_generation_fence.supported) {
        var held = try platform.process_generation_fence.Lease.acquire(client.io, fence_dir.dir, "inference-worker-generation.lock");
        defer held.close(client.io);
        Fault.spawns = 0;
        try std.testing.expectError(error.InferenceWorkerUnavailable, client.ensureWorker(.{}));
        try std.testing.expectEqual(@as(usize, 0), Fault.spawns);
        try std.testing.expect(client.worker == null);
    }
    // Failure before any reader, failure after the reader owns cleanup, and
    // failure during initialization must all use the same child handle. With
    // no injected spawn failure, cat echoes our offer as an invalid peer frame.
    for ([_]usize{ 1, 2, 0 }) |fail_at| {
        Fault.fail_at = fail_at;
        Fault.groups = 0;
        Fault.kills = 0;
        try std.testing.expectError(
            if (fail_at == 0) error.InferenceWorkerUnavailable else error.ConcurrencyUnavailable,
            client.ensureWorker(.{}),
        );
        try std.testing.expectEqual(@as(usize, 1), Fault.kills);
        try std.testing.expect(Fault.baseline_at_kill);
        try std.testing.expectEqual(@as(u64, 0), client.worker_baseline);
        try std.testing.expectEqual(@as(u64, 0), budget.transient);
        try std.testing.expect(client.worker == null);
        try std.testing.expectEqual(@as(u64, 0), client.generation);
        try std.testing.expect(!budget.leased);
        if (platform.process_generation_fence.supported) {
            var next = try platform.process_generation_fence.Lease.acquire(client.io, fence_dir.dir, "inference-worker-generation.lock");
            next.close(client.io);
        }
    }
}

test "inference worker resource replies distinguish denials from ownership uncertainty" {
    for ([_]anyerror{ error.ResourceLimitExceeded, error.ResourceTemporarilyUnavailable }) |err| {
        const status = bridge.statusFromError(err);
        const denial: rpc.ResourceReply = .{ .code = status.code, .detail = status.detail };
        for ([_]wire.Operation{ .reserve, .retain, .transfer, .prompt_cache, .tokenizer_cache }) |operation|
            try validateResourceReply(operation, denial);
        try std.testing.expectError(error.InvalidResourceReply, validateResourceReply(.release, denial));
    }
    try std.testing.expectError(error.InvalidResourceReply, validateResourceReply(.reserve, .{}));
    try std.testing.expectError(error.InvalidResourceReply, validateResourceReply(.release, .{ .value = 7 }));
    try std.testing.expectError(error.InvalidResourceReply, validateResourceReply(.reserve, .{ .code = 999 }));
    try std.testing.expectError(error.InvalidResourceReply, validateResourceReply(.retain, .{ .detail = 999 }));
    const failure = bridge.statusFromError(error.OutOfMemory);
    try std.testing.expectError(error.InvalidResourceReply, validateResourceReply(.reserve, .{ .code = failure.code, .detail = failure.detail }));
}

test "runnable execution worker budget bounds progress through production callbacks without a phase handoff" {
    var pair: rpc.TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    const Handler = struct {
        fn run(_: *anyopaque, request: *rpc.Request) !rpc.OwnedPayload {
            const model: [Progress.max_model_bytes * 4]u8 = @splat('m');
            const backend: [Progress.max_backend_bytes * 4]u8 = @splat('b');
            Child.progress(request, 3, 7, 9, .init(&model), .init(&backend));
            if (request.cancelled.load(.acquire)) return error.TestProgressEncodingFailed;
            // Simulate a still-active reporter. Further callbacks must finish
            // without taking a second transport slot or waiting on this gate.
            if (!request.progress_gate.enter(std.math.maxInt(u64))) return error.TestProgressGateBusy;
            defer request.progress_gate.leave();
            for (0..1000) |_| Child.progress(request, 3, 8, 9, .init(&model), .init(&backend));
            return request.endpoint.copyPayload(request.payload.view());
        }
    };
    const Capture = struct {
        count: usize = 0,
        fn update(raw: ?*anyopaque, phase: u8, completed: u64, total: u64, model: bridge.String, backend: bridge.String) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (phase == 3 and completed == 7 and total == 9 and model.len == Progress.max_model_bytes and backend.len == Progress.max_backend_bytes) self.count += 1 else self.count += 100;
        }
        fn rejectPhase(_: ?*anyopaque, _: rpc.Phase) !void {
            return error.TestProgressMustNotParkNativeTeam;
        }
    };
    pair.child.handler = Handler.run;
    var capture: Capture = .{};
    var control: ForwardControl = .{ .alloc = std.testing.allocator, .cancellation = .{}, .progress = .{ .context = &capture, .update_progress = Capture.update } };
    var view = control.view();
    view.phase = Capture.rejectPhase;
    const response = try pair.parent.call(.{ .body = "progress-result" }, view);
    defer response.deinit();
    try std.testing.expectEqualStrings("progress-result", response.view().body);
    try std.testing.expectEqual(@as(usize, 1), capture.count);
}

test "runnable execution worker budget carries controlled streaming deadlines through production callbacks" {
    const io = std.testing.io;
    var pair: rpc.TestPair = undefined;
    try pair.init();
    defer pair.deinit();
    const Handler = struct {
        fn check(_: ?*anyopaque) !void {}
        fn run(_: *anyopaque, request: *rpc.Request) !rpc.OwnedPayload {
            var source: HttpControl.Parent = .{ .original_ptr = null, .original_check = check, .original_deadline_ns = platform.time.monotonicNs() + 2000 * std.time.ns_per_ms, .event_deadline_ns = null, .event = .{} };
            const view_control = source.view();
            try HttpControl.result(Child.streamStartControlled(request, 200, .init("text/plain"), .{}, &view_control));
            // Start the short physical credit-wait deadline only after headers
            // have been acknowledged. Tighten the original bound; never renew it.
            source.original_deadline_ns = @min(source.original_deadline_ns.?, platform.time.monotonicNs() + 100 * std.time.ns_per_ms);
            const status = Child.streamWriteControlled(request, .init("blocked"), &view_control);
            if (status != .timeout) return error.TestWorkerDeadlineWasLost;
            const pair_state: *rpc.TestPair = @ptrCast(@alignCast(request.endpoint.context));
            pair_state.phase_denied.set(io);
            return error.Timeout;
        }
    };
    pair.child.handler = Handler.run;
    var listener = try httpx.TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, io);
    defer listener.deinit();
    var socket = try httpx.Socket.connect(listener.getLocalAddress(), io);
    defer socket.close();
    socket.enableNativeTimeouts();
    var peer = try listener.accept();
    defer peer.socket.close();
    var h2 = httpx.H2Connection.initServer(std.testing.allocator, io);
    defer h2.deinit();
    const stream = try h2.stream_manager.getOrCreateStream(1);
    stream.state = .open;
    stream.send_window = 0;
    var request = try httpx.Request.init(std.testing.allocator, .GET, "/stream");
    defer request.deinit();
    var context = httpx.Context.init(std.testing.allocator, io, &request);
    defer context.deinit();
    context.h2 = &h2;
    context.h2_sock = &socket;
    context.h2_stream_id = 1;
    var outbound: @import("antfly_runtime_abi").http_bridge.Outbound = .{ .context = &context };
    var control: ForwardControl = .{ .alloc = std.testing.allocator, .cancellation = .{}, .deadline_ns = platform.time.monotonicNs() + 2000 * std.time.ns_per_ms, .stream = outbound.stream() };
    const started = platform.time.monotonicNs();
    try std.testing.expectError(error.Timeout, pair.parent.call(.{ .body = "stream" }, control.view()));
    try std.testing.expect(platform.time.monotonicNs() - started < std.time.ns_per_s);
    try pair.phase_denied.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(1), .clock = .awake } });
    try std.testing.expect(control.output.started and outbound.started);
    try std.testing.expect(outbound.writer.?.control.ptr == null);
    try std.testing.expect(outbound.writer.?.h2_writer.?.control.ptr == null);
    try std.testing.expect(socket.request_deadline_ms == null);
}

test "inference worker HTTP response keeps its original slab and shared credit through retirement" {
    const alloc = std.testing.allocator;
    const body = try alloc.alloc(u8, 128 * 1024);
    defer alloc.free(body);
    @memset(body, 0xa6);
    const budget = try rpc.PayloadBudget.create(alloc, .{ .retained = body.len });
    defer budget.releaseOwner();
    const result = blk: {
        var pair: rpc.TestPair = undefined;
        try pair.init();
        defer pair.deinit();
        pair.parent.payload_budget = budget;
        const received = try pair.parent.call(.{ .body = body }, .{});
        break :blk received.detach();
    };
    var result_owned = true;
    defer if (result_owned) result.deinit();
    var response = httpx.Response.init(alloc, 200);
    var response_live = true;
    defer if (response_live) response.deinit();
    try attachHttpPayload(&response, result);
    result_owned = false;
    try std.testing.expectEqual(@intFromPtr(result.view().body.ptr), @intFromPtr(response.body.?.ptr));
    try std.testing.expectEqualSlices(u8, body, response.body.?);
    try std.testing.expectEqual(body.len, budget.used().retained);
    // Releasing/replacing the public body conservatively keeps retirement debt
    // until the response owner joins; it cannot make the original slab uncharged.
    response.releaseBody();
    try std.testing.expectEqual(body.len, budget.used().retained);
    response.deinit();
    response_live = false;
    try std.testing.expectEqual(@as(usize, 0), budget.used().retained);
}

test "inference worker HTTP response owner allocation refusal preserves payload ownership" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    const budget = try rpc.PayloadBudget.create(std.testing.allocator, .{});
    defer budget.releaseOwner();
    var endpoint: rpc.Endpoint = .{
        .alloc = failing.allocator(),
        .io = std.testing.io,
        .input = undefined,
        .output = undefined,
        .context = undefined,
        .handler = undefined,
        .on_closed = undefined,
        .next_id = 1,
        .payload_budget = budget,
    };
    const result = (try endpoint.copyPayload(.{ .body = "retained response" })).detach();
    defer result.deinit();
    var response = httpx.Response.init(failing.allocator(), 200);
    defer response.deinit();
    try std.testing.expectError(error.OutOfMemory, attachHttpPayload(&response, result));
    try std.testing.expect(response.body == null and response.retirement == null);
    try std.testing.expectEqualStrings("retained response", result.view().body);
    try std.testing.expectEqual(result.bytes.len, budget.used().control);
}

const ProviderPublicationFixture = struct {
    handle: ?*anyopaque = null,
    json: bridge.String = .init(""),
    numeric: bridge.NumericResult = .{},
    fn context(self: *@This()) bridge.ProviderInvokeContext {
        return .{
            .abi_version = bridge.abi_version,
            .handle = undefined,
            .operation = @backingInt(bridge.ProviderOperation.embed_dense_rasters),
            .request_json = .init("{}"),
            .has_deadline = 0,
            .deadline_ns = 0,
            .out_response_handle = &self.handle,
            .out_response_json = &self.json,
            .out_numeric_result = &self.numeric,
        };
    }
    fn deinit(self: *@This()) void {
        if (self.handle) |handle| host.linkedInferenceDestroyProviderResponse(handle);
        self.handle = null;
    }
};

test "inference worker provider JSON stays charged through the production ABI response handle" {
    const budget = try rpc.PayloadBudget.create(std.testing.allocator, .{ .retained = 512, .control = 0 });
    defer budget.releaseOwner();
    const alloc = budget.retainedAllocator();
    const bytes = try alloc.dupe(u8, "{\"value\":7}");
    var fixture: ProviderPublicationFixture = .{};
    defer fixture.deinit();
    const context = fixture.context();
    try host.publishWorkerProviderResponse(&context, .{ .json = .{ .bytes = bytes, .alloc = alloc } });
    try std.testing.expectEqual(@intFromPtr(bytes.ptr), @intFromPtr(fixture.json.ptr));
    try std.testing.expectEqualStrings("{\"value\":7}", fixture.json.slice());
    try std.testing.expect(budget.used().retained > bytes.len);
    try std.testing.expectEqual(@as(usize, 0), budget.used().control);
    try std.testing.expectError(error.OutOfMemory, alloc.alloc(u8, 512));
    fixture.deinit();
    try std.testing.expectEqual(@as(usize, 0), budget.used().retained);
    const next = try alloc.alloc(u8, 512);
    alloc.free(next);
}

test "inference worker numeric decoding and ABI descriptors retain shared credit until destruction" {
    const budget = try rpc.PayloadBudget.create(std.testing.allocator, .{ .retained = 512, .control = 0 });
    defer budget.releaseOwner();
    const alloc = budget.retainedAllocator();
    var body: [12]u8 = undefined;
    for ([_]f32{ 0.25, -0.5, 1.0 }, 0..) |value, i|
        std.mem.writeInt(u32, body[i * 4 ..][0..4], @bitCast(value), .little);
    const output = wire.ProviderOutput{ .kind = .scores, .row_lengths = &.{ 2, 1, 0 } };
    const rows = try output.decode(alloc, &body);
    const decoded_bytes = budget.used().retained;
    try std.testing.expectEqual(@as(usize, 3 * @sizeOf([]f32) + 12), decoded_bytes);
    var fixture: ProviderPublicationFixture = .{};
    defer fixture.deinit();
    const context = fixture.context();
    try host.publishWorkerProviderResponse(&context, .{ .numeric = .{ .rows = rows, .kind = .scores, .alloc = alloc } });
    try std.testing.expect(budget.used().retained > decoded_bytes);
    try std.testing.expectEqual(@intFromPtr(rows[0].ptr), @intFromPtr(fixture.numeric.rows.?[0].values.?));
    try std.testing.expectEqual(@as(usize, 0), budget.used().control);
    const copied = try fixture.numeric.copyRows(std.testing.allocator);
    defer {
        for (copied) |row| std.testing.allocator.free(row);
        std.testing.allocator.free(copied);
    }
    fixture.deinit();
    try std.testing.expectEqual(@as(usize, 0), budget.used().retained);
    try std.testing.expectEqualSlices(f32, &.{ 0.25, -0.5 }, copied[0]);
    try std.testing.expectEqualSlices(f32, &.{1.0}, copied[1]);
    try std.testing.expectEqual(@as(usize, 0), copied[2].len);
}

test "inference worker numeric decode quota and malformed rows unwind all derived allocations" {
    var body: [8]u8 = undefined;
    std.mem.writeInt(u32, body[0..4], @bitCast(@as(f32, 0.25)), .little);
    std.mem.writeInt(u32, body[4..8], @bitCast(@as(f32, 0.5)), .little);
    const output = wire.ProviderOutput{ .kind = .dense_vectors, .row_lengths = &.{ 1, 1 } };
    const budget = try rpc.PayloadBudget.create(std.testing.allocator, .{ .retained = 2 * @sizeOf([]f32) + 4 });
    defer budget.releaseOwner();
    const alloc = budget.retainedAllocator();
    try std.testing.expectError(error.OutOfMemory, output.decode(alloc, &body));
    try std.testing.expectEqual(@as(usize, 0), budget.used().retained);
    // Increase only this test's idle budget after the partial-row unwind.
    budget.limits.retained += 4;
    std.mem.writeInt(u32, body[4..8], @bitCast(std.math.nan(f32)), .little);
    try std.testing.expectError(error.InvalidInferenceNumericResult, output.decode(alloc, &body));
    try std.testing.expectEqual(@as(usize, 0), budget.used().retained);
}

test "inference worker provider publication refusal consumes results without publishing a handle" {
    const budget = try rpc.PayloadBudget.create(std.testing.allocator, .{ .retained = 1 });
    defer budget.releaseOwner();
    const alloc = budget.retainedAllocator();
    var fixture: ProviderPublicationFixture = .{};
    defer fixture.deinit();
    var context = fixture.context();
    const bytes = try alloc.dupe(u8, "x");
    try std.testing.expectError(error.OutOfMemory, host.publishWorkerProviderResponse(&context, .{ .json = .{ .bytes = bytes, .alloc = alloc } }));
    try std.testing.expect(fixture.handle == null and fixture.json.len == 0);
    try std.testing.expectEqual(@as(usize, 0), budget.used().retained);
    const Canceled = struct {
        fn check(_: ?*const anyopaque) callconv(.c) u8 {
            return 1;
        }
    };
    context.cancellation = .{ .context = null, .is_cancelled = Canceled.check };
    const canceled = try alloc.dupe(u8, "x");
    try std.testing.expectError(error.Canceled, host.publishWorkerProviderResponse(&context, .{ .json = .{ .bytes = canceled, .alloc = alloc } }));
    try std.testing.expect(fixture.handle == null);
    try std.testing.expectEqual(@as(usize, 0), budget.used().retained);
}

test "inference worker published provider response retains its allocator after the client anchor retires" {
    const budget = try rpc.PayloadBudget.create(std.testing.allocator, .{});
    var owner_live = true;
    defer if (owner_live) budget.releaseOwner();
    const alloc = budget.retainedAllocator();
    var fixture: ProviderPublicationFixture = .{};
    defer fixture.deinit();
    const context = fixture.context();
    const bytes = try alloc.dupe(u8, "owned");
    try host.publishWorkerProviderResponse(&context, .{ .json = .{ .bytes = bytes, .alloc = alloc } });
    budget.releaseOwner();
    owner_live = false;
    try std.testing.expectEqualStrings("owned", fixture.json.slice());
    fixture.deinit();
}

const worker_capability_fixture_json = "{\"task\":\"read\",\"input_modalities\":{\"image\":true},\"input_granularity\":\"page\",\"output\":\"read_result\",\"borrowed_rasters\":true}";

test "inference worker capability rewriting retains budgeted output and preserves transport constraints" {
    const budget = try rpc.PayloadBudget.create(std.testing.allocator, .{});
    defer budget.releaseOwner();
    const alloc = budget.retainedAllocator();
    const bytes = try alloc.dupe(u8, worker_capability_fixture_json);
    var fixture: ProviderPublicationFixture = .{};
    defer fixture.deinit();
    var context = fixture.context();
    context.operation = @backingInt(bridge.ProviderOperation.model_capabilities);
    try host.publishWorkerProviderResponse(&context, .{ .json = .{ .bytes = bytes, .alloc = alloc } });
    const work = @import("antfly_inference_work");
    const parsed = try std.json.parseFromSlice(work.InferenceCapabilities, std.testing.allocator, fixture.json.slice(), .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(?usize, rpc.max_body_bytes), parsed.value.attachment_envelope_max_bytes);
    try std.testing.expectEqual(@as(?usize, wire.provider_attachment_limits.max_metadata_bytes), parsed.value.attachment_metadata_max_bytes);
    try std.testing.expect(parsed.value.borrowed_rasters);
    try std.testing.expect(budget.used().retained >= fixture.json.len);
    try std.testing.expectEqual(@as(usize, 0), budget.used().control);
    fixture.deinit();
    try std.testing.expectEqual(@as(usize, 0), budget.used().retained);
}

test "inference worker capability rewriting allocation failures leave no derived debt or partial handle" {
    const Probe = struct {
        fn run(backing: std.mem.Allocator) !void {
            const budget = try rpc.PayloadBudget.create(backing, .{});
            defer budget.releaseOwner();
            const alloc = budget.retainedAllocator();
            var fixture: ProviderPublicationFixture = .{};
            defer {
                fixture.deinit();
                std.debug.assert(budget.used().retained == 0 and budget.used().control == 0);
            }
            var context = fixture.context();
            context.operation = @backingInt(bridge.ProviderOperation.model_capabilities);
            const bytes = try alloc.dupe(u8, worker_capability_fixture_json);
            host.publishWorkerProviderResponse(&context, .{ .json = .{ .bytes = bytes, .alloc = alloc } }) catch |err| {
                try std.testing.expect(fixture.handle == null and fixture.json.len == 0);
                return err;
            };
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

test "inference worker cancellation during numeric descriptor allocation refuses publication and releases debt" {
    const Backing = struct {
        armed: bool = false,
        canceled: bool = false,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = allocate, .free = free, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap } };
        }
        fn allocate(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const bytes = std.testing.allocator.rawAlloc(len, alignment, ret_addr) orelse return null;
            if (self.armed) self.canceled = true;
            return bytes;
        }
        fn free(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
            std.testing.allocator.rawFree(memory, alignment, ret_addr);
        }
        fn check(raw: ?*const anyopaque) callconv(.c) u8 {
            const self: *const @This() = @ptrCast(@alignCast(raw.?));
            return @intFromBool(self.canceled);
        }
    };
    var backing: Backing = .{};
    const budget = try rpc.PayloadBudget.create(backing.allocator(), .{});
    defer budget.releaseOwner();
    const alloc = budget.retainedAllocator();
    var body: [4]u8 = undefined;
    std.mem.writeInt(u32, &body, @bitCast(@as(f32, 0.25)), .little);
    const output = wire.ProviderOutput{ .kind = .scores, .row_lengths = &.{1} };
    const rows = try output.decode(alloc, &body);
    var fixture: ProviderPublicationFixture = .{};
    defer fixture.deinit();
    var context = fixture.context();
    context.cancellation = .{ .context = &backing, .is_cancelled = Backing.check };
    // The next backing allocation is the ABI row descriptor array, after the
    // initial control checks but before publication. Refusal must consume all
    // decoded rows and descriptors without publishing a partially live handle.
    backing.armed = true;
    try std.testing.expectError(error.Canceled, host.publishWorkerProviderResponse(&context, .{ .numeric = .{ .rows = rows, .kind = .scores, .alloc = alloc } }));
    try std.testing.expect(backing.canceled);
    try std.testing.expect(fixture.handle == null and fixture.numeric.kind == .absent);
    try std.testing.expectEqual(@as(usize, 0), budget.used().retained);
}

test "inference worker requires the node transient hook before retaining or spawning" {
    const Probe = struct {
        refs: usize = 0,
        fn retain(raw: *anyopaque) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refs += 1;
            return 1;
        }
        fn release(raw: *anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refs -= 1;
        }
    };
    var probe: Probe = .{};
    const budget = try rpc.PayloadBudget.create(std.testing.allocator, .{});
    defer budget.releaseOwner();
    var client: Client = .{ .alloc = std.testing.allocator, .io = std.testing.io, .create_json = undefined, .environment = undefined, .payload_budget = budget };
    const port: bridge.ResourceBudget = .{
        .abi_version = bridge.abi_version,
        .context = &probe,
        .retain_context = Probe.retain,
        .release_context = Probe.release,
        .reserve_admission = undefined,
        .retain_admission = undefined,
        .release_admission = undefined,
        .observe_prompt_cache = undefined,
        .observe_tokenizer_cache = undefined,
    };
    try std.testing.expectError(error.UnsupportedVersion, client.configure(port));
    try std.testing.expectEqual(@as(usize, 0), probe.refs);
    try std.testing.expect(client.worker == null and client.budget == null);
    try std.testing.expect(budget.external == null);
    try std.testing.expect(!bridge.validContext(bridge.ResourceBudget, 37, @sizeOf(bridge.ResourceBudget)));
    try std.testing.expect(!bridge.validContext(bridge.ResourceBudget, bridge.abi_version, @offsetOf(bridge.ResourceBudget, "adjust_transient_host")));
}

test "inference worker child heap uses dedicated replies and restores borrowed execution before allocation" {
    const io = std.testing.io;
    const Probe = struct {
        bytes: u64 = 0,
        parked: std.atomic.Value(bool) = .init(false),
        entries: usize = 0,
        fn check(_: *anyopaque) !void {}
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(!self.parked.swap(true, .acq_rel));
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(self.parked.swap(false, .acq_rel));
            self.entries += 1;
        }
        fn adjust(raw: *anyopaque, _: usize, previous: u64, next: u64) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (previous != self.bytes) return 0;
            if (next > previous and !self.parked.load(.acquire)) return 0;
            if (next > 64) return 0;
            self.bytes = next;
            return 1;
        }
        fn closed(_: *anyopaque) void {}
    };
    var probe: Probe = .{};
    var pair: rpc.TestPair = undefined;
    try pair.initUnstarted();
    var failing_control = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var client: Client = .{
        .alloc = failing_control.allocator(),
        .io = io,
        .create_json = undefined,
        .environment = undefined,
        .budget = .{
            .abi_version = bridge.abi_version,
            .context = &probe,
            .retain_context = undefined,
            .release_context = undefined,
            .reserve_admission = undefined,
            .retain_admission = undefined,
            .release_admission = undefined,
            .observe_prompt_cache = undefined,
            .observe_tokenizer_cache = undefined,
            .adjust_transient_host = Probe.adjust,
        },
    };
    var worker: Worker = .{ .owner = &client, .child = undefined, .endpoint = pair.parent };
    worker.endpoint.alloc = failing_control.allocator();
    worker.endpoint.context = &worker;
    worker.endpoint.resource_handler = Worker.resourceRequest;
    worker.endpoint.on_closed = Probe.closed;
    var heap: worker_heap.Heap = undefined;
    try heap.init(std.testing.allocator, &pair.child);
    pair.child.compute_allocator = heap.allocator();
    defer {
        worker.endpoint.control_bytes = 0;
        worker.endpoint.retained_bytes = 0;
        worker.endpoint.fail();
        pair.child.deinit();
        worker.endpoint.releasePeerAfterPhysicalJoin();
        worker.endpoint.deinit();
        heap.deinit();
        worker.releaseResources();
        for (pair.files) |file| file.close(io);
    }
    try worker.endpoint.start();
    try pair.child.start();
    // Resource requests and their fixed replies must work with both ordinary
    // allowances saturated; copyPayload cannot hold the child's reader lock.
    worker.endpoint.control_bytes = 4 * 1024 * 1024;
    worker.endpoint.retained_bytes = 256 * 1024 * 1024;
    var scope = execution_control.CheckpointScope.init(.{ .context = &probe, .check_fn = Probe.check, .wait_begin_fn = Probe.begin, .wait_end_fn = Probe.end });
    defer scope.deinit();
    const result = try pair.child.copyPayload(.{ .body = "child result" });
    var live = true;
    defer if (live) result.deinit();
    try std.testing.expectEqual(@as(u64, "child result".len), worker.child_transient);
    try std.testing.expectEqual(@as(usize, 1), probe.entries);
    try std.testing.expect(!probe.parked.load(.acquire));
    result.deinit();
    live = false;
    try std.testing.expectEqual(@as(u64, 0), worker.child_transient);
    try std.testing.expectEqual(@as(u64, 0), probe.bytes);
    try std.testing.expectError(error.OutOfMemory, heap.allocator().alloc(u8, 128));
    try std.testing.expectEqual(@as(u64, 0), worker.child_transient);
    try std.testing.expect(!worker.endpoint.closed.load(.acquire));
    const retry = try heap.allocator().alloc(u8, 8);
    heap.allocator().free(retry);
    try std.testing.expectEqual(@as(u64, 0), probe.bytes);
    try std.testing.expectEqual(@as(usize, 3), probe.entries);
}

test "inference worker fixed baseline refuses before allocation and rolls back failed startup" {
    const Probe = struct {
        used: u64 = 0,
        limit: u64 = 0,
        adjustments: usize = 0,
        fn adjust(raw: *anyopaque, _: usize, previous: u64, next: u64) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (previous != self.used or next > self.limit) return 0;
            self.used = next;
            self.adjustments += 1;
            return 1;
        }
    };
    var probe: Probe = .{};
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var client: Client = .{ .alloc = failing.allocator(), .configuration_allocator = failing.allocator(), .io = std.testing.io, .create_json = undefined, .environment = undefined, .budget = .{
        .abi_version = bridge.abi_version,
        .context = &probe,
        .retain_context = undefined,
        .release_context = undefined,
        .reserve_admission = undefined,
        .retain_admission = undefined,
        .release_admission = undefined,
        .observe_prompt_cache = undefined,
        .observe_tokenizer_cache = undefined,
        .adjust_transient_host = Probe.adjust,
    } };
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, client.ensureWorker(.{}));
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
    try std.testing.expectEqual(@as(u64, 0), client.worker_baseline);
    probe.limit = Client.worker_fixed_bytes;
    try std.testing.expectError(error.OutOfMemory, client.ensureWorker(.{}));
    try std.testing.expectEqual(@as(u64, 0), client.worker_baseline);
    try std.testing.expectEqual(@as(u64, 0), probe.used);
    try std.testing.expectEqual(@as(usize, 2), probe.adjustments);
    try client.reserveWorkerBaseline();
    try std.testing.expectEqual(@as(u64, Client.worker_fixed_bytes), probe.used);
    client.releaseWorkerBaseline();
    try std.testing.expectEqual(@as(u64, 0), probe.used);
}

test "inference worker metadata refuses before mutation and retires under exhausted quotas" {
    const Probe = struct {
        memory: u64 = 0,
        limit: u64 = 64 * 1024,
        reserves: usize = 0,
        releases: usize = 0,
        cached: u64 = 0,
        cache_calls: usize = 0,
        deny_cache: bool = false,
        fn retain(_: *anyopaque) callconv(.c) u8 {
            return 1;
        }
        fn drop(_: *anyopaque) callconv(.c) void {}
        fn adjust(raw: *anyopaque, _: usize, previous: u64, next: u64) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (previous != self.memory or (next > previous and next > self.limit)) return 0;
            self.memory = next;
            return 1;
        }
        fn reserve(raw: *anyopaque, _: *const bridge.AdmissionAmounts, out: *usize) callconv(.c) bridge.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reserves += 1;
            out.* = 7;
            return .ok;
        }
        fn release(raw: *anyopaque, lease: usize) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(lease == 7);
            self.releases += 1;
        }
        fn observe(raw: *anyopaque, _: usize, previous: u64, next: u64) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.cache_calls += 1;
            if (self.cached != previous or self.deny_cache) return 0;
            self.cached = next;
            return 1;
        }
    };
    const alloc = std.testing.allocator;
    var probe: Probe = .{};
    var client: Client = .{ .alloc = alloc, .io = std.testing.io, .create_json = undefined, .environment = undefined, .budget = .{
        .abi_version = bridge.abi_version,
        .context = &probe,
        .retain_context = Probe.retain,
        .release_context = Probe.drop,
        .reserve_admission = Probe.reserve,
        .retain_admission = undefined,
        .release_admission = Probe.release,
        .observe_prompt_cache = Probe.observe,
        .observe_tokenizer_cache = Probe.observe,
        .adjust_transient_host = Probe.adjust,
    } };
    const metadata = try Worker.createMetadataBudget(alloc, client.budget.?);
    defer metadata.releaseOwner();
    var worker: Worker = .{ .owner = &client, .child = undefined, .metadata_budget = metadata, .endpoint = .{
        .alloc = alloc,
        .io = std.testing.io,
        .input = undefined,
        .output = undefined,
        .context = undefined,
        .handler = Worker.rejectRequest,
        .on_closed = undefined,
        .next_id = 1,
    } };
    defer worker.releaseResources();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const reserve_data = try std.json.Stringify.valueAlloc(arena.allocator(), wire.Reservation{ .amounts = .{
        .host_weight_bytes = 1,
        .backend_weight_bytes = 0,
        .host_kv_bytes = 0,
        .backend_kv_bytes = 0,
        .host_scratch_bytes = 0,
        .backend_scratch_bytes = 0,
    } }, .{});
    const reserve = try std.json.Stringify.valueAlloc(arena.allocator(), wire.ResourceRequest{ .operation = .reserve, .data = reserve_data }, .{});
    const observe = try std.json.Stringify.valueAlloc(arena.allocator(), wire.ResourceRequest{ .operation = .prompt_cache, .data = "{\"key\":9,\"previous\":0,\"next\":64}" }, .{});
    // No root/model mutation occurs if its tracking allocation cannot be owned.
    metadata.limits.retained = 0;
    const refused = try Worker.resourceRequest(&worker, reserve);
    try std.testing.expectEqual(bridge.statusFromError(error.ResourceTemporarilyUnavailable).detail, refused.detail);
    try std.testing.expectEqual(@as(usize, 0), probe.reserves);
    const refused_cache = try Worker.resourceRequest(&worker, observe);
    try std.testing.expectEqual(bridge.statusFromError(error.ResourceTemporarilyUnavailable).detail, refused_cache.detail);
    try std.testing.expectEqual(@as(usize, 0), probe.cache_calls);
    try std.testing.expectEqual(@as(u64, 0), probe.memory);
    metadata.limits.retained = Worker.metadata_limit;
    try validateResourceReply(.reserve, try Worker.resourceRequest(&worker, reserve));
    try std.testing.expectEqual(@as(usize, 1), probe.reserves);
    // A denied cache callback must not retain an unpublished observation object.
    probe.deny_cache = true;
    const denied = try Worker.resourceRequest(&worker, observe);
    try std.testing.expectEqual(bridge.statusFromError(error.ResourceLimitExceeded).detail, denied.detail);
    try std.testing.expectEqual(@as(usize, 0), worker.observations.items.len);
    const after_denial = metadata.used().retained;
    _ = try Worker.resourceRequest(&worker, observe);
    try std.testing.expectEqual(after_denial, metadata.used().retained);
    probe.deny_cache = false;
    try validateResourceReply(.prompt_cache, try Worker.resourceRequest(&worker, observe));
    try std.testing.expectEqual(@as(u64, 64), probe.cached);
    try std.testing.expectEqual(@as(u64, metadata.used().retained), probe.memory);
    // Bulk exhaustion is independent of this metadata allowance. Tightening
    // both metadata and node limits still permits physical teardown.
    worker.endpoint.control_bytes = 4 * 1024 * 1024;
    worker.endpoint.retained_bytes = 256 * 1024 * 1024;
    metadata.limits.retained = 0;
    probe.limit = 0;
    const release = try std.json.Stringify.valueAlloc(arena.allocator(), wire.ResourceRequest{ .operation = .release, .data = "7" }, .{});
    try validateResourceReply(.release, try Worker.resourceRequest(&worker, release));
    try std.testing.expectEqual(@as(usize, 1), probe.releases);
    worker.releaseResources();
    try std.testing.expectEqual(@as(u64, 0), probe.memory);
    try std.testing.expectEqual(@as(u64, 0), probe.cached);
    try std.testing.expectEqual(@as(usize, 0), metadata.used().retained);
    try std.testing.expect(!worker.endpoint.closed.load(.acquire));
}

test "inference worker observational cache denials require a physical fence" {
    for ([_]anyerror{ error.ResourceTemporarilyUnavailable, error.ResourceLimitExceeded }) |err| {
        const denied = bridge.statusFromError(err);
        try std.testing.expect(worker_lifetime.cacheObservationMustFence(true, 0, 64, denied.isOk()));
        try std.testing.expect(worker_lifetime.cacheObservationMustFence(true, 64, 0, denied.isOk()));
        try std.testing.expect(!worker_lifetime.cacheObservationMustFence(false, 0, 64, denied.isOk()));
        try std.testing.expect(worker_lifetime.cacheObservationMustFence(false, 64, 0, denied.isOk()));
    }
    for ([_]wire.Operation{ .prompt_cache, .tokenizer_cache }) |operation| {
        try std.testing.expect(!worker_lifetime.cacheObservationMustFence(operation == .prompt_cache, 0, 64, true));
        try std.testing.expect(!worker_lifetime.cacheObservationMustFence(operation == .prompt_cache, 64, 0, true));
    }
}

test "inference worker HTTP marshaling and ABI response retain node ownership after client retirement" {
    const Probe = struct {
        bytes: u64 = 0,
        refs: usize = 1,
        handled: usize = 0,
        fn retain(raw: *anyopaque) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refs += 1;
            return 1;
        }
        fn release(raw: *anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refs -= 1;
        }
        fn adjust(raw: *anyopaque, _: usize, previous: u64, next: u64) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.bytes != previous) return 0;
            self.bytes = next;
            return 1;
        }
        fn handle(raw: *anyopaque, request: *rpc.Request) !rpc.OwnedPayload {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.handled += 1;
            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();
            const envelope = try requestEnvelope(arena.allocator(), request);
            try std.testing.expectEqual(wire.Operation.http, envelope.operation);
            const value = try std.json.parseFromSliceLeaky(wire.Http, arena.allocator(), envelope.options, .{});
            try std.testing.expectEqualStrings("/chat", value.route);
            try std.testing.expectEqualStrings("value", value.headers[0].value);
            return reply(request.endpoint, .ok, "{\"status\":200,\"headers\":[{\"name\":\"x-worker\",\"value\":\"owned\"}]}", "done");
        }
    };
    var probe: Probe = .{};
    const port: bridge.ResourceBudget = .{ .abi_version = bridge.abi_version, .context = &probe, .retain_context = Probe.retain, .release_context = Probe.release, .reserve_admission = undefined, .retain_admission = undefined, .release_admission = undefined, .observe_prompt_cache = undefined, .observe_tokenizer_cache = undefined, .adjust_transient_host = Probe.adjust };
    const budget = try rpc.PayloadBudget.create(std.testing.allocator, .{ .retained = 0 });
    var anchored = true;
    defer if (anchored) budget.releaseOwner();
    try budget.configureExternal(.{ .context = &probe, .retain = Probe.retain, .release = Probe.release, .adjust = Probe.adjust });
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var client: Client = .{ .alloc = failing.allocator(), .io = std.testing.io, .create_json = undefined, .environment = undefined, .payload_budget = budget, .budget = port };
    var pair: rpc.TestPair = undefined;
    try pair.initUnstarted();
    var worker: Worker = .{ .owner = &client, .child = undefined, .endpoint = pair.parent };
    worker.endpoint.payload_budget = budget;
    worker.endpoint.compute_allocator = budget.retainedAllocator();
    pair.child.context = &probe;
    pair.child.handler = Probe.handle;
    client.worker = &worker;
    var network_live = true;
    defer if (network_live) {
        worker.endpoint.fail();
        pair.child.deinit();
        worker.endpoint.deinit();
        for (pair.files) |file| file.close(std.testing.io);
    };
    try worker.endpoint.start();
    try pair.child.start();
    const headers = [_]http.HeaderView{.{ .name = .init("x-input"), .value = .init("value") }};
    const request: http.HttpRequestView = .{ .method = .post, .path = .init("/chat"), .headers_ptr = &headers, .headers_len = headers.len, .body = .init("input") };
    var handle: ?*anyopaque = null;
    var view: http.HttpResponseView = undefined;
    const context: bridge.HttpHandleContext = .{ .abi_version = bridge.abi_version, .route_handle = &client, .request = &request, .out_response_handle = &handle, .out_response = &view };
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, invokeHttp(&client, "/chat", &context));
    try std.testing.expectEqual(@as(usize, 0), probe.handled);
    try std.testing.expectEqual(@as(u64, 0), probe.bytes);
    budget.limits.retained = 1024 * 1024;
    var response = try invokeHttp(&client, "/chat", &context);
    var response_live = true;
    defer if (response_live) response.deinit();
    try std.testing.expectEqualStrings("done", response.body.?);
    try std.testing.expectEqualStrings("owned", response.headers.get("x-worker").?);
    try std.testing.expect(probe.bytes > response.body.?.len);
    worker.endpoint.fail();
    pair.child.deinit();
    worker.endpoint.deinit();
    for (pair.files) |file| file.close(std.testing.io);
    network_live = false;
    const result_alloc = response.allocator;
    budget.releaseOwner();
    anchored = false;
    // Header removal cannot destroy the response's allocator owner.
    response.headers.deinit();
    response.headers = httpx.Headers.init(result_alloc);
    try response.headers.append("x-after-retirement", "yes");
    response_live = false; // The production publisher consumes every path.
    try host.publishHttpResponse(&context, response, result_alloc);
    try std.testing.expect(handle != null);
    try std.testing.expectEqualStrings("done", view.body.slice());
    try std.testing.expectEqualStrings("x-after-retirement", view.headers_ptr.?[0].name.slice());
    host.linkedInferenceDestroyHttpResponse(handle.?);
    handle = null;
    // Static error bodies have no received slab to retain their allocator.
    const static_budget = try rpc.PayloadBudget.create(std.testing.allocator, .{});
    var static_anchor = true;
    defer if (static_anchor) static_budget.releaseOwner();
    try static_budget.configureExternal(.{ .context = &probe, .retain = Probe.retain, .release = Probe.release, .adjust = Probe.adjust });
    var static_response = try initHttpResponse(static_budget.retainedAllocator(), 503);
    var static_live = true;
    defer if (static_live) static_response.deinit();
    static_response.body = "capacity unavailable";
    try static_response.headers.append("content-type", "text/plain");
    static_budget.releaseOwner();
    static_anchor = false;
    static_response.headers.deinit();
    static_response.headers = httpx.Headers.init(static_response.allocator);
    const temporary = try static_response.allocator.dupe(u8, "allocator survives cleared headers");
    static_response.allocator.free(temporary);
    try std.testing.expect(probe.bytes > 0);
    static_response.deinit();
    static_live = false;
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
    try std.testing.expectEqual(@as(u64, 0), probe.bytes);
    try std.testing.expectEqual(@as(usize, 1), probe.refs);
}

test "inference worker unconfigured marshaling refuses before reading request metadata" {
    var client: Client = .{ .alloc = std.testing.allocator, .io = std.testing.io, .create_json = undefined, .environment = undefined };
    try std.testing.expectError(error.ResourceOwnerNotConfigured, invokeHttp(&client, "unused", undefined));
    try std.testing.expectError(error.ResourceOwnerNotConfigured, invokeProvider(&client, undefined));
}

test "inference worker controlled event serialization uses and releases its compute quota" {
    const Probe = struct {
        budget: *rpc.PayloadBudget,
        events: usize = 0,
        fn check(_: ?*anyopaque) !void {}
        fn handle(_: *anyopaque, request: *rpc.Request) !rpc.OwnedPayload {
            var control: HttpControl.Parent = .{ .original_ptr = null, .original_check = check, .original_deadline_ns = null, .event_deadline_ns = null, .event = .{} };
            const view = control.view();
            try HttpControl.result(Child.streamWriteControlled(request, .init("chunk"), &view));
            return request.endpoint.copyPayload(.{ .body = "done" });
        }
        fn event(raw: ?*anyopaque, _: rpc.Payload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expect(self.budget.used().retained > 0);
            self.events += 1;
        }
    };
    const budget = try rpc.PayloadBudget.create(std.testing.allocator, .{ .retained = 0 });
    defer budget.releaseOwner();
    var pair: rpc.TestPair = undefined;
    try pair.initUnstarted();
    pair.child.compute_allocator = budget.retainedAllocator();
    pair.child.handler = Probe.handle;
    var probe = Probe{ .budget = budget };
    defer pair.deinit();
    try pair.parent.start();
    try pair.child.start();
    try std.testing.expectError(error.InferenceWorkerRequestFailed, pair.parent.call(.{ .body = "stream" }, .{ .ptr = &probe, .event = Probe.event }));
    try std.testing.expectEqual(@as(usize, 0), probe.events);
    try std.testing.expectEqual(@as(usize, 0), budget.used().retained);
    budget.limits.retained = 1024 * 1024;
    const response = try pair.parent.call(.{ .body = "stream" }, .{ .ptr = &probe, .event = Probe.event });
    defer response.deinit();
    try std.testing.expectEqualStrings("done", response.view().body);
    try std.testing.expectEqual(@as(usize, 1), probe.events);
    try std.testing.expect(!pair.child.closed.load(.acquire));
}

test "inference worker child HTTP reconstruction and response publication use scratch ownership" {
    try host.qualifyChildHttpScratchOwnership();
}

const ConfigurationTest = struct {
    const Probe = struct {
        keys: [2]usize = @splat(0),
        bytes: [2]u64 = @splat(0),
        total: u64 = 0,
        bootstrap: u64 = 0,
        busy: bool = false,
        limit: u64 = 16 * 1024 * 1024,
        references: usize = 0,
        fn retain(raw: *anyopaque) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.references += 1;
            return 1;
        }
        fn release(raw: *anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.references != 0);
            if (self.references == 1) std.debug.assert(self.total == 0);
            self.references -= 1;
        }
        fn adjust(raw: *anyopaque, key: usize, previous: u64, next: u64) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var index: ?usize = null;
            for (self.keys, 0..) |known, i| if (known == key) {
                index = i;
                break;
            };
            if (index == null) for (self.keys, 0..) |known, i| if (known == 0) {
                index = i;
                break;
            };
            const i = index orelse return 0;
            if (self.bytes[i] != previous or (next > previous and next - previous > self.limit -| self.total)) return 0;
            self.total = self.total - previous + next;
            self.bytes[i] = next;
            self.keys[i] = if (next == 0) 0 else key;
            return 1;
        }
        fn reserveBootstrap(raw: *anyopaque, bytes: u64) callconv(.c) bridge.MemoryParent.ReserveResult {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.busy) return .busy;
            if (bytes > self.limit -| self.total) return .denied;
            self.total += bytes;
            self.bootstrap += bytes;
            return .reserved;
        }
        fn releaseBootstrap(raw: *anyopaque, bytes: u64) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(bytes <= self.bootstrap);
            self.bootstrap -= bytes;
            self.total -= bytes;
        }
        fn parent(self: *@This()) bridge.MemoryParent {
            return .{ .ptr = self, .reserve_fn = undefined, .reclaim_fn = undefined, .observe_fn = undefined, .release_fn = releaseBootstrap, .snapshot_fn = undefined, .epoch_fn = undefined, .wait_fn = undefined, .notify_fn = undefined, .try_reserve_fn = reserveBootstrap };
        }
        fn port(self: *@This()) bridge.ResourceBudget {
            return .{
                .abi_version = bridge.abi_version,
                .context = self,
                .retain_context = retain,
                .release_context = release,
                .reserve_admission = undefined,
                .retain_admission = undefined,
                .release_admission = undefined,
                .observe_prompt_cache = undefined,
                .observe_tokenizer_cache = undefined,
                .adjust_transient_host = adjust,
            };
        }
    };

    fn create(alloc: std.mem.Allocator, probe: *Probe) !*Client {
        const environment = [_]EnvironmentEntry{
            .{ .name = "VISIBLE", .value = "exact λ \"quoted\" value" },
            .{ .name = platform.inference_process_supervisor.worker_env, .value = "skip" },
            .{ .name = "ANTFLY_INFERENCE_SUPERVISOR_LIFELINE", .value = "skip" },
        };
        const settings = try std.json.Stringify.valueAlloc(alloc, .{ .worker_environment = &environment }, .{});
        defer alloc.free(settings);
        var handle: ?*anyopaque = null;
        var io = std.testing.io;
        var parent = probe.parent();
        const context: bridge.CreateContext = .{
            .abi_version = bridge.abi_version,
            .data_dir_ptr = "worker-configuration-test".ptr,
            .data_dir_len = "worker-configuration-test".len,
            .models_dir = .init("models"),
            .ml_dir = .init("ml"),
            .host_limit_bytes = 0,
            .backend_limit_bytes = 0,
            .combined_limit_bytes = 0,
            .kv_limit_bytes = 0,
            .scratch_limit_bytes = 0,
            .process_memory_limit_bytes = 0,
            .preload_ptr = null,
            .preload_len = 0,
            .keep_alive = .init(null),
            .max_loaded_models = 0,
            .has_max_loaded_models = 0,
            .content_security_json = .init(null),
            .s3_credentials_json = .init(null),
            .runtime_config_json = .init(settings),
            .executor = .init(&io),
            .out_handle = &handle,
            .bootstrap_memory_parent = &parent,
        };
        return Client.create(alloc, io, &context);
    }

    fn allocationCase(alloc: std.mem.Allocator) !void {
        var probe: Probe = .{};
        defer std.debug.assert(probe.total == 0 and probe.references == 0);
        const client = try create(alloc, &probe);
        defer client.deinit();
        try std.testing.expectEqual(@as(usize, 0), client.environment.count());
        try client.configureMemory(probe.port());
        try std.testing.expect(client.configuration_allocator != null);
        try std.testing.expectEqual(@as(usize, 1), client.environment.count());
        try std.testing.expectEqualStrings("exact λ \"quoted\" value", client.environment.get("VISIBLE").?);
        try std.testing.expect(client.environment.get(platform.inference_process_supervisor.worker_env) == null);
        try std.testing.expect(client.environment.get("ANTFLY_INFERENCE_SUPERVISOR_LIFELINE") == null);
        try std.testing.expectEqualStrings("worker-configuration-test", client.data_dir.?);
        try std.testing.expectEqual(@as(u64, @intCast(client.payload_budget.?.used().retained)) + probe.bootstrap, probe.total);
        try std.testing.expectEqual(@as(u64, @sizeOf(worker_heap.Bootstrap) + @sizeOf(Client) + @sizeOf(rpc.PayloadBudget)), probe.bootstrap);
        try std.testing.expect(probe.total >= client.create_json.len + client.data_dir.?.len);
        // Worker generations can come and go without dropping configuration.
        const retained = probe.total;
        try client.reserveWorkerBaseline();
        try std.testing.expectEqual(retained + Client.worker_fixed_bytes, probe.total);
        client.releaseWorkerBaseline();
        try std.testing.expectEqual(retained, probe.total);
    }
};

test "inference worker configured launch metadata uses node admission and survives generation retirement" {
    try ConfigurationTest.allocationCase(std.testing.allocator);
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, ConfigurationTest.allocationCase, .{});
}

test "inference worker refused configuration cannot launch or bypass its node owner" {
    var probe: ConfigurationTest.Probe = .{};
    defer std.debug.assert(probe.total == 0 and probe.references == 0);
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const client = try ConfigurationTest.create(failing.allocator(), &probe);
    defer client.deinit();
    const allocations = failing.alloc_index;
    const bootstrap_bytes = probe.total;
    probe.limit = bootstrap_bytes;
    try std.testing.expectError(error.OutOfMemory, client.configureMemory(probe.port()));
    try std.testing.expect(client.configuration_allocator == null and client.worker == null);
    try std.testing.expectEqual(@as(usize, 0), client.environment.count());
    try std.testing.expectEqual(allocations, failing.alloc_index);
    probe.limit = 16 * 1024 * 1024;
    try std.testing.expectError(error.ResourceOwnerNotConfigured, client.ensureWorker(.{}));
    try std.testing.expectEqual(allocations, failing.alloc_index);
    try std.testing.expectError(error.ExternalResourceBudgetsAlreadyConfigured, client.configureMemory(probe.port()));
    try std.testing.expectEqual(bootstrap_bytes, probe.total);
}

test "inference worker output retains its independent node edge after configured client destruction" {
    var probe: ConfigurationTest.Probe = .{};
    defer std.debug.assert(probe.total == 0 and probe.references == 0);
    const client = try ConfigurationTest.create(std.testing.allocator, &probe);
    var live = true;
    defer if (live) client.deinit();
    try client.configureMemory(probe.port());
    const allocator = try client.resultAllocator();
    const output = try allocator.dupe(u8, "output outlives launch configuration");
    defer allocator.free(output);
    try std.testing.expectEqual(@as(usize, 2), probe.references);
    client.deinit();
    live = false;
    try std.testing.expectEqual(@as(u64, @sizeOf(worker_heap.Bootstrap) + @sizeOf(rpc.PayloadBudget)), probe.bootstrap);
    try std.testing.expectEqual(@as(u64, output.len) + probe.bootstrap, probe.total);
    try std.testing.expectEqual(@as(usize, 1), probe.references);
    try std.testing.expectEqualStrings("output outlives launch configuration", output);
}

test "inference worker bootstrap refuses missing invalid busy and exhausted parents before allocation" {
    var probe: ConfigurationTest.Probe = .{ .limit = 0 };
    defer std.debug.assert(probe.total == 0);
    var parent = probe.parent();
    var context: bridge.CreateContext = undefined;
    context.bootstrap_memory_parent = null;
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.ResourceOwnerNotConfigured, Client.create(failing.allocator(), std.testing.io, &context));
    context.bootstrap_memory_parent = &parent;
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, Client.create(failing.allocator(), std.testing.io, &context));
    probe.limit = 1024 * 1024;
    probe.busy = true;
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, Client.create(failing.allocator(), std.testing.io, &context));
    parent.version -= 1;
    try std.testing.expectError(error.UnsupportedVersion, Client.create(failing.allocator(), std.testing.io, &context));
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
}

test "inference worker child initialization accounts owned host metadata after launch arena retirement" {
    const io = std.testing.io;
    var probe: ConfigurationTest.Probe = .{};
    defer std.debug.assert(probe.total == 0);
    const launch = try ConfigurationTest.create(std.testing.allocator, &probe);
    defer launch.deinit();
    const bootstrap_bytes = probe.total;
    var pair: rpc.TestPair = undefined;
    try pair.initUnstarted();
    defer for (pair.files) |file| file.close(io);
    var client: Client = .{ .alloc = std.testing.allocator, .io = io, .create_json = undefined, .environment = undefined, .budget = probe.port() };
    var parent: Worker = .{ .owner = &client, .child = undefined, .endpoint = pair.parent };
    parent.endpoint.context = &parent;
    parent.endpoint.resource_handler = Worker.resourceRequest;
    parent.endpoint.handler = Worker.rejectRequest;
    var child: Child = .{ .alloc = std.testing.allocator, .io = io, .endpoint = pair.child, .create_arena = undefined };
    child.endpoint.context = &child;
    child.endpoint.handler = Child.dispatch;
    try child.heap.init(std.testing.allocator, &child.endpoint);
    child.endpoint.compute_allocator = child.heap.allocator();
    child.create_arena = std.heap.ArenaAllocator.init(child.heap.allocator());
    defer {
        if (child.state) |state| host.linkedInferenceDestroy(state);
        child.create_arena.deinit();
        parent.endpoint.fail();
        child.endpoint.deinit();
        parent.endpoint.releasePeerAfterPhysicalJoin();
        parent.endpoint.deinit();
        child.heap.deinit();
        parent.releaseResources();
    }
    try parent.endpoint.start();
    try child.endpoint.start();
    const response = try call(&parent.endpoint, .initialize, launch.create_json, .{});
    defer std.testing.allocator.free(response);
    // Join the physical response free while keeping resource replies available.
    try child.endpoint.handlers.await(io);
    try std.testing.expectEqual(@as(usize, 0), child.create_arena.queryCapacity());
    try std.testing.expect(child.state != null and parent.child_transient >= @sizeOf(host.LinkedInferenceState));
    try std.testing.expectEqualStrings("exact λ \"quoted\" value", child.state.?.runtime_config.value.worker_environment[0].value);
    try std.testing.expectEqual(bootstrap_bytes + parent.child_transient, probe.total);
    const metadata_bytes = parent.child_transient;
    {
        var cache = try child.state.?.node.model_manager.initPromptCache();
        defer cache.deinit();
        try std.testing.expect(child.state.?.node.promptCacheRuntimeConfig(.native).resource_usage_observer == null);
        cache.configure(.{ .enabled = true, .mode = .simple, .min_tokens = 2, .max_bytes = 1024 * 1024 });
        const pool_id = (try cache.ensurePool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_kv_heads = 1, .head_dim = 2 })).?;
        const sequence = try cache.manager.attachSequence(pool_id);
        try cache.manager.appendTokens(sequence, 2);
        try cache.storeFromSequence("", &.{ 1, 2 }, sequence);
        try cache.manager.releaseSequence(sequence);
        try std.testing.expect(parent.child_transient > metadata_bytes);
        try std.testing.expectEqual(bootstrap_bytes + parent.child_transient, probe.total);
    }
    try std.testing.expectEqual(metadata_bytes, parent.child_transient);
    host.linkedInferenceDestroy(child.state.?);
    child.state = null;
    try std.testing.expectEqual(@as(u64, 0), parent.child_transient);
    try std.testing.expectEqual(@as(usize, 0), child.heap.budget.used().retained);
    try std.testing.expectEqual(bootstrap_bytes, probe.total);
}

test "inference worker batch transfer needs no publication allocation and joined cleanup owns both tokens" {
    const Budget = struct {
        amounts: [2]?usize = @splat(null),
        next: usize = 0,
        deny: bool = true,
        calls: usize = 0,
        fn retainContext(_: *anyopaque) callconv(.c) u8 {
            return 1;
        }
        fn releaseContext(_: *anyopaque) callconv(.c) void {}
        fn adjust(_: *anyopaque, _: usize, _: u64, _: u64) callconv(.c) u8 {
            return 1;
        }
        fn observe(_: *anyopaque, _: usize, _: u64, _: u64) callconv(.c) u8 {
            return 1;
        }
        fn reserve(raw: *anyopaque, value: *const bridge.AdmissionAmounts, out: *usize) callconv(.c) bridge.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.next == 2) return bridge.statusFromError(error.ResourceLimitExceeded);
            self.amounts[self.next] = value.host_kv_bytes;
            self.next += 1;
            out.* = self.next;
            return .ok;
        }
        fn retain(_: *anyopaque, _: usize, _: *const bridge.AdmissionAmounts) callconv(.c) bridge.Status {
            return .ok;
        }
        fn release(raw: *anyopaque, token: usize) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.amounts[token - 1] != null);
            self.amounts[token - 1] = null;
        }
        fn transfer(raw: *anyopaque, source: usize, destination: usize, value: *const bridge.AdmissionAmounts) callconv(.c) bridge.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.deny) return bridge.statusFromError(error.ResourceTemporarilyUnavailable);
            std.debug.assert(source == 1 and destination == 2 and value.host_kv_bytes == 12);
            self.amounts[0].? -= 12;
            self.amounts[1].? += 12;
            return .ok;
        }
    };
    const alloc = std.testing.allocator;
    var failing = std.testing.FailingAllocator.init(alloc, .{});
    var budget: Budget = .{};
    var client: Client = .{
        .alloc = failing.allocator(),
        .io = std.testing.io,
        .create_json = undefined,
        .environment = undefined,
        .budget = .{ .abi_version = bridge.abi_version, .context = &budget, .retain_context = Budget.retainContext, .release_context = Budget.releaseContext, .reserve_admission = Budget.reserve, .retain_admission = Budget.retain, .transfer_admission = Budget.transfer, .release_admission = Budget.release, .observe_prompt_cache = Budget.observe, .observe_tokenizer_cache = Budget.observe, .adjust_transient_host = Budget.adjust },
    };
    var worker: Worker = .{ .owner = &client, .child = undefined, .endpoint = .{ .alloc = failing.allocator(), .io = std.testing.io, .input = undefined, .output = undefined, .context = undefined, .handler = Worker.rejectRequest, .resource_handler = Worker.resourceRequest, .on_closed = Worker.closed, .next_id = 1 } };
    worker.metadata_budget = try Worker.createMetadataBudget(failing.allocator(), client.budget.?);
    defer worker.metadata_budget.?.releaseOwner();
    defer worker.releaseResources();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const initial: bridge.AdmissionAmounts = .{ .host_weight_bytes = 0, .backend_weight_bytes = 0, .host_kv_bytes = 32, .backend_kv_bytes = 0, .host_scratch_bytes = 0, .backend_scratch_bytes = 0 };
    var empty = initial;
    empty.host_kv_bytes = 0;
    for ([_]bridge.AdmissionAmounts{ initial, empty }) |value| {
        const data = try std.json.Stringify.valueAlloc(arena.allocator(), wire.Reservation{ .amounts = value }, .{});
        _ = try worker.resourceOperation(arena.allocator(), .{ .operation = .reserve, .data = data });
    }
    var moved = empty;
    moved.host_kv_bytes = 12;
    const data = try std.json.Stringify.valueAlloc(arena.allocator(), wire.Transfer{ .source = 1, .destination = 2, .amounts = moved }, .{});
    const request_json = try std.json.Stringify.valueAlloc(arena.allocator(), wire.ResourceRequest{ .operation = .transfer, .data = data }, .{});
    const allocations = failing.alloc_index;
    failing.fail_index = allocations;
    // Both tokens are registered before the shared pool or ordinary reply
    // budget becomes unavailable. Denial keeps both original quantities.
    worker.endpoint.control_bytes = 4 * 1024 * 1024;
    worker.endpoint.retained_bytes = 256 * 1024 * 1024;
    defer {
        worker.endpoint.control_bytes = 0;
        worker.endpoint.retained_bytes = 0;
    }
    const denied = try Worker.resourceRequest(&worker, request_json);
    try validateResourceReply(.transfer, denied);
    try std.testing.expectEqual(bridge.statusFromError(error.ResourceTemporarilyUnavailable).detail, denied.detail);
    try std.testing.expectEqual(@as(?usize, 32), budget.amounts[0]);
    try std.testing.expectEqual(@as(?usize, 0), budget.amounts[1]);
    budget.deny = false;
    // Simulate a successful owner handoff whose reply the child never sees.
    const reply_value = try Worker.resourceRequest(&worker, request_json);
    try validateResourceReply(.transfer, reply_value);
    try std.testing.expectEqual(@as(?usize, 20), budget.amounts[0]);
    try std.testing.expectEqual(@as(?usize, 12), budget.amounts[1]);
    try std.testing.expectEqual(@as(usize, 2), worker.leases.count());
    try std.testing.expectEqual(allocations, failing.alloc_index);
    const invalid = try std.json.Stringify.valueAlloc(arena.allocator(), wire.Transfer{ .source = 1, .destination = 99, .amounts = empty }, .{});
    try std.testing.expectError(error.InvalidGenerationAdmission, worker.resourceOperation(arena.allocator(), .{ .operation = .transfer, .data = invalid }));
    try std.testing.expectEqual(@as(usize, 2), budget.calls);
    worker.endpoint.closed.store(true, .release);
    // Production calls this only after physical child join; closure alone
    // above did not retire either token. This unit exercises joined cleanup.
    try std.testing.expect(budget.amounts[0] != null and budget.amounts[1] != null);
    worker.releaseResources();
    try std.testing.expect(budget.amounts[0] == null and budget.amounts[1] == null);
    try std.testing.expectEqual(@as(usize, 0), worker.leases.count());
    worker.releaseResources();
}

test "runnable execution worker budget carries controlled incremental uploads through production callbacks" {
    const io = std.testing.io;
    for ([_]bool{ false, true }) |stalled| {
        var pair: rpc.TestPair = undefined;
        try pair.init();
        defer pair.deinit();
        const Handler = struct {
            stalled: bool,
            rejected: bool = false,
            fn check(_: ?*anyopaque) !void {}
            fn run(raw: *anyopaque, request: *rpc.Request) !rpc.OwnedPayload {
                const self: *@This() = @ptrCast(@alignCast(raw));
                var authority: HttpControl.Parent = .{ .original_ptr = null, .original_check = check, .original_deadline_ns = platform.time.monotonicNs() + 100 * std.time.ns_per_ms, .event_deadline_ns = null, .event = .{} };
                const view = authority.view();
                var bytes: [4]u8 = undefined;
                var count: usize = 0;
                const result = Child.readBodyControlled(request, &bytes, bytes.len, &count, &view);
                if (self.stalled) {
                    try std.testing.expectEqual(http.CallbackStatus.timeout, result);
                    self.rejected = true;
                    return error.Timeout;
                }
                try HttpControl.result(result);
                try std.testing.expectEqual(@as(usize, 4), count);
                try std.testing.expectEqualStrings("pcm!", &bytes);
                try HttpControl.result(Child.readBodyControlled(request, &bytes, bytes.len, &count, &view));
                try std.testing.expectEqual(@as(usize, 0), count);
                return request.endpoint.copyPayload(.{ .body = "complete" });
            }
        };
        var handler: Handler = .{ .stalled = stalled };
        pair.child.context = &handler;
        pair.child.handler = Handler.run;
        var request = try httpx.Request.init(std.testing.allocator, .POST, "/upload");
        defer request.deinit();
        if (!stalled) request.body = "pcm!";
        var context = httpx.Context.init(std.testing.allocator, io, &request);
        defer context.deinit();
        var h2 = httpx.H2Connection.initServer(std.testing.allocator, io);
        defer h2.deinit();
        const stream = try h2.stream_manager.getOrCreateStream(1);
        stream.state = .open;
        var event: std.Io.Event = .unset;
        var reader: httpx.Context.H2StreamReader = .{ .h2_stream = stream, .io = io, .data_event = &event };
        if (stalled) context.h2_body_reader = &reader;
        var outbound: @import("antfly_runtime_abi").http_bridge.Outbound = .{ .context = &context };
        var control: ForwardControl = .{ .alloc = std.testing.allocator, .cancellation = .{}, .deadline_ns = platform.time.monotonicNs() + 2000 * std.time.ns_per_ms, .body_source = outbound.bodySource() };
        const started = platform.time.monotonicNs();
        if (stalled) {
            try std.testing.expectError(error.InferenceWorkerRequestFailed, pair.parent.call(.{ .body = "upload" }, control.view()));
            try std.testing.expect(handler.rejected);
            // The parent must cancel and physically join its source read before
            // releasing the callback's stack controls or the request context.
            try std.testing.expect(!reader.control.active());
            try std.testing.expect(platform.time.monotonicNs() - started < std.time.ns_per_s);
        } else {
            const response = try pair.parent.call(.{ .body = "upload" }, control.view());
            defer response.deinit();
            try std.testing.expectEqualStrings("complete", response.view().body);
        }
    }
}
