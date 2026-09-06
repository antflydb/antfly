// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Host-owned replaceable inference process. Only wire values cross the pipes.
const std = @import("std");
const rpc = @import("inference_worker_rpc.zig");
pub const wire = @import("inference_worker_wire.zig");
const bridge = @import("inference_bridge.zig");
const http = @import("../runtime_http_abi.zig");
const host = @import("inference_host.zig");
const platform = @import("antfly_platform");
const httpx = @import("httpx");
pub const EnvironmentEntry = struct { name: []const u8, value: []const u8 };

const ForwardControl = struct {
    alloc: std.mem.Allocator,
    cancellation: http.CancellationView,
    deadline_ns: ?u64 = null,
    progress: bridge.ProgressView = .{},
    stream: http.StreamSink = .{},
    stream_started: bool = false,

    fn check(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        if (self.cancellation.requested()) return error.Cancelled;
        if (self.deadline_ns) |deadline| {
            if (platform.time.monotonicNs() >= deadline) return error.Timeout;
        }
    }

    fn event(raw: ?*anyopaque, bytes: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        var parsed = try std.json.parseFromSlice(wire.Event, self.alloc, bytes, .{});
        defer parsed.deinit();
        const value = parsed.value;
        switch (value.kind) {
            .progress => self.progress.update(value.phase, value.completed, value.total, value.model, value.backend),
            .stream_start => {
                try callbackResult((self.stream.start orelse return error.StreamingUnavailable)(self.stream.context, value.status));
                self.stream_started = true;
            },
            .stream_write => {
                const decoded = try wire.decodeBytes(self.alloc, value.data);
                defer self.alloc.free(decoded);
                try callbackResult((self.stream.write orelse return error.StreamingUnavailable)(self.stream.context, .init(decoded)));
            },
            .stream_close => try callbackResult((self.stream.close orelse return error.StreamingUnavailable)(self.stream.context)),
        }
    }

    fn view(self: *@This()) rpc.Control {
        return .{ .ptr = self, .check = check, .event = event };
    }
};

fn callbackResult(status: http.CallbackStatus) !void {
    return switch (status) {
        .ok => {},
        .canceled => error.Cancelled,
        .timeout => error.Timeout,
        .body_too_large => error.BodyTooLarge,
        .body_capacity_exceeded => error.BodyCapacityExceeded,
        .end_of_stream => error.EndOfStream,
        .failed => error.InferenceTransportFailed,
    };
}

pub fn invokeProvider(client: *Client, context: *const bridge.ProviderInvokeContext) ![]u8 {
    var control = ForwardControl{
        .alloc = client.alloc,
        .cancellation = context.cancellation,
        .deadline_ns = if (context.has_deadline != 0) context.deadline_ns else null,
        .progress = context.progress,
    };
    const data = try std.json.Stringify.valueAlloc(client.alloc, wire.Provider{
        .operation = context.operation,
        .request_json = context.request_json.slice(),
        .deadline_ns = control.deadline_ns,
    }, .{});
    defer client.alloc.free(data);
    return client.invoke(.provider, data, control.view());
}

pub fn invokeHttp(client: *Client, route: []const u8, context: *const bridge.HttpHandleContext) !httpx.Response {
    var arena = std.heap.ArenaAllocator.init(client.alloc);
    defer arena.deinit();
    const alloc = arena.allocator();
    // Events can be numerous on a long stream: use a freeing allocator rather
    // than retaining every decoded frame in the request arena.
    var control = ForwardControl{ .alloc = client.alloc, .cancellation = context.cancellation, .stream = context.stream };
    try ForwardControl.check(&control);
    var body = context.request.body;
    if (body.slice() == null) if (context.body_source.read_all) |read| {
        try callbackResult(read(context.body_source.context, &body));
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
        .body_b64 = if (body.slice()) |bytes| try wire.encodeBytes(alloc, bytes) else null,
    }, .{});
    const result = client.invoke(.http, data, control.view()) catch |err| {
        if (control.stream_started) return err;
        if (err == error.ResourceTemporarilyUnavailable) {
            var response = httpx.Response.init(client.alloc, 503);
            errdefer response.deinit();
            try response.headers.append("content-type", "application/json");
            try response.headers.append("retry-after", "1");
            response.body = "{\"error\":\"inference worker unavailable; retry the request\"}";
            return response;
        }
        return err;
    };
    defer client.alloc.free(result);
    const value = try std.json.parseFromSliceLeaky(wire.HttpResponse, alloc, result, .{});
    var response = httpx.Response.init(client.alloc, value.status);
    errdefer response.deinit();
    for (value.headers) |header| try response.headers.append(header.name, header.value);
    response.body = try wire.decodeBytes(client.alloc, value.body_b64);
    response.body_owned = true;
    return response;
}

pub const Client = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    create_json: []u8,
    environment: std.process.Environ.Map,
    mutex: std.Io.Mutex = .init,
    worker: ?*Worker = null,
    active: usize = 0,
    generation: u64 = 0,
    budget: ?bridge.ResourceBudget = null,

    pub fn create(alloc: std.mem.Allocator, io: std.Io, context: *const bridge.CreateContext) !*Client {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const json = try std.json.Stringify.valueAlloc(alloc, try wire.Create.fromContext(arena.allocator(), context), .{});
        errdefer alloc.free(json);
        const settings = try std.json.parseFromSliceLeaky(struct { worker_environment: []const EnvironmentEntry = &.{} }, arena.allocator(), context.runtime_config_json.slice(), .{ .ignore_unknown_fields = true });
        var environment = std.process.Environ.Map.init(alloc);
        errdefer environment.deinit();
        // This child uses RPC as its lifeline, not the separate inference-run
        // supervisor's stdin reader. Never inherit that competing reader.
        for (settings.worker_environment) |entry| {
            if (std.mem.eql(u8, entry.name, platform.inference_process_supervisor.worker_env) or
                std.mem.eql(u8, entry.name, "ANTFLY_INFERENCE_SUPERVISOR_LIFELINE")) continue;
            try environment.put(entry.name, entry.value);
        }
        const self = try alloc.create(Client);
        self.* = .{ .alloc = alloc, .io = io, .create_json = json, .environment = environment };
        return self;
    }

    pub fn configure(self: *Client, budget: bridge.ResourceBudget) !void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.budget != null) return error.ExternalResourceBudgetsAlreadyConfigured;
        if (budget.retain_context(budget.context) == 0) return error.ResourceOwnerShuttingDown;
        self.budget = budget;
        _ = try self.ensureWorker(.{});
    }

    pub fn deinit(self: *Client) void {
        std.debug.assert(self.active == 0);
        if (self.worker) |worker| worker.destroy();
        if (self.budget) |budget| budget.release_context(budget.context);
        self.alloc.free(self.create_json);
        self.environment.deinit();
        self.alloc.destroy(self);
    }

    fn ensureWorker(self: *Client, control: rpc.Control) !*Worker {
        if (control.check) |check| try check(control.ptr);
        if (self.worker) |worker| {
            if (!worker.endpoint.closed.load(.acquire)) return worker;
            if (self.active != 0) return error.ResourceTemporarilyUnavailable;
            worker.destroy();
            self.worker = null;
        }
        if (self.budget == null) return error.ResourceOwnerNotConfigured;
        const worker = try self.alloc.create(Worker);
        errdefer self.alloc.destroy(worker);
        const executable = try std.process.executablePathAlloc(self.io, self.alloc);
        defer self.alloc.free(executable);
        var child = try std.process.spawn(self.io, .{
            .argv = &.{ executable, "inference", "_worker" },
            .stdin = .pipe,
            .stdout = .pipe,
            .stderr = .inherit,
            .environ_map = &self.environment,
        });
        var endpoint_started = false;
        errdefer if (!endpoint_started) child.kill(self.io);
        worker.* = .{
            .owner = self,
            .child = child,
            .endpoint = .{
                .alloc = self.alloc,
                .io = self.io,
                .input = child.stdout.?,
                .output = child.stdin.?,
                .context = worker,
                .handler = Worker.resourceRequest,
                .on_closed = Worker.closed,
                .next_id = 1,
            },
        };
        try worker.endpoint.start();
        endpoint_started = true;
        errdefer worker.endpoint.deinit();
        const initialized = try call(&worker.endpoint, .initialize, self.create_json, control);
        self.alloc.free(initialized);
        const configured = try call(&worker.endpoint, .configure, "", control);
        self.alloc.free(configured);
        self.generation += 1;
        self.worker = worker;
        std.log.info("standalone inference worker ready generation={d}", .{self.generation});
        return worker;
    }

    pub fn invoke(self: *Client, operation: wire.Operation, data: []const u8, control: rpc.Control) ![]u8 {
        while (!self.mutex.tryLock()) {
            if (control.check) |check| try check(control.ptr);
            try self.io.sleep(.fromMilliseconds(1), .awake);
        }
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
        return call(&worker.endpoint, operation, data, control);
    }
};

fn call(endpoint: *rpc.Endpoint, operation: wire.Operation, data: []const u8, control: rpc.Control) ![]u8 {
    const alloc = endpoint.alloc;
    const request = try std.json.Stringify.valueAlloc(alloc, wire.Envelope{ .operation = operation, .data = data }, .{});
    defer alloc.free(request);
    const response = endpoint.call(request, control) catch |err| switch (err) {
        error.InferenceWorkerUnavailable, error.InferenceWorkerRequestFailed, error.BrokenPipe => return error.ResourceTemporarilyUnavailable,
        else => return err,
    };
    defer alloc.free(response);
    var parsed = try std.json.parseFromSlice(wire.Reply, alloc, response, .{});
    defer parsed.deinit();
    if (!parsed.value.status.isOk()) return bridge.errorFromStatus(parsed.value.status);
    return alloc.dupe(u8, parsed.value.data);
}

const Observation = struct { key: u64, bytes: u64 = 0, tokenizer: bool };
const Worker = struct {
    owner: *Client,
    child: std.process.Child,
    endpoint: rpc.Endpoint,
    resources: std.Io.Mutex = .init,
    leases: std.AutoHashMapUnmanaged(usize, void) = .empty,
    observations: std.ArrayListUnmanaged(*Observation) = .empty,

    fn destroy(self: *Worker) void {
        self.endpoint.deinit();
        self.owner.alloc.destroy(self);
    }

    fn closed(raw: *anyopaque) void {
        const self: *Worker = @ptrCast(@alignCast(raw));
        // Reaping is the fence: device memory and borrowed reservations cannot
        // be reused just because the transport disconnected.
        self.child.kill(self.owner.io);
        self.releaseResources();
    }

    fn releaseResources(self: *Worker) void {
        self.resources.lockUncancelable(self.owner.io);
        defer self.resources.unlock(self.owner.io);
        const budget = self.owner.budget.?;
        var leases = self.leases.keyIterator();
        while (leases.next()) |lease| budget.release_admission(budget.context, lease.*);
        self.leases.deinit(self.owner.alloc);
        self.leases = .empty;
        for (self.observations.items) |observation| {
            const callback = if (observation.tokenizer) budget.observe_tokenizer_cache else budget.observe_prompt_cache;
            _ = callback(budget.context, @intFromPtr(observation), observation.bytes, 0);
            self.owner.alloc.destroy(observation);
        }
        self.observations.deinit(self.owner.alloc);
        self.observations = .empty;
    }

    fn resourceRequest(raw: *anyopaque, request: *rpc.Request) ![]u8 {
        const self: *Worker = @ptrCast(@alignCast(raw));
        const alloc = self.owner.alloc;
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const envelope = try std.json.parseFromSliceLeaky(wire.Envelope, arena.allocator(), request.payload, .{});
        self.resources.lockUncancelable(self.owner.io);
        defer self.resources.unlock(self.owner.io);
        const result = self.resourceOperation(arena.allocator(), envelope) catch |err|
            return std.json.Stringify.valueAlloc(alloc, wire.Reply{ .status = bridge.statusFromError(err) }, .{});
        return std.json.Stringify.valueAlloc(alloc, wire.Reply{ .data = result }, .{});
    }

    fn resourceOperation(self: *Worker, arena: std.mem.Allocator, envelope: wire.Envelope) ![]const u8 {
        if (self.endpoint.closed.load(.acquire)) return error.ResourceOwnerShuttingDown;
        const budget = self.owner.budget.?;
        switch (envelope.operation) {
            .reserve => {
                const reservation = try std.json.parseFromSliceLeaky(wire.Reservation, arena, envelope.data, .{});
                var lease: usize = 0;
                const status = budget.reserve_admission(budget.context, &reservation.amounts, &lease);
                if (!status.isOk()) return bridge.errorFromStatus(status);
                errdefer budget.release_admission(budget.context, lease);
                if (lease == 0 or self.leases.contains(lease)) return error.InvalidGenerationAdmission;
                try self.leases.put(self.owner.alloc, lease, {});
                return std.json.Stringify.valueAlloc(arena, lease, .{});
            },
            .retain => {
                const reservation = try std.json.parseFromSliceLeaky(wire.Reservation, arena, envelope.data, .{});
                if (!self.leases.contains(reservation.lease)) return error.InvalidGenerationAdmission;
                const status = budget.retain_admission(budget.context, reservation.lease, &reservation.amounts);
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
                const observation = found: {
                    for (self.observations.items) |item| if (item.key == value.key and item.tokenizer == tokenizer) break :found item;
                    if (value.previous != 0) return error.InvalidGenerationAdmission;
                    const item = try self.owner.alloc.create(Observation);
                    errdefer self.owner.alloc.destroy(item);
                    item.* = .{ .key = value.key, .tokenizer = tokenizer };
                    try self.observations.append(self.owner.alloc, item);
                    break :found item;
                };
                if (observation.bytes != value.previous) return error.InvalidGenerationAdmission;
                const callback = if (tokenizer) budget.observe_tokenizer_cache else budget.observe_prompt_cache;
                if (callback(budget.context, @intFromPtr(observation), value.previous, value.next) == 0) return error.ResourceLimitExceeded;
                observation.bytes = value.next;
                if (value.next == 0) {
                    for (self.observations.items, 0..) |item, i| {
                        if (item == observation) {
                            _ = self.observations.swapRemove(i);
                            self.owner.alloc.destroy(item);
                            break;
                        }
                    }
                }
            },
            else => return error.UnsupportedOperation,
        }
        return "";
    }
};

const Child = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    endpoint: rpc.Endpoint,
    state: ?*host.LinkedInferenceState = null,
    create_arena: std.heap.ArenaAllocator,
    configured: bool = false,
    startup_mutex: std.Io.Mutex = .init,

    fn closed(_: *anyopaque) void {
        // Loss of the owning database is a hard lifetime boundary, even if a
        // concurrent driver call is wedged and cannot join cooperatively.
        std.process.exit(0);
    }

    fn dispatch(raw: *anyopaque, request: *rpc.Request) ![]u8 {
        const self: *Child = @ptrCast(@alignCast(raw));
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const envelope = try std.json.parseFromSliceLeaky(wire.Envelope, arena.allocator(), request.payload, .{});
        const result = self.execute(arena.allocator(), request, envelope) catch |err|
            return std.json.Stringify.valueAlloc(self.alloc, wire.Reply{ .status = bridge.statusFromError(err) }, .{});
        return std.json.Stringify.valueAlloc(self.alloc, wire.Reply{ .data = result }, .{});
    }

    fn execute(self: *Child, arena: std.mem.Allocator, request: *rpc.Request, envelope: wire.Envelope) ![]const u8 {
        if (envelope.operation == .initialize) {
            try self.startup_mutex.lock(self.io);
            defer self.startup_mutex.unlock(self.io);
            if (self.state != null) return error.AlreadyExists;
            const create_arena = self.create_arena.allocator();
            const create = try std.json.parseFromSliceLeaky(wire.Create, create_arena, envelope.data, .{ .allocate = .alloc_always });
            var out: ?*anyopaque = null;
            const context = try create.toContext(create_arena, &self.io, &out);
            self.state = @ptrCast(@alignCast(try host.linkedInferenceCreateLocal(&context, true)));
            return "";
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
            return "";
        }
        const configured = self.configured;
        self.startup_mutex.unlock(self.io);
        if (!configured) return error.ResourceOwnerNotConfigured;
        switch (envelope.operation) {
            .provider => {
                const provider = try std.json.parseFromSliceLeaky(wire.Provider, arena, envelope.data, .{});
                var response_handle: ?*anyopaque = null;
                var response: bridge.String = undefined;
                defer if (response_handle) |handle| host.linkedInferenceDestroyProviderResponse(handle);
                try host.linkedInferenceInvokeProvider(&.{
                    .abi_version = bridge.abi_version,
                    .handle = state,
                    .operation = provider.operation,
                    .request_json = .init(provider.request_json),
                    .deadline_ns = provider.deadline_ns orelse 0,
                    .has_deadline = @intFromBool(provider.deadline_ns != null),
                    .out_response_handle = &response_handle,
                    .out_response_json = &response,
                    .cancellation = .{ .context = request, .is_cancelled = cancelled },
                    .progress = .{ .context = request, .update_progress = progress },
                });
                return arena.dupe(u8, response.slice());
            },
            .http => {
                const incoming = try std.json.parseFromSliceLeaky(wire.Http, arena, envelope.data, .{});
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
                const body = if (incoming.body_b64) |value| try wire.decodeBytes(arena, value) else null;
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
                try host.linkedInferenceHandleHttp(&.{
                    .abi_version = bridge.abi_version,
                    .route_handle = route_handle,
                    .request = &view,
                    .cancellation = .{ .context = request, .is_cancelled = cancelled },
                    .stream = .{ .context = request, .start = streamStart, .write = streamWrite, .close = streamClose },
                    .out_response_handle = &response_handle,
                    .out_response = &response,
                });
                const output_headers = try arena.alloc(wire.Header, response.headers_len);
                const source_headers = if (response.headers_ptr) |ptr| ptr[0..response.headers_len] else &.{};
                for (source_headers, output_headers) |header, *out| out.* = .{ .name = header.name.slice(), .value = header.value.slice() };
                return std.json.Stringify.valueAlloc(arena, wire.HttpResponse{
                    .status = response.status,
                    .headers = output_headers,
                    .body_b64 = try wire.encodeBytes(arena, response.body.slice()),
                }, .{});
            },
            else => return error.UnsupportedOperation,
        }
    }

    fn cancelled(raw: ?*const anyopaque) callconv(.c) u8 {
        const request: *const rpc.Request = @ptrCast(@alignCast(raw.?));
        return @intFromBool(request.cancelled.load(.acquire));
    }

    fn emit(request: *rpc.Request, event: wire.Event) !void {
        const alloc = request.endpoint.alloc;
        const data = try std.json.Stringify.valueAlloc(alloc, event, .{});
        defer alloc.free(data);
        try request.event(data);
    }

    fn progress(raw: ?*anyopaque, phase: u8, completed: u64, total: u64, model: bridge.String, backend: bridge.String) callconv(.c) void {
        const request: *rpc.Request = @ptrCast(@alignCast(raw.?));
        emit(request, .{ .kind = .progress, .phase = phase, .completed = completed, .total = total, .model = model.slice(), .backend = backend.slice() }) catch
            request.cancelled.store(true, .release);
    }
    fn streamStart(raw: ?*anyopaque, status: u16) callconv(.c) http.CallbackStatus {
        const request: *rpc.Request = @ptrCast(@alignCast(raw.?));
        emit(request, .{ .kind = .stream_start, .status = status }) catch return .canceled;
        return .ok;
    }
    fn streamWrite(raw: ?*anyopaque, bytes: http.Bytes) callconv(.c) http.CallbackStatus {
        const request: *rpc.Request = @ptrCast(@alignCast(raw.?));
        const encoded = wire.encodeBytes(request.endpoint.alloc, bytes.slice()) catch return .failed;
        defer request.endpoint.alloc.free(encoded);
        emit(request, .{ .kind = .stream_write, .data = encoded }) catch return .canceled;
        return .ok;
    }
    fn streamClose(raw: ?*anyopaque) callconv(.c) http.CallbackStatus {
        const request: *rpc.Request = @ptrCast(@alignCast(raw.?));
        emit(request, .{ .kind = .stream_close }) catch return .canceled;
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
            .release_admission = release,
            .observe_prompt_cache = observePrompt,
            .observe_tokenizer_cache = observeTokenizer,
        };
    }
    fn retainContext(_: *anyopaque) callconv(.c) u8 {
        return 1;
    }
    fn releaseContext(_: *anyopaque) callconv(.c) void {}
    fn resourceCall(self: *Child, operation: wire.Operation, value: anytype) ![]u8 {
        const data = try std.json.Stringify.valueAlloc(self.alloc, value, .{});
        defer self.alloc.free(data);
        return call(&self.endpoint, operation, data, .{});
    }
    fn reserve(raw: *anyopaque, amounts: *const bridge.AdmissionAmounts, lease: *usize) callconv(.c) bridge.Status {
        const self: *Child = @ptrCast(@alignCast(raw));
        const data = self.resourceCall(.reserve, wire.Reservation{ .amounts = amounts.* }) catch |err| return bridge.statusFromError(err);
        defer self.alloc.free(data);
        lease.* = std.fmt.parseUnsigned(usize, data, 10) catch return bridge.statusFromError(error.InvalidGenerationAdmission);
        return .ok;
    }
    fn retain(raw: *anyopaque, lease: usize, amounts: *const bridge.AdmissionAmounts) callconv(.c) bridge.Status {
        const self: *Child = @ptrCast(@alignCast(raw));
        const data = self.resourceCall(.retain, wire.Reservation{ .lease = lease, .amounts = amounts.* }) catch |err| return bridge.statusFromError(err);
        self.alloc.free(data);
        return .ok;
    }
    fn release(raw: *anyopaque, lease: usize) callconv(.c) void {
        const self: *Child = @ptrCast(@alignCast(raw));
        const data = self.resourceCall(.release, lease) catch return;
        self.alloc.free(data);
    }
    fn observe(self: *Child, operation: wire.Operation, key: usize, previous: u64, next: u64) u8 {
        const data = self.resourceCall(operation, wire.Observation{ .key = key, .previous = previous, .next = next }) catch return 0;
        self.alloc.free(data);
        return 1;
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
        .on_closed = Child.closed,
        .next_id = 2,
    };
    try child.endpoint.start();
    defer child.endpoint.deinit();
    var lifetime: std.Io.Event = .unset;
    try lifetime.wait(io);
}

test "inference worker retains reservations until reaped cleanup" {
    const Budget = struct {
        leased: bool = false,
        cached: u64 = 0,
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
        },
    };
    var worker: Worker = .{
        .owner = &client,
        .child = undefined,
        .endpoint = .{ .alloc = alloc, .io = std.testing.io, .input = undefined, .output = undefined, .context = undefined, .handler = Worker.resourceRequest, .on_closed = Worker.closed, .next_id = 1 },
    };
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
    try std.testing.expectEqualStrings("7", try worker.resourceOperation(arena.allocator(), .{ .operation = .reserve, .data = reserve_json }));
    _ = try worker.resourceOperation(arena.allocator(), .{ .operation = .prompt_cache, .data = "{\"key\":9,\"previous\":0,\"next\":64}" });
    try std.testing.expectError(error.InvalidGenerationAdmission, worker.resourceOperation(arena.allocator(), .{ .operation = .release, .data = "8" }));
    worker.endpoint.closed.store(true, .release);
    try std.testing.expect(budget.leased);
    try std.testing.expectEqual(@as(u64, 64), budget.cached);
    try std.testing.expectError(error.ResourceOwnerShuttingDown, worker.resourceOperation(arena.allocator(), .{ .operation = .reserve, .data = reserve_json }));
    worker.releaseResources();
    try std.testing.expect(!budget.leased);
    try std.testing.expectEqual(@as(u64, 0), budget.cached);
}
