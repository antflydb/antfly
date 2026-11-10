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
const document_abi = @import("antfly_runtime_abi").document_abi;
const host = @import("host.zig");
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
    body_source: http.RequestBodySource = .{},
    borrowed_body: ?[]const u8 = null,
    body_offset: usize = 0,
    stream_body: bool = false,

    fn readNativeBody(raw: ?*anyopaque, destination: []u8) !usize {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        try check(self);
        if (!self.stream_body) return error.BodySourceUnavailable;
        if (destination.len == 0) return 0;
        // Permit one extra byte only to distinguish EOF from an over-limit
        // source. The request-scoped reader is joined before its owner exits.
        const capacity = @min(destination.len, wire.native_checkpoint_upload_limit - self.body_offset + 1);
        const count = if (self.borrowed_body) |body| borrowed: {
            const n = @min(capacity, body.len - self.body_offset);
            @memcpy(destination[0..n], body[self.body_offset..][0..n]);
            break :borrowed n;
        } else streamed: {
            const read = self.body_source.read orelse break :streamed @as(usize, 0);
            var n: usize = 0;
            try callbackResult(read(self.body_source.context, destination.ptr, capacity, &n));
            break :streamed n;
        };
        if (count > capacity) return error.InvalidBodyReadCount;
        if (count > wire.native_checkpoint_upload_limit - self.body_offset) return error.BodyTooLarge;
        self.body_offset += count;
        try check(self);
        return count;
    }

    fn check(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        if (self.cancellation.requested()) return error.Cancelled;
        if (self.deadline_ns) |deadline| {
            if (platform.time.monotonicNs() >= deadline) return error.Timeout;
        }
    }

    fn event(raw: ?*anyopaque, payload: rpc.Payload) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        var parsed = try std.json.parseFromSlice(wire.Event, self.alloc, payload.metadata, .{});
        defer parsed.deinit();
        const value = parsed.value;
        switch (value.kind) {
            .progress => self.progress.update(value.phase, value.completed, value.total, value.model, value.backend),
            .stream_start => {
                const headers = try self.alloc.alloc(http.HeaderView, value.headers.len);
                defer self.alloc.free(headers);
                for (value.headers, headers) |header, *header_view| header_view.* = .{ .name = .init(header.name), .value = .init(header.value) };
                try callbackResult((self.stream.start orelse return error.StreamingUnavailable)(self.stream.context, value.status, .init(payload.body), .{ .ptr = headers.ptr, .len = headers.len }));
                self.stream_started = true;
            },
            .stream_write => {
                try callbackResult((self.stream.write orelse return error.StreamingUnavailable)(self.stream.context, .init(payload.body)));
            },
            .stream_close => try callbackResult((self.stream.close orelse return error.StreamingUnavailable)(self.stream.context)),
        }
    }

    fn readBody(raw: ?*anyopaque, dest: []u8) !usize {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        try check(raw);
        var count: usize = 0;
        try callbackResult((self.body_source.read orelse return error.BodySourceUnavailable)(self.body_source.context, dest.ptr, dest.len, &count));
        if (count > dest.len) return error.BodyReadFailed;
        return count;
    }
    fn view(self: *@This()) rpc.Control {
        return .{ .ptr = self, .check = check, .event = event, .read_body = if (self.stream_body) readNativeBody else if (self.body_source.read != null) readBody else null };
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

pub const ProviderResult = union(enum) {
    json: []u8,
    numeric: struct { rows: [][]f32, kind: @FieldType(bridge.NumericResult, "kind") },
};

pub fn invokeProvider(client: *Client, context: *const bridge.ProviderInvokeContext) !ProviderResult {
    var arena = std.heap.ArenaAllocator.init(client.alloc);
    defer arena.deinit();
    var control = ForwardControl{
        .alloc = client.alloc,
        .cancellation = context.cancellation,
        .deadline_ns = if (context.has_deadline != 0) context.deadline_ns else null,
        .progress = context.progress,
    };
    try ForwardControl.check(&control);
    const document_key = try client.registerDocumentStore(context.document_store);
    defer if (document_key) |key| client.unregisterDocumentStore(key);
    var input = try wire.ProviderInput.init(arena.allocator(), context);
    input.options.document_store = document_key;
    const data = try std.json.Stringify.valueAlloc(arena.allocator(), input.options, .{});
    const response = try client.invokePayload(.provider, data, .{ .body_segments = input.body.segments }, control.view());
    defer response.deinit();
    try ForwardControl.check(&control);
    const reply_options = try std.json.parseFromSliceLeaky(wire.Reply, arena.allocator(), response.view().metadata, .{});
    const output = try std.json.parseFromSliceLeaky(wire.ProviderOutput, arena.allocator(), reply_options.options, .{});
    if (output.kind != .absent) {
        if (!input.options.numeric) return error.InvalidInferenceNumericResult;
        return .{ .numeric = .{ .rows = try output.decode(client.alloc, response.view().body), .kind = output.kind } };
    }
    if (input.options.numeric) return error.InvalidInferenceNumericResult;
    return .{ .json = try client.alloc.dupe(u8, response.view().body) };
}

pub fn invokeHttp(client: *Client, route: []const u8, context: *const bridge.HttpHandleContext) !httpx.Response {
    var arena = std.heap.ArenaAllocator.init(client.alloc);
    defer arena.deinit();
    const alloc = arena.allocator();
    // Events can be numerous on a long stream: use a freeing allocator rather
    // than retaining every decoded frame in the request arena.
    var control = ForwardControl{ .alloc = client.alloc, .cancellation = context.cancellation, .stream = context.stream, .body_source = context.body_source };
    try ForwardControl.check(&control);
    const stream_body = wire.nativeCheckpointResume(context.request.method, route) and
        wire.nativeCheckpointResume(context.request.method, context.request.path.slice());
    var body = context.request.body;
    const streaming_body = context.body_source.streaming != 0 and context.body_source.read != null and
        std.mem.endsWith(u8, context.request.path.slice(), "/stream") and
        std.mem.indexOf(u8, context.request.path.slice(), "/transcription/sessions/") != null;
    if (stream_body) {
        if (body.slice() == null and context.body_source.read == null and context.body_source.read_all != null)
            return error.StreamingUnavailable;
        control.stream_body = true;
        control.body_source = context.body_source;
        control.borrowed_body = body.slice();
        body = .{};
    } else if (!streaming_body and body.slice() == null) if (context.body_source.read_all) |read| {
        try callbackResult(read(context.body_source.context, &body));
    };
    const headers = try alloc.alloc(wire.Header, context.request.headers_len);
    const source_headers = if (context.request.headers_ptr) |ptr| ptr[0..context.request.headers_len] else &.{};
    for (source_headers, headers) |header, *out| out.* = .{ .name = header.name.slice(), .value = header.value.slice() };
    const params = try alloc.alloc(wire.Header, context.request.params_len);
    const source_params = if (context.request.params_ptr) |ptr| ptr[0..context.request.params_len] else &.{};
    for (source_params, params) |param, *out| out.* = .{ .name = param.name.slice(), .value = param.value.slice() };
    const document_key = try client.registerDocumentStore(context.document_store);
    defer if (document_key) |key| client.unregisterDocumentStore(key);
    const data = try std.json.Stringify.valueAlloc(alloc, wire.Http{
        .document_store = document_key,
        .route = route,
        .method = context.request.method,
        .path = context.request.path.slice(),
        .query = context.request.query.slice(),
        .headers = headers,
        .params = params,
        .has_body = body.slice() != null,
        .stream_body = stream_body,
        .streaming_body = streaming_body,
    }, .{});
    const result = client.invoke(.http, data, body.slice() orelse "", control.view()) catch |err| {
        if (control.stream_started) return err;
        if (err == error.BodyTooLarge) {
            var response = httpx.Response.init(client.alloc, 413);
            errdefer response.deinit();
            try response.headers.append("content-type", "application/json");
            response.body = "{\"error\":\"inference request too large\"}";
            return response;
        }
        if (err == error.InferenceWorkerExecutableNotConfigured) {
            var response = httpx.Response.init(client.alloc, 503);
            errdefer response.deinit();
            try response.headers.append("content-type", "application/json");
            response.body = "{\"error\":\"inference worker executable not configured; set ANTFLY_INFERENCE_WORKER to an antfly executable\",\"code\":\"InferenceWorkerExecutableNotConfigured\"}";
            return response;
        }
        if (err == error.InferenceWorkerUnavailable or err == error.ResourceTemporarilyUnavailable or err == error.QueueFull) {
            var response = httpx.Response.init(client.alloc, 503);
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
    defer result.deinit();
    const result_header = try std.json.parseFromSliceLeaky(wire.Reply, alloc, result.view().metadata, .{});
    const value = try std.json.parseFromSliceLeaky(wire.HttpResponse, alloc, result_header.options, .{});
    var response = httpx.Response.init(client.alloc, value.status);
    errdefer response.deinit();
    for (value.headers) |header| try response.headers.append(header.name, header.value);
    response.body = try client.alloc.dupe(u8, result.view().body);
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
    document_mutex: std.Io.Mutex = .init,
    document_bindings: std.AutoHashMapUnmanaged([16]u8, bridge.DocumentStoreView) = .empty,

    fn registerDocumentStore(self: *Client, view: bridge.DocumentStoreView) !?[16]u8 {
        if (view.context == null) return null;
        try view.retain();
        errdefer view.release();
        self.document_mutex.lockUncancelable(self.io);
        defer self.document_mutex.unlock(self.io);
        if (self.document_bindings.count() >= 128) return error.ResourceLimitExceeded;
        var key: [16]u8 = undefined;
        while (true) {
            try self.io.randomSecure(&key);
            if (!self.document_bindings.contains(key)) break;
        }
        try self.document_bindings.put(self.alloc, key, view);
        return key;
    }
    fn unregisterDocumentStore(self: *Client, key: [16]u8) void {
        self.document_mutex.lockUncancelable(self.io);
        const entry = self.document_bindings.fetchRemove(key);
        self.document_mutex.unlock(self.io);
        if (entry) |removed| removed.value.release();
    }
    fn borrowDocumentStore(self: *Client, key: [16]u8) !bridge.DocumentStoreView {
        self.document_mutex.lockUncancelable(self.io);
        defer self.document_mutex.unlock(self.io);
        const view = self.document_bindings.get(key) orelse return error.DocumentRunLeaseLost;
        try view.retain();
        return view;
    }

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
        std.debug.assert(self.document_bindings.count() == 0);
        self.document_bindings.deinit(self.alloc);
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
            if (self.active != 0) return error.InferenceWorkerUnavailable;
            worker.destroy();
            self.worker = null;
        }
        if (self.budget == null) return error.ResourceOwnerNotConfigured;
        const worker = try self.alloc.create(Worker);
        errdefer self.alloc.destroy(worker);
        const executable = try resolveWorkerExecutable(self.alloc, self.io);
        defer self.alloc.free(executable);
        const child = std.process.spawn(self.io, .{
            .argv = &.{ executable, "inference", "_worker" },
            .stdin = .pipe,
            .stdout = .pipe,
            .stderr = .inherit,
            .environ_map = &self.environment,
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
            .endpoint = .{
                .alloc = self.alloc,
                .io = self.io,
                .input = child.stdout.?,
                .output = child.stdin.?,
                .context = worker,
                .handler = Worker.documentRequest,
                .resource_handler = Worker.resourceRequest,
                .on_closed = Worker.closed,
                .next_id = 1,
            },
        };
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
        std.log.info("standalone inference worker ready generation={d}", .{self.generation});
        return worker;
    }

    pub fn invoke(self: *Client, operation: wire.Operation, options: []const u8, data: []const u8, control: rpc.Control) !rpc.OwnedPayload {
        return self.invokePayload(operation, options, .{ .body = data }, control);
    }

    fn invokePayload(self: *Client, operation: wire.Operation, options: []const u8, payload: rpc.Payload, control: rpc.Control) !rpc.OwnedPayload {
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
        const result = try callSegments(&worker.endpoint, operation, options, payload, control);
        // Hand off an independent response before releasing this worker lease.
        return result.detach();
    }
};

fn callPayload(endpoint: *rpc.Endpoint, operation: wire.Operation, options: []const u8, data: []const u8, control: rpc.Control) !rpc.OwnedPayload {
    return callSegments(endpoint, operation, options, .{ .body = data }, control);
}

fn callSegments(endpoint: *rpc.Endpoint, operation: wire.Operation, options: []const u8, payload: rpc.Payload, control: rpc.Control) !rpc.OwnedPayload {
    const alloc = endpoint.alloc;
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
/// Go test binary, `examples/antfly-docs-knowledge-base`, or any other consumer) has no such
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
    const header = try std.json.Stringify.valueAlloc(endpoint.alloc, wire.Reply{ .status = status, .options = options }, .{});
    defer endpoint.alloc.free(header);
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

    fn documentRequest(raw: *anyopaque, request: *rpc.Request) !rpc.OwnedPayload {
        const self: *Worker = @ptrCast(@alignCast(raw));
        var arena = std.heap.ArenaAllocator.init(self.owner.alloc);
        defer arena.deinit();
        const envelope = try requestEnvelope(arena.allocator(), request);
        if (envelope.operation != .document_store) return error.UnsupportedOperation;
        const incoming = try std.json.parseFromSliceLeaky(wire.DocumentStoreRequest, arena.allocator(), envelope.options, .{});
        const view = self.owner.borrowDocumentStore(incoming.capability) catch |err| return reply(&self.endpoint, bridge.statusFromError(err), "", "");
        defer view.release();
        var result: document_abi.Response = .{};
        const status = view.invoke.?(view.context.?, .init(incoming.metadata), .init(envelope.data), &result);
        defer view.destroy(result);
        return reply(&self.endpoint, status, result.metadata.slice(), result.body.slice());
    }

    fn resourceRequest(raw: *anyopaque, bytes: []const u8) !rpc.ResourceReply {
        const self: *Worker = @ptrCast(@alignCast(raw));
        var arena = std.heap.ArenaAllocator.init(self.owner.alloc);
        defer arena.deinit();
        const incoming = try std.json.parseFromSliceLeaky(wire.ResourceRequest, arena.allocator(), bytes, .{});
        self.resources.lockUncancelable(self.owner.io);
        defer self.resources.unlock(self.owner.io);
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
            .reserve => {
                const reservation = try std.json.parseFromSliceLeaky(wire.Reservation, arena, envelope.data, .{});
                // No allocation may fail after a successful reserve before it
                // is recorded for reaped cleanup.
                try self.leases.ensureUnusedCapacity(self.owner.alloc, 1);
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
        return 0;
    }
};

const NativeHttpBody = struct {
    request: *rpc.Request,
    buffer: [rpc.max_frame_bytes]u8 = undefined,
    offset: usize = 0,
    len: usize = 0,
    received: usize = 0,
    failed: ?http.CallbackStatus = null,

    fn read(raw: ?*anyopaque, destination: [*]u8, capacity: usize, out_count: *usize) callconv(.c) http.CallbackStatus {
        const self: *@This() = @ptrCast(@alignCast(raw orelse return .failed));
        out_count.* = 0;
        if (self.failed) |failure| return failure;
        if (self.request.cancelled.load(.acquire)) return .canceled;
        if (capacity == 0) return .ok;
        if (self.offset == self.len) {
            self.len = self.request.readBody(&self.buffer) catch |err| {
                self.failed = switch (err) {
                    error.Cancelled, error.Canceled => .canceled,
                    error.Timeout => .timeout,
                    else => .failed,
                };
                return self.failed.?;
            };
            self.offset = 0;
            if (self.len > wire.native_checkpoint_upload_limit - self.received) {
                self.failed = .body_too_large;
                return .body_too_large;
            }
            self.received += self.len;
        }
        const count = @min(capacity, self.len - self.offset);
        @memcpy(destination[0..count], self.buffer[self.offset..][0..count]);
        self.offset += count;
        out_count.* = count;
        return .ok;
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

    const ProviderDiagnostic = struct {
        operation: c_int = 0,
        request_json: []const u8 = "",
        has_deadline: bool = false,
    };

    fn closed(_: *anyopaque) void {
        // Loss of the owning database is a hard lifetime boundary, even if a
        // concurrent driver call is wedged and cannot join cooperatively.
        std.process.exit(0);
    }

    fn dispatch(raw: *anyopaque, request: *rpc.Request) !rpc.OwnedPayload {
        const self: *Child = @ptrCast(@alignCast(raw));
        var arena = std.heap.ArenaAllocator.init(self.alloc);
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
            const create = try std.json.parseFromSliceLeaky(wire.Create, create_arena, envelope.data, .{ .allocate = .alloc_always });
            var out: ?*anyopaque = null;
            const context = try create.toContext(create_arena, &self.io, &out);
            self.state = @ptrCast(@alignCast(try host.linkedInferenceCreateLocal(&context, true)));
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
                const documents = if (provider.document_store) |key| try ForwardDocuments.create(self, key) else null;
                defer if (documents) |binding| binding.view().release();
                var response_handle: ?*anyopaque = null;
                var response: bridge.String = undefined;
                var numeric: bridge.NumericResult = .{};
                defer if (response_handle) |handle| host.linkedInferenceDestroyProviderResponse(handle);
                try host.linkedInferenceInvokeProvider(&.{
                    .abi_version = bridge.abi_version,
                    .handle = state,
                    .operation = provider.operation,
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
                    .document_store = if (documents) |binding| binding.view() else .{},
                });
                if (numeric.kind != .absent) {
                    const output = try wire.ProviderOutput.encode(arena, numeric);
                    return reply(&self.endpoint, .ok, try std.json.Stringify.valueAlloc(arena, output.options, .{}), output.body);
                }
                return reply(&self.endpoint, .ok, "{}", response.slice());
            },
            .http => {
                const incoming = try std.json.parseFromSliceLeaky(wire.Http, arena, envelope.options, .{});
                if (incoming.stream_body and (incoming.has_body or envelope.data.len != 0 or
                    !wire.nativeCheckpointResume(incoming.method, incoming.route) or
                    !wire.nativeCheckpointResume(incoming.method, incoming.path))) return error.InvalidInput;
                var streamed_body: NativeHttpBody = .{ .request = request };
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
                const documents = if (incoming.document_store) |key| try ForwardDocuments.create(self, key) else null;
                defer if (documents) |binding| binding.view().release();
                var response_handle: ?*anyopaque = null;
                var response: http.HttpResponseView = undefined;
                defer if (response_handle) |handle| host.linkedInferenceDestroyHttpResponse(handle);
                try host.linkedInferenceHandleHttp(&.{
                    .abi_version = bridge.abi_version,
                    .route_handle = route_handle,
                    .request = &view,
                    .document_store = if (documents) |binding| binding.view() else .{},
                    .cancellation = .{ .context = request, .is_cancelled = cancelled },
                    .body_source = if (incoming.stream_body) .{ .context = &streamed_body, .read = NativeHttpBody.read, .streaming = 1 } else if (incoming.streaming_body) .{ .context = request, .streaming = 1, .read = readBody, .read_all = readAllBody } else .{},
                    .stream = .{ .context = request, .start = streamStart, .write = streamWrite, .close = streamClose },
                    .out_response_handle = &response_handle,
                    .out_response = &response,
                });
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
    fn readAllBody(_: ?*anyopaque, _: *http.OptionalBytes) callconv(.c) http.CallbackStatus {
        return .failed;
    }

    fn cancelled(raw: ?*const anyopaque) callconv(.c) u8 {
        const request: *const rpc.Request = @ptrCast(@alignCast(raw.?));
        return @intFromBool(request.cancelled.load(.acquire));
    }

    fn emit(request: *rpc.Request, event: wire.Event) !void {
        return emitBody(request, event, "");
    }

    fn emitBody(request: *rpc.Request, event: wire.Event, body: []const u8) !void {
        const alloc = request.endpoint.alloc;
        const data = try std.json.Stringify.valueAlloc(alloc, event, .{});
        defer alloc.free(data);
        try request.event(.{ .metadata = data, .body = body });
    }

    fn progress(raw: ?*anyopaque, phase: u8, completed: u64, total: u64, model: bridge.String, backend: bridge.String) callconv(.c) void {
        const request: *rpc.Request = @ptrCast(@alignCast(raw.?));
        emit(request, .{ .kind = .progress, .phase = phase, .completed = completed, .total = total, .model = model.slice(), .backend = backend.slice() }) catch
            request.cancelled.store(true, .release);
    }
    fn streamStart(raw: ?*anyopaque, status: u16, content_type: http.Bytes, headers: http.HeaderList) callconv(.c) http.CallbackStatus {
        const request: *rpc.Request = @ptrCast(@alignCast(raw.?));
        const alloc = request.endpoint.alloc;
        const values = alloc.alloc(wire.Header, headers.len) catch return .failed;
        defer alloc.free(values);
        for (headers.slice(), values) |header, *value| value.* = .{ .name = header.name.slice(), .value = header.value.slice() };
        // The start event's body carries the content type; subsequent chunks
        // keep their existing envelope without an extra JSON field per event.
        emitBody(request, .{ .kind = .stream_start, .status = status, .headers = values }, content_type.slice()) catch return .canceled;
        return .ok;
    }
    fn streamWrite(raw: ?*anyopaque, bytes: http.Bytes) callconv(.c) http.CallbackStatus {
        const request: *rpc.Request = @ptrCast(@alignCast(raw.?));
        emitBody(request, .{ .kind = .stream_write }, bytes.slice()) catch return .canceled;
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
    fn resourceFailure(self: *Child) noreturn {
        // Continuing could reuse unaccounted device memory. Exiting also wakes
        // a parent whose resource reply failed; it reaps before reclaiming.
        self.endpoint.fail();
        std.process.exit(1);
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
    fn release(raw: *anyopaque, lease: usize) callconv(.c) void {
        const self: *Child = @ptrCast(@alignCast(raw));
        _ = self.resourceCall(.release, lease);
    }
    fn observe(self: *Child, operation: wire.Operation, key: usize, previous: u64, next: u64) u8 {
        const response = self.resourceCall(operation, wire.Observation{ .key = key, .previous = previous, .next = next });
        const status: bridge.Status = .{ .code = response.code, .detail = response.detail };
        // Cache teardown must reach the parent; its caller cannot retry a lost
        // release once the cache object is destroyed.
        if (!status.isOk() and next < previous) self.resourceFailure();
        return @intFromBool(status.isOk());
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
        .on_closed = Child.closed,
        .next_id = 2,
    };
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
        try std.testing.expect(control.stream_started);
        try std.testing.expect(capture.saw_headers);
        try std.testing.expectEqual(@as(u16, 202), capture.status);
        try std.testing.expectEqualStrings(content_type, capture.bytes[0..capture.len]);
    }
}

test "inference worker retains reservations until reaped cleanup and owns partial startup" {
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
        .endpoint = .{ .alloc = alloc, .io = std.testing.io, .input = undefined, .output = undefined, .context = undefined, .handler = Worker.documentRequest, .resource_handler = Worker.resourceRequest, .on_closed = Worker.closed, .next_id = 1 },
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
    client.create_json = try alloc.dupe(u8, "{}");
    defer alloc.free(client.create_json);
    client.environment = std.process.Environ.Map.init(alloc);
    defer client.environment.deinit();
    const Fault = struct {
        var fail_at: usize = 0;
        var groups: usize = 0;
        var kills: usize = 0;
        var missing_executable: bool = false;
        fn concurrent(raw: ?*anyopaque, group: *std.Io.Group, context: []const u8, alignment: std.mem.Alignment, start: *const fn (*const anyopaque) void) std.Io.ConcurrentError!void {
            groups += 1;
            if (groups == fail_at) return error.ConcurrencyUnavailable;
            return std.testing.io.vtable.groupConcurrent(raw, group, context, alignment, start);
        }
        fn spawn(raw: ?*anyopaque, options: std.process.SpawnOptions) std.process.SpawnError!std.process.Child {
            if (missing_executable) return error.FileNotFound;
            var modified = options;
            modified.argv = &.{"/bin/cat"};
            modified.environ_map = null;
            return std.testing.io.vtable.processSpawn(raw, modified);
        }
        fn kill(raw: ?*anyopaque, child: *std.process.Child) void {
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
    client.io = .{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    // A missing override/sibling must stay actionable across the ABI and
    // must not enter the transient worker restart/capacity retry path.
    Fault.missing_executable = true;
    Fault.kills = 0;
    try std.testing.expectError(error.InferenceWorkerExecutableNotConfigured, client.ensureWorker(.{}));
    try std.testing.expectEqual(@as(usize, 0), Fault.kills);
    try std.testing.expect(client.worker == null);
    Fault.missing_executable = false;
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
        try std.testing.expect(client.worker == null);
        try std.testing.expectEqual(@as(u64, 0), client.generation);
        try std.testing.expect(!budget.leased);
    }
}

test "inference worker resource replies distinguish denials from ownership uncertainty" {
    for ([_]anyerror{ error.ResourceLimitExceeded, error.ResourceTemporarilyUnavailable }) |err| {
        const status = bridge.statusFromError(err);
        const denial: rpc.ResourceReply = .{ .code = status.code, .detail = status.detail };
        for ([_]wire.Operation{ .reserve, .retain, .prompt_cache, .tokenizer_cache }) |operation|
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

/// No borrowed root-request pointer enters this retained forwarding context.
/// Structured inference work drains before the root call returns; cancellation
/// may invalidate its parent capability while an already-borrowed call finishes.
const ForwardDocuments = struct {
    child: *Child,
    key: [16]u8,
    references: std.atomic.Value(usize) = .init(1),
    fn create(child: *Child, key: [16]u8) !*ForwardDocuments {
        const self = try child.alloc.create(ForwardDocuments);
        self.* = .{ .child = child, .key = key };
        return self;
    }
    fn view(self: *ForwardDocuments) bridge.DocumentStoreView {
        return .{ .context = self, .retain_context = retain, .release_context = release, .invoke = invoke, .destroy_response = destroyResponse };
    }
    fn retain(raw: *anyopaque) callconv(.c) u8 {
        const self: *ForwardDocuments = @ptrCast(@alignCast(raw));
        _ = self.references.fetchAdd(1, .monotonic);
        return 1;
    }
    fn release(raw: *anyopaque) callconv(.c) void {
        const self: *ForwardDocuments = @ptrCast(@alignCast(raw));
        if (self.references.fetchSub(1, .acq_rel) == 1) self.child.alloc.destroy(self);
    }
    const Response = struct { payload: rpc.OwnedPayload, parsed: std.json.Parsed(wire.Reply) };
    fn invoke(raw: *anyopaque, metadata: http.Bytes, body: http.Bytes, out: *document_abi.Response) callconv(.c) bridge.Status {
        const self: *ForwardDocuments = @ptrCast(@alignCast(raw));
        out.* = .{};
        self.call(metadata.slice(), body.slice(), out) catch |err| return bridge.statusFromErrorWithFallback(err, error.InferenceWorkerUnavailable);
        return .ok;
    }
    fn call(self: *ForwardDocuments, metadata: []const u8, body: []const u8, out: *document_abi.Response) !void {
        const a = self.child.alloc;
        const options = try std.json.Stringify.valueAlloc(a, wire.DocumentStoreRequest{ .capability = self.key, .metadata = metadata }, .{});
        defer a.free(options);
        const payload = try callSegments(&self.child.endpoint, .document_store, options, .{ .body = body }, .{});
        errdefer payload.deinit();
        var parsed = try std.json.parseFromSlice(wire.Reply, a, payload.view().metadata, .{});
        errdefer parsed.deinit();
        const response = try a.create(Response);
        response.* = .{ .payload = payload, .parsed = parsed };
        _ = retain(self);
        out.* = .{ .handle = response, .metadata = .init(parsed.value.options), .body = .init(payload.view().body) };
    }
    fn destroyResponse(raw: *anyopaque, handle: *anyopaque) callconv(.c) void {
        const self: *ForwardDocuments = @ptrCast(@alignCast(raw));
        const response: *Response = @ptrCast(@alignCast(handle));
        response.parsed.deinit();
        response.payload.deinit();
        self.child.alloc.destroy(response);
        release(raw);
    }
};

test "inference worker document capabilities revoke new borrows while preserving in-flight ownership" {
    const Owner = struct {
        references: usize = 1,
        fn retain(raw: *anyopaque) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.references += 1;
            return 1;
        }
        fn release(raw: *anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.references -= 1;
        }
        fn invoke(_: *anyopaque, metadata: http.Bytes, body: http.Bytes, out: *document_abi.Response) callconv(.c) bridge.Status {
            out.* = .{ .metadata = metadata, .body = body };
            return .ok;
        }
        fn destroy(_: *anyopaque, _: *anyopaque) callconv(.c) void {}
    };
    var owner = Owner{};
    var client = Client{ .alloc = std.testing.allocator, .io = std.testing.io, .create_json = undefined, .environment = undefined };
    defer client.document_bindings.deinit(std.testing.allocator);
    const view: bridge.DocumentStoreView = .{ .context = &owner, .retain_context = Owner.retain, .release_context = Owner.release, .invoke = Owner.invoke, .destroy_response = Owner.destroy };
    const token = (try client.registerDocumentStore(view)).?;
    const active = try client.borrowDocumentStore(token);
    try std.testing.expectEqual(@as(usize, 3), owner.references);
    client.unregisterDocumentStore(token);
    try std.testing.expectError(error.DocumentRunLeaseLost, client.borrowDocumentStore(token));
    try std.testing.expectEqual(@as(usize, 2), owner.references);
    var response: document_abi.Response = .{};
    const status = active.invoke.?(active.context.?, .init("metadata"), .init("raw checkpoint bytes"), &response);
    try std.testing.expect(status.isOk());
    try std.testing.expectEqualStrings("raw checkpoint bytes", response.body.slice());
    active.destroy(response);
    active.release();
    try std.testing.expectEqual(@as(usize, 1), owner.references);
    Owner.release(&owner);
    try std.testing.expectEqual(@as(usize, 0), owner.references);
}

test "native checkpoint worker upload honors cancellation counts and cumulative ceiling" {
    const Source = struct {
        calls: usize = 0,
        fail: http.CallbackStatus = .ok,
        oversized: bool = false,
        fn read(raw: ?*anyopaque, destination: [*]u8, capacity: usize, count: *usize) callconv(.c) http.CallbackStatus {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.fail != .ok) return self.fail;
            count.* = if (self.oversized) capacity + 1 else @min(capacity, 7);
            if (!self.oversized) @memset(destination[0..count.*], 42);
            return .ok;
        }
        fn canceled(_: ?*const anyopaque) callconv(.c) u8 {
            return 1;
        }
    };
    var source: Source = .{};
    var control: ForwardControl = .{ .alloc = std.testing.allocator, .cancellation = .{}, .stream_body = true, .body_source = .{ .context = &source, .read = Source.read, .streaming = 1 } };
    var buffer: [10]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 7), try control.view().read_body.?(&control, &buffer));
    source.oversized = true;
    try std.testing.expectError(error.InvalidBodyReadCount, control.view().read_body.?(&control, &buffer));
    try std.testing.expectEqual(@as(usize, 7), control.body_offset);
    source.oversized = false;
    control.body_offset = wire.native_checkpoint_upload_limit;
    try std.testing.expectError(error.BodyTooLarge, control.view().read_body.?(&control, &buffer));
    control.body_offset = 0;
    source.fail = .timeout;
    try std.testing.expectError(error.Timeout, control.view().read_body.?(&control, &buffer));
    control.cancellation = .{ .is_cancelled = Source.canceled };
    const calls = source.calls;
    try std.testing.expectError(error.Cancelled, control.view().read_body.?(&control, &buffer));
    try std.testing.expectEqual(calls, source.calls);
    control.cancellation = .{};
    control.borrowed_body = "checkpoint";
    try std.testing.expectEqual(@as(usize, 10), try control.view().read_body.?(&control, &buffer));
    try std.testing.expectEqualStrings("checkpoint", &buffer);
    try std.testing.expectEqual(@as(usize, 0), try control.view().read_body.?(&control, &buffer));
    try std.testing.expectEqual(calls, source.calls);
}

test "inference worker checkpoint upload feeds incremental HTTP reads and recovers after cancellation" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    const Fixture = struct {
        parent: rpc.Endpoint = undefined,
        child: rpc.Endpoint = undefined,
        files: [4]std.Io.File = undefined,
        remaining: usize = 0,
        sent: usize = 0,
        calls: usize = 0,
        cancel_after: usize = std.math.maxInt(usize),
        fn closed(_: *anyopaque) void {}
        fn reject(_: *anyopaque, _: *rpc.Request) !rpc.OwnedPayload {
            return error.UnexpectedRequest;
        }
        fn read(raw: ?*anyopaque, destination: [*]u8, capacity: usize, count: *usize) callconv(.c) http.CallbackStatus {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.calls >= self.cancel_after) return .canceled;
            self.calls += 1;
            const n = @min(self.remaining, capacity, if (self.calls == 2) @as(usize, 7) else capacity);
            for (destination[0..n], 0..) |*byte, index| byte.* = @truncate((self.sent + index) *% 31);
            count.* = n;
            self.remaining -= n;
            self.sent += n;
            return .ok;
        }
        fn consume(_: *anyopaque, request: *rpc.Request) !rpc.OwnedPayload {
            var body: NativeHttpBody = .{ .request = request };
            var buffer: [4093]u8 = undefined;
            var total: usize = 0;
            while (true) {
                const capacity: usize = if (total == 0) 1 else if (total == 1) 3 else buffer.len;
                var n: usize = 0;
                try callbackResult(NativeHttpBody.read(&body, &buffer, capacity, &n));
                if (n == 0) break;
                if (n > capacity) return error.InvalidBodyReadCount;
                for (buffer[0..n], 0..) |byte, index|
                    if (byte != @as(u8, @truncate((total + index) *% 31))) return error.BodyContentMismatch;
                total += n;
            }
            var eof_count: usize = 99;
            try callbackResult(NativeHttpBody.read(&body, &buffer, buffer.len, &eof_count));
            if (eof_count != 0) return error.BodyContentAfterEof;
            var result: [8]u8 = undefined;
            std.mem.writeInt(u64, &result, total, .little);
            return request.endpoint.copyPayload(.{ .body = &result });
        }
        fn reset(self: *@This(), bytes: usize, cancel_after: usize) void {
            self.remaining = bytes;
            self.sent = 0;
            self.calls = 0;
            self.cancel_after = cancel_after;
        }
        fn run(self: *@This()) !rpc.OwnedPayload {
            var control: ForwardControl = .{
                .alloc = std.testing.allocator,
                .cancellation = .{},
                .stream_body = true,
                .body_source = .{ .context = self, .read = read, .streaming = 1 },
            };
            return self.parent.call(.{}, control.view());
        }
    };
    var fixture: Fixture = .{};
    const up = try std.Io.Threaded.pipe2(.{ .CLOEXEC = true });
    const down = try std.Io.Threaded.pipe2(.{ .CLOEXEC = true });
    for (up ++ down, &fixture.files) |fd, *file| file.* = .{ .handle = fd, .flags = .{ .nonblocking = false } };
    defer for (fixture.files) |file| file.close(std.testing.io);
    fixture.parent = .{ .alloc = std.testing.allocator, .io = std.testing.io, .input = fixture.files[0], .output = fixture.files[3], .context = &fixture, .handler = Fixture.reject, .on_closed = Fixture.closed, .next_id = 1 };
    fixture.child = .{ .alloc = std.testing.allocator, .io = std.testing.io, .input = fixture.files[2], .output = fixture.files[1], .context = &fixture, .handler = Fixture.consume, .on_closed = Fixture.closed, .next_id = 2 };
    try fixture.parent.start();
    defer fixture.parent.deinit();
    try fixture.child.start();
    defer fixture.child.deinit();
    fixture.reset(rpc.max_body_bytes + 19, std.math.maxInt(usize));
    const large = try fixture.run();
    defer large.deinit();
    try std.testing.expectEqual(@as(u64, rpc.max_body_bytes + 19), std.mem.readInt(u64, large.view().body[0..8], .little));
    try std.testing.expect(fixture.calls > 1024);
    fixture.reset(1024 * 1024, 2);
    try std.testing.expectError(error.Cancelled, fixture.run());
    fixture.reset(100123, std.math.maxInt(usize));
    const recovery = try fixture.run();
    defer recovery.deinit();
    try std.testing.expectEqual(@as(u64, 100123), std.mem.readInt(u64, recovery.view().body[0..8], .little));
    try std.testing.expect(!fixture.parent.closed.load(.acquire));
    try std.testing.expect(!fixture.child.closed.load(.acquire));
}
