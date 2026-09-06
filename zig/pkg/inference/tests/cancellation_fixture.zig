// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Test-only executable. The HTTP routes, model handles, tokenizer, admission,
//! execution control, watchdog and process supervisor are production code.
//! Only the Session vtable is synthetic; no fault hooks ship in the server.
const std = @import("std");
const inference = @import("inference");
const httpx = @import("httpx");
const platform = @import("antfly_platform");
const Session = inference.backends.Session;
const Tensor = inference.backends.Tensor;
const TensorInfo = inference.backends.TensorInfo;
const Control = inference.InferenceExecutionControl;

const Fixture = struct {
    node: *inference.server.Node,
    hard: bool,
    block_next: std.atomic.Value(bool) = .init(false),
    active: std.atomic.Value(usize) = .init(0),
    cancelled: std.atomic.Value(usize) = .init(0),
    entered: std.atomic.Value(usize) = .init(0),

    fn state(self: *@This(), ctx: *httpx.Context) !httpx.Response {
        const memory = self.node.model_manager.resource_domain.?.admission.snapshot();
        return ctx.json(.{
            .pid = std.posix.system.getpid(),
            .active = self.active.load(.acquire),
            .entered = self.entered.load(.acquire),
            .cancelled = self.cancelled.load(.acquire),
            .requests = self.node.inference_admission.inFlightRequests(),
            .units = self.node.inference_admission.inFlightUnits(),
            .scratch = memory.host_scratch_bytes,
        });
    }

    fn arm(self: *@This(), ctx: *httpx.Context) !httpx.Response {
        self.block_next.store(true, .release);
        return ctx.json(.{ .armed = true });
    }

    fn run(_: *anyopaque, _: []const Tensor, _: std.mem.Allocator) ![]Tensor {
        return error.UncontrolledFixtureExecution;
    }

    fn runWithControl(raw: *anyopaque, inputs: []const Tensor, alloc: std.mem.Allocator, control: Control) ![]Tensor {
        const self: *@This() = @ptrCast(@alignCast(raw));
        _ = self.active.fetchAdd(1, .acq_rel);
        defer _ = self.active.fetchSub(1, .acq_rel);
        _ = self.entered.fetchAdd(1, .acq_rel);
        if (self.block_next.swap(false, .acq_rel)) {
            if (self.hard) {
                // An actual uninterruptible call: no checks, sleeps, or direct
                // restart request. Only the production watchdog can stop it.
                while (true) std.atomic.spinLoopHint();
            }
            while (true) {
                control.check() catch |err| {
                    _ = self.cancelled.fetchAdd(1, .acq_rel);
                    return err;
                };
                try control.io.?.sleep(.fromMilliseconds(1), .awake);
            }
        }
        try control.check();
        const batch = inputs[0].shape[0];
        const sequence = inputs[0].shape[1];
        const values = try alloc.alloc(f32, @intCast(batch * sequence * 4));
        defer alloc.free(values);
        @memset(values, 1);
        const outputs = try alloc.alloc(Tensor, 1);
        errdefer alloc.free(outputs);
        outputs[0] = try Tensor.initFloat32(alloc, "last_hidden_state", &.{ batch, sequence, 4 }, values);
        return outputs;
    }

    fn inputInfo(_: *anyopaque) []const TensorInfo {
        return &.{
            .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
            .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, -1 } },
        };
    }
    fn outputInfo(_: *anyopaque) []const TensorInfo {
        return &.{.{ .name = "last_hidden_state", .dtype = .f32, .shape = &.{ -1, -1, 4 } }};
    }
    fn backend(_: *anyopaque) inference.backends.BackendType {
        return .native;
    }
    fn interruption(raw: *anyopaque) inference.execution_control.Interruption {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return if (self.hard) .process_required else .cooperative;
    }
    fn close(_: *anyopaque) void {}
    const vtable = Session.VTable{
        .run = run,
        .runWithControl = runWithControl,
        .inputInfo = inputInfo,
        .outputInfo = outputInfo,
        .backend = backend,
        .interruption = interruption,
        .close = close,
    };
};

pub fn main(init: std.process.Init) !void {
    var lifetime = platform.inference_process_supervisor.WorkerLifetime{};
    defer lifetime.deinit(init.io);
    if (try platform.inference_process_supervisor.runIfNeeded(init, 1, &lifetime)) return;
    const alloc = init.gpa;
    const model_dir = init.environ_map.get("FIXTURE_MODEL_DIR") orelse return error.MissingModelDir;
    const port = try std.fmt.parseInt(u16, init.environ_map.get("FIXTURE_PORT") orelse return error.MissingPort, 10);
    var node = try inference.server.Node.init(alloc, .{
        .models_dir = std.fs.path.dirname(model_dir) orelse return error.MissingModelsRoot,
        .allow_unknown_models = true,
        .process_termination_available = true,
        .max_concurrent_requests = 1,
    });
    defer node.deinit();
    try node.attachIo(init.io);
    try node.model_manager.ensureResourceOwnerReady();
    var fixture = Fixture{ .node = &node, .hard = std.mem.eql(u8, init.environ_map.get("FIXTURE_MODE") orelse "", "hard") };

    const ModelPtr = @typeInfo(@TypeOf(node.model_manager.loaded.get(model_dir))).optional.child;
    const model = try alloc.create(std.meta.Child(ModelPtr));
    model.* = .{
        .allocator = alloc,
        .model_dir = try alloc.dupe(u8, model_dir),
        .manifest = try inference.models.manifest.loadFromDir(alloc, model_dir),
        .hf_tok = try inference.hf_tokenizer.HfTokenizer.loadFromBytes(alloc,
            \\{"version":"1.0","model":{"type":"BPE","vocab":{"a":0,"b":1},"merges":[]}}
        ),
        .sp_tok = null,
        .session = .{
            .ptr = &fixture,
            .vtable = &Fixture.vtable,
            .run_admission = .{
                .controller = &node.model_manager.resource_domain.?.admission,
                .backend_class = .cpu,
                .limits = .{},
                .static_workspace_bytes = 4096,
            },
        },
        .session_manager = &node.session_manager,
        .model_manager = &node.model_manager,
        .prompt_prefix_cache = inference.runtime.kv.prompt_cache.PromptPrefixCache.init(alloc),
        .native_generation_graph_cache = inference.graph.cache.GraphCache.init(alloc),
        .pinned = true,
    };
    try node.model_manager.loaded.put(alloc, try alloc.dupe(u8, model_dir), model);

    var server = httpx.Server.initWithConfig(alloc, init.io, node.httpServerConfig("127.0.0.1", port));
    defer server.deinit();
    try node.registerHttpRoutes(&server);
    try server.get("/_fixture/state", httpx.Handler.bind(&fixture, Fixture.state));
    try server.post("/_fixture/arm", httpx.Handler.bind(&fixture, Fixture.arm));
    try server.listen();
}
