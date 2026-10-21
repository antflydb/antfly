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

//! Native engine owner. No server coordination or server command roots.
const std = @import("std");
const engine = @import("antfly_inference_engine");
const inference = @import("inference_server");
const httpx = @import("httpx");
const io_abi = @import("antfly_runtime_abi").io_abi;
const memory = engine.memory;
const OwnedNode = struct {
    memory: memory.Allocator,
    node: inference.server.Node,
    io_receiver: ?io_abi.Receiver = null,
    fn allocator(self: *OwnedNode) std.mem.Allocator {
        return self.memory.asStd();
    }
};
fn node(handle: *anyopaque) *OwnedNode {
    return @ptrCast(@alignCast(handle));
}
fn createNode(allocator: memory.Allocator, config: engine.NodeConfig) !*anyopaque {
    if (!allocator.valid()) return error.InvalidArgument;
    const owner = try allocator.asStd().create(OwnedNode);
    errdefer allocator.asStd().destroy(owner);
    owner.memory = allocator;
    owner.io_receiver = null;
    owner.node = try inference.server.Node.init(owner.allocator(), config);
    return owner;
}
fn destroyNode(handle: *anyopaque) void {
    const owner = node(handle);
    owner.node.deinit();
    const allocator = owner.memory;
    allocator.asStd().destroy(owner);
}
fn attachIo(handle: *anyopaque, io: io_abi.Borrow) !void {
    const owner = node(handle);
    if (owner.io_receiver != null) return error.InvalidArgument;
    owner.io_receiver = try io.receive();
    try owner.node.attachIo(owner.io_receiver.?.io());
}
fn warm(handle: *anyopaque) !void {
    const owner = node(handle);
    try owner.node.warmConfiguredGenerators(owner.allocator());
}
fn readiness(handle: *anyopaque) !void {
    const owner = node(handle);
    if (owner.io_receiver == null) return error.InvalidArgument;
    try owner.node.startReadinessInventory(owner.io_receiver.?.io());
}
fn testAdmission(handle: *anyopaque) void {
    node(handle).node.configureForcedRunAdmissionDenialsFromEnvironmentForTesting();
}
fn validateBind(handle: *anyopaque, host: []const u8) !void {
    try node(handle).node.validateHttpBind(host);
}
fn serve(handle: *anyopaque, host: []const u8, port: u16, lifecycle: engine.Lifecycle) !void {
    const owner = node(handle);
    if (owner.io_receiver == null) return error.InvalidArgument;
    const io = owner.io_receiver.?.io();
    const alloc = owner.allocator();
    var server = httpx.Server.initWithConfig(alloc, io, owner.node.httpServerConfig(host, port));
    defer server.deinit();
    try owner.node.registerHttpRoutes(&server);
    var listener = httpx.ListenerTask.init(&server);
    try listener.start();
    defer {
        listener.shutdown(lifecycle.remaining_ms(lifecycle.context));
        listener.join() catch |err| std.log.err("inference listener failed during shutdown err={s}", .{@errorName(err)});
    }
    if (server.boundAddress()) |address| std.debug.print("listening on http://{f}\n", .{address}) else return error.MissingInferenceListener;
    const readiness_status = lifecycle.ready(lifecycle.context);
    if (!readiness_status.isOk()) return @import("antfly_runtime_abi").error_abi.errorFromStatus(readiness_status);
    while (lifecycle.poll(lifecycle.context) == 0) {
        if (listener.runtimeFailure()) |err| {
            std.log.err("inference public HTTP failed err={s}", .{@errorName(err)});
            lifecycle.fail(lifecycle.context, @import("antfly_runtime_abi").error_abi.statusFromError(err));
            return err;
        }
        io.sleep(.fromMilliseconds(10), .awake) catch |err| switch (err) {
            error.Canceled => lifecycle.cancel(lifecycle.context),
        };
    }
}
const OwnedRegistry = struct { memory: memory.Allocator, registry: inference.registry.ModelRegistry };
fn registry(handle: *anyopaque) *OwnedRegistry {
    return @ptrCast(@alignCast(handle));
}
fn createRegistry(allocator: memory.Allocator, directory: []const u8) !*anyopaque {
    if (!allocator.valid()) return error.InvalidArgument;
    const owner = try allocator.asStd().create(OwnedRegistry);
    owner.memory = allocator;
    owner.registry = inference.registry.ModelRegistry.init(owner.memory.asStd(), directory);
    return owner;
}
fn destroyRegistry(handle: *anyopaque) void {
    const owner = registry(handle);
    owner.registry.deinit();
    const allocator = owner.memory;
    allocator.asStd().destroy(owner);
}
fn listModels(handle: *anyopaque, io: io_abi.Borrow) !void {
    const owner = registry(handle);
    var receiver = try io.receive();
    const models = try owner.registry.discover(receiver.io());
    defer owner.memory.asStd().free(models);
    for (models) |model| std.debug.print("{s:<12} {s}\n", .{ @tagName(model.kind), model.name });
}
fn pull(handle: *anyopaque, io: io_abi.Borrow, ref: []const u8, config: engine.HubConfig, tasks: ?[]const u8, capabilities: ?[]const u8, projector: engine.ProjectorSelection) !void {
    var receiver = try io.receive();
    try registry(handle).registry.pull(receiver.io(), ref, config, tasks, capabilities, projector);
}
fn command(kind: engine.Command, init: *const std.process.Init, allocator: memory.Allocator, executor: io_abi.Borrow, args: []const []const u8, _: []const u8) !void {
    var receiver = try executor.receive();
    var local_init = init.*;
    local_init.io = receiver.io();
    if (!allocator.valid()) return error.InvalidArgument;
    local_init.gpa = allocator.asStd();
    const alloc = local_init.gpa;
    const io = local_init.io;
    switch (kind) {
        .worker => try @import("antfly_inference_host").worker_module.runChild(alloc, io),
        .embed => try inference.native_embed.main(alloc, io, args),
        .classify => try inference.native_classify.main(alloc, io, args),
        .generate => try inference.native_generate.main(alloc, io, args),
        .chat => try inference.native_chat.main(alloc, io, args),
        .compile_artifact => try inference.native_compile.main(alloc, io, args),
        .export_model => try inference.native_export.main(alloc, io, args),
        .quantize => try inference.native_quantize.main(alloc, io, args),
        .run_artifact => try inference.native_run_artifact.main(alloc, io, args),
        .transcribe => try inference.native_transcribe.main(alloc, io, args),
        .read => try inference.native_read.main(alloc, io, args),
        .extract => try inference.native_extract.main(alloc, io, args),
        .compare => try inference.compare_generate.main(alloc, io, args),
        .finetune => try inference.finetune_cli.main(local_init, args),
        .cuda_info => try inference.cuda_info.main(alloc, io, args),
        .smoke => try inference.native_smoke.main(alloc, io, args),
        .convert => try inference.tabular.cli.convertMain(alloc, io, args),
    }
}
fn pullPredictor(allocator: memory.Allocator, executor: io_abi.Borrow, args: []const []const u8, ml_dir: []const u8) !void {
    if (!allocator.valid()) return error.InvalidArgument;
    var receiver = try executor.receive();
    try inference.tabular.cli.pullMain(allocator.asStd(), receiver.io(), args, ml_dir);
}
fn createNodeOutcome(allocator: memory.Allocator, config: engine.NodeConfig) engine.Outcome(*anyopaque) {
    return .{ .value = createNode(allocator, config) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn destroyNodeOutcome(handle: *anyopaque) engine.Outcome(void) {
    return .{ .value = destroyNode(handle) };
}
fn attachIoOutcome(handle: *anyopaque, io: io_abi.Borrow) engine.Outcome(void) {
    return .{ .value = attachIo(handle, io) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn warmOutcome(handle: *anyopaque) engine.Outcome(void) {
    return .{ .value = warm(handle) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn readinessOutcome(handle: *anyopaque) engine.Outcome(void) {
    return .{ .value = readiness(handle) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn testAdmissionOutcome(handle: *anyopaque) engine.Outcome(void) {
    return .{ .value = testAdmission(handle) };
}
fn validateBindOutcome(handle: *anyopaque, host: []const u8) engine.Outcome(void) {
    return .{ .value = validateBind(handle, host) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn serveOutcome(handle: *anyopaque, host: []const u8, port: u16, lifecycle: engine.Lifecycle) engine.Outcome(void) {
    return .{ .value = serve(handle, host, port, lifecycle) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn createRegistryOutcome(allocator: memory.Allocator, directory: []const u8) engine.Outcome(*anyopaque) {
    return .{ .value = createRegistry(allocator, directory) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn destroyRegistryOutcome(handle: *anyopaque) engine.Outcome(void) {
    return .{ .value = destroyRegistry(handle) };
}
fn listModelsOutcome(handle: *anyopaque, io: io_abi.Borrow) engine.Outcome(void) {
    return .{ .value = listModels(handle, io) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn pullOutcome(handle: *anyopaque, io: io_abi.Borrow, ref: []const u8, config: engine.HubConfig, tasks: ?[]const u8, capabilities: ?[]const u8, projector: engine.ProjectorSelection) engine.Outcome(void) {
    return .{ .value = pull(handle, io, ref, config, tasks, capabilities, projector) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn commandOutcome(kind: engine.Command, init: *const std.process.Init, allocator: memory.Allocator, executor: io_abi.Borrow, args: []const []const u8, unused: []const u8) engine.Outcome(void) {
    return .{ .value = command(kind, init, allocator, executor, args, unused) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn pullPredictorOutcome(allocator: memory.Allocator, executor: io_abi.Borrow, args: []const []const u8, ml_dir: []const u8) engine.Outcome(void) {
    return .{ .value = pullPredictor(allocator, executor, args, ml_dir) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
const table: engine.VTable = .{ .create_node = createNodeOutcome, .destroy_node = destroyNodeOutcome, .attach_io = attachIoOutcome, .warm = warmOutcome, .readiness = readinessOutcome, .test_admission = testAdmissionOutcome, .validate_bind = validateBindOutcome, .serve = serveOutcome, .create_registry = createRegistryOutcome, .destroy_registry = destroyRegistryOutcome, .list_models = listModelsOutcome, .pull = pullOutcome, .command = commandOutcome, .predictor_pull = pullPredictorOutcome };
pub const descriptor: engine.Descriptor = .{ .layout = engine.layout, .dispatch = engine.Boundary.local_dispatch, .table = &table };
fn getFunctionTable() callconv(.c) *const engine.Descriptor {
    return &descriptor;
}
comptime {
    if (!@import("builtin").is_test) @export(&getFunctionTable, .{ .name = "antfly_inference_engine_get_function_table", .visibility = .hidden });
}
