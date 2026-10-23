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

//! Checked private engine calls shared by server adapters and the Apache host.
const std = @import("std");
pub const enabled = @import("engine_options").shared_inference_core;
pub const exports = @import("engine_exports.zig");
const inference = @import("inference_server");
const httpx = @import("httpx");
const native = @import("antfly_runtime_abi").native_abi;
const io_abi = @import("antfly_runtime_abi").io_abi;
pub const memory = @import("engine_memory");
pub const callback = @import("antfly_runtime_abi").callback_abi;
pub const NodeConfig = inference.server.NodeConfig;
pub const HubConfig = inference.registry.download.HubConfig;
pub const ProjectorSelection = inference.registry.download.ProjectorSelection;
pub const Command = enum { worker, embed, classify, generate, chat, compile_artifact, export_model, quantize, run_artifact, transcribe, read, extract, compare, finetune, cuda_info, smoke, convert, rerank, decisions, extract_v2, a4b_pack, quant_kernel_codegen, metal_info, bench_cuda };

// Borrowed for the blocking serve operation. The server adapter owns signals,
// the hard shutdown watchdog and readiness/failure publication; the core owns
// its HTTP transport so raw handler and I/O errors never cross archives.
pub const Lifecycle = extern struct {
    context: *anyopaque,
    poll: *const fn (*anyopaque) callconv(.c) u8,
    ready: *const fn (*anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status,
    fail: *const fn (*anyopaque, @import("antfly_runtime_abi").error_abi.Status) callconv(.c) void,
    cancel: *const fn (*anyopaque) callconv(.c) void,
    remaining_ms: *const fn (*anyopaque) callconv(.c) u64,
};
pub const Failure = extern struct {
    ptr: ?[*]const u8 = null,
    len: usize = 0,
    pub fn init(name: []const u8) Failure {
        return .{ .ptr = name.ptr, .len = name.len };
    }
};
pub fn Outcome(comptime T: type) type {
    return struct { value: T = undefined, failure: Failure = .{} };
}
pub const VTable = struct {
    create_node: *const fn (memory.Allocator, NodeConfig) Outcome(*anyopaque),
    destroy_node: *const fn (*anyopaque) Outcome(void),
    attach_io: *const fn (*anyopaque, io_abi.Borrow) Outcome(void),
    warm: *const fn (*anyopaque) Outcome(void),
    readiness: *const fn (*anyopaque) Outcome(void),
    test_admission: *const fn (*anyopaque) Outcome(void),
    validate_bind: *const fn (*anyopaque, []const u8) Outcome(void),
    serve: *const fn (*anyopaque, []const u8, u16, Lifecycle) Outcome(void),
    create_registry: *const fn (memory.Allocator, []const u8) Outcome(*anyopaque),
    destroy_registry: *const fn (*anyopaque) Outcome(void),
    list_models: *const fn (*anyopaque, io_abi.Borrow) Outcome(void),
    pull: *const fn (*anyopaque, io_abi.Borrow, []const u8, HubConfig, ?[]const u8, ?[]const u8, ProjectorSelection) Outcome(void),
    predictor_pull: *const fn (memory.Allocator, io_abi.Borrow, []const []const u8, []const u8) Outcome(void),
    command: *const fn (Command, *const std.process.Init, memory.Allocator, io_abi.Borrow, []const []const u8, []const u8) Outcome(void),
};
pub const Boundary = callback.Boundary(VTable);
pub const layout = blk: {
    @setEvalBranchQuota(8192);
    break :blk native.TypeContract.of(VTable);
};
pub const Descriptor = extern struct {
    layout: native.TypeContract,
    dispatch: callback.CallbackDispatch,
    table: *const anyopaque,
};
extern fn antfly_inference_engine_get_function_table() callconv(.c) *const Descriptor;

fn Result(comptime name: []const u8) type {
    const Function = @typeInfo(@FieldType(VTable, name)).pointer.child;
    const Wire = @typeInfo(Function).@"fn".return_type.?;
    return anyerror!@FieldType(Wire, "value");
}
pub fn call(comptime name: []const u8, args: anytype) Result(name) {
    const descriptor = if (@import("builtin").is_test) &@import("engine_exports.zig").descriptor else antfly_inference_engine_get_function_table();
    if (!descriptor.layout.matches(layout)) return error.UnsupportedVersion;
    const table: *const VTable = @ptrCast(@alignCast(descriptor.table));
    const outcome = try Boundary.call(name, descriptor.dispatch, @field(table, name), args);
    if (outcome.failure.ptr) |ptr| {
        const error_name = ptr[0..outcome.failure.len];
        return @import("engine_errors").fromName(error_name);
    }
    return outcome.value;
}

// The callback descriptor points to a stable allocation, never an init stack
// parameter. Destroy the remote owner before releasing this allocator borrow.
const AllocatorLease = struct {
    value: *std.mem.Allocator,
    fn init(alloc: std.mem.Allocator) !AllocatorLease {
        const value = try alloc.create(std.mem.Allocator);
        value.* = alloc;
        return .{ .value = value };
    }
    fn borrow(self: AllocatorLease) memory.Allocator {
        return memory.Allocator.fromStd(self.value);
    }
    fn deinit(self: AllocatorLease) void {
        const alloc = self.value.*;
        alloc.destroy(self.value);
    }
};

pub const Node = struct {
    handle: *anyopaque,
    lease: AllocatorLease,
    pub fn init(alloc: std.mem.Allocator, config: NodeConfig) !Node {
        const lease = try AllocatorLease.init(alloc);
        errdefer lease.deinit();
        return .{ .handle = try call("create_node", .{ lease.borrow(), config }), .lease = lease };
    }
    pub fn deinit(self: *Node) void {
        call("destroy_node", .{self.handle}) catch unreachable;
        self.lease.deinit();
    }
    pub fn attachIo(self: *Node, io: std.Io) !void {
        return call("attach_io", .{ self.handle, io_abi.Borrow.init(&io) });
    }
    pub fn warmConfiguredGenerators(self: *Node, _: std.mem.Allocator) !void {
        return call("warm", .{self.handle});
    }
    pub fn startReadinessInventory(self: *Node, _: std.Io) !void {
        return call("readiness", .{self.handle});
    }
    pub fn configureForcedRunAdmissionDenialsFromEnvironmentForTesting(self: *Node) void {
        call("test_admission", .{self.handle}) catch unreachable;
    }
    pub fn validateHttpBind(self: *Node, host: []const u8) !void {
        return call("validate_bind", .{ self.handle, host });
    }
    pub fn serve(self: *Node, host: []const u8, port: u16, lifecycle: Lifecycle) !void {
        return call("serve", .{ self.handle, host, port, lifecycle });
    }
};
pub const ModelRegistry = struct {
    handle: *anyopaque,
    lease: AllocatorLease,
    pub fn init(alloc: std.mem.Allocator, directory: []const u8) !ModelRegistry {
        const lease = try AllocatorLease.init(alloc);
        errdefer lease.deinit();
        return .{ .handle = try call("create_registry", .{ lease.borrow(), directory }), .lease = lease };
    }
    pub fn deinit(self: *ModelRegistry) void {
        call("destroy_registry", .{self.handle}) catch unreachable;
        self.lease.deinit();
    }
    pub fn list(self: *ModelRegistry, io: std.Io) !void {
        return call("list_models", .{ self.handle, io_abi.Borrow.init(&io) });
    }
    pub fn pull(self: *ModelRegistry, io: std.Io, ref: []const u8, config: HubConfig, tasks: ?[]const u8, capabilities: ?[]const u8, projector: ProjectorSelection) !void {
        return call("pull", .{ self.handle, io_abi.Borrow.init(&io), ref, config, tasks, capabilities, projector });
    }
};
pub fn runCommand(command: Command, init: std.process.Init, args: []const []const u8, ml_dir: []const u8) !void {
    return call("command", .{ command, &init, memory.Allocator.fromStd(&init.gpa), io_abi.Borrow.init(&init.io), args, ml_dir });
}

pub fn pullPredictor(alloc: std.mem.Allocator, io: std.Io, args: []const []const u8, ml_dir: []const u8) !void {
    return call("predictor_pull", .{ memory.Allocator.fromStd(&alloc), io_abi.Borrow.init(&io), args, ml_dir });
}
