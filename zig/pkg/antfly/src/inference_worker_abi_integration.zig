// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Real private worker transport gate. The child enters the production archive
//! through the process ABI; the parent uses the production worker client.
const std = @import("std");
const bridge = @import("antfly_inference_bridge");
const worker = @import("antfly_inference_host").worker_module;
const process = @import("runtime_bridge.zig");
extern fn antfly_runtime_inference(*const process.Context) callconv(.c) c_int;

pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();
    if (args.next()) |command| {
        if (!std.mem.eql(u8, command, "inference") or !std.mem.eql(u8, args.next() orelse "", "_worker") or args.next() != null)
            return error.InvalidArguments;
        const views = [_]process.Bytes{.init("_worker")};
        const code = antfly_runtime_inference(&.{ .command = .init("inference"), .arguments_ptr = &views, .arguments_len = views.len });
        if (code != 0) return error.InferenceWorkerFailed;
        return;
    }
    const alloc = init.gpa;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var ignored: ?*anyopaque = null;
    const context = bridge.CreateContext{
        .abi_version = bridge.abi_version,
        .data_dir_ptr = ".".ptr,
        .data_dir_len = 1,
        .models_dir = .init("."),
        .ml_dir = .init("."),
        .host_limit_bytes = 0,
        .backend_limit_bytes = 0,
        .combined_limit_bytes = 0,
        .kv_limit_bytes = 0,
        .scratch_limit_bytes = 0,
        .process_memory_limit_bytes = std.math.maxInt(usize),
        .preload_ptr = null,
        .preload_len = 0,
        .keep_alive = .{},
        .max_loaded_models = 0,
        .has_max_loaded_models = 0,
        .content_security_json = .{},
        .s3_credentials_json = .{},
        .runtime_config_json = .init("{\"embedded_enabled\":false}"),
        .executor = .init(&io),
        .out_handle = &ignored,
    };
    var budget = Budget{};
    const client = try worker.Client.create(alloc, io, &context);
    var live = true;
    defer if (live) client.deinit();
    try client.configure(budget.port());
    var parent = Parent{};
    const borrow = bridge.runnable_abi.Borrowed{ .context = &parent, .cpu_slots = 2, .parent_class = @backingInt(bridge.runnable_abi.Class.query), .checkpoint = Parent.check, .park = Parent.park, .enter = Parent.enter };
    var response: ?*anyopaque = null;
    var json = bridge.String.init("");
    const invoke = bridge.ProviderInvokeContext{ .abi_version = bridge.abi_version, .handle = undefined, .operation = @backingInt(bridge.ProviderOperation.read_encoded_images), .request_json = .init("{\"model\":\"unused\",\"image_count\":1}"), .deadline_ns = 0, .has_deadline = 0, .out_response_handle = &response, .out_response_json = &json, .runnable = &borrow };
    var transfer = try bridge.runnable_abi.Transfer.init(borrow, .inference);
    const result = worker.invokeProvider(client, &invoke);
    try transfer.returnToParent();
    if (result) |owned| {
        switch (owned) {
            .json => |bytes| alloc.free(bytes),
            .numeric => |numeric| {
                for (numeric.rows) |row| alloc.free(row);
                alloc.free(numeric.rows);
            },
        }
        return error.UnexpectedWorkerResponse;
    } else |err| if (err != error.InvalidArguments) return err;
    if (parent.enters != 2 or parent.parked or parent.class != @backingInt(bridge.runnable_abi.Class.query))
        return error.InvalidParentRestoration;
    // Bypass the client-side demand validator to prove rejection is performed
    // by the real child before it attempts to parse an empty attachment body.
    const invalid = client.invoke(.provider, "{\"operation\":13,\"deadline_ns\":null,\"numeric\":false,\"attachment_refs\":[],\"admitted_cpu_slots\":65536}", "", .{});
    if (invalid) |owned| {
        var value = owned;
        value.deinit();
        return error.InvalidWorkerDemandAccepted;
    } else |err| if (err != error.InvalidRunnableDemand) return err;
    client.deinit();
    live = false;
    if (budget.refs != 0 or budget.leases != 0 or budget.transient_bytes != 0) return error.WorkerOwnershipLeaked;
}

const Budget = struct {
    refs: usize = 0,
    leases: usize = 0,
    next_lease: usize = 1,
    transient_bytes: u64 = 0,
    fn retain(raw: *anyopaque) callconv(.c) u8 {
        const self: *Budget = @ptrCast(@alignCast(raw));
        self.refs += 1;
        return 1;
    }
    fn release(raw: *anyopaque) callconv(.c) void {
        const self: *Budget = @ptrCast(@alignCast(raw));
        self.refs -= 1;
    }
    fn reserve(raw: *anyopaque, _: *const bridge.AdmissionAmounts, out: *usize) callconv(.c) bridge.Status {
        const self: *Budget = @ptrCast(@alignCast(raw));
        self.leases += 1;
        out.* = self.next_lease;
        self.next_lease += 1;
        return .ok;
    }
    fn retainLease(_: *anyopaque, _: usize, _: *const bridge.AdmissionAmounts) callconv(.c) bridge.Status {
        return .ok;
    }
    fn releaseLease(raw: *anyopaque, _: usize) callconv(.c) void {
        const self: *Budget = @ptrCast(@alignCast(raw));
        self.leases -= 1;
    }
    fn observe(_: *anyopaque, _: usize, _: u64, _: u64) callconv(.c) u8 {
        return 1;
    }
    fn adjust(raw: *anyopaque, _: usize, previous: u64, next: u64) callconv(.c) u8 {
        const self: *Budget = @ptrCast(@alignCast(raw));
        if (self.transient_bytes != previous) return 0;
        self.transient_bytes = next;
        return 1;
    }
    fn port(self: *Budget) bridge.ResourceBudget {
        return .{ .abi_version = bridge.abi_version, .context = self, .retain_context = retain, .release_context = release, .reserve_admission = reserve, .retain_admission = retainLease, .release_admission = releaseLease, .observe_prompt_cache = observe, .observe_tokenizer_cache = observe, .adjust_transient_host = adjust };
    }
};

const Parent = struct {
    class: c_int = @backingInt(bridge.runnable_abi.Class.query),
    parked: bool = false,
    enters: usize = 0,
    fn check(_: *anyopaque) callconv(.c) bridge.Status {
        return .ok;
    }
    fn park(raw: *anyopaque, _: c_int) callconv(.c) bridge.Status {
        const self: *Parent = @ptrCast(@alignCast(raw));
        self.parked = true;
        return .ok;
    }
    fn enter(raw: *anyopaque, class: c_int) callconv(.c) bridge.Status {
        const self: *Parent = @ptrCast(@alignCast(raw));
        self.parked = false;
        self.class = class;
        self.enters += 1;
        return .ok;
    }
};
