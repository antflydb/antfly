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

const std = @import("std");
const engine = @import("antfly_inference_engine");
const inference = @import("inference_server");

// This calls the production host function table and engine_exports.createNode,
// rather than configuring the guard through a standalone executable helper.
test "CUDA production engine applies shared cap before model loading and restores multiple node ownership" {
    if (!@hasDecl(inference.cuda_memory, "Context")) return error.SkipZigTest;
    const gpu = inference.cuda_memory;
    const Mock = struct {
        fn current(_: gpu.driver.CUcontext) callconv(.c) gpu.driver.CUresult {
            return 0;
        }
        fn alloc(ptr: *gpu.driver.CUdeviceptr, _: usize) callconv(.c) gpu.driver.CUresult {
            ptr.* = 4321;
            return 0;
        }
        fn free(_: gpu.driver.CUdeviceptr) callconv(.c) gpu.driver.CUresult {
            return 0;
        }
    };
    const budget = &gpu.budget.process_device_allocations;
    const before = budget.snapshot();
    var first = try engine.Node.init(std.testing.allocator, .{ .generation_budget_overrides = .{ .backend_limit_bytes = before.live + 64 } });
    defer first.deinit();
    try std.testing.expectEqual(before.live + 64, budget.snapshot().limit);
    var ctx: gpu.Context = undefined;
    ctx.ctx = @ptrFromInt(0xface0001);
    ctx.device_allocations = .{};
    ctx.driver.fns.cuCtxSetCurrent = Mock.current;
    ctx.driver.fns.cuMemAlloc = Mock.alloc;
    ctx.driver.fns.cuMemFree = Mock.free;
    var allocation = try gpu.Buffer.alloc(&ctx, 48);
    defer allocation.free(&ctx);
    try std.testing.expectError(error.CudaMemoryLimitExceeded, gpu.Buffer.alloc(&ctx, 32));
    try std.testing.expectError(error.CudaMemoryLimitExceeded, engine.Node.init(std.testing.allocator, .{ .generation_budget_overrides = .{ .backend_limit_bytes = before.live + 32 } }));
    try std.testing.expectEqual(before.live + 64, budget.snapshot().limit);
    allocation.free(&ctx);
    var second = try engine.Node.init(std.testing.allocator, .{ .generation_budget_overrides = .{ .backend_limit_bytes = before.live + 32 } });
    var unbounded = try engine.Node.init(std.testing.allocator, .{});
    unbounded.deinit();
    try std.testing.expectEqual(before.live + 32, budget.snapshot().limit);
    second.deinit();
    try std.testing.expectEqual(before.live + 64, budget.snapshot().limit);
}

test "CUDA production engine physical pressure and failed-free reload qualification" {
    if (!@hasDecl(inference.cuda_memory, "Context")) return error.SkipZigTest;
    if (!inference.platform.env.getenvBool("ANTFLY_QUALIFY_CUDA_DEVICE_BUDGET")) return error.SkipZigTest;
    const gpu = inference.cuda_memory;
    const budget = &gpu.budget.process_device_allocations;
    const before = budget.snapshot();
    const mib = 1024 * 1024;
    var host = try engine.Node.init(std.testing.allocator, .{ .generation_budget_overrides = .{ .backend_limit_bytes = 64 * mib } });
    defer host.deinit();
    var ctx = try gpu.Context.initDefault();
    var ctx_owned = true;
    defer if (ctx_owned) ctx.deinit();
    var allocation = try gpu.Buffer.alloc(&ctx, 48 * mib);
    defer allocation.free(&ctx);
    try allocation.copyFromHost(&ctx, &.{ 1, 2, 3, 4 });
    try ctx.synchronize();
    try std.testing.expectError(error.CudaMemoryLimitExceeded, gpu.Buffer.alloc(&ctx, 32 * mib));
    try std.testing.expectEqual(before.live + 48 * mib, budget.snapshot().live);
    std.debug.print("CUDA_PRESSURE phase=guard-rejected managed_limit={d} managed_live={d} managed_peak={d} rejected_bytes={d}\n", .{ budget.snapshot().limit, budget.snapshot().live, budget.snapshot().peak, 32 * mib });
    try std.testing.io.sleep(std.Io.Duration.fromSeconds(1), .awake);
    // A real driver allocation is retained by the process ledger while the
    // injected free fails. No driver symbol or other process is modified.
    const Fault = struct {
        fn free(_: gpu.driver.CUdeviceptr) callconv(.c) gpu.driver.CUresult {
            return 201;
        }
    };
    ctx.driver.fns.cuMemFree = Fault.free;
    allocation.free(&ctx);
    try std.testing.expectEqual(before.live + 48 * mib, budget.snapshot().live);
    std.debug.print("CUDA_PRESSURE phase=failed-free managed_limit={d} managed_live={d} managed_peak={d}\n", .{ budget.snapshot().limit, budget.snapshot().live, budget.snapshot().peak });
    try std.testing.io.sleep(std.Io.Duration.fromSeconds(1), .awake);
    // An external user releases between managed owners. Quarantine must retain
    // our original primary reference rather than relying on raw handle reuse.
    var external_driver = try gpu.driver.CudaDriver.open();
    defer external_driver.deinit();
    const device = ctx.device;
    var external: gpu.driver.CUcontext = null;
    try external_driver.check(external_driver.fns.cuDevicePrimaryCtxRetain(&external, device));
    std.debug.print("CUDA_PRESSURE phase=teardown managed_live={d}\n", .{budget.snapshot().live});
    ctx.deinit();
    ctx_owned = false;
    try std.testing.expectEqual(before.live + 48 * mib, budget.snapshot().live);
    try external_driver.check(external_driver.fns.cuDevicePrimaryCtxRelease(device));
    var flags: c_uint = 0;
    var active: c_int = 0;
    try external_driver.check(external_driver.fns.cuDevicePrimaryCtxGetState.?(device, &flags, &active));
    try std.testing.expectEqual(@as(c_int, 1), active);
    std.debug.print("CUDA_PRESSURE phase=quarantined managed_limit={d} managed_live={d} managed_peak={d}\n", .{ budget.snapshot().limit, budget.snapshot().live, budget.snapshot().peak });
    try std.testing.io.sleep(std.Io.Duration.fromSeconds(1), .awake);
    var reloaded = try gpu.Context.initDefault();
    defer reloaded.deinit();
    try std.testing.expectEqual(external, reloaded.ctx);
    try std.testing.expectEqual(before.live, budget.snapshot().live);
    std.debug.print("CUDA_PRESSURE phase=confirmed-free managed_live={d}\n", .{budget.snapshot().live});
    var replacement = try gpu.Buffer.alloc(&reloaded, 64 * mib);
    defer replacement.free(&reloaded);
    try replacement.copyFromHost(&reloaded, &.{ 5, 6, 7, 8 });
    try reloaded.synchronize();
    var bytes: [4]u8 = undefined;
    try replacement.copyToHost(&reloaded, &bytes);
    try reloaded.synchronize();
    try std.testing.expectEqualSlices(u8, &.{ 5, 6, 7, 8 }, &bytes);
    std.debug.print("CUDA_PRESSURE phase=reloaded managed_limit={d} managed_live={d} managed_peak={d} rejected_bytes={d} reload=true\n", .{ budget.snapshot().limit, budget.snapshot().live, budget.snapshot().peak, 32 * mib });
    // Allow an independent NVML sampler to observe the retained real buffer.
    try std.testing.io.sleep(std.Io.Duration.fromSeconds(2), .awake);
    std.debug.print("CUDA_PRESSURE phase=complete\n", .{});
    replacement.free(&reloaded);
    try std.testing.expectEqual(before.live, budget.snapshot().live);
}
