// Copyright 2026 Antfly, Inc.
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

const context_mod = @import("context.zig");
const driver_mod = @import("driver.zig");

pub const Error = driver_mod.Error || error{ CudaGraphUnavailable, CudaGraphCaptureFailed };

pub const CapturedGraph = struct {
    graph: driver_mod.CUgraph = null,
    exec: driver_mod.CUgraphExec = null,

    pub fn instantiate(ctx: *context_mod.CudaContext, graph: driver_mod.CUgraph) Error!CapturedGraph {
        const instantiate_with_flags = ctx.driver.fns.cuGraphInstantiateWithFlags orelse return error.CudaGraphUnavailable;
        var exec: driver_mod.CUgraphExec = null;
        try ctx.makeCurrent();
        try ctx.driver.check(instantiate_with_flags(&exec, graph, 0));
        return .{ .graph = graph, .exec = exec };
    }

    pub fn launch(self: *const CapturedGraph, ctx: *context_mod.CudaContext) Error!void {
        const launch_fn = ctx.driver.fns.cuGraphLaunch orelse return error.CudaGraphUnavailable;
        if (self.exec == null) return error.InvalidCudaState;
        try ctx.makeCurrent();
        try ctx.driver.check(launch_fn(self.exec, ctx.stream));
    }

    pub fn deinit(self: *CapturedGraph, ctx: *context_mod.CudaContext) void {
        ctx.makeCurrent() catch {};
        if (self.exec) |exec| {
            if (ctx.driver.fns.cuGraphExecDestroy) |destroy_exec| {
                _ = destroy_exec(exec);
            }
            self.exec = null;
        }
        if (self.graph) |graph| {
            if (ctx.driver.fns.cuGraphDestroy) |destroy_graph| {
                _ = destroy_graph(graph);
            }
            self.graph = null;
        }
    }
};

pub fn available(ctx: *const context_mod.CudaContext) bool {
    return ctx.driver.fns.cuStreamBeginCapture != null and
        ctx.driver.fns.cuStreamEndCapture != null and
        ctx.driver.fns.cuGraphInstantiateWithFlags != null and
        ctx.driver.fns.cuGraphLaunch != null and
        ctx.driver.fns.cuGraphExecDestroy != null and
        ctx.driver.fns.cuGraphDestroy != null and
        ctx.driver.fns.cuMemAllocHost != null and
        ctx.driver.fns.cuMemFreeHost != null;
}

pub fn beginCapture(ctx: *context_mod.CudaContext) Error!void {
    const begin_fn = ctx.driver.fns.cuStreamBeginCapture orelse return error.CudaGraphUnavailable;
    try ctx.makeCurrent();
    try ctx.driver.check(begin_fn(ctx.stream, driver_mod.CU_STREAM_CAPTURE_MODE_RELAXED));
}

pub fn endCapture(ctx: *context_mod.CudaContext) Error!CapturedGraph {
    const end_fn = ctx.driver.fns.cuStreamEndCapture orelse return error.CudaGraphUnavailable;
    var graph: driver_mod.CUgraph = null;
    try ctx.makeCurrent();
    try ctx.driver.check(end_fn(ctx.stream, &graph));
    if (graph == null) return error.CudaGraphCaptureFailed;
    return CapturedGraph.instantiate(ctx, graph);
}
