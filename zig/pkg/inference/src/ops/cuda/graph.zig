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
        const exec = try ctx.instantiateGraph(graph);
        return .{ .graph = graph, .exec = exec };
    }

    pub fn launch(self: *const CapturedGraph, ctx: *context_mod.CudaContext) Error!void {
        if (self.exec == null) return error.InvalidCudaState;
        try ctx.launchGraph(self.exec);
    }

    pub fn deinit(self: *CapturedGraph, ctx: *context_mod.CudaContext) void {
        if (self.exec) |exec| {
            ctx.destroyGraphExec(exec);
            self.exec = null;
        }
        if (self.graph) |graph| {
            ctx.destroyGraph(graph);
            self.graph = null;
        }
    }
};

pub fn available(ctx: *const context_mod.CudaContext) bool {
    return ctx.driver.fns.cuMemAllocHost != null and
        ctx.driver.fns.cuMemFreeHost != null;
}

pub fn beginCapture(ctx: *context_mod.CudaContext) Error!void {
    try ctx.beginStreamCapture(driver_mod.CU_STREAM_CAPTURE_MODE_RELAXED);
}

pub fn endCapture(ctx: *context_mod.CudaContext) Error!CapturedGraph {
    const graph = try ctx.endStreamCapture();
    errdefer ctx.destroyGraph(graph);
    return CapturedGraph.instantiate(ctx, graph);
}
