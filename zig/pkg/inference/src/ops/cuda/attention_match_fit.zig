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

//! Native context-owned numerical acceleration for attention matching. This
//! prepares private candidates; it does not install caches or authorize serving.
//! The caller holds the context exclusively and keeps it alive until deinit.
const std = @import("std");
const checkpoint = @import("../../runtime/kv/attention_match_checkpoint.zig");
const solver = @import("../../runtime/kv/attention_match.zig");
const Context = @import("context.zig").CudaContext;
const Buffer = @import("buffer.zig").DeviceBuffer;
const driver = @import("driver.zig");
const algebra = @import("attention_match_linalg.zig");
const check = algebra.check;
const ptr = algebra.ptr;
const int = algebra.int;
const Kernel = enum { zero_values, normalize, pick_column, pack_basis, copy_scaled_column, finalize_column, delete_r_column, retriangularize, transpose_probabilities, select_column_cooperative };
const Program = @import("embedded_module.zig").EmbeddedModule(@embedFile("artifacts/attention_match_fit.fatbin"), Kernel);

pub const Fitter = struct {
    context: *Context,
    stream: driver.CUstream,
    owns_stream: bool = false,
    /// Physical capacity remains charged until releaseWorkspace/deinit.
    workspace_limit: usize = 0,
    // Separate stage slots retain the large QR workspace while attention and
    // selection reuse their own tensors. The aggregate cap is unchanged.
    cached_buffers: [3 * 20]Buffer = @splat(.{}),
    workspace_bytes: usize = 0,
    workspace_reuses: usize = 0,
    frame_active: bool = false,
    linalg: algebra.Linalg,
    program: Program,
    max_device_bytes: usize,
    /// Opt-in until real-model latency and reliability qualification completes.
    cooperative_enabled: bool = false,
    cooperative_blocks: usize = 0,
    cooperative_launches: usize = 0,
    cooperative_restarts: usize = 0,

    pub fn init(allocator: std.mem.Allocator, context: *Context, max_device_bytes: usize) !Fitter {
        return initOnStream(allocator, context, context.stream, max_device_bytes);
    }
    /// The caller owns the context exclusively and admits the aggregate budget
    /// before creating workers. Each worker is used by exactly one host thread.
    pub fn initWorker(allocator: std.mem.Allocator, context: *Context, max_device_bytes: usize, workspace_limit: usize) !Fitter {
        try context.makeCurrent();
        if (workspace_limit == 0 or workspace_limit > max_device_bytes) return error.CompactionWorkspaceLimit;
        if (try context.streamCaptureActive()) return error.CompactionDuringGraphCapture;
        var stream: driver.CUstream = null;
        try context.driver.check(context.driver.fns.cuStreamCreate(&stream, 1)); // non-blocking
        errdefer _ = context.driver.fns.cuStreamDestroy(stream);
        var result = try initOnStream(allocator, context, stream, max_device_bytes);
        result.owns_stream = true;
        result.workspace_limit = workspace_limit;
        return result;
    }
    fn initOnStream(allocator: std.mem.Allocator, context: *Context, stream: driver.CUstream, max_device_bytes: usize) !Fitter {
        try context.makeCurrent();
        if (try context.streamCaptureActive()) return error.CompactionDuringGraphCapture;
        if (max_device_bytes < 4 * 1024 * 1024 or context.device_allocations.snapshot().live > max_device_bytes - (4 * 1024 * 1024)) return error.CudaMemoryLimitExceeded;
        var linalg = try algebra.Linalg.initOnStream(context, stream);
        errdefer linalg.deinit();
        _ = allocator;
        var program: Program = .{};
        try program.initCurrent(&context.driver);
        errdefer program.deinitCurrent(&context.driver) catch {};
        var blocks: usize = 0;
        const fns = context.driver.fns;
        if (fns.cuLaunchCooperativeKernel != null and fns.cuOccupancyMaxActiveBlocksPerMultiprocessor != null and fns.cuDeviceGetAttribute != null) {
            var supported: c_int = 0;
            try context.driver.check(fns.cuDeviceGetAttribute.?(&supported, 95, context.device));
            if (supported != 0) {
                var multiprocessors: c_int = 0;
                var occupancy: c_int = 0;
                try context.driver.check(fns.cuDeviceGetAttribute.?(&multiprocessors, 16, context.device));
                try context.driver.check(fns.cuOccupancyMaxActiveBlocksPerMultiprocessor.?(&occupancy, program.function(.select_column_cooperative), 128, 0));
                if (multiprocessors > 0 and occupancy > 0) blocks = @min(64, @as(usize, @intCast(multiprocessors)) * @as(usize, @intCast(occupancy)));
            }
        }
        return .{ .context = context, .stream = stream, .linalg = linalg, .program = program, .max_device_bytes = max_device_bytes, .cooperative_blocks = blocks };
    }
    pub fn deinit(self: *Fitter) void {
        self.context.makeCurrent() catch {};
        self.synchronize() catch {};
        self.releaseWorkspace();
        self.program.deinitCurrent(&self.context.driver) catch return;
        self.linalg.deinit();
        if (self.owns_stream) _ = self.context.driver.fns.cuStreamDestroy(self.stream);
        self.* = undefined;
    }
    fn synchronize(self: *Fitter) !void {
        try self.context.makeCurrent();
        try self.context.driver.check(self.context.driver.fns.cuStreamSynchronize(self.stream));
    }
    pub fn releaseWorkspace(self: *Fitter) void {
        std.debug.assert(!self.frame_active);
        self.synchronize() catch {};
        for (&self.cached_buffers) |*buffer| buffer.free(self.context);
        self.workspace_bytes = 0;
    }
    pub fn backend(self: *Fitter) solver.Backend {
        return backendForContext(self);
    }
    fn backendForContext(context: ?*anyopaque) solver.Backend {
        return .{ .context = context, .validation_batch = .rows_256, .workspace_fn = workspace, .attend_fn = attend, .summary_fn = summary, .values_fn = values, .select_fn = select };
    }
    /// Pure sizing only: no CUDA context, module, stream or allocation. The
    /// descriptor stays private because numerical callbacks require a Fitter.
    pub fn headHostWorkspace(
        original_rows: usize,
        target_rows: usize,
        block_rows: usize,
        train_elements: usize,
        heldout_elements: usize,
        dim: usize,
        options: @import("../../runtime/kv/attention_match_engine.zig").Options,
    ) !@import("../../runtime/kv/attention_match_engine.zig").HeadWorkspace {
        var planned = options;
        planned.fit.backend = backendForContext(null);
        return @import("../../runtime/kv/attention_match_engine.zig").headWorkspaceBytes(
            original_rows,
            target_rows,
            block_rows,
            train_elements,
            heldout_elements,
            dim,
            planned,
        );
    }
    fn enter(self: *Fitter, control: solver.Control) !void {
        try control.check();
        try self.context.makeCurrent();
        if (try self.context.streamCaptureActive()) return error.CompactionDuringGraphCapture;
        if (self.owns_stream) if (self.context.driver.fns.cuStreamIsCapturing) |query| {
            var status: driver.CUstreamCaptureStatus = driver.CU_STREAM_CAPTURE_STATUS_NONE;
            try self.context.driver.check(query(self.stream, &status));
            if (status != driver.CU_STREAM_CAPTURE_STATUS_NONE) return error.CompactionDuringGraphCapture;
        };
    }
    fn launch(self: *Fitter, kernel: Kernel, blocks: usize, threads: usize, args: anytype) !void {
        return self.launchWith(kernel, blocks, threads, args, false);
    }
    fn launchWith(self: *Fitter, kernel: Kernel, blocks: usize, threads: usize, args: anytype, cooperative: bool) !void {
        const names = @typeInfo(@TypeOf(args)).@"struct".field_names;
        const Types = comptime blk: {
            var types: [names.len]type = undefined;
            for (names, 0..) |name, i| types[i] = @FieldType(@TypeOf(args), name);
            break :blk types;
        };
        var values_copy: @Tuple(&Types) = undefined;
        inline for (0..names.len) |i| values_copy[i] = args[i];
        var params: [@typeInfo(@TypeOf(args)).@"struct".field_names.len]?*anyopaque = undefined;
        inline for (&params, 0..) |*param, i| param.* = @ptrCast(&values_copy[i]);
        if (cooperative) {
            if (blocks == 0 or blocks > self.cooperative_blocks) return error.UnsupportedCooperativeFit;
            const launch_fn = self.context.driver.fns.cuLaunchCooperativeKernel orelse return error.UnsupportedCooperativeFit;
            try self.context.driver.check(launch_fn(self.program.function(kernel), @intCast(blocks), 1, 1, @intCast(threads), 1, 1, 0, self.stream, &params));
            self.cooperative_launches += 1;
        } else {
            try self.context.driver.check(self.context.driver.fns.cuLaunchKernel(self.program.function(kernel), @intCast(blocks), 1, 1, @intCast(threads), 1, 1, 0, self.stream, &params, null));
        }
    }
    fn upload(self: *Fitter, device: usize, bytes: []const u8) !void {
        try self.context.driver.check(self.context.driver.fns.cuMemcpyHtoDAsync(device, bytes.ptr, bytes.len, self.stream));
        // Pageable input ownership is retained until the stream has consumed it.
        try self.synchronize();
    }
    fn download(self: *Fitter, bytes: []u8, device: usize) !void {
        try self.context.driver.check(self.context.driver.fns.cuMemcpyDtoHAsync(bytes.ptr, device, bytes.len, self.stream));
        try self.synchronize();
    }
    fn copy(self: *Fitter, dst: usize, src: usize, bytes: usize) !void {
        try self.context.driver.check(self.context.driver.fns.cuMemcpyDtoDAsync(dst, src, bytes, self.stream));
    }
    fn from(raw: ?*anyopaque) *Fitter {
        return @ptrCast(@alignCast(raw.?));
    }
    fn workspace(_: ?*anyopaque, n: usize, m: usize, q: usize, d: usize) !usize {
        // Includes padded QR input/RHS and all host selection/fallback arrays.
        // Device scratch is admitted independently against the live context.
        const rows = std.math.add(usize, q, m) catch return error.CompactionWorkspaceLimit;
        const width = std.math.add(usize, m, d) catch return error.CompactionWorkspaceLimit;
        const qr = std.math.mul(usize, rows, width) catch return error.CompactionWorkspaceLimit;
        const basis = std.math.mul(usize, q, m) catch return error.CompactionWorkspaceLimit;
        var count = std.math.add(usize, qr, basis) catch return error.CompactionWorkspaceLimit;
        count = std.math.add(usize, count, std.math.mul(usize, std.math.add(usize, n, rows) catch return error.CompactionWorkspaceLimit, 16) catch return error.CompactionWorkspaceLimit) catch return error.CompactionWorkspaceLimit;
        return std.math.mul(usize, count, 8) catch error.CompactionWorkspaceLimit;
    }
    fn attend(raw: ?*anyopaque, _: std.mem.Allocator, head: solver.Head, queries: []const f64, probabilities: []f64, targets: ?[]f64, masses: ?[]f64, control: solver.Control) !void {
        return attendImpl(raw, head, queries, probabilities, targets, masses, control);
    }
    fn summary(raw: ?*anyopaque, _: std.mem.Allocator, head: solver.Head, queries: []const f64, targets: []f64, masses: []f64, control: solver.Control) !void {
        return attendImpl(raw, head, queries, null, targets, masses, control);
    }
    fn attendImpl(raw: ?*anyopaque, head: solver.Head, queries: []const f64, probabilities: ?[]f64, targets: ?[]f64, masses: ?[]f64, control: solver.Control) !void {
        const self = from(raw);
        try self.enter(control);
        const n = head.rows;
        const q = queries.len / head.dim;
        const d = head.dim;
        if (n == 0 or n > checkpoint.max_context_tokens or q == 0 or q > 8192 or d == 0 or d > 1024) return error.InvalidAttentionShape;
        var frame: Frame = .{ .fitter = self, .stage = .attention };
        defer frame.deinit();
        const k = try frame.alloc(n * d * 8);
        const query = try frame.alloc(q * d * 8);
        const p = try frame.alloc(n * q * 8);
        const mass = try frame.alloc(q * 8);
        try self.upload(k, std.mem.sliceAsBytes(head.keys));
        try self.upload(query, std.mem.sliceAsBytes(queries));
        var bias: usize = 0;
        if (head.bias) |b| {
            bias = try frame.alloc(n * 8);
            try self.upload(bias, std.mem.sliceAsBytes(b));
        }
        const zero: f64 = 0;
        const one: f64 = 1;
        const l = &self.linalg;
        try check(l.blas.gemm(l.bh, 1, 0, int(n), int(q), int(d), &head.scale, ptr(k), int(d), ptr(query), int(d), &zero, ptr(p), int(n)));
        try self.launch(.normalize, q, 128, .{ p, bias, int(n), head.softcap, mass });
        if (targets) |out| {
            const v = try frame.alloc(n * d * 8);
            const y = try frame.alloc(q * d * 8);
            try self.upload(v, std.mem.sliceAsBytes(head.values));
            try check(l.blas.gemm(l.bh, 0, 0, int(d), int(q), int(n), &one, ptr(v), int(d), ptr(p), int(n), &zero, ptr(y), int(d)));
            try self.download(std.mem.sliceAsBytes(out), y);
        }
        if (probabilities) |out| try self.download(std.mem.sliceAsBytes(out), p);
        if (masses) |out| try self.download(std.mem.sliceAsBytes(out), mass);
        try control.check();
    }
    fn values(raw: ?*anyopaque, allocator: std.mem.Allocator, a: []const f64, y: []const f64, q: usize, m: usize, d: usize, ridge: f64, control: solver.Control, out: []f64) !void {
        const self = from(raw);
        try self.enter(control);
        if (q == 0 or q > 8192 or m == 0 or m > 2048 or d == 0 or d > 1024 or a.len != q * m or y.len != q * d or out.len != m * d or !std.math.isFinite(ridge) or ridge < 0) return error.InvalidAttentionShape;
        const rows = q + m;
        const ha = try allocator.alloc(f64, rows * m);
        defer allocator.free(ha);
        const hy = try allocator.alloc(f64, rows * d);
        defer allocator.free(hy);
        @memset(ha, 0);
        @memset(hy, 0);
        for (0..m) |j| {
            for (0..q) |i| ha[j * rows + i] = a[i * m + j];
            ha[j * rows + q + j] = @sqrt(ridge);
        }
        for (0..d) |j| for (0..q) |i| {
            hy[j * rows + i] = y[i * d + j];
        };
        var frame: Frame = .{ .fitter = self, .stage = .values };
        defer frame.deinit();
        const da = try frame.alloc(ha.len * 8);
        const dy = try frame.alloc(hy.len * 8);
        const tau = try frame.alloc(m * 8);
        const info = try frame.alloc(@sizeOf(c_int));
        try self.upload(da, std.mem.sliceAsBytes(ha));
        try self.upload(dy, std.mem.sliceAsBytes(hy));
        const l = &self.linalg;
        var w1: c_int = 0;
        var w2: c_int = 0;
        try check(l.solver.qrSize(l.sh, int(rows), int(m), ptr(da), int(rows), &w1));
        try check(l.solver.applySize(l.sh, 0, 1, int(rows), int(d), int(m), ptr(da), int(rows), ptr(tau), ptr(dy), int(rows), &w2));
        if (w1 <= 0 or w2 <= 0) return error.CompactionCudaLinalgFailure;
        const work_count = @max(w1, w2);
        const work = try frame.alloc(@as(usize, @intCast(work_count)) * 8);
        try control.check();
        try check(l.solver.qr(l.sh, int(rows), int(m), ptr(da), int(rows), ptr(tau), ptr(work), work_count, @ptrFromInt(info)));
        var result: c_int = 0;
        try self.download(std.mem.asBytes(&result), info);
        if (result != 0) return error.CompactionCudaLinalgFailure;
        try control.check();
        try check(l.solver.apply(l.sh, 0, 1, int(rows), int(d), int(m), ptr(da), int(rows), ptr(tau), ptr(dy), int(rows), ptr(work), work_count, @ptrFromInt(info)));
        try self.download(std.mem.asBytes(&result), info);
        if (result != 0) return error.CompactionCudaLinalgFailure;
        const one: f64 = 1;
        try check(l.blas.trsm(l.bh, 0, 1, 0, 0, int(m), int(d), &one, ptr(da), int(rows), ptr(dy), int(rows)));
        try self.download(std.mem.sliceAsBytes(hy), dy);
        for (0..d) |j| for (0..m) |i| {
            const value = hy[j * rows + i];
            if (!std.math.isFinite(value)) return error.NonFiniteAttention;
            out[i * d + j] = value;
        };
        try control.check();
    }
    fn selectCooperative(self: *Fitter, probabilities: []const f64, importance: []const f64, q: usize, n: usize, indices: []usize, weights: []f64, cfg: solver.Config) !usize {
        const m = indices.len;
        if (q == 0 or q > 8192 or n == 0 or n > 8192 or m == 0 or m > n or m > 2048 or weights.len != m or probabilities.len != q * n or importance.len != n or cfg.max_sweeps == 0 or !std.math.isFinite(cfg.nnls_tolerance) or cfg.nnls_tolerance <= 0) return error.InvalidAttentionShape;
        for (importance) |norm| if (!std.math.isFinite(norm) or norm < 0) return error.NonFiniteAttention;
        var frame: Frame = .{ .fitter = self, .stage = .selection };
        defer frame.deinit();
        const probabilities_device = try frame.alloc(q * n * 8);
        const columns = try frame.alloc(q * n * 8);
        const norms = try frame.alloc(n * 8);
        const selected_indices = try frame.alloc(m * 8);
        const selected_weights = try frame.alloc(m * 8);
        const residual = try frame.alloc(q * 8);
        const scores = try frame.alloc(n * 8);
        const orth = try frame.alloc(q * m * 8);
        const r = try frame.alloc(m * m * 8);
        const rtmp = try frame.alloc(m * m * 8);
        const projected = try frame.alloc(m * 8);
        const dots = try frame.alloc(m * 8);
        const trial = try frame.alloc(m * 8);
        const proposal = try frame.alloc(m * 8);
        const active = try frame.alloc(m * 4);
        const used = try frame.alloc(n * 4);
        const flags = try frame.alloc(m * 4);
        const state = try frame.alloc(48);
        try self.upload(probabilities_device, std.mem.sliceAsBytes(probabilities));
        try self.upload(norms, std.mem.sliceAsBytes(importance));
        try self.launch(.transpose_probabilities, (q * n + 127) / 128, 128, .{ probabilities_device, columns, int(q), int(n) });
        const tolerance = cfg.nnls_tolerance * @sqrt(@as(f64, @floatFromInt(q)));
        const maximum = int(@min(@as(usize, 128), cfg.max_sweeps));
        for (0..m) |column| {
            try cfg.control.check();
            try self.launchWith(.select_column_cooperative, self.cooperative_blocks, 128, .{ columns, norms, selected_indices, selected_weights, residual, scores, orth, r, rtmp, projected, dots, trial, proposal, active, used, flags, state, int(q), int(n), int(m), int(column), tolerance, maximum }, true);
            var status: c_int = 0;
            try self.download(std.mem.asBytes(&status), state);
            switch (status) {
                0 => {},
                7 => return error.CooperativeRankDeficient,
                8 => return error.MassFitDidNotConverge,
                else => return error.NonFiniteAttention,
            }
        }
        try self.download(std.mem.sliceAsBytes(indices), selected_indices);
        try self.download(std.mem.sliceAsBytes(weights), selected_weights);
        try cfg.control.check();
        return 0;
    }

    fn select(raw: ?*anyopaque, allocator: std.mem.Allocator, probabilities: []const f64, importance: []const f64, q: usize, n: usize, indices: []usize, weights: []f64, cfg: solver.Config) !usize {
        const self = from(raw);
        try self.enter(cfg.control);
        if (self.cooperative_enabled and self.cooperative_blocks > 0) {
            return self.selectCooperative(probabilities, importance, q, n, indices, weights, cfg) catch |err| switch (err) {
                // Restart from the immutable input using the existing native
                // factorization fallback; never accept an incomplete solution.
                error.CooperativeRankDeficient => blk: {
                    self.cooperative_restarts += 1;
                    break :blk selectReference(raw, allocator, probabilities, importance, q, n, indices, weights, cfg);
                },
                else => err,
            };
        }
        return selectReference(raw, allocator, probabilities, importance, q, n, indices, weights, cfg);
    }
    fn selectReference(raw: ?*anyopaque, allocator: std.mem.Allocator, probabilities: []const f64, importance: []const f64, q: usize, n: usize, indices: []usize, weights: []f64, cfg: solver.Config) !usize {
        const self = from(raw);
        try self.enter(cfg.control);
        const m = indices.len;
        if (q == 0 or q > 8192 or n == 0 or n > 8192 or m == 0 or m > n or m > 2048 or weights.len != m or probabilities.len != q * n or importance.len != n) return error.InvalidAttentionShape;
        // One host allocation avoids fixed-buffer alignment gaps and is released
        // before the next chunk. Its bound is included in workspace().
        const host = try allocator.alloc(f64, q * m + 2 * q + n + 5 * m);
        defer allocator.free(host);
        var remaining = host;
        const host_basis = take(&remaining, q * m);
        const host_residual = take(&remaining, q);
        const host_ones = take(&remaining, q);
        const scores = take(&remaining, n);
        const norms = take(&remaining, m);
        const proposal = take(&remaining, m);
        const solution = take(&remaining, m);
        const gradient = take(&remaining, m);
        const work_norms = take(&remaining, m);
        const active = try allocator.alloc(usize, m);
        defer allocator.free(active);
        @memset(host_ones, 1);
        @memset(weights, 0);
        @memset(norms, 0);
        var frame: Frame = .{ .fitter = self, .stage = .selection };
        defer frame.deinit();
        var state: Selection = .{
            .fitter = self,
            .allocator = allocator,
            .cfg = cfg,
            .q = q,
            .n = n,
            .m = m,
            .dp = try frame.alloc(q * n * 8),
            .basis = try frame.alloc(q * m * 8),
            .orth = try frame.alloc(q * m * 8),
            .r = try frame.alloc(m * m * 8),
            .rtmp = try frame.alloc(m * m * 8),
            .projected = try frame.alloc(m * 8),
            .dots = try frame.alloc(m * 8),
            .residual = try frame.alloc(q * 8),
            .trial = try frame.alloc(m * 8),
            .correlation = try frame.alloc(n * 8),
            .ones = try frame.alloc(q * 8),
            .packed_basis = try frame.alloc(q * m * 8),
            .column_status = try frame.alloc(@sizeOf(c_int)),
            .active = active,
            .norms = norms,
            .weights = weights,
            .host_basis = host_basis,
            .host_residual = host_residual,
            .work_norms = work_norms,
        };
        try self.launch(.zero_values, (m * m + 127) / 128, 128, .{ state.r, int(m * m) });
        try self.launch(.zero_values, (m * m + 127) / 128, 128, .{ state.rtmp, int(m * m) });
        try self.upload(state.dp, std.mem.sliceAsBytes(probabilities));
        try self.upload(state.ones, std.mem.sliceAsBytes(host_ones));
        try self.copy(state.residual, state.ones, q * 8);
        const tolerance = cfg.nnls_tolerance * @sqrt(@as(f64, @floatFromInt(q)));
        for (0..m) |selected| {
            try cfg.control.check();
            try self.linalg.gemv(0, n, q, 1, state.dp, n, state.residual, 0, state.correlation);
            try self.download(std.mem.sliceAsBytes(scores), state.correlation);
            var best: ?usize = null;
            var best_score: f64 = -std.math.inf(f64);
            for (scores, importance, 0..) |score, norm, j| {
                if (!std.math.isFinite(norm) or norm < 0) return error.NonFiniteAttention;
                if (std.mem.indexOfScalar(usize, indices[0..selected], j) != null) continue;
                const normalized = if (norm > 0) score / @sqrt(norm) else 0;
                if (!std.math.isFinite(normalized)) return error.NonFiniteAttention;
                if (best == null or normalized > best_score) {
                    best = j;
                    best_score = normalized;
                }
            }
            const chosen = best orelse return error.InvalidSelectionResult;
            indices[selected] = chosen;
            norms[selected] = @sqrt(importance[chosen]);
            const count = selected + 1;
            try self.launch(.pick_column, (q + 127) / 128, 128, .{ state.dp, int(n), int(q), int(chosen), @as(f64, 1), state.basis + selected * q * 8, state.packed_basis });
            if (!state.factor_valid) {
                try state.fallback(count);
                continue;
            }
            var iterations: usize = 0;
            while (true) {
                try step(&iterations, cfg);
                try self.upload(state.trial, std.mem.sliceAsBytes(weights[0..count]));
                try self.copy(state.residual, state.ones, q * 8);
                try self.linalg.gemv(0, q, count, -1, state.basis, q, state.trial, 1, state.residual);
                try self.linalg.gemv(1, q, count, 1, state.basis, q, state.residual, 0, state.dots);
                try self.download(std.mem.sliceAsBytes(gradient[0..count]), state.dots);
                var violation: f64 = 0;
                var best_gradient = tolerance;
                var entering: ?usize = null;
                for (gradient[0..count], norms[0..count], weights[0..count], 0..) |raw_gradient, norm, weight, j| {
                    if (norm == 0) continue;
                    const g = raw_gradient / norm;
                    if (!std.math.isFinite(g)) return error.NonFiniteAttention;
                    violation = @max(violation, if (weight > 0) @abs(g) else @max(0, g));
                    if (std.mem.indexOfScalar(usize, active[0..state.active_count], j) == null and g > best_gradient) {
                        best_gradient = g;
                        entering = j;
                    }
                }
                if (violation <= tolerance) break;
                if (entering == null or !try state.addActive(entering.?)) {
                    try state.fallback(count);
                    break;
                }
                while (state.active_count > 0) {
                    try step(&iterations, cfg);
                    const ac = state.active_count;
                    try self.copy(state.trial, state.projected, ac * 8);
                    try check(self.linalg.blas.trsv(self.linalg.bh, 1, 0, 0, int(ac), ptr(state.r), int(m), ptr(state.trial), 1));
                    try self.download(std.mem.sliceAsBytes(solution[0..ac]), state.trial);
                    @memset(proposal, 0);
                    var feasible = true;
                    for (active[0..ac], solution[0..ac]) |idx, value| {
                        proposal[idx] = value / norms[idx];
                        if (!std.math.isFinite(proposal[idx])) return error.NonFiniteAttention;
                        if (proposal[idx] <= 0) feasible = false;
                    }
                    if (feasible) {
                        @memcpy(weights[0..count], proposal[0..count]);
                        break;
                    }
                    var alpha: f64 = 1;
                    var drop: ?usize = null;
                    for (active[0..ac]) |idx| if (proposal[idx] <= 0) {
                        const fraction = if (weights[idx] == 0) 0 else weights[idx] / (weights[idx] - proposal[idx]);
                        if (drop == null or fraction < alpha) {
                            alpha = fraction;
                            drop = idx;
                        }
                    };
                    if (drop == null or !std.math.isFinite(alpha) or alpha < 0 or alpha > 1) return error.NonFiniteAttention;
                    for (weights[0..count], proposal[0..count]) |*w, proposed| {
                        w.* += alpha * (proposed - w.*);
                        if (!std.math.isFinite(w.*)) return error.NonFiniteAttention;
                    }
                    var j = ac;
                    while (j > 0) {
                        j -= 1;
                        const idx = active[j];
                        if (weights[idx] <= 0 or idx == drop.?) try state.removeActive(j);
                    }
                }
            }
        }
        try cfg.control.check();
        return state.fallback_solves;
    }
};

fn take(values: *[]f64, count: usize) []f64 {
    const result = values.*[0..count];
    values.* = values.*[count..];
    return result;
}
fn step(iterations: *usize, cfg: solver.Config) !void {
    try cfg.control.check();
    if (iterations.* >= cfg.max_sweeps) return error.MassFitDidNotConverge;
    iterations.* += 1;
}
const Frame = struct {
    fitter: *Fitter,
    buffers: [20]Buffer = @splat(.{}),
    stage: enum { attention, values, selection },
    count: usize = 0,
    entered: bool = false,
    fn alloc(self: *Frame, bytes: usize) !usize {
        if (self.count == self.buffers.len) return error.CompactionWorkspaceLimit;
        const fitter = self.fitter;
        if (!self.entered) {
            if (fitter.frame_active) return error.CompactionWorkspaceInUse;
            fitter.frame_active = true;
            self.entered = true;
        }
        if (fitter.workspace_limit != 0) {
            const index = @as(usize, @backingInt(self.stage)) * self.buffers.len + self.count;
            const slot = &fitter.cached_buffers[index];
            if (slot.len < bytes) {
                var kept = fitter.workspace_bytes - slot.allocation_bytes;
                if (kept > fitter.workspace_limit or bytes > fitter.workspace_limit - kept) {
                    // Other stages and later slots belong only to earlier,
                    // fenced callbacks. Evict their idle capacity before
                    // rejecting; never evict this frame's borrowed buffers.
                    for (&fitter.cached_buffers, 0..) |*idle, i| {
                        if (i == index or (i / self.buffers.len == @backingInt(self.stage) and i < index)) continue;
                        const released = idle.allocation_bytes;
                        idle.free(fitter.context);
                        fitter.workspace_bytes -= released;
                    }
                    kept = fitter.workspace_bytes - slot.allocation_bytes;
                }
                if (kept > fitter.workspace_limit or bytes > fitter.workspace_limit - kept) {
                    std.log.err("attention_match_workspace requested={d} retained={d} limit={d}", .{ bytes, kept, fitter.workspace_limit });
                    return error.CompactionWorkspaceLimit;
                }
                slot.free(fitter.context);
                fitter.workspace_bytes = kept;
                const live = fitter.context.device_allocations.snapshot().live;
                if (live > fitter.max_device_bytes or bytes > fitter.max_device_bytes - live) return error.CudaMemoryLimitExceeded;
                slot.* = try Buffer.alloc(fitter.context, bytes);
                fitter.workspace_bytes += slot.allocation_bytes;
            } else fitter.workspace_reuses += 1;
            self.count += 1;
            return slot.ptr;
        }
        const live = self.fitter.context.device_allocations.snapshot().live;
        if (live > self.fitter.max_device_bytes or bytes > self.fitter.max_device_bytes - live) return error.CudaMemoryLimitExceeded;
        self.buffers[self.count] = try Buffer.alloc(self.fitter.context, bytes);
        const result = self.buffers[self.count].ptr;
        self.count += 1;
        return result;
    }
    fn deinit(self: *Frame) void {
        if (!self.entered) return;
        self.fitter.synchronize() catch {};
        if (self.fitter.workspace_limit == 0) for (self.buffers[0..self.count]) |*buffer| buffer.free(self.fitter.context);
        self.fitter.frame_active = false;
    }
};
const Selection = struct {
    fitter: *Fitter,
    allocator: std.mem.Allocator,
    cfg: solver.Config,
    q: usize,
    n: usize,
    m: usize,
    dp: usize,
    basis: usize,
    orth: usize,
    r: usize,
    rtmp: usize,
    projected: usize,
    dots: usize,
    residual: usize,
    trial: usize,
    correlation: usize,
    ones: usize,
    packed_basis: usize,
    column_status: usize,
    active: []usize,
    active_count: usize = 0,
    norms: []f64,
    weights: []f64,
    host_basis: []f64,
    host_residual: []f64,
    work_norms: []f64,
    factor_valid: bool = true,
    fallback_solves: usize = 0,

    fn addActive(self: *Selection, index: usize) !bool {
        const f = self.fitter;
        const count = self.active_count;
        if (count >= self.q or self.norms[index] <= 0) return false;
        const column = self.orth + count * self.q * 8;
        // First reorthogonalization overwrites R; second pass accumulates it.
        try f.launch(.copy_scaled_column, (self.q + 127) / 128, 128, .{ self.basis + index * self.q * 8, column, int(self.q), 1 / self.norms[index] });
        if (count > 0) for (0..2) |pass| {
            try f.linalg.gemv(1, self.q, count, 1, self.orth, self.q, column, 0, self.dots);
            if (pass == 0) try f.copy(self.r + count * self.m * 8, self.dots, count * 8) else {
                const one: f64 = 1;
                try check(f.linalg.blas.axpy(f.linalg.bh, int(count), &one, ptr(self.dots), 1, ptr(self.r + count * self.m * 8), 1));
            }
            try f.linalg.gemv(0, self.q, count, -1, self.orth, self.q, self.dots, 1, column);
        };
        try f.launch(.finalize_column, 1, 256, .{ column, int(self.q), self.r + (count * self.m + count) * 8, self.projected + count * 8, self.column_status });
        var status: c_int = 0;
        try f.download(std.mem.asBytes(&status), self.column_status);
        if (status != 0) return false;
        self.active[count] = index;
        self.active_count += 1;
        return true;
    }
    fn removeActive(self: *Selection, column: usize) !void {
        const f = self.fitter;
        const count = self.active_count;
        self.weights[self.active[column]] = 0;
        if (count > 1) {
            try f.launch(.delete_r_column, (count * (count - 1) + 127) / 128, 128, .{ self.r, self.rtmp, int(self.m), int(count), int(column) });
            try f.copy(self.r, self.rtmp, self.m * self.m * 8);
            try f.launch(.retriangularize, 1, 128, .{ self.orth, self.r, self.projected, int(self.q), int(self.m), int(count), int(column) });
        }
        std.mem.copyForwards(usize, self.active[column .. count - 1], self.active[column + 1 .. count]);
        self.active_count -= 1;
    }
    fn fallback(self: *Selection, count: usize) !void {
        const f = self.fitter;
        try f.launch(.pack_basis, (self.q * count + 127) / 128, 128, .{ self.basis, self.packed_basis, int(self.q), int(count) });
        try f.download(std.mem.sliceAsBytes(self.host_basis[0 .. self.q * count]), self.packed_basis);
        self.fallback_solves += try solver.fitMassActive(self.allocator, self.host_basis[0 .. self.q * count], self.q, count, self.weights[0..count], self.host_residual, self.work_norms[0..count], self.cfg, true);
        try f.upload(self.residual, std.mem.sliceAsBytes(self.host_residual));
        // Rebuild from the feasible reference solution. If its basis is too
        // ill-conditioned for incremental QR, continue with bounded native NNLS.
        self.active_count = 0;
        self.factor_valid = true;
        for (self.weights[0..count], 0..) |weight, index| if (weight > 0) {
            if (!try self.addActive(index)) {
                self.factor_valid = false;
                break;
            }
        };
    }
};
