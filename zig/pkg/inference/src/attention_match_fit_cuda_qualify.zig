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

//! Standalone qualification entry; deliberately not linked into serving.
const std = @import("std");
const Context = @import("ops/cuda/context.zig").CudaContext;
const Fitter = @import("ops/cuda/attention_match_fit.zig").Fitter;
const engine = @import("runtime/kv/attention_match_engine.zig");
const checkpoint = @import("runtime/kv/attention_match_checkpoint.zig");
const solver = @import("runtime/kv/attention_match.zig");
pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.page_allocator;
    var context = try Context.initDefault();
    defer context.deinit();
    try context.device_allocations.setLimit(18 * 1024 * 1024 * 1024);
    if (Fitter.init(allocator, &context, 1)) |_| return error.ExpectedMemoryLimit else |err| if (err != error.CudaMemoryLimitExceeded) return err;
    const init_started = std.Io.Timestamp.now(init.io, .awake);
    {
        var fitter = try Fitter.init(allocator, &context, 18 * 1024 * 1024 * 1024);
        defer fitter.deinit();
        std.debug.print("INIT elapsed_ns={d} live_device_bytes={d}\n", .{ init_started.untilNow(init.io, .awake).nanoseconds, context.device_allocations.snapshot().live });
        var args = std.process.Args.Iterator.init(init.minimal.args);
        _ = args.next();
        const mode_arg = args.next();
        if (mode_arg != null and (std.mem.eql(u8, mode_arg.?, "preserve-parallel") or std.mem.eql(u8, mode_arg.?, "verify-parallel"))) {
            try runParallel(init, &context, &args, std.mem.eql(u8, mode_arg.?, "verify-parallel"));
        } else if (mode_arg != null and (std.mem.eql(u8, mode_arg.?, "worker-cooperative-only") or std.mem.eql(u8, mode_arg.?, "worker-full"))) {
            if (args.next() != null) return error.InvalidMode;
            const stats_before = context.stats;
            const live_before = context.device_allocations.snapshot().live;
            {
                var worker = try Fitter.initWorker(allocator, &context, 18 * 1024 * 1024 * 1024, if (std.mem.eql(u8, mode_arg.?, "worker-full")) 512 * 1024 * 1024 else 64 * 1024 * 1024);
                defer worker.deinit();
                if (worker.stream == context.stream or !worker.owns_stream) return error.WorkerStreamAliased;
                worker.cooperative_enabled = true;
                if (std.mem.eql(u8, mode_arg.?, "worker-full")) {
                    try qualifyAttention(allocator, &worker);
                    worker.releaseWorkspace();
                    const h: solver.Head = .{ .keys = &.{ 0, 0, 0, 0 }, .values = &.{ 2, 4, 6, 8 }, .rows = 4, .dim = 1, .scale = 1 };
                    var result = try solver.fit(allocator, h, &.{ 0, 1 }, &.{ -2, 3 }, .{ .target_rows = 2, .selection = .residual_omp, .mass_solver = .active_set, .backend = worker.backend() });
                    defer result.deinit();
                    if (result.metrics.max_relative_mass_error > 1e-6 or result.metrics.relative_output_rmse > 1e-6) return error.NumericalQualificationFailed;
                    worker.releaseWorkspace();
                }
                try qualifySelection(allocator, &context, &worker);
                if (worker.workspace_reuses == 0 or worker.cooperative_launches == 0) return error.MissingCooperativeExecution;
                std.debug.print("PASS worker_full_stages={any} serving_qualified=false workspace_reuses={d} cooperative_launches={d}\n", .{ std.mem.eql(u8, mode_arg.?, "worker-full"), worker.workspace_reuses, worker.cooperative_launches });
            }
            if (context.device_allocations.snapshot().live != live_before) return error.DeviceAllocationLeak;
            if (!std.meta.eql(stats_before, context.stats)) return error.SharedContextStatsMutation;
        } else if (mode_arg != null and (std.mem.eql(u8, mode_arg.?, "selection-only") or std.mem.eql(u8, mode_arg.?, "cooperative-only"))) {
            if (args.next() != null) return error.InvalidMode;
            if (std.mem.eql(u8, mode_arg.?, "selection-only")) try qualifySelection(allocator, &context, &fitter);
            fitter.cooperative_enabled = true;
            if (fitter.cooperative_blocks == 0) return error.UnsupportedCooperativeFit;
            try qualifySelection(allocator, &context, &fitter);
            if (fitter.cooperative_launches == 0) return error.MissingCooperativeExecution;
            std.debug.print("PASS selection_only=true full_fitter_qualified=false cooperative_launches={d} cooperative_restarts={d}\n", .{ fitter.cooperative_launches, fitter.cooperative_restarts });
        } else if (mode_arg) |mode| {
            const preserve = std.mem.eql(u8, mode, "preserve") or std.mem.eql(u8, mode, "preserve-cooperative");
            fitter.cooperative_enabled = std.mem.eql(u8, mode, "preserve-cooperative");
            if (!preserve and !std.mem.eql(u8, mode, "reject")) return error.InvalidMode;
            if (fitter.cooperative_enabled and fitter.cooperative_blocks == 0) return error.UnsupportedCooperativeFit;
            var sources: usize = 0;
            var candidates: usize = 0;
            var preserved: usize = 0;
            var count: usize = 0;
            const started = std.Io.Timestamp.now(init.io, .awake);
            while (args.next()) |path| {
                const prepared = try run(init, path, preserve, fitter.backend());
                sources += prepared.source_bytes;
                candidates += prepared.candidate_bytes;
                preserved += prepared.preserved;
                count += 1;
                if (context.device_allocations.snapshot().live != 4 * 1024 * 1024) return error.DeviceAllocationLeak;
            }
            if (count == 0) return error.MissingFixture;
            std.debug.print("TOTAL numerical_only=true serving_qualified=false prepared={d} preserved={d} source_global_bytes={d} candidate_global_bytes={d} global_savings_ratio={d} elapsed_ns={d} peak_device_bytes={d} cooperative_launches={d} cooperative_restarts={d}\n", .{ count, preserved, sources, candidates, @as(f64, @floatFromInt(sources)) / @as(f64, @floatFromInt(candidates)), started.untilNow(init.io, .awake).nanoseconds, context.device_allocations.snapshot().peak, fitter.cooperative_launches, fitter.cooperative_restarts });
        } else {
            try qualifyAttention(allocator, &fitter);
            try qualifySelection(allocator, &context, &fitter);
            fitter.cooperative_enabled = true;
            if (fitter.cooperative_blocks == 0) return error.UnsupportedCooperativeFit;
            try qualifySelection(allocator, &context, &fitter);
            const h: solver.Head = .{ .keys = &.{ 0, 0, 0, 0 }, .values = &.{ 2, 4, 6, 8 }, .rows = 4, .dim = 1, .scale = 1 };
            var r = try solver.fit(allocator, h, &.{ 0, 1 }, &.{ -2, 3 }, .{ .target_rows = 2, .selection = .residual_omp, .mass_solver = .active_set, .backend = fitter.backend() });
            defer r.deinit();
            if (r.metrics.max_relative_mass_error > 1e-6 or r.metrics.relative_output_rmse > 1e-6) return error.NumericalQualificationFailed;
            std.debug.print("PASS numerical_only=true mass={d} output={d} live_device_bytes={d}\n", .{ r.metrics.max_relative_mass_error, r.metrics.relative_output_rmse, context.device_allocations.snapshot().live });
        }
    }
    if (context.device_allocations.snapshot().live != 0) return error.DeviceAllocationLeak;
}

const Prepared = struct { source_bytes: usize, candidate_bytes: usize, preserved: usize };
fn run(init: std.process.Init, path: []const u8, preserve: bool, backend: solver.Backend) !Prepared {
    const a = std.heap.page_allocator;
    const chunk: usize = 1024;
    const divisor: usize = 4;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, path, a, .limited(160 * 1024 * 1024));
    defer a.free(bytes);
    if (bytes.len % 4 != 0 or bytes.len < 24) return error.InvalidFixture;
    const x = try a.alloc(f32, bytes.len / 4);
    defer a.free(x);
    for (x, 0..) |*v, i| {
        v.* = @bitCast(std.mem.readInt(u32, bytes[i * 4 ..][0..4], .little));
        if (!std.math.isFinite(v.*)) return error.InvalidFixture;
    }
    if (x[0] != 1 or x[1] < 2050 or x[1] > 32768 or x[2] < 1 or x[2] > 512 or x[3] < 1 or x[3] > 4096 or x[4] < 1 or x[4] > 1024) return error.InvalidFixture;
    const n: usize = @intFromFloat(x[1]);
    const d: usize = @intFromFloat(x[2]);
    const qt: usize = @intFromFloat(x[3]);
    const qh: usize = @intFromFloat(x[4]);
    if (x.len != 6 + (2 * n + qt + qh) * d) return error.InvalidFixture;
    const positions = try a.alloc(u64, n);
    defer a.free(positions);
    const ends = try a.alloc(u64, n);
    defer a.free(ends);
    const bias = try a.alloc(f32, n);
    defer a.free(bias);
    @memset(bias, 0);
    const tokens = try a.alloc(u32, n);
    defer a.free(tokens);
    @memset(tokens, 0);
    for (positions, ends, 0..) |*p, *end, i| {
        p.* = i;
        end.* = i + 1;
    }
    const tp = try a.alloc(u64, qt);
    defer a.free(tp);
    const hp = try a.alloc(u64, qh);
    defer a.free(hp);
    for (tp, 0..) |*p, i| p.* = n + i;
    for (hp, 0..) |*p, i| p.* = n + qt + i;
    // Numerical reproducer only: consumer queries remain pooled, while metadata
    // is deliberately synthetic and cannot represent a serving checkpoint.
    const h: checkpoint.Head = .{ .dim = @intCast(d), .positions = positions, .coverage_ends = ends, .bias = bias, .keys = x[6..][0 .. n * d], .values = x[6 + n * d ..][0 .. n * d] };
    const state: checkpoint.State = .{ .identity = .{ .weights = @splat(0), .tokenizer = @splat(0), .template = @splat(0), .layout = @splat(0) }, .logical_length = n, .tokens = tokens, .logits = &.{0}, .layer_donors = &.{0}, .donors = &.{.{ .layer = 0, .heads = &.{h} }} };
    const batches = [_]engine.ReferenceBatch{.{ .consumer_layer = 0, .query_head = 0, .train = x[6 + 2 * n * d ..][0 .. qt * d], .heldout = x[6 + (2 * n + qt) * d ..][0 .. qh * d], .train_positions = tp, .heldout_positions = hp }};
    var stats: engine.Stats = .{};
    const started = std.Io.Timestamp.now(init.io, .awake);
    var result = engine.prepare(a, state, &batches, .{ .query_heads_by_layer = &.{1}, .scales_by_donor = &.{1.0 / @sqrt(@as(f64, @floatFromInt(d)))}, .softcaps_by_donor = &.{0} }, .{ .stats = &stats, .rejected_head_policy = if (preserve) .preserve_original else .reject, .storage_precision = .f16, .retention_divisor = divisor, .prefix_rows = 128, .chunk_rows = chunk, .validation = .whole_head, .max_queries_per_consumer = 4096, .fit = .{ .backend = backend, .target_rows = 1, .selection = .residual_omp, .mass_solver = .active_set } }) catch |err| {
        std.debug.print("REJECT error={s} offset={d} completed={d} validating_head={} mass={d} output={d} elapsed_ns={d}\n", .{ @errorName(err), stats.chunk_start, stats.completed_chunks, stats.validating_head, stats.rounded_fit.max_relative_mass_error, stats.rounded_fit.relative_output_rmse, started.untilNow(init.io, .awake).nanoseconds });
        return err;
    };
    defer result.deinit();
    std.debug.print("PASS numerical_only=true serving_qualified=false preserved={d} rows={d} retained={d} completed={d} validated={d} mass={d} output={d} elapsed_ns={d}\n", .{ stats.preserved_heads, n, result.state.donors[0].heads[0].positions.len, stats.completed_chunks, stats.validated_heads, stats.max_head_mass_error, stats.max_head_output_rmse, started.untilNow(init.io, .awake).nanoseconds });
    return .{ .source_bytes = stats.source_global_bytes, .candidate_bytes = stats.candidate_global_bytes, .preserved = stats.preserved_heads };
}

const Cancel = struct {
    calls: usize = 0,
    fn check(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.calls += 1;
        if (self.calls >= 8) return error.CompactionCancelled;
    }
};
fn qualifySelection(allocator: std.mem.Allocator, context: *Context, fitter: *Fitter) !void {
    const q = 65;
    const n = 96;
    const m = 32;
    var probabilities: [q * n]f64 = undefined;
    var norms: [n]f64 = undefined;
    var indices: [m]usize = undefined;
    var weights: [m]f64 = undefined;
    var basis: [q * m]f64 = undefined;
    var oracle: [m]f64 = undefined;
    var residual: [q]f64 = undefined;
    var work_norms: [m]f64 = undefined;
    const backend = fitter.backend();
    var cfg: solver.Config = .{ .target_rows = m, .selection = .residual_omp, .mass_solver = .active_set, .backend = backend };
    const live = context.device_allocations.snapshot().live;
    for (0..8) |seed| {
        @memset(&norms, 0);
        for (0..q) |i| {
            var sum: f64 = 0;
            for (0..n) |j| {
                var dot: f64 = 0;
                for (0..8) |k| {
                    dot += @sin(@as(f64, @floatFromInt((i + 1) * (k + 3) + seed * 17))) * @sin(@as(f64, @floatFromInt((j + 3) * (k + 1) + seed * 11)));
                }
                probabilities[i * n + j] = @exp(dot / @sqrt(@as(f64, 8)));
                sum += probabilities[i * n + j];
            }
            for (0..n) |j| {
                probabilities[i * n + j] /= sum;
                norms[j] += probabilities[i * n + j] * probabilities[i * n + j];
            }
        }
        _ = try backend.select_fn.?(backend.context, allocator, &probabilities, &norms, q, n, &indices, &weights, cfg);
        for (indices, 0..) |idx, j| {
            if (idx >= n or std.mem.indexOfScalar(usize, indices[0..j], idx) != null) return error.InvalidSelectionResult;
            for (0..q) |i| basis[i * m + j] = probabilities[i * n + idx];
        }
        var reference_cfg = cfg;
        reference_cfg.backend = null;
        _ = try solver.fitMassActive(allocator, &basis, q, m, &oracle, &residual, &work_norms, reference_cfg, false);
        var actual_norm: f64 = 0;
        var oracle_norm: f64 = 0;
        for (0..q) |i| {
            var actual: f64 = 1;
            for (0..m) |j| actual -= basis[i * m + j] * weights[j];
            actual_norm = std.math.hypot(actual_norm, actual);
            oracle_norm = std.math.hypot(oracle_norm, residual[i]);
        }
        var violation: f64 = 0;
        for (0..m) |j| {
            var gradient: f64 = 0;
            var column_norm: f64 = 0;
            for (0..q) |i| {
                var r: f64 = 1;
                for (0..m) |k| r -= basis[i * m + k] * weights[k];
                gradient += basis[i * m + j] * r;
                column_norm = std.math.hypot(column_norm, basis[i * m + j]);
            }
            if (column_norm > 0) violation = @max(violation, (if (weights[j] > 0) @abs(gradient) else @max(0, gradient)) / column_norm);
        }
        std.debug.print("DETAIL seed={d} actual_norm={d} oracle_norm={d} kkt={d}\n", .{ seed, actual_norm, oracle_norm, violation });
        // A smaller GPU objective is valid: the CPU oracle uses the same
        // finite KKT tolerance and may stop earlier on ill-conditioned columns.
        if (actual_norm - oracle_norm > 1e-7 or violation > 1.01 * cfg.nnls_tolerance * @sqrt(@as(f64, q))) return error.NumericalQualificationFailed;
        if (fitter.workspace_limit != 0) {
            const expected_indices = indices;
            const expected_weights = weights;
            _ = try backend.select_fn.?(backend.context, allocator, &probabilities, &norms, q, n, &indices, &weights, cfg);
            if (!std.mem.eql(usize, &expected_indices, &indices) or !std.mem.eql(f64, &expected_weights, &weights)) return error.WorkspaceReuseChangedResult;
        }
        if (fitter.workspace_limit != 0) fitter.releaseWorkspace();
        if (context.device_allocations.snapshot().live != live) return error.DeviceAllocationLeak;
        std.debug.print("SELECT seed={d} objective_gap={d}\n", .{ seed, actual_norm - oracle_norm });
    }
    var cancel: Cancel = .{};
    cfg.control = .{ .context = &cancel, .check_fn = Cancel.check };
    if (backend.select_fn.?(backend.context, allocator, &probabilities, &norms, q, n, &indices, &weights, cfg)) |_| return error.ExpectedCancellation else |err| if (err != error.CompactionCancelled) return err;
    if (fitter.workspace_limit != 0) fitter.releaseWorkspace();
    if (cancel.calls != 8 or context.device_allocations.snapshot().live != live) return error.DeviceAllocationLeak;
    cfg.control = .{};
    cfg.max_sweeps = 1;
    if (backend.select_fn.?(backend.context, allocator, &probabilities, &norms, q, n, &indices, &weights, cfg)) |_| return error.ExpectedWorkLimit else |err| if (err != error.MassFitDidNotConverge) return err;
    if (fitter.workspace_limit != 0) fitter.releaseWorkspace();
    if (context.device_allocations.snapshot().live != live) return error.DeviceAllocationLeak;
    cfg.max_sweeps = 4096;
    const old_limit = context.device_allocations.snapshot().limit;
    try context.device_allocations.setLimit(live + q * n * 8 + q * m * 8 + 128);
    defer context.device_allocations.setLimit(old_limit) catch {};
    if (backend.select_fn.?(backend.context, allocator, &probabilities, &norms, q, n, &indices, &weights, cfg)) |_| return error.ExpectedMemoryLimit else |err| if (err != error.CudaMemoryLimitExceeded) return err;
    if (fitter.workspace_limit != 0) fitter.releaseWorkspace();
    if (context.device_allocations.snapshot().live != live) return error.DeviceAllocationLeak;
    std.debug.print("FAULTS cancellation=true work_limit=true partial_allocation_oom=true live_device_bytes={d}\n", .{live});
}

fn qualifyAttention(allocator: std.mem.Allocator, fitter: *Fitter) !void {
    const d = 64;
    const q = 257;
    var keys: [1025 * d]f64 = undefined;
    var vals: [1025 * d]f64 = undefined;
    var bias: [1025]f64 = undefined;
    var queries: [q * d]f64 = undefined;
    var probabilities: [q * 1025]f64 = undefined;
    var targets: [q * d]f64 = undefined;
    var masses: [q]f64 = undefined;
    var reference_p: [1025]f64 = undefined;
    var reference_y: [d]f64 = undefined;
    for (&keys, &vals, 0..) |*k, *v, i| {
        k.* = @sin(@as(f64, @floatFromInt(i)));
        v.* = @cos(@as(f64, @floatFromInt(i * 13)));
    }
    for (&queries, 0..) |*value, i| value.* = @sin(@as(f64, @floatFromInt(i * 7)));
    for (&bias, 0..) |*value, i| value.* = @as(f64, @floatFromInt(i % 5)) - 2;
    const backend = fitter.backend();
    for ([_]usize{ 7, 31, 129, 1025 }) |n| for ([_]f64{ 0, 2 }) |cap| {
        const h: solver.Head = .{ .keys = keys[0 .. n * d], .values = vals[0 .. n * d], .bias = bias[0..n], .rows = n, .dim = d, .scale = 0.125, .softcap = cap };
        try solver.attendBatch(allocator, h, &queries, probabilities[0 .. q * n], &targets, &masses, .{ .target_rows = 1, .backend = backend });
        for (0..q) |i| {
            const mass = try solver.attend(h, queries[i * d ..][0..d], reference_p[0..n], &reference_y);
            if (@abs(mass - masses[i]) > 1e-11) return error.NumericalQualificationFailed;
            for (reference_p[0..n], probabilities[i * n ..][0..n]) |a, b| if (@abs(a - b) > 1e-11) return error.NumericalQualificationFailed;
            for (reference_y, targets[i * d ..][0..d]) |a, b| if (@abs(a - b) > 1e-11) return error.NumericalQualificationFailed;
        }
        var summary_targets: [q * d]f64 = undefined;
        var summary_masses: [q]f64 = undefined;
        try solver.attendSummary(allocator, h, &queries, &summary_targets, &summary_masses, .{ .target_rows = 1, .backend = backend });
        if (!std.mem.eql(u8, std.mem.sliceAsBytes(&targets), std.mem.sliceAsBytes(&summary_targets)) or
            !std.mem.eql(u8, std.mem.sliceAsBytes(&masses), std.mem.sliceAsBytes(&summary_masses))) return error.NumericalQualificationFailed;
        std.debug.print("ATTEND rows={d} softcap={d} bias_after_softcap=true\n", .{ n, cap });
    };
}

fn runParallel(init: std.process.Init, context: *Context, args: *std.process.Args.Iterator, verify: bool) !void {
    const worker_count = try std.fmt.parseInt(usize, args.next() orelse return error.MissingConcurrency, 10);
    if (worker_count < 2 or worker_count > 4) return error.InvalidConcurrency;
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var heads: [8]checkpoint.Head = undefined;
    var donors: [8]checkpoint.Donor = undefined;
    var layer_donors: [14]u32 = undefined;
    var batches: [14]engine.ReferenceBatch = undefined;
    var layer: usize = 0;
    var logical_length: usize = 0;
    var scales: [8]f64 = undefined;
    for (0..8) |hi| {
        const path = args.next() orelse return error.MissingFixture;
        const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, path, a, .limited(160 * 1024 * 1024));
        if (bytes.len < 24 or bytes.len % 4 != 0) return error.InvalidFixture;
        const x = try a.alloc(f32, bytes.len / 4);
        for (x, 0..) |*value, i| {
            value.* = @bitCast(std.mem.readInt(u32, bytes[i * 4 ..][0..4], .little));
            if (!std.math.isFinite(value.*)) return error.InvalidFixture;
        }
        if (x[0] != 1 or x[1] < 2050 or x[1] > 32768 or x[2] < 1 or x[2] > 512 or x[3] < 1 or x[3] > 4096 or x[4] < 1 or x[4] > 1024) return error.InvalidFixture;
        for (x[1..5]) |value| if (value != @trunc(value)) return error.InvalidFixture;
        const n: usize = @intFromFloat(x[1]);
        const d: usize = @intFromFloat(x[2]);
        const qt: usize = @intFromFloat(x[3]);
        const qh: usize = @intFromFloat(x[4]);
        const consumers: usize = if (hi < 6) 1 else 4;
        if ((hi != 0 and n != logical_length) or qt != consumers * 1024 or qh != consumers * 256 or x.len != 6 + (2 * n + qt + qh) * d) return error.InvalidFixture;
        logical_length = n;
        const positions = try a.alloc(u64, n);
        const ends = try a.alloc(u64, n);
        const bias = try a.alloc(f32, n);
        @memset(bias, 0);
        for (positions, ends, 0..) |*position, *end, i| {
            position.* = i;
            end.* = i + 1;
        }
        heads[hi] = .{ .dim = @intCast(d), .positions = positions, .coverage_ends = ends, .bias = bias, .keys = x[6..][0 .. n * d], .values = x[6 + n * d ..][0 .. n * d] };
        donors[hi] = .{ .layer = @intCast(layer), .heads = heads[hi..][0..1] };
        scales[hi] = 1.0 / @sqrt(@as(f64, @floatFromInt(d)));
        const train_positions = try a.alloc(u64, 1024);
        const heldout_positions = try a.alloc(u64, 256);
        for (train_positions, 0..) |*position, i| position.* = n + i;
        for (heldout_positions, 0..) |*position, i| position.* = n + 1024 + i;
        for (0..consumers) |consumer| {
            layer_donors[layer] = @intCast(hi);
            batches[layer] = .{ .consumer_layer = @intCast(layer), .query_head = 0, .train = x[6 + (2 * n + consumer * 1024) * d ..][0 .. 1024 * d], .heldout = x[6 + (2 * n + qt + consumer * 256) * d ..][0 .. 256 * d], .train_positions = train_positions, .heldout_positions = heldout_positions };
            layer += 1;
        }
    }
    if (args.next() != null) return error.InvalidFixture;
    const tokens = try a.alloc(u32, logical_length);
    @memset(tokens, 0);
    // Pooled fixture queries use synthetic consumer metadata: numerical evidence
    // only, never a portable model checkpoint or public serving qualification.
    const state: checkpoint.State = .{ .identity = .{ .weights = @splat(0), .tokenizer = @splat(0), .template = @splat(0), .layout = @splat(0) }, .logical_length = logical_length, .tokens = tokens, .logits = &.{0}, .layer_donors = &layer_donors, .donors = &donors };
    var workers: [4]Fitter = undefined;
    var initialized: usize = 0;
    defer for (workers[0..initialized]) |*worker| worker.deinit();
    var backends: [4]solver.Backend = undefined;
    const live_before = context.device_allocations.snapshot().live;
    const started = std.Io.Timestamp.now(init.io, .awake);
    for (workers[0..worker_count], backends[0..worker_count]) |*worker, *backend| {
        worker.* = try Fitter.initWorker(std.heap.page_allocator, context, 18 * 1024 * 1024 * 1024, 512 * 1024 * 1024);
        initialized += 1;
        worker.cooperative_enabled = true;
        if (worker.cooperative_blocks == 0) return error.UnsupportedCooperativeFit;
        backend.* = worker.backend();
    }
    var stats: engine.Stats = .{};
    var candidate = engine.prepareParallel(state, &batches, .{ .query_heads_by_layer = &(@as([14]u32, @splat(1))), .scales_by_donor = &scales, .softcaps_by_donor = &(@as([8]f64, @splat(0))) }, .{ .stats = &stats, .rejected_head_policy = .preserve_original, .storage_precision = .f16, .prefix_rows = 128, .chunk_rows = 1024, .validation = .whole_head, .max_queries_per_consumer = 1024, .fit = .{ .target_rows = 1, .selection = .residual_omp, .mass_solver = .active_set } }, backends[0..worker_count]) catch |err| {
        std.debug.print("PARALLEL_REJECT error={s} layer={d} head={d} chunk={d} attempted={d} completed={d}\n", .{ @errorName(err), stats.donor_layer, stats.head, stats.chunk_start, stats.attempted_chunks, stats.completed_chunks });
        for (workers[0..initialized], 0..) |worker, i| std.debug.print("WORKER index={d} cache_bytes={d} launches={d} restarts={d} reuses={d}\n", .{ i, worker.workspace_bytes, worker.cooperative_launches, worker.cooperative_restarts, worker.workspace_reuses });
        return err;
    };
    defer candidate.deinit();
    var launches: usize = 0;
    var restarts: usize = 0;
    var reuses: usize = 0;
    for (workers[0..initialized]) |*worker| {
        launches += worker.cooperative_launches;
        restarts += worker.cooperative_restarts;
        reuses += worker.workspace_reuses;
        worker.deinit();
    }
    initialized = 0;
    if (context.device_allocations.snapshot().live != live_before) return error.DeviceAllocationLeak;
    if (stats.validated_heads != 6 or stats.preserved_heads != 2 or reuses == 0 or launches == 0) return error.NumericalQualificationFailed;
    for (candidate.state.donors, 0..) |donor, hi| {
        const expected = if (hi == 1 or hi == 5) logical_length else 3680;
        if (donor.heads[0].positions.len != expected) return error.NumericalQualificationFailed;
    }
    const fit_elapsed = started.untilNow(init.io, .awake).nanoseconds;
    if (verify) {
        var reference_fitter = try Fitter.init(std.heap.page_allocator, context, 18 * 1024 * 1024 * 1024);
        defer reference_fitter.deinit();
        for ([_]bool{ false, true }) |cooperative| {
            reference_fitter.cooperative_enabled = cooperative;
            var reference = try engine.prepare(std.heap.page_allocator, state, &batches, .{ .query_heads_by_layer = &(@as([14]u32, @splat(1))), .scales_by_donor = &scales, .softcaps_by_donor = &(@as([8]f64, @splat(0))) }, .{ .rejected_head_policy = .preserve_original, .storage_precision = .f16, .prefix_rows = 128, .chunk_rows = 1024, .validation = .whole_head, .max_queries_per_consumer = 1024, .fit = .{ .target_rows = 1, .selection = .residual_omp, .mass_solver = .active_set, .backend = reference_fitter.backend() } });
            defer reference.deinit();
            var differences: usize = 0;
            for (candidate.state.donors, reference.state.donors, 0..) |actual_donor, expected_donor, hi| {
                const actual = actual_donor.heads[0];
                const expected = expected_donor.heads[0];
                if (!std.mem.eql(u64, actual.positions, expected.positions) or !std.mem.eql(u64, actual.coverage_ends, expected.coverage_ends) or actual.compacted_until != expected.compacted_until) return error.ParallelMetadataMismatch;
                inline for (.{ "keys", "values", "bias" }) |field| {
                    const xs = @field(actual, field);
                    const ys = @field(expected, field);
                    if (xs.len != ys.len) return error.ParallelMetadataMismatch;
                    var changed: usize = 0;
                    var maximum: f64 = 0;
                    for (xs, ys) |x, y| {
                        if (@as(u32, @bitCast(x)) != @as(u32, @bitCast(y))) changed += 1;
                        maximum = @max(maximum, @abs(@as(f64, x) - y));
                    }
                    differences += changed;
                    std.debug.print("COMPARE cooperative_reference={d} head={d} field={s} changed={d} max_absolute={d}\n", .{ @intFromBool(cooperative), hi, field, changed, maximum });
                }
            }
            if (cooperative and differences != 0) return error.ParallelNumericalMismatch;
        }
    }
    std.debug.print("TOTAL numerical_only=true serving_qualified=false workers={d} prepared=8 preserved={d} validated={d} source_global_bytes={d} candidate_global_bytes={d} global_savings_ratio={d} elapsed_ns={d} cooperative_launches={d} cooperative_restarts={d} workspace_reuses={d} peak_device_bytes={d}\n", .{ worker_count, stats.preserved_heads, stats.validated_heads, stats.source_global_bytes, stats.candidate_global_bytes, stats.actual_global_savings_ratio, fit_elapsed, launches, restarts, reuses, context.device_allocations.snapshot().peak });
}
