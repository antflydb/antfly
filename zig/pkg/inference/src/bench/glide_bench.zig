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

// GLiDE backbone parity + throughput benchmark over pre-tokenized prompts.
//
// The fixture is the JSON written by the Python reference capture script
// (prompts[i] = {ids, logits[255], n}). Each prompt runs through the GLiDE
// session (`input_ids`/`attention_mask`/`readout_index` -> `logits`), and the
// first `n` logits/probabilities are compared against the reference.
//
//   --mode single   one prompt per forward (latency, parity)
//   --mode batched  length-sorted right-padded micro-batches under --budget
//                   padded tokens, the reference's packing rule (throughput)

const std = @import("std");
const build_options = @import("build_options");
const inference = @import("inference_internal");
const backends = inference.backends;
const glide = inference.models.glide;
const native_backend_guard = inference.native_backend_guard;
const metal_runtime = inference.metal_runtime;
const Tensor = backends.Tensor;

const BackendChoice = enum { native, metal };
const Mode = enum { single, batched, both };

const Options = struct {
    model_dir: []const u8 = "",
    fixture: []const u8 = "",
    backend: BackendChoice = .metal,
    mode: Mode = .both,
    budget: usize = 16384,
    warmup_iters: usize = 1,
    measure_iters: usize = 3,
    only: ?[]usize = null,
    verbose: bool = false,
};

const Prompt = struct {
    ids: []const i64,
    logits: []const f32,
    n: usize,
};
const Fixture = struct { prompts: []const Prompt };

const Stats = struct {
    max_dlogit: f64 = 0,
    max_dprob: f64 = 0,
    flips: usize = 0,
    count: usize = 0,

    fn add(self: *Stats, got: []const f32, want: []const f32, n: usize, temperature: f64) !void {
        var p_got: [glide.max_options]f64 = undefined;
        var p_want: [glide.max_options]f64 = undefined;
        try glide.optionProbabilities(got, n, temperature, p_got[0..n]);
        try glide.optionProbabilities(want, n, temperature, p_want[0..n]);
        var arg_got: usize = 0;
        var arg_want: usize = 0;
        for (0..n) |i| {
            self.max_dlogit = @max(self.max_dlogit, @abs(@as(f64, got[i]) - @as(f64, want[i])));
            self.max_dprob = @max(self.max_dprob, @abs(p_got[i] - p_want[i]));
            if (p_got[i] > p_got[arg_got]) arg_got = i;
            if (p_want[i] > p_want[arg_want]) arg_want = i;
        }
        if (arg_got != arg_want) self.flips += 1;
        self.count += 1;
    }
};

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.c_allocator;
    const opts = try parseArgs(allocator, init);
    if (opts.model_dir.len == 0 or opts.fixture.len == 0) {
        printUsage();
        return error.InvalidArguments;
    }
    try ensureBackendAvailable(opts.backend);

    const fixture_bytes = try inference.util.c_file.readFileMax(allocator, opts.fixture, 512 * 1024 * 1024);
    defer allocator.free(fixture_bytes);
    const parsed = try std.json.parseFromSlice(Fixture, allocator, fixture_bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    var selected: std.ArrayList(usize) = .empty;
    defer selected.deinit(allocator);
    if (opts.only) |only| {
        for (only) |i| if (i < parsed.value.prompts.len) try selected.append(allocator, i);
    } else for (0..parsed.value.prompts.len) |i| try selected.append(allocator, i);
    const prompts = parsed.value.prompts;

    var decision = try glide.loadDecisionConfig(allocator, opts.model_dir);
    defer decision.deinit(allocator);

    var session_manager = backends.SessionManager.initWithIo(allocator, init.io);
    session_manager.preferred_backends = switch (opts.backend) {
        .native => &.{backends.BackendType.native},
        .metal => &.{backends.BackendType.metal},
    };
    const load_start = nowNs();
    const session = session_manager.loadModel(opts.model_dir) catch |err| {
        std.debug.print("glide_bench: model_load_error={s}\n", .{@errorName(err)});
        return err;
    };
    defer session.close();
    std.debug.print("glide_bench: loaded backend={s} in {d:.2}s prompts={d}\n", .{ @tagName(session.backend()), nsToS(nowNs() - load_start), selected.items.len });

    var total_tokens: usize = 0;
    for (selected.items) |i| total_tokens += prompts[i].ids.len;

    if (opts.mode == .single or opts.mode == .both) {
        var stats: Stats = .{};
        const samples = try allocator.alloc(u64, opts.measure_iters);
        defer allocator.free(samples);
        var sum_median_ns: u64 = 0;
        for (selected.items) |i| {
            const prompt = prompts[i];
            const one = [_]usize{i};
            for (0..opts.warmup_iters) |_| {
                const logits = try runBatch(allocator, session, decision, prompts, &one);
                allocator.free(logits);
            }
            var last: []f32 = &.{};
            defer allocator.free(last);
            for (samples) |*sample| {
                allocator.free(last);
                const start = nowNs();
                last = try runBatch(allocator, session, decision, prompts, &one);
                sample.* = nowNs() - start;
            }
            const median = medianNs(samples);
            sum_median_ns += median;
            var row: Stats = .{};
            try row.add(last, prompt.logits, prompt.n, decision.temperature);
            try stats.add(last, prompt.logits, prompt.n, decision.temperature);
            if (opts.verbose) std.debug.print("  got[0..3]={d:.6},{d:.6},{d:.6} want[0..3]={d:.6},{d:.6},{d:.6}\n", .{ last[0], last[1], last[2], prompt.logits[0], prompt.logits[1], prompt.logits[2] });
            std.debug.print("single prompt={d} len={d} n={d} median_ms={d:.2} tok_s={d:.0} dlogit={d:.5} dprob={d:.6} flip={}\n", .{
                i,              prompt.ids.len,                                          prompt.n,
                nsToMs(median), @as(f64, @floatFromInt(prompt.ids.len)) / nsToS(median), row.max_dlogit,
                row.max_dprob,  row.flips != 0,
            });
        }
        std.debug.print("SINGLE prompts={d} tokens={d} sum_median_ms={d:.1} tok_s={d:.0} max_dlogit={d:.5} max_dprob={d:.6} flips={d}\n", .{
            stats.count,                                                  total_tokens,     nsToMs(sum_median_ns),
            @as(f64, @floatFromInt(total_tokens)) / nsToS(sum_median_ns), stats.max_dlogit, stats.max_dprob,
            stats.flips,
        });
    }

    if (opts.mode == .batched or opts.mode == .both) {
        // Reference packing: length-sorted; extend while (count+1)*len <= budget.
        const order = try allocator.dupe(usize, selected.items);
        defer allocator.free(order);
        std.mem.sort(usize, order, prompts, struct {
            fn lessThan(ps: []const Prompt, a: usize, b: usize) bool {
                return ps[a].ids.len < ps[b].ids.len;
            }
        }.lessThan);
        var batches: std.ArrayList([]const usize) = .empty;
        defer batches.deinit(allocator);
        var start: usize = 0;
        while (start < order.len) {
            var end = start + 1;
            while (end < order.len and opts.budget != 0 and (end - start + 1) * prompts[order[end]].ids.len <= opts.budget) end += 1;
            try batches.append(allocator, order[start..end]);
            start = end;
        }
        var stats: Stats = .{};
        const samples = try allocator.alloc(u64, opts.measure_iters);
        defer allocator.free(samples);
        for (0..opts.warmup_iters) |_| for (batches.items) |batch| {
            const logits = try runBatch(allocator, session, decision, prompts, batch);
            allocator.free(logits);
        };
        for (samples, 0..) |*sample, iter| {
            const t0 = nowNs();
            for (batches.items) |batch| {
                const logits = try runBatch(allocator, session, decision, prompts, batch);
                defer allocator.free(logits);
                if (iter == 0) for (batch, 0..) |i, r| {
                    try stats.add(logits[r * glide.max_options ..][0..glide.max_options], prompts[i].logits, prompts[i].n, decision.temperature);
                };
            }
            sample.* = nowNs() - t0;
        }
        const median = medianNs(samples);
        var padded: usize = 0;
        for (batches.items) |batch| {
            var max_len: usize = 0;
            for (batch) |i| max_len = @max(max_len, prompts[i].ids.len);
            padded += max_len * batch.len;
        }
        std.debug.print("BATCHED prompts={d} batches={d} tokens={d} padded={d} median_ms={d:.1} tok_s={d:.0} q_s={d:.1} max_dlogit={d:.5} max_dprob={d:.6} flips={d}\n", .{
            stats.count,                                          batches.items.len, total_tokens,
            padded,                                               nsToMs(median),    @as(f64, @floatFromInt(total_tokens)) / nsToS(median),
            @as(f64, @floatFromInt(stats.count)) / nsToS(median), stats.max_dlogit,  stats.max_dprob,
            stats.flips,
        });
    }
}

/// Right-pads `rows` prompts and returns their [rows * 255] logits.
fn runBatch(allocator: std.mem.Allocator, session: backends.Session, decision: glide.DecisionConfig, prompts: []const Prompt, rows: []const usize) ![]f32 {
    var seq_len: usize = 0;
    for (rows) |i| seq_len = @max(seq_len, prompts[i].ids.len);
    const batch = rows.len;
    const ids = try allocator.alloc(i64, batch * seq_len);
    defer allocator.free(ids);
    const mask = try allocator.alloc(i64, batch * seq_len);
    defer allocator.free(mask);
    const readout = try allocator.alloc(i64, batch);
    defer allocator.free(readout);
    @memset(mask, 0);
    @memset(ids, 0);
    for (rows, 0..) |i, r| {
        const p = prompts[i].ids;
        @memcpy(ids[r * seq_len ..][0..p.len], p);
        // Pad with the row's last id so pad slots hold a valid token.
        @memset(ids[r * seq_len + p.len ..][0 .. seq_len - p.len], p[p.len - 1]);
        @memset(mask[r * seq_len ..][0..p.len], 1);
        readout[r] = @intCast(try decision.readoutIndex(p.len));
    }
    var inputs = [_]Tensor{
        try Tensor.initInt64(allocator, "input_ids", &.{ @intCast(batch), @intCast(seq_len) }, ids),
        try Tensor.initInt64(allocator, "attention_mask", &.{ @intCast(batch), @intCast(seq_len) }, mask),
        try Tensor.initInt64(allocator, "readout_index", &.{@intCast(batch)}, readout),
    };
    defer for (&inputs) |*t| t.deinit();
    const outputs = try session.run(&inputs, allocator);
    defer {
        for (outputs) |*t| t.deinit();
        allocator.free(outputs);
    }
    if (outputs.len != 1) return error.UnexpectedGlideOutputs;
    const values = outputs[0].asFloat32();
    if (values.len != batch * glide.max_options) return error.UnexpectedGlideOutputs;
    return allocator.dupe(f32, values);
}

fn ensureBackendAvailable(backend: BackendChoice) !void {
    if (backend != .metal) return;
    if (native_backend_guard.checkMetal(build_options.enable_metal, metal_runtime.metalDeviceAvailable())) |failure| {
        native_backend_guard.printFailure(failure);
        return native_backend_guard.raise(failure);
    }
}

fn parseArgs(allocator: std.mem.Allocator, init: std.process.Init) !Options {
    var opts = Options{};
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--model-dir")) {
            opts.model_dir = args.next() orelse return error.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--fixture")) {
            opts.fixture = args.next() orelse return error.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--backend")) {
            opts.backend = std.meta.stringToEnum(BackendChoice, args.next() orelse return error.InvalidArguments) orelse return error.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--mode")) {
            opts.mode = std.meta.stringToEnum(Mode, args.next() orelse return error.InvalidArguments) orelse return error.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--budget")) {
            opts.budget = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArguments, 10);
        } else if (std.mem.eql(u8, arg, "--warmup")) {
            opts.warmup_iters = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArguments, 10);
        } else if (std.mem.eql(u8, arg, "--iters")) {
            opts.measure_iters = @max(1, try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArguments, 10));
        } else if (std.mem.eql(u8, arg, "--only")) {
            var list: std.ArrayList(usize) = .empty;
            var it = std.mem.splitScalar(u8, args.next() orelse return error.InvalidArguments, ',');
            while (it.next()) |part| try list.append(allocator, try std.fmt.parseInt(usize, part, 10));
            opts.only = try list.toOwnedSlice(allocator);
        } else if (std.mem.eql(u8, arg, "--verbose")) {
            opts.verbose = true;
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            std.process.exit(0);
        } else return error.InvalidArguments;
    }
    return opts;
}

fn medianNs(samples: []u64) u64 {
    std.mem.sort(u64, samples, {}, std.sort.asc(u64));
    return samples[samples.len / 2];
}

fn nowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1.0e6;
}

fn nsToS(ns: u64) f64 {
    return @as(f64, @floatFromInt(@max(ns, 1))) / 1.0e9;
}

fn printUsage() void {
    std.debug.print(
        "usage: zig build bench-glide -Doptimize=ReleaseFast -- --model-dir <GLiDE dir> --fixture <reference.json> [--backend metal|native] [--mode single|batched|both] [--budget 16384] [--warmup 1] [--iters 3] [--only 0,3,5]\n",
        .{},
    );
}
