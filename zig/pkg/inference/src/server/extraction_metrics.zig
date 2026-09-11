// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Fixed-cardinality extraction metrics. Recording allocates nothing, and all
//! label values are compile-time enums. Traces include owned request teardown;
//! a forced worker exit cannot report a completed in-process trace.
const std = @import("std");
const prometheus = @import("prometheus");
const time = @import("antfly_platform").time;
const wire = @import("../extractors/extraction_v2.zig");
pub const observation = @import("../extractors/extraction_observer.zig");
pub const Stage = observation.Stage;
pub const Transport = enum { http, direct };
pub const Outcome = enum { success, invalid, unsupported, admission, resource_limit, memory_budget, backing_oom, cancelled, timeout, search_exhausted, infeasible, model_error, internal };
const Counter = prometheus.Counter(u64);

/// Static equivalent of the existing dynamically allocated CounterVec. The
/// finite enum creates every series up front and makes record/render races
/// atomic without adding allocator ownership to the server Metrics object.
fn Counts(comptime Label: type) type {
    return struct {
        values: [std.enums.values(Label).len]u64 = @splat(0),
        const Self = @This();
        fn add(self: *Self, label: Label, value: u64) void {
            _ = @atomicRmw(u64, &self.values[@intFromEnum(label)], .Add, value, .monotonic);
        }
        pub fn get(self: *const Self, label: Label) u64 {
            return @atomicLoad(u64, &self.values[@intFromEnum(label)], .monotonic);
        }
        fn write(self: *const Self, writer: *std.Io.Writer, comptime name: []const u8, comptime help: []const u8, comptime label: []const u8) !void {
            try writer.writeAll("# HELP " ++ name ++ " " ++ help ++ "\n# TYPE " ++ name ++ " counter\n");
            for (std.enums.values(Label)) |value| try writer.print(name ++ "{{" ++ label ++ "=\"{s}\"}} {d}\n", .{ @tagName(value), self.get(value) });
        }
    };
}

pub fn outcome(err: anyerror, stage: Stage) Outcome {
    switch (err) {
        error.Cancelled, error.Canceled => return .cancelled,
        error.Timeout => return .timeout,
        error.QueueFull, error.ResourceTemporarilyUnavailable, error.ConcurrencyUnavailable => return .admission,
        error.MemoryBudgetExceeded => return .memory_budget,
        error.OutOfMemory => return .backing_oom,
        error.ResourceLimitExceeded => return .resource_limit,
        else => {},
    }
    if (wire.errorDetails(err)) |details| {
        if (std.mem.eql(u8, details.code, "EXTRACTION_SEARCH_EXHAUSTED")) return .search_exhausted;
        if (std.mem.eql(u8, details.code, "EXTRACTION_CONSTRAINTS_INFEASIBLE")) return .infeasible;
        if (std.mem.eql(u8, details.code, "UNSUPPORTED_EXTRACTION_FEATURE")) return .unsupported;
        if (details.status == 413) return .resource_limit;
        if (details.status == 400) return .invalid;
    }
    return if (stage == .model) .model_error else .internal;
}

pub const Metrics = struct {
    requests: Counts(Transport) = .{},
    outcomes: Counts(Outcome) = .{},
    failure_stages: Counts(Stage) = .{},
    envelope_failures: Counts(Outcome) = .{},
    phase_visits: Counts(Stage) = .{},
    phase_ns: Counts(Stage) = .{},
    solver_results: [3]Counts(observation.Status) = .{ .{}, .{}, .{} },
    solver_exhausted: Counts(observation.Solver) = .{},
    solver_nodes: Counts(observation.Solver) = .{},
    active: prometheus.Gauge(i64) = prometheus.Gauge(i64).init("antfly_inference_extract_v2_active", .{ .help = "Recognized V2 calls including admission and owned teardown" }, .{}),
    duration: prometheus.Histogram(u64, &.{ 1_000_000, 5_000_000, 10_000_000, 50_000_000, 100_000_000, 500_000_000, 1_000_000_000, 5_000_000_000, 30_000_000_000, 120_000_000_000 }) = .init("antfly_inference_extract_v2_duration_ns", .{ .help = "V2 dispatch through owned teardown duration in nanoseconds; excludes HTTP body and version probe" }, .{}),
    parsed_items: Counter = Counter.init("antfly_inference_extract_v2_parsed_items_total", .{ .help = "Items in successfully parsed V2 envelopes" }, .{}),
    input_bytes: Counter = Counter.init("antfly_inference_extract_v2_input_bytes_total", .{ .help = "Joined source bytes in successfully parsed V2 envelopes" }, .{}),
    decoded_items: Counter = Counter.init("antfly_inference_extract_v2_decoded_items_total", .{ .help = "Decoded items including items in subsequently rejected atomic requests" }, .{}),
    returned_items: Counter = Counter.init("antfly_inference_extract_v2_returned_items_total", .{ .help = "Items returned by successful atomic V2 calls" }, .{}),
    decoded_prompt_tokens: Counter = Counter.init("antfly_inference_extract_v2_decoded_prompt_tokens_total", .{ .help = "Prompt tokens for decoded items including overlapping document windows" }, .{}),
    decoded_output_values: Counter = Counter.init("antfly_inference_extract_v2_decoded_output_values_total", .{ .help = "Scalar output values counted by the executor output budget" }, .{}),
    documents_planned: Counter = Counter.init("antfly_inference_extract_v2_documents_planned_total", .{ .help = "Long-document plans before encoded-token admission" }, .{}),
    windows_planned: Counter = Counter.init("antfly_inference_extract_v2_windows_planned_total", .{ .help = "Source windows planned before encoded-token admission" }, .{}),
    windows_completed: Counter = Counter.init("antfly_inference_extract_v2_windows_completed_total", .{ .help = "Windows whose learned outputs and retained evidence were collected" }, .{}),
    host_peak_value: u64 = 0,

    pub fn begin(self: *Metrics, transport: Transport) Trace {
        return self.beginAt(transport, time.monotonicNs());
    }
    fn beginAt(self: *Metrics, transport: Transport, now: u64) Trace {
        self.requests.add(transport, 1);
        self.active.incr();
        return .{ .metrics = self, .started_ns = now, .phase_started_ns = now };
    }
    pub fn envelopeFailure(self: *Metrics, err: anyerror) void {
        // The version is not trustworthy yet, so this series intentionally
        // covers extraction envelope probes of either legacy or V2 requests.
        self.envelope_failures.add(outcome(err, .parsing), 1);
    }
    fn hostPeak(self: *Metrics, bytes: usize) void {
        const next: u64 = @intCast(bytes);
        var old = @atomicLoad(u64, &self.host_peak_value, .monotonic);
        while (next > old) {
            old = @cmpxchgWeak(u64, &self.host_peak_value, old, next, .monotonic, .monotonic) orelse break;
        }
    }
    pub fn render(self: *Metrics, writer: *std.Io.Writer) !void {
        try prometheus.write(self, writer);
        try writer.print("# HELP antfly_inference_extract_v2_host_peak_bytes_max Maximum request-owned capped host allocation peak; excludes model residency and device allocations\n# TYPE antfly_inference_extract_v2_host_peak_bytes_max gauge\nantfly_inference_extract_v2_host_peak_bytes_max {d}\n", .{@atomicLoad(u64, &self.host_peak_value, .monotonic)});
        try self.requests.write(writer, "antfly_inference_extract_v2_requests_total", "Recognized V2 calls before control and admission", "transport");
        try self.outcomes.write(writer, "antfly_inference_extract_v2_outcomes_total", "Completed V2 dispatch outcomes including rejection and cancellation", "outcome");
        try self.failure_stages.write(writer, "antfly_inference_extract_v2_failures_total", "Failed V2 calls by bounded lifecycle stage", "stage");
        try self.envelope_failures.write(writer, "antfly_inference_extract_envelope_failures_total", "Extraction envelope/version failures before reliable version dispatch", "outcome");
        try self.phase_visits.write(writer, "antfly_inference_extract_v2_phase_visits_total", "Completed V2 lifecycle phase visits", "stage");
        try self.phase_ns.write(writer, "antfly_inference_extract_v2_phase_duration_ns_total", "Time in V2 lifecycle phases including failed phases", "stage");
        try self.solver_exhausted.write(writer, "antfly_inference_extract_v2_solver_exhausted_total", "Decoded solver witnesses marked exhausted; strict errors are in outcomes", "solver");
        try self.solver_nodes.write(writer, "antfly_inference_extract_v2_solver_nodes_total", "Visited search nodes reported by decoded solver witnesses", "solver");
        try writer.writeAll("# HELP antfly_inference_extract_v2_solver_results_total Decoded solver witnesses including subsequently rejected atomic requests\n# TYPE antfly_inference_extract_v2_solver_results_total counter\n");
        for (std.enums.values(observation.Solver)) |solver| for (std.enums.values(observation.Status)) |status| {
            try writer.print("antfly_inference_extract_v2_solver_results_total{{solver=\"{s}\",status=\"{s}\"}} {d}\n", .{ @tagName(solver), @tagName(status), self.solver_results[@intFromEnum(solver)].get(status) });
        };
    }
};

pub const Trace = struct {
    metrics: *Metrics,
    started_ns: u64,
    phase_started_ns: u64,
    stage: Stage = .admission,
    decoded: u64 = 0,
    finished: bool = false,

    pub fn observer(self: *Trace) observation.Observer {
        return .{ .context = self, .emit_fn = receive };
    }
    fn receive(raw: ?*anyopaque, event: observation.Event) void {
        const self: *Trace = @ptrCast(@alignCast(raw.?));
        self.observeAt(event, time.monotonicNs());
    }
    fn phaseAt(self: *Trace, stage: Stage, now: u64) void {
        if (self.stage == stage) return;
        self.endPhase(now);
        self.stage = stage;
        self.phase_started_ns = now;
    }
    fn endPhase(self: *Trace, now: u64) void {
        self.metrics.phase_visits.add(self.stage, 1);
        self.metrics.phase_ns.add(self.stage, now -| self.phase_started_ns);
    }
    fn observeAt(self: *Trace, event: observation.Event, now: u64) void {
        if (self.finished) return;
        switch (event) {
            .phase => |stage| self.phaseAt(stage, now),
            .parsed => |parsed| {
                self.metrics.parsed_items.incrBy(@intCast(parsed.items));
                self.metrics.input_bytes.incrBy(@intCast(parsed.input_bytes));
            },
            .sample_decoded => |sample| {
                self.decoded +|= 1;
                self.metrics.decoded_items.incr();
                self.metrics.decoded_prompt_tokens.incrBy(@intCast(sample.prompt_tokens));
                self.metrics.decoded_output_values.incrBy(@intCast(sample.output_values));
                for (sample.solvers, std.enums.values(observation.Solver)) |value, solver| if (value) |diagnostic| {
                    self.metrics.solver_results[@intFromEnum(solver)].add(diagnostic.status, 1);
                    self.metrics.solver_nodes.add(solver, @intCast(diagnostic.visited_nodes));
                    if (diagnostic.exhausted) self.metrics.solver_exhausted.add(solver, 1);
                };
            },
            .document_planned => |windows| {
                self.metrics.documents_planned.incr();
                self.metrics.windows_planned.incrBy(@intCast(windows));
            },
            .window_completed => self.metrics.windows_completed.incr(),
            .host_peak => |bytes| self.metrics.hostPeak(bytes),
        }
    }
    pub fn finish(self: *Trace, failure: ?anyerror) void {
        self.finishAt(failure, time.monotonicNs());
    }
    fn finishAt(self: *Trace, failure: ?anyerror, now: u64) void {
        if (self.finished) return;
        self.finished = true;
        self.endPhase(now);
        self.metrics.active.incrBy(-1);
        self.metrics.duration.observe(now -| self.started_ns);
        if (failure) |err| {
            // The trace's typed phase also distinguishes tokenizing/execution/
            // merging inside long documents, whose wire context is coarser.
            self.metrics.outcomes.add(outcome(err, self.stage), 1);
            self.metrics.failure_stages.add(self.stage, 1);
        } else {
            self.metrics.outcomes.add(.success, 1);
            self.metrics.returned_items.incrBy(self.decoded);
        }
    }
};

test "extraction observability accounts phases through cleanup and atomic success" {
    var metrics = Metrics{};
    var trace = metrics.beginAt(.direct, 10);
    trace.observeAt(.{ .phase = .parsing }, 20);
    trace.observeAt(.{ .parsed = .{ .items = 2, .input_bytes = 8 } }, 25);
    trace.observeAt(.{ .phase = .execution }, 30);
    trace.observeAt(.{ .sample_decoded = .{ .prompt_tokens = 5, .output_values = 1 } }, 40);
    trace.observeAt(.{ .sample_decoded = .{ .prompt_tokens = 3, .output_values = 2 } }, 45);
    trace.observeAt(.{ .phase = .teardown }, 50);
    trace.observeAt(.{ .host_peak = 128 }, 60);
    trace.finishAt(null, 70);
    try std.testing.expectEqual(@as(i64, 0), metrics.active.impl.value);
    try std.testing.expectEqual(@as(u64, 1), metrics.requests.get(.direct));
    try std.testing.expectEqual(@as(u64, 1), metrics.outcomes.get(.success));
    try std.testing.expectEqual(@as(u64, 2), metrics.returned_items.impl.count);
    try std.testing.expectEqual(@as(u64, 20), metrics.phase_ns.get(.teardown));
    var phases: u64 = 0;
    for (std.enums.values(Stage)) |stage| phases += metrics.phase_ns.get(stage);
    try std.testing.expectEqual(@as(u64, 60), phases);
    try std.testing.expectEqual(phases, metrics.duration.impl.sum);
    trace.finishAt(error.Timeout, 80);
    try std.testing.expectEqual(@as(i64, 0), metrics.active.impl.value);
    try std.testing.expectEqual(@as(u64, 0), metrics.outcomes.get(.timeout));
}

test "extraction observability keeps rejection cancellation and partial work distinct" {
    var metrics = Metrics{};
    for ([_]struct { err: anyerror, expected: Outcome }{
        .{ .err = error.QueueFull, .expected = .admission },
        .{ .err = error.Cancelled, .expected = .cancelled },
        .{ .err = error.Timeout, .expected = .timeout },
        .{ .err = error.MemoryBudgetExceeded, .expected = .memory_budget },
        .{ .err = error.OutOfMemory, .expected = .backing_oom },
    }) |case| {
        var trace = metrics.beginAt(.http, 0);
        trace.finishAt(case.err, 2);
        try std.testing.expectEqual(@as(u64, 1), metrics.outcomes.get(case.expected));
    }
    var trace = metrics.beginAt(.direct, 5);
    trace.observeAt(.{ .phase = .windowing }, 6);
    trace.observeAt(.{ .document_planned = 4 }, 7);
    trace.observeAt(.window_completed, 8);
    trace.observeAt(.window_completed, 9);
    trace.observeAt(.{ .sample_decoded = .{ .prompt_tokens = 13, .output_values = 2, .solvers = .{ .{ .status = .feasible, .exhausted = true, .visited_nodes = 7 }, null, null } } }, 10);
    trace.observeAt(.{ .phase = .merging }, 11);
    trace.finishAt(error.LongDocumentRecordSearchExhausted, 12);
    try std.testing.expectEqual(@as(u64, 0), metrics.returned_items.impl.count);
    try std.testing.expectEqual(@as(u64, 1), metrics.decoded_items.impl.count);
    try std.testing.expectEqual(@as(u64, 4), metrics.windows_planned.impl.count);
    try std.testing.expectEqual(@as(u64, 2), metrics.windows_completed.impl.count);
    try std.testing.expectEqual(@as(u64, 1), metrics.solver_exhausted.get(.classification));
    try std.testing.expectEqual(@as(u64, 7), metrics.solver_nodes.get(.classification));
    try std.testing.expectEqual(@as(u64, 1), metrics.outcomes.get(.search_exhausted));
    try std.testing.expectEqual(@as(u64, 1), metrics.failure_stages.get(.merging));
    try std.testing.expectEqual(@as(i64, 0), metrics.active.impl.value);
}

test "extraction observability exposes only fixed labels and optional observer is inert" {
    var metrics = Metrics{};
    var trace = metrics.beginAt(.direct, 0);
    trace.observeAt(.{ .phase = .parsing }, 1);
    try std.testing.expect(Stage.fromFailure("secret-model\"}\nrequest_text=secret") == null);
    trace.finishAt(error.InvalidExtractionSchema, 2);
    observation.emit(null, .{ .host_peak = 9999 });
    try std.testing.expectEqual(@as(u64, 0), metrics.host_peak_value);
    var writer = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer writer.deinit();
    try metrics.render(&writer.writer);
    try std.testing.expect(std.mem.indexOf(u8, writer.written(), "secret") == null);
    try std.testing.expect(std.mem.indexOf(u8, writer.written(), "model=") == null);
    try std.testing.expect(std.mem.indexOf(u8, writer.written(), "schema=") == null);
    try std.testing.expectEqual(@as(u64, 1), metrics.outcomes.get(.invalid));
    try std.testing.expectEqual(@as(u64, 1), metrics.failure_stages.get(.parsing));
}
