// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Deterministic transaction scenario whose diagnostic replay can feed the
//! existing TraceAntflyTransaction.tla event format without changing the VOPR
//! decision artifact.

const std = @import("std");
const vopr = @import("vopr");
const backend_erased = @import("backend_erased.zig");
const mem_backend = @import("mem_backend.zig");
const transactions = @import("transactions.zig");
const tracing = @import("../tracing/antfly_trace_writer.zig");

const Allocator = std.mem.Allocator;
const txn_id: transactions.TxnId = .{ 0xa1, 0x7f, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 };
const begin_timestamp: u64 = 10_000;
const commit_timestamp: u64 = 10_001;

const begin_id = vopr.id.stable("transition", "storage.transaction.begin");
const write_id = vopr.id.stable("transition", "storage.transaction.write_intent");
const commit_id = vopr.id.stable("transition", "storage.transaction.commit");
const abort_id = vopr.id.stable("transition", "storage.transaction.abort");
const protocol_property_id = vopr.id.stable("property", "storage.transaction.protocol_state_is_valid");
const terminal_property_id = vopr.id.stable("property", "storage.transaction.terminal_decision_reached");

pub const TraceContext = struct {
    writer: *std.Io.Writer,
};

pub const Scenario = struct {
    pub const name: []const u8 = "modeled-transaction";
    pub const version: u32 = 1;
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = protocol_property_id, .name = "storage.transaction.protocol_state_is_valid", .kind = .always },
        .{ .id = terminal_property_id, .name = "storage.transaction.terminal_decision_reached", .kind = .reachable },
    };

    const Phase = enum { begin, write, decide, terminal };
    const State = struct {
        allocator: Allocator,
        backend: mem_backend.Backend,
        runtime_store: backend_erased.Store,
        manager: transactions.TxnManager,
        trace_sink: ?tracing.AntflyNdjsonTraceWriter = null,
        phase: Phase = .begin,
        terminal_status: ?transactions.TxnStatus = null,

        fn deinit(self: *State) void {
            self.manager.deinit();
            self.runtime_store.deinit();
            self.backend.close();
        }
    };

    pub const World = struct { state: *State };

    pub fn init(allocator: Allocator) !World {
        return initWithContext(allocator, null);
    }

    pub fn initWithContext(allocator: Allocator, opaque_context: ?*anyopaque) !World {
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        state.allocator = allocator;
        state.backend = mem_backend.Backend.init(allocator, .{});
        errdefer state.backend.close();
        state.runtime_store = try state.backend.runtimeStore(allocator, .{ .name = "docs" });
        errdefer state.runtime_store.deinit();
        state.manager = try transactions.TxnManager.init(allocator, &state.runtime_store);
        errdefer state.manager.deinit();
        state.trace_sink = null;
        state.phase = .begin;
        state.terminal_status = null;
        if (opaque_context) |context_ptr| {
            const context: *TraceContext = @ptrCast(@alignCast(context_ptr));
            state.trace_sink = .{ .writer = context.writer };
            state.manager.trace_writer = state.trace_sink.?.traceWriter();
        } else {
            // VOPR controls formal tracing explicitly even in a with_tla build.
            state.manager.trace_writer = null;
        }
        state.manager.shard_id = "docs:1";
        return .{ .state = state };
    }

    pub fn deinit(world: *World, allocator: Allocator) void {
        world.state.deinit();
        allocator.destroy(world.state);
        world.* = undefined;
    }

    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: Allocator) !void {
        switch (world.state.phase) {
            .begin => try list.append(allocator, .{ .id = begin_id, .name = "storage.transaction.begin", .kind = .workload }),
            .write => try list.append(allocator, .{ .id = write_id, .name = "storage.transaction.write_intent", .kind = .workload }),
            .decide => {
                try list.append(allocator, .{ .id = commit_id, .name = "storage.transaction.commit", .kind = .workload });
                try list.append(allocator, .{ .id = abort_id, .name = "storage.transaction.abort", .kind = .workload });
            },
            .terminal => {},
        }
    }

    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: Allocator) !vopr.outcome.TransitionOutcome {
        const state = world.state;
        if (selected.id == begin_id and state.phase == .begin) {
            try state.manager.initTransaction(txn_id, begin_timestamp);
            state.phase = .write;
            try events.emitNamed(allocator, .client_response, "storage.transaction.began", begin_timestamp);
            return vopr.outcome.TransitionOutcome.applied();
        }
        if (selected.id == write_id and state.phase == .write) {
            try state.manager.writeIntents(txn_id, &.{.{ .key = "doc:a", .value = "value-a" }}, &.{});
            state.phase = .decide;
            try events.emitNamed(allocator, .client_response, "storage.transaction.intent_written", vopr.id.digest("doc:a"));
            return vopr.outcome.TransitionOutcome.applied();
        }
        if (state.phase == .decide and (selected.id == commit_id or selected.id == abort_id)) {
            const status: transactions.TxnStatus = if (selected.id == commit_id) .committed else .aborted;
            try state.manager.resolveIntents(txn_id, status, commit_timestamp);
            state.terminal_status = status;
            state.phase = .terminal;
            try events.emitNamed(
                allocator,
                .client_response,
                if (status == .committed) "storage.transaction.committed" else "storage.transaction.aborted",
                commit_timestamp,
            );
            return vopr.outcome.TransitionOutcome.targetReached(
                if (status == .committed) "storage.transaction.committed" else "storage.transaction.aborted",
                commit_timestamp,
            );
        }
        return error.InvalidTransactionVoprTransition;
    }

    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: Allocator) !void {
        const state = world.state;
        const persisted_status: i64 = if (state.phase == .begin)
            -1
        else
            @intCast(@intFromEnum(try state.manager.getTransactionStatus(txn_id)));
        try builder.addNamed(allocator, "storage.transaction.phase", @intCast(@intFromEnum(state.phase)));
        try builder.addNamed(allocator, "storage.transaction.persisted_status", persisted_status);
        try builder.addNamed(allocator, "storage.transaction.terminal", @intFromBool(state.phase == .terminal));
    }

    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: Allocator) !void {
        const state = world.state;
        const valid = switch (state.phase) {
            .begin => state.terminal_status == null,
            .write, .decide => state.terminal_status == null and try state.manager.getTransactionStatus(txn_id) == .pending,
            .terminal => state.terminal_status != null and try state.manager.getTransactionStatus(txn_id) == state.terminal_status.?,
        };
        try sink.check(allocator, protocol_property_id, valid);
        try sink.check(allocator, terminal_property_id, state.phase == .terminal);
    }

    pub fn done(world: *World) bool {
        return world.state.phase == .terminal;
    }
};

pub fn record(allocator: Allocator, seed: u64) !vopr.trace.Trace {
    var seeded = vopr.choice.Seeded.init(seed);
    return vopr.runner.run(Scenario, allocator, seeded.source(), .{
        .system = "antfly",
        .seed = seed,
        .transition_budget = 3,
        .source_revision = "transaction-vopr",
        .target = "native",
        .optimize = @tagName(@import("builtin").mode),
    });
}

pub fn replay(allocator: Allocator, artifact: *const vopr.trace.Trace) !vopr.trace.Trace {
    return vopr.replay.exact(Scenario, allocator, artifact);
}

pub fn replayToTransactionTrace(
    allocator: Allocator,
    artifact: *const vopr.trace.Trace,
    writer: *std.Io.Writer,
) !vopr.trace.Trace {
    var context = TraceContext{ .writer = writer };
    return vopr.replay.exactWithContext(Scenario, allocator, artifact, &context);
}

test "transaction VOPR exactly replays and emits a formal sidecar" {
    var artifact = try record(std.testing.allocator, 0xA17F_7A4A);
    defer artifact.deinit();
    try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
    var replayed = try replay(std.testing.allocator, &artifact);
    replayed.deinit();
    var out: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer out.deinit();
    var formal = try replayToTransactionTrace(std.testing.allocator, &artifact, &out.writer);
    formal.deinit();
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "\"tag\":\"antfly-trace\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "\"name\":\"InitTransaction\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "\"name\":\"WriteIntentOnShard\"") != null);
}
