// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

const std = @import("std");
const antfly = @import("antfly");
const vopr = @import("vopr");

const max_trace_bytes = 256 * 1024 * 1024;

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;
    const io = init.io;
    const argv = try init.minimal.args.toSlice(alloc);
    defer alloc.free(argv);
    return dispatch(alloc, io, argv);
}

pub fn dispatch(alloc: std.mem.Allocator, io: std.Io, argv: []const []const u8) !void {
    if (argv.len < 2) return usage();
    if (std.mem.eql(u8, argv[1], "run")) return runCommand(alloc, io, argv[2..]);
    if (std.mem.eql(u8, argv[1], "replay")) return replayCommand(alloc, io, argv[2..]);
    if (std.mem.eql(u8, argv[1], "campaign")) return campaignCommand(alloc, io, argv[2..]);
    if (std.mem.eql(u8, argv[1], "reduce")) return reduceCommand(alloc, io, argv[2..]);
    if (std.mem.eql(u8, argv[1], "promote")) return promoteCommand(alloc, io, argv[2..]);
    if (std.mem.eql(u8, argv[1], "migrate")) return migrateCommand(alloc, io, argv[2..]);
    if (std.mem.eql(u8, argv[1], "tla")) return tlaCommand(alloc, io, argv[2..]);
    if (std.mem.eql(u8, argv[1], "explain")) return explainCommand(alloc, io, argv[2..]);
    return usage();
}

fn runCommand(alloc: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var seed: u64 = 0xa17f_0001;
    var transitions: ?usize = null;
    var scenario: []const u8 = "metadata";
    var workload: antfly.metadata_sim_harness.MetadataVoprWorkload = .smoke;
    var trace_out: ?[]const u8 = null;
    var index: usize = 0;
    while (index < args.len) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--scenario")) {
            scenario = try nextValue(args, &index);
            if (!std.mem.eql(u8, scenario, "metadata") and
                !std.mem.eql(u8, scenario, "transaction") and
                !std.mem.eql(u8, scenario, "distributed-data") and
                !std.mem.eql(u8, scenario, "raft") and
                !std.mem.eql(u8, scenario, "lmdb") and
                !std.mem.eql(u8, scenario, "lsm")) return error.UnsupportedScenario;
        } else if (std.mem.eql(u8, arg, "--seed")) {
            seed = try std.fmt.parseInt(u64, try nextValue(args, &index), 0);
        } else if (std.mem.eql(u8, arg, "--transitions")) {
            transitions = try std.fmt.parseInt(usize, try nextValue(args, &index), 10);
        } else if (std.mem.eql(u8, arg, "--workload")) {
            const value = try nextValue(args, &index);
            workload = if (std.mem.eql(u8, value, "smoke")) .smoke else if (std.mem.eql(u8, value, "expanded")) .expanded else return error.InvalidWorkload;
        } else if (std.mem.eql(u8, arg, "--trace-out")) {
            trace_out = try nextValue(args, &index);
        } else return error.UnknownArgument;
        index += 1;
    }
    const output_path = trace_out orelse return error.TraceOutputRequired;
    const transition_budget: usize = transitions orelse if (std.mem.eql(u8, scenario, "metadata"))
        64
    else if (std.mem.eql(u8, scenario, "distributed-data"))
        4
    else if (std.mem.eql(u8, scenario, "raft"))
        33
    else if (std.mem.eql(u8, scenario, "lmdb"))
        13
    else if (std.mem.eql(u8, scenario, "lsm"))
        49
    else
        3;
    if (transition_budget == 0) return error.InvalidTransitionBudget;
    var artifact = if (std.mem.eql(u8, scenario, "metadata")) blk: {
        const base_id = 10_000 + seed % 1_000_000;
        break :blk try antfly.metadata_sim_harness.recordMetadataVoprCampaign(alloc, .{
            .seed = seed,
            .operation_count = transition_budget,
            .metadata_group_id = base_id,
            .table_id = base_id + 1,
            .range_group_id = base_id + 2,
            .split_group_id = base_id + 3,
            .split_transition_id = base_id + 4,
            .workload = workload,
        });
    } else if (std.mem.eql(u8, scenario, "distributed-data")) blk: {
        if (transition_budget != 4) return error.DistributedDataScenarioRequiresFourTransitions;
        const table_id = 10_000 + seed % 100_000;
        break :blk try antfly.metadata_sim_harness.recordDistributedDataVoprCampaign(alloc, .{
            .seed = seed,
            .table_id = table_id,
        });
    } else if (std.mem.eql(u8, scenario, "raft")) blk: {
        if (transition_budget != 33) return error.RaftScenarioRequiresThirtyThreeTransitions;
        break :blk try antfly.raft_vopr.record(alloc, seed);
    } else if (std.mem.eql(u8, scenario, "lmdb")) blk: {
        if (transition_budget != 13) return error.LmdbScenarioRequiresThirteenTransitions;
        break :blk try antfly.lmdb_vopr.record(alloc, seed);
    } else if (std.mem.eql(u8, scenario, "lsm")) blk: {
        if (transition_budget != 49) return error.LsmScenarioRequiresFortyNineTransitions;
        break :blk try antfly.lsm_vopr.record(alloc, seed);
    } else blk: {
        if (transition_budget != 3) return error.TransactionScenarioRequiresThreeTransitions;
        break :blk try antfly.transaction_vopr.record(alloc, seed);
    };
    defer artifact.deinit();
    const encoded = try artifact.renderAlloc(alloc);
    defer alloc.free(encoded);
    if (std.fs.path.dirname(output_path)) |parent| {
        if (!std.fs.path.isAbsolute(parent)) try std.Io.Dir.cwd().createDirPath(io, parent);
    }
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = output_path, .data = encoded });
    report("recorded", output_path, &artifact);
}

fn replayCommand(alloc: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var trace_path: ?[]const u8 = null;
    var index: usize = 0;
    while (index < args.len) {
        if (std.mem.eql(u8, args[index], "--trace")) {
            trace_path = try nextValue(args, &index);
        } else return error.UnknownArgument;
        index += 1;
    }
    const path = trace_path orelse return error.TracePathRequired;
    const encoded = try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(max_trace_bytes));
    defer alloc.free(encoded);
    var recorded = try vopr.trace.parseAlloc(alloc, encoded);
    defer recorded.deinit();
    var replayed = try replayKnownScenario(alloc, &recorded);
    defer replayed.deinit();
    report("replayed", path, &replayed);
}

fn replayKnownScenario(alloc: std.mem.Allocator, recorded: *const vopr.trace.Trace) !vopr.trace.Trace {
    if (std.mem.eql(u8, recorded.header.scenario, "metadata-vopr"))
        return antfly.metadata_sim_harness.replayMetadataVoprCampaign(alloc, recorded);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.metadata_sim_harness.DistributedDataVoprScenario.name))
        return antfly.metadata_sim_harness.replayDistributedDataVoprCampaign(alloc, recorded);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.transaction_vopr.Scenario.name))
        return antfly.transaction_vopr.replay(alloc, recorded);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.raft_vopr.CliScenario.name))
        return antfly.raft_vopr.replay(alloc, recorded);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.lmdb_vopr.CliScenario.name))
        return antfly.lmdb_vopr.replay(alloc, recorded);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.lsm_vopr.CliScenario.name))
        return antfly.lsm_vopr.replay(alloc, recorded);
    return error.UnsupportedScenario;
}

fn tlaCommand(alloc: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var trace_path: ?[]const u8 = null;
    var output_path: ?[]const u8 = null;
    var domain: []const u8 = "raft";
    var index: usize = 0;
    while (index < args.len) {
        if (std.mem.eql(u8, args[index], "--trace")) {
            trace_path = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--out")) {
            output_path = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--domain")) {
            domain = try nextValue(args, &index);
        } else return error.UnknownArgument;
        index += 1;
    }
    if (!std.mem.eql(u8, domain, "raft") and !std.mem.eql(u8, domain, "transaction")) return error.UnsupportedTlaDomain;
    const input = trace_path orelse return error.TracePathRequired;
    const output = output_path orelse return error.TraceOutputRequired;
    const encoded = try std.Io.Dir.cwd().readFileAlloc(io, input, alloc, .limited(max_trace_bytes));
    defer alloc.free(encoded);
    var recorded = try vopr.trace.parseAlloc(alloc, encoded);
    defer recorded.deinit();

    var ndjson: std.Io.Writer.Allocating = .init(alloc);
    defer ndjson.deinit();
    var replayed = if (std.mem.eql(u8, domain, "raft"))
        try antfly.metadata_sim_harness.replayMetadataVoprCampaignToRaftTrace(alloc, &recorded, &ndjson.writer)
    else
        try antfly.transaction_vopr.replayToTransactionTrace(alloc, &recorded, &ndjson.writer);
    defer replayed.deinit();
    if (std.mem.eql(u8, domain, "raft"))
        try validateRaftTraceNdjson(alloc, ndjson.written())
    else
        try validateTransactionTraceNdjson(alloc, ndjson.written());
    if (std.fs.path.dirname(output)) |parent| try ensureDir(io, parent);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = output, .data = ndjson.written() });
    std.debug.print("VOPR TLA export domain={s} events={d} trace={s} out={s}\n", .{
        domain,
        countNonEmptyLines(ndjson.written()),
        input,
        output,
    });
}

fn validateRaftTraceNdjson(alloc: std.mem.Allocator, encoded: []const u8) !void {
    return validateTlaTraceNdjson(alloc, encoded, "trace");
}

fn validateTransactionTraceNdjson(alloc: std.mem.Allocator, encoded: []const u8) !void {
    return validateTlaTraceNdjson(alloc, encoded, "antfly-trace");
}

fn validateTlaTraceNdjson(alloc: std.mem.Allocator, encoded: []const u8, expected_tag: []const u8) !void {
    var event_count: usize = 0;
    var lines = std.mem.splitScalar(u8, encoded, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, line, .{});
        defer parsed.deinit();
        const object = switch (parsed.value) {
            .object => |value| value,
            else => return error.InvalidTlaTraceRecord,
        };
        const tag = object.get("tag") orelse return error.InvalidTlaTraceRecord;
        const tag_text = switch (tag) {
            .string => |value| value,
            else => return error.InvalidTlaTraceRecord,
        };
        if (!std.mem.eql(u8, tag_text, expected_tag)) return error.InvalidTlaTraceRecord;
        const event_value = object.get("event") orelse return error.InvalidTlaTraceRecord;
        const event_object = switch (event_value) {
            .object => |value| value,
            else => return error.InvalidTlaTraceRecord,
        };
        if (event_object.get("name") == null) return error.InvalidTlaTraceRecord;
        event_count += 1;
    }
    if (event_count == 0) return error.EmptyTlaTrace;
}

fn countNonEmptyLines(encoded: []const u8) usize {
    var count: usize = 0;
    var lines = std.mem.splitScalar(u8, encoded, '\n');
    while (lines.next()) |line| count += @intFromBool(line.len > 0);
    return count;
}

fn explainCommand(alloc: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var trace_path: ?[]const u8 = null;
    var output_path: ?[]const u8 = null;
    var failure_ordinal: usize = 0;
    var index: usize = 0;
    while (index < args.len) {
        if (std.mem.eql(u8, args[index], "--trace")) {
            trace_path = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--out")) {
            output_path = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--failure")) {
            failure_ordinal = try std.fmt.parseInt(usize, try nextValue(args, &index), 10);
        } else return error.UnknownArgument;
        index += 1;
    }
    const input = trace_path orelse return error.TracePathRequired;
    const output = output_path orelse return error.TraceOutputRequired;
    const encoded = try std.Io.Dir.cwd().readFileAlloc(io, input, alloc, .limited(max_trace_bytes));
    defer alloc.free(encoded);
    var recorded = try vopr.trace.parseAlloc(alloc, encoded);
    defer recorded.deinit();
    var replayed = try replayKnownScenario(alloc, &recorded);
    replayed.deinit();
    var causal_report = try vopr.causal.analyzeAlloc(alloc, &recorded, failure_ordinal);
    defer causal_report.deinit();
    const report_bytes = try causal_report.renderAlloc(alloc);
    defer alloc.free(report_bytes);
    if (std.fs.path.dirname(output)) |parent| try ensureDir(io, parent);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = output, .data = report_bytes });
    std.debug.print("VOPR causal report fingerprint={x} causes={d} out={s}\n", .{
        causal_report.failure_fingerprint,
        causal_report.items.len,
        output,
    });
}

fn campaignCommand(alloc: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var histories: u64 = 100;
    var transitions: usize = 64;
    var workers: usize = 1;
    var seed: u64 = 0xa17f_1000;
    var artifact_dir: []const u8 = "/tmp/antfly-sim/metadata";
    var index: usize = 0;
    while (index < args.len) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--scenario")) {
            if (!std.mem.eql(u8, try nextValue(args, &index), "metadata")) return error.UnsupportedScenario;
        } else if (std.mem.eql(u8, arg, "--histories")) {
            histories = try std.fmt.parseInt(u64, try nextValue(args, &index), 10);
        } else if (std.mem.eql(u8, arg, "--transitions")) {
            transitions = try std.fmt.parseInt(usize, try nextValue(args, &index), 10);
        } else if (std.mem.eql(u8, arg, "--workers")) {
            workers = try std.fmt.parseInt(usize, try nextValue(args, &index), 10);
        } else if (std.mem.eql(u8, arg, "--seed")) {
            seed = try std.fmt.parseInt(u64, try nextValue(args, &index), 0);
        } else if (std.mem.eql(u8, arg, "--artifact-dir")) {
            artifact_dir = try nextValue(args, &index);
        } else return error.UnknownArgument;
        index += 1;
    }
    if (histories == 0 or transitions == 0 or workers == 0) return error.InvalidCampaignBudget;
    try ensureDir(io, artifact_dir);

    var context = CampaignContext{
        .io = io,
        .histories = histories,
        .transitions = transitions,
        .base_seed = seed,
        .artifact_dir = artifact_dir,
        .coverage = vopr.coverage.Tracker.init(std.heap.smp_allocator),
        .corpus = vopr.corpus.Corpus.init(std.heap.smp_allocator),
    };
    defer context.deinitReport();
    try context.primeCorpus(alloc);
    defer context.corpus.deinit();
    defer context.coverage.deinit();
    const worker_count = @min(workers, @as(usize, @intCast(histories)));
    context.worker_count = worker_count;
    const threads = try alloc.alloc(std.Thread, worker_count);
    defer alloc.free(threads);
    var spawned: usize = 0;
    errdefer for (threads[0..spawned]) |thread| thread.join();
    for (threads) |*thread| {
        thread.* = try std.Thread.spawn(.{ .stack_size = 8 * 1024 * 1024 }, CampaignContext.worker, .{&context});
        spawned += 1;
    }
    for (threads) |thread| thread.join();
    try context.reportSummary();
    if (context.first_error) |err| return err;
}

fn reduceCommand(alloc: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var trace_path: ?[]const u8 = null;
    var output_path: ?[]const u8 = null;
    var attempts: u64 = 1_000;
    var index: usize = 0;
    while (index < args.len) {
        if (std.mem.eql(u8, args[index], "--trace")) {
            trace_path = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--out")) {
            output_path = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--attempts")) {
            attempts = try std.fmt.parseInt(u64, try nextValue(args, &index), 10);
        } else return error.UnknownArgument;
        index += 1;
    }
    const input = trace_path orelse return error.TracePathRequired;
    const output = output_path orelse return error.TraceOutputRequired;
    const encoded = try std.Io.Dir.cwd().readFileAlloc(io, input, alloc, .limited(max_trace_bytes));
    defer alloc.free(encoded);
    var recorded = try vopr.trace.parseAlloc(alloc, encoded);
    defer recorded.deinit();
    if (std.mem.eql(u8, recorded.header.scenario, "metadata-vopr")) {
        var reduced = try antfly.metadata_sim_harness.reduceMetadataVoprCampaign(alloc, &recorded, attempts);
        defer reduced.deinit();
        return writeReducedArtifact(
            alloc,
            io,
            output,
            &reduced.artifact,
            reduced.original_transitions,
            reduced.reduced_transitions,
            reduced.attempts,
            reduced.target_fingerprint,
        );
    }
    if (std.mem.eql(u8, recorded.header.scenario, antfly.transaction_vopr.Scenario.name)) {
        const target = if (recorded.failures.items.len > 0)
            recorded.failures.items[0].fingerprint
        else
            return error.FailingTraceRequired;
        var reduced = try vopr.reducer.reduce(
            antfly.transaction_vopr.Scenario,
            alloc,
            &recorded,
            target,
            .{ .max_attempts = attempts },
        );
        defer reduced.deinit();
        return writeReducedArtifact(
            alloc,
            io,
            output,
            &reduced.artifact,
            reduced.report.original_transitions,
            reduced.report.reduced_transitions,
            reduced.report.attempts,
            reduced.report.target_fingerprint,
        );
    }
    if (std.mem.eql(u8, recorded.header.scenario, antfly.raft_vopr.CliScenario.name)) {
        const target = if (recorded.failures.items.len > 0)
            recorded.failures.items[0].fingerprint
        else
            return error.FailingTraceRequired;
        var reduced = try vopr.reducer.reduce(
            antfly.raft_vopr.CliScenario,
            alloc,
            &recorded,
            target,
            .{ .max_attempts = attempts },
        );
        defer reduced.deinit();
        return writeReducedArtifact(
            alloc,
            io,
            output,
            &reduced.artifact,
            reduced.report.original_transitions,
            reduced.report.reduced_transitions,
            reduced.report.attempts,
            reduced.report.target_fingerprint,
        );
    }
    if (std.mem.eql(u8, recorded.header.scenario, antfly.lmdb_vopr.CliScenario.name)) {
        const target = if (recorded.failures.items.len > 0)
            recorded.failures.items[0].fingerprint
        else
            return error.FailingTraceRequired;
        var reduced = try vopr.reducer.reduce(
            antfly.lmdb_vopr.CliScenario,
            alloc,
            &recorded,
            target,
            .{ .max_attempts = attempts },
        );
        defer reduced.deinit();
        return writeReducedArtifact(
            alloc,
            io,
            output,
            &reduced.artifact,
            reduced.report.original_transitions,
            reduced.report.reduced_transitions,
            reduced.report.attempts,
            reduced.report.target_fingerprint,
        );
    }
    if (std.mem.eql(u8, recorded.header.scenario, antfly.lsm_vopr.CliScenario.name)) {
        const target = if (recorded.failures.items.len > 0)
            recorded.failures.items[0].fingerprint
        else
            return error.FailingTraceRequired;
        var reduced = try vopr.reducer.reduce(
            antfly.lsm_vopr.CliScenario,
            alloc,
            &recorded,
            target,
            .{ .max_attempts = attempts },
        );
        defer reduced.deinit();
        return writeReducedArtifact(
            alloc,
            io,
            output,
            &reduced.artifact,
            reduced.report.original_transitions,
            reduced.report.reduced_transitions,
            reduced.report.attempts,
            reduced.report.target_fingerprint,
        );
    }
    if (std.mem.eql(u8, recorded.header.scenario, antfly.metadata_sim_harness.DistributedDataVoprScenario.name)) {
        var reduced = try antfly.metadata_sim_harness.reduceDistributedDataVoprCampaign(alloc, &recorded, attempts);
        defer reduced.deinit();
        return writeReducedArtifact(
            alloc,
            io,
            output,
            &reduced.artifact,
            reduced.report.original_transitions,
            reduced.report.reduced_transitions,
            reduced.report.attempts,
            reduced.report.target_fingerprint,
        );
    }
    return error.UnsupportedScenario;
}

fn writeReducedArtifact(
    alloc: std.mem.Allocator,
    io: std.Io,
    output: []const u8,
    artifact: *const vopr.trace.Trace,
    original_transitions: u64,
    reduced_transitions: u64,
    attempts: u64,
    target_fingerprint: u64,
) !void {
    const reduced_bytes = try artifact.renderAlloc(alloc);
    defer alloc.free(reduced_bytes);
    if (std.fs.path.dirname(output)) |parent| try ensureDir(io, parent);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = output, .data = reduced_bytes });
    std.debug.print(
        "VOPR reduced transitions={d}->{d} attempts={d} fingerprint={x} trace={s}\n",
        .{ original_transitions, reduced_transitions, attempts, target_fingerprint, output },
    );
}

fn promoteCommand(alloc: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var trace_path: ?[]const u8 = null;
    var fixture_name: ?[]const u8 = null;
    var force = false;
    var index: usize = 0;
    while (index < args.len) {
        if (std.mem.eql(u8, args[index], "--trace")) {
            trace_path = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--name")) {
            fixture_name = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--force")) {
            force = true;
        } else return error.UnknownArgument;
        index += 1;
    }
    const input = trace_path orelse return error.TracePathRequired;
    const name = fixture_name orelse return error.FixtureNameRequired;
    try vopr.fixture.validateName(name);
    const encoded = try std.Io.Dir.cwd().readFileAlloc(io, input, alloc, .limited(max_trace_bytes));
    defer alloc.free(encoded);
    var recorded = try vopr.trace.parseAlloc(alloc, encoded);
    defer recorded.deinit();
    if (recorded.failures.items.len == 0) return error.FailingTraceRequired;
    var replayed = try replayKnownScenario(alloc, &recorded);
    replayed.deinit();
    const fixture_dir = try fixtureDirForScenario(&recorded);
    try ensureDir(io, fixture_dir);
    var dir = try std.Io.Dir.cwd().openDir(io, fixture_dir, .{});
    defer dir.close(io);
    const filename = try promoteRecordedToDir(alloc, io, dir, &recorded, name, force);
    defer alloc.free(filename);
    const output = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ fixture_dir, filename });
    defer alloc.free(output);
    std.debug.print("VOPR promoted fingerprint={x} fixture={s}\n", .{ recorded.failures.items[0].fingerprint, output });
}

fn migrateCommand(alloc: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var trace_path: ?[]const u8 = null;
    var output_path: ?[]const u8 = null;
    var force = false;
    var index: usize = 0;
    while (index < args.len) {
        if (std.mem.eql(u8, args[index], "--trace")) {
            trace_path = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--out")) {
            output_path = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--force")) {
            force = true;
        } else return error.UnknownArgument;
        index += 1;
    }
    const input = trace_path orelse return error.TracePathRequired;
    const output = output_path orelse return error.TraceOutputRequired;
    if (std.mem.eql(u8, input, output)) return error.InPlaceFixtureMigrationForbidden;
    const encoded = try std.Io.Dir.cwd().readFileAlloc(io, input, alloc, .limited(max_trace_bytes));
    defer alloc.free(encoded);
    var recorded = try vopr.trace.parseAlloc(alloc, encoded);
    defer recorded.deinit();
    var migration = try vopr.fixture.migrate(
        alloc,
        &recorded,
        replayKnownScenario,
        vopr.fixture.canonicalClone,
        replayKnownScenario,
        .{},
    );
    defer migration.deinit();
    if (!force) {
        if (std.Io.Dir.cwd().statFile(io, output, .{})) |_| {
            return error.MigrationOutputAlreadyExists;
        } else |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        }
    }
    const migrated_bytes = try migration.artifact.renderAlloc(alloc);
    defer alloc.free(migrated_bytes);
    if (std.fs.path.dirname(output)) |parent| try ensureDir(io, parent);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = output, .data = migrated_bytes });
    std.debug.print(
        "VOPR migrated scenario={s} version={d}->{d} trace={x}->{x} outcome={x} out={s}\n",
        .{
            migration.artifact.header.scenario,
            migration.report.source_scenario_version,
            migration.report.migrated_scenario_version,
            migration.report.source_trace_digest,
            migration.report.migrated_trace_digest,
            migration.report.migrated_outcome_digest,
            output,
        },
    );
}

fn fixtureDirForScenario(recorded: *const vopr.trace.Trace) ![]const u8 {
    if (std.mem.eql(u8, recorded.header.scenario, "metadata-vopr"))
        return "pkg/antfly/src/sim/fixtures/metadata";
    if (std.mem.eql(u8, recorded.header.scenario, antfly.metadata_sim_harness.DistributedDataVoprScenario.name))
        return "pkg/antfly/src/sim/fixtures/distributed-data";
    if (std.mem.eql(u8, recorded.header.scenario, antfly.transaction_vopr.Scenario.name))
        return "pkg/antfly/src/sim/fixtures/transaction";
    if (std.mem.eql(u8, recorded.header.scenario, antfly.raft_vopr.CliScenario.name))
        return "pkg/antfly/src/sim/fixtures/raft";
    if (std.mem.eql(u8, recorded.header.scenario, antfly.lmdb_vopr.CliScenario.name))
        return "pkg/antfly/src/sim/fixtures/lmdb";
    if (std.mem.eql(u8, recorded.header.scenario, antfly.lsm_vopr.CliScenario.name))
        return "pkg/antfly/src/sim/fixtures/lsm";
    return error.UnsupportedScenario;
}

fn promoteRecordedToDir(
    alloc: std.mem.Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    recorded: *const vopr.trace.Trace,
    name: []const u8,
    force: bool,
) ![]u8 {
    try vopr.fixture.validatePromotion(recorded, name);
    const filename = try std.fmt.allocPrint(alloc, "{s}.simtrace", .{name});
    errdefer alloc.free(filename);
    if (!force) {
        if (dir.statFile(io, filename, .{})) |_| {
            return error.FixtureAlreadyExists;
        } else |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        }
    }
    const canonical = try recorded.renderAlloc(alloc);
    defer alloc.free(canonical);
    try dir.writeFile(io, .{ .sub_path = filename, .data = canonical });
    return filename;
}

const CampaignContext = struct {
    const Candidate = struct {
        artifact: vopr.trace.Trace,
        parent_indexes: [2]usize = undefined,
        parent_count: u2 = 0,
    };

    const PropertySummary = struct {
        name: []u8,
        evaluations: u64 = 0,
        ever_true: bool = false,
        ever_false: bool = false,
        failed: bool = false,

        fn status(self: PropertySummary) []const u8 {
            if (self.failed) return "fail";
            if (self.evaluations == 0) return "not-reached";
            return "pass";
        }
    };

    const FailureSummary = struct {
        first_history: u64,
        first_path: []u8,
        smallest_history: u64,
        smallest_transitions: u64,
        smallest_path: []u8,
    };

    io: std.Io,
    histories: u64,
    transitions: usize,
    base_seed: u64,
    artifact_dir: []const u8,
    worker_count: usize = 0,
    next_history: std.atomic.Value(u64) = .init(0),
    mutex: std.Io.Mutex = .init,
    coverage: vopr.coverage.Tracker,
    corpus: vopr.corpus.Corpus,
    seeded_entries: u64 = 0,
    retained: u64 = 0,
    failures: u64 = 0,
    clean_histories: u64 = 0,
    transitions_executed: u64 = 0,
    exact_replays: u64 = 0,
    replay_divergences: u64 = 0,
    harness_errors: u64 = 0,
    splice_attempts: u64 = 0,
    spliced: u64 = 0,
    splice_rejected: u64 = 0,
    semantic_states: std.AutoHashMapUnmanaged(u64, void) = .empty,
    transition_ids: std.AutoHashMapUnmanaged(u64, void) = .empty,
    fault_ids: std.AutoHashMapUnmanaged(u64, void) = .empty,
    workload_ids: std.AutoHashMapUnmanaged(u64, void) = .empty,
    properties: std.AutoHashMapUnmanaged(u64, PropertySummary) = .empty,
    failure_summaries: std.AutoHashMapUnmanaged(u64, FailureSummary) = .empty,
    first_error: ?anyerror = null,

    fn deinitReport(self: *@This()) void {
        const alloc = std.heap.smp_allocator;
        var property_it = self.properties.valueIterator();
        while (property_it.next()) |summary| alloc.free(summary.name);
        self.properties.deinit(alloc);
        var failure_it = self.failure_summaries.valueIterator();
        while (failure_it.next()) |summary| {
            alloc.free(summary.first_path);
            alloc.free(summary.smallest_path);
        }
        self.failure_summaries.deinit(alloc);
        self.semantic_states.deinit(alloc);
        self.transition_ids.deinit(alloc);
        self.fault_ids.deinit(alloc);
        self.workload_ids.deinit(alloc);
    }

    fn worker(self: *@This()) void {
        while (true) {
            const history_index = self.next_history.fetchAdd(1, .monotonic);
            if (history_index >= self.histories) return;
            self.runHistory(history_index) catch |err| {
                self.mutex.lock(self.io) catch return;
                defer self.mutex.unlock(self.io);
                self.harness_errors += 1;
                if (self.first_error == null) self.first_error = err;
                return;
            };
        }
    }

    fn runHistory(self: *@This(), history_index: u64) !void {
        const alloc = std.heap.smp_allocator;
        const seed = self.base_seed +% history_index *% 0x9e37_79b9_7f4a_7c15;
        const candidate = (try self.mutateCorpusEntry(alloc, seed)) orelse blk: {
            // Keep semantic identities stable across a campaign. The history
            // seed still varies scheduling, while stable IDs allow compatible
            // observation states from independent histories to be spliced.
            const base_id = 10_000 + self.base_seed % 1_000_000;
            break :blk Candidate{
                .artifact = try antfly.metadata_sim_harness.recordMetadataVoprCampaign(alloc, .{
                    .seed = seed,
                    .operation_count = self.transitions,
                    .metadata_group_id = base_id,
                    .table_id = base_id + 1,
                    .range_group_id = base_id + 2,
                    .split_group_id = base_id + 3,
                    .split_transition_id = base_id + 4,
                }),
            };
        };
        var artifact = candidate.artifact;
        defer artifact.deinit();

        var replayed = antfly.metadata_sim_harness.replayMetadataVoprCampaign(alloc, &artifact) catch |err| {
            try self.mutex.lock(self.io);
            defer self.mutex.unlock(self.io);
            self.replay_divergences += 1;
            if (self.first_error == null) self.first_error = err;
            return;
        };
        replayed.deinit();

        try self.mutex.lock(self.io);
        self.exact_replays += 1;
        self.observeArtifactLocked(&artifact) catch |err| {
            self.mutex.unlock(self.io);
            return err;
        };
        const novelty = self.coverage.observe(&artifact) catch |err| {
            self.mutex.unlock(self.io);
            return err;
        };
        const failed = artifact.failures.items.len > 0;
        const retain = history_index == 0 or novelty.discovered > 0 or failed;
        if (failed) self.failures += 1 else self.clean_histories += 1;
        const added = if (retain) self.corpus.add(&artifact, novelty) catch |err| {
            self.mutex.unlock(self.io);
            return err;
        } else null;
        const inserted = if (added) |result| result.inserted else false;
        if (inserted) {
            self.retained += 1;
            for (candidate.parent_indexes[0..candidate.parent_count]) |parent_index| {
                self.corpus.markProductive(parent_index) catch unreachable;
            }
        }
        self.mutex.unlock(self.io);
        // Corpus retention is deduplicated, but every failing history still
        // receives a standalone artifact and a stable report entry.
        if (!inserted and !failed) return;

        const bytes = try artifact.renderAlloc(alloc);
        defer alloc.free(bytes);
        const path = try std.fmt.allocPrint(alloc, "{s}/history-{d}-{x}.simtrace", .{ self.artifact_dir, history_index, seed });
        defer alloc.free(path);
        try std.Io.Dir.cwd().writeFile(self.io, .{ .sub_path = path, .data = bytes });
        if (failed) {
            try self.mutex.lock(self.io);
            defer self.mutex.unlock(self.io);
            try self.recordFailureArtifactsLocked(&artifact, history_index, path);
        }
    }

    fn observeArtifactLocked(self: *@This(), artifact: *const vopr.trace.Trace) !void {
        const alloc = std.heap.smp_allocator;
        self.transitions_executed +|= artifact.summary.?.transitions;
        for (artifact.observations.items) |record| try self.semantic_states.put(alloc, record.digest, {});
        for (artifact.transitions.items) |record| {
            try self.transition_ids.put(alloc, record.id, {});
            if (record.kind == .workload) try self.workload_ids.put(alloc, record.id, {});
        }
        for (artifact.faults.items) |record| try self.fault_ids.put(alloc, record.id, {});
        for (artifact.properties.items) |record| {
            const gop = try self.properties.getOrPut(alloc, record.property_id);
            if (!gop.found_existing) {
                const name = alloc.dupe(u8, record.name) catch |err| {
                    _ = self.properties.remove(record.property_id);
                    return err;
                };
                gop.value_ptr.* = .{ .name = name };
            }
            gop.value_ptr.evaluations +|= 1;
            gop.value_ptr.ever_true = gop.value_ptr.ever_true or record.condition;
            gop.value_ptr.ever_false = gop.value_ptr.ever_false or !record.condition;
        }
        for (artifact.failures.items) |failure| if (failure.property_id) |property_id| {
            if (self.properties.getPtr(property_id)) |summary| summary.failed = true;
        };
    }

    fn recordFailureArtifactsLocked(
        self: *@This(),
        artifact: *const vopr.trace.Trace,
        history_index: u64,
        path: []const u8,
    ) !void {
        const alloc = std.heap.smp_allocator;
        for (artifact.failures.items) |failure| {
            const gop = try self.failure_summaries.getOrPut(alloc, failure.fingerprint);
            if (!gop.found_existing) {
                errdefer _ = self.failure_summaries.remove(failure.fingerprint);
                const first_path = try alloc.dupe(u8, path);
                errdefer alloc.free(first_path);
                const smallest_path = try alloc.dupe(u8, path);
                errdefer alloc.free(smallest_path);
                gop.value_ptr.* = .{
                    .first_history = history_index,
                    .first_path = first_path,
                    .smallest_history = history_index,
                    .smallest_transitions = artifact.summary.?.transitions,
                    .smallest_path = smallest_path,
                };
                continue;
            }
            if (history_index < gop.value_ptr.first_history) {
                const replacement = try alloc.dupe(u8, path);
                alloc.free(gop.value_ptr.first_path);
                gop.value_ptr.first_path = replacement;
                gop.value_ptr.first_history = history_index;
            }
            if (artifact.summary.?.transitions < gop.value_ptr.smallest_transitions or
                (artifact.summary.?.transitions == gop.value_ptr.smallest_transitions and history_index < gop.value_ptr.smallest_history))
            {
                const replacement = try alloc.dupe(u8, path);
                alloc.free(gop.value_ptr.smallest_path);
                gop.value_ptr.smallest_path = replacement;
                gop.value_ptr.smallest_history = history_index;
                gop.value_ptr.smallest_transitions = artifact.summary.?.transitions;
            }
        }
    }

    fn reportSummary(self: *@This()) !void {
        const alloc = std.heap.smp_allocator;
        var productive: usize = 0;
        for (self.corpus.entries.items) |entry| productive += @intFromBool(entry.productive_children > 0);
        std.debug.print(
            "VOPR campaign histories={d} transitions={d} clean={d} failed={d} divergent={d} harness_errors={d} exact_replays={d} seeded={d} retained={d} states={d} transition_kinds={d} faults_reached={d} workloads_reached={d} productive_inputs={d} splice_attempts={d} spliced={d} splice_rejected={d} workers={d} artifacts={s}\n",
            .{
                self.histories,
                self.transitions_executed,
                self.clean_histories,
                self.failures,
                self.replay_divergences,
                self.harness_errors,
                self.exact_replays,
                self.seeded_entries,
                self.retained,
                self.semantic_states.count(),
                self.transition_ids.count(),
                self.fault_ids.count(),
                self.workload_ids.count(),
                productive,
                self.splice_attempts,
                self.spliced,
                self.splice_rejected,
                self.worker_count,
                self.artifact_dir,
            },
        );
        const property_ids = try alloc.alloc(u64, self.properties.count());
        defer alloc.free(property_ids);
        var property_it = self.properties.keyIterator();
        var property_count: usize = 0;
        while (property_it.next()) |property_id| : (property_count += 1) property_ids[property_count] = property_id.*;
        std.mem.sort(u64, property_ids, {}, std.sort.asc(u64));
        for (property_ids) |property_id| {
            const summary = self.properties.get(property_id).?;
            std.debug.print("VOPR property id={x} name={s} status={s} evaluations={d} true={} false={}\n", .{
                property_id,
                summary.name,
                summary.status(),
                summary.evaluations,
                summary.ever_true,
                summary.ever_false,
            });
        }
        const fingerprints = try alloc.alloc(u64, self.failure_summaries.count());
        defer alloc.free(fingerprints);
        var failure_it = self.failure_summaries.keyIterator();
        var failure_count: usize = 0;
        while (failure_it.next()) |fingerprint| : (failure_count += 1) fingerprints[failure_count] = fingerprint.*;
        std.mem.sort(u64, fingerprints, {}, std.sort.asc(u64));
        for (fingerprints) |fingerprint| {
            const summary = self.failure_summaries.get(fingerprint).?;
            std.debug.print(
                "VOPR failure fingerprint={x} first={s} smallest_transitions={d} smallest={s} replay='zig build sim-replay -- --trace {s}' reduce='zig build sim-reduce -- --trace {s} --out <reduced.simtrace>'\n",
                .{ fingerprint, summary.first_path, summary.smallest_transitions, summary.smallest_path, summary.smallest_path, summary.smallest_path },
            );
        }
    }

    fn mutateCorpusEntry(self: *@This(), alloc: std.mem.Allocator, seed: u64) !?Candidate {
        var prng = std.Random.DefaultPrng.init(seed ^ 0x6d75_7461_7465_3031);
        try self.mutex.lock(self.io);
        if (self.corpus.entries.items.len == 0) {
            self.mutex.unlock(self.io);
            return null;
        }
        const should_splice = self.corpus.entries.items.len >= 2 and prng.random().uintLessThan(u8, 100) < 20;
        self.mutex.unlock(self.io);
        if (should_splice) {
            if (try self.spliceCorpusEntries(alloc, &prng)) |artifact| return artifact;
        }

        try self.mutex.lock(self.io);
        const parent_index = self.corpus.select(prng.random()) catch |err| {
            self.mutex.unlock(self.io);
            return err;
        };
        const parent_bytes = alloc.dupe(u8, self.corpus.entries.items[parent_index].bytes) catch |err| {
            self.mutex.unlock(self.io);
            return err;
        };
        self.mutex.unlock(self.io);
        defer alloc.free(parent_bytes);

        var parent = try vopr.trace.parseAlloc(alloc, parent_bytes);
        defer parent.deinit();
        var mutable_count: usize = 0;
        for (parent.choices.items) |record| mutable_count += @intFromBool(record.enabled_ids.len > 1);
        if (mutable_count == 0) return null;
        var ordinal = prng.random().uintLessThan(usize, mutable_count);
        var mutation_index: usize = 0;
        for (parent.choices.items, 0..) |record, index| {
            if (record.enabled_ids.len <= 1) continue;
            if (ordinal == 0) {
                mutation_index = index;
                break;
            }
            ordinal -= 1;
        }
        const record = parent.choices.items[mutation_index];
        var replacement_count: usize = 0;
        for (record.enabled_ids) |candidate| replacement_count += @intFromBool(candidate != record.selected_id);
        var replacement_ordinal = prng.random().uintLessThan(usize, replacement_count);
        var replacement = record.selected_id;
        for (record.enabled_ids) |candidate| {
            if (candidate == record.selected_id) continue;
            if (replacement_ordinal == 0) {
                replacement = candidate;
                break;
            }
            replacement_ordinal -= 1;
        }
        const cfg = try antfly.metadata_sim_harness.MetadataVoprCampaignConfig.fromTrace(&parent);
        var source = vopr.choice.Mutating.init(parent.choices.items, mutation_index, replacement, seed);
        const artifact = antfly.metadata_sim_harness.runMetadataVoprCampaignWithChoices(alloc, cfg, source.source()) catch |err| switch (err) {
            error.MutationPointNotReached,
            error.MutationPointOutOfRange,
            error.MutationPrefixExhausted,
            error.ReplayChoiceSiteDiverged,
            error.ReplayEnabledSetDiverged,
            error.ChoiceSourceSelectedDisabledAlternative,
            => return null,
            else => return err,
        };
        return .{ .artifact = artifact, .parent_indexes = .{ parent_index, undefined }, .parent_count = 1 };
    }

    fn spliceCorpusEntries(
        self: *@This(),
        alloc: std.mem.Allocator,
        prng: *std.Random.DefaultPrng,
    ) !?Candidate {
        try self.mutex.lock(self.io);
        if (self.corpus.entries.items.len < 2) {
            self.mutex.unlock(self.io);
            return null;
        }
        self.splice_attempts += 1;
        const left_index = self.corpus.select(prng.random()) catch |err| {
            self.mutex.unlock(self.io);
            return err;
        };
        var right_index = self.corpus.select(prng.random()) catch |err| {
            self.mutex.unlock(self.io);
            return err;
        };
        if (right_index == left_index) right_index = (right_index + 1) % self.corpus.entries.items.len;
        const left_bytes = alloc.dupe(u8, self.corpus.entries.items[left_index].bytes) catch |err| {
            self.mutex.unlock(self.io);
            return err;
        };
        const right_bytes = alloc.dupe(u8, self.corpus.entries.items[right_index].bytes) catch |err| {
            alloc.free(left_bytes);
            self.mutex.unlock(self.io);
            return err;
        };
        self.mutex.unlock(self.io);
        defer alloc.free(left_bytes);
        defer alloc.free(right_bytes);

        var left = try vopr.trace.parseAlloc(alloc, left_bytes);
        defer left.deinit();
        var right = try vopr.trace.parseAlloc(alloc, right_bytes);
        defer right.deinit();
        if (!std.mem.eql(u8, left.header.scenario, right.header.scenario) or
            left.header.scenario_version != right.header.scenario_version)
        {
            try self.recordSpliceResult(false);
            return null;
        }
        const points = try vopr.splice.findPoints(alloc, &left, &right);
        defer alloc.free(points);
        var usable: usize = 0;
        for (points) |point| {
            const combined = point.left_choice_count + right.choices.items.len - point.right_choice_start;
            // The metadata driver has a fixed operation count plus quiet
            // suffix. Preserve its total choice count while joining states.
            usable += @intFromBool(combined == left.choices.items.len);
        }
        if (usable == 0) {
            try self.recordSpliceResult(false);
            return null;
        }
        var ordinal = prng.random().uintLessThan(usize, usable);
        const point = for (points) |candidate| {
            const combined = candidate.left_choice_count + right.choices.items.len - candidate.right_choice_start;
            if (combined != left.choices.items.len) continue;
            if (ordinal == 0) break candidate;
            ordinal -= 1;
        } else unreachable;
        const cfg = try antfly.metadata_sim_harness.MetadataVoprCampaignConfig.fromTrace(&left);
        var source = try vopr.splice.Source.init(left.choices.items, right.choices.items, point);
        var artifact = antfly.metadata_sim_harness.runMetadataVoprCampaignWithChoices(alloc, cfg, source.source()) catch |err| switch (err) {
            error.SpliceChoiceExhausted,
            error.SpliceChoiceSiteDiverged,
            error.SpliceEnabledSetDiverged,
            error.SpliceHasTrailingChoices,
            error.ChoiceSourceSelectedDisabledAlternative,
            => {
                try self.recordSpliceResult(false);
                return null;
            },
            else => return err,
        };
        errdefer artifact.deinit();
        var replayed = try antfly.metadata_sim_harness.replayMetadataVoprCampaign(alloc, &artifact);
        replayed.deinit();
        try self.recordSpliceResult(true);
        return .{
            .artifact = artifact,
            .parent_indexes = .{ left_index, right_index },
            .parent_count = 2,
        };
    }

    fn recordSpliceResult(self: *@This(), accepted: bool) !void {
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);
        if (accepted) {
            self.spliced += 1;
        } else {
            self.splice_rejected += 1;
        }
    }

    fn primeCorpus(self: *@This(), alloc: std.mem.Allocator) !void {
        var dir = if (std.fs.path.isAbsolute(self.artifact_dir))
            try std.Io.Dir.openDirAbsolute(self.io, self.artifact_dir, .{ .iterate = true })
        else
            try std.Io.Dir.cwd().openDir(self.io, self.artifact_dir, .{ .iterate = true });
        defer dir.close(self.io);
        var names: std.ArrayListUnmanaged([]u8) = .empty;
        defer {
            for (names.items) |name| alloc.free(name);
            names.deinit(alloc);
        }
        var walker = try dir.walk(alloc);
        defer walker.deinit();
        while (try walker.next(self.io)) |entry| {
            if (entry.kind != .file or !std.mem.endsWith(u8, entry.path, ".simtrace")) continue;
            try names.append(alloc, try alloc.dupe(u8, entry.path));
        }
        std.mem.sort([]u8, names.items, {}, struct {
            fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
                return std.mem.lessThan(u8, lhs, rhs);
            }
        }.lessThan);
        for (names.items) |name| {
            const encoded = try dir.readFileAlloc(self.io, name, alloc, .limited(max_trace_bytes));
            defer alloc.free(encoded);
            var artifact = try vopr.trace.parseAlloc(alloc, encoded);
            defer artifact.deinit();
            // Corpus files must still be executable under the current ABI.
            var replayed = try antfly.metadata_sim_harness.replayMetadataVoprCampaign(alloc, &artifact);
            replayed.deinit();
            const novelty = try self.coverage.observe(&artifact);
            const added = try self.corpus.add(&artifact, novelty);
            if (added.inserted) self.seeded_entries += 1;
        }
    }
};

fn ensureDir(io: std.Io, path: []const u8) !void {
    if (!std.fs.path.isAbsolute(path)) return try std.Io.Dir.cwd().createDirPath(io, path);
    if (std.Io.Dir.openDirAbsolute(io, path, .{})) |dir_value| {
        var dir = dir_value;
        dir.close(io);
        return;
    } else |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    }
    const parent = std.fs.path.dirname(path) orelse return error.InvalidAbsoluteDirectory;
    try ensureDir(io, parent);
    var parent_dir = try std.Io.Dir.openDirAbsolute(io, parent, .{});
    defer parent_dir.close(io);
    try parent_dir.createDirPath(io, std.fs.path.basename(path));
}

fn nextValue(args: []const []const u8, index: *usize) ![]const u8 {
    index.* += 1;
    if (index.* >= args.len) return error.MissingArgumentValue;
    return args[index.*];
}

fn report(action: []const u8, path: []const u8, artifact: *const vopr.trace.Trace) void {
    const summary = artifact.summary.?;
    std.debug.print(
        "VOPR {s} scenario={s} transitions={d} failures={d} trace={s}\n",
        .{ action, artifact.header.scenario, summary.transitions, artifact.failures.items.len, path },
    );
}

fn usage() error{InvalidUsage} {
    std.debug.print(
        \\usage:
        \\  vopr run --scenario metadata|transaction|distributed-data --seed <u64> [--transitions <n>] [--workload smoke|expanded] --trace-out <path>
        \\  vopr replay --trace <path>
        \\  vopr campaign --scenario metadata --histories <n> --transitions <n> --workers <n> --artifact-dir <path>
        \\  vopr reduce --trace <path> --out <path> [--attempts <n>]
        \\  vopr promote --trace <path> --name <fixture-name> [--force]
        \\  vopr migrate --trace <path> --out <path> [--force]
        \\  vopr tla --trace <path> --domain raft|transaction --out <path.ndjson>
        \\  vopr explain --trace <path> [--failure <ordinal>] --out <path.json>
        \\
    , .{});
    return error.InvalidUsage;
}

test "VOPR command entrypoint" {
    const init_ptr: *const std.process.Init.Minimal = @ptrCast(@alignCast(antflyVoprProcessInit()));
    const init = init_ptr.*;
    const argv = try init.args.toSlice(std.testing.allocator);
    defer std.testing.allocator.free(argv);
    try dispatch(std.testing.allocator, std.testing.io, argv);
}

extern fn antflyVoprProcessInit() *const anyopaque;

test "Antfly injected bug is discovered replayed reduced and promoted" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var discovered = try antfly.metadata_sim_harness.discoverMetadataVoprInjectedOverlap(alloc, 0xA17F_FA11);
    defer discovered.deinit();
    try std.testing.expectEqual(@as(usize, 1), discovered.failures.items.len);

    var replayed = try antfly.metadata_sim_harness.replayMetadataVoprCampaign(alloc, &discovered);
    replayed.deinit();
    var reduced = try antfly.metadata_sim_harness.reduceMetadataVoprCampaign(alloc, &discovered, 8);
    defer reduced.deinit();
    try std.testing.expectEqual(discovered.failures.items[0].fingerprint, reduced.target_fingerprint);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const filename = try promoteRecordedToDir(alloc, io, tmp.dir, &reduced.artifact, "injected-overlap", false);
    defer alloc.free(filename);
    try std.testing.expectError(error.FixtureAlreadyExists, promoteRecordedToDir(alloc, io, tmp.dir, &reduced.artifact, "injected-overlap", false));

    const encoded = try tmp.dir.readFileAlloc(io, filename, alloc, .limited(max_trace_bytes));
    defer alloc.free(encoded);
    var promoted = try vopr.trace.parseAlloc(alloc, encoded);
    defer promoted.deinit();
    try std.testing.expectEqual(reduced.target_fingerprint, promoted.failures.items[0].fingerprint);
    var promoted_replay = try antfly.metadata_sim_harness.replayMetadataVoprCampaign(alloc, &promoted);
    promoted_replay.deinit();
    var raft_ndjson: std.Io.Writer.Allocating = .init(alloc);
    defer raft_ndjson.deinit();
    var formal_replay = try antfly.metadata_sim_harness.replayMetadataVoprCampaignToRaftTrace(alloc, &promoted, &raft_ndjson.writer);
    formal_replay.deinit();
    try validateRaftTraceNdjson(alloc, raft_ndjson.written());
    try std.testing.expect(std.mem.indexOf(u8, raft_ndjson.written(), "\"name\":\"InitState\"") != null);
    var causal_report = try vopr.causal.analyzeAlloc(alloc, &promoted, 0);
    defer causal_report.deinit();
    try std.testing.expect(causal_report.items.len > 0);
    const causal_json = try causal_report.renderAlloc(alloc);
    defer alloc.free(causal_json);
    try std.testing.expect(std.mem.indexOf(u8, causal_json, "metadata.injected.overlapping_link_fault_safety") != null);

    const corpus_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(corpus_path);
    var context = CampaignContext{
        .io = io,
        .histories = 1,
        .transitions = 2,
        .base_seed = 1,
        .artifact_dir = corpus_path,
        .coverage = vopr.coverage.Tracker.init(alloc),
        .corpus = vopr.corpus.Corpus.init(alloc),
    };
    defer context.corpus.deinit();
    defer context.coverage.deinit();
    try context.primeCorpus(alloc);
    try std.testing.expectEqual(@as(u64, 1), context.seeded_entries);
    try std.testing.expectEqual(@as(usize, 1), context.corpus.entries.items.len);
    if (try context.mutateCorpusEntry(alloc, 0xA17F_FA12)) |mutated_value| {
        var mutated = mutated_value;
        defer mutated.artifact.deinit();
        var mutation_replay = try antfly.metadata_sim_harness.replayMetadataVoprCampaign(alloc, &mutated.artifact);
        mutation_replay.deinit();
    } else return error.PersistentCorpusMutationRequired;

    var transaction_artifact = try antfly.transaction_vopr.record(alloc, 0xA17F_7A4A);
    defer transaction_artifact.deinit();
    try std.testing.expectEqualStrings("pkg/antfly/src/sim/fixtures/metadata", try fixtureDirForScenario(&promoted));
    try std.testing.expectEqualStrings("pkg/antfly/src/sim/fixtures/transaction", try fixtureDirForScenario(&transaction_artifact));
    var generic_transaction_replay = try replayKnownScenario(alloc, &transaction_artifact);
    generic_transaction_replay.deinit();
    var transaction_ndjson: std.Io.Writer.Allocating = .init(alloc);
    defer transaction_ndjson.deinit();
    var transaction_replay = try antfly.transaction_vopr.replayToTransactionTrace(alloc, &transaction_artifact, &transaction_ndjson.writer);
    transaction_replay.deinit();
    try validateTransactionTraceNdjson(alloc, transaction_ndjson.written());
    try std.testing.expect(std.mem.indexOf(u8, transaction_ndjson.written(), "\"name\":\"InitTransaction\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, transaction_ndjson.written(), "\"name\":\"WriteIntentOnShard\"") != null);
}
