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
    if (std.mem.eql(u8, argv[1], "debug")) return debugCommand(alloc, io, argv[2..]);
    if (std.mem.eql(u8, argv[1], "results")) return resultsCommand(alloc, io, argv[2..]);
    if (std.mem.eql(u8, argv[1], "events")) return eventsCommand(alloc, io, argv[2..]);
    if (std.mem.eql(u8, argv[1], "recipe")) return recipeCommand(alloc, io, argv[2..]);
    if (std.mem.eql(u8, argv[1], "index")) return indexCommand(alloc, io, argv[2..]);
    if (std.mem.eql(u8, argv[1], "corpus-merge")) return corpusMergeCommand(alloc, io, argv[2..]);
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
                !std.mem.eql(u8, scenario, "wal") and
                !std.mem.eql(u8, scenario, "persistent") and
                !std.mem.eql(u8, scenario, "index-manager") and
                !std.mem.eql(u8, scenario, "db-split") and
                !std.mem.eql(u8, scenario, "raft") and
                !std.mem.eql(u8, scenario, "lmdb") and
                !std.mem.eql(u8, scenario, "lsm") and
                !std.mem.eql(u8, scenario, "ha") and
                antfly.domain_vopr.kindFromCliName(scenario) == null) return error.UnsupportedScenario;
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
    else if (std.mem.eql(u8, scenario, "wal"))
        25
    else if (std.mem.eql(u8, scenario, "persistent"))
        17
    else if (std.mem.eql(u8, scenario, "index-manager"))
        11
    else if (std.mem.eql(u8, scenario, "db-split"))
        12
    else if (std.mem.eql(u8, scenario, "raft"))
        33
    else if (std.mem.eql(u8, scenario, "lmdb"))
        13
    else if (std.mem.eql(u8, scenario, "lsm"))
        49
    else if (std.mem.eql(u8, scenario, "ha"))
        33
    else if (antfly.domain_vopr.kindFromCliName(scenario)) |kind|
        kind.transitionBudget()
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
    } else if (std.mem.eql(u8, scenario, "wal")) blk: {
        if (transition_budget != 25) return error.WalScenarioRequiresTwentyFiveTransitions;
        break :blk try antfly.wal_vopr.record(alloc, seed);
    } else if (std.mem.eql(u8, scenario, "persistent")) blk: {
        if (transition_budget != 17) return error.PersistentScenarioRequiresSeventeenTransitions;
        break :blk try antfly.persistent_vopr.record(alloc, seed);
    } else if (std.mem.eql(u8, scenario, "index-manager")) blk: {
        if (transition_budget != 11) return error.IndexManagerScenarioRequiresElevenTransitions;
        break :blk try antfly.index_manager_vopr.record(alloc, seed);
    } else if (std.mem.eql(u8, scenario, "db-split")) blk: {
        if (transition_budget != 12) return error.DbSplitScenarioRequiresTwelveTransitions;
        break :blk try antfly.db_split_vopr.record(alloc, seed);
    } else if (std.mem.eql(u8, scenario, "raft")) blk: {
        if (transition_budget != 33) return error.RaftScenarioRequiresThirtyThreeTransitions;
        break :blk try antfly.raft_vopr.record(alloc, seed);
    } else if (std.mem.eql(u8, scenario, "lmdb")) blk: {
        if (transition_budget != 13) return error.LmdbScenarioRequiresThirteenTransitions;
        break :blk try antfly.lmdb_vopr.record(alloc, seed);
    } else if (std.mem.eql(u8, scenario, "lsm")) blk: {
        if (transition_budget != 49) return error.LsmScenarioRequiresFortyNineTransitions;
        break :blk try antfly.lsm_vopr.record(alloc, seed);
    } else if (std.mem.eql(u8, scenario, "ha")) blk: {
        if (transition_budget != 33) return error.HaScenarioRequiresThirtyThreeTransitions;
        break :blk try antfly.ha_vopr.record(alloc, seed);
    } else if (antfly.domain_vopr.kindFromCliName(scenario)) |kind| blk: {
        if (transition_budget != kind.transitionBudget()) return error.ScenarioRequiresFixedTransitionBudget;
        break :blk try antfly.domain_vopr.recordNamed(alloc, scenario, seed);
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

fn resultsCommand(alloc: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var trace_path: ?[]const u8 = null;
    var json_path: ?[]const u8 = null;
    var html_path: ?[]const u8 = null;
    var run_id: ?[]const u8 = null;
    var history_id: ?[]const u8 = null;
    var index: usize = 0;
    while (index < args.len) {
        if (std.mem.eql(u8, args[index], "--trace")) {
            trace_path = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--json-out")) {
            json_path = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--html-out")) {
            html_path = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--run-id")) {
            run_id = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--history-id")) {
            history_id = try nextValue(args, &index);
        } else return error.UnknownArgument;
        index += 1;
    }
    const input = trace_path orelse return error.TracePathRequired;
    if (json_path == null and html_path == null) return error.ResultsOutputRequired;
    const encoded = try std.Io.Dir.cwd().readFileAlloc(io, input, alloc, .limited(max_trace_bytes));
    defer alloc.free(encoded);
    var recorded = try vopr.trace.parseAlloc(alloc, encoded);
    defer recorded.deinit();
    var replayed = try replayKnownScenario(alloc, &recorded);
    const replayed_health = replayed.health_evidence;
    replayed.deinit();
    const artifact_paths = [_][]const u8{input};
    var results = try vopr.report.Results.build(alloc, &recorded, declarationsKnown(&recorded), .{
        .run_id = run_id,
        .history_id = history_id,
        .artifacts = &artifact_paths,
        .health_evidence = replayed_health,
    });
    defer results.deinit();
    if (json_path) |output| {
        const bytes = try results.renderJsonAlloc(alloc);
        defer alloc.free(bytes);
        try writeDebugArtifact(io, output, bytes);
    }
    if (html_path) |output| {
        const bytes = try results.renderHtmlAlloc(alloc);
        defer alloc.free(bytes);
        try writeDebugArtifact(io, output, bytes);
    }
    std.debug.print("VOPR results run={s} history={s} json={s} html={s}\n", .{
        results.run_id,
        results.history_id,
        json_path orelse "-",
        html_path orelse "-",
    });
}

fn eventsCommand(alloc: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var trace_paths: std.ArrayListUnmanaged([]const u8) = .empty;
    defer trace_paths.deinit(alloc);
    var query_path: ?[]const u8 = null;
    var output_path: ?[]const u8 = null;
    var count_only = false;
    var validate_only = false;
    var index: usize = 0;
    while (index < args.len) {
        if (std.mem.eql(u8, args[index], "--trace")) {
            try trace_paths.append(alloc, try nextValue(args, &index));
        } else if (std.mem.eql(u8, args[index], "--query")) {
            query_path = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--out")) {
            output_path = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--count")) {
            count_only = true;
        } else if (std.mem.eql(u8, args[index], "--validate")) {
            validate_only = true;
        } else return error.UnknownArgument;
        index += 1;
    }
    const query_input = query_path orelse return error.EventQueryPathRequired;
    const query_bytes = try std.Io.Dir.cwd().readFileAlloc(io, query_input, alloc, .limited(1024 * 1024));
    defer alloc.free(query_bytes);

    var parsed_plan = try vopr.event_set.parseAlloc(alloc, query_bytes);
    defer parsed_plan.deinit();
    const plan = parsed_plan.value;
    try plan.validate();
    if (validate_only) {
        std.debug.print("VOPR event query valid format={s} name={s}\n", .{ plan.format, plan.name });
        return;
    }
    if (trace_paths.items.len == 0) return error.TracePathRequired;
    const output = output_path orelse return error.EventQueryOutputRequired;

    var traces: std.ArrayListUnmanaged(vopr.trace.Trace) = .empty;
    defer {
        for (traces.items) |*recorded| recorded.deinit();
        traces.deinit(alloc);
    }
    for (trace_paths.items) |input| {
        const encoded = try std.Io.Dir.cwd().readFileAlloc(io, input, alloc, .limited(max_trace_bytes));
        defer alloc.free(encoded);
        var recorded = try vopr.trace.parseAlloc(alloc, encoded);
        errdefer recorded.deinit();
        var replayed = try replayKnownScenario(alloc, &recorded);
        replayed.deinit();
        try traces.append(alloc, recorded);
    }
    const histories = try alloc.alloc(vopr.event_set.History, traces.items.len);
    defer alloc.free(histories);
    for (histories, traces.items, trace_paths.items) |*history, *recorded, input|
        history.* = .{ .id = input, .artifact = recorded };

    var matches: []vopr.event_set.Match = &.{};
    defer if (matches.len > 0) alloc.free(matches);
    const match_count = if (count_only)
        try vopr.event_set.count(alloc, histories, plan)
    else blk: {
        var evaluated = try vopr.event_set.evaluateAlloc(alloc, histories, plan);
        matches = evaluated.matches;
        evaluated.matches = &.{};
        break :blk matches.len;
    };
    const result = try std.json.Stringify.valueAlloc(alloc, .{
        .format = "vopr-event-set-result-v1",
        .traces = trace_paths.items,
        .query_name = plan.name,
        .count_only = count_only,
        .match_count = match_count,
        .matches = matches,
    }, .{ .whitespace = .indent_2 });
    defer alloc.free(result);
    try writeDebugArtifact(io, output, result);
    std.debug.print("VOPR event query matches={d} histories={d} out={s}\n", .{ match_count, histories.len, output });
}

fn recipeCommand(alloc: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var trace_path: ?[]const u8 = null;
    var output_path: ?[]const u8 = null;
    var reduced_path: ?[]const u8 = null;
    var failure_ordinal: usize = 0;
    var attempts: u64 = 1_024;
    var flight_filter_path: ?[]const u8 = null;
    var flight_config: vopr.debug_recipe.FlightConfig = .{};
    var index: usize = 0;
    while (index < args.len) {
        if (std.mem.eql(u8, args[index], "--trace")) {
            trace_path = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--out")) {
            output_path = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--reduced-out")) {
            reduced_path = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--failure")) {
            failure_ordinal = try std.fmt.parseInt(usize, try nextValue(args, &index), 10);
        } else if (std.mem.eql(u8, args[index], "--attempts")) {
            attempts = try std.fmt.parseInt(u64, try nextValue(args, &index), 10);
        } else if (std.mem.eql(u8, args[index], "--flight-filter")) {
            flight_filter_path = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--flight-before")) {
            flight_config.before_records = try std.fmt.parseInt(usize, try nextValue(args, &index), 10);
        } else if (std.mem.eql(u8, args[index], "--flight-after")) {
            flight_config.after_records = try std.fmt.parseInt(usize, try nextValue(args, &index), 10);
        } else if (std.mem.eql(u8, args[index], "--flight-limit")) {
            flight_config.max_records = try std.fmt.parseInt(usize, try nextValue(args, &index), 10);
        } else if (std.mem.eql(u8, args[index], "--flight-capacity")) {
            flight_config.capacity = try std.fmt.parseInt(usize, try nextValue(args, &index), 10);
        } else if (std.mem.eql(u8, args[index], "--flight-anywhere")) {
            flight_config.anchor_failure_transition = false;
        } else return error.UnknownArgument;
        index += 1;
    }
    const input = trace_path orelse return error.TracePathRequired;
    const output = output_path orelse return error.DebugRecipeOutputRequired;
    const reduced_output = reduced_path orelse return error.ReducedTraceOutputRequired;
    const encoded = try std.Io.Dir.cwd().readFileAlloc(io, input, alloc, .limited(max_trace_bytes));
    defer alloc.free(encoded);
    var recorded = try vopr.trace.parseAlloc(alloc, encoded);
    defer recorded.deinit();
    var filter_arena = std.heap.ArenaAllocator.init(alloc);
    defer filter_arena.deinit();
    if (flight_filter_path) |path| {
        const filter_bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(1024 * 1024));
        defer alloc.free(filter_bytes);
        flight_config.filter = try std.json.parseFromSliceLeaky(
            vopr.flight_recorder.Filter,
            filter_arena.allocator(),
            filter_bytes,
            .{},
        );
    }
    var package = try runDebugRecipeKnown(alloc, &recorded, failure_ordinal, attempts, flight_config);
    defer package.deinit();
    const package_bytes = try package.renderAlloc(alloc);
    defer alloc.free(package_bytes);
    try writeDebugArtifact(io, output, package_bytes);
    const reduced_bytes = try package.reduced.artifact.renderAlloc(alloc);
    defer alloc.free(reduced_bytes);
    try writeDebugArtifact(io, reduced_output, reduced_bytes);
    std.debug.print("VOPR debug recipe fingerprint={x} transitions={d}->{d} out={s} reduced={s}\n", .{
        package.fingerprint,
        package.reduced.report.original_transitions,
        package.reduced.report.reduced_transitions,
        output,
        reduced_output,
    });
}

fn corpusMergeCommand(alloc: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var base_path: ?[]const u8 = null;
    var output_dir: ?[]const u8 = null;
    var force = false;
    var trace_paths: std.ArrayListUnmanaged([]const u8) = .empty;
    defer trace_paths.deinit(alloc);
    var index: usize = 0;
    while (index < args.len) {
        if (std.mem.eql(u8, args[index], "--base")) {
            base_path = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--trace")) {
            try trace_paths.append(alloc, try nextValue(args, &index));
        } else if (std.mem.eql(u8, args[index], "--out-dir")) {
            output_dir = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--force")) {
            force = true;
        } else return error.UnknownArgument;
        index += 1;
    }
    const base_input = base_path orelse return error.CorpusBaseTraceRequired;
    const output = output_dir orelse return error.CorpusOutputDirectoryRequired;
    const manifest_path = try std.fmt.allocPrint(alloc, "{s}/index.json", .{output});
    defer alloc.free(manifest_path);
    if (!force) {
        if (std.Io.Dir.cwd().statFile(io, manifest_path, .{})) |_| {
            return error.CorpusOutputAlreadyExists;
        } else |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        }
    }

    var owned_bytes: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (owned_bytes.items) |bytes| alloc.free(bytes);
        owned_bytes.deinit(alloc);
    }
    try owned_bytes.append(alloc, try std.Io.Dir.cwd().readFileAlloc(io, base_input, alloc, .limited(max_trace_bytes)));
    for (trace_paths.items) |path| try owned_bytes.append(
        alloc,
        try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(max_trace_bytes)),
    );
    var base = try vopr.trace.parseAlloc(alloc, owned_bytes.items[0]);
    defer base.deinit();
    var base_replay = try replayKnownScenario(alloc, &base);
    base_replay.deinit();
    const compatibility = vopr.corpus.Compatibility{
        .scenario = base.header.scenario,
        .scenario_version = base.header.scenario_version,
        .backend_ids = base.config.backend_ids,
    };
    var corpus = vopr.corpus.Corpus.init(alloc);
    defer corpus.deinit();
    var candidates: std.ArrayListUnmanaged(vopr.corpus.MergeCandidate) = .empty;
    defer candidates.deinit(alloc);
    var replay_quarantined: u64 = 0;
    for (owned_bytes.items) |bytes| {
        var candidate = vopr.trace.parseAlloc(alloc, bytes) catch {
            try candidates.append(alloc, .{ .bytes = bytes });
            continue;
        };
        defer candidate.deinit();
        const compatible = std.mem.eql(u8, candidate.header.scenario, compatibility.scenario) and
            candidate.header.scenario_version == compatibility.scenario_version and
            std.mem.eql(vopr.id.StableId, candidate.config.backend_ids, compatibility.backend_ids);
        if (compatible) {
            var replayed = replayKnownScenario(alloc, &candidate) catch {
                _ = try corpus.quarantineBytes(bytes, .replay_diverged);
                replay_quarantined += 1;
                continue;
            };
            replayed.deinit();
        }
        try candidates.append(alloc, .{ .bytes = bytes });
    }
    var merge_report = try corpus.merge(candidates.items, compatibility);
    merge_report.candidates += replay_quarantined;
    merge_report.quarantined += replay_quarantined;
    try ensureDir(io, output);
    const quarantine_dir = try std.fmt.allocPrint(alloc, "{s}/quarantine", .{output});
    defer alloc.free(quarantine_dir);
    if (corpus.quarantined.items.len != 0) try ensureDir(io, quarantine_dir);

    const ManifestArtifact = struct { trace_digest: u64, path: []const u8 };
    const ManifestQuarantine = struct { trace_digest: u64, reason: vopr.corpus.QuarantineReason, path: []const u8 };
    const artifacts = try alloc.alloc(ManifestArtifact, corpus.entries.items.len);
    defer alloc.free(artifacts);
    var artifacts_initialized: usize = 0;
    defer for (artifacts[0..artifacts_initialized]) |item| alloc.free(item.path);
    for (corpus.entries.items, 0..) |entry, entry_index| {
        const relative_path = try std.fmt.allocPrint(alloc, "trace-{x}.voprtrace", .{entry.key.trace_digest});
        artifacts[entry_index] = .{ .trace_digest = entry.key.trace_digest, .path = relative_path };
        artifacts_initialized += 1;
        const full_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ output, relative_path });
        defer alloc.free(full_path);
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = full_path, .data = entry.bytes });
    }
    const quarantined = try alloc.alloc(ManifestQuarantine, corpus.quarantined.items.len);
    defer alloc.free(quarantined);
    var quarantine_initialized: usize = 0;
    defer for (quarantined[0..quarantine_initialized]) |item| alloc.free(item.path);
    for (corpus.quarantined.items, 0..) |entry, entry_index| {
        const relative_path = try std.fmt.allocPrint(
            alloc,
            "quarantine/{s}-{x}.voprquarantine",
            .{ @tagName(entry.reason), entry.trace_digest },
        );
        quarantined[entry_index] = .{
            .trace_digest = entry.trace_digest,
            .reason = entry.reason,
            .path = relative_path,
        };
        quarantine_initialized += 1;
        const full_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ output, relative_path });
        defer alloc.free(full_path);
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = full_path, .data = entry.bytes });
    }
    const manifest = try std.json.Stringify.valueAlloc(alloc, .{
        .format = "vopr-corpus-merge-v1",
        .scenario = compatibility.scenario,
        .scenario_version = compatibility.scenario_version,
        .backend_ids = compatibility.backend_ids,
        .report = merge_report,
        .artifacts = artifacts,
        .quarantine = quarantined,
        .property_history = corpus.property_history.items,
    }, .{ .whitespace = .indent_2 });
    defer alloc.free(manifest);
    // The manifest is the completion marker and is written after every
    // referenced retained/quarantined byte artifact is durable to the API.
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = manifest_path, .data = manifest });
    std.debug.print("VOPR corpus merge candidates={d} retained={d} duplicates={d} quarantined={d} out={s}\n", .{
        merge_report.candidates,
        merge_report.inserted,
        merge_report.duplicates,
        merge_report.quarantined,
        output,
    });
}

fn indexCommand(alloc: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var index_path: ?[]const u8 = null;
    var json_path: ?[]const u8 = null;
    var html_path: ?[]const u8 = null;
    var additions: std.ArrayListUnmanaged([]const u8) = .empty;
    defer additions.deinit(alloc);
    var query: vopr.run_index.Query = .{};
    var arg_index: usize = 0;
    while (arg_index < args.len) {
        const arg = args[arg_index];
        if (std.mem.eql(u8, arg, "--index")) {
            index_path = try nextValue(args, &arg_index);
        } else if (std.mem.eql(u8, arg, "--add")) {
            try additions.append(alloc, try nextValue(args, &arg_index));
        } else if (std.mem.eql(u8, arg, "--json-out")) {
            json_path = try nextValue(args, &arg_index);
        } else if (std.mem.eql(u8, arg, "--html-out")) {
            html_path = try nextValue(args, &arg_index);
        } else if (std.mem.eql(u8, arg, "--run-id")) {
            query.run_id = try nextValue(args, &arg_index);
        } else if (std.mem.eql(u8, arg, "--revision")) {
            query.source_revision = try nextValue(args, &arg_index);
        } else if (std.mem.eql(u8, arg, "--scenario")) {
            query.scenario = try nextValue(args, &arg_index);
        } else if (std.mem.eql(u8, arg, "--property")) {
            query.property_id = try std.fmt.parseInt(u64, try nextValue(args, &arg_index), 0);
        } else if (std.mem.eql(u8, arg, "--property-name")) {
            query.property_name = try nextValue(args, &arg_index);
        } else if (std.mem.eql(u8, arg, "--fingerprint")) {
            query.fingerprint = try std.fmt.parseInt(u64, try nextValue(args, &arg_index), 0);
        } else if (std.mem.eql(u8, arg, "--corpus")) {
            const value = try nextValue(args, &arg_index);
            query.corpus_state = if (std.mem.eql(u8, value, "retained"))
                .retained
            else if (std.mem.eql(u8, value, "quarantined"))
                .quarantined
            else
                return error.InvalidCorpusState;
        } else if (std.mem.eql(u8, arg, "--artifact-contains")) {
            query.artifact_contains = try nextValue(args, &arg_index);
        } else if (std.mem.eql(u8, arg, "--min-transitions")) {
            query.min_transitions_consumed = try std.fmt.parseInt(u64, try nextValue(args, &arg_index), 10);
        } else if (std.mem.eql(u8, arg, "--max-transitions")) {
            query.max_transitions_consumed = try std.fmt.parseInt(u64, try nextValue(args, &arg_index), 10);
        } else if (std.mem.eql(u8, arg, "--min-resources")) {
            query.min_resources_consumed = try std.fmt.parseInt(u64, try nextValue(args, &arg_index), 10);
        } else if (std.mem.eql(u8, arg, "--max-resources")) {
            query.max_resources_consumed = try std.fmt.parseInt(u64, try nextValue(args, &arg_index), 10);
        } else if (std.mem.eql(u8, arg, "--min-histories")) {
            query.min_histories_consumed = try std.fmt.parseInt(u64, try nextValue(args, &arg_index), 10);
        } else if (std.mem.eql(u8, arg, "--limit")) {
            query.limit = try std.fmt.parseInt(usize, try nextValue(args, &arg_index), 10);
        } else return error.UnknownArgument;
        arg_index += 1;
    }
    const persistent_path = index_path orelse return error.RunIndexPathRequired;
    if (additions.items.len == 0 and json_path == null and html_path == null)
        return error.RunIndexActionRequired;
    try query.validate();
    var index = vopr.run_index.Index.init(alloc);
    defer index.deinit();
    const existing = std.Io.Dir.cwd().readFileAlloc(io, persistent_path, alloc, .limited(max_trace_bytes)) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
    if (existing) |bytes| {
        defer alloc.free(bytes);
        _ = try index.ingestJson(bytes);
    }
    var added: u64 = 0;
    for (additions.items) |path| {
        const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(max_trace_bytes));
        defer alloc.free(bytes);
        added +|= try index.ingestJson(bytes);
    }
    if (additions.items.len > 0) {
        const bytes = try index.renderJsonAlloc(alloc);
        defer alloc.free(bytes);
        try writeAtomicArtifact(alloc, io, persistent_path, bytes);
    }
    if (json_path) |path| {
        const bytes = try index.renderQueryJsonAlloc(alloc, query);
        defer alloc.free(bytes);
        try writeDebugArtifact(io, path, bytes);
    }
    if (html_path) |path| {
        const bytes = try index.renderQueryHtmlAlloc(alloc, query);
        defer alloc.free(bytes);
        try writeDebugArtifact(io, path, bytes);
    }
    std.debug.print(
        "VOPR run index runs={d} added={d} properties={d} fingerprints={d} artifacts={d} index={s} json={s} html={s}\n",
        .{
            index.runs.items.len,
            added,
            index.properties.items.len,
            index.fingerprints.items.len,
            index.artifacts.items.len,
            persistent_path,
            json_path orelse "-",
            html_path orelse "-",
        },
    );
}

fn replayKnownScenario(alloc: std.mem.Allocator, recorded: *const vopr.trace.Trace) !vopr.trace.Trace {
    if (std.mem.eql(u8, recorded.header.scenario, "metadata-vopr"))
        return antfly.metadata_sim_harness.replayMetadataVoprCampaign(alloc, recorded);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.metadata_sim_harness.DistributedDataVoprScenario.name))
        return antfly.metadata_sim_harness.replayDistributedDataVoprCampaign(alloc, recorded);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.transaction_vopr.Scenario.name))
        return antfly.transaction_vopr.replay(alloc, recorded);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.wal_vopr.CliScenario.name))
        return antfly.wal_vopr.replay(alloc, recorded);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.persistent_vopr.CliScenario.name))
        return antfly.persistent_vopr.replay(alloc, recorded);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.index_manager_vopr.CliScenario.name))
        return antfly.index_manager_vopr.replay(alloc, recorded);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.db_split_vopr.CliScenario.name))
        return antfly.db_split_vopr.replay(alloc, recorded);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.raft_vopr.CliScenario.name))
        return antfly.raft_vopr.replay(alloc, recorded);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.lmdb_vopr.CliScenario.name))
        return antfly.lmdb_vopr.replay(alloc, recorded);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.lsm_vopr.CliScenario.name))
        return antfly.lsm_vopr.replay(alloc, recorded);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.ha_vopr.CliScenario.name))
        return antfly.ha_vopr.replay(alloc, recorded);
    if (antfly.domain_vopr.kindFromArtifact(recorded) != null)
        return antfly.domain_vopr.replayKnown(alloc, recorded);
    return error.UnsupportedScenario;
}

fn runContextFreeWithChoices(
    comptime Scenario: type,
    alloc: std.mem.Allocator,
    recorded: *const vopr.trace.Trace,
    source: vopr.choice.Source,
) !vopr.trace.Trace {
    return runContextFreeWithChoicesAndRecorder(Scenario, alloc, recorded, source, null);
}

fn runContextFreeWithChoicesAndRecorder(
    comptime Scenario: type,
    alloc: std.mem.Allocator,
    recorded: *const vopr.trace.Trace,
    source: vopr.choice.Source,
    recorder: ?*vopr.flight_recorder.Recorder,
) !vopr.trace.Trace {
    return vopr.runner.run(Scenario, alloc, source, .{
        .system = recorded.header.system,
        .seed = recorded.config.seed,
        .transition_budget = recorded.config.transition_budget,
        .resource_budget = recorded.config.resource_budget,
        .fixture_hashes = recorded.config.fixture_hashes,
        .feature_flags = recorded.config.feature_flags,
        .backend_ids = recorded.config.backend_ids,
        .scenario_parameters = recorded.config.scenario_parameters,
        .source_revision = recorded.header.source_revision,
        .target = recorded.header.target,
        .optimize = recorded.header.optimize,
        .flight_recorder = recorder,
    });
}

fn runKnownScenarioWithChoices(
    alloc: std.mem.Allocator,
    recorded: *const vopr.trace.Trace,
    source: vopr.choice.Source,
) !vopr.trace.Trace {
    return runKnownScenarioWithChoicesAndRecorder(alloc, recorded, source, null);
}

fn runKnownScenarioWithChoicesAndRecorder(
    alloc: std.mem.Allocator,
    recorded: *const vopr.trace.Trace,
    source: vopr.choice.Source,
    recorder: ?*vopr.flight_recorder.Recorder,
) !vopr.trace.Trace {
    if (std.mem.eql(u8, recorded.header.scenario, "metadata-vopr")) {
        const cfg = try antfly.metadata_sim_harness.MetadataVoprCampaignConfig.fromTrace(recorded);
        return antfly.metadata_sim_harness.runMetadataVoprCampaignWithChoicesAndRecorder(alloc, cfg, source, recorder);
    }
    if (std.mem.eql(u8, recorded.header.scenario, antfly.metadata_sim_harness.DistributedDataVoprScenario.name)) {
        const cfg = try antfly.metadata_sim_harness.DistributedDataVoprCampaignConfig.fromTrace(recorded);
        return antfly.metadata_sim_harness.runDistributedDataVoprCampaignWithChoices(alloc, cfg, source, recorder);
    }
    if (std.mem.eql(u8, recorded.header.scenario, antfly.transaction_vopr.Scenario.name))
        return runContextFreeWithChoicesAndRecorder(antfly.transaction_vopr.Scenario, alloc, recorded, source, recorder);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.wal_vopr.CliScenario.name))
        return runContextFreeWithChoicesAndRecorder(antfly.wal_vopr.CliScenario, alloc, recorded, source, recorder);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.persistent_vopr.CliScenario.name))
        return runContextFreeWithChoicesAndRecorder(antfly.persistent_vopr.CliScenario, alloc, recorded, source, recorder);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.index_manager_vopr.CliScenario.name))
        return runContextFreeWithChoicesAndRecorder(antfly.index_manager_vopr.CliScenario, alloc, recorded, source, recorder);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.db_split_vopr.CliScenario.name))
        return runContextFreeWithChoicesAndRecorder(antfly.db_split_vopr.CliScenario, alloc, recorded, source, recorder);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.raft_vopr.CliScenario.name))
        return runContextFreeWithChoicesAndRecorder(antfly.raft_vopr.CliScenario, alloc, recorded, source, recorder);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.lmdb_vopr.CliScenario.name))
        return runContextFreeWithChoicesAndRecorder(antfly.lmdb_vopr.CliScenario, alloc, recorded, source, recorder);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.lsm_vopr.CliScenario.name))
        return runContextFreeWithChoicesAndRecorder(antfly.lsm_vopr.CliScenario, alloc, recorded, source, recorder);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.ha_vopr.CliScenario.name))
        return runContextFreeWithChoicesAndRecorder(antfly.ha_vopr.CliScenario, alloc, recorded, source, recorder);
    if (antfly.domain_vopr.kindFromArtifact(recorded) != null) {
        return antfly.domain_vopr.runKnownWithChoicesAndRecorder(alloc, recorded, source, recorder);
    }
    return error.UnsupportedScenario;
}

fn replayKnownScenarioWithRecorder(
    alloc: std.mem.Allocator,
    recorded: *const vopr.trace.Trace,
    recorder: *vopr.flight_recorder.Recorder,
) !vopr.trace.Trace {
    var source = vopr.choice.Replay{ .records = recorded.choices.items };
    var replayed = try runKnownScenarioWithChoicesAndRecorder(alloc, recorded, source.source(), recorder);
    errdefer replayed.deinit();
    const expected = try recorded.renderAlloc(alloc);
    defer alloc.free(expected);
    const actual = try replayed.renderAlloc(alloc);
    defer alloc.free(actual);
    if (!std.mem.eql(u8, expected, actual)) return error.VoprReplayArtifactDiverged;
    return replayed;
}

fn recipeFlightReplayKnown(
    _: ?*anyopaque,
    alloc: std.mem.Allocator,
    recorded: *const vopr.trace.Trace,
    recorder: *vopr.flight_recorder.Recorder,
) !vopr.trace.Trace {
    return replayKnownScenarioWithRecorder(alloc, recorded, recorder);
}

fn counterfactualRunKnown(
    _: ?*anyopaque,
    alloc: std.mem.Allocator,
    recorded: *const vopr.trace.Trace,
    source: vopr.choice.Source,
) !vopr.trace.Trace {
    return runKnownScenarioWithChoices(alloc, recorded, source);
}

fn counterfactualReplayKnown(
    _: ?*anyopaque,
    alloc: std.mem.Allocator,
    recorded: *const vopr.trace.Trace,
) !vopr.trace.Trace {
    return replayKnownScenario(alloc, recorded);
}

fn recipeCollectKnown(
    _: ?*anyopaque,
    alloc: std.mem.Allocator,
    recorded: *const vopr.trace.Trace,
    prefix: usize,
) !vopr.collector.Sink {
    return collectKnownAt(alloc, recorded, prefix);
}

fn runDebugRecipeKnown(
    alloc: std.mem.Allocator,
    recorded: *const vopr.trace.Trace,
    failure_ordinal: usize,
    attempts: u64,
    flight_config: vopr.debug_recipe.FlightConfig,
) !vopr.debug_recipe.Package {
    const queries = [_]vopr.debug_recipe.QuerySpec{
        .{ .name = "injected-errors", .query = .{ .selector = .{ .kind = .injected_error } } },
        .{ .name = "client-responses", .query = .{ .selector = .{ .kind = .client_response } } },
        .{ .name = "fault-starts", .query = .{ .selector = .{ .kind = .fault_started } } },
    };
    const execution = vopr.reducer.ExecutionRunner{
        .run_fn = counterfactualRunKnown,
        .replay_fn = counterfactualReplayKnown,
    };
    return vopr.debug_recipe.runWithRunner(alloc, recorded, .{
        .failure_ordinal = failure_ordinal,
        .reduction = .{ .max_attempts = attempts },
        .counterfactual = .{
            .prefix_window = 8,
            .descendants_per_alternative = 2,
            .max_experiments = 16,
            .suffix_seed = recorded.config.seed orelse 0,
        },
        .event_queries = &queries,
        .collect_failure_window = antfly.domain_vopr.kindFromArtifact(recorded) != null,
        .flight = flight_config,
    }, .{
        .execution = execution,
        .collectors = if (antfly.domain_vopr.kindFromArtifact(recorded) != null)
            .{ .collect_fn = recipeCollectKnown }
        else
            null,
        .flight_replay_fn = recipeFlightReplayKnown,
    });
}

fn declarationsKnown(recorded: *const vopr.trace.Trace) []const vopr.property.Declaration {
    if (std.mem.eql(u8, recorded.header.scenario, antfly.metadata_sim_harness.DistributedDataVoprScenario.name))
        return antfly.metadata_sim_harness.DistributedDataVoprScenario.properties;
    if (std.mem.eql(u8, recorded.header.scenario, antfly.transaction_vopr.Scenario.name))
        return antfly.transaction_vopr.Scenario.properties;
    if (std.mem.eql(u8, recorded.header.scenario, antfly.wal_vopr.CliScenario.name))
        return antfly.wal_vopr.CliScenario.properties;
    if (std.mem.eql(u8, recorded.header.scenario, antfly.persistent_vopr.CliScenario.name))
        return antfly.persistent_vopr.CliScenario.properties;
    if (std.mem.eql(u8, recorded.header.scenario, antfly.index_manager_vopr.CliScenario.name))
        return antfly.index_manager_vopr.CliScenario.properties;
    if (std.mem.eql(u8, recorded.header.scenario, antfly.db_split_vopr.CliScenario.name))
        return antfly.db_split_vopr.CliScenario.properties;
    if (std.mem.eql(u8, recorded.header.scenario, antfly.raft_vopr.CliScenario.name))
        return antfly.raft_vopr.CliScenario.properties;
    if (std.mem.eql(u8, recorded.header.scenario, antfly.lmdb_vopr.CliScenario.name))
        return antfly.lmdb_vopr.CliScenario.properties;
    if (std.mem.eql(u8, recorded.header.scenario, antfly.lsm_vopr.CliScenario.name))
        return antfly.lsm_vopr.CliScenario.properties;
    if (std.mem.eql(u8, recorded.header.scenario, antfly.ha_vopr.CliScenario.name))
        return antfly.ha_vopr.CliScenario.properties;
    if (antfly.domain_vopr.kindFromArtifact(recorded)) |kind| return switch (kind) {
        .distributed_transaction => antfly.domain_vopr.DistributedTransactionScenario.properties,
        .data_plane => antfly.domain_vopr.DataPlaneScenario.properties,
        .derived_workflow => antfly.domain_vopr.DerivedWorkflowScenario.properties,
        .backup_restore => antfly.domain_vopr.BackupRestoreScenario.properties,
        .clock_fault => antfly.domain_vopr.ClockLeaseTtlScenario.properties,
    };
    // The metadata harness declares its operation properties dynamically from
    // trace parameters, so the generic report derives the complete encountered
    // catalog from canonical property records for that one scenario.
    return &.{};
}

fn defaultCampaignTransitions(scenario: []const u8) !usize {
    if (std.mem.eql(u8, scenario, "metadata")) return 64;
    if (std.mem.eql(u8, scenario, "distributed-data")) return 4;
    if (std.mem.eql(u8, scenario, "transaction")) return 3;
    if (std.mem.eql(u8, scenario, "wal")) return 25;
    if (std.mem.eql(u8, scenario, "persistent")) return 17;
    if (std.mem.eql(u8, scenario, "index-manager")) return 11;
    if (std.mem.eql(u8, scenario, "db-split")) return 12;
    if (std.mem.eql(u8, scenario, "raft")) return 33;
    if (std.mem.eql(u8, scenario, "lmdb")) return 13;
    if (std.mem.eql(u8, scenario, "lsm")) return 49;
    if (std.mem.eql(u8, scenario, "ha")) return 33;
    if (antfly.domain_vopr.kindFromCliName(scenario)) |kind| return kind.transitionBudget();
    return error.UnsupportedScenario;
}

fn validateCampaignTransitions(scenario: []const u8, transitions: usize) !void {
    if (std.mem.eql(u8, scenario, "metadata")) return;
    if (transitions != try defaultCampaignTransitions(scenario)) return error.ScenarioRequiresFixedTransitionBudget;
}

fn recordCampaignScenario(
    alloc: std.mem.Allocator,
    scenario: []const u8,
    seed: u64,
    transitions: usize,
    campaign_seed: u64,
) !vopr.trace.Trace {
    if (std.mem.eql(u8, scenario, "metadata")) {
        const base_id = 10_000 + campaign_seed % 1_000_000;
        return antfly.metadata_sim_harness.recordMetadataVoprCampaign(alloc, .{
            .seed = seed,
            .operation_count = transitions,
            .metadata_group_id = base_id,
            .table_id = base_id + 1,
            .range_group_id = base_id + 2,
            .split_group_id = base_id + 3,
            .split_transition_id = base_id + 4,
        });
    }
    if (std.mem.eql(u8, scenario, "distributed-data"))
        return antfly.metadata_sim_harness.recordDistributedDataVoprCampaign(alloc, .{ .seed = seed, .table_id = 10_000 + campaign_seed % 100_000 });
    if (std.mem.eql(u8, scenario, "transaction")) return antfly.transaction_vopr.record(alloc, seed);
    if (std.mem.eql(u8, scenario, "wal")) return antfly.wal_vopr.record(alloc, seed);
    if (std.mem.eql(u8, scenario, "persistent")) return antfly.persistent_vopr.record(alloc, seed);
    if (std.mem.eql(u8, scenario, "index-manager")) return antfly.index_manager_vopr.record(alloc, seed);
    if (std.mem.eql(u8, scenario, "db-split")) return antfly.db_split_vopr.record(alloc, seed);
    if (std.mem.eql(u8, scenario, "raft")) return antfly.raft_vopr.record(alloc, seed);
    if (std.mem.eql(u8, scenario, "lmdb")) return antfly.lmdb_vopr.record(alloc, seed);
    if (std.mem.eql(u8, scenario, "lsm")) return antfly.lsm_vopr.record(alloc, seed);
    if (std.mem.eql(u8, scenario, "ha")) return antfly.ha_vopr.record(alloc, seed);
    if (antfly.domain_vopr.kindFromCliName(scenario) != null) return antfly.domain_vopr.recordNamed(alloc, scenario, seed);
    return error.UnsupportedScenario;
}

fn artifactMatchesScenario(artifact: *const vopr.trace.Trace, scenario: []const u8) bool {
    if (std.mem.eql(u8, scenario, "metadata")) return std.mem.eql(u8, artifact.header.scenario, "metadata-vopr");
    if (std.mem.eql(u8, scenario, "distributed-data")) return std.mem.eql(u8, artifact.header.scenario, antfly.metadata_sim_harness.DistributedDataVoprScenario.name);
    if (std.mem.eql(u8, scenario, "transaction")) return std.mem.eql(u8, artifact.header.scenario, antfly.transaction_vopr.Scenario.name);
    if (std.mem.eql(u8, scenario, "wal")) return std.mem.eql(u8, artifact.header.scenario, antfly.wal_vopr.CliScenario.name);
    if (std.mem.eql(u8, scenario, "persistent")) return std.mem.eql(u8, artifact.header.scenario, antfly.persistent_vopr.CliScenario.name);
    if (std.mem.eql(u8, scenario, "index-manager")) return std.mem.eql(u8, artifact.header.scenario, antfly.index_manager_vopr.CliScenario.name);
    if (std.mem.eql(u8, scenario, "db-split")) return std.mem.eql(u8, artifact.header.scenario, antfly.db_split_vopr.CliScenario.name);
    if (std.mem.eql(u8, scenario, "raft")) return std.mem.eql(u8, artifact.header.scenario, antfly.raft_vopr.CliScenario.name);
    if (std.mem.eql(u8, scenario, "lmdb")) return std.mem.eql(u8, artifact.header.scenario, antfly.lmdb_vopr.CliScenario.name);
    if (std.mem.eql(u8, scenario, "lsm")) return std.mem.eql(u8, artifact.header.scenario, antfly.lsm_vopr.CliScenario.name);
    if (std.mem.eql(u8, scenario, "ha")) return std.mem.eql(u8, artifact.header.scenario, antfly.ha_vopr.CliScenario.name);
    if (antfly.domain_vopr.kindFromCliName(scenario) != null) return antfly.domain_vopr.artifactMatchesCliName(artifact, scenario);
    return false;
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
    else if (std.mem.eql(u8, recorded.header.scenario, antfly.domain_vopr.DistributedTransactionScenario.name))
        try antfly.domain_vopr.replayDistributedTransactionToTrace(alloc, &recorded, &ndjson.writer)
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

fn collectKnownAt(
    alloc: std.mem.Allocator,
    recorded: *const vopr.trace.Trace,
    prefix: usize,
) !vopr.collector.Sink {
    const kind = antfly.domain_vopr.kindFromArtifact(recorded) orelse
        return error.ScenarioCollectorsUnsupported;
    return switch (kind) {
        .distributed_transaction => vopr.debugger.collectAt(antfly.domain_vopr.DistributedTransactionScenario, alloc, recorded, prefix),
        .data_plane => vopr.debugger.collectAt(antfly.domain_vopr.DataPlaneScenario, alloc, recorded, prefix),
        .derived_workflow => vopr.debugger.collectAt(antfly.domain_vopr.DerivedWorkflowScenario, alloc, recorded, prefix),
        .backup_restore => vopr.debugger.collectAt(antfly.domain_vopr.BackupRestoreScenario, alloc, recorded, prefix),
        .clock_fault => vopr.debugger.collectAt(antfly.domain_vopr.ClockLeaseTtlScenario, alloc, recorded, prefix),
    };
}

fn writeDebugArtifact(io: std.Io, path: []const u8, bytes: []const u8) !void {
    if (std.fs.path.dirname(path)) |parent| try ensureDir(io, parent);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = bytes });
}

fn writeAtomicArtifact(alloc: std.mem.Allocator, io: std.Io, path: []const u8, bytes: []const u8) !void {
    if (std.fs.path.dirname(path)) |parent| try ensureDir(io, parent);
    const temporary = try std.fmt.allocPrint(alloc, "{s}.tmp-vopr-index", .{path});
    defer alloc.free(temporary);
    errdefer std.Io.Dir.cwd().deleteFile(io, temporary) catch {};
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = temporary, .data = bytes });
    try std.Io.Dir.rename(std.Io.Dir.cwd(), temporary, std.Io.Dir.cwd(), path, io);
}

const DebugSession = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    recorded: *const vopr.trace.Trace,
    prefix: usize,

    /// Commands are deliberately line-oriented so the exact same debugger is
    /// usable from a terminal, CI recipe, or editor integration.
    fn execute(self: *@This(), line_raw: []const u8) !bool {
        const line = std.mem.trim(u8, line_raw, " \t\r\n");
        if (line.len == 0 or line[0] == '#') return true;
        var tokens = std.mem.tokenizeAny(u8, line, " \t");
        const command = tokens.next() orelse return true;
        if (std.mem.eql(u8, command, "quit") or std.mem.eql(u8, command, "exit")) return false;
        if (std.mem.eql(u8, command, "help")) {
            std.debug.print(
                "show <out> | seek <prefix> [out] | causal <failure> <out> | causal-window <failure> <prefix-window> <descendants> <experiments> <suffix-seed> <out> | collector <prefix> <out> | branch <choice> <replacement-id> <suffix-seed> <child-trace> <comparison-out> | compare <child-trace> <out> | quit\n",
                .{},
            );
            return true;
        }
        if (std.mem.eql(u8, command, "show") or std.mem.eql(u8, command, "seek")) {
            if (std.mem.eql(u8, command, "seek")) {
                self.prefix = try std.fmt.parseInt(usize, tokens.next() orelse return error.MissingDebuggerArgument, 10);
                if (self.prefix > self.recorded.choices.items.len) return error.DebuggerPrefixOutOfRange;
            }
            if (tokens.next()) |output| {
                const snapshot = try vopr.debugger.inspectAlloc(self.alloc, self.recorded, self.prefix);
                defer self.alloc.free(snapshot);
                try writeDebugArtifact(self.io, output, snapshot);
            } else std.debug.print("VOPR debugger prefix={d}/{d}\n", .{ self.prefix, self.recorded.choices.items.len });
            return true;
        }
        if (std.mem.eql(u8, command, "causal")) {
            const ordinal = try std.fmt.parseInt(usize, tokens.next() orelse return error.MissingDebuggerArgument, 10);
            const output = tokens.next() orelse return error.MissingDebuggerArgument;
            var report_value = try vopr.causal.analyzeAlloc(self.alloc, self.recorded, ordinal);
            defer report_value.deinit();
            const encoded = try report_value.renderAlloc(self.alloc);
            defer self.alloc.free(encoded);
            try writeDebugArtifact(self.io, output, encoded);
            return true;
        }
        if (std.mem.eql(u8, command, "causal-window")) {
            const ordinal = try std.fmt.parseInt(usize, tokens.next() orelse return error.MissingDebuggerArgument, 10);
            const prefix_window = try std.fmt.parseInt(usize, tokens.next() orelse return error.MissingDebuggerArgument, 10);
            const descendants = try std.fmt.parseInt(usize, tokens.next() orelse return error.MissingDebuggerArgument, 10);
            const experiments = try std.fmt.parseInt(usize, tokens.next() orelse return error.MissingDebuggerArgument, 10);
            const suffix_seed = try std.fmt.parseInt(u64, tokens.next() orelse return error.MissingDebuggerArgument, 0);
            const output = tokens.next() orelse return error.MissingDebuggerArgument;
            var report_value = try vopr.causal.analyzeCounterfactualWithRunner(
                self.alloc,
                self.recorded,
                ordinal,
                .{ .prefix_window = prefix_window, .descendants_per_alternative = descendants, .max_experiments = experiments, .suffix_seed = suffix_seed },
                .{ .run_fn = counterfactualRunKnown, .replay_fn = counterfactualReplayKnown },
            );
            defer report_value.deinit();
            const encoded = try report_value.renderAlloc(self.alloc);
            defer self.alloc.free(encoded);
            try writeDebugArtifact(self.io, output, encoded);
            return true;
        }
        if (std.mem.eql(u8, command, "collector")) {
            const prefix = try std.fmt.parseInt(usize, tokens.next() orelse return error.MissingDebuggerArgument, 10);
            const output = tokens.next() orelse return error.MissingDebuggerArgument;
            var sink = try collectKnownAt(self.alloc, self.recorded, prefix);
            defer sink.deinit();
            const encoded = try sink.renderAlloc(self.alloc);
            defer self.alloc.free(encoded);
            try writeDebugArtifact(self.io, output, encoded);
            return true;
        }
        if (std.mem.eql(u8, command, "branch")) {
            const choice_index = try std.fmt.parseInt(usize, tokens.next() orelse return error.MissingDebuggerArgument, 10);
            const replacement = try std.fmt.parseInt(vopr.id.StableId, tokens.next() orelse return error.MissingDebuggerArgument, 0);
            const suffix_seed = try std.fmt.parseInt(u64, tokens.next() orelse return error.MissingDebuggerArgument, 0);
            const child_output = tokens.next() orelse return error.MissingDebuggerArgument;
            const comparison_output = tokens.next() orelse return error.MissingDebuggerArgument;
            if (choice_index >= self.recorded.choices.items.len) return error.DebuggerPrefixOutOfRange;
            const choice_record = self.recorded.choices.items[choice_index];
            if (std.mem.indexOfScalar(vopr.id.StableId, choice_record.enabled_ids, replacement) == null)
                return error.DebuggerAlternativeNotEnabled;
            var source = vopr.choice.Mutating.init(self.recorded.choices.items, choice_index, replacement, suffix_seed);
            var child = try runKnownScenarioWithChoices(self.alloc, self.recorded, source.source());
            defer child.deinit();
            var replayed = try replayKnownScenario(self.alloc, &child);
            replayed.deinit();
            const child_bytes = try child.renderAlloc(self.alloc);
            defer self.alloc.free(child_bytes);
            try writeDebugArtifact(self.io, child_output, child_bytes);
            const comparison = try vopr.debugger.compareAlloc(self.alloc, self.recorded, &child);
            defer self.alloc.free(comparison);
            try writeDebugArtifact(self.io, comparison_output, comparison);
            return true;
        }
        if (std.mem.eql(u8, command, "compare")) {
            const child_path = tokens.next() orelse return error.MissingDebuggerArgument;
            const output = tokens.next() orelse return error.MissingDebuggerArgument;
            const child_bytes = try std.Io.Dir.cwd().readFileAlloc(self.io, child_path, self.alloc, .limited(max_trace_bytes));
            defer self.alloc.free(child_bytes);
            var child = try vopr.trace.parseAlloc(self.alloc, child_bytes);
            defer child.deinit();
            var replayed = try replayKnownScenario(self.alloc, &child);
            replayed.deinit();
            const comparison = try vopr.debugger.compareAlloc(self.alloc, self.recorded, &child);
            defer self.alloc.free(comparison);
            try writeDebugArtifact(self.io, output, comparison);
            return true;
        }
        return error.UnknownDebuggerCommand;
    }
};

fn debugCommand(alloc: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var trace_path: ?[]const u8 = null;
    var output_path: ?[]const u8 = null;
    var commands_path: ?[]const u8 = null;
    var interactive = false;
    var prefix: usize = 0;
    var index: usize = 0;
    while (index < args.len) {
        if (std.mem.eql(u8, args[index], "--trace")) {
            trace_path = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--out")) {
            output_path = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--prefix")) {
            prefix = try std.fmt.parseInt(usize, try nextValue(args, &index), 10);
        } else if (std.mem.eql(u8, args[index], "--commands")) {
            commands_path = try nextValue(args, &index);
        } else if (std.mem.eql(u8, args[index], "--interactive")) {
            interactive = true;
        } else return error.UnknownArgument;
        index += 1;
    }
    const input = trace_path orelse return error.TracePathRequired;
    const encoded = try std.Io.Dir.cwd().readFileAlloc(io, input, alloc, .limited(max_trace_bytes));
    defer alloc.free(encoded);
    var recorded = try vopr.trace.parseAlloc(alloc, encoded);
    defer recorded.deinit();
    var replayed = try replayKnownScenario(alloc, &recorded);
    replayed.deinit();
    var session = DebugSession{ .alloc = alloc, .io = io, .recorded = &recorded, .prefix = prefix };
    if (output_path) |output| {
        const snapshot = try vopr.debugger.inspectAlloc(alloc, &recorded, prefix);
        defer alloc.free(snapshot);
        try writeDebugArtifact(io, output, snapshot);
        std.debug.print("VOPR debug snapshot prefix={d} out={s}\n", .{ prefix, output });
    }
    if (commands_path) |path| {
        const commands = try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(1024 * 1024));
        defer alloc.free(commands);
        var lines = std.mem.splitScalar(u8, commands, '\n');
        while (lines.next()) |line| if (!try session.execute(line)) break;
    }
    if (interactive) {
        var stdin_buffer: [64 * 1024]u8 = undefined;
        var stdin_reader = std.Io.File.stdin().readerStreaming(io, &stdin_buffer);
        const stdin = &stdin_reader.interface;
        std.debug.print("VOPR debugger ready; type help or quit\n", .{});
        while (true) {
            std.debug.print("vopr[{d}]> ", .{session.prefix});
            const line = try stdin.takeDelimiter('\n') orelse break;
            if (!try session.execute(line)) break;
        }
    }
    if (output_path == null and commands_path == null and !interactive) return error.DebuggerActionRequired;
}

fn campaignCommand(alloc: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var histories: u64 = 100;
    var requested_transitions: ?usize = null;
    var workers: usize = 1;
    var seed: u64 = 0xa17f_1000;
    var scenario: []const u8 = "metadata";
    var requested_artifact_dir: ?[]const u8 = null;
    var index: usize = 0;
    while (index < args.len) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--scenario")) {
            scenario = try nextValue(args, &index);
            _ = try defaultCampaignTransitions(scenario);
        } else if (std.mem.eql(u8, arg, "--histories")) {
            histories = try std.fmt.parseInt(u64, try nextValue(args, &index), 10);
        } else if (std.mem.eql(u8, arg, "--transitions")) {
            requested_transitions = try std.fmt.parseInt(usize, try nextValue(args, &index), 10);
        } else if (std.mem.eql(u8, arg, "--workers")) {
            workers = try std.fmt.parseInt(usize, try nextValue(args, &index), 10);
        } else if (std.mem.eql(u8, arg, "--seed")) {
            seed = try std.fmt.parseInt(u64, try nextValue(args, &index), 0);
        } else if (std.mem.eql(u8, arg, "--artifact-dir")) {
            requested_artifact_dir = try nextValue(args, &index);
        } else return error.UnknownArgument;
        index += 1;
    }
    const transitions = requested_transitions orelse try defaultCampaignTransitions(scenario);
    const owned_artifact_dir = if (requested_artifact_dir == null)
        try std.fmt.allocPrint(alloc, "/tmp/antfly-vopr/{s}", .{scenario})
    else
        null;
    defer if (owned_artifact_dir) |path| alloc.free(path);
    const artifact_dir = requested_artifact_dir orelse owned_artifact_dir.?;
    if (histories == 0 or transitions == 0 or workers == 0) return error.InvalidCampaignBudget;
    try validateCampaignTransitions(scenario, transitions);
    try ensureDir(io, artifact_dir);

    var context = CampaignContext{
        .io = io,
        .histories = histories,
        .transitions = transitions,
        .scenario = scenario,
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
    try context.exportQuarantineArtifacts();
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
    if (std.mem.eql(u8, recorded.header.scenario, antfly.wal_vopr.CliScenario.name)) {
        const target = if (recorded.failures.items.len > 0)
            recorded.failures.items[0].fingerprint
        else
            return error.FailingTraceRequired;
        var reduced = try vopr.reducer.reduce(
            antfly.wal_vopr.CliScenario,
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
    if (std.mem.eql(u8, recorded.header.scenario, antfly.persistent_vopr.CliScenario.name))
        return reduceContextFree(antfly.persistent_vopr.CliScenario, alloc, io, output, &recorded, attempts);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.index_manager_vopr.CliScenario.name))
        return reduceContextFree(antfly.index_manager_vopr.CliScenario, alloc, io, output, &recorded, attempts);
    if (std.mem.eql(u8, recorded.header.scenario, antfly.db_split_vopr.CliScenario.name))
        return reduceContextFree(antfly.db_split_vopr.CliScenario, alloc, io, output, &recorded, attempts);
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
    if (std.mem.eql(u8, recorded.header.scenario, antfly.ha_vopr.CliScenario.name)) {
        const target = if (recorded.failures.items.len > 0)
            recorded.failures.items[0].fingerprint
        else
            return error.FailingTraceRequired;
        var reduced = try vopr.reducer.reduce(
            antfly.ha_vopr.CliScenario,
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
    if (antfly.domain_vopr.kindFromArtifact(&recorded) != null) {
        const target = if (recorded.failures.items.len > 0)
            recorded.failures.items[0].fingerprint
        else
            return error.FailingTraceRequired;
        var reduced = try antfly.domain_vopr.reduceKnown(alloc, &recorded, target, .{ .max_attempts = attempts });
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

fn reduceContextFree(
    comptime Scenario: type,
    alloc: std.mem.Allocator,
    io: std.Io,
    output: []const u8,
    recorded: *const vopr.trace.Trace,
    attempts: u64,
) !void {
    const target = if (recorded.failures.items.len > 0)
        recorded.failures.items[0].fingerprint
    else
        return error.FailingTraceRequired;
    var reduced = try vopr.reducer.reduce(Scenario, alloc, recorded, target, .{ .max_attempts = attempts });
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
        return "pkg/antfly/src/vopr/fixtures/metadata";
    if (std.mem.eql(u8, recorded.header.scenario, antfly.metadata_sim_harness.DistributedDataVoprScenario.name))
        return "pkg/antfly/src/vopr/fixtures/distributed-data";
    if (std.mem.eql(u8, recorded.header.scenario, antfly.transaction_vopr.Scenario.name))
        return "pkg/antfly/src/vopr/fixtures/transaction";
    if (std.mem.eql(u8, recorded.header.scenario, antfly.wal_vopr.CliScenario.name))
        return "pkg/antfly/src/vopr/fixtures/wal";
    if (std.mem.eql(u8, recorded.header.scenario, antfly.persistent_vopr.CliScenario.name))
        return "pkg/antfly/src/vopr/fixtures/persistent";
    if (std.mem.eql(u8, recorded.header.scenario, antfly.index_manager_vopr.CliScenario.name))
        return "pkg/antfly/src/vopr/fixtures/index-manager";
    if (std.mem.eql(u8, recorded.header.scenario, antfly.db_split_vopr.CliScenario.name))
        return "pkg/antfly/src/vopr/fixtures/db-split";
    if (std.mem.eql(u8, recorded.header.scenario, antfly.raft_vopr.CliScenario.name))
        return "pkg/antfly/src/vopr/fixtures/raft";
    if (std.mem.eql(u8, recorded.header.scenario, antfly.lmdb_vopr.CliScenario.name))
        return "pkg/antfly/src/vopr/fixtures/lmdb";
    if (std.mem.eql(u8, recorded.header.scenario, antfly.lsm_vopr.CliScenario.name))
        return "pkg/antfly/src/vopr/fixtures/lsm";
    if (std.mem.eql(u8, recorded.header.scenario, antfly.ha_vopr.CliScenario.name))
        return "pkg/antfly/src/vopr/fixtures/ha";
    if (antfly.domain_vopr.kindFromArtifact(recorded)) |kind| return switch (kind) {
        .distributed_transaction => "pkg/antfly/src/vopr/fixtures/distributed-transaction",
        .data_plane => "pkg/antfly/src/vopr/fixtures/data-plane",
        .derived_workflow => "pkg/antfly/src/vopr/fixtures/derived-workflow",
        .backup_restore => "pkg/antfly/src/vopr/fixtures/backup-restore",
        .clock_fault => "pkg/antfly/src/vopr/fixtures/clock-fault",
    };
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
    const filename = try std.fmt.allocPrint(alloc, "{s}.voprtrace", .{name});
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
    scenario: []const u8,
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
    counterfactual_reports: u64 = 0,
    flight_recordings: u64 = 0,
    flight_records: u64 = 0,
    flight_records_dropped: u64 = 0,
    retained_artifacts: std.ArrayListUnmanaged([]u8) = .empty,
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
        for (self.retained_artifacts.items) |path| alloc.free(path);
        self.retained_artifacts.deinit(alloc);
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
            break :blk Candidate{
                .artifact = try recordCampaignScenario(alloc, self.scenario, seed, self.transitions, self.base_seed),
            };
        };
        var artifact = candidate.artifact;
        defer artifact.deinit();

        var recorder = try vopr.flight_recorder.Recorder.init(alloc, 1024);
        defer recorder.deinit();
        var replayed = replayKnownScenarioWithRecorder(alloc, &artifact, &recorder) catch |err| {
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
        const path = try std.fmt.allocPrint(alloc, "{s}/history-{d}-{x}.voprtrace", .{ self.artifact_dir, history_index, seed });
        defer alloc.free(path);
        try std.Io.Dir.cwd().writeFile(self.io, .{ .sub_path = path, .data = bytes });
        try self.mutex.lock(self.io);
        const retained_path = alloc.dupe(u8, path) catch |err| {
            self.mutex.unlock(self.io);
            return err;
        };
        self.retained_artifacts.append(alloc, retained_path) catch |err| {
            alloc.free(retained_path);
            self.mutex.unlock(self.io);
            return err;
        };
        self.mutex.unlock(self.io);
        var flight = try recorder.materialize(alloc, if (failed) .failure else .novelty);
        defer flight.deinit();
        const flight_bytes = try flight.renderJsonAlloc(alloc);
        defer alloc.free(flight_bytes);
        const flight_path = try std.fmt.allocPrint(alloc, "{s}/history-{d}-{x}.flight.json", .{ self.artifact_dir, history_index, seed });
        defer alloc.free(flight_path);
        try std.Io.Dir.cwd().writeFile(self.io, .{ .sub_path = flight_path, .data = flight_bytes });
        try self.mutex.lock(self.io);
        self.flight_recordings += 1;
        self.flight_records +|= flight.records.len;
        self.flight_records_dropped +|= flight.dropped;
        self.mutex.unlock(self.io);
        if (failed) {
            var new_failure_ordinals: std.ArrayListUnmanaged(usize) = .empty;
            defer new_failure_ordinals.deinit(alloc);
            {
                try self.mutex.lock(self.io);
                defer self.mutex.unlock(self.io);
                try self.recordFailureArtifactsLocked(&artifact, history_index, path, &new_failure_ordinals);
            }
            for (new_failure_ordinals.items) |failure_ordinal| {
                try self.writeFailureDiagnostics(&artifact, failure_ordinal);
                try self.mutex.lock(self.io);
                self.counterfactual_reports += 1;
                self.mutex.unlock(self.io);
            }
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
        new_failure_ordinals: *std.ArrayListUnmanaged(usize),
    ) !void {
        const alloc = std.heap.smp_allocator;
        for (artifact.failures.items, 0..) |failure, failure_ordinal| {
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
                try new_failure_ordinals.append(alloc, failure_ordinal);
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

    fn writeFailureDiagnostics(
        self: *@This(),
        artifact: *const vopr.trace.Trace,
        failure_ordinal: usize,
    ) !void {
        const alloc = std.heap.smp_allocator;
        const failure = artifact.failures.items[failure_ordinal];
        var package = try runDebugRecipeKnown(alloc, artifact, failure_ordinal, 256, .{});
        defer package.deinit();
        const recipe_bytes = try package.renderAlloc(alloc);
        defer alloc.free(recipe_bytes);
        const recipe_path = try std.fmt.allocPrint(
            alloc,
            "{s}/failure-{x}.recipe.json",
            .{ self.artifact_dir, failure.fingerprint },
        );
        defer alloc.free(recipe_path);
        try std.Io.Dir.cwd().writeFile(self.io, .{ .sub_path = recipe_path, .data = recipe_bytes });
        const reduced_bytes = try package.reduced.artifact.renderAlloc(alloc);
        defer alloc.free(reduced_bytes);
        const reduced_path = try std.fmt.allocPrint(
            alloc,
            "{s}/failure-{x}.reduced.voprtrace",
            .{ self.artifact_dir, failure.fingerprint },
        );
        defer alloc.free(reduced_path);
        try std.Io.Dir.cwd().writeFile(self.io, .{
            .sub_path = reduced_path,
            .data = reduced_bytes,
        });
    }

    fn reportSummary(self: *@This()) !void {
        const alloc = std.heap.smp_allocator;
        var productive: usize = 0;
        for (self.corpus.entries.items) |entry| productive += @intFromBool(entry.productive_children > 0);
        std.debug.print(
            "VOPR campaign scenario={s} histories={d} transitions={d} clean={d} failed={d} divergent={d} harness_errors={d} exact_replays={d} seeded={d} quarantined={d} retained={d} states={d} transition_kinds={d} faults_reached={d} workloads_reached={d} productive_inputs={d} splice_attempts={d} spliced={d} splice_rejected={d} counterfactual_reports={d} flight_recordings={d} flight_records={d} flight_dropped={d} workers={d} artifacts={s}\n",
            .{
                self.scenario,
                self.histories,
                self.transitions_executed,
                self.clean_histories,
                self.failures,
                self.replay_divergences,
                self.harness_errors,
                self.exact_replays,
                self.seeded_entries,
                self.corpus.quarantined.items.len,
                self.retained,
                self.semantic_states.count(),
                self.transition_ids.count(),
                self.fault_ids.count(),
                self.workload_ids.count(),
                productive,
                self.splice_attempts,
                self.spliced,
                self.splice_rejected,
                self.counterfactual_reports,
                self.flight_recordings,
                self.flight_records,
                self.flight_records_dropped,
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
                "VOPR failure fingerprint={x} first={s} smallest_transitions={d} smallest={s} replay='zig build vopr-replay -- --trace {s}' reduce='zig build vopr-reduce -- --trace {s} --out <reduced.voprtrace>'\n",
                .{ fingerprint, summary.first_path, summary.smallest_transitions, summary.smallest_path, summary.smallest_path, summary.smallest_path },
            );
        }
        const aggregate_properties = try alloc.alloc(vopr.report.AggregateProperty, property_ids.len);
        defer alloc.free(aggregate_properties);
        for (property_ids, aggregate_properties) |property_id, *out| {
            const summary = self.properties.get(property_id).?;
            out.* = .{
                .property_id = property_id,
                .name = summary.name,
                .status = summary.status(),
                .evaluations = summary.evaluations,
                .ever_true = summary.ever_true,
                .ever_false = summary.ever_false,
            };
        }
        const aggregate_failures = try alloc.alloc(vopr.report.AggregateFailure, fingerprints.len);
        defer alloc.free(aggregate_failures);
        for (fingerprints, aggregate_failures) |fingerprint, *out| {
            const summary = self.failure_summaries.get(fingerprint).?;
            out.* = .{
                .fingerprint = fingerprint,
                .first_history = summary.first_history,
                .first_artifact = summary.first_path,
                .smallest_history = summary.smallest_history,
                .smallest_transitions = summary.smallest_transitions,
                .smallest_artifact = summary.smallest_path,
            };
        }
        std.mem.sort([]u8, self.retained_artifacts.items, {}, struct {
            fn lessThan(_: void, lhs: []u8, rhs: []u8) bool {
                return std.mem.lessThan(u8, lhs, rhs);
            }
        }.lessThan);
        var artifact_names: std.ArrayListUnmanaged([]const u8) = .empty;
        defer artifact_names.deinit(alloc);
        try artifact_names.appendSlice(alloc, &.{ "results.json", "results.html" });
        for (self.retained_artifacts.items) |path| try artifact_names.append(alloc, path);
        const quarantine_manifest = if (self.corpus.quarantined.items.len > 0)
            try std.fmt.allocPrint(alloc, "{s}/quarantine/index.json", .{self.artifact_dir})
        else
            null;
        defer if (quarantine_manifest) |path| alloc.free(path);
        if (quarantine_manifest) |path| try artifact_names.append(alloc, path);
        std.mem.sort([]const u8, artifact_names.items, {}, struct {
            fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
                return std.mem.lessThan(u8, lhs, rhs);
            }
        }.lessThan);
        var source_revision: []const u8 = "unknown";
        var target: []const u8 = "native";
        var optimize: []const u8 = "unknown";
        var representative: ?vopr.trace.Trace = null;
        defer if (representative) |*artifact| artifact.deinit();
        if (self.corpus.entries.items.len > 0) {
            representative = try vopr.trace.parseAlloc(alloc, self.corpus.entries.items[0].bytes);
            source_revision = representative.?.header.source_revision;
            target = representative.?.header.target;
            optimize = representative.?.header.optimize;
        }
        const run_id = try std.fmt.allocPrint(
            alloc,
            "campaign-{s}-{x:0>16}-{d}-{d}",
            .{ self.scenario, self.base_seed, self.histories, self.transitions },
        );
        defer alloc.free(run_id);
        const aggregate = vopr.report.Aggregate{
            .run_id = run_id,
            .scenario = self.scenario,
            .source_revision = source_revision,
            .target = target,
            .optimize = optimize,
            .base_seed = self.base_seed,
            .histories_limit = self.histories,
            .histories_consumed = self.clean_histories + self.failures + self.replay_divergences + self.harness_errors,
            .transition_limit_per_history = self.transitions,
            .transitions_consumed = self.transitions_executed,
            .clean_histories = self.clean_histories,
            .failed_histories = self.failures,
            .replay_divergences = self.replay_divergences,
            .harness_errors = self.harness_errors,
            .exact_replays = self.exact_replays,
            .corpus_entries = self.corpus.entries.items.len,
            .quarantined_entries = self.corpus.quarantined.items.len,
            .retained_entries = self.retained,
            .semantic_states = self.semantic_states.count(),
            .transition_kinds = self.transition_ids.count(),
            .faults_reached = self.fault_ids.count(),
            .workloads_reached = self.workload_ids.count(),
            .flight_recordings = self.flight_recordings,
            .flight_records = self.flight_records,
            .flight_records_dropped = self.flight_records_dropped,
            .properties = aggregate_properties,
            .failures = aggregate_failures,
            .artifacts = artifact_names.items,
        };
        const json = try aggregate.renderJsonAlloc(alloc);
        defer alloc.free(json);
        const html = try aggregate.renderHtmlAlloc(alloc);
        defer alloc.free(html);
        const json_path = try std.fmt.allocPrint(alloc, "{s}/results.json", .{self.artifact_dir});
        defer alloc.free(json_path);
        const html_path = try std.fmt.allocPrint(alloc, "{s}/results.html", .{self.artifact_dir});
        defer alloc.free(html_path);
        try std.Io.Dir.cwd().writeFile(self.io, .{ .sub_path = json_path, .data = json });
        try std.Io.Dir.cwd().writeFile(self.io, .{ .sub_path = html_path, .data = html });
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
        var source = vopr.choice.Mutating.init(parent.choices.items, mutation_index, replacement, seed);
        const artifact = runKnownScenarioWithChoices(alloc, &parent, source.source()) catch |err| switch (err) {
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
            // A splice preserves the original decision budget while joining
            // two compatible logical states.
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
        var source = try vopr.splice.Source.init(left.choices.items, right.choices.items, point);
        var artifact = runKnownScenarioWithChoices(alloc, &left, source.source()) catch |err| switch (err) {
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
        var replayed = try replayKnownScenario(alloc, &artifact);
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
            if (entry.kind != .file or !std.mem.endsWith(u8, entry.path, ".voprtrace")) continue;
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
            var artifact = vopr.trace.parseAlloc(alloc, encoded) catch {
                _ = try self.corpus.quarantineBytes(encoded, .invalid_trace);
                continue;
            };
            defer artifact.deinit();
            if (!artifactMatchesScenario(&artifact, self.scenario)) {
                _ = try self.corpus.quarantineBytes(encoded, .scenario_changed);
                continue;
            }
            // Corpus files must still be executable under the current ABI.
            var replayed = replayKnownScenario(alloc, &artifact) catch {
                _ = try self.corpus.quarantineBytes(encoded, .scenario_version_changed);
                continue;
            };
            replayed.deinit();
            const novelty = try self.coverage.observe(&artifact);
            const added = try self.corpus.add(&artifact, novelty);
            if (added.inserted) self.seeded_entries += 1;
        }
    }

    fn exportQuarantineArtifacts(self: *@This()) !void {
        if (self.corpus.quarantined.items.len == 0) return;
        const alloc = std.heap.smp_allocator;
        const quarantine_dir = try std.fmt.allocPrint(alloc, "{s}/quarantine", .{self.artifact_dir});
        defer alloc.free(quarantine_dir);
        try ensureDir(self.io, quarantine_dir);

        const ManifestEntry = struct {
            trace_digest: u64,
            reason: vopr.corpus.QuarantineReason,
            path: []const u8,
        };
        const entries = try alloc.alloc(ManifestEntry, self.corpus.quarantined.items.len);
        defer alloc.free(entries);
        var initialized: usize = 0;
        defer for (entries[0..initialized]) |entry| alloc.free(entry.path);
        for (self.corpus.quarantined.items, 0..) |entry, index| {
            const relative_path = try std.fmt.allocPrint(
                alloc,
                "quarantine/{s}-{x}.voprquarantine",
                .{ @tagName(entry.reason), entry.trace_digest },
            );
            entries[index] = .{
                .trace_digest = entry.trace_digest,
                .reason = entry.reason,
                .path = relative_path,
            };
            initialized += 1;
            const full_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ self.artifact_dir, relative_path });
            defer alloc.free(full_path);
            try std.Io.Dir.cwd().writeFile(self.io, .{ .sub_path = full_path, .data = entry.bytes });
        }
        const manifest = try std.json.Stringify.valueAlloc(alloc, .{
            .format = "vopr-quarantine-manifest-v1",
            .scenario = self.scenario,
            .entries = entries,
        }, .{ .whitespace = .indent_2 });
        defer alloc.free(manifest);
        const manifest_path = try std.fmt.allocPrint(alloc, "{s}/quarantine/index.json", .{self.artifact_dir});
        defer alloc.free(manifest_path);
        try std.Io.Dir.cwd().writeFile(self.io, .{ .sub_path = manifest_path, .data = manifest });
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
        \\  vopr run --scenario metadata|transaction|distributed-data|distributed-transaction|data-plane|derived-workflow|backup-restore|clock-fault|wal|persistent|index-manager|db-split|raft|lmdb|lsm|ha --seed <u64> [--transitions <n>] [--workload smoke|expanded] --trace-out <path>
        \\  vopr replay --trace <path>
        \\  vopr campaign --scenario metadata|transaction|distributed-data|distributed-transaction|data-plane|derived-workflow|backup-restore|clock-fault|wal|persistent|index-manager|db-split|raft|lmdb|lsm|ha --histories <n> [--transitions <n>] --workers <n> --artifact-dir <path>
        \\  vopr reduce --trace <path> --out <path> [--attempts <n>]
        \\  vopr promote --trace <path> --name <fixture-name> [--force]
        \\  vopr migrate --trace <path> --out <path> [--force]
        \\  vopr tla --trace <path> --domain raft|transaction --out <path.ndjson>
        \\  vopr explain --trace <path> [--failure <ordinal>] --out <path.json>
        \\  vopr debug --trace <path> [--prefix <choice-index>] [--out <path.json>] [--commands <path>] [--interactive]
        \\  vopr results --trace <path> [--run-id <id>] [--history-id <id>] [--json-out <path>] [--html-out <path>]
        \\  vopr events [--trace <path> ...] --query <query.json> [--validate] [--count] [--out <matches.json>]
        \\  vopr recipe --trace <path> [--failure <ordinal>] [--attempts <n>] [--flight-filter <filter.json>] [--flight-before <n>] [--flight-after <n>] [--flight-limit <n>] [--flight-capacity <n>] [--flight-anywhere] --out <recipe.json> --reduced-out <reduced.voprtrace>
        \\  vopr index --index <index.json> [--add <results.json> ...] [--run-id <id>] [--revision <revision>] [--scenario <name>] [--property <id>] [--property-name <name>] [--fingerprint <id>] [--corpus retained|quarantined] [--artifact-contains <text>] [--min-transitions <n>] [--max-transitions <n>] [--min-resources <n>] [--max-resources <n>] [--min-histories <n>] [--limit <n>] [--json-out <query.json>] [--html-out <summary.html>]
        \\  vopr corpus-merge --base <trace> [--trace <trace> ...] --out-dir <directory> [--force]
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
    const promoted_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ corpus_path, filename });
    defer alloc.free(promoted_path);
    const results_json_path = try std.fmt.allocPrint(alloc, "{s}/results.json", .{corpus_path});
    defer alloc.free(results_json_path);
    const results_html_path = try std.fmt.allocPrint(alloc, "{s}/results.html", .{corpus_path});
    defer alloc.free(results_html_path);
    try resultsCommand(alloc, io, &.{
        "--trace",    promoted_path,
        "--run-id",   "meta-test-run",
        "--json-out", results_json_path,
        "--html-out", results_html_path,
    });
    const results_json = try std.Io.Dir.cwd().readFileAlloc(io, results_json_path, alloc, .limited(max_trace_bytes));
    defer alloc.free(results_json);
    try std.testing.expect(std.mem.indexOf(u8, results_json, vopr.report.format) != null);
    try std.testing.expect(std.mem.indexOf(u8, results_json, "meta-test-run") != null);
    try std.testing.expect(std.mem.indexOf(u8, results_json, "health_checks") != null);
    const results_html = try std.Io.Dir.cwd().readFileAlloc(io, results_html_path, alloc, .limited(max_trace_bytes));
    defer alloc.free(results_html);
    try std.testing.expect(std.mem.startsWith(u8, results_html, "<!doctype html>"));

    const local_index_path = try std.fmt.allocPrint(alloc, "{s}/run-index.json", .{corpus_path});
    defer alloc.free(local_index_path);
    const local_query_path = try std.fmt.allocPrint(alloc, "{s}/run-query.json", .{corpus_path});
    defer alloc.free(local_query_path);
    const local_summary_path = try std.fmt.allocPrint(alloc, "{s}/run-summary.html", .{corpus_path});
    defer alloc.free(local_summary_path);
    const local_fingerprint = try std.fmt.allocPrint(alloc, "0x{x}", .{promoted.failures.items[0].fingerprint});
    defer alloc.free(local_fingerprint);
    try indexCommand(alloc, io, &.{
        "--index",       local_index_path,
        "--add",         results_json_path,
        "--run-id",      "meta-test-run",
        "--revision",    promoted.header.source_revision,
        "--fingerprint", local_fingerprint,
        "--json-out",    local_query_path,
        "--html-out",    local_summary_path,
    });
    const local_index = try std.Io.Dir.cwd().readFileAlloc(io, local_index_path, alloc, .limited(max_trace_bytes));
    defer alloc.free(local_index);
    try std.testing.expect(std.mem.indexOf(u8, local_index, vopr.run_index.format) != null);
    const local_query = try std.Io.Dir.cwd().readFileAlloc(io, local_query_path, alloc, .limited(max_trace_bytes));
    defer alloc.free(local_query);
    try std.testing.expect(std.mem.indexOf(u8, local_query, vopr.run_index.query_format) != null);
    try std.testing.expect(std.mem.indexOf(u8, local_query, "meta-test-run") != null);
    const local_summary = try std.Io.Dir.cwd().readFileAlloc(io, local_summary_path, alloc, .limited(max_trace_bytes));
    defer alloc.free(local_summary);
    try std.testing.expect(std.mem.startsWith(u8, local_summary, "<!doctype html>"));

    const query_path = try std.fmt.allocPrint(alloc, "{s}/query.json", .{corpus_path});
    defer alloc.free(query_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = query_path, .data =
        \\{"format":"vopr-event-set-v1","name":"all-events","steps":[{"name":"events","operation":"select","query":{"selector":{},"limit":16}}],"result":0}
    });
    try eventsCommand(alloc, io, &.{ "--query", query_path, "--validate" });
    const query_result_path = try std.fmt.allocPrint(alloc, "{s}/query-result.json", .{corpus_path});
    defer alloc.free(query_result_path);
    try eventsCommand(alloc, io, &.{ "--trace", promoted_path, "--query", query_path, "--out", query_result_path });
    const query_result = try std.Io.Dir.cwd().readFileAlloc(io, query_result_path, alloc, .limited(max_trace_bytes));
    defer alloc.free(query_result);
    try std.testing.expect(std.mem.indexOf(u8, query_result, "vopr-event-set-result-v1") != null);
    var parsed_query_result = try std.json.parseFromSlice(std.json.Value, alloc, query_result, .{});
    defer parsed_query_result.deinit();
    const match_count = parsed_query_result.value.object.get("match_count") orelse return error.TestUnexpectedResult;
    try std.testing.expect(match_count.integer > 0);

    const count_result_path = try std.fmt.allocPrint(alloc, "{s}/query-count.json", .{corpus_path});
    defer alloc.free(count_result_path);
    const promoted_copy_path = try std.fmt.allocPrint(alloc, "{s}/promoted-copy.voprtrace", .{corpus_path});
    defer alloc.free(promoted_copy_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = promoted_copy_path, .data = encoded });
    try eventsCommand(alloc, io, &.{
        "--trace",         promoted_path,
        "--trace",         promoted_copy_path,
        "--query",         query_path,
        "--count",         "--out",
        count_result_path,
    });
    const count_result = try std.Io.Dir.cwd().readFileAlloc(io, count_result_path, alloc, .limited(max_trace_bytes));
    defer alloc.free(count_result);
    try std.testing.expect(std.mem.indexOf(u8, count_result, "\"count_only\": true") != null);

    const merge_invalid_path = try std.fmt.allocPrint(alloc, "{s}/invalid.voprtrace", .{corpus_path});
    defer alloc.free(merge_invalid_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = merge_invalid_path, .data = "not-a-trace" });
    const merge_output_path = try std.fmt.allocPrint(alloc, "{s}/merged", .{corpus_path});
    defer alloc.free(merge_output_path);
    try corpusMergeCommand(alloc, io, &.{
        "--base",    promoted_path,
        "--trace",   promoted_path,
        "--trace",   merge_invalid_path,
        "--out-dir", merge_output_path,
    });
    const merge_manifest_path = try std.fmt.allocPrint(alloc, "{s}/index.json", .{merge_output_path});
    defer alloc.free(merge_manifest_path);
    const merge_manifest = try std.Io.Dir.cwd().readFileAlloc(io, merge_manifest_path, alloc, .limited(max_trace_bytes));
    defer alloc.free(merge_manifest);
    try std.testing.expect(std.mem.indexOf(u8, merge_manifest, "vopr-corpus-merge-v1") != null);
    try std.testing.expect(std.mem.indexOf(u8, merge_manifest, "invalid_trace") != null);

    var context = CampaignContext{
        .io = io,
        .histories = 1,
        .transitions = 2,
        .scenario = "metadata",
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

    try context.writeFailureDiagnostics(&promoted, 0);
    const recipe_artifact_path = try std.fmt.allocPrint(
        alloc,
        "{s}/failure-{x}.recipe.json",
        .{ corpus_path, promoted.failures.items[0].fingerprint },
    );
    defer alloc.free(recipe_artifact_path);
    const recipe_artifact = try std.Io.Dir.cwd().readFileAlloc(io, recipe_artifact_path, alloc, .limited(max_trace_bytes));
    defer alloc.free(recipe_artifact);
    try std.testing.expect(std.mem.indexOf(u8, recipe_artifact, vopr.debug_recipe.format) != null);
    try std.testing.expect(std.mem.indexOf(u8, recipe_artifact, "failure_identity") != null);
    try std.testing.expect(std.mem.indexOf(u8, recipe_artifact, "counterfactual") != null);
    try std.testing.expect(std.mem.indexOf(u8, recipe_artifact, "event_queries") != null);
    try std.testing.expect(std.mem.indexOf(u8, recipe_artifact, vopr.flight_recorder.format) != null);
    try std.testing.expect(std.mem.indexOf(u8, recipe_artifact, "metadata.action_applied") != null);
    try std.testing.expect(std.mem.indexOf(u8, recipe_artifact, "transition_kind") != null);
    const reduced_artifact_path = try std.fmt.allocPrint(
        alloc,
        "{s}/failure-{x}.reduced.voprtrace",
        .{ corpus_path, promoted.failures.items[0].fingerprint },
    );
    defer alloc.free(reduced_artifact_path);
    const reduced_artifact = try std.Io.Dir.cwd().readFileAlloc(io, reduced_artifact_path, alloc, .limited(max_trace_bytes));
    defer alloc.free(reduced_artifact);
    var packaged_reduction = try vopr.trace.parseAlloc(alloc, reduced_artifact);
    defer packaged_reduction.deinit();
    try std.testing.expectEqual(promoted.failures.items[0].fingerprint, packaged_reduction.failures.items[0].fingerprint);
    var packaged_replay = try replayKnownScenario(alloc, &packaged_reduction);
    packaged_replay.deinit();

    _ = try context.corpus.quarantineBytes("not-a-vopr-trace", .invalid_trace);
    try context.exportQuarantineArtifacts();
    const quarantine_manifest_path = try std.fmt.allocPrint(alloc, "{s}/quarantine/index.json", .{corpus_path});
    defer alloc.free(quarantine_manifest_path);
    const quarantine_manifest = try std.Io.Dir.cwd().readFileAlloc(io, quarantine_manifest_path, alloc, .limited(max_trace_bytes));
    defer alloc.free(quarantine_manifest);
    try std.testing.expect(std.mem.indexOf(u8, quarantine_manifest, "vopr-quarantine-manifest-v1") != null);
    try std.testing.expect(std.mem.indexOf(u8, quarantine_manifest, "invalid_trace") != null);

    var debug_session = DebugSession{ .alloc = alloc, .io = io, .recorded = &promoted, .prefix = 0 };
    const debug_snapshot_path = try std.fmt.allocPrint(alloc, "{s}/debug-snapshot.json", .{corpus_path});
    defer alloc.free(debug_snapshot_path);
    const seek_command = try std.fmt.allocPrint(alloc, "seek 0 {s}", .{debug_snapshot_path});
    defer alloc.free(seek_command);
    try std.testing.expect(try debug_session.execute(seek_command));
    const debug_snapshot = try std.Io.Dir.cwd().readFileAlloc(io, debug_snapshot_path, alloc, .limited(max_trace_bytes));
    defer alloc.free(debug_snapshot);
    try std.testing.expect(std.mem.indexOf(u8, debug_snapshot, "vopr-debug-snapshot-v1") != null);
    const causal_window_path = try std.fmt.allocPrint(alloc, "{s}/debug-causal-window.json", .{corpus_path});
    defer alloc.free(causal_window_path);
    const causal_window_command = try std.fmt.allocPrint(alloc, "causal-window 0 4 1 4 9 {s}", .{causal_window_path});
    defer alloc.free(causal_window_command);
    try std.testing.expect(try debug_session.execute(causal_window_command));
    const causal_window = try std.Io.Dir.cwd().readFileAlloc(io, causal_window_path, alloc, .limited(max_trace_bytes));
    defer alloc.free(causal_window);
    try std.testing.expect(std.mem.indexOf(u8, causal_window, "vopr-multiverse-v1") != null);

    var branch_index: ?usize = null;
    var branch_replacement: vopr.id.StableId = 0;
    for (promoted.choices.items, 0..) |choice_record, choice_index| {
        for (choice_record.enabled_ids) |candidate| if (candidate != choice_record.selected_id) {
            branch_index = choice_index;
            branch_replacement = candidate;
            break;
        };
        if (branch_index != null) break;
    }
    const chosen_branch_index = branch_index orelse return error.DebuggerFixtureRequiresAlternative;
    const debug_child_path = try std.fmt.allocPrint(alloc, "{s}/debug-child.voprtrace", .{corpus_path});
    defer alloc.free(debug_child_path);
    const debug_comparison_path = try std.fmt.allocPrint(alloc, "{s}/debug-comparison.json", .{corpus_path});
    defer alloc.free(debug_comparison_path);
    const branch_command = try std.fmt.allocPrint(
        alloc,
        "branch {d} {d} 17 {s} {s}",
        .{ chosen_branch_index, branch_replacement, debug_child_path, debug_comparison_path },
    );
    defer alloc.free(branch_command);
    try std.testing.expect(try debug_session.execute(branch_command));
    const debug_comparison = try std.Io.Dir.cwd().readFileAlloc(io, debug_comparison_path, alloc, .limited(max_trace_bytes));
    defer alloc.free(debug_comparison);
    try std.testing.expect(std.mem.indexOf(u8, debug_comparison, "vopr-debug-comparison-v1") != null);

    var transaction_artifact = try antfly.transaction_vopr.record(alloc, 0xA17F_7A4A);
    defer transaction_artifact.deinit();
    try std.testing.expectEqualStrings("pkg/antfly/src/vopr/fixtures/metadata", try fixtureDirForScenario(&promoted));
    try std.testing.expectEqualStrings("pkg/antfly/src/vopr/fixtures/transaction", try fixtureDirForScenario(&transaction_artifact));
    var generic_transaction_replay = try replayKnownScenario(alloc, &transaction_artifact);
    generic_transaction_replay.deinit();
    var transaction_ndjson: std.Io.Writer.Allocating = .init(alloc);
    defer transaction_ndjson.deinit();
    var transaction_replay = try antfly.transaction_vopr.replayToTransactionTrace(alloc, &transaction_artifact, &transaction_ndjson.writer);
    transaction_replay.deinit();
    try validateTransactionTraceNdjson(alloc, transaction_ndjson.written());
    try std.testing.expect(std.mem.indexOf(u8, transaction_ndjson.written(), "\"name\":\"InitTransaction\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, transaction_ndjson.written(), "\"name\":\"WriteIntentOnShard\"") != null);

    var data_plane = try antfly.domain_vopr.recordDataPlane(alloc, 0xA17F_C011);
    defer data_plane.deinit();
    var collector_session = DebugSession{ .alloc = alloc, .io = io, .recorded = &data_plane, .prefix = 0 };
    const collector_path = try std.fmt.allocPrint(alloc, "{s}/debug-collector.json", .{corpus_path});
    defer alloc.free(collector_path);
    const collector_command = try std.fmt.allocPrint(alloc, "collector 0 {s}", .{collector_path});
    defer alloc.free(collector_command);
    try std.testing.expect(try collector_session.execute(collector_command));
    const collector_bytes = try std.Io.Dir.cwd().readFileAlloc(io, collector_path, alloc, .limited(max_trace_bytes));
    defer alloc.free(collector_bytes);
    try std.testing.expect(collector_bytes.len > 2);

    var transaction_campaign = CampaignContext{
        .io = io,
        .histories = 1,
        .transitions = 3,
        .scenario = "transaction",
        .base_seed = 0x1234,
        .artifact_dir = corpus_path,
        .worker_count = 1,
        .coverage = vopr.coverage.Tracker.init(std.heap.smp_allocator),
        .corpus = vopr.corpus.Corpus.init(std.heap.smp_allocator),
    };
    defer transaction_campaign.deinitReport();
    defer transaction_campaign.corpus.deinit();
    defer transaction_campaign.coverage.deinit();
    try transaction_campaign.runHistory(0);
    try transaction_campaign.reportSummary();
    const flight_path = try std.fmt.allocPrint(alloc, "{s}/history-0-1234.flight.json", .{corpus_path});
    defer alloc.free(flight_path);
    const flight_json = try std.Io.Dir.cwd().readFileAlloc(io, flight_path, alloc, .limited(max_trace_bytes));
    defer alloc.free(flight_json);
    try std.testing.expect(std.mem.indexOf(u8, flight_json, vopr.flight_recorder.format) != null);
    const aggregate_results_path = try std.fmt.allocPrint(alloc, "{s}/results.json", .{corpus_path});
    defer alloc.free(aggregate_results_path);
    const aggregate_results = try std.Io.Dir.cwd().readFileAlloc(io, aggregate_results_path, alloc, .limited(max_trace_bytes));
    defer alloc.free(aggregate_results);
    try std.testing.expect(std.mem.indexOf(u8, aggregate_results, vopr.report.aggregate_format) != null);
    try std.testing.expect(std.mem.indexOf(u8, aggregate_results, "\"materialized\": 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, aggregate_results, "\"progress\": true") != null);
    try std.testing.expect(std.mem.indexOf(u8, aggregate_results, "\"exact_replay\": true") != null);
    try std.testing.expect(std.mem.indexOf(u8, aggregate_results, "\"harness\": true") != null);
    try indexCommand(alloc, io, &.{ "--index", local_index_path, "--add", aggregate_results_path });
    const combined_index = try std.Io.Dir.cwd().readFileAlloc(io, local_index_path, alloc, .limited(max_trace_bytes));
    defer alloc.free(combined_index);
    var parsed_combined = try std.json.parseFromSlice(std.json.Value, alloc, combined_index, .{});
    defer parsed_combined.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed_combined.value.object.get("runs").?.array.items.len);
}

test "VOPR scenario registry records and exactly replays every context-free domain" {
    const alloc = std.testing.allocator;
    const cases = [_][]const u8{
        "transaction",
        "wal",
        "persistent",
        "index-manager",
        "db-split",
        "raft",
        "lmdb",
        "lsm",
        "ha",
        "distributed-transaction",
        "data-plane",
        "derived-workflow",
        "backup-restore",
        "clock-fault",
    };
    for (cases, 0..) |scenario, index| {
        const transitions = try defaultCampaignTransitions(scenario);
        var artifact = try recordCampaignScenario(alloc, scenario, 0xA17F_C000 + index, transitions, 0xA17F_C000);
        defer artifact.deinit();
        try std.testing.expect(artifactMatchesScenario(&artifact, scenario));
        try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
        var replayed = try replayKnownScenario(alloc, &artifact);
        replayed.deinit();
    }
}
