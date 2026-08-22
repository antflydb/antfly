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
    if (argv.len < 2) return usage();
    if (std.mem.eql(u8, argv[1], "run")) return runCommand(alloc, io, argv[2..]);
    if (std.mem.eql(u8, argv[1], "replay")) return replayCommand(alloc, io, argv[2..]);
    if (std.mem.eql(u8, argv[1], "campaign")) return campaignCommand(alloc, io, argv[2..]);
    if (std.mem.eql(u8, argv[1], "reduce")) return reduceCommand(alloc, io, argv[2..]);
    if (std.mem.eql(u8, argv[1], "promote")) return promoteCommand(alloc, io, argv[2..]);
    if (std.mem.eql(u8, argv[1], "tla")) return tlaCommand(alloc, io, argv[2..]);
    return usage();
}

fn runCommand(alloc: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var seed: u64 = 0xa17f_0001;
    var transitions: usize = 64;
    var workload: antfly.metadata_sim_harness.MetadataVoprWorkload = .smoke;
    var trace_out: ?[]const u8 = null;
    var index: usize = 0;
    while (index < args.len) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--scenario")) {
            const value = try nextValue(args, &index);
            if (!std.mem.eql(u8, value, "metadata")) return error.UnsupportedScenario;
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
    if (transitions == 0) return error.InvalidTransitionBudget;
    const base_id = 10_000 + seed % 1_000_000;
    var artifact = try antfly.metadata_sim_harness.recordMetadataVoprCampaign(alloc, .{
        .seed = seed,
        .operation_count = transitions,
        .metadata_group_id = base_id,
        .table_id = base_id + 1,
        .range_group_id = base_id + 2,
        .split_group_id = base_id + 3,
        .split_transition_id = base_id + 4,
        .workload = workload,
    });
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
    var replayed = try antfly.metadata_sim_harness.replayMetadataVoprCampaign(alloc, &recorded);
    defer replayed.deinit();
    report("replayed", path, &replayed);
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
    if (!std.mem.eql(u8, domain, "raft")) return error.UnsupportedTlaDomain;
    const input = trace_path orelse return error.TracePathRequired;
    const output = output_path orelse return error.TraceOutputRequired;
    const encoded = try std.Io.Dir.cwd().readFileAlloc(io, input, alloc, .limited(max_trace_bytes));
    defer alloc.free(encoded);
    var recorded = try vopr.trace.parseAlloc(alloc, encoded);
    defer recorded.deinit();

    var ndjson: std.Io.Writer.Allocating = .init(alloc);
    defer ndjson.deinit();
    var replayed = try antfly.metadata_sim_harness.replayMetadataVoprCampaignToRaftTrace(alloc, &recorded, &ndjson.writer);
    defer replayed.deinit();
    try validateRaftTraceNdjson(alloc, ndjson.written());
    if (std.fs.path.dirname(output)) |parent| try ensureDir(io, parent);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = output, .data = ndjson.written() });
    std.debug.print("VOPR TLA export domain=raft events={d} trace={s} out={s}\n", .{
        countNonEmptyLines(ndjson.written()),
        input,
        output,
    });
}

fn validateRaftTraceNdjson(alloc: std.mem.Allocator, encoded: []const u8) !void {
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
        if (!std.mem.eql(u8, tag_text, "trace")) return error.InvalidTlaTraceRecord;
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
    try context.primeCorpus(alloc);
    defer context.corpus.deinit();
    defer context.coverage.deinit();
    const worker_count = @min(workers, @as(usize, @intCast(histories)));
    const threads = try alloc.alloc(std.Thread, worker_count);
    defer alloc.free(threads);
    var spawned: usize = 0;
    errdefer for (threads[0..spawned]) |thread| thread.join();
    for (threads) |*thread| {
        thread.* = try std.Thread.spawn(.{ .stack_size = 8 * 1024 * 1024 }, CampaignContext.worker, .{&context});
        spawned += 1;
    }
    for (threads) |thread| thread.join();
    if (context.first_error) |err| return err;
    std.debug.print(
        "VOPR campaign histories={d} seeded={d} retained={d} failures={d} features={d} workers={d} artifacts={s}\n",
        .{ histories, context.seeded_entries, context.retained, context.failures, context.coverage.hits.count(), worker_count, artifact_dir },
    );
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
    var reduced = try antfly.metadata_sim_harness.reduceMetadataVoprCampaign(alloc, &recorded, attempts);
    defer reduced.deinit();
    const reduced_bytes = try reduced.artifact.renderAlloc(alloc);
    defer alloc.free(reduced_bytes);
    if (std.fs.path.dirname(output)) |parent| try ensureDir(io, parent);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = output, .data = reduced_bytes });
    std.debug.print(
        "VOPR reduced transitions={d}->{d} attempts={d} fingerprint={x} trace={s}\n",
        .{ reduced.original_transitions, reduced.reduced_transitions, reduced.attempts, reduced.target_fingerprint, output },
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
    try validateFixtureName(name);
    const encoded = try std.Io.Dir.cwd().readFileAlloc(io, input, alloc, .limited(max_trace_bytes));
    defer alloc.free(encoded);
    var recorded = try vopr.trace.parseAlloc(alloc, encoded);
    defer recorded.deinit();
    if (recorded.failures.items.len == 0) return error.FailingTraceRequired;
    var replayed = try antfly.metadata_sim_harness.replayMetadataVoprCampaign(alloc, &recorded);
    replayed.deinit();
    const fixture_dir = "pkg/antfly/src/sim/fixtures/metadata";
    try ensureDir(io, fixture_dir);
    var dir = try std.Io.Dir.cwd().openDir(io, fixture_dir, .{});
    defer dir.close(io);
    const filename = try promoteRecordedToDir(alloc, io, dir, &recorded, name, force);
    defer alloc.free(filename);
    const output = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ fixture_dir, filename });
    defer alloc.free(output);
    std.debug.print("VOPR promoted fingerprint={x} fixture={s}\n", .{ recorded.failures.items[0].fingerprint, output });
}

fn promoteRecordedToDir(
    alloc: std.mem.Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    recorded: *const vopr.trace.Trace,
    name: []const u8,
    force: bool,
) ![]u8 {
    try validateFixtureName(name);
    try recorded.validate();
    if (recorded.failures.items.len == 0) return error.FailingTraceRequired;
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

fn validateFixtureName(name: []const u8) !void {
    if (name.len == 0) return error.InvalidFixtureName;
    for (name) |byte| {
        if (std.ascii.isLower(byte) or std.ascii.isDigit(byte) or byte == '-') continue;
        return error.InvalidFixtureName;
    }
}

const CampaignContext = struct {
    io: std.Io,
    histories: u64,
    transitions: usize,
    base_seed: u64,
    artifact_dir: []const u8,
    next_history: std.atomic.Value(u64) = .init(0),
    mutex: std.Io.Mutex = .init,
    coverage: vopr.coverage.Tracker,
    corpus: vopr.corpus.Corpus,
    seeded_entries: u64 = 0,
    retained: u64 = 0,
    failures: u64 = 0,
    first_error: ?anyerror = null,

    fn worker(self: *@This()) void {
        while (true) {
            const history_index = self.next_history.fetchAdd(1, .monotonic);
            if (history_index >= self.histories) return;
            self.runHistory(history_index) catch |err| {
                self.mutex.lock(self.io) catch return;
                defer self.mutex.unlock(self.io);
                if (self.first_error == null) self.first_error = err;
                return;
            };
        }
    }

    fn runHistory(self: *@This(), history_index: u64) !void {
        const alloc = std.heap.smp_allocator;
        const seed = self.base_seed +% history_index *% 0x9e37_79b9_7f4a_7c15;
        var artifact = (try self.mutateCorpusEntry(alloc, seed)) orelse blk: {
            const base_id = 10_000 + seed % 1_000_000;
            break :blk try antfly.metadata_sim_harness.recordMetadataVoprCampaign(alloc, .{
                .seed = seed,
                .operation_count = self.transitions,
                .metadata_group_id = base_id,
                .table_id = base_id + 1,
                .range_group_id = base_id + 2,
                .split_group_id = base_id + 3,
                .split_transition_id = base_id + 4,
            });
        };
        defer artifact.deinit();

        try self.mutex.lock(self.io);
        const novelty = self.coverage.observe(&artifact) catch |err| {
            self.mutex.unlock(self.io);
            return err;
        };
        const failed = artifact.failures.items.len > 0;
        const retain = history_index == 0 or novelty.discovered > 0 or failed;
        if (failed) self.failures += 1;
        const added = if (retain) self.corpus.add(&artifact, novelty) catch |err| {
            self.mutex.unlock(self.io);
            return err;
        } else null;
        const inserted = if (added) |result| result.inserted else false;
        if (inserted) self.retained += 1;
        self.mutex.unlock(self.io);
        if (!inserted) return;

        const bytes = try artifact.renderAlloc(alloc);
        defer alloc.free(bytes);
        const path = try std.fmt.allocPrint(alloc, "{s}/history-{d}-{x}.simtrace", .{ self.artifact_dir, history_index, seed });
        defer alloc.free(path);
        try std.Io.Dir.cwd().writeFile(self.io, .{ .sub_path = path, .data = bytes });
    }

    fn mutateCorpusEntry(self: *@This(), alloc: std.mem.Allocator, seed: u64) !?vopr.trace.Trace {
        var prng = std.Random.DefaultPrng.init(seed ^ 0x6d75_7461_7465_3031);
        try self.mutex.lock(self.io);
        if (self.corpus.entries.items.len == 0) {
            self.mutex.unlock(self.io);
            return null;
        }
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
        return antfly.metadata_sim_harness.runMetadataVoprCampaignWithChoices(alloc, cfg, source.source()) catch |err| switch (err) {
            error.MutationPointNotReached,
            error.MutationPointOutOfRange,
            error.MutationPrefixExhausted,
            error.ReplayChoiceSiteDiverged,
            error.ReplayEnabledSetDiverged,
            error.ChoiceSourceSelectedDisabledAlternative,
            => null,
            else => return err,
        };
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
    var root = try std.Io.Dir.openDirAbsolute(io, "/", .{});
    defer root.close(io);
    const relative = path[1..];
    if (relative.len > 0) try root.createDirPath(io, relative);
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
        \\  vopr run --scenario metadata --seed <u64> --transitions <n> [--workload smoke|expanded] --trace-out <path>
        \\  vopr replay --trace <path>
        \\  vopr campaign --scenario metadata --histories <n> --transitions <n> --workers <n> --artifact-dir <path>
        \\  vopr reduce --trace <path> --out <path> [--attempts <n>]
        \\  vopr promote --trace <path> --name <fixture-name> [--force]
        \\  vopr tla --trace <path> --domain raft --out <path.ndjson>
        \\
    , .{});
    return error.InvalidUsage;
}

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
        defer mutated.deinit();
        var mutation_replay = try antfly.metadata_sim_harness.replayMetadataVoprCampaign(alloc, &mutated);
        mutation_replay.deinit();
    } else return error.PersistentCorpusMutationRequired;
}
