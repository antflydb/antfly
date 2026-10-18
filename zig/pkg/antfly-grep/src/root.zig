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

const std = @import("std");
pub const interrupt = @import("interrupt.zig");
pub const concurrency = @import("concurrency.zig");
pub const setup = @import("setup.zig");
pub const tooling = @import("tooling.zig");
pub const workspace = @import("workspace.zig");
pub const Options = @import("options.zig").Options;
pub const Matcher = @import("matcher.zig").Matcher;
pub const discovery = @import("discovery.zig");
pub const filetypes = @import("filetypes.zig");
pub const decision = @import("decision.zig");
const Allocator = std.mem.Allocator;
pub const Summary = struct {
    index_requests: usize = 0,
    source_cache_hits: usize = 0,
    source_parses: usize = 0,
    files: usize = 0,
    bytes: usize = 0,
    matches: usize = 0,
    reading_leads: usize = 0,
    /// Exact when occurrence counting was requested; otherwise a lower bound.
    occurrences: usize = 0,
    occurrences_exact: bool = true,
    files_with_matches: usize = 0,
    regions_considered: usize = 0,
    regions_evaluated: usize = 0,
    errors: usize = 0,
    skipped_paths: usize = 0,
    invalid_utf8_files: usize = 0,
    complete: bool = true,
};
pub const Line = @import("source_cache.zig").Line;
const Limits = @import("budget.zig").Limits;
const Spill = @import("spill.zig").Store;
const Temp = @import("temporary.zig").File;
const Task = struct { workspace_entry: ?*const @import("workspace_store.zig").Entry = null, prepared: bool = false, path: []const u8, display: []const u8, filename: bool, explicit_file: bool = false, timestamp: i96 = 0, ordinal: usize = 0, result: ?[]u8 = null, score: f64 = 0 };
const EvidenceRow = struct { line: usize, byte_offset: usize, text: []const u8, p: ?f64, region_p: ?f64, matched: bool };
const EvidenceView = struct {
    rows: []EvidenceRow,
    regions: []@import("evidence.zig").Region,
    calls: []@import("source.zig").Call,
    declarations: []const @import("source.zig").Declaration,
    file_score: ?f64,
    parse_complete: bool,
    any_selected: bool,
    omitted_lines: usize,
    omitted_bytes: usize,
};
const RenderRow = struct { raw: []const u8, length: usize, line: usize, offset: usize, matched: bool, probability: f64, independent: bool, gap: bool };
const Candidate = struct {
    a: Allocator,
    budget: @import("antfly_regex").Budget,
    arena: std.heap.ArenaAllocator,
    path: []const u8 = "",
    score: f64 = 0,
    ordinal: usize = 0,
    provenance: @import("document.zig").Provenance = .{},

    rows: []RenderRow = &.{},
    view: ?EvidenceView = null,
    before: Summary = .{},
    summary: Summary = .{},
    filename: bool = true,
    color: bool = false,
    elapsed_ns: i96 = 0,
    pub fn bytes(self: *const Candidate) usize {
        return @sizeOf(Candidate) + self.budget.live;
    }
    pub fn deinit(self: *Candidate) void {
        const a = self.a;
        self.arena.deinit();
        a.destroy(self);
    }
};
const Ranking = @import("ranked.zig").Heap(Candidate);
// Clone only retained output evidence; parsed source and page buffers can be released.
fn retainedClone(comptime T: type, a: Allocator, value: T, depth: usize) error{ OutOfMemory, ResultNestingTooDeep }!T {
    if (depth > 512) return error.ResultNestingTooDeep;
    if (T == std.json.Value) return switch (value) {
        .string => |v| .{ .string = try a.dupe(u8, v) },
        .number_string => |v| .{ .number_string = try a.dupe(u8, v) },
        .array => |v| blk: {
            var out = std.json.Array.init(a);
            for (v.items) |item| try out.append(try retainedClone(T, a, item, depth + 1));
            break :blk .{ .array = out };
        },
        .object => |v| blk: {
            var out: std.json.ObjectMap = .empty;
            var it = v.iterator();
            while (it.next()) |entry| try out.put(a, try a.dupe(u8, entry.key_ptr.*), try retainedClone(T, a, entry.value_ptr.*, depth + 1));
            break :blk .{ .object = out };
        },
        else => value,
    };
    return switch (@typeInfo(T)) {
        .optional => |o| if (value) |v| try retainedClone(o.child, a, v, depth + 1) else null,
        .@"struct" => blk: {
            var out: T = undefined;
            inline for (@typeInfo(T).@"struct".field_names) |name| @field(out, name) = try retainedClone(@FieldType(T, name), a, @field(value, name), depth + 1);
            break :blk out;
        },
        .pointer => |ptr| blk: {
            if (ptr.size != .slice) @compileError("retained pointer must be a slice");
            const out = try a.alloc(ptr.child, value.len);
            for (value, out) |v, *dest| dest.* = try retainedClone(ptr.child, a, v, depth + 1);
            break :blk out;
        },
        else => value,
    };
}
const Region = @import("regions.zig").Region;
const ExternalDictionary = @import("external_dictionary.zig").Shared;
pub const Search = struct {
    workspace_generation: ?*const @import("workspace_store.zig").Generation = null,
    workspace_entry: ?*const @import("workspace_store.zig").Entry = null,
    coalescer: decision.Coalescer = .{},
    spill: ?*Spill = null,
    owns_spill: bool = false,
    discovery_stream: bool = false,
    single_source: bool = false,
    inventory_file: ?Temp = null,
    inventory_bytes: u64 = 0,
    inventory_reserved: usize = 0,
    inventory_count: usize = 0,
    discovery_decider: ?*decision.Client = null,
    source_views: ?*@import("source_views.zig").Store = null,
    ranking: ?*Ranking = null,
    owns_ranking: bool = false,
    deferred_candidate: ?*Candidate = null,
    next_ordinal: usize = 0,
    result_ordinal: usize = 0,
    file_before: Summary = .{},
    independent_override: ?bool = null,
    reserved_output: usize = 0,
    index_worker: ?*Search = null,
    local_cwd: ?[]const u8 = null,
    owns_cwd: bool = false,
    provenance: @import("document.zig").Provenance = .{},
    dictionary: ?*ExternalDictionary = null,
    a: Allocator,
    io: std.Io,
    options: *const Options,
    matcher: Matcher,
    decider: decision.Client,
    summary: Summary = .{},
    stop: bool = false,
    render_filename: bool = true,
    header_printed: bool = false,
    explicit_file: bool = false,
    binary_offset: ?usize = null,
    binary_mode: bool = false,
    binary_stop: bool = false,
    binary_checked: bool = false,
    literal_buffer_rejected: bool = false,
    color_enabled: bool = false,
    stdout_tty: bool = false,
    input_buffer: ?[]u8 = null,
    raw_line: ?[]const u8 = null,
    replay_spans: bool = false,
    span_runs: [64]SpanRun = undefined,
    span_run_count: usize = 0,
    span_runs_valid: bool = false,
    row_occurrences: usize = 0,
    current_hits: ?[]const @import("matcher.zig").Match = null,
    current_hit_text: []const u8 = "",
    replacement_absolute_start: bool = true,
    replacement_file_context: bool = false,
    sink: ?*std.Io.Writer = null,
    retaining_output: bool = false,
    output_bytes: usize = 0,
    output_buffer: std.ArrayList(u8) = .empty,
    collecting: bool = true,
    pipeline: ?*Pipeline = null,
    tasks: std.ArrayList(Task) = .empty,
    limits: Limits = .{},
    shared_limits: ?*Limits = null,
    best_probability: f64 = 0,
    file_path_triage: bool = false,
    transcoding: bool = false,
    stream_eof: bool = false,
    source_cache_state: @import("source_cache.zig").State = .{},
    json_started: bool = false,
    compact_file_id: [64]u8 = undefined,
    file_output_start: usize = 0,
    file_started: ?std.Io.Timestamp = null,
    file_elapsed_ns: ?i96 = null,
    evidence: ?*@import("evidence.zig").Evidence = null,
    pub fn init(a: Allocator, io: std.Io, o: *const Options) !Search {
        var matcher = try Matcher.initForSearch(a, o);
        errdefer matcher.deinit();
        const dictionary = if (o.search_zip and o.dictionary_path != null) try ExternalDictionary.load(a, io, o.dictionary_path.?, o.dictionary_size_limit, o.dictionary_format) else null;
        errdefer if (dictionary) |d| d.release();
        const cwd = if (o.json) try std.process.currentPathAlloc(io, a) else null;
        const tty = std.Io.File.stdout().isTty(io) catch false;
        return .{ .a = a, .io = io, .options = o, .matcher = matcher, .decider = decision.Client.init(a, io, o), .dictionary = dictionary, .stdout_tty = tty, .local_cwd = cwd, .owns_cwd = cwd != null };
    }
    fn initShared(root: *Search) !Search {
        const matcher = try root.matcher.clone(root.a);
        var result: Search = .{ .a = root.a, .io = root.io, .options = root.options, .dictionary = if (root.dictionary) |dictionary| dictionary.retain() else null, .matcher = matcher, .decider = decision.Client.init(root.a, root.io, root.options), .stdout_tty = root.stdout_tty };
        result.local_cwd = root.local_cwd;
        result.workspace_generation = root.workspace_generation;
        result.decider.shareCache(&root.decider);
        result.decider.coalescer = root.decider.coalescer;
        result.ranking = root.ranking;
        result.spill = root.spill;
        result.source_cache_state.views = root.source_cache_state.views;
        return result;
    }
    pub fn deinit(self: *Search) void {
        if (self.discovery_decider) |client| {
            client.deinit();
            self.a.destroy(client);
        }
        if (self.input_buffer) |buffer| self.a.free(buffer);
        if (self.index_worker) |worker| {
            worker.deinit();
            self.a.destroy(worker);
        }
        if (self.deferred_candidate) |candidate| candidate.deinit();
        if (self.owns_ranking) {
            self.ranking.?.deinit();
            self.a.destroy(self.ranking.?);
        }
        if (self.owns_spill) {
            self.spill.?.deinit();
            self.a.destroy(self.spill.?);
        }
        if (self.inventory_file) |*scratch| scratch.deinit(self.a, self.io);
        _ = self.limits.spill_bytes.fetchSub(self.inventory_reserved, .monotonic);
        if (self.dictionary) |dictionary| dictionary.release();
        if (self.source_views) |views| {
            views.deinit(self.io);
            self.a.destroy(views);
        }
        self.output_buffer.deinit(self.a);
        self.matcher.deinit();
        self.decider.deinit();
        for (self.tasks.items) |task| {
            self.a.free(task.path);
            self.a.free(task.display);
            if (task.result) |result| self.a.free(result);
        }
        self.tasks.deinit(self.a);
        if (self.owns_cwd) self.a.free(self.local_cwd.?);
    }
    pub fn stopped(self: *Search) bool {
        return self.stop or (if (self.shared_limits) |limits| limits.cancel.load(.monotonic) else false);
    }
    pub fn reportError(self: *Search, path: []const u8, err: anyerror) !void {
        if (err == error.WriteFailed or err == error.BrokenPipe) return err;
        if (self.pipeline) |pool| pool.context.mutex.lockUncancelable(self.io);
        defer if (self.pipeline) |pool| pool.context.mutex.unlock(self.io);
        self.summary.errors += 1;
        self.summary.complete = false;
        if (err == error.RegionBudgetExceeded or err == error.RequestBudgetExceeded or err == error.SourceBudgetExceeded or err == error.PendingOutputBudgetExceeded or err == error.SpillBudgetExceeded or err == error.OutputBudgetExceeded or err == error.IndexDocumentBudgetExceeded) {
            self.stop = true;
            if (self.shared_limits) |limits| limits.cancel.store(true, .monotonic);
        }
        if (self.options.json) try self.json(.{ .type = "error", .path = path, .message = @errorName(err) });
        if (!self.options.no_messages) std.debug.print("antfly-grep: {s}: {s}\n", .{ path, @errorName(err) });
    }
    pub fn run(self: *Search) !u8 {
        errdefer self.flushOutput() catch {};
        const started = std.Io.Clock.awake.now(self.io);
        if (self.options.debug or self.options.trace) std.debug.print("antfly-grep: engine={s} threads={d} encoding={s} multiline={}\n", .{ @tagName(self.options.engine), if (self.options.files) self.options.jobs else self.fileJobs(), self.options.encoding, self.options.multiline });
        if (self.options.debug or self.options.trace) {
            const jit = self.matcher.jitStats();
            std.debug.print("antfly-grep: native JIT programs={d} code_bytes={d} (VM fallback for other operations)\n", .{ jit.programs, jit.bytes });
        }
        self.shared_limits = &self.limits;
        self.decider.limits = &self.limits;
        if (self.options.ask != null) {
            if (!self.options.no_cache and self.options.cache_dir != null) {
                const views = try self.a.create(@import("source_views.zig").Store);
                views.* = .{ .a = self.a };
                self.source_views = views;
                self.source_cache_state.views = views;
            }
            self.decider.prepareCache();
            try self.decider.ensureLocal();
        }
        const implicit_directory = self.options.ask != null and self.options.paths.items.len == 0 and self.options.table_config == null and (try std.Io.File.stdin().isTty(self.io));
        if (self.options.ask != null and self.options.ranked and self.options.sort == null and
            self.options.max_result_files != std.math.maxInt(usize) and !self.hasStdinPath() and
            (self.options.paths.items.len > 0 or self.options.table_config != null or implicit_directory) and
            !self.options.quiet and !self.options.count and !self.options.count_matches and
            !self.options.files and !self.options.files_with_matches and !self.options.files_without_match and
            !self.options.inventory and !self.options.multiline)
        {
            const ranking = try self.a.create(Ranking);
            ranking.* = .{ .a = self.a, .io = self.io, .limits = &self.limits, .capacity = self.options.max_result_files, .max_bytes = self.options.max_pending_output };
            self.ranking = ranking;
            self.owns_ranking = true;
        }
        const single_regular_file = if (self.workspace_generation) |generation| generation.entries.items.len == 1 else self.options.paths.items.len == 1 and !self.hasStdinPath() and
            (if (std.Io.Dir.cwd().statFile(self.io, self.options.paths.items[0], .{})) |info| info.kind == .file else |_| false);
        self.single_source = single_regular_file and self.options.table_config == null;
        if (self.ranking == null and self.options.ask != null and self.options.ranked and self.options.sort == null and
            self.options.max_result_files == std.math.maxInt(usize) and !self.hasStdinPath() and
            (self.options.paths.items.len > 0 or self.options.table_config != null or implicit_directory) and
            !self.options.quiet and !self.options.count and !self.options.count_matches and !self.options.files and
            !self.options.files_with_matches and !self.options.files_without_match and !self.options.inventory and !self.options.multiline)
        {
            const spill = try self.a.create(Spill);
            spill.* = .{ .a = self.a, .io = self.io, .directory = self.options.temporary_dir, .limits = &self.limits, .max_bytes = self.options.max_spill_bytes };
            self.spill = spill;
            self.owns_spill = true;
        }
        self.discovery_stream = self.ranking != null or self.spill != null or
            (self.options.ask != null and !self.options.ranked and !self.options.jev_mode and
                (self.options.sort == null or std.mem.eql(u8, self.options.sort.?, "none")) and
                !self.options.quiet and !self.options.inventory and !self.hasStdinPath());
        if (self.discovery_stream and (self.single_source or self.options.triage or self.options.max_files == std.math.maxInt(usize))) {
            self.file_path_triage = self.options.triage or (self.single_source and 1 > self.options.max_files);
            self.pipeline = try Pipeline.start(self);
        }
        const streaming_files = self.options.files and self.options.ask == null and !self.options.inventory and self.options.table_config == null and (self.options.sort == null or std.mem.eql(u8, self.options.sort.?, "none"));
        if (streaming_files) self.collecting = false;

        // Lexical scans and relevance ranking can overlap bounded discovery;
        // explicit sorting retains its complete-inventory execution path.
        const pipelined = !streaming_files and self.options.paths.items.len > 0 and !single_regular_file and !self.hasStdinPath() and
            self.options.ask == null and !self.options.inventory and !self.options.quiet and
            (self.options.sort == null or std.mem.eql(u8, self.options.sort.?, "none")) and
            self.options.max_regions == std.math.maxInt(usize);
        if (pipelined and self.options.table_config == null and self.pipeline == null) self.pipeline = try Pipeline.start(self);
        defer if (self.pipeline) |pool| {
            pool.finish() catch {};
            self.pipeline = null;
        };

        if (self.workspace_generation) |generation| {
            for (generation.entries.items) |*entry| {
                if (self.stopped()) break;
                self.workspace_entry = entry;
                self.explicit_file = entry.explicit_file;
                self.render_filename = true;
                self.file(entry.path, entry.path) catch |err| try self.reportError(entry.path, err);
            }
            self.workspace_entry = null;
        } else {
            if (self.options.table_config != null) @import("indexed.zig").run(self) catch |err| {
                try self.reportError("<antfly>", err);
            };
            if (self.options.paths.items.len == 0 and self.options.table_config != null) {
                // A table-only invocation never consumes ambient stdin.
            } else if (self.options.paths.items.len == 0) {
                if (!self.options.files and !try std.Io.File.stdin().isTty(self.io)) self.stdin() catch |err| {
                    try self.reportError("<stdin>", err);
                } else self.searchPath(".") catch |err| {
                    try self.reportError(".", err);
                };
            } else for (self.options.paths.items) |path| {
                if (self.stopped()) break;
                if (std.mem.eql(u8, path, "-")) self.stdin() catch |err| {
                    try self.reportError("<stdin>", err);
                } else self.searchPath(path) catch |err| {
                    try self.reportError(path, err);
                };
            }
        }
        if (self.inventory_file != null and !self.stopped()) try self.startInventory(false);
        if (self.pipeline) |pool| {
            pool.finish() catch |err| {
                self.pipeline = null;
                return err;
            };
            self.pipeline = null;
        }
        if (self.options.inventory) {
            try self.renderInventory();
            return if (self.summary.complete) 0 else 2;
        }
        try self.flushTasks();
        if (self.ranking != null) try self.emitRanked();
        if (self.spill != null) try self.emitSpilled();
        if (self.limits.cancel.load(.acquire) and self.summary.complete and !self.options.quiet) {
            self.summary.complete = false;
            try self.reportError("<search>", error.Canceled);
        }
        if (self.discovery_decider) |client| {
            self.decider.requests += client.requests;
            self.decider.cache_hits += client.cache_hits;
            self.decider.input_tokens += client.input_tokens;
            self.decider.output_tokens += client.output_tokens;
            self.decider.request_bytes += client.request_bytes;
            self.decider.request_inputs += client.request_inputs;
            self.decider.peak_request_bytes = @max(self.decider.peak_request_bytes, client.peak_request_bytes);
        }
        const elapsed = started.durationTo(std.Io.Clock.awake.now(self.io)).nanoseconds;
        const seconds = @as(f64, @floatFromInt(elapsed)) / 1_000_000_000.0;
        const human = try std.fmt.allocPrint(self.a, "{d:.6}s", .{seconds});
        defer self.a.free(human);
        if (self.options.json and !self.options.quiet) try self.json(.{ .type = "summary", .data = .{ .elapsed_total = .{ .secs = @as(u64, @intCast(@divFloor(elapsed, 1_000_000_000))), .nanos = @as(u32, @intCast(@mod(elapsed, 1_000_000_000))), .human = human }, .stats = self.jsonStats(.{}) }, .stats = self.summary, .requests = self.decider.requests, .input_tokens = self.decider.input_tokens, .output_tokens = self.decider.output_tokens, .request_bytes = self.decider.request_bytes, .request_inputs = self.decider.request_inputs, .peak_request_bytes = self.decider.peak_request_bytes, .cache_hits = self.decider.cache_hits });
        if (self.options.stats and !self.options.json) std.debug.print("{d} matches\n{d} matched lines\n{d} files contained matches\n{d} files searched\n{d} bytes searched\n{s} total\n", .{ self.summary.occurrences, self.summary.matches, self.summary.files_with_matches, self.summary.files, self.summary.bytes, human });
        if (self.options.ask != null and !self.options.quiet and !self.options.suppress_progress) std.debug.print("antfly-grep: {d} files, {d}/{d} regions evaluated, {d} matches, {d} requests, {d} input tokens, {s}\n", .{ self.summary.files, self.summary.regions_evaluated, self.summary.regions_considered, self.summary.matches, self.decider.requests, self.decider.input_tokens, if (self.summary.complete) "complete" else "partial" });
        try self.flushOutput();
        return if (self.options.quiet and self.summary.matches > 0) 0 else if (!self.summary.complete) 2 else if (self.summary.matches != 0 or self.summary.reading_leads != 0) 0 else 1;
    }
    pub fn skipped(self: *Search) void {
        if (self.pipeline) |pool| pool.context.mutex.lockUncancelable(self.io);
        defer if (self.pipeline) |pool| pool.context.mutex.unlock(self.io);
        self.summary.skipped_paths += 1;
    }
    fn renderInventory(self: *Search) !void {
        const Group = struct { name: []const u8, files: usize = 0, bytes: u64 = 0 };
        var groups: std.ArrayList(Group) = .empty;
        defer groups.deinit(self.a);
        var bytes: u64 = 0;
        var files: usize = 0;
        for (self.tasks.items) |task| {
            const stat = std.Io.Dir.cwd().statFile(self.io, task.path, .{}) catch |err| {
                try self.reportError(task.display, err);
                continue;
            };
            files += 1;
            bytes += stat.size;
            var display = task.display;
            while (std.mem.startsWith(u8, display, "./")) display = display[2..];
            const name = if (std.mem.indexOfScalar(u8, display, '/')) |slash| display[0..slash] else ".";
            var found = false;
            for (groups.items) |*group| if (std.mem.eql(u8, group.name, name)) {
                group.files += 1;
                group.bytes += stat.size;
                found = true;
                break;
            };
            if (!found) try groups.append(self.a, .{ .name = name, .files = 1, .bytes = stat.size });
        }
        std.mem.sort(Group, groups.items, {}, struct {
            fn less(_: void, x: Group, y: Group) bool {
                return std.mem.order(u8, x.name, y.name) == .lt;
            }
        }.less);
        if (self.options.json) try self.json(.{ .type = "inventory", .files = files, .bytes = bytes, .groups = groups.items, .skipped_paths = self.summary.skipped_paths, .complete = self.summary.complete }) else {
            var output: std.Io.Writer.Allocating = .init(self.a);
            defer output.deinit();
            try output.writer.print("{d} eligible files, {d} bytes; {d} paths skipped\n", .{ files, bytes, self.summary.skipped_paths });
            for (groups.items) |group| try output.writer.print("{s}: {d} files, {d} bytes\n", .{ group.name, group.files, group.bytes });
            try self.write(output.written());
        }
        try self.flushOutput();
    }
    fn inventoryTask(self: *Search, task: Task) !void {
        if (self.inventory_file == null) self.inventory_file = try Temp.create(self.a, self.io, self.options.temporary_dir);
        const size = 17 + task.path.len + task.display.len;
        if (!Limits.reserve(&self.limits.spill_bytes, size, self.options.max_spill_bytes)) return error.SpillBudgetExceeded;
        self.inventory_reserved += size;
        const record = try self.a.alloc(u8, size);
        defer self.a.free(record);
        std.mem.writeInt(u32, record[0..4], @intCast(task.path.len), .little);
        std.mem.writeInt(u32, record[4..8], @intCast(task.display.len), .little);
        std.mem.writeInt(u64, record[8..16], task.ordinal, .little);
        record[16] = @as(u8, @intFromBool(task.filename)) | (@as(u8, @intFromBool(task.explicit_file)) << 1);
        @memcpy(record[17..][0..task.path.len], task.path);
        @memcpy(record[17 + task.path.len ..], task.display);
        try self.inventory_file.?.file.writePositionalAll(self.io, record, self.inventory_bytes);
        self.inventory_bytes += size;
        self.inventory_count += 1;
    }
    fn startInventory(self: *Search, triage: bool) !void {
        self.file_path_triage = self.options.triage or triage;
        if (self.inventory_count == 1 and self.options.table_config == null) self.single_source = true;
        if (self.pipeline == null) self.pipeline = try Pipeline.start(self);
        var buffer: [65536]u8 = undefined;
        var reader = self.inventory_file.?.file.reader(self.io, &buffer);
        while (!self.stopped()) {
            var h: [17]u8 = undefined;
            const n = try reader.interface.readSliceShort(&h);
            if (n == 0) break;
            if (n != h.len) return error.InvalidSpill;
            const plen = std.mem.readInt(u32, h[0..4], .little);
            const dlen = std.mem.readInt(u32, h[4..8], .little);
            if (plen > 1024 * 1024 or dlen > 1024 * 1024) return error.InvalidSpill;
            const path = try self.a.alloc(u8, plen);
            errdefer self.a.free(path);
            const display = try self.a.alloc(u8, dlen);
            errdefer self.a.free(display);
            try reader.interface.readSliceAll(path);
            try reader.interface.readSliceAll(display);
            const accepted = try self.pipeline.?.submit(.{ .path = path, .display = display, .ordinal = @intCast(std.mem.readInt(u64, h[8..16], .little)), .filename = h[16] & 1 != 0, .explicit_file = h[16] & 2 != 0 });
            if (!accepted) {
                self.a.free(path);
                self.a.free(display);
                break;
            }
        }
        self.inventory_file.?.deinit(self.a, self.io);
        self.inventory_file = null;
        _ = self.limits.spill_bytes.fetchSub(self.inventory_reserved, .monotonic);
        self.inventory_reserved = 0;
    }
    fn emitSpilled(self: *Search) !void {
        const spill = self.spill.?;
        try spill.finish();
        if (self.options.jev_mode and !self.options.json) {
            const header = try std.fmt.allocPrint(self.a, "Search: {s}\n{d} files searched; {d} selected source lines; {s}\n", .{ self.options.ask.?, self.summary.files, self.summary.matches, if (self.summary.complete) "complete" else "incomplete" });
            defer self.a.free(header);
            try self.write(header);
            try spill.visit(self, struct {
                fn row(search: *Search, entry: @import("spill.zig").Entry) !void {
                    const bytes = try std.fmt.allocPrint(search.a, "  {s}  {d:.3}\n", .{ entry.path, entry.score });
                    defer search.a.free(bytes);
                    try search.write(bytes);
                }
            }.row);
            try self.write("\n");
        }
        try spill.visit(self, struct {
            fn row(search: *Search, entry: @import("spill.zig").Entry) !void {
                try search.rankedBoundary(search.options.filename orelse true);
                if (entry.length > search.options.max_output_bytes - search.output_bytes) return error.OutputBudgetExceeded;
                try search.spill.?.stream(entry, search, Search.write);
            }
        }.row);
    }
    fn flushTasks(self: *Search) !void {
        if (self.tasks.items.len == 0) return;
        self.file_path_triage = self.options.triage or self.tasks.items.len > self.options.max_files;
        if (self.options.sort != null and !std.mem.eql(u8, self.options.sort.?, "none")) std.mem.sort(Task, self.tasks.items, self.options.reverse_sort, struct {
            fn less(reverse: bool, a: Task, b: Task) bool {
                if (a.timestamp != b.timestamp) return if (reverse) a.timestamp > b.timestamp else a.timestamp < b.timestamp;
                var left = std.mem.splitScalar(u8, a.display, '/');
                var right = std.mem.splitScalar(u8, b.display, '/');
                while (true) {
                    const x = left.next();
                    const y = right.next();
                    const order = if (x == null) (if (y == null) std.math.Order.eq else std.math.Order.lt) else if (y == null) std.math.Order.gt else std.mem.order(u8, x.?, y.?);
                    if (order != .eq) return if (reverse) order == .gt else order == .lt;
                    if (x == null) return false;
                }
            }
        }.less);
        const sorted = self.options.sort != null and !std.mem.eql(u8, self.options.sort.?, "none");
        const jobs = if (sorted or self.options.quiet or self.options.max_regions != std.math.maxInt(usize)) 1 else @min(self.fileJobs(), self.tasks.items.len);
        if (jobs == 1 and !self.options.jev_mode and !(self.options.ask != null and self.options.ranked and self.options.sort == null)) {
            self.collecting = false;
            for (self.tasks.items) |task| {
                if (task.prepared) {
                    if (task.result) |result| try self.write(result);
                    continue;
                }
                if (self.stopped()) break;
                self.render_filename = task.filename;
                self.explicit_file = task.explicit_file;
                self.workspace_entry = task.workspace_entry;
                self.file(task.path, task.display) catch |err| try self.reportError(task.display, err);
            }
            return;
        }
        self.decider.coalescer = if (jobs > 1 and self.options.ask != null) &self.coalescer else null;
        var context = Parallel{ .search = self };
        const threads = try self.a.alloc(std.Thread, jobs);
        defer self.a.free(threads);
        var started: usize = 0;
        defer for (threads[0..started]) |thread| thread.join();
        for (threads) |*thread| {
            thread.* = try std.Thread.spawn(.{}, Parallel.worker, .{&context});
            started += 1;
        }
        for (threads[0..started]) |thread| thread.join();
        started = 0;
        if (context.failure) |err| return err;
        if (self.ranking == null and self.spill == null and self.options.ask != null and (self.options.ranked or self.options.jev_mode)) {
            if (self.options.ranked and self.options.sort == null) std.mem.sort(Task, self.tasks.items, {}, struct {
                fn less(_: void, a: Task, b: Task) bool {
                    if (a.score != b.score) return a.score > b.score;
                    return std.mem.order(u8, a.display, b.display) == .lt;
                }
            }.less);
            if (self.options.jev_mode and !self.options.json and !self.options.quiet) {
                const header = try std.fmt.allocPrint(self.a, "Search: {s}\n{d} files searched; {d} selected source lines; {s}\n", .{ self.options.ask.?, self.summary.files, self.summary.matches, if (self.summary.complete) "complete" else "incomplete" });
                defer self.a.free(header);
                try self.write(header);
                var listed: usize = 0;
                for (self.tasks.items) |task| if (task.result) |bytes| {
                    if (bytes.len == 0) continue;
                    if (listed == self.options.max_result_files) break;
                    const row = try std.fmt.allocPrint(self.a, "  {s}  {d:.3}\n", .{ task.display, task.score });
                    defer self.a.free(row);
                    try self.write(row);
                    listed += 1;
                };
                try self.write("\n");
            }
            var shown: usize = 0;
            var omitted: usize = 0;
            for (self.tasks.items) |*task| if (task.result) |bytes| {
                if (bytes.len > 0) {
                    if (shown < self.options.max_result_files) {
                        try self.write(bytes);
                        shown += 1;
                    } else omitted += 1;
                }
                self.a.free(bytes);
                task.result = null;
            };
            if (omitted > 0) {
                if (self.options.json) try self.json(.{ .type = "omitted_files", .count = omitted }) else {
                    const row = try std.fmt.allocPrint(self.a, "[omitted {d} qualifying files; increase -n/--max-result-files]\n", .{omitted});
                    defer self.a.free(row);
                    try self.write(row);
                }
            }
        }
    }

    const Queue = struct {
        slots: []Task,
        head: usize = 0,
        count: usize = 0,
        closed: bool = false,
        mutex: std.Io.Mutex = .init,
        readable: std.Io.Condition = .init,
        writable: std.Io.Condition = .init,
        fn pop(self: *Queue, io: std.Io) ?Task {
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);
            while (self.count == 0 and !self.closed) self.readable.waitUncancelable(io, &self.mutex);
            if (self.count == 0) return null;
            const task = self.slots[self.head];
            self.head = (self.head + 1) % self.slots.len;
            self.count -= 1;
            self.writable.signal(io);
            return task;
        }
        fn close(self: *Queue, io: std.Io) void {
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);
            self.closed = true;
            self.readable.broadcast(io);
            self.writable.broadcast(io);
        }
    };
    const Pipeline = struct {
        context: Parallel,
        queue: Queue,
        threads: []std.Thread,
        started: usize = 0,
        growing: bool = false,
        growth_failed: bool = false,
        fn start(root: *Search) !*Pipeline {
            const jobs = if (root.single_source or root.options.max_regions != std.math.maxInt(usize)) 1 else root.fileJobs();
            if (jobs > 1 and root.options.ask != null) root.decider.coalescer = &root.coalescer;
            const self = try root.a.create(Pipeline);
            errdefer root.a.destroy(self);
            const slots = try root.a.alloc(Task, @min(256, jobs *| 4));
            errdefer root.a.free(slots);
            const threads = try root.a.alloc(std.Thread, jobs);
            errdefer root.a.free(threads);
            self.* = .{ .context = .{ .search = root }, .queue = .{ .slots = slots }, .threads = threads };
            self.context.queue = &self.queue;
            errdefer {
                self.queue.close(root.io);
                for (threads[0..self.started]) |thread| thread.join();
            }
            for (threads[0..if (root.single_source) 0 else if (root.options.jobs_auto) @min(2, jobs) else jobs]) |*thread| {
                thread.* = try std.Thread.spawn(.{}, Parallel.worker, .{&self.context});
                self.started += 1;
            }
            return self;
        }
        fn submit(self: *Pipeline, task: Task) !bool {
            const root = self.context.search;
            if (root.single_source) {
                // A known single source needs no discovery/file worker thread;
                // decision batches may still use their own bounded concurrency.
                std.debug.assert(self.started == 0 and self.queue.count == 0);
                self.queue.head = 0;
                self.queue.slots[0] = task;
                self.queue.count = 1;
                self.queue.closed = true;
                Parallel.worker(&self.context);
                return true;
            }
            self.queue.mutex.lockUncancelable(root.io);
            while (self.queue.count == self.queue.slots.len and !self.queue.closed) self.queue.writable.waitUncancelable(root.io, &self.queue.mutex);
            if (self.queue.closed) {
                self.queue.mutex.unlock(root.io);
                return false;
            }
            // Reserve a single growth operation. Publish the task before thread
            // creation so existing consumers can keep draining the queue.
            const grow = !self.growing and !self.growth_failed and self.started < self.threads.len and self.queue.count >= self.started * 2;
            if (grow) self.growing = true;
            self.queue.slots[(self.queue.head + self.queue.count) % self.queue.slots.len] = task;
            self.queue.count += 1;
            self.queue.readable.signal(root.io);
            self.queue.mutex.unlock(root.io);
            if (grow) {
                const thread = std.Thread.spawn(.{}, Parallel.worker, .{&self.context}) catch null;
                self.queue.mutex.lockUncancelable(root.io);
                defer self.queue.mutex.unlock(root.io);
                if (thread) |worker| {
                    self.threads[self.started] = worker;
                    self.started += 1;
                } else self.growth_failed = true;
                self.growing = false;
            }
            return true;
        }
        fn finish(self: *Pipeline) !void {
            const root = self.context.search;
            self.queue.close(root.io);
            for (self.threads[0..self.started]) |thread| thread.join();
            // Canceled workers may leave queued paths; ownership stays here.
            while (self.queue.pop(root.io)) |task| {
                root.a.free(task.path);
                root.a.free(task.display);
            }
            const failure = self.context.failure;
            root.a.free(self.threads);
            root.a.free(self.queue.slots);
            root.a.destroy(self);
            if (failure) |err| return err;
        }
    };

    const Parallel = struct {
        search: *Search,
        index: std.atomic.Value(usize) = .init(0),
        queue: ?*Queue = null,
        mutex: std.Io.Mutex = .init,
        failure: ?anyerror = null,
        const StreamingSink = struct {
            ctx: *Parallel,
            filename: bool,
            writer: std.Io.Writer,
            locked: bool = false,
            fn release(self: *StreamingSink) void {
                if (self.locked) {
                    self.ctx.mutex.unlock(self.ctx.search.io);
                    self.locked = false;
                }
            }
            fn send(self: *StreamingSink, bytes: []const u8) std.Io.Writer.Error!void {
                if (bytes.len == 0) return;
                const root = self.ctx.search;
                if (!self.locked) {
                    self.ctx.mutex.lockUncancelable(root.io);
                    self.locked = true;
                    const options = root.options;
                    if ((options.heading orelse false) and self.filename and !options.json and !options.count and !options.count_matches and !options.files_with_matches and !options.files_without_match and root.output_bytes > 0) root.write("\n") catch return error.WriteFailed;
                }
                root.write(bytes) catch |err| {
                    self.ctx.failure = err;
                    root.limits.cancel.store(true, .monotonic);
                    if (self.ctx.queue) |queue| queue.close(root.io);
                    self.release();
                    return error.WriteFailed;
                };
            }
            fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
                const self: *StreamingSink = @alignCast(@fieldParentPtr("writer", w));
                try self.send(w.buffer[0..w.end]);
                w.end = 0;
                var count: usize = 0;
                for (data[0 .. data.len - 1]) |bytes| {
                    try self.send(bytes);
                    count += bytes.len;
                }
                for (0..splat) |_| {
                    const bytes = data[data.len - 1];
                    try self.send(bytes);
                    count += bytes.len;
                }
                return count;
            }
            fn finish(self: *StreamingSink) !void {
                defer self.release();
                try self.writer.flush();
                if (self.locked and !self.ctx.search.options.block_buffered) try self.ctx.search.flushOutput();
            }
        };
        fn worker(ctx: *Parallel) void {
            const root = ctx.search;
            defer if (ctx.queue) |queue| queue.close(root.io);
            var local = Search.initShared(root) catch |err| {
                ctx.fail(err);
                return;
            };
            defer local.deinit();
            local.collecting = false;
            local.file_path_triage = root.file_path_triage;
            local.shared_limits = &root.limits;
            local.decider.limits = &root.limits;
            local.decider.allow_parallel = root.single_source or root.tasks.items.len == 1;
            local.decider.shared_local = if (root.decider.local) |provider| provider else null;
            while (!root.limits.cancel.load(.monotonic)) {
                var queued: Task = undefined;
                const task = if (ctx.queue) |queue| blk: {
                    queued = queue.pop(root.io) orelse break;
                    break :blk &queued;
                } else blk: {
                    const i = ctx.index.fetchAdd(1, .monotonic);
                    if (i >= root.tasks.items.len) break;
                    break :blk &root.tasks.items[i];
                };
                if (task.prepared) continue;
                defer if (ctx.queue != null) {
                    root.a.free(task.path);
                    root.a.free(task.display);
                };
                var output: std.Io.Writer.Allocating = .init(root.a);
                defer output.deinit();
                var stream_buffer: [65536]u8 = undefined;
                var streaming = StreamingSink{ .ctx = ctx, .filename = task.filename, .writer = .{ .vtable = &.{ .drain = StreamingSink.drain }, .buffer = &stream_buffer } };
                defer streaming.release();
                const ranked = root.options.ask != null and (root.options.ranked or root.options.jev_mode);
                local.retaining_output = ranked;
                local.sink = if (ranked) &output.writer else &streaming.writer;
                local.render_filename = task.filename;
                local.explicit_file = task.explicit_file;
                local.workspace_entry = task.workspace_entry;
                local.best_probability = 0;
                if (root.spill != null) local.output_bytes = 0;
                local.result_ordinal = task.ordinal;
                const reserved_before = local.reserved_output;
                local.file(task.path, task.display) catch |err| {
                    local.reportStagedError(task.display, err) catch |failure| {
                        streaming.release();
                        ctx.fail(failure);
                        return;
                    };
                };
                task.score = local.best_probability;
                if (local.stop) {
                    root.limits.cancel.store(true, .monotonic);
                    if (ctx.queue) |queue| queue.close(root.io);
                }
                if (root.ranking) |ranking| {
                    _ = root.limits.pending_output.fetchSub(local.reserved_output - reserved_before, .monotonic);
                    ctx.mutex.lockUncancelable(root.io);
                    root.write(output.written()) catch |err| {
                        ctx.mutex.unlock(root.io);
                        ctx.fail(err);
                        return;
                    };
                    ctx.mutex.unlock(root.io);
                    if (local.deferred_candidate) |candidate| {
                        local.deferred_candidate = null;
                        candidate.summary = local.summary;
                        ranking.offer(candidate) catch |err| {
                            if (root.pipeline == null) ctx.mutex.lockUncancelable(root.io);
                            root.reportError(task.display, err) catch |failure| {
                                if (root.pipeline == null) ctx.mutex.unlock(root.io);
                                ctx.fail(failure);
                                return;
                            };
                            if (root.pipeline == null) ctx.mutex.unlock(root.io);
                        };
                    }
                } else if (root.spill) |spill| {
                    spill.add(task.display, task.score, task.ordinal, output.written()) catch |err| {
                        local.retaining_output = false;
                        if (err == error.SpillBudgetExceeded) {
                            if (root.pipeline == null) ctx.mutex.lockUncancelable(root.io);
                            root.reportError(task.display, err) catch |failure| {
                                if (root.pipeline == null) ctx.mutex.unlock(root.io);
                                ctx.fail(failure);
                                return;
                            };
                            if (root.pipeline == null) ctx.mutex.unlock(root.io);
                            _ = root.limits.pending_output.fetchSub(local.reserved_output - reserved_before, .monotonic);
                            break;
                        }
                        ctx.fail(err);
                        return;
                    };
                    _ = root.limits.pending_output.fetchSub(local.reserved_output - reserved_before, .monotonic);
                } else if (root.options.ask != null and (root.options.ranked or root.options.jev_mode)) {
                    task.result = output.toOwnedSlice() catch |err| {
                        ctx.fail(err);
                        return;
                    };
                } else {
                    streaming.finish() catch |err| {
                        ctx.fail(err);
                        return;
                    };
                }
            }
            ctx.mutex.lockUncancelable(root.io);
            defer ctx.mutex.unlock(root.io);
            root.summary.occurrences_exact = root.summary.occurrences_exact and local.summary.occurrences_exact;
            inline for (.{ "files", "bytes", "matches", "reading_leads", "occurrences", "files_with_matches", "regions_considered", "regions_evaluated", "errors", "skipped_paths", "invalid_utf8_files", "source_cache_hits", "source_parses" }) |field| @field(root.summary, field) += @field(local.summary, field);
            root.summary.complete = root.summary.complete and local.summary.complete;
            root.decider.requests += local.decider.requests;
            root.decider.input_tokens += local.decider.input_tokens;
            root.decider.output_tokens += local.decider.output_tokens;
            root.decider.request_bytes += local.decider.request_bytes;
            root.decider.request_inputs += local.decider.request_inputs;
            root.decider.peak_request_bytes = @max(root.decider.peak_request_bytes, local.decider.peak_request_bytes);
            root.decider.cache_hits += local.decider.cache_hits;
        }
        fn fail(ctx: *Parallel, err: anyerror) void {
            ctx.mutex.lockUncancelable(ctx.search.io);
            defer ctx.mutex.unlock(ctx.search.io);
            if (ctx.failure == null) ctx.failure = err;
            ctx.search.limits.cancel.store(true, .monotonic);
            if (ctx.queue) |queue| queue.close(ctx.search.io);
        }
    };
    pub fn directoryAllowed(self: *Search, path_name: []const u8) !bool {
        if ((!self.options.triage and !self.options.jev_mode) or self.options.ask == null or self.options.broad) return true;
        // Discovery owns its client. Provider latency must never hold the
        // result/publication mutex; counters merge only after workers join.
        const client = self.discovery_decider orelse blk: {
            const c = try self.a.create(decision.Client);
            c.* = decision.Client.init(self.a, self.io, self.options);
            c.shareCache(&self.decider);
            c.shared_local = self.decider.local;
            c.limits = &self.limits;
            c.stage = .directory;
            self.discovery_decider = c;
            break :blk c;
        };
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        var listing: std.Io.Writer.Allocating = .init(a);
        try listing.writer.print("TARGET directory: {s}\n", .{path_name});
        var dir = try std.Io.Dir.cwd().openDir(self.io, path_name, .{ .iterate = true });
        defer dir.close(self.io);
        var it = dir.iterate();
        var n: usize = 0;
        while (try it.next(self.io)) |entry| {
            if (entry.name[0] == '.') continue;
            try listing.writer.print("TARGET child: {s}\n", .{entry.name});
            n += 1;
            if (n == 200) break;
        }
        var scores: [1]f64 = undefined;
        try client.search(&.{.{ .id = "directory", .input = listing.written() }}, &scores);
        return scores[0] >= @min(self.options.threshold, 0.25);
    }
    fn searchPath(self: *Search, name: []const u8) !void {
        self.explicit_file = false;
        const stat = try std.Io.Dir.cwd().statFile(self.io, name, .{});
        self.render_filename = self.options.filename orelse (self.options.table_config != null or self.options.ask != null or self.options.paths.items.len > 1 or stat.kind == .directory);
        if (stat.kind == .directory) {
            const cwd = try std.process.currentPathAlloc(self.io, self.a);
            defer self.a.free(cwd);
            const full = try std.fs.path.resolveAlloc(self.a, &.{ cwd, name });
            defer self.a.free(full);
            var d = discovery.Discovery{ .a = self.a, .io = self.io, .options = self.options };
            defer d.deinit();
            try d.prepare(full);
            const display = if (std.mem.eql(u8, name, ".") and self.options.paths.items.len == 0) "" else name;
            if (self.options.files or self.pipeline != null) try d.walkParallel(full, display, self) else try d.walk(full, display, self);
        } else if (stat.kind == .file) {
            self.explicit_file = true;
            try self.file(name, name);
        } else return error.NotRegularFile;
    }
    fn fileJobs(self: *const Search) usize {
        const requested = @max(1, self.options.jobs);
        // Literal and literal-union scans saturate filesystem/output bandwidth
        // before CPU-heavy regexes. Keep their measured eight-worker ceiling
        // across output modes; retain full concurrency for regexes and always
        // honor an explicitly selected worker count.
        if (self.options.ask == null and self.options.jobs_auto and self.matcher.isLiteralSearch()) return @min(8, requested);
        return requested;
    }
    pub fn discoveryStopped(self: *Search) bool {
        return self.limits.cancel.load(.acquire);
    }
    pub fn concurrentFile(self: *Search, path: []const u8, display: []const u8) !bool {
        // During a directory walk these producer settings are immutable. Each
        // task owns its strings and Pipeline.submit handles concurrent producers.
        if (!self.collecting or self.pipeline == null) return false;
        try self.file(path, display);
        return true;
    }
    pub fn canBatchFileLists(self: *Search) bool {
        return self.options.files and !self.collecting and !self.options.skip_oversize and !self.options.json and !self.options.line_buffered and self.options.max_output_bytes == std.math.maxInt(usize);
    }
    pub fn filesBatch(self: *Search, bytes: []const u8, count: usize) !void {
        try self.write(bytes);
        self.summary.files += count;
        self.summary.matches += count;
    }
    pub fn file(self: *Search, path_name: []const u8, display: []const u8) !void {
        if (self.collecting) {
            const path = try self.a.dupe(u8, path_name);
            errdefer self.a.free(path);
            const shown = try self.a.dupe(u8, display);
            errdefer self.a.free(shown);
            var timestamp: i96 = 0;
            if (self.options.sort) |sort| {
                if (!std.mem.eql(u8, sort, "path") and !std.mem.eql(u8, sort, "none")) {
                    const stat = try std.Io.Dir.cwd().statFile(self.io, path_name, .{});
                    timestamp = if (std.mem.eql(u8, sort, "modified")) stat.mtime.nanoseconds else if (std.mem.eql(u8, sort, "accessed")) (stat.atime orelse std.Io.Timestamp.zero).nanoseconds else (try @import("filesystem.zig").info(self.a, self.io, path_name)).created orelse return error.CreationTimeUnavailable;
                }
            }
            const task: Task = .{ .workspace_entry = self.workspace_entry, .ordinal = self.next_ordinal, .path = path, .display = shown, .filename = self.render_filename, .timestamp = timestamp, .explicit_file = self.explicit_file };
            self.next_ordinal += 1;
            if (self.pipeline) |pool| {
                if (!try pool.submit(task)) {
                    self.a.free(path);
                    self.a.free(shown);
                    return;
                }
            } else if (self.discovery_stream) {
                try self.inventoryTask(task);
                if (self.inventory_count > self.options.max_files) try self.startInventory(true);
                self.a.free(task.path);
                self.a.free(task.display);
            } else try self.tasks.append(self.a, task);
            return;
        }
        if (self.options.files and self.options.ask == null and !self.options.skip_oversize and !self.options.json) {
            try self.write(display);
            try self.write(if (self.options.null_paths) "\x00" else "\n");
            self.summary.files += 1;
            self.summary.matches += 1;
            return;
        }
        const local_uri = if (self.options.json) try @import("document.zig").localUriAt(self.a, self.local_cwd.?, path_name) else null;
        defer if (local_uri) |uri| self.a.free(uri);
        const previous_source = self.provenance;
        defer self.provenance = previous_source;
        self.provenance = .{ .uri = local_uri, .observed_at = if (self.options.json) std.Io.Clock.real.now(self.io).toSeconds() else null };
        if (self.workspace_entry) |entry| {
            self.provenance.revision = &entry.revision;
            self.provenance.snapshot = &self.workspace_generation.?.revision;
            self.provenance.observed_at = self.workspace_generation.?.captured_at;
            self.provenance.freshness = .snapshot;
        }
        const before = self.summary;
        self.file_elapsed_ns = null;
        self.file_before = before;
        self.file_output_start = self.output_bytes;
        self.file_started = if (self.options.json) std.Io.Clock.awake.now(self.io) else null;
        self.matcher.failure = null;
        self.current_hits = null;
        self.replay_spans = false;
        self.span_runs_valid = false;
        self.binary_mode = false;
        self.binary_stop = false;
        self.binary_offset = null;
        self.replacement_file_context = false;
        self.json_started = false;
        self.readFile(path_name, display) catch |err| {
            try self.jsonEnd(display, before);
            return err;
        };
        if (self.matcher.failure) |err| return err;
        try self.jsonEnd(display, before);
        if (self.deferred_candidate) |c| c.elapsed_ns = if (self.file_started) |start| @intCast(@max(0, start.durationTo(std.Io.Clock.awake.now(self.io)).nanoseconds)) else 0;
    }
    fn reportStagedError(self: *Search, path: []const u8, err: anyerror) !void {
        self.reportError(path, err) catch |failure| {
            if ((err == error.OutputBudgetExceeded or err == error.PendingOutputBudgetExceeded) and
                (failure == error.OutputBudgetExceeded or failure == error.PendingOutputBudgetExceeded))
            {
                // Already-complete JSON records remain owned by the staging
                // writer, even when the error/end record cannot fit.
                self.retaining_output = false;
                return;
            }
            return failure;
        };
    }
    fn rankedBoundary(self: *Search, filename: bool) !void {
        if (filename and (self.options.heading orelse false) and !self.options.json and
            !self.options.count and !self.options.count_matches and !self.options.files_with_matches and
            !self.options.files_without_match and self.output_bytes > 0) try self.write("\n");
    }
    pub fn indexDocument(self: *Search, doc: @import("document.zig").Document) !void {
        if (doc.content.len > self.options.max_file_bytes) {
            if (self.options.skip_oversize) {
                self.skipped();
                return;
            }
            return error.FileTooBig;
        }
        const timestamp: i96 = if (self.options.sort) |sort| blk: {
            const seconds = if (std.mem.eql(u8, sort, "modified")) doc.modified_at else if (std.mem.eql(u8, sort, "created")) doc.created_at else if (std.mem.eql(u8, sort, "accessed")) doc.accessed_at else break :blk 0;
            break :blk @as(i96, seconds orelse return error.IndexedTimestampUnavailable) * 1000000000;
        } else 0;
        // Keep matcher scratch, lazy DFA caches, native models and the HTTP
        // connection pool warm across pages; document bytes remain page-owned.
        const local = self.index_worker orelse blk: {
            const worker = try self.a.create(Search);
            errdefer self.a.destroy(worker);
            worker.* = try Search.initShared(self);
            worker.decider.coalescer = null;
            self.index_worker = worker;
            break :blk worker;
        };
        local.file_elapsed_ns = null;
        local.summary = .{};
        local.file_before = .{};
        local.result_ordinal = self.next_ordinal;
        self.next_ordinal += 1;
        const reserved_before = local.reserved_output;
        local.stop = false;
        local.output_bytes = 0;
        local.best_probability = 0;
        local.header_printed = false;
        local.json_started = false;
        local.binary_mode = false;
        local.binary_stop = false;
        local.binary_checked = false;
        local.binary_offset = null;
        local.current_hits = null;
        local.replay_spans = false;
        local.span_runs_valid = false;
        local.raw_line = null;
        local.matcher.failure = null;
        local.file_started = std.Io.Clock.awake.now(self.io);
        local.file_output_start = 0;
        local.replacement_absolute_start = true;
        local.replacement_file_context = false;
        local.color_enabled = self.options.color == .always or (self.options.color == .auto and self.stdout_tty);
        local.decider.cache_scope = doc.cache_scope;
        local.decider.requests = 0;
        local.decider.cache_hits = 0;
        local.decider.input_tokens = 0;
        local.decider.output_tokens = 0;
        local.decider.request_bytes = 0;
        local.decider.request_inputs = 0;
        local.decider.peak_request_bytes = 0;
        defer {
            local.provenance = .{};
            local.sink = null;
        }
        local.collecting = false;
        local.shared_limits = &self.limits;
        local.decider.limits = &self.limits;
        local.decider.shared_local = if (self.decider.local) |provider| provider else null;
        local.provenance = doc.provenance;
        local.render_filename = self.options.filename orelse true;
        local.explicit_file = true;
        const ranked = (self.options.ask != null and (self.options.ranked or self.options.jev_mode)) or (self.options.sort != null and !std.mem.eql(u8, self.options.sort.?, "none"));
        var output: std.Io.Writer.Allocating = .init(self.a);
        defer output.deinit();
        var forward = ForwardSink{ .search = self, .writer = .{ .vtable = &.{ .drain = ForwardSink.drain }, .buffer = &.{} } };
        local.retaining_output = ranked;
        local.sink = if (ranked) &output.writer else &forward.writer;
        defer {
            inline for (.{ "files", "bytes", "matches", "reading_leads", "occurrences", "files_with_matches", "regions_considered", "regions_evaluated", "errors", "skipped_paths", "invalid_utf8_files", "source_cache_hits", "source_parses" }) |field| @field(self.summary, field) += @field(local.summary, field);
            self.summary.complete = self.summary.complete and local.summary.complete;
            self.summary.occurrences_exact = self.summary.occurrences_exact and local.summary.occurrences_exact;
            self.decider.requests += local.decider.requests;
            self.decider.input_tokens += local.decider.input_tokens;
            self.decider.output_tokens += local.decider.output_tokens;
            self.decider.request_bytes += local.decider.request_bytes;
            self.decider.request_inputs += local.decider.request_inputs;
            self.decider.peak_request_bytes = @max(self.decider.peak_request_bytes, local.decider.peak_request_bytes);
            self.decider.cache_hits += local.decider.cache_hits;
            self.stop = self.stop or local.stop;
        }
        if (self.options.files) {
            try local.write(doc.path);
            try local.write(if (self.options.null_paths) "\x00" else "\n");
            local.summary.files += 1;
            local.summary.matches += 1;
        } else {
            if (self.options.ask == null and !self.options.multiline and !Limits.reserve(&self.limits.source_bytes, doc.content.len, self.options.max_source_bytes)) return error.SourceBudgetExceeded;
            local.processBytes(doc.path, doc.content) catch |err| try local.reportStagedError(doc.path, err);
            if (local.matcher.failure) |err| try local.reportStagedError(doc.path, err);
            local.jsonEnd(doc.path, .{}) catch |err| try local.reportStagedError(doc.path, err);
        }
        if (self.ranking) |ranking| {
            _ = self.limits.pending_output.fetchSub(local.reserved_output - reserved_before, .monotonic);
            try self.write(output.written());
            if (local.deferred_candidate) |candidate| {
                local.deferred_candidate = null;
                candidate.summary = local.summary;
                candidate.elapsed_ns = if (local.file_started) |start| @intCast(@max(0, start.durationTo(std.Io.Clock.awake.now(self.io)).nanoseconds)) else 0;
                ranking.offer(candidate) catch |err| try self.reportError(doc.path, err);
            }
            return;
        }
        if (self.spill) |spill| {
            try spill.add(doc.path, local.best_probability, local.result_ordinal, output.written());
            _ = self.limits.pending_output.fetchSub(local.reserved_output - reserved_before, .monotonic);
            return;
        }
        if (!ranked) {
            try local.flushOutput();
            if (!self.options.block_buffered) try self.flushOutput();
            return;
        }
        if (output.written().len == 0) return;
        const path = try self.a.dupe(u8, doc.path);
        errdefer self.a.free(path);
        const display = try self.a.dupe(u8, doc.path);
        errdefer self.a.free(display);
        const result = try output.toOwnedSlice();
        errdefer self.a.free(result);
        try self.tasks.append(self.a, .{ .path = path, .display = display, .filename = true, .prepared = true, .result = result, .score = local.best_probability, .timestamp = timestamp });
    }
    fn readFile(self: *Search, path_name: []const u8, display: []const u8) !void {
        if (self.workspace_entry) |entry| {
            if (entry.bytes.len > self.options.max_file_bytes) {
                if (self.options.skip_oversize) return;
                return error.StreamTooLong;
            }
            self.replacement_file_context = @import("builtin").os.tag == .linux and (self.options.mmap or (self.options.mmap_auto and self.options.ask == null));
            return self.processBytes(display, entry.bytes);
        }
        if (self.options.skip_oversize) {
            const info = try std.Io.Dir.cwd().statFile(self.io, path_name, .{});
            if (info.size > self.options.max_file_bytes) return;
        }
        if (self.options.ask != null and self.file_path_triage and !self.options.broad) {
            const listing = try std.fmt.allocPrint(self.a, "TARGET file path: {s}", .{display});
            defer self.a.free(listing);
            const old = self.decider.stage;
            self.decider.stage = .file;
            defer self.decider.stage = old;
            var scores: [1]f64 = undefined;
            try self.decider.search(&.{.{ .id = "file-path", .input = listing }}, &scores);
            if (scores[0] < 0.25) return;
        }
        self.header_printed = false;
        self.color_enabled = self.options.color == .always or (self.options.color == .auto and self.stdout_tty);
        if (self.options.files) {
            try self.write(display);
            try self.write(if (self.options.null_paths) "\x00" else "\n");
            self.summary.files += 1;
            self.summary.matches += 1;
            return;
        }
        if (self.options.pre) |program| {
            var use = self.options.pre_globs.items.len == 0;
            for (self.options.pre_globs.items) |raw| {
                const negative = std.mem.startsWith(u8, raw, "!");
                const pattern = if (negative) raw[1..] else raw;
                var glob = try discovery.glob(self.a, pattern);
                defer glob.deinit();
                if (try glob.matches(self.a, if (std.mem.indexOfScalar(u8, pattern, '/') == null) std.fs.path.basename(path_name) else path_name)) use = !negative;
            }
            if (use) {
                var input_file = try std.Io.Dir.cwd().openFile(self.io, path_name, .{});
                defer input_file.close(self.io);
                var child = try std.process.spawn(self.io, .{ .argv = &.{ program, path_name }, .stdin = .{ .file = input_file }, .stdout = .pipe });
                defer child.kill(self.io);
                var buffer: [16384]u8 = undefined;
                var reader = child.stdout.?.readerStreaming(self.io, &buffer);
                if (!self.options.multiline and self.options.ask == null) {
                    self.stream_eof = false;
                    try self.streamDecoded(display, &reader.interface);
                    // A bounded search may finish while the preprocessor is
                    // blocked writing. Terminate/reap it rather than waiting
                    // for output that this search will never consume.
                    if (!self.stream_eof or self.binary_stop) return;
                } else {
                    try self.snapshotDecoded(display, &reader.interface);
                }
                const term = try child.wait(self.io);
                if (term != .exited or term.exited != 0) return error.PreprocessorFailed;
                return;
            }
        }
        if (self.options.search_zip) if (@import("compression.zig").kind(path_name)) |format| {
            const dictionary: []const u8 = if (self.dictionary) |shared| shared.bytes else &.{};
            const brotli_dictionary = if (format == .brotli and self.dictionary != null) try self.dictionary.?.getBrotli(self.io) else null;
            if (dictionary.len > 0 and format != .brotli and format != .lz4 and format != .zstd) return error.UnsupportedDictionary;
            if (format == .bzip2 or format == .compress or format == .brotli or format == .lz4 or format == .lzma or format == .xz or format == .lzip) {
                var file_handle = try std.Io.Dir.cwd().openFile(self.io, path_name, .{});
                defer file_handle.close(self.io);
                var compressed_buffer: [65536]u8 = undefined;
                var decoded_buffer: [65536]u8 = undefined;
                var input = file_handle.reader(self.io, &compressed_buffer);
                const codec = @import("codec_stream.zig");
                const decoder = try codec.Stream.start(self.a, self.io, &input.interface, if (format == .bzip2) codec.bzip2 else if (format == .brotli) codec.brotli else if (format == .lz4) codec.lz4 else if (format == .lzma) codec.lzma else if (format == .xz) codec.xz else if (format == .lzip) codec.lzip else codec.lzw, &decoded_buffer, self.options.max_file_bytes, .{ .dictionary = dictionary, .brotli_dictionary = brotli_dictionary, .max_window_bytes = self.options.decompression_window });
                defer decoder.deinit();
                self.searchDecoded(display, &decoder.reader) catch |err| return decoder.err orelse err;
                return;
            }
            const compression = @import("compression.zig");
            if (compression.Reader.supported(format)) {
                var file_handle = try std.Io.Dir.cwd().openFile(self.io, path_name, .{});
                defer file_handle.close(self.io);
                var compressed_buffer: [65536]u8 = undefined;
                var decoded_buffer: [65536]u8 = undefined;
                var input = file_handle.reader(self.io, &compressed_buffer);
                var decoder = compression.Reader.init(self.a, &input.interface, format, &decoded_buffer, self.options.max_file_bytes);
                decoder.dictionary_bytes = dictionary;
                decoder.max_window_bytes = self.options.decompression_window;
                defer decoder.deinit();
                self.searchDecoded(display, &decoder.reader) catch |err| return decoder.err orelse err;
                return;
            }
            return error.UnsupportedCompression;
        };
        if (self.options.mmap or (self.options.mmap_auto and self.options.multiline and self.options.ask == null)) mapped: {
            var mapped_file = try std.Io.Dir.cwd().openFile(self.io, path_name, .{});
            defer mapped_file.close(self.io);
            const stat = try mapped_file.stat(self.io);
            if (stat.kind != .file) break :mapped;
            if (stat.size > self.options.max_file_bytes) return error.StreamTooLong;
            if (stat.size == 0) {
                try self.processBytes(display, "");
                return;
            }
            var mapping = @import("filesystem.zig").Mapping.open(self.io, mapped_file, @intCast(stat.size)) catch |err| {
                if (self.options.mmap) return err;
                break :mapped;
            };
            defer mapping.deinit(self.io);
            self.replacement_file_context = @import("builtin").os.tag == .linux;
            try self.processBytes(display, mapping.memory);
            return;
        }
        if (!self.options.multiline and self.options.ask == null) {
            var file_handle = try std.Io.Dir.cwd().openFile(self.io, path_name, .{});
            defer file_handle.close(self.io);
            // Discovery already classified these entries as regular files.
            // Avoid a metadata syscall for every small repository file; explicit
            // paths still use size information to choose larger I/O buffers.
            const stat = if (self.explicit_file) try file_handle.stat(self.io) else null;
            const size = if (stat) |s| s.size else 0;
            // Upstream Linux ordinary-file replacement uses whole-file anchor
            // context under its automatic mmap policy. Preserve that visible
            // behavior even when our adaptive backend chooses buffered reads.
            self.replacement_file_context = @import("builtin").os.tag == .linux and (if (stat) |s| s.kind == .file else true) and (self.options.mmap or self.options.mmap_auto);
            const capacity: usize = if (size >= 8 * 1024 * 1024) 256 * 1024 else 64 * 1024;
            if (self.input_buffer == null or self.input_buffer.?.len < capacity) {
                const buffer = try self.a.alloc(u8, capacity);
                if (self.input_buffer) |previous| self.a.free(previous);
                self.input_buffer = buffer;
            }
            const buffer = self.input_buffer.?[0..capacity];
            var reader = file_handle.reader(self.io, buffer);
            _ = reader.interface.peek(4) catch &.{};
            try self.streamDecoded(display, &reader.interface);
            return;
        }
        const bytes = try std.Io.Dir.cwd().readFileAlloc(self.io, path_name, self.a, .limited(self.options.max_file_bytes));
        defer self.a.free(bytes);
        try self.processBytes(display, bytes);
    }
    fn binaryContent(self: *Search, path: []const u8, bytes: []const u8) !void {
        if (!self.explicit_file and !self.options.binary) {
            self.summary.files += 1;
            return;
        }
        self.binary_mode = true;
        self.binary_offset = std.mem.indexOfScalar(u8, bytes, 0);
        const changed = try self.a.dupe(u8, bytes);
        defer self.a.free(changed);
        for (changed) |*ch| if (ch.* == 0) {
            ch.* = '\n';
        };
        const before = self.summary.matches;
        if (self.options.multiline or self.options.ask != null) try self.content(path, changed) else {
            var reader = std.Io.Reader.fixed(changed);
            try self.stream(path, &reader);
        }
        if (self.summary.matches > before and !self.options.quiet and !self.options.json and !self.options.count and !self.options.count_matches and !self.options.files_with_matches and !self.options.files_without_match) {
            const text = if (self.render_filename) try std.fmt.allocPrint(self.a, "{s}: binary file matches (found \"\\0\" byte around offset {d})\n", .{ path, self.binary_offset.? }) else try std.fmt.allocPrint(self.a, "binary file matches (found \"\\0\" byte around offset {d})\n", .{self.binary_offset.?});
            defer self.a.free(text);
            try self.write(text);
        }
    }
    fn searchDecoded(self: *Search, path: []const u8, input: *std.Io.Reader) !void {
        if (self.options.multiline or self.options.ask != null) return self.snapshotDecoded(path, input);
        return self.streamDecoded(path, input);
    }
    fn snapshotDecoded(self: *Search, path: []const u8, input: *std.Io.Reader) !void {
        const old = self.transcoding;
        defer self.transcoding = old;
        _ = input.peek(4) catch |err| switch (err) {
            error.EndOfStream => &.{},
            else => return err,
        };
        self.transcoding = !std.mem.eql(u8, self.options.encoding, "none") and (!std.mem.eql(u8, self.options.encoding, "auto") or @import("antfly_encoding").hasBom(input.buffered()));
        var buffer: [65536]u8 = undefined;
        var decoder: ?@import("stream_encoding.zig").Reader = if (self.transcoding) try .init(self.a, input, self.options.encoding, &buffer, self.options.max_file_bytes) else null;
        defer if (decoder) |*d| d.deinit();
        const source = if (decoder) |*d| &d.reader else input;
        var snapshot = @import("snapshot.zig").Snapshot.read(self.a, self.io, source, self.byteLimit(), self.options.temporary_dir) catch |err| {
            if (decoder) |*d| return d.err orelse err;
            return err;
        };
        defer snapshot.deinit(self.a, self.io);
        if (!self.options.text and !self.options.null_data and std.mem.indexOfScalar(u8, snapshot.bytes, 0) != null) return self.binaryContent(path, snapshot.bytes);
        try self.content(path, snapshot.bytes);
    }
    fn processBytes(self: *Search, display: []const u8, bytes: []const u8) !void {
        const old = self.transcoding;
        self.transcoding = !std.mem.eql(u8, self.options.encoding, "none") and (!std.mem.eql(u8, self.options.encoding, "auto") or @import("antfly_encoding").hasBom(bytes));
        defer self.transcoding = old;
        const owned = if (self.transcoding) try @import("antfly_encoding").decode(self.a, bytes, self.options.encoding) else null;
        defer if (owned) |allocation| self.a.free(allocation);
        const decoded: []const u8 = owned orelse bytes;
        if (!self.options.text and !self.options.null_data and std.mem.indexOfScalar(u8, decoded, 0) != null) {
            try self.binaryContent(display, decoded);
            return;
        }
        if (self.options.multiline or self.options.ask != null) try self.content(display, decoded) else {
            var reader = std.Io.Reader.fixed(decoded);
            try self.stream(display, &reader);
        }
    }
    fn stdin(self: *Search) !void {
        self.replacement_file_context = false;
        self.binary_mode = false;
        self.binary_stop = false;
        self.binary_offset = null;
        const before = self.summary;
        self.file_elapsed_ns = null;
        self.file_before = before;
        self.file_output_start = self.output_bytes;
        self.file_started = if (self.options.json) std.Io.Clock.awake.now(self.io) else null;
        self.json_started = false;
        self.readStdin() catch |err| {
            try self.jsonEnd("<stdin>", before);
            return if (self.limits.cancel.load(.acquire)) error.Canceled else err;
        };
        try self.jsonEnd("<stdin>", before);
    }
    fn readStdin(self: *Search) !void {
        self.explicit_file = true;
        self.render_filename = self.options.filename orelse (self.options.paths.items.len > 1);
        self.header_printed = false;
        self.color_enabled = self.options.color == .always or (self.options.color == .auto and self.stdout_tty);
        var buffer: [16384]u8 = undefined;
        var reader = @import("interrupt.zig").Stdin.init(self.io, &(self.shared_limits orelse &self.limits).cancel, &buffer);
        const first = reader.interface.peekByte() catch |err| switch (err) {
            error.EndOfStream => 0,
            else => return err,
        };
        if (!std.mem.eql(u8, self.options.encoding, "auto") or first == 0xef or first == 0xff or first == 0xfe or first == 0) {
            try self.streamDecoded("<stdin>", &reader.interface);
            return;
        }
        if (!self.options.multiline) {
            try self.streamInput("<stdin>", &reader.interface);
            return;
        }
        const bytes = try reader.interface.allocRemaining(self.a, .limited(self.options.max_file_bytes));
        defer self.a.free(bytes);
        try self.content("<stdin>", bytes);
    }
    fn byteLimit(self: *const Search) usize {
        if (self.options.skip_oversize) return std.math.maxInt(usize);
        return if (self.transcoding) self.options.max_file_bytes *| 4 else self.options.max_file_bytes;
    }
    fn streamDecoded(self: *Search, path: []const u8, input: *std.Io.Reader) !void {
        if (!self.options.multiline and (std.mem.eql(u8, self.options.encoding, "auto") or std.mem.eql(u8, self.options.encoding, "none"))) {
            _ = input.peek(4) catch |err| switch (err) {
                error.EndOfStream => &.{},
                else => return err,
            };
            if (std.mem.eql(u8, self.options.encoding, "none") or !@import("antfly_encoding").hasBom(input.buffered())) return self.streamInput(path, input);
        }
        const old = self.transcoding;
        self.transcoding = true;
        defer self.transcoding = old;
        var buffer: [16384]u8 = undefined;
        var decoder = try @import("stream_encoding.zig").Reader.init(self.a, input, self.options.encoding, &buffer, self.options.max_file_bytes);
        defer decoder.deinit();
        if (self.options.multiline) {
            const bytes = decoder.reader.allocRemaining(self.a, .limited(self.options.max_file_bytes *| 4)) catch |err| return decoder.err orelse err;
            defer self.a.free(bytes);
            try self.content(path, bytes);
        } else self.streamInput(path, &decoder.reader) catch |err| return decoder.err orelse err;
    }
    const BinaryReader = struct {
        search: *Search,
        source: *std.Io.Reader,
        reader: std.Io.Reader,
        position: usize = 0,
        scanned_until: usize = 0,
        window_rejected: bool = false,
        scratch: []u8,
        fn readVec(r: *std.Io.Reader, data: [][]u8) std.Io.Reader.Error!usize {
            const self: *BinaryReader = @alignCast(@fieldParentPtr("reader", r));
            self.search.literal_buffer_rejected = false;
            if (r.buffer.ptr != self.scratch.ptr) {
                const remaining = r.buffered();
                @memcpy(self.scratch[0..remaining.len], remaining);
                r.buffer = self.scratch;
                r.seek = 0;
                r.end = remaining.len;
            }
            return std.Io.Reader.defaultReadVec(r, data);
        }
        fn stream(r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
            const self: *BinaryReader = @alignCast(@fieldParentPtr("reader", r));
            const search = self.search;
            search.literal_buffer_rejected = false;
            if (search.binary_stop) return error.EndOfStream;
            _ = try self.source.peekByte();
            const available = self.source.buffered();
            if (self.position == self.scanned_until) {
                const chunk = available[0..@min(65536, available.len)];
                self.scanned_until = self.position + chunk.len;
                self.window_rejected = chunk.len == 65536 and !search.binary_mode and search.options.ask == null and !search.options.invert and !search.options.passthru and search.options.before == 0 and search.options.after == 0 and search.matcher.rejectLiteralAndBinary(chunk);
                if (!self.window_rejected and search.binary_offset == null) if (std.mem.indexOfScalar(u8, chunk, 0)) |at| {
                    search.binary_offset = self.position + at;
                    if (!search.explicit_file and !search.options.binary) {
                        search.binary_stop = true;
                        return error.EndOfStream;
                    }
                    search.binary_mode = true;
                };
            }
            const n = limit.minInt(@min(available.len, self.scanned_until - self.position));
            const bytes = available[0..n];
            if (!search.binary_mode and r.seek == r.end and n == 65536) {
                // The source window stays stable until the next refill. Claim
                // one classified window and lend it directly to the scanner.
                r.buffer = bytes;
                r.seek = 0;
                r.end = n;
                // The proof belongs only to this borrowed window. readVec and
                // stream clear it before any refill or buffer concatenation.
                search.literal_buffer_rejected = self.window_rejected;
                self.source.toss(n);
                self.position += n;
                return 0;
            }
            var start: usize = 0;
            if (search.binary_mode) for (bytes, 0..) |ch, i| if (ch == 0) {
                try w.writeAll(bytes[start..i]);
                try w.writeByte('\n');
                start = i + 1;
            };
            try w.writeAll(bytes[start..]);
            self.source.toss(n);
            self.position += n;
            return n;
        }
    };
    fn streamInput(self: *Search, path: []const u8, input: *std.Io.Reader) !void {
        if (self.options.text or self.options.null_data or self.binary_mode) return self.stream(path, input);
        const before = self.summary.matches;
        var buffer: [65536]u8 = undefined;
        var checked = BinaryReader{ .search = self, .source = input, .scratch = &buffer, .reader = .{ .vtable = &.{ .stream = BinaryReader.stream, .readVec = BinaryReader.readVec }, .buffer = &buffer, .seek = 0, .end = 0 } };
        self.binary_checked = true;
        defer {
            self.binary_checked = false;
            self.literal_buffer_rejected = false;
        }
        try self.stream(path, &checked.reader);
        if (self.binary_offset != null and self.summary.matches > before and !self.options.quiet and !self.options.json and !self.options.count and !self.options.count_matches and !self.options.files_with_matches and !self.options.files_without_match) {
            const warning = if (self.binary_stop) "WARNING: stopped searching binary file after match" else "binary file matches";
            const message = if (self.render_filename) try std.fmt.allocPrint(self.a, "{s}: {s} (found \"\\0\" byte around offset {d})\n", .{ path, warning, self.binary_offset.? }) else try std.fmt.allocPrint(self.a, "{s} (found \"\\0\" byte around offset {d})\n", .{ warning, self.binary_offset.? });
            defer self.a.free(message);
            try self.write(message);
        }
    }

    fn stream(self: *Search, path_name: []const u8, reader: *std.Io.Reader) !void {
        self.stream_eof = false;
        const Saved = struct { bytes: []u8, line: usize, offset: usize };
        var history: std.ArrayList(Saved) = .empty;
        defer {
            for (history.items) |row| self.a.free(row.bytes);
            history.deinit(self.a);
        }
        const keep = @max(self.options.before, if (self.options.ask != null) self.options.context_lines else 0);
        var prefilter_checked_until: usize = 0;
        var line: usize = 0;
        var offset: usize = 0;
        var count: usize = 0;
        var match_count: usize = 0;
        var after: usize = 0;
        var shown: usize = 0;
        self.summary.files += 1;
        const delimiter: u8 = if (self.options.null_data) 0 else '\n';
        var storage: std.Io.Writer.Allocating = .init(self.a);
        defer storage.deinit();
        const context_output = !self.options.count and !self.options.count_matches and !self.options.quiet and !self.options.files_with_matches and !self.options.files_without_match and (self.options.json or self.options.only_matching or self.options.replacement != null or self.options.column or self.options.vimgrep or self.color_enabled);
        const context_limit = if (self.options.ask == null and (context_output or self.options.count_matches or (self.options.count and self.options.only_matching) or self.options.stats)) try self.matcher.renderContextBytes() else 0;
        var previous: std.ArrayList(u8) = .empty;
        defer previous.deinit(self.a);
        var render_subject: std.Io.Writer.Allocating = .init(self.a);
        defer render_subject.deinit();
        var hit_arena = std.heap.ArenaAllocator.init(self.a);
        defer hit_arena.deinit();
        defer self.current_hits = null;
        defer self.replay_spans = false;
        self.span_runs_valid = false;
        while (!self.stopped()) {
            self.current_hits = null;
            self.replay_spans = false;
            self.span_runs_valid = false;
            _ = hit_arena.reset(.retain_capacity);
            storage.clearRetainingCapacity();
            if (offset >= prefilter_checked_until and !(self.options.stop_on_nonmatch and count > 0) and self.options.ask == null and !self.options.invert and !self.options.passthru and self.options.before == 0 and after == 0) {
                _ = reader.peekByte() catch |err| switch (err) {
                    error.EndOfStream => {
                        self.stream_eof = true;
                        break;
                    },
                    else => return err,
                };
                const available = reader.buffered();
                const buffered = available[0..@min(256 * 1024, available.len)];
                if (std.mem.lastIndexOfScalar(u8, buffered, delimiter)) |last| {
                    const whole = buffered[0 .. last + 1];
                    prefilter_checked_until = offset + whole.len;
                    const rejected = if (self.binary_checked or self.options.text or self.options.null_data or std.mem.indexOfScalar(u8, whole, 0) == null) (if (self.literal_buffer_rejected) whole.len else self.matcher.rejectPrefix(whole, delimiter)) else 0;
                    if (rejected > 0) {
                        if (rejected > self.byteLimit() - offset) return error.StreamTooLong;
                        if (self.shared_limits) |limits| if (!Limits.reserve(&limits.source_bytes, rejected, self.options.max_source_bytes)) return error.SourceBudgetExceeded;
                        if (self.options.line_numbers or self.options.json or self.options.vimgrep or self.options.hyperlink_format != null or self.options.before > 0 or self.options.after > 0) line += std.mem.count(u8, whole[0..rejected], &.{delimiter});
                        offset += rejected;
                        self.summary.bytes += rejected;
                        reader.toss(rejected);
                        continue;
                    }
                }
            }

            if (reader.seek == reader.end) _ = reader.peekByte() catch |err| switch (err) {
                error.EndOfStream => {
                    self.stream_eof = true;
                    break;
                },
                else => return err,
            };
            const record_buffer_start = reader.seek;
            const record_buffer_available = reader.buffered().len;
            _ = try reader.streamDelimiterLimit(&storage.writer, delimiter, .limited(self.byteLimit() - offset));
            const peek = reader.peekByte() catch |err| switch (err) {
                error.EndOfStream => null,
                else => return err,
            };
            if (peek == null) self.stream_eof = true;
            if (peek == null and storage.written().len == 0) break;
            if (peek != null) {
                reader.toss(1);
                try storage.writer.writeByte(delimiter);
            }
            const raw = storage.written();
            self.replacement_absolute_start = if (self.replacement_file_context and !self.transcoding) offset == 0 else record_buffer_start == 0 or peek == null or raw.len > record_buffer_available;
            var text: []const u8 = if (peek != null) raw[0 .. raw.len - 1] else raw;
            if (self.options.crlf and std.mem.endsWith(u8, text, "\r")) text = text[0 .. text.len - 1];
            line += 1;
            if (self.shared_limits) |limits| if (!Limits.reserve(&limits.source_bytes, raw.len, self.options.max_source_bytes)) return error.SourceBudgetExceeded;
            self.summary.bytes += raw.len;
            if (!self.binary_checked and !self.options.text and !self.options.null_data and std.mem.indexOfScalar(u8, text, 0) != null) break;
            var probability = std.math.nan(f64);
            var matched = false;
            var exact_occurrences: ?usize = null;
            if (count < self.options.max_count) {
                if (self.options.ask != null) {
                    if (self.summary.regions_evaluated == self.options.max_regions) {
                        try self.reportError(path_name, error.RegionBudgetExceeded);
                        break;
                    }
                    var arena = std.heap.ArenaAllocator.init(self.a);
                    defer arena.deinit();
                    const a = arena.allocator();
                    var listing: std.Io.Writer.Allocating = .init(a);
                    try listing.writer.print("File: {s}\n", .{path_name});
                    for (history.items) |row| try listing.writer.print("CONTEXT {d}: {s}\n", .{ row.line, std.mem.trimEnd(u8, row.bytes, "\r\n") });
                    try listing.writer.print("TARGET {d}: {s}\n", .{ line, text });
                    if (!std.unicode.utf8ValidateSlice(listing.written())) return error.DecisionInputNotUtf8;
                    const id = try std.fmt.allocPrint(a, "line-{d}", .{line});
                    var scores: [1]f64 = undefined;
                    self.summary.regions_considered += 1;
                    try self.decider.search(&.{.{ .id = id, .input = listing.written() }}, &scores);
                    self.summary.regions_evaluated += 1;
                    probability = scores[0];
                    matched = scores[0] >= 0 and ((scores[0] >= self.options.threshold) != self.options.invert);
                } else if (context_limit == 0 and !self.options.invert and self.options.replacement == null and !self.options.vimgrep and (self.options.json or self.options.only_matching or self.color_enabled)) {
                    self.replay_spans = true;
                    if (self.options.only_matching and !self.options.json and !self.options.stats and !self.options.count and !self.options.count_matches) {
                        // Output enumerates spans once. Plain match-only output
                        // needs no preliminary full-row occurrence count.
                        matched = try self.matcher.matches(text);
                    } else if (self.matcher.isLiteralSearch()) {
                        const counted = try self.matcher.count(text, std.math.maxInt(usize), peek != null);
                        matched = counted.total != 0;
                        exact_occurrences = counted.total - @intFromBool(counted.terminal_empty);
                    } else {
                        var spans = SpanIterator{ .matcher = &self.matcher, .text = text, .terminated = peek != null };
                        var total: usize = 0;
                        self.span_run_count = 0;
                        self.span_runs_valid = true;
                        while (spans.next()) |span| {
                            self.appendSpanRun(span);
                            total += 1;
                        }
                        if (self.matcher.failure) |err| return err;
                        matched = spans.found;
                        exact_occurrences = total;
                    }
                } else if (!self.options.json and (self.options.count_matches or (self.options.count and (self.options.only_matching or self.options.stats)) or (self.options.stats and !self.options.only_matching and self.options.replacement == null and !self.color_enabled and !self.options.column and !self.options.vimgrep))) {
                    const limit = if (self.options.quiet or self.options.files_with_matches or self.options.invert) @as(usize, 1) else std.math.maxInt(usize);
                    const counted = try self.matcher.count(text, limit, peek != null);
                    matched = (counted.total > 0) != self.options.invert;
                    exact_occurrences = counted.total - (if (!self.options.count and !self.options.count_matches and counted.terminal_empty) @as(usize, 1) else 0);
                    if (limit == 1 and !self.options.invert and matched) self.summary.occurrences_exact = false;
                } else if (!self.options.stats and !self.options.json and !self.options.count_matches and !(self.options.count and self.options.only_matching) and !self.options.only_matching and self.options.replacement == null and !self.color_enabled and !self.options.column and !self.options.vimgrep) {
                    matched = (try self.matcher.matches(text)) != self.options.invert;
                } else {
                    const ha = hit_arena.allocator();
                    var hits: std.ArrayList(@import("matcher.zig").Match) = .empty;
                    var hit_found = false;
                    var cursor: usize = 0;
                    var previous_end: ?usize = null;
                    while (self.matcher.find(text, cursor)) |hit| {
                        hit_found = true;
                        const duplicate = hit.span.start == hit.span.end and previous_end != null and previous_end.? == hit.span.end;
                        if (!duplicate and !(peek == null and hit.span.start == text.len and hit.span.end == text.len)) {
                            const captures: []const ?@import("matcher.zig").Span = if (self.options.replacement != null) try ha.dupe(?@import("matcher.zig").Span, hit.captures) else &.{};
                            try hits.append(ha, .{ .span = hit.span, .captures = captures, .names = hit.names });
                            previous_end = hit.span.end;
                        }
                        cursor = if (hit.span.start == hit.span.end) self.matcher.advance(text, hit.span.end) else hit.span.end;
                        if (cursor > text.len or (peek == null and cursor == text.len)) break;
                        if (self.options.quiet or self.options.files_with_matches or self.options.invert or (self.firstSpanOnly() and hits.items.len > 0)) break;
                    }
                    if (self.matcher.failure) |err| return err;
                    if (self.firstSpanOnly() and self.options.stats) {
                        const counted = try self.matcher.count(text, std.math.maxInt(usize), peek != null);
                        exact_occurrences = counted.total - (if (counted.terminal_empty) @as(usize, 1) else 0);
                    }
                    self.current_hits = hits.items;
                    self.current_hit_text = text;
                    matched = hit_found != self.options.invert;
                }
            }
            if (matched and !self.options.invert and !self.replacement_absolute_start and context_limit > 0 and previous.items.len > 0) {
                // Selection uses an isolated record. Capture rendering uses the
                // bounded preceding source window, as ripgrep's capture iterator
                // does, so assertions at the start of selected records see context.
                const base = if (self.replacement_file_context and !self.transcoding) previous.items.len else @min(previous.items.len, record_buffer_start);
                // Borrow contiguous input while the reader window is stable.
                // This also avoids copying large explicitly mapped records.
                const subject = if (base <= record_buffer_start and raw.len <= record_buffer_available and record_buffer_start + text.len <= reader.end)
                    reader.buffer[record_buffer_start - base .. record_buffer_start + text.len]
                else copy: {
                    render_subject.clearRetainingCapacity();
                    try render_subject.writer.writeAll(previous.items[previous.items.len - base ..]);
                    try render_subject.writer.writeAll(text);
                    break :copy render_subject.written();
                };
                self.matcher.allowAbsoluteStart(offset == base);
                defer self.matcher.allowAbsoluteStart(true);
                const ha = hit_arena.allocator();
                var contextual: std.ArrayList(@import("matcher.zig").Match) = .empty;
                var cursor = base;
                var prior_end: ?usize = null;
                var total: usize = 0;
                while (self.matcher.find(subject, cursor)) |hit| {
                    const duplicate = hit.span.start == hit.span.end and prior_end != null and prior_end.? == hit.span.end;
                    if (!duplicate and !(peek == null and hit.span.start == subject.len and hit.span.end == subject.len)) {
                        total += 1;
                        if (context_output and (!self.firstSpanOnly() or contextual.items.len == 0)) try contextual.append(ha, .{ .span = .{ .start = hit.span.start - base, .end = hit.span.end - base }, .captures = if (self.options.replacement != null) try ha.dupe(?@import("matcher.zig").Span, hit.captures) else &.{}, .names = hit.names, .capture_text = subject });
                        prior_end = hit.span.end;
                    }
                    cursor = if (hit.span.start == hit.span.end) self.matcher.advance(subject, hit.span.end) else hit.span.end;
                    if (cursor > subject.len or (peek == null and cursor == subject.len) or (self.firstSpanOnly() and !self.options.stats and contextual.items.len > 0)) break;
                }
                if (self.matcher.failure) |err| return err;
                exact_occurrences = total;
                if (context_output) {
                    self.current_hits = contextual.items;
                    self.current_hit_text = text;
                }
            }
            if (matched) {
                count += 1;
                if (count == 1) self.summary.files_with_matches += 1;
                if (!self.options.files_without_match) self.summary.matches += 1;
                if (self.options.ask == null and !self.options.invert and (self.current_hits == null or self.firstSpanOnly()) and exact_occurrences == null) self.summary.occurrences_exact = false;
                const occurrences = if (self.options.ask != null or self.options.invert) @as(usize, 1) else (exact_occurrences orelse (if (self.current_hits) |hits| hits.len else 1));
                match_count += occurrences;
                self.summary.occurrences += occurrences;
                if (self.options.quiet) {
                    self.stop = true;
                    return;
                }
                if (self.options.files_with_matches) {
                    try self.write(path_name);
                    try self.write(if (self.options.null_paths) "\x00" else "\n");
                    return;
                }
            }
            if (!self.options.count and !self.options.count_matches and !self.options.files_without_match) {
                if (matched) {
                    if (self.options.before > 0) {
                        const first = history.items.len - @min(history.items.len, self.options.before);
                        for (history.items[first..]) |row| if (row.line > shown) {
                            if (shown > 0 and row.line > shown + 1 and self.options.context_separator.len > 0) {
                                try self.write(self.options.context_separator);
                                try self.write("\n");
                            }
                            self.raw_line = row.bytes;
                            try self.emit(path_name, std.mem.trimEnd(u8, row.bytes, if (self.options.crlf) "\r\n" else "\n"), row.line, row.offset, false, std.math.nan(f64));
                            shown = row.line;
                        };
                    }
                    if (shown > 0 and line > shown + 1 and (self.options.before > 0 or self.options.after > 0) and self.options.context_separator.len > 0) {
                        try self.write(self.options.context_separator);
                        try self.write("\n");
                    }
                    self.raw_line = raw;
                    self.row_occurrences = exact_occurrences orelse if (self.current_hits) |hits| hits.len else 1;
                    try self.emit(path_name, text, line, offset, true, probability);
                    shown = line;
                    after = self.options.after;
                } else if (after > 0 or self.options.passthru) {
                    self.raw_line = raw;
                    try self.emit(path_name, text, line, offset, false, probability);
                    shown = line;
                    after -|= 1;
                }
            }
            if (context_limit > 0) {
                if (raw.len >= context_limit) {
                    previous.clearRetainingCapacity();
                    try previous.appendSlice(self.a, raw[raw.len - context_limit ..]);
                } else {
                    const remove = (previous.items.len + raw.len) -| context_limit;
                    std.mem.copyForwards(u8, previous.items[0 .. previous.items.len - remove], previous.items[remove..]);
                    previous.shrinkRetainingCapacity(previous.items.len - remove);
                    try previous.appendSlice(self.a, raw);
                }
            }
            offset += raw.len;
            if (keep > 0) {
                try history.append(self.a, .{ .bytes = try self.a.dupe(u8, raw), .line = line, .offset = offset - raw.len });
                while (history.items.len > keep) {
                    const old = history.orderedRemove(0);
                    self.a.free(old.bytes);
                }
            }
            if (self.options.stop_on_nonmatch and count > 0 and !matched) break;
            if (count == self.options.max_count and after == 0) break;
            if (peek == null) break;
        }
        self.raw_line = null;
        if (self.binary_stop and (self.options.count or self.options.count_matches or self.options.files_without_match)) {
            self.summary.matches -= if (self.options.files_without_match) 0 else count;
            self.summary.occurrences -= match_count;
            self.summary.files_with_matches -= if (count > 0) @as(usize, 1) else 0;
            return;
        }
        if (self.options.files_without_match) {
            if (count == 0) {
                try self.write(path_name);
                try self.write(if (self.options.null_paths) "\x00" else "\n");
                self.summary.matches += 1;
            }
            return;
        }
        if (self.options.count or self.options.count_matches) {
            const n = if ((self.options.count_matches or self.options.only_matching) and !self.options.invert and self.options.ask == null) match_count else count;
            if (n > 0 or self.options.include_zero) {
                const out = if (self.render_filename) try std.fmt.allocPrint(self.a, "{s}{s}{d}\n", .{ path_name, if (self.options.null_paths) "\x00" else self.options.field_match_separator, n }) else try std.fmt.allocPrint(self.a, "{d}\n", .{n});
                defer self.a.free(out);
                try self.write(out);
            }
        }
    }
    pub fn content(self: *Search, path_name: []const u8, bytes: []const u8) !void {
        self.replay_spans = false;
        self.span_runs_valid = false;
        self.current_hits = null;
        if (self.shared_limits) |limits| if (!Limits.reserve(&limits.source_bytes, bytes.len, self.options.max_source_bytes)) return error.SourceBudgetExceeded;
        self.summary.files += 1;
        self.summary.bytes += bytes.len;
        if (self.options.ask != null and !std.unicode.utf8ValidateSlice(bytes)) {
            self.summary.invalid_utf8_files += 1;
            return;
        }
        if (!self.options.text and !self.options.null_data and std.mem.indexOfScalar(u8, bytes, 0) != null) return;
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        var prepared: ?@import("source_cache.zig").Prepared = null;
        const lines = if (self.options.ask != null) blk: {
            var hit = false;
            prepared = if (self.workspace_entry) |entry| blk2: {
                if (!self.options.crlf and !self.options.null_data and bytes.ptr == entry.bytes.ptr and bytes.len == entry.bytes.len and entry.prepared != null) {
                    hit = true;
                    break :blk2 entry.prepared.?;
                }
                break :blk2 try @import("source_cache.zig").get(a, self.io, self.options, path_name, bytes, self.decider.cache_scope, &hit, &self.source_cache_state);
            } else try @import("source_cache.zig").get(a, self.io, self.options, path_name, bytes, self.decider.cache_scope, &hit, &self.source_cache_state);
            self.summary.source_cache_hits += @intFromBool(hit);
            self.summary.source_parses += @intFromBool(!hit);
            break :blk prepared.?.lines;
        } else try lineRangesWith(a, bytes, if (self.options.null_data) 0 else '\n', self.options.crlf);
        const selected = try a.alloc(bool, lines.len);
        @memset(selected, false);
        const probabilities: []f64 = if (self.options.ask != null) try a.alloc(f64, lines.len) else &.{};
        @memset(probabilities, std.math.nan(f64));
        self.evidence = null;
        defer self.evidence = null;
        if (self.options.ask != null) {
            const evidence = try a.create(@import("evidence.zig").Evidence);
            const scores = try a.alloc(f64, lines.len);
            @memset(scores, std.math.nan(f64));
            evidence.* = .{ .index = prepared.?.index, .line_scores = scores };
            self.evidence = evidence;
            if (self.options.jev_mode) {
                const listing = try self.filePreview(a, path_name, bytes, lines, evidence.index);
                const old = self.decider.stage;
                self.decider.stage = .file;
                defer self.decider.stage = old;
                var score: [1]f64 = undefined;
                try self.decider.search(&.{.{ .id = "file-preview", .input = listing }}, &score);
                evidence.file_score = score[0];
                if (score[0] < self.options.file_threshold and !self.options.broad) return;
            }
        }
        var multiline_hits: std.ArrayList(@import("matcher.zig").Match) = .empty;
        var multiline_groups: usize = 0;
        var multiline_occurrences: usize = 0;
        const retain_multiline_hits = !self.options.count and !self.options.count_matches and !self.options.quiet and !self.options.files_with_matches and !self.options.files_without_match;
        if (self.options.multiline and self.options.ask == null) {
            const count_each_match = self.matcher.multilineUsesMatches();
            var offset: usize = 0;
            var last_line: ?usize = null;
            var previous_end: ?usize = null;
            while (self.matcher.find(bytes, offset)) |hit| {
                if (lines.len == 0 or (hit.span.start == bytes.len and hit.span.end == bytes.len and (bytes[bytes.len - 1] == '\n' or (self.options.null_data and bytes[bytes.len - 1] == 0)))) break;
                if (hit.span.start == hit.span.end and previous_end != null and previous_end.? == hit.span.end) {
                    offset = self.matcher.advance(bytes, hit.span.end);
                    if (offset > bytes.len) break;
                    continue;
                }
                const first = lineIndex(lines, hit.span.start);
                const last = lineIndex(lines, if (hit.span.end > hit.span.start) hit.span.end - 1 else hit.span.end);
                if (count_each_match or last_line == null or first > last_line.?) {
                    if (multiline_groups == self.options.max_count) break;
                    multiline_groups += 1;
                }
                last_line = last;
                previous_end = hit.span.end;
                const terminal_empty = hit.span.start == bytes.len and hit.span.end == bytes.len;
                if (!terminal_empty or self.options.count or self.options.count_matches) multiline_occurrences += 1;
                if (retain_multiline_hits and !terminal_empty) {
                    const captures: []const ?@import("matcher.zig").Span = if (self.options.replacement != null) try a.dupe(?@import("matcher.zig").Span, hit.captures) else &.{};
                    try multiline_hits.append(a, .{ .span = hit.span, .captures = captures, .names = hit.names });
                }
                if (lines.len > 0) @memset(selected[first .. last + 1], true);
                if (hit.span.end == hit.span.start) offset = self.matcher.advance(bytes, hit.span.end) else offset = hit.span.end;
                if (offset > bytes.len or (offset == bytes.len and bytes.len > 0 and bytes[bytes.len - 1] != (if (self.options.null_data) @as(u8, 0) else @as(u8, '\n')))) break;
            }
            if (self.matcher.failure) |err| return err;
            if (self.options.invert) for (selected) |*yes| {
                yes.* = !yes.*;
            };
        } else if (self.options.ask != null) {
            self.evaluate(a, path_name, bytes, lines, selected, probabilities) catch |err| {
                try self.reportError(path_name, err);
                // Keep successful earlier batches, but do not keep sending
                // the rest of the tree to a failed or incompatible provider.
                self.stop = true;
            };
        } else {
            for (lines, 0..) |line, i| {
                selected[i] = (try self.matcher.matches(bytes[line.start..line.end])) != self.options.invert;
            }
        }
        var count: usize = 0;
        for (selected) |*yes| {
            if (!yes.*) continue;
            if (!(self.options.multiline and self.options.ask == null and !self.options.invert) and count == self.options.max_count) yes.* = false else count += 1;
        }
        if (self.options.jev_mode and self.evidence != null and !self.options.quiet and !self.options.count and !self.options.files_with_matches) {
            if (count == 0) {
                const preview = self.evidence.?.file_score orelse 0;
                if (preview < self.options.file_threshold) return;
                self.best_probability = @max(self.best_probability, preview);
                self.summary.reading_leads += 1;
                self.summary.files_with_matches += 1;
            }
            self.summary.matches += count;
            self.summary.occurrences += count;
            if (count > 0) self.summary.files_with_matches += 1;
            if (self.ranking != null) {
                try self.retainCandidate(a, path_name, bytes, lines, selected, probabilities, true);
                return;
            }
            try self.renderEvidence(a, path_name, bytes, lines, selected);
            return;
        }
        if (count > 0) self.summary.files_with_matches += 1;
        var matched_occurrences = count;
        if (self.options.ask == null and !self.options.invert) {
            if (self.options.multiline) {
                matched_occurrences = multiline_occurrences;
            } else if (self.options.stats or self.options.json or self.options.count_matches or (self.options.count and self.options.only_matching)) {
                matched_occurrences = 0;
                for (lines, selected, 0..) |line, yes, i| {
                    const terminated = i + 1 < lines.len or (bytes.len > 0 and bytes[bytes.len - 1] == (if (self.options.null_data) @as(u8, 0) else @as(u8, '\n')));
                    if (yes) {
                        const counted = try self.matcher.count(bytes[line.start..line.end], std.math.maxInt(usize), terminated);
                        matched_occurrences += counted.total - (if (!self.options.count and !self.options.count_matches and counted.terminal_empty) @as(usize, 1) else 0);
                    }
                }
            } else if (count > 0) self.summary.occurrences_exact = false;
        }
        self.summary.occurrences += matched_occurrences;
        if (!self.options.files_without_match) self.summary.matches += count;
        if (self.options.quiet) {
            if (count > 0) self.stop = true;
            return;
        }
        if (self.options.files_with_matches or self.options.files_without_match) {
            if ((count > 0) != self.options.files_without_match) {
                if (self.options.files_without_match) self.summary.matches += 1;
                try self.write(path_name);
                try self.write(if (self.options.null_paths) "\x00" else "\n");
            }
            return;
        }
        if (self.options.count or self.options.count_matches) {
            if (self.options.multiline and self.options.ask == null and !self.options.invert) count = multiline_groups;
            if ((self.options.count_matches or self.options.only_matching) and !self.options.invert and self.options.ask == null) {
                count = matched_occurrences;
            }
            if (count == 0 and !self.options.include_zero) return;
            const output = if (self.render_filename) try std.fmt.allocPrint(a, "{s}{s}{d}\n", .{ path_name, if (self.options.null_paths) "\x00" else self.options.field_match_separator, count }) else try std.fmt.allocPrint(a, "{d}\n", .{count});
            try self.write(output);
            return;
        }
        if (self.options.json and self.options.multiline and self.options.ask == null and !self.options.invert) {
            try self.renderMultilineJson(a, path_name, bytes, lines, selected, multiline_hits.items);
            return;
        }
        if (self.options.multiline and self.options.only_matching and !self.options.invert) {
            for (multiline_hits.items) |hit| {
                if (hit.span.start == hit.span.end and self.options.replacement == null) continue;
                var line_no = lineIndex(lines, hit.span.start) + 1;
                const rendered = if (self.options.replacement) |template| try Matcher.replacement(a, template, bytes, hit) else bytes[hit.span.start..hit.span.end];
                var split = std.mem.splitScalar(u8, rendered, '\n');
                while (split.next()) |row| {
                    defer line_no += 1;
                    if (row.len == 0) continue;
                    var output: std.Io.Writer.Allocating = .init(a);
                    try self.prefix(&output.writer, path_name, line_no, hit.span.start, 1, true);
                    if (self.color_enabled) try @import("style.zig").start(&output.writer, self.options, "match");
                    try output.writer.writeAll(row);
                    if (self.color_enabled) try output.writer.writeAll("\x1b[0m");
                    try output.writer.writeByte('\n');
                    try self.write(output.written());
                }
            }
            return;
        }
        if (self.options.multiline and self.options.replacement != null and !self.options.invert) {
            try self.renderMultilineReplacement(a, path_name, bytes, lines, selected, multiline_hits.items);
            return;
        }
        if (self.ranking != null) {
            if (count > 0) try self.retainCandidate(a, path_name, bytes, lines, selected, probabilities, false);
            return;
        }
        try self.renderSelectedLines(a, path_name, bytes, lines, selected, probabilities);
    }
    fn retainCandidate(self: *Search, scratch: Allocator, path: []const u8, bytes: []const u8, lines: []const Line, selected: []const bool, probabilities: []const f64, jev: bool) !void {
        const ranking = self.ranking.?;
        if (!ranking.consider(self.best_probability, path, self.result_ordinal)) return;
        errdefer self.retaining_output = false;
        if (self.options.max_pending_output <= @sizeOf(Candidate)) return error.PendingOutputBudgetExceeded;
        const candidate = try self.a.create(Candidate);
        candidate.* = .{ .a = self.a, .budget = .{ .backing = self.a, .limit = self.options.max_pending_output - @sizeOf(Candidate) }, .arena = undefined };
        candidate.arena = std.heap.ArenaAllocator.init(candidate.budget.allocator());
        errdefer candidate.deinit();
        self.fillCandidate(candidate, scratch, path, bytes, lines, selected, probabilities, jev) catch |err| return if (candidate.budget.denied) error.PendingOutputBudgetExceeded else err;
        self.deferred_candidate = candidate;
    }
    fn fillCandidate(self: *Search, c: *Candidate, scratch: Allocator, path: []const u8, bytes: []const u8, lines: []const Line, selected: []const bool, probabilities: []const f64, jev: bool) !void {
        const a = c.arena.allocator();
        c.path = try a.dupe(u8, path);
        c.provenance = try retainedClone(@TypeOf(self.provenance), a, self.provenance, 0);
        c.score = self.best_probability;
        c.ordinal = self.result_ordinal;
        c.before = self.file_before;
        c.filename = self.render_filename;
        c.color = self.color_enabled;

        if (jev) {
            c.view = try retainedClone(EvidenceView, a, try self.evidenceView(scratch, bytes, lines, selected), 0);
        } else {
            var rows: std.ArrayList(RenderRow) = .empty;
            var last: ?usize = null;
            const shown = try @import("evidence.zig").contextMask(scratch, selected, self.options.before, self.options.after);
            for (lines, shown, 0..) |line, show, i| {
                if (!show) continue;
                const p = probabilities[i];
                const independent = if (self.evidence) |e| std.math.isFinite(e.line_scores[i]) and e.line_scores[i] >= 0 and e.line_scores[i] == p else true;
                try rows.append(a, .{ .raw = try a.dupe(u8, bytes[line.start..if (i + 1 < lines.len) lines[i + 1].start else bytes.len]), .length = line.end - line.start, .line = i + 1, .offset = line.start, .matched = selected[i], .probability = p, .independent = independent, .gap = last != null and i > last.? + 1 and (self.options.before > 0 or self.options.after > 0) });
                last = i;
            }
            c.rows = rows.items;
        }
    }
    fn emitRanked(self: *Search) !void {
        const ranking = self.ranking.?;
        ranking.sort();
        if (self.options.jev_mode and !self.options.json) {
            const header = try std.fmt.allocPrint(self.a, "Search: {s}\n{d} files searched; {d} selected source lines; {s}\n", .{ self.options.ask.?, self.summary.files, self.summary.matches, if (self.summary.complete) "complete" else "incomplete" });
            defer self.a.free(header);
            try self.write(header);
            for (ranking.items.items) |c| {
                const row = try std.fmt.allocPrint(self.a, "  {s}  {d:.3}\n", .{ c.path, c.score });
                defer self.a.free(row);
                try self.write(row);
            }
            try self.write("\n");
        }
        var renderer = try Search.initShared(self);
        defer renderer.deinit();
        var render_options = self.options.*;
        renderer.options = &render_options;
        var forward = ForwardSink{ .search = self, .writer = .{ .vtable = &.{ .drain = ForwardSink.drain }, .buffer = &.{} } };
        forward.started = true;
        renderer.sink = &forward.writer;
        for (ranking.items.items) |c| {
            try self.rankedBoundary(c.filename);
            const render_started = std.Io.Clock.awake.now(self.io);
            render_options.max_output_bytes = self.options.max_output_bytes - self.output_bytes;
            renderer.summary = c.summary;
            renderer.best_probability = c.score;
            renderer.provenance = c.provenance;
            renderer.render_filename = c.filename;
            renderer.color_enabled = c.color;
            renderer.header_printed = false;
            renderer.file_started = null;
            renderer.file_elapsed_ns = c.elapsed_ns;
            renderer.file_output_start = 0;
            renderer.output_bytes = 0;
            renderer.json_started = false;
            if (c.view) |view| {
                var arena = std.heap.ArenaAllocator.init(self.a);
                defer arena.deinit();
                renderer.renderEvidenceView(arena.allocator(), c.path, view) catch |err| return forward.failure orelse err;
            } else for (c.rows) |row| {
                if (row.gap and !self.options.json and self.options.context_separator.len > 0) {
                    try renderer.write(self.options.context_separator);
                    try renderer.write("\n");
                }
                renderer.raw_line = row.raw;
                renderer.independent_override = row.independent;
                renderer.emit(c.path, row.raw[0..row.length], row.line, row.offset, row.matched, row.probability) catch |err| return forward.failure orelse err;
            }
            // Add actual rendering work, excluding the wait for other candidates.
            renderer.file_elapsed_ns = c.elapsed_ns + @as(i96, @intCast(@max(0, render_started.durationTo(std.Io.Clock.awake.now(self.io)).nanoseconds)));
            renderer.jsonEnd(c.path, c.before) catch |err| return forward.failure orelse err;
        }
        const omitted = ranking.considered - ranking.items.items.len;
        if (omitted > 0) {
            if (self.options.json) try self.json(.{ .type = "omitted_files", .count = omitted }) else {
                const row = try std.fmt.allocPrint(self.a, "[omitted {d} qualifying files; increase -n/--max-result-files]\n", .{omitted});
                defer self.a.free(row);
                try self.write(row);
            }
        }
    }
    fn renderSelectedLines(self: *Search, a: Allocator, path_name: []const u8, bytes: []const u8, lines: []const Line, selected: []const bool, probabilities: []const f64) !void {
        var last_shown: ?usize = null;
        const shown = try @import("evidence.zig").contextMask(a, selected, self.options.before, self.options.after);
        for (lines, shown, 0..) |line, show, i| {
            if (!show) continue;
            if (!self.options.json and last_shown != null and i > last_shown.? + 1 and (self.options.before > 0 or self.options.after > 0) and self.options.context_separator.len > 0) {
                try self.write(self.options.context_separator);
                try self.write("\n");
            }
            self.raw_line = bytes[line.start..if (i + 1 < lines.len) lines[i + 1].start else bytes.len];
            try self.emit(path_name, bytes[line.start..line.end], i + 1, line.start, selected[i], if (probabilities.len > 0) probabilities[i] else std.math.nan(f64));
            last_shown = i;
        }
    }
    fn renderMultilineJson(self: *Search, a: Allocator, path: []const u8, bytes: []const u8, lines: []const Line, selected: []const bool, hits: []const @import("matcher.zig").Match) !void {
        defer self.current_hits = null;
        var i: usize = 0;
        var hit_index: usize = 0;
        while (i < lines.len) {
            const first = i;
            if (!selected[i]) {
                var show = false;
                const low = i -| self.options.after;
                const high = i + @min(self.options.before, lines.len - i - 1) + 1;
                for (selected[low..high]) |yes| if (yes) {
                    show = true;
                    break;
                };
                i += 1;
                if (!show) continue;
                self.raw_line = bytes[lines[first].start..if (i < lines.len) lines[i].start else bytes.len];
                try self.emit(path, bytes[lines[first].start..lines[first].end], first + 1, lines[first].start, false, std.math.nan(f64));
                continue;
            }
            const start_hit = hit_index;
            var end_line = i;
            var spanning = self.matcher.joinsMultilineJson();
            while (hit_index < hits.len) {
                const hit = hits[hit_index];
                const start_line = lineIndex(lines, hit.span.start);
                const last = lineIndex(lines, if (hit.span.end > hit.span.start) hit.span.end - 1 else hit.span.end);
                if (start_line > end_line and !(spanning and start_line == end_line + 1)) break;
                end_line = @max(end_line, last);
                spanning = spanning or last > start_line;
                hit_index += 1;
            }
            i = end_line + 1;
            const raw_end = if (i < lines.len) lines[i].start else bytes.len;
            const relative = try a.alloc(@import("matcher.zig").Match, hit_index - start_hit);
            for (hits[start_hit..hit_index], relative) |hit, *target| {
                target.* = hit;
                target.span = .{ .start = hit.span.start - lines[first].start, .end = hit.span.end - lines[first].start };
            }
            // A match may include the record terminator itself.
            const rendered = bytes[lines[first].start..@max(lines[end_line].end, if (relative.len > 0) hits[hit_index - 1].span.end else lines[end_line].end)];
            self.current_hits = relative;
            self.current_hit_text = rendered;
            self.raw_line = bytes[lines[first].start..raw_end];
            try self.emit(path, rendered, first + 1, lines[first].start, true, std.math.nan(f64));
        }
    }
    fn renderMultilineReplacement(self: *Search, a: Allocator, path: []const u8, bytes: []const u8, lines: []const Line, selected: []const bool, hits: []const @import("matcher.zig").Match) !void {
        var i: usize = 0;
        var last_shown: ?usize = null;
        while (i < lines.len) {
            if (!selected[i]) {
                const first = i - @min(i, self.options.after);
                const end = i + @min(self.options.before +| 1, lines.len - i);
                var context = false;
                for (selected[first..end]) |yes| if (yes) {
                    context = true;
                    break;
                };
                if (context) {
                    if (last_shown != null and i > last_shown.? + 1 and self.options.context_separator.len > 0) {
                        try self.write(self.options.context_separator);
                        try self.write("\n");
                    }
                    try self.emit(path, bytes[lines[i].start..lines[i].end], i + 1, lines[i].start, false, std.math.nan(f64));
                    last_shown = i;
                }
                i += 1;
                continue;
            }
            var end = i + 1;
            while (end < lines.len and selected[end]) end += 1;
            if (last_shown != null and i > last_shown.? + 1 and (self.options.before > 0 or self.options.after > 0) and self.options.context_separator.len > 0) {
                try self.write(self.options.context_separator);
                try self.write("\n");
            }
            const begin_byte = lines[i].start;
            const end_byte = lines[end - 1].end;
            var changed: std.Io.Writer.Allocating = .init(a);
            var spans: std.ArrayList(@import("matcher.zig").Span) = .empty;
            var cursor = begin_byte;
            for (hits) |hit| {
                if (hit.span.start < begin_byte or hit.span.end > end_byte or hit.span.start < cursor) continue;
                try changed.writer.writeAll(bytes[cursor..hit.span.start]);
                const start = changed.written().len;
                try changed.writer.writeAll(try Matcher.replacement(a, self.options.replacement.?, bytes, hit));
                try spans.append(a, .{ .start = start, .end = changed.written().len });
                cursor = hit.span.end;
            }
            try changed.writer.writeAll(bytes[cursor..end_byte]);
            var split = std.mem.splitScalar(u8, changed.written(), '\n');
            var row: usize = 0;
            var offset: usize = 0;
            while (split.next()) |text| {
                var out: std.Io.Writer.Allocating = .init(a);
                try self.prefix(&out.writer, path, i + row + 1, begin_byte + offset, 1, true);
                var at: usize = 0;
                for (spans.items) |span| {
                    const first = @max(span.start, offset);
                    const stop = @min(span.end, offset + text.len);
                    if (stop <= first) continue;
                    try out.writer.writeAll(text[at .. first - offset]);
                    if (self.color_enabled) try @import("style.zig").start(&out.writer, self.options, "match");
                    try out.writer.writeAll(text[first - offset .. stop - offset]);
                    if (self.color_enabled) try out.writer.writeAll("\x1b[0m");
                    at = stop - offset;
                }
                try out.writer.writeAll(text[at..]);
                try out.writer.writeByte('\n');
                try self.write(out.written());
                row += 1;
                offset += text.len + 1;
            }
            last_shown = end - 1;
            i = end;
        }
    }
    fn filePreview(self: *Search, a: Allocator, path: []const u8, bytes: []const u8, lines: []const Line, index: @import("source.zig").Index) ![]const u8 {
        _ = self;
        var listing: std.Io.Writer.Allocating = .init(a);
        try listing.writer.print("TARGET file: {s}\n", .{path});
        for (index.declarations) |decl| try listing.writer.print("TARGET declaration {d}-{d}: {s} {s}\n", .{ decl.start, decl.end, decl.kind, decl.name });
        const n = @min(128, lines.len);
        for (0..n) |sample| {
            const i = if (n == lines.len) sample else sample * (lines.len - 1) / @max(1, n - 1);
            const line = lines[i];
            const text = bytes[line.start..line.end];
            if (listing.written().len + text.len > 128 * 1024) break;
            try listing.writer.print("TARGET {d}: {s}\n", .{ i + 1, text });
        }
        return listing.toOwnedSlice();
    }
    fn evidenceView(self: *Search, a: Allocator, bytes: []const u8, lines: []const Line, selected: []const bool) !EvidenceView {
        const evidence = self.evidence.?;
        const any_selected = std.mem.indexOfScalar(bool, selected, true) != null;
        var rows: std.ArrayList(EvidenceRow) = .empty;
        var included: usize = 0;
        var omitted_lines: usize = 0;
        var omitted_bytes: usize = 0;
        const shown = try @import("evidence.zig").contextMask(a, selected, self.options.before, self.options.after);
        const allowed = try a.alloc(bool, lines.len);
        @memset(allowed, false);
        // Keep scored evidence before spending the excerpt budget on context.
        for (0..2) |pass| for (lines, shown, selected, 0..) |line, show, yes, i| {
            if (!show or yes != (pass == 0)) continue;
            const size = line.end - line.start;
            if (size > self.options.source_excerpt_bytes - included) {
                omitted_lines += 1;
                omitted_bytes += size;
            } else {
                allowed[i] = true;
                included += size;
            }
        };
        var region_cursor: @import("evidence.zig").RegionCursor = .{ .regions = evidence.regions.items };
        for (lines, allowed, selected, 0..) |line, allow, yes, i| {
            if (!allow) continue;
            const text = bytes[line.start..line.end];
            const region_p = region_cursor.probability(i + 1);
            const p = evidence.line_scores[i];
            try rows.append(a, .{ .line = i + 1, .byte_offset = line.start, .text = text, .p = if (std.math.isNan(p) or p < 0) null else p, .region_p = region_p, .matched = yes });
        }
        var regions: std.ArrayList(@import("evidence.zig").Region) = .empty;
        for (evidence.regions.items) |region| if (region.p >= self.options.threshold) try regions.append(a, region);
        var calls: std.ArrayList(@import("source.zig").Call) = .empty;
        for (evidence.index.calls) |call| if (call.line > 0 and call.line <= selected.len and selected[call.line - 1]) try calls.append(a, call);
        return .{ .rows = rows.items, .regions = regions.items, .calls = calls.items, .declarations = evidence.index.declarations, .file_score = evidence.file_score, .parse_complete = evidence.index.complete, .any_selected = any_selected, .omitted_lines = omitted_lines, .omitted_bytes = omitted_bytes };
    }
    fn renderEvidence(self: *Search, a: Allocator, path: []const u8, bytes: []const u8, lines: []const Line, selected: []const bool) !void {
        try self.renderEvidenceView(a, path, try self.evidenceView(a, bytes, lines, selected));
    }
    fn renderEvidenceView(self: *Search, a: Allocator, path: []const u8, view: EvidenceView) !void {
        if (self.options.json) {
            if (self.options.json_compact) try self.jsonBegin(path);
            try self.json(.{ .type = "file", .path = path, .relevance = self.best_probability, .file_preview_score = view.file_score, .selection = .{ .query = self.options.ask, .additional_queries = self.options.queries.items, .lexical_prefilters = self.options.patterns.items, .policies = .{ .filters = self.options.filters.items, .only = self.options.only.items, .exclude = self.options.not.items }, .file_threshold = self.options.file_threshold, .region_threshold = self.options.threshold, .refined = self.options.refine, .reason = if (view.any_selected) "scored_source_evidence" else "file_preview_reading_lead" }, .match = if (view.any_selected) "strong" else "lead", .reading_leads = view.declarations, .regions = view.regions[0..@min(view.regions.len, self.options.max_regions_per_file)], .lines = view.rows, .calls = view.calls, .omitted = .{ .lines = view.omitted_lines, .bytes = view.omitted_bytes, .regions = view.regions.len - @min(view.regions.len, self.options.max_regions_per_file) }, .parse_complete = view.parse_complete, .complete = self.summary.complete });
            return;
        }
        var output: std.Io.Writer.Allocating = .init(a);
        try output.writer.print("{s}  relevance={d:.3}\n", .{ path, self.best_probability });
        if (self.provenance.backend == .antfly) try output.writer.print("  source={s} revision={s} indexed_at={d} (indexed snapshot)\n", .{ self.provenance.uri.?, self.provenance.revision.?, self.provenance.indexed_at.? });
        for (view.regions[0..@min(view.regions.len, self.options.max_regions_per_file)]) |region| try output.writer.print("  {d}-{d}  {d:.3}  {s}\n", .{ region.start, region.end, region.p, region.label });
        if (view.regions.len > self.options.max_regions_per_file) try output.writer.print("  [omitted {d} qualifying regions; increase --max-regions]\n", .{view.regions.len - self.options.max_regions_per_file});
        if (view.regions.len == 0 and view.declarations.len > 0) {
            try output.writer.writeAll("  Reading leads (declaration locations; no qualifying region score):\n");
            for (view.declarations[0..@min(10, view.declarations.len)]) |decl| try output.writer.print("    {d}-{d}  {s} {s}\n", .{ decl.start, decl.end, decl.kind, decl.name });
        }
        for (view.rows) |row| {
            if (!row.matched) {
                try output.writer.print("    {d}  context  {s}\n", .{ row.line, row.text });
                continue;
            }
            if (row.p) |p| try output.writer.print("    {d}  {d:.3}  {s}\n", .{ row.line, p, row.text }) else try output.writer.print("    {d}  region={d:.3}  {s}\n", .{ row.line, row.region_p orelse 0, row.text });
        }
        if (view.omitted_lines > 0) try output.writer.print("  [omitted {d} source lines, {d} bytes; increase --max-source-bytes]\n", .{ view.omitted_lines, view.omitted_bytes });
        for (view.calls) |call| try output.writer.print("  call {d}:{d}  {s}\n", .{ call.line, call.column, call.name });
        if (view.rows.len == 0 and view.regions.len == 0) try output.writer.writeAll("  [file qualified from its preview; no excerpt cleared the threshold]\n");
        try self.write(output.written());
    }
    fn evaluate(self: *Search, a: Allocator, path_name: []const u8, bytes: []const u8, lines: []const Line, selected: []bool, probabilities: []f64) !void {
        var regions: std.ArrayList(Region) = .empty;
        if (self.options.semantic_files_only) return;
        const built = try @import("regions.zig").buildPrepared(a, bytes, lines, self.options.chunk_lines, self.evidence.?.index);
        for (built) |region| {
            var eligible = self.options.patterns.items.len == 0;
            if (!eligible) for (lines[region.start..region.end]) |line| if (try self.matcher.matches(bytes[line.start..line.end])) {
                eligible = true;
                break;
            };
            if (eligible) try regions.append(a, region);
        }
        self.summary.regions_considered += regions.items.len;
        var offset: usize = 0;
        while (offset < regions.items.len) {
            const remaining = self.options.max_regions -| (if (self.shared_limits) |limits| limits.regions.load(.monotonic) else self.summary.regions_evaluated);
            if (remaining == 0) {
                try self.reportError(path_name, error.RegionBudgetExceeded);
                return;
            }
            const n = @min(@min(self.options.batch_size * (if (self.decider.allow_parallel) @max(1, self.options.jobs) else @as(usize, 1)), regions.items.len - offset), remaining);
            if (self.shared_limits) |limits| if (!Limits.reserve(&limits.regions, n, self.options.max_regions)) return error.RegionBudgetExceeded;
            var batch_arena = std.heap.ArenaAllocator.init(self.a);
            defer batch_arena.deinit();
            const ba = batch_arena.allocator();
            const inputs = try ba.alloc(decision.Input, n);
            for (regions.items[offset..][0..n], 0..) |region, i| {
                const id = try std.fmt.allocPrint(ba, "region-{d}", .{offset + i});
                var listing: std.Io.Writer.Allocating = .init(ba);
                try listing.writer.print("File: {s}\nTARGET lines {d}-{d}\n", .{ path_name, region.start + 1, region.end });
                const first = region.start -| self.options.context_lines;
                const last = region.end + @min(self.options.context_lines, lines.len - region.end);
                for (lines[first..last], first..) |line, j| try listing.writer.print("{s} {d}: {s}\n", .{ if (j >= region.start and j < region.end) "TARGET" else "CONTEXT", j + 1, bytes[line.start..line.end] });
                if (listing.written().len > 1024 * 1024) return error.DecisionInputTooLarge;
                if (!std.unicode.utf8ValidateSlice(listing.written())) return error.DecisionInputNotUtf8;
                inputs[i] = .{ .id = id, .input = listing.written() };
            }
            const scores = try ba.alloc(f64, n);
            var failure: ?anyerror = null;
            self.decider.stage = if (self.options.chunk_lines == 1) .line else .region;
            self.decider.search(inputs, scores) catch |err| {
                failure = err;
            };
            // Build one refinement work set across this bounded region window.
            // Each region owns one context; target text and score slices retain
            // independent line evidence even when the coarse region rejected it.
            var target_count: usize = 0;
            const contexts = try ba.alloc([]const u8, n);
            const context_hashes = try ba.alloc([32]u8, n);
            const starts = try ba.alloc(usize, n + 1);
            for (regions.items[offset..][0..n], scores, 0..) |region, score, i| {
                starts[i] = target_count;
                if (!std.math.isFinite(score)) continue;
                self.summary.regions_evaluated += 1;
                self.best_probability = @max(self.best_probability, score);
                if (score >= 0) if (self.evidence) |evidence| try evidence.regions.append(a, .{ .start = region.start + 1, .end = region.end, .p = score, .label = region.label, .label_line = region.label_line });
                if (region.end - region.start == 1) if (self.evidence) |evidence| {
                    evidence.line_scores[region.start] = score;
                };
                if (!self.options.refine or region.end - region.start <= 1) continue;
                const count = region.end - region.start;
                self.summary.regions_considered += count;
                contexts[i] = try std.fmt.allocPrint(ba, "File: {s}\nCONTEXT region lines {d}-{d}:\n{s}\n", .{ path_name, region.start + 1, region.end, bytes[lines[region.start].start..lines[region.end - 1].end] });
                std.crypto.hash.sha2.Sha256.hash(contexts[i], &context_hashes[i], .{});
                target_count += count;
            }
            starts[n] = target_count;
            const line_scores = try ba.alloc(f64, target_count);
            @memset(line_scores, std.math.nan(f64));
            var target_offset: usize = 0;
            var region_index: usize = 0;
            const target_window = self.options.batch_size * (if (self.decider.allow_parallel) @max(1, self.options.jobs) else @as(usize, 1));
            while (target_offset < target_count) {
                // Materialize only one request window, even for declarations
                // spanning millions of lines. Context stays shared and scores
                // retain stable offsets for coarse-result fallback semantics.
                var target_arena = std.heap.ArenaAllocator.init(self.a);
                defer target_arena.deinit();
                const ta = target_arena.allocator();
                const remaining_lines = self.options.max_regions -| (if (self.shared_limits) |limits| limits.regions.load(.monotonic) else self.summary.regions_evaluated);
                const m = @min(@min(target_window, target_count - target_offset), remaining_lines);
                if (m == 0) {
                    failure = error.RegionBudgetExceeded;
                    break;
                }
                if (self.shared_limits) |limits| if (!Limits.reserve(&limits.regions, m, self.options.max_regions)) {
                    failure = error.RegionBudgetExceeded;
                    break;
                };
                const targets = try ta.alloc(decision.Input, m);
                for (targets, target_offset..) |*target, flat| {
                    while (flat >= starts[region_index + 1]) region_index += 1;
                    const region = regions.items[offset + region_index];
                    const j = region.start + flat - starts[region_index];
                    target.* = .{ .id = try std.fmt.allocPrint(ta, "refine-{d}-{d}", .{ offset + region_index, j }), .input = try std.fmt.allocPrint(ta, "TARGET {d}: {s}\n", .{ j + 1, bytes[lines[j].start..lines[j].end] }), .context = contexts[region_index], .context_hash = context_hashes[region_index] };
                }
                self.decider.stage = .line;
                self.decider.search(targets, line_scores[target_offset..][0..m]) catch |err| {
                    failure = err;
                    break;
                };
                target_offset += m;
            }
            for (regions.items[offset..][0..n], scores, 0..) |region, score, i| {
                if (!std.math.isFinite(score)) continue;
                const fine_scores = line_scores[starts[i]..starts[i + 1]];
                for (fine_scores, region.start..) |p, j| {
                    if (std.math.isFinite(p)) self.summary.regions_evaluated += 1;
                    if (p >= 0) self.best_probability = @max(self.best_probability, p);
                    if (self.evidence) |evidence| evidence.line_scores[j] = if (std.math.isNan(evidence.line_scores[j])) p else @max(evidence.line_scores[j], p);
                }
                const fine = fine_scores.len > 0;
                for (region.start..region.end, 0..) |j, k| {
                    const p = if (fine) fine_scores[k] else score;
                    if (p >= 0 and ((p >= self.options.threshold) != self.options.invert)) {
                        selected[j] = true;
                        probabilities[j] = if (std.math.isNan(probabilities[j])) p else @max(probabilities[j], p);
                    }
                }
            }
            if (failure) |err| return err;
            offset += n;
        }
    }
    const ForwardSink = struct {
        search: *Search,
        failure: ?anyerror = null,
        writer: std.Io.Writer,
        started: bool = false,
        fn drain(writer: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
            const self: *ForwardSink = @alignCast(@fieldParentPtr("writer", writer));
            if (!self.started) {
                self.started = true;
                const options = self.search.options;
                if ((options.heading orelse false) and !options.json and !options.count and !options.count_matches and !options.files_with_matches and !options.files_without_match and self.search.output_bytes > 0) self.search.write("\n") catch |err| {
                    self.failure = err;
                    return error.WriteFailed;
                };
            }
            var count: usize = 0;
            for (data[0 .. data.len - 1]) |bytes| {
                self.search.write(bytes) catch |err| {
                    self.failure = err;
                    return error.WriteFailed;
                };
                count += bytes.len;
            }
            for (0..splat) |_| {
                const bytes = data[data.len - 1];
                self.search.write(bytes) catch |err| {
                    self.failure = err;
                    return error.WriteFailed;
                };
                count += bytes.len;
            }
            return count;
        }
    };
    pub fn write(self: *Search, bytes: []const u8) !void {
        if (self.retaining_output) if (self.shared_limits) |limits| {
            if (!Limits.reserve(&limits.pending_output, bytes.len, self.options.max_pending_output)) {
                self.retaining_output = false;
                return error.PendingOutputBudgetExceeded;
            }
            self.reserved_output += bytes.len;
        };
        if (bytes.len > self.options.max_output_bytes - self.output_bytes) return error.OutputBudgetExceeded;
        self.output_bytes += bytes.len;
        if (self.sink) |sink| {
            try sink.writeAll(bytes);
            if (self.options.line_buffered) try sink.flush();
        } else if (self.options.block_buffered or (!self.options.line_buffered and (self.options.paths.items.len > 0 or self.options.table_config != null) and !self.hasStdinPath())) {
            if (bytes.len >= 65536) {
                try self.flushOutput();
                try std.Io.File.stdout().writeStreamingAll(self.io, bytes);
            } else {
                try self.output_buffer.appendSlice(self.a, bytes);
                if (self.output_buffer.items.len >= 65536) try self.flushOutput();
            }
        } else try std.Io.File.stdout().writeStreamingAll(self.io, bytes);
    }
    fn hasStdinPath(self: *Search) bool {
        for (self.options.paths.items) |path| if (std.mem.eql(u8, path, "-")) return true;
        return false;
    }
    fn flushOutput(self: *Search) !void {
        if (self.output_buffer.items.len == 0) return;
        try std.Io.File.stdout().writeStreamingAll(self.io, self.output_buffer.items);
        self.output_buffer.clearRetainingCapacity();
    }
    fn prefix(self: *Search, w: *std.Io.Writer, path: []const u8, line: usize, offset: usize, column: usize, matched: bool) !void {
        const sep = if (matched) self.options.field_match_separator else self.options.field_context_separator;
        var linked = false;
        const metadata = self.options.line_numbers or self.options.byte_offset or self.options.column;
        if (self.render_filename and !(self.options.heading orelse false)) {
            linked = try @import("style.zig").hyperlink(self.a, self.io, w, self.options, path, line, column);
            if (self.color_enabled) try @import("style.zig").start(w, self.options, "path");
            if (self.options.path_separator) |separator| {
                for (path) |ch| {
                    if (ch == '/' or ch == '\\') try w.writeAll(separator) else try w.writeByte(ch);
                }
            } else try w.writeAll(path);
            if (self.color_enabled) try w.writeAll("\x1b[0m");
            if (linked and !metadata) try w.writeAll("\x1b]8;;\x1b\\");
            try w.writeAll(if (self.options.null_paths) "\x00" else sep);
        }
        if (self.options.line_numbers) {
            if (self.color_enabled) try @import("style.zig").start(w, self.options, "line");
            try w.print("{d}", .{line});
            if (self.color_enabled) try w.writeAll("\x1b[0m");
            if (linked and !self.options.byte_offset and !self.options.column) try w.writeAll("\x1b]8;;\x1b\\");
            try w.writeAll(sep);
        }
        if (self.options.byte_offset) {
            if (self.color_enabled) try @import("style.zig").start(w, self.options, "column");
            try w.print("{d}", .{offset});
            if (self.color_enabled) try w.writeAll("\x1b[0m");
            if (linked and !self.options.column) try w.writeAll("\x1b]8;;\x1b\\");
            try w.writeAll(sep);
        }
        if (self.options.column) {
            if (self.color_enabled) try @import("style.zig").start(w, self.options, "column");
            try w.print("{d}", .{column});
            if (self.color_enabled) try w.writeAll("\x1b[0m");
            if (linked) try w.writeAll("\x1b]8;;\x1b\\");
            try w.writeAll(sep);
        }
    }
    fn firstSpanOnly(self: *const Search) bool {
        return self.options.column and !self.options.json and !self.options.only_matching and self.options.replacement == null and !self.color_enabled and !self.options.vimgrep and !self.options.count_matches;
    }
    fn emit(self: *Search, path_name: []const u8, text: []const u8, line: usize, byte_offset: usize, matched: bool, probability: f64) !void {
        if (self.binary_mode and !self.options.json) return;
        // Plain rows need no capture rendering or temporary output allocation.
        if (!self.options.json and std.math.isNan(probability) and !self.render_filename and !self.options.line_numbers and !self.options.column and !self.options.byte_offset and !self.options.vimgrep and !self.options.only_matching and self.options.replacement == null and !self.color_enabled and !self.options.trim and text.len <= self.options.max_columns) {
            try self.write(text);
            try self.write(if (self.options.null_data) "\x00" else "\n");
            return;
        }
        if (self.options.only_matching and !self.options.json and self.options.replacement == null and self.options.max_columns == std.math.maxInt(usize) and !self.options.trim and !self.color_enabled and !self.render_filename and !self.options.line_numbers and !self.options.column and !self.options.byte_offset and !self.options.vimgrep and std.math.isNan(probability) and !self.options.invert and self.current_hits != null and self.current_hits.?.len > 0 and self.current_hit_text.ptr == text.ptr and self.current_hit_text.len == text.len) {
            if (!matched) return;
            const delimiter: u8 = if (self.options.null_data) 0 else '\n';
            for (self.current_hits.?) |hit| {
                try self.write(text[hit.span.start..hit.span.end]);
                try self.write(&.{delimiter});
            }
            return;
        }
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        const p: ?f64 = if (std.math.isNan(probability)) null else probability;
        const independent = self.independent_override orelse if (self.evidence) |evidence| line > 0 and line <= evidence.line_scores.len and std.math.isFinite(evidence.line_scores[line - 1]) and evidence.line_scores[line - 1] >= 0 and evidence.line_scores[line - 1] == probability else self.options.ask != null;
        const replacement_context = self.options.replacement != null and !self.options.multiline and !self.options.json and !self.replacement_absolute_start and self.matcher.hasAbsoluteStart();
        if (replacement_context) self.matcher.allowAbsoluteStart(false);
        defer if (replacement_context) self.matcher.allowAbsoluteStart(true);
        const raw_record = self.raw_line orelse text;
        const terminated = raw_record.len > 0 and raw_record[raw_record.len - 1] == (if (self.options.null_data) @as(u8, 0) else @as(u8, '\n'));
        var hits: std.ArrayList(@import("matcher.zig").Match) = .empty;
        var cursor: usize = 0;
        var previous_end: ?usize = null;
        if (!replacement_context and self.current_hits != null and self.current_hit_text.ptr == text.ptr and self.current_hit_text.len == text.len and matched and p == null and !self.options.invert) {
            // Borrow the scanner's stable spans; rendering changes only the
            // view length, never the backing entries or capture storage.
            hits.items = @constCast(self.current_hits.?);
        } else if (!self.replay_spans and matched and p == null and !self.options.invert and (self.options.json or self.options.only_matching or self.options.replacement != null or self.color_enabled or self.options.column or self.options.vimgrep)) while (self.matcher.find(text, cursor)) |hit| {
            if (hit.span.start == hit.span.end and previous_end != null and previous_end.? == hit.span.end) {
                cursor = self.matcher.advance(text, hit.span.end);
                if (cursor > text.len or (!terminated and cursor == text.len)) break;
                continue;
            }
            const captures: []const ?@import("matcher.zig").Span = if (self.options.replacement != null) try a.dupe(?@import("matcher.zig").Span, hit.captures) else &.{};
            try hits.append(a, .{ .span = hit.span, .captures = captures, .names = hit.names });
            previous_end = hit.span.end;
            if (self.firstSpanOnly()) break;
            cursor = if (hit.span.start == hit.span.end) self.matcher.advance(text, hit.span.end) else hit.span.end;
            if (cursor > text.len or (!terminated and cursor == text.len)) break;
        };
        if (!terminated) while (hits.items.len > 0) {
            const last = hits.items[hits.items.len - 1];
            if (last.span.start != text.len or last.span.end != text.len) break;
            hits.items.len -= 1;
        };
        if (self.options.json) {
            const encoded_path = if (self.options.json_compact) JsonText{ .text = null, .bytes = null } else try jsonText(a, path_name);
            try self.jsonBegin(path_name);
            const raw = self.raw_line orelse text;
            const utf8 = self.options.json_compact or std.unicode.utf8ValidateSlice(text);
            const encoded = if (utf8) null else try base64(a, text);
            const subs = JsonSubmatches{ .matcher = &self.matcher, .text = text, .terminated = terminated, .runs = if (self.replay_spans and matched) self.cachedRuns() else null, .hits = if (self.replay_spans and self.current_hits == null and matched and p == null and !self.options.invert) null else hits.items };
            const raw_valid = std.unicode.utf8ValidateSlice(raw);
            if (self.options.json_compact) {
                try self.json(.{ .type = if (matched) "match" else "context", .data = .{ .lines = JsonText{ .text = if (raw_valid) raw else null, .bytes = if (raw_valid) null else try base64(a, raw) }, .line_number = line, .absolute_offset = byte_offset, .submatches = subs }, .probability = p, .scope = if (p != null and !independent) "region" else "line" });
            } else {
                try self.json(.{ .type = if (matched) "match" else "context", .data = .{ .path = encoded_path, .lines = JsonText{ .text = if (raw_valid) raw else null, .bytes = if (raw_valid) null else try base64(a, raw) }, .line_number = line, .absolute_offset = byte_offset, .submatches = subs }, .path = if (encoded_path.text != null) path_name else null, .path_bytes_base64 = encoded_path.bytes, .line_number = line, .byte_offset = byte_offset, .text = if (utf8) text else null, .bytes_base64 = encoded, .probability = p, .scope = if (p != null and !independent) "region" else "line" });
            }
            return;
        }
        if ((self.options.heading orelse false) and self.render_filename) {
            if (!self.header_printed) {
                if (self.sink == null and self.output_bytes > 0) try self.write("\n");
                var header: std.Io.Writer.Allocating = .init(a);
                if (self.color_enabled) try @import("style.zig").start(&header.writer, self.options, "path");
                try header.writer.writeAll(path_name);
                if (self.color_enabled) try header.writer.writeAll("\x1b[0m");
                try header.writer.writeByte('\n');
                try self.write(header.written());
                self.header_printed = true;
            }
        }
        if ((self.replay_spans or (self.options.only_matching and !self.options.vimgrep and self.options.replacement == null and self.options.max_columns != std.math.maxInt(usize))) and matched and p == null and !self.options.invert) {
            return self.emitReplay(path_name, text, line, byte_offset, terminated);
        }
        if (self.options.only_matching and p == null) {
            if (!matched) return;
            if (hits.items.len == 0) {
                var output: std.Io.Writer.Allocating = .init(a);
                try self.prefix(&output.writer, path_name, line, byte_offset, 1, true);
                try output.writer.writeAll(text);
                try output.writer.writeByte(if (self.options.null_data) 0 else '\n');
                try self.write(output.written());
                return;
            }
            for (hits.items) |hit| {
                var output: std.Io.Writer.Allocating = .init(a);
                try self.prefix(&output.writer, path_name, line, byte_offset + hit.span.start, hit.span.start + 1, true);
                if (self.color_enabled) try @import("style.zig").start(&output.writer, self.options, "match");
                if (self.options.replacement) |template| {
                    const rendered = try Matcher.replacement(a, template, text, hit);
                    if (rendered.len == 0) continue;
                    try output.writer.writeAll(rendered);
                } else try output.writer.writeAll(text[hit.span.start..hit.span.end]);
                if (self.color_enabled) try output.writer.writeAll("\x1b[0m");
                try output.writer.writeByte(if (self.options.null_data) 0 else '\n');
                try self.write(output.written());
            }
            return;
        }
        const repetitions = if (self.options.vimgrep and hits.items.len > 0) hits.items.len else @as(usize, 1);
        for (0..repetitions) |rendering| {
            var output: std.Io.Writer.Allocating = .init(a);
            try self.prefix(&output.writer, path_name, line, byte_offset, if (hits.items.len > 0) hits.items[if (self.options.vimgrep) rendering else 0].span.start + 1 else 1, matched);
            if (p) |score| try output.writer.print("{d:.3}:", .{score});
            if (text.len > self.options.max_columns) {
                if (self.options.max_columns_preview) {
                    try output.writer.writeAll(text[0..self.options.max_columns]);
                    try output.writer.writeAll(" [... omitted end of long line]");
                } else if (self.color_enabled and matched and p == null) try output.writer.print("[Omitted long line with {d} matches]", .{hits.items.len}) else try output.writer.print("[Omitted long {s} line]", .{if (matched) "matching" else "context"});
                try output.writer.writeByte(if (self.options.null_data) 0 else '\n');
                try self.write(output.written());
                return;
            }
            const trim_start = if (self.options.trim and self.options.replacement == null) text.len - std.mem.trimStart(u8, text, " \t").len else 0;
            var start: usize = trim_start;
            for (hits.items) |hit| {
                if (self.options.replacement == null and hit.span.end <= trim_start) continue;
                const visible_start = @max(hit.span.start, trim_start);
                try output.writer.writeAll(if (self.options.trim and start == 0) std.mem.trimStart(u8, text[start..visible_start], " \t") else text[start..visible_start]);
                if (self.color_enabled) try @import("style.zig").start(&output.writer, self.options, "match");
                if (self.options.replacement) |template| try output.writer.writeAll(try Matcher.replacement(a, template, text, hit)) else try output.writer.writeAll(text[visible_start..hit.span.end]);
                if (self.color_enabled) try output.writer.writeAll("\x1b[0m");
                start = hit.span.end;
            }
            const tail = text[start..];
            try output.writer.writeAll(if (self.options.trim and hits.items.len == 0) std.mem.trimStart(u8, tail, " \t") else tail);
            try output.writer.writeByte(if (self.options.null_data) 0 else '\n');
            try self.write(output.written());
        }
    }
    /// The preflight iterator and renderer share zero-width/EOF semantics.
    /// Captures are never retained: capture-dependent rendering keeps its
    /// existing source-context path instead of entering replay mode.
    /// A repeated stride/length run represents arbitrarily dense occurrences
    /// without retaining one object per match. Irregular rows fall back to
    /// replay after at most 64 runs, keeping per-worker span storage bounded.
    const SpanRun = struct { start: usize, length: usize, stride: usize = 0, count: usize = 1 };
    fn appendSpanRun(self: *Search, span: @import("matcher.zig").Span) void {
        if (!self.span_runs_valid) return;
        if (self.span_run_count > 0) {
            const last = &self.span_runs[self.span_run_count - 1];
            const previous = last.start + last.stride * (last.count - 1);
            const stride = span.start - previous;
            if (span.end - span.start == last.length and (last.count == 1 or stride == last.stride)) {
                last.stride = stride;
                last.count += 1;
                return;
            }
        }
        if (self.span_run_count == self.span_runs.len) {
            self.span_runs_valid = false;
            return;
        }
        self.span_runs[self.span_run_count] = .{ .start = span.start, .length = span.end - span.start };
        self.span_run_count += 1;
    }
    fn cachedRuns(self: *Search) ?[]const SpanRun {
        return if (self.span_runs_valid) self.span_runs[0..self.span_run_count] else null;
    }
    const SpanIterator = struct {
        matcher: *Matcher,
        text: []const u8,
        terminated: bool,
        cursor: usize = 0,
        previous_end: ?usize = null,
        done: bool = false,
        found: bool = false,
        cached: ?[]const @import("matcher.zig").Match = null,
        cached_index: usize = 0,
        runs: ?[]const SpanRun = null,
        run_index: usize = 0,
        run_offset: usize = 0,
        fn next(self: *SpanIterator) ?@import("matcher.zig").Span {
            if (self.cached) |hits| {
                if (self.cached_index == hits.len) return null;
                const span = hits[self.cached_index].span;
                self.cached_index += 1;
                return span;
            }
            if (self.runs) |runs| {
                if (self.run_index == runs.len) return null;
                const span_run = runs[self.run_index];
                const start = span_run.start + self.run_offset * span_run.stride;
                self.run_offset += 1;
                if (self.run_offset == span_run.count) {
                    self.run_index += 1;
                    self.run_offset = 0;
                }
                return .{ .start = start, .end = start + span_run.length };
            }
            while (!self.done) {
                const hit = self.matcher.find(self.text, self.cursor) orelse {
                    self.done = true;
                    return null;
                };
                self.found = true;
                const empty = hit.span.start == hit.span.end;
                const duplicate = empty and self.previous_end != null and self.previous_end.? == hit.span.end;
                const terminal = !self.terminated and empty and hit.span.start == self.text.len;
                self.cursor = if (empty) self.matcher.advance(self.text, hit.span.end) else hit.span.end;
                self.done = self.cursor > self.text.len or (!self.terminated and self.cursor == self.text.len);
                if (duplicate or terminal) continue;
                self.previous_end = hit.span.end;
                return hit.span;
            }
            return null;
        }
    };
    const JsonSubmatches = struct {
        matcher: *Matcher,
        text: []const u8,
        terminated: bool,
        hits: ?[]const @import("matcher.zig").Match,
        runs: ?[]const SpanRun = null,
        pub fn jsonStringify(self: @This(), json_writer: *std.json.Stringify) !void {
            try json_writer.beginArray();
            if (self.hits) |hits| {
                for (hits) |hit| try self.item(json_writer, hit.span);
            } else {
                var spans = SpanIterator{ .matcher = self.matcher, .text = self.text, .terminated = self.terminated, .runs = self.runs };
                while (spans.next()) |span| try self.item(json_writer, span);
                if (self.matcher.failure != null) return error.WriteFailed;
            }
            try json_writer.endArray();
        }
        fn item(self: @This(), json_writer: *std.json.Stringify, span: @import("matcher.zig").Span) !void {
            const bytes = self.text[span.start..span.end];
            if (std.unicode.utf8ValidateSlice(bytes)) {
                try json_writer.write(.{ .match = .{ .text = bytes }, .start = span.start, .end = span.end });
            } else {
                // Invalid byte matches are rare; bounded chunks of base64 avoid
                // a second array proportional to the number of occurrences.
                try json_writer.beginObject();
                try json_writer.objectField("match");
                try json_writer.beginObject();
                try json_writer.objectField("bytes");
                try json_writer.beginWriteRaw();
                try json_writer.writer.writeByte('"');
                var at: usize = 0;
                var encoded: [4096]u8 = undefined;
                while (at < bytes.len) {
                    const count = @min(bytes.len - at, 3072);
                    try json_writer.writer.writeAll(std.base64.standard.Encoder.encode(&encoded, bytes[at..][0..count]));
                    at += count;
                }
                try json_writer.writer.writeByte('"');
                json_writer.endWriteRaw();
                try json_writer.endObject();
                try json_writer.objectField("start");
                try json_writer.write(span.start);
                try json_writer.objectField("end");
                try json_writer.write(span.end);
                try json_writer.endObject();
            }
        }
    };
    const OutputWriter = struct {
        search: *Search,
        writer: std.Io.Writer,
        failure: ?anyerror = null,
        fn drain(writer: *std.Io.Writer, slices: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
            const self: *@This() = @alignCast(@fieldParentPtr("writer", writer));
            self.search.write(writer.buffer[0..writer.end]) catch |err| {
                self.failure = err;
                return error.WriteFailed;
            };
            writer.end = 0;
            var count: usize = 0;
            for (slices[0 .. slices.len - 1]) |bytes| {
                self.search.write(bytes) catch |err| {
                    self.failure = err;
                    return error.WriteFailed;
                };
                count += bytes.len;
            }
            for (0..splat) |_| {
                const bytes = slices[slices.len - 1];
                self.search.write(bytes) catch |err| {
                    self.failure = err;
                    return error.WriteFailed;
                };
                count += bytes.len;
            }
            return count;
        }
    };
    fn outputError(self: *Search, output: *OutputWriter, fallback: anyerror) anyerror {
        if (output.failure) |err| return err;
        if (self.matcher.failure) |err| return err;
        return fallback;
    }
    fn emitReplay(self: *Search, path: []const u8, text: []const u8, line: usize, offset: usize, terminated: bool) !void {
        var buffer: [65536]u8 = undefined;
        var output = OutputWriter{ .search = self, .writer = .{ .vtable = &.{ .drain = OutputWriter.drain }, .buffer = &buffer } };
        self.renderReplay(&output.writer, path, text, line, offset, terminated) catch |err| return self.outputError(&output, err);
        output.writer.flush() catch |err| return output.failure orelse err;
    }
    fn renderPreview(self: *Search, writer: *std.Io.Writer, text: []const u8, from: usize, end: usize, original_end: usize, terminated: bool, only: bool) !void {
        var spans = SpanIterator{ .matcher = &self.matcher, .text = text, .terminated = terminated, .cached = self.current_hits, .runs = self.cachedRuns() };
        var position = from;
        var remaining: usize = 0;
        var highlight_end: ?usize = null;
        while (spans.next()) |span| {
            // The uncolored match-only printer uses match-relative excerpts.
            const cutoff = if (only and !self.color_enabled) end - from else end;
            const last = if (only and !self.color_enabled) original_end - from else original_end;
            if (span.start >= cutoff and span.start < last) remaining += 1;
            if (!self.color_enabled or span.end <= from or span.start >= end or span.start == span.end) continue;
            const lo = @max(from, span.start);
            const hi = @min(end, span.end);
            if (highlight_end != null and highlight_end.? != lo) {
                try writer.writeAll("\x1b[0m");
                highlight_end = null;
            }
            if (position < lo) try writer.writeAll(text[position..lo]);
            if (highlight_end == null) try @import("style.zig").start(writer, self.options, "match");
            try writer.writeAll(text[lo..hi]);
            position = hi;
            highlight_end = hi;
        }
        if (highlight_end != null) try writer.writeAll("\x1b[0m");
        try writer.writeAll(text[position..end]);
        if (only or self.color_enabled or self.options.stats) {
            try writer.print(" [... {d} more {s}]", .{ remaining, if (remaining == 1) "match" else "matches" });
        } else try writer.writeAll(" [... omitted end of long line]");
    }
    fn renderReplay(self: *Search, writer: *std.Io.Writer, path: []const u8, text: []const u8, line: usize, offset: usize, terminated: bool) !void {
        const prefix_needed = (self.render_filename and !(self.options.heading orelse false)) or self.options.line_numbers or self.options.byte_offset or self.options.column;
        if (self.options.only_matching and !prefix_needed and !self.color_enabled and !self.options.trim and self.options.max_columns >= 1 and self.current_hits == null) if (self.matcher.singleByteLiteral()) |byte| {
            const counted = try self.matcher.count(text, std.math.maxInt(usize), terminated);
            if (self.options.trace) std.debug.print("antfly-grep: bulk literal bytes={d} occurrences={d}\n", .{ text.len, counted.total });
            const record = [_]u8{ byte, if (self.options.null_data) 0 else '\n' };
            var block: [8192]u8 = undefined;
            for (0..block.len / 2) |i| block[i * 2 ..][0..2].* = record;
            var remaining = counted.total;
            while (remaining >= block.len / 2) {
                try self.write(&block);
                remaining -= block.len / 2;
            }
            if (remaining != 0) try self.write(block[0 .. remaining * 2]);
            return;
        };
        var spans = SpanIterator{ .matcher = &self.matcher, .text = text, .terminated = terminated, .cached = self.current_hits, .runs = self.cachedRuns() };
        var hit = spans.next();
        const delimiter: u8 = if (self.options.null_data) 0 else '\n';
        if (self.options.only_matching) {
            if (hit == null) {
                try self.prefix(writer, path, line, offset, 1, true);
                try writer.writeAll(text);
                try writer.writeByte(delimiter);
            }
            while (hit) |span| : (hit = spans.next()) {
                if (prefix_needed) try self.prefix(writer, path, line, offset + span.start, span.start + 1, true);
                const visible = if (self.options.trim) std.mem.trimStart(u8, text[span.start..span.end], " \t") else text[span.start..span.end];
                if (visible.len > self.options.max_columns) {
                    if (self.options.max_columns_preview) {
                        const from = span.end - visible.len;
                        try self.renderPreview(writer, text, from, from + self.options.max_columns, span.end, terminated, true);
                    } else try writer.writeAll("[Omitted long matching line]");
                } else {
                    if (self.color_enabled and visible.len != 0) try @import("style.zig").start(writer, self.options, "match");
                    try writer.writeAll(visible);
                    if (self.color_enabled and visible.len != 0) try writer.writeAll("\x1b[0m");
                }
                try writer.writeByte(delimiter);
            }
        } else {
            try self.prefix(writer, path, line, offset, if (hit) |span| span.start + 1 else 1, true);
            if (text.len > self.options.max_columns) {
                if (self.options.max_columns_preview) {
                    try self.renderPreview(writer, text, 0, self.options.max_columns, text.len, terminated, false);
                } else if (self.color_enabled) try writer.print("[Omitted long line with {d} matches]", .{self.row_occurrences}) else try writer.writeAll("[Omitted long matching line]");
                try writer.writeByte(delimiter);
                return;
            }
            const trim_start = if (self.options.trim) text.len - std.mem.trimStart(u8, text, " \t").len else 0;
            var start: usize = trim_start;
            while (hit) |span| {
                hit = spans.next();
                if (span.start == span.end or span.end <= trim_start) continue;
                var end = span.end;
                // Adjacent matches share one highlight range; zero-width spans
                // affect selection/metadata, never terminal escape sequences.
                while (hit) |next| {
                    if (next.start != end) break;
                    end = @max(end, next.end);
                    hit = spans.next();
                }
                const visible_start = @max(span.start, trim_start);
                try writer.writeAll(text[start..visible_start]);
                if (self.color_enabled) try @import("style.zig").start(writer, self.options, "match");
                try writer.writeAll(text[visible_start..end]);
                if (self.color_enabled) try writer.writeAll("\x1b[0m");
                start = end;
            }
            try writer.writeAll(text[start..]);
            try writer.writeByte(delimiter);
        }
        if (self.matcher.failure) |err| return err;
    }
    const JsonText = struct {
        text: ?[]const u8,
        bytes: ?[]const u8,
        pub fn jsonStringify(self: @This(), writer: anytype) !void {
            if (self.text) |value| try writer.write(.{ .text = value }) else try writer.write(.{ .bytes = self.bytes.? });
        }
    };
    fn jsonText(a: Allocator, text: []const u8) !JsonText {
        const valid = std.unicode.utf8ValidateSlice(text);
        return .{ .text = if (valid) text else null, .bytes = if (valid) null else try base64(a, text) };
    }
    const JsonStats = struct { elapsed: struct { secs: u64 = 0, nanos: u32 = 0, human: []const u8 = "0.000000s" } = .{}, searches: usize, searches_with_match: usize, bytes_searched: usize, bytes_printed: usize, matched_lines: usize, matches: usize };
    fn jsonStats(self: *Search, before: Summary) JsonStats {
        return .{ .searches = self.summary.files - before.files, .searches_with_match = self.summary.files_with_matches - before.files_with_matches, .bytes_searched = self.summary.bytes - before.bytes, .bytes_printed = self.output_bytes, .matched_lines = self.summary.matches - before.matches, .matches = self.summary.occurrences - before.occurrences };
    }
    fn jsonBegin(self: *Search, path: []const u8) !void {
        if (self.json_started) return;
        const encoded_path = try jsonText(self.a, path);
        defer if (encoded_path.bytes) |bytes| self.a.free(bytes);
        if (self.options.json_compact) {
            // The ID is opaque and observation-local. Generate it without
            // serializing provenance outside the budgeted output writer.
            var nonce: [32]u8 = undefined;
            self.io.random(&nonce);
            self.compact_file_id = std.fmt.bytesToHex(nonce, .lower);
            try self.json(.{ .type = "begin", .data = .{ .path = encoded_path, .record_delimiter = if (self.options.null_data) "nul" else "lf", .crlf = self.options.crlf } });
        } else try self.json(.{ .type = "begin", .data = .{ .path = encoded_path } });
        self.json_started = true;
    }
    fn jsonEnd(self: *Search, path: []const u8, before: Summary) !void {
        if (!self.options.json or !self.json_started) return;
        var stats = self.jsonStats(before);
        stats.bytes_printed = self.output_bytes - self.file_output_start;
        const elapsed = self.file_elapsed_ns orelse if (self.file_started) |start| @max(0, start.durationTo(std.Io.Clock.awake.now(self.io)).nanoseconds) else 0;
        const human = try std.fmt.allocPrint(self.a, "{d:.6}s", .{@as(f64, @floatFromInt(elapsed)) / 1_000_000_000});
        defer self.a.free(human);
        stats.elapsed = .{ .secs = @intCast(@divFloor(elapsed, 1_000_000_000)), .nanos = @intCast(@mod(elapsed, 1_000_000_000)), .human = human };
        const encoded_path = try jsonText(self.a, path);
        defer if (encoded_path.bytes) |bytes| self.a.free(bytes);
        if (self.options.json_compact) {
            try self.json(.{ .type = "end", .data = .{ .binary_offset = self.binary_offset, .stats = stats } });
        } else try self.json(.{ .type = "end", .data = .{ .path = encoded_path, .binary_offset = self.binary_offset, .stats = stats } });
        self.json_started = false;
    }
    const AtomicRecord = struct {
        a: Allocator,
        bytes: std.ArrayList(u8) = .empty,
        limit: usize,
        budget_error: anyerror,
        writer: std.Io.Writer,
        failure: ?anyerror = null,
        fn append(self: *@This(), bytes: []const u8) std.Io.Writer.Error!void {
            if (bytes.len > self.limit - self.bytes.items.len) {
                self.failure = self.budget_error;
                return error.WriteFailed;
            }
            const needed = self.bytes.items.len + bytes.len;
            if (needed > self.bytes.capacity) {
                const capacity = @min(self.limit, @max(needed, self.bytes.capacity + self.bytes.capacity / 2 + 64));
                self.bytes.ensureTotalCapacityPrecise(self.a, capacity) catch |err| {
                    self.failure = err;
                    return error.WriteFailed;
                };
            }
            self.bytes.appendSliceAssumeCapacity(bytes);
        }
        fn drain(writer: *std.Io.Writer, slices: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
            const self: *@This() = @alignCast(@fieldParentPtr("writer", writer));
            try self.append(writer.buffer[0..writer.end]);
            writer.end = 0;
            var count: usize = 0;
            for (slices[0 .. slices.len - 1]) |bytes| {
                try self.append(bytes);
                count += bytes.len;
            }
            for (0..splat) |_| {
                const bytes = slices[slices.len - 1];
                try self.append(bytes);
                count += bytes.len;
            }
            return count;
        }
    };
    fn recordError(self: *Search, record: *AtomicRecord, fallback: anyerror) anyerror {
        const err = record.failure orelse fallback;
        if (err == error.PendingOutputBudgetExceeded) self.retaining_output = false;
        return err;
    }
    fn encodeJson(self: *Search, value: anytype, writer: *std.Io.Writer) !void {
        const Envelope = struct {
            value: @TypeOf(value),
            source: @TypeOf(self.provenance),
            compact: bool,
            file_id: ?[64]u8,
            pub fn jsonStringify(envelope: @This(), json_writer: *std.json.Stringify) !void {
                try json_writer.beginObject();
                inline for (@typeInfo(@FieldType(@This(), "value")).@"struct".field_names) |field_name| {
                    try json_writer.objectField(field_name);
                    try json_writer.write(@field(envelope.value, field_name));
                }
                if (envelope.compact) {
                    try json_writer.objectField("result_schema_version");
                    try json_writer.write(@as(u32, 2));
                    if (envelope.file_id) |id| {
                        try json_writer.objectField("file_id");
                        try json_writer.write(@as([]const u8, &id));
                    }
                }
                if (@hasField(@TypeOf(value), "type") and !std.mem.eql(u8, envelope.value.type, "summary")) {
                    if (!envelope.compact) {
                        try json_writer.objectField("result_schema_version");
                        try json_writer.write(@as(u32, 1));
                    }
                    if (!envelope.compact or envelope.file_id == null or std.mem.eql(u8, envelope.value.type, "begin")) {
                        try json_writer.objectField("source");
                        try json_writer.write(envelope.source);
                    }
                }
                try json_writer.endObject();
            }
        };
        try std.json.Stringify.value(Envelope{ .value = value, .source = self.provenance, .compact = self.options.json_compact, .file_id = if (self.options.json_compact and (@hasField(@TypeOf(value), "type") and (std.mem.eql(u8, value.type, "begin") or (self.json_started and !std.mem.eql(u8, value.type, "summary") and !std.mem.eql(u8, value.type, "inventory") and !std.mem.eql(u8, value.type, "omitted_files"))))) self.compact_file_id else null }, .{}, writer);
        try writer.writeByte('\n');
    }
    fn json(self: *Search, value: anytype) !void {
        if (self.options.max_output_bytes == std.math.maxInt(usize) and !self.retaining_output) {
            var buffer: [65536]u8 = undefined;
            var output = OutputWriter{ .search = self, .writer = .{ .vtable = &.{ .drain = OutputWriter.drain }, .buffer = &buffer } };
            self.encodeJson(value, &output.writer) catch |err| return self.outputError(&output, err);
            output.writer.flush() catch |err| return output.failure orelse err;
        } else {
            // Record-atomic output is bounded by the remaining allowance, not
            // by the full rendered record. Oversized records stop encoding
            // before allocating their expanded JSON or publishing any bytes.
            var limit = self.options.max_output_bytes - self.output_bytes;
            var budget_error: anyerror = error.OutputBudgetExceeded;
            if (self.retaining_output) if (self.shared_limits) |limits| {
                const pending = self.options.max_pending_output -| limits.pending_output.load(.monotonic);
                if (pending < limit) {
                    limit = pending;
                    budget_error = error.PendingOutputBudgetExceeded;
                }
            };
            var buffer: [4096]u8 = undefined;
            var record = AtomicRecord{ .a = self.a, .limit = limit, .budget_error = budget_error, .writer = .{ .vtable = &.{ .drain = AtomicRecord.drain }, .buffer = &buffer } };
            defer record.bytes.deinit(self.a);
            self.encodeJson(value, &record.writer) catch |err| return self.recordError(&record, err);
            record.writer.flush() catch |err| return self.recordError(&record, err);
            try self.write(record.bytes.items);
        }
    }
};
fn base64(a: Allocator, bytes: []const u8) ![]const u8 {
    const out = try a.alloc(u8, std.base64.standard.Encoder.calcSize(bytes.len));
    return std.base64.standard.Encoder.encode(out, bytes);
}
fn lineIndex(lines: []const Line, offset: usize) usize {
    if (lines.len == 0) return 0;
    var lo: usize = 0;
    var hi = lines.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (lines[mid].start <= offset) lo = mid + 1 else hi = mid;
    }
    return lo -| 1;
}
pub fn lineRanges(a: Allocator, bytes: []const u8) ![]Line {
    return lineRangesWith(a, bytes, '\n', true);
}
pub fn lineRangesWith(a: Allocator, bytes: []const u8, delimiter: u8, crlf: bool) ![]Line {
    return @import("source_cache.zig").lines(a, bytes, delimiter, crlf);
}
test "line offsets preserve CRLF and unterminated final lines" {
    const a = std.testing.allocator;
    const lines = try lineRanges(a, "a\r\n\nlast");
    defer a.free(lines);
    try std.testing.expectEqual(@as(usize, 3), lines.len);
    try std.testing.expectEqual(@as(usize, 1), lines[0].end);
    try std.testing.expectEqual(@as(usize, 4), lines[2].start);
    const empty = try lineRanges(a, "");
    defer a.free(empty);
    try std.testing.expectEqual(@as(usize, 0), empty.len);
}
test {
    _ = @import("evidence.zig");
    _ = @import("source_views.zig");
    _ = @import("workspace_store.zig");
    _ = @import("spill.zig");
    _ = @import("ranked.zig");
    _ = @import("batch_store.zig");
    _ = @import("concurrency.zig");
    _ = @import("options.zig");
    _ = @import("matcher.zig");
    _ = @import("discovery.zig");
    _ = @import("decision.zig");
    _ = @import("regions.zig");
}

test "bounded span runs preserve sparse dense and zero width spans" {
    var search: Search = undefined;
    search.span_runs_valid = true;
    search.span_run_count = 0;
    for (0..262144) |i| search.appendSpanRun(.{ .start = i * 2, .end = i * 2 + 1 });
    try std.testing.expect(search.span_runs_valid);
    try std.testing.expectEqual(@as(usize, 1), search.span_run_count);
    var spans = Search.SpanIterator{ .matcher = undefined, .text = "", .terminated = true, .runs = search.cachedRuns() };
    var i: usize = 0;
    while (spans.next()) |span| : (i += 1) {
        try std.testing.expectEqual(i * 2, span.start);
        try std.testing.expectEqual(i * 2 + 1, span.end);
    }
    try std.testing.expectEqual(@as(usize, 262144), i);
    search.span_run_count = 0;
    for (0..16) |j| search.appendSpanRun(.{ .start = j, .end = j });
    try std.testing.expectEqual(@as(usize, 1), search.span_run_count);
    search.span_run_count = 0;
    for (0..128) |j| search.appendSpanRun(.{ .start = j * j, .end = j * j + (j % 3) });
    try std.testing.expect(!search.span_runs_valid);
    try std.testing.expectEqual(@as(usize, 64), search.span_run_count);
}

test "inventory budget failure releases copied task paths" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    const options: Options = .{ .ask = "test", .model = "fixture", .max_spill_bytes = 1, .temporary_dir = directory };
    var search = try Search.init(a, io, &options);
    defer search.deinit();
    search.discovery_stream = true;
    try std.testing.expectError(error.SpillBudgetExceeded, search.file("example.py", "example.py"));
    try std.testing.expectEqual(@as(usize, 0), search.inventory_reserved);
    try std.testing.expectEqual(@as(u64, 0), search.inventory_bytes);
}
