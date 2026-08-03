// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const antfly_client = @import("antfly-client");
const cli = @import("mod.zig");
const hbs = @import("handlebars");

const default_batch_size: usize = 1000;
const default_max_batches: usize = 100;
const default_read_buffer_bytes: usize = 1024 * 1024;
const default_max_line_bytes: usize = 16 * 1024 * 1024;
const default_batch_bytes: usize = 8 * 1024 * 1024;
const checkpoint_version: u32 = 1;
const checkpoint_max_bytes: usize = 1024 * 1024;

pub fn insert(allocator: std.mem.Allocator, _: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    var catalog = cli.CatalogFlags.defaultsFromEnv();
    var key: ?[]const u8 = null;
    var value_json: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (cli.parseCatalogFlag(&catalog, arg, args)) {
            continue;
        } else if (std.mem.eql(u8, arg, "--key")) {
            key = args.next();
        } else if (std.mem.eql(u8, arg, "--document") or std.mem.eql(u8, arg, "--value")) {
            if (value_json != null) cli.fatal("use only one of --document or --value", .{});
            value_json = args.next();
        }
    }

    const tbl = table_name orelse cli.fatal("--table is required", .{});
    const k = key orelse cli.fatal("--key is required", .{});
    const v = value_json orelse cli.fatal("--document is required", .{});

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, v, .{});
    defer parsed.deinit();

    var inserts: std.json.ArrayHashMap(std.json.Value) = .{};
    defer inserts.deinit(allocator);
    try inserts.map.put(allocator, k, parsed.value);

    var resp = try sendBatch(client, catalog, tbl, .{
        .inserts = inserts,
        .sync_level = .full_index,
    });
    defer resp.deinit();
    std.debug.print("Insert successful.\n", .{});
}

pub fn delete(_: std.mem.Allocator, _: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var table_name: ?[]const u8 = null;
    var catalog = cli.CatalogFlags.defaultsFromEnv();
    var key: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (cli.parseCatalogFlag(&catalog, arg, args)) {
            continue;
        } else if (std.mem.eql(u8, arg, "--key")) {
            key = args.next();
        }
    }

    const tbl = table_name orelse cli.fatal("--table is required", .{});
    const k = key orelse cli.fatal("--key is required", .{});

    const deletes = [_][]const u8{k};
    var resp = try sendBatch(client, catalog, tbl, .{
        .deletes = &deletes,
        .sync_level = .full_index,
    });
    defer resp.deinit();
    if (resp.data) |data| {
        if (data.value.deleted) |deleted| {
            std.debug.print("Delete successful. Deleted: {d}\n", .{deleted});
        } else {
            std.debug.print("Delete successful.\n", .{});
        }
    }
}

const LoadOptions = struct {
    table_name: []const u8,
    catalog: cli.CatalogFlags = .{},
    file_path: []const u8,
    batch_size: usize = default_batch_size,
    max_batches: usize = default_max_batches,
    id_field: ?[]const u8 = null,
    id_template: ?[]const u8 = null,
    sync_level: ?antfly_client.types.SyncLevel = null,
    read_buffer_bytes: usize = default_read_buffer_bytes,
    max_line_bytes: usize = default_max_line_bytes,
    batch_bytes: usize = default_batch_bytes,
    checkpoint_path: ?[]const u8 = null,
    resume_load: bool = false,
    checkpoint_enabled: bool = true,
    dry_run: bool = false,
    max_errors: ?u64 = null,
};

const LoadStats = struct {
    lines_seen: u64 = 0,
    loaded: u64 = 0,
    committed: u64 = 0,
    sent: u64 = 0,
    batch_count: u64 = 0,
    invalid_json: u64 = 0,
    bad_id: u64 = 0,
    empty_line: u64 = 0,
    too_long: u64 = 0,
    duplicate_in_batch: u64 = 0,

    fn rejected(self: LoadStats) u64 {
        return self.invalid_json + self.bad_id + self.too_long;
    }
};

const FileMetadata = struct {
    size: u64,
    mtime_ns: i128,
};

const LoadCheckpoint = struct {
    version: u32 = checkpoint_version,
    source_path: []const u8,
    source_size: u64,
    source_mtime_ns: i128,
    table_name: []const u8,
    database_name: ?[]const u8 = null,
    namespace_name: ?[]const u8 = null,
    id_field: ?[]const u8 = null,
    id_template: ?[]const u8 = null,
    sync_level: ?[]const u8 = null,
    offset: u64 = 0,
    line_number: u64 = 0,
    loaded: u64 = 0,
    committed: u64 = 0,
    invalid_json: u64 = 0,
    bad_id: u64 = 0,
    too_long: u64 = 0,
    empty_line: u64 = 0,
    duplicate_in_batch: u64 = 0,
    batch_count: u64 = 0,
};

const Line = struct {
    bytes: []const u8,
    line_number: u64,
    start_offset: u64,
    end_offset: u64,
};

const TooLongLine = struct {
    line_number: u64,
    start_offset: u64,
    end_offset: u64,
    byte_len: usize,
};

const LineScanner = struct {
    alloc: std.mem.Allocator,
    max_line_bytes: usize,
    pending: std.ArrayListUnmanaged(u8) = .empty,
    pending_base_offset: u64 = 0,
    consumed: usize = 0,
    scan_index: usize = 0,
    next_line_number: u64 = 1,
    discarding_too_long: bool = false,
    discard_start_offset: u64 = 0,
    discarded_len: usize = 0,

    fn init(alloc: std.mem.Allocator, max_line_bytes: usize, start_offset: u64, start_line_number: u64) LineScanner {
        return .{
            .alloc = alloc,
            .max_line_bytes = max_line_bytes,
            .pending_base_offset = start_offset,
            .next_line_number = start_line_number,
        };
    }

    fn deinit(self: *LineScanner) void {
        self.pending.deinit(self.alloc);
    }

    fn feed(self: *LineScanner, bytes: []const u8, chunk_offset: u64, sink: anytype) !void {
        var rest = bytes;
        var rest_offset = chunk_offset;
        while (rest.len > 0) {
            if (self.discarding_too_long) {
                if (std.mem.indexOfScalar(u8, rest, '\n')) |newline_index| {
                    self.discarded_len += newline_index;
                    try sink.handleTooLong(.{
                        .line_number = self.next_line_number,
                        .start_offset = self.discard_start_offset,
                        .end_offset = rest_offset + newline_index + 1,
                        .byte_len = self.discarded_len,
                    });
                    self.next_line_number += 1;
                    self.discarding_too_long = false;
                    self.discarded_len = 0;
                    rest = rest[newline_index + 1 ..];
                    rest_offset += newline_index + 1;
                } else {
                    self.discarded_len += rest.len;
                    return;
                }
                continue;
            }

            try self.pending.appendSlice(self.alloc, rest);
            try self.emitCompleteLines(sink);
            try self.enterDiscardIfCurrentLineTooLong();
            return;
        }
    }

    fn finish(self: *LineScanner, sink: anytype) !void {
        if (self.discarding_too_long) {
            try sink.handleTooLong(.{
                .line_number = self.next_line_number,
                .start_offset = self.discard_start_offset,
                .end_offset = self.discard_start_offset + self.discarded_len,
                .byte_len = self.discarded_len,
            });
            self.next_line_number += 1;
            self.discarding_too_long = false;
            self.discarded_len = 0;
            return;
        }

        if (self.pending.items.len > self.consumed) {
            const raw = self.pending.items[self.consumed..];
            const line = trimLine(raw);
            if (line.len > self.max_line_bytes) {
                try sink.handleTooLong(.{
                    .line_number = self.next_line_number,
                    .start_offset = self.pending_base_offset + self.consumed,
                    .end_offset = self.pending_base_offset + self.pending.items.len,
                    .byte_len = line.len,
                });
            } else {
                try sink.handleLine(.{
                    .bytes = line,
                    .line_number = self.next_line_number,
                    .start_offset = self.pending_base_offset + self.consumed,
                    .end_offset = self.pending_base_offset + self.pending.items.len,
                });
            }
            self.next_line_number += 1;
        }
        self.pending.clearRetainingCapacity();
        self.pending_base_offset = 0;
        self.consumed = 0;
        self.scan_index = 0;
    }

    fn emitCompleteLines(self: *LineScanner, sink: anytype) !void {
        while (self.scan_index < self.pending.items.len) : (self.scan_index += 1) {
            if (self.pending.items[self.scan_index] != '\n') continue;

            const raw = self.pending.items[self.consumed..self.scan_index];
            const line = trimLine(raw);
            if (line.len > self.max_line_bytes) {
                try sink.handleTooLong(.{
                    .line_number = self.next_line_number,
                    .start_offset = self.pending_base_offset + self.consumed,
                    .end_offset = self.pending_base_offset + self.scan_index + 1,
                    .byte_len = line.len,
                });
            } else {
                try sink.handleLine(.{
                    .bytes = line,
                    .line_number = self.next_line_number,
                    .start_offset = self.pending_base_offset + self.consumed,
                    .end_offset = self.pending_base_offset + self.scan_index + 1,
                });
            }
            self.next_line_number += 1;
            self.consumed = self.scan_index + 1;
        }
        self.compactIfUseful();
    }

    fn enterDiscardIfCurrentLineTooLong(self: *LineScanner) !void {
        const current_len = self.pending.items.len - self.consumed;
        if (current_len <= self.max_line_bytes) return;

        self.discarding_too_long = true;
        self.discard_start_offset = self.pending_base_offset + self.consumed;
        self.discarded_len = current_len;
        self.pending.clearRetainingCapacity();
        self.pending_base_offset = self.discard_start_offset + current_len;
        self.consumed = 0;
        self.scan_index = 0;
    }

    fn compactIfUseful(self: *LineScanner) void {
        if (self.consumed == 0) return;
        if (self.consumed < 4096 and self.consumed * 2 < self.pending.items.len) return;

        const remaining = self.pending.items[self.consumed..];
        std.mem.copyForwards(u8, self.pending.items[0..remaining.len], remaining);
        self.pending.items.len = remaining.len;
        self.pending_base_offset += self.consumed;
        self.scan_index -= self.consumed;
        self.consumed = 0;
    }
};

const BatchBuilder = struct {
    parent_alloc: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    inserts: std.json.ArrayHashMap(std.json.Value) = .{},
    docs: usize = 0,
    bytes: usize = 0,

    fn init(parent_alloc: std.mem.Allocator) BatchBuilder {
        return .{
            .parent_alloc = parent_alloc,
            .arena = std.heap.ArenaAllocator.init(parent_alloc),
        };
    }

    fn deinit(self: *BatchBuilder) void {
        self.arena.deinit();
    }

    fn allocator(self: *BatchBuilder) std.mem.Allocator {
        return self.arena.allocator();
    }

    fn reset(self: *BatchBuilder) void {
        self.inserts = .{};
        self.docs = 0;
        self.bytes = 0;
        _ = self.arena.reset(.retain_capacity);
    }

    fn shouldFlushBeforeAdd(self: *const BatchBuilder, line_len: usize, opts: LoadOptions) bool {
        if (self.docs == 0) return false;
        return self.docs >= opts.batch_size or self.bytes + line_len > opts.batch_bytes;
    }
};

const LoadProcessor = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    client: ?*antfly_client.AntflyClient,
    opts: LoadOptions,
    file_meta: FileMetadata,
    stats: LoadStats = .{},
    last_committed_offset: u64 = 0,
    last_committed_line: u64 = 0,
    checkpoint_path: ?[]const u8 = null,
    batch: BatchBuilder,
    line_arena: std.heap.ArenaAllocator,
    id_template: ?hbs.Template = null,

    fn init(
        alloc: std.mem.Allocator,
        io: std.Io,
        client: ?*antfly_client.AntflyClient,
        opts: LoadOptions,
        file_meta: FileMetadata,
        checkpoint_path: ?[]const u8,
        start: ?LoadCheckpoint,
    ) !LoadProcessor {
        var processor = LoadProcessor{
            .alloc = alloc,
            .io = io,
            .client = client,
            .opts = opts,
            .file_meta = file_meta,
            .last_committed_offset = if (start) |cp| cp.offset else 0,
            .last_committed_line = if (start) |cp| cp.line_number else 0,
            .checkpoint_path = checkpoint_path,
            .batch = BatchBuilder.init(alloc),
            .line_arena = std.heap.ArenaAllocator.init(alloc),
        };
        errdefer processor.deinit();
        if (opts.id_template) |template_source| {
            processor.id_template = try hbs.Template.init(alloc, template_source);
        }
        if (start) |cp| {
            processor.stats.loaded = cp.loaded;
            processor.stats.committed = cp.committed;
            processor.stats.invalid_json = cp.invalid_json;
            processor.stats.bad_id = cp.bad_id;
            processor.stats.too_long = cp.too_long;
            processor.stats.empty_line = cp.empty_line;
            processor.stats.duplicate_in_batch = cp.duplicate_in_batch;
            processor.stats.batch_count = cp.batch_count;
        }
        return processor;
    }

    fn deinit(self: *LoadProcessor) void {
        if (self.id_template) |*template| template.deinit();
        self.line_arena.deinit();
        self.batch.deinit();
    }

    fn handleLine(self: *LoadProcessor, line: Line) !void {
        if (self.stopForBatchLimit() and self.batch.docs == 0) return;
        self.stats.lines_seen = line.line_number;
        if (line.bytes.len == 0) {
            self.stats.empty_line += 1;
            self.last_committed_offset = line.end_offset;
            self.last_committed_line = line.line_number;
            return;
        }

        if (self.batch.shouldFlushBeforeAdd(line.bytes.len, self.opts)) {
            try self.flushBatch();
            if (self.stopForBatchLimit()) return;
        }

        _ = self.line_arena.reset(.retain_capacity);
        const line_alloc = self.line_arena.allocator();
        const parsed_for_id = std.json.parseFromSliceLeaky(std.json.Value, line_alloc, line.bytes, .{
            .allocate = .alloc_always,
        }) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => {
                self.stats.invalid_json += 1;
                self.last_committed_offset = line.end_offset;
                self.last_committed_line = line.line_number;
                try self.checkErrorLimit();
                return;
            },
        };

        const batch_alloc = self.batch.allocator();
        const doc_id = self.documentId(batch_alloc, parsed_for_id, line.bytes) catch |err| switch (err) {
            error.BadDocumentId => {
                self.last_committed_offset = line.end_offset;
                self.last_committed_line = line.line_number;
                return;
            },
            else => return err,
        };
        const is_duplicate = self.batch.inserts.map.contains(doc_id);
        if (is_duplicate) {
            self.stats.duplicate_in_batch += 1;
        }
        const parsed = std.json.parseFromSliceLeaky(std.json.Value, batch_alloc, line.bytes, .{
            .allocate = .alloc_always,
        }) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => {
                self.stats.invalid_json += 1;
                self.last_committed_offset = line.end_offset;
                self.last_committed_line = line.line_number;
                try self.checkErrorLimit();
                return;
            },
        };
        try self.batch.inserts.map.put(batch_alloc, doc_id, parsed);
        if (!is_duplicate) {
            self.batch.docs += 1;
            self.stats.loaded += 1;
        }
        self.batch.bytes += line.bytes.len + doc_id.len;
        self.last_committed_offset = line.end_offset;
        self.last_committed_line = line.line_number;

        if (self.batch.docs >= self.opts.batch_size or self.batch.bytes >= self.opts.batch_bytes) {
            try self.flushBatch();
        }
    }

    fn handleTooLong(self: *LoadProcessor, line: TooLongLine) !void {
        self.stats.lines_seen = line.line_number;
        self.stats.too_long += 1;
        self.last_committed_offset = line.end_offset;
        self.last_committed_line = line.line_number;
        try self.checkErrorLimit();
    }

    fn documentId(self: *LoadProcessor, batch_alloc: std.mem.Allocator, parsed: std.json.Value, line: []const u8) ![]const u8 {
        if (self.opts.id_field) |field| {
            if (parsed != .object) {
                self.stats.bad_id += 1;
                try self.checkErrorLimit();
                return error.BadDocumentId;
            }
            const val = parsed.object.get(field) orelse {
                self.stats.bad_id += 1;
                try self.checkErrorLimit();
                return error.BadDocumentId;
            };
            if (val != .string) {
                self.stats.bad_id += 1;
                try self.checkErrorLimit();
                return error.BadDocumentId;
            }
            return try batch_alloc.dupe(u8, val.string);
        }

        if (self.id_template) |*template| {
            const line_alloc = self.line_arena.allocator();
            const context = try jsonToHandlebarsValue(line_alloc, parsed);
            const rendered = template.renderSimple(line_alloc, context) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => {
                    self.stats.bad_id += 1;
                    try self.checkErrorLimit();
                    return error.BadDocumentId;
                },
            };
            const trimmed = std.mem.trim(u8, rendered, " \t\r\n");
            if (trimmed.len == 0) {
                self.stats.bad_id += 1;
                try self.checkErrorLimit();
                return error.BadDocumentId;
            }
            return try batch_alloc.dupe(u8, trimmed);
        }

        const hash = std.hash.Wyhash.hash(0, line);
        return try std.fmt.allocPrint(batch_alloc, "{x}", .{hash});
    }

    fn checkErrorLimit(self: *LoadProcessor) !void {
        if (self.opts.max_errors) |max_errors| {
            if (self.stats.rejected() > max_errors) return error.TooManyLoadErrors;
        }
    }

    fn stopForBatchLimit(self: *const LoadProcessor) bool {
        return self.stats.batch_count >= self.opts.max_batches;
    }

    fn flushBatch(self: *LoadProcessor) !void {
        if (self.batch.docs == 0) return;
        if (self.stopForBatchLimit()) return;

        if (!self.opts.dry_run) {
            var resp = try sendBatch(self.client.?, self.opts.catalog, self.opts.table_name, .{
                .inserts = self.batch.inserts,
                .sync_level = self.opts.sync_level,
            });
            defer resp.deinit();
            self.stats.committed += @intCast(self.batch.docs);
            if (self.opts.checkpoint_enabled) {
                if (self.checkpoint_path) |path| try self.writeCheckpoint(path);
            }
        }

        self.stats.sent += @intCast(self.batch.docs);
        self.stats.batch_count += 1;
        std.debug.print("Batch {d}: loaded {d} items (total loaded: {d}, rejected: {d})\n", .{
            self.stats.batch_count,
            self.batch.docs,
            self.stats.loaded,
            self.stats.rejected(),
        });
        self.batch.reset();
    }

    fn writeCheckpoint(self: *LoadProcessor, path: []const u8) !void {
        const checkpoint = LoadCheckpoint{
            .source_path = self.opts.file_path,
            .source_size = self.file_meta.size,
            .source_mtime_ns = self.file_meta.mtime_ns,
            .table_name = self.opts.table_name,
            .database_name = self.opts.catalog.database,
            .namespace_name = self.opts.catalog.namespace,
            .id_field = self.opts.id_field,
            .id_template = self.opts.id_template,
            .sync_level = if (self.opts.sync_level) |level| syncLevelName(level) else null,
            .offset = self.last_committed_offset,
            .line_number = self.last_committed_line,
            .loaded = self.stats.loaded,
            .committed = self.stats.committed,
            .invalid_json = self.stats.invalid_json,
            .bad_id = self.stats.bad_id,
            .too_long = self.stats.too_long,
            .empty_line = self.stats.empty_line,
            .duplicate_in_batch = self.stats.duplicate_in_batch,
            .batch_count = self.stats.batch_count + 1,
        };
        try writeCheckpointAtomically(self.alloc, self.io, path, checkpoint);
    }
};

pub fn load(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const opts = parseLoadOptions(allocator, args);
    const checkpoint_path = if (opts.checkpoint_enabled and opts.checkpoint_path == null)
        try defaultCheckpointPath(allocator, opts.file_path)
    else
        null;
    defer if (checkpoint_path) |path| allocator.free(path);

    const effective_checkpoint_path = opts.checkpoint_path orelse checkpoint_path;
    _ = try runLoad(allocator, io, client, opts, effective_checkpoint_path);
}

fn parseLoadOptions(allocator: std.mem.Allocator, args: *std.process.Args.Iterator) LoadOptions {
    var table_name: ?[]const u8 = null;
    var catalog = cli.CatalogFlags.defaultsFromEnv();
    var file_path: ?[]const u8 = null;
    var batch_size: usize = default_batch_size;
    var max_batches: usize = default_max_batches;
    var id_field: ?[]const u8 = null;
    var id_template: ?[]const u8 = null;
    var sync_level: ?antfly_client.types.SyncLevel = null;
    var read_buffer_bytes: usize = default_read_buffer_bytes;
    var max_line_bytes: usize = default_max_line_bytes;
    var batch_bytes: usize = default_batch_bytes;
    var checkpoint_path: ?[]const u8 = null;
    var resume_load = false;
    var checkpoint_enabled = true;
    var dry_run = false;
    var max_errors: ?u64 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--table") or std.mem.eql(u8, arg, "-t")) {
            table_name = args.next();
        } else if (cli.parseCatalogFlag(&catalog, arg, args)) {
            continue;
        } else if (std.mem.eql(u8, arg, "--file") or std.mem.eql(u8, arg, "-f")) {
            file_path = args.next();
        } else if (std.mem.eql(u8, arg, "--size")) {
            batch_size = parsePositive(usize, args.next(), "--size");
        } else if (std.mem.eql(u8, arg, "--batches")) {
            max_batches = parsePositive(usize, args.next(), "--batches");
        } else if (std.mem.eql(u8, arg, "--id-field")) {
            id_field = args.next();
        } else if (std.mem.eql(u8, arg, "--id-template")) {
            id_template = args.next();
        } else if (std.mem.eql(u8, arg, "--sync-level")) {
            sync_level = parseSyncLevel(args.next() orelse cli.fatal("--sync-level requires a value", .{})) orelse
                cli.fatal("invalid --sync-level", .{});
        } else if (std.mem.eql(u8, arg, "--read-buffer-bytes")) {
            read_buffer_bytes = parsePositive(usize, args.next(), "--read-buffer-bytes");
        } else if (std.mem.eql(u8, arg, "--max-line-bytes")) {
            max_line_bytes = parsePositive(usize, args.next(), "--max-line-bytes");
        } else if (std.mem.eql(u8, arg, "--batch-bytes")) {
            batch_bytes = parsePositive(usize, args.next(), "--batch-bytes");
        } else if (std.mem.eql(u8, arg, "--checkpoint")) {
            checkpoint_path = args.next();
        } else if (std.mem.eql(u8, arg, "--resume")) {
            resume_load = true;
        } else if (std.mem.eql(u8, arg, "--no-checkpoint")) {
            checkpoint_enabled = false;
        } else if (std.mem.eql(u8, arg, "--dry-run")) {
            dry_run = true;
        } else if (std.mem.eql(u8, arg, "--max-errors")) {
            max_errors = std.fmt.parseInt(u64, args.next() orelse cli.fatal("--max-errors requires a value", .{}), 10) catch cli.fatal("invalid --max-errors", .{});
        } else if (std.mem.eql(u8, arg, "--strict")) {
            max_errors = 0;
        } else {
            cli.fatal("unknown load flag: {s}", .{arg});
        }
    }
    _ = allocator;

    if (!checkpoint_enabled and resume_load) cli.fatal("--resume cannot be used with --no-checkpoint", .{});
    if (id_field != null and id_template != null) cli.fatal("--id-field and --id-template are mutually exclusive", .{});
    if (batch_size == 0) cli.fatal("--size must be greater than zero", .{});
    if (max_batches == 0) cli.fatal("--batches must be greater than zero", .{});
    if (read_buffer_bytes == 0) cli.fatal("--read-buffer-bytes must be greater than zero", .{});
    if (max_line_bytes == 0) cli.fatal("--max-line-bytes must be greater than zero", .{});
    if (batch_bytes == 0) cli.fatal("--batch-bytes must be greater than zero", .{});

    return .{
        .table_name = table_name orelse cli.fatal("--table is required", .{}),
        .catalog = catalog,
        .file_path = file_path orelse cli.fatal("--file is required", .{}),
        .batch_size = batch_size,
        .max_batches = max_batches,
        .id_field = id_field,
        .id_template = id_template,
        .sync_level = sync_level,
        .read_buffer_bytes = read_buffer_bytes,
        .max_line_bytes = max_line_bytes,
        .batch_bytes = batch_bytes,
        .checkpoint_path = checkpoint_path,
        .resume_load = resume_load,
        .checkpoint_enabled = checkpoint_enabled,
        .dry_run = dry_run,
        .max_errors = max_errors,
    };
}

fn parsePositive(comptime T: type, raw: ?[]const u8, flag: []const u8) T {
    const text = raw orelse cli.fatal("{s} requires a value", .{flag});
    return std.fmt.parseInt(T, text, 10) catch cli.fatal("invalid {s}", .{flag});
}

fn parseSyncLevel(text: []const u8) ?antfly_client.types.SyncLevel {
    if (std.mem.eql(u8, text, "propose")) return .propose;
    if (std.mem.eql(u8, text, "write")) return .write;
    if (std.mem.eql(u8, text, "query")) return .query;
    if (std.mem.eql(u8, text, "enrichments")) return .enrichments;
    if (std.mem.eql(u8, text, "full_index")) return .full_index;
    return null;
}

fn syncLevelName(level: antfly_client.types.SyncLevel) []const u8 {
    return switch (level) {
        .propose => "propose",
        .write => "write",
        .query => "query",
        .enrichments => "enrichments",
        .full_index => "full_index",
    };
}

fn runLoad(
    allocator: std.mem.Allocator,
    io: std.Io,
    client: ?*antfly_client.AntflyClient,
    opts: LoadOptions,
    checkpoint_path: ?[]const u8,
) !LoadStats {
    const file_meta = try statFile(io, opts.file_path);
    var checkpoint_parsed: ?std.json.Parsed(LoadCheckpoint) = null;
    defer if (checkpoint_parsed) |parsed| parsed.deinit();

    const checkpoint = if (opts.resume_load) blk: {
        const path = checkpoint_path orelse return error.CheckpointDisabled;
        checkpoint_parsed = try loadCheckpoint(allocator, io, path);
        try validateCheckpoint(checkpoint_parsed.?.value, opts, file_meta);
        break :blk checkpoint_parsed.?.value;
    } else null;

    const start_offset = if (checkpoint) |cp| cp.offset else 0;
    const start_line = if (checkpoint) |cp| cp.line_number + 1 else 1;

    const file = if (std.fs.path.isAbsolute(opts.file_path))
        try std.Io.Dir.openFileAbsolute(io, opts.file_path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, opts.file_path, .{});
    defer file.close(io);

    var reader = file.reader(io, &.{});
    try reader.seekTo(start_offset);

    var processor = try LoadProcessor.init(allocator, io, client, opts, file_meta, checkpoint_path, checkpoint);
    defer processor.deinit();

    var scanner = LineScanner.init(allocator, opts.max_line_bytes, start_offset, start_line);
    defer scanner.deinit();

    const read_buf = try allocator.alloc(u8, opts.read_buffer_bytes);
    defer allocator.free(read_buf);

    var offset = start_offset;
    while (!processor.stopForBatchLimit()) {
        const n = try reader.interface.readSliceShort(read_buf);
        if (n == 0) break;
        try scanner.feed(read_buf[0..n], offset, &processor);
        offset += n;
    }
    if (!processor.stopForBatchLimit()) try scanner.finish(&processor);
    try processor.flushBatch();

    std.debug.print(
        "Bulk load command successful. loaded={d} committed={d} sent={d} rejected={d} invalid_json={d} bad_id={d} too_long={d} empty={d} duplicate_in_batch={d}\n",
        .{
            processor.stats.loaded,
            processor.stats.committed,
            processor.stats.sent,
            processor.stats.rejected(),
            processor.stats.invalid_json,
            processor.stats.bad_id,
            processor.stats.too_long,
            processor.stats.empty_line,
            processor.stats.duplicate_in_batch,
        },
    );
    return processor.stats;
}

fn trimLine(line: []const u8) []const u8 {
    if (line.len > 0 and line[line.len - 1] == '\r') return line[0 .. line.len - 1];
    return line;
}

fn statFile(io: std.Io, path: []const u8) !FileMetadata {
    const stat = if (std.fs.path.isAbsolute(path)) blk: {
        var file = try std.Io.Dir.openFileAbsolute(io, path, .{});
        defer file.close(io);
        break :blk try file.stat(io);
    } else try std.Io.Dir.cwd().statFile(io, path, .{});
    return .{
        .size = stat.size,
        .mtime_ns = stat.mtime.toNanoseconds(),
    };
}

fn defaultCheckpointPath(alloc: std.mem.Allocator, file_path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}.antfly-load.checkpoint", .{file_path});
}

fn loadCheckpoint(alloc: std.mem.Allocator, io: std.Io, path: []const u8) !std.json.Parsed(LoadCheckpoint) {
    const raw = try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(checkpoint_max_bytes));
    defer alloc.free(raw);
    return try std.json.parseFromSlice(LoadCheckpoint, alloc, raw, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    });
}

fn validateCheckpoint(cp: LoadCheckpoint, opts: LoadOptions, meta: FileMetadata) !void {
    if (cp.version != checkpoint_version) return error.InvalidLoadCheckpoint;
    if (!std.mem.eql(u8, cp.source_path, opts.file_path)) return error.InvalidLoadCheckpoint;
    if (cp.source_size != meta.size or cp.source_mtime_ns != meta.mtime_ns) return error.InvalidLoadCheckpoint;
    if (!std.mem.eql(u8, cp.table_name, opts.table_name)) return error.InvalidLoadCheckpoint;
    if (!optionalStringEql(cp.database_name, opts.catalog.database)) return error.InvalidLoadCheckpoint;
    if (!optionalStringEql(cp.namespace_name, opts.catalog.namespace)) return error.InvalidLoadCheckpoint;
    if (!optionalStringEql(cp.id_field, opts.id_field)) return error.InvalidLoadCheckpoint;
    if (!optionalStringEql(cp.id_template, opts.id_template)) return error.InvalidLoadCheckpoint;
    const opts_sync_level = if (opts.sync_level) |level| syncLevelName(level) else null;
    if (!optionalStringEql(cp.sync_level, opts_sync_level)) return error.InvalidLoadCheckpoint;
    if (cp.offset > meta.size) return error.InvalidLoadCheckpoint;
}

fn optionalStringEql(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?, b.?);
}

fn sendBatch(
    client: *antfly_client.AntflyClient,
    catalog: cli.CatalogFlags,
    table_name: []const u8,
    req: antfly_client.types.BatchRequest,
) !antfly_client.openapi.ApiResponse(antfly_client.types.BatchResponse) {
    if (catalog.explicit()) |explicit| {
        return try client.inner.batchNamespaceTable(explicit.database, explicit.namespace, table_name, req);
    }
    return try client.batch(table_name, req);
}

fn jsonToHandlebarsValue(arena: std.mem.Allocator, value: std.json.Value) std.mem.Allocator.Error!hbs.Value {
    return switch (value) {
        .null => .null,
        .bool => |b| .{ .boolean = b },
        .integer => |i| .{ .integer = i },
        .float => |f| .{ .float = f },
        .number_string => |s| .{ .string = s },
        .string => |s| .{ .string = s },
        .array => |arr| blk: {
            const items = try arena.alloc(hbs.Value, arr.items.len);
            for (arr.items, 0..) |item, i| {
                items[i] = try jsonToHandlebarsValue(arena, item);
            }
            break :blk .{ .array = items };
        },
        .object => |obj| blk: {
            var map: hbs.ValueMap = .{};
            var it = obj.iterator();
            while (it.next()) |entry| {
                try map.put(arena, entry.key_ptr.*, try jsonToHandlebarsValue(arena, entry.value_ptr.*));
            }
            break :blk .{ .map = map };
        },
    };
}

fn writeCheckpointAtomically(alloc: std.mem.Allocator, io: std.Io, path: []const u8, checkpoint: LoadCheckpoint) !void {
    const encoded = try std.json.Stringify.valueAlloc(alloc, checkpoint, .{});
    defer alloc.free(encoded);

    if (std.fs.path.dirname(path)) |parent| {
        if (!std.fs.path.isAbsolute(parent)) try std.Io.Dir.cwd().createDirPath(io, parent);
    }

    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp", .{path});
    defer alloc.free(tmp_path);

    {
        var file = if (std.fs.path.isAbsolute(tmp_path))
            try std.Io.Dir.createFileAbsolute(io, tmp_path, .{ .truncate = true })
        else
            try std.Io.Dir.cwd().createFile(io, tmp_path, .{ .truncate = true });
        defer file.close(io);
        var buf: [4096]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(encoded);
        try writer.end();
        try file.sync(io);
    }

    std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io) catch |err| {
        std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
        return err;
    };
}

test "line scanner handles split lines crlf final line and long lines" {
    const alloc = std.testing.allocator;

    const Sink = struct {
        lines: std.ArrayListUnmanaged([]u8) = .empty,
        too_long: u64 = 0,

        fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            for (self.lines.items) |line| allocator.free(line);
            self.lines.deinit(allocator);
        }

        fn handleLine(self: *@This(), line: Line) !void {
            try self.lines.append(alloc, try alloc.dupe(u8, line.bytes));
        }

        fn handleTooLong(self: *@This(), _: TooLongLine) !void {
            self.too_long += 1;
        }
    };

    var sink = Sink{};
    defer sink.deinit(alloc);
    var scanner = LineScanner.init(alloc, 6, 0, 1);
    defer scanner.deinit();

    try scanner.feed("ab", 0, &sink);
    try scanner.feed("c\r\nde", 2, &sink);
    try scanner.feed("f\n012345", 7, &sink);
    try scanner.feed("678\nz", 14, &sink);
    try scanner.finish(&sink);

    try std.testing.expectEqual(@as(usize, 3), sink.lines.items.len);
    try std.testing.expectEqualStrings("abc", sink.lines.items[0]);
    try std.testing.expectEqualStrings("def", sink.lines.items[1]);
    try std.testing.expectEqualStrings("z", sink.lines.items[2]);
    try std.testing.expectEqual(@as(u64, 1), sink.too_long);
}

test "load processor allocates parsed strings independent of streaming buffer" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var processor = try LoadProcessor.init(alloc, io, null, .{
        .table_name = "t",
        .file_path = "in.ndjson",
        .id_field = "id",
        .dry_run = true,
        .checkpoint_enabled = false,
    }, .{ .size = 100, .mtime_ns = 1 }, null, null);
    defer processor.deinit();

    var backing = try alloc.dupe(
        u8,
        "{\"id\":\"doc-1\",\"body\":\"first boundary value\"}\n{\"id\":\"doc-2\",\"body\":\"second boundary value\"}",
    );
    defer alloc.free(backing);

    var scanner = LineScanner.init(alloc, 1024, 0, 1);
    defer scanner.deinit();
    try scanner.feed(backing[0..17], 0, &processor);
    @memset(backing[0..17], 'x');
    try scanner.feed(backing[17..], 17, &processor);
    @memset(backing, 'y');
    try scanner.finish(&processor);

    try std.testing.expectEqual(@as(usize, 2), processor.batch.docs);
    const doc1 = processor.batch.inserts.map.get("doc-1").?;
    const doc2 = processor.batch.inserts.map.get("doc-2").?;
    try std.testing.expectEqualStrings("first boundary value", doc1.object.get("body").?.string);
    try std.testing.expectEqualStrings("second boundary value", doc2.object.get("body").?.string);
}

test "load processor rejects malformed json and bad ids" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var processor = try LoadProcessor.init(alloc, io, null, .{
        .table_name = "t",
        .file_path = "in.ndjson",
        .id_field = "id",
        .dry_run = true,
        .checkpoint_enabled = false,
    }, .{ .size = 100, .mtime_ns = 1 }, null, null);
    defer processor.deinit();

    try processor.handleLine(.{ .bytes = "{\"id\":\"ok\",\"v\":1}", .line_number = 1, .start_offset = 0, .end_offset = 17 });
    try processor.handleLine(.{ .bytes = "{", .line_number = 2, .start_offset = 18, .end_offset = 19 });
    try processor.handleLine(.{ .bytes = "{\"id\":7}", .line_number = 3, .start_offset = 20, .end_offset = 28 });

    try std.testing.expectEqual(@as(u64, 1), processor.stats.loaded);
    try std.testing.expectEqual(@as(u64, 1), processor.stats.invalid_json);
    try std.testing.expectEqual(@as(u64, 1), processor.stats.bad_id);
}

test "load processor advances resume cursor for tolerated rejected and empty lines" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var processor = try LoadProcessor.init(alloc, io, null, .{
        .table_name = "t",
        .file_path = "in.ndjson",
        .id_field = "id",
        .dry_run = true,
        .checkpoint_enabled = false,
    }, .{ .size = 100, .mtime_ns = 1 }, null, null);
    defer processor.deinit();

    try processor.handleLine(.{ .bytes = "{\"id\":\"ok\",\"v\":1}", .line_number = 1, .start_offset = 0, .end_offset = 17 });
    try std.testing.expectEqual(@as(u64, 17), processor.last_committed_offset);
    try std.testing.expectEqual(@as(u64, 1), processor.last_committed_line);

    try processor.handleLine(.{ .bytes = "{", .line_number = 2, .start_offset = 18, .end_offset = 20 });
    try std.testing.expectEqual(@as(u64, 20), processor.last_committed_offset);
    try std.testing.expectEqual(@as(u64, 2), processor.last_committed_line);

    try processor.handleLine(.{ .bytes = "{\"id\":7}", .line_number = 3, .start_offset = 21, .end_offset = 29 });
    try std.testing.expectEqual(@as(u64, 29), processor.last_committed_offset);
    try std.testing.expectEqual(@as(u64, 3), processor.last_committed_line);

    try processor.handleLine(.{ .bytes = "", .line_number = 4, .start_offset = 30, .end_offset = 31 });
    try std.testing.expectEqual(@as(u64, 31), processor.last_committed_offset);
    try std.testing.expectEqual(@as(u64, 4), processor.last_committed_line);
    try std.testing.expectEqual(@as(u64, 1), processor.stats.invalid_json);
    try std.testing.expectEqual(@as(u64, 1), processor.stats.bad_id);
    try std.testing.expectEqual(@as(u64, 1), processor.stats.empty_line);
}

test "load processor handles duplicate ids as last write wins without inflating loaded count" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var processor = try LoadProcessor.init(alloc, io, null, .{
        .table_name = "t",
        .file_path = "in.ndjson",
        .id_field = "id",
        .dry_run = true,
        .checkpoint_enabled = false,
    }, .{ .size = 100, .mtime_ns = 1 }, null, null);
    defer processor.deinit();

    try processor.handleLine(.{ .bytes = "{\"id\":\"dup\",\"v\":1}", .line_number = 1, .start_offset = 0, .end_offset = 18 });
    try processor.handleLine(.{ .bytes = "{\"id\":\"dup\",\"v\":2}", .line_number = 2, .start_offset = 19, .end_offset = 37 });

    try std.testing.expectEqual(@as(u64, 1), processor.stats.loaded);
    try std.testing.expectEqual(@as(usize, 1), processor.batch.docs);
    try std.testing.expectEqual(@as(u64, 1), processor.stats.duplicate_in_batch);
    try std.testing.expectEqual(@as(u64, 0), processor.stats.rejected());
    try std.testing.expectEqual(@as(u64, 37), processor.last_committed_offset);
    const doc = processor.batch.inserts.map.get("dup").?;
    try std.testing.expectEqual(@as(i64, 2), doc.object.get("v").?.integer);
}

test "load processor renders ids from handlebars template" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var processor = try LoadProcessor.init(alloc, io, null, .{
        .table_name = "t",
        .file_path = "in.ndjson",
        .id_template = "{{tenant}}:{{nested.id}}",
        .dry_run = true,
        .checkpoint_enabled = false,
    }, .{ .size = 100, .mtime_ns = 1 }, null, null);
    defer processor.deinit();

    try processor.handleLine(.{ .bytes = "{\"tenant\":\"acme\",\"nested\":{\"id\":\"doc-7\"},\"body\":\"ok\"}", .line_number = 1, .start_offset = 0, .end_offset = 55 });

    try std.testing.expectEqual(@as(u64, 1), processor.stats.loaded);
    try std.testing.expect(processor.batch.inserts.map.contains("acme:doc-7"));
}

test "load processor rejects empty rendered id template" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var processor = try LoadProcessor.init(alloc, io, null, .{
        .table_name = "t",
        .file_path = "in.ndjson",
        .id_template = "{{missing}}",
        .dry_run = true,
        .checkpoint_enabled = false,
    }, .{ .size = 100, .mtime_ns = 1 }, null, null);
    defer processor.deinit();

    try processor.handleLine(.{ .bytes = "{\"id\":\"doc-1\"}", .line_number = 1, .start_offset = 0, .end_offset = 14 });

    try std.testing.expectEqual(@as(u64, 0), processor.stats.loaded);
    try std.testing.expectEqual(@as(u64, 1), processor.stats.bad_id);
}

test "load sync level parser supports public values" {
    try std.testing.expectEqual(antfly_client.types.SyncLevel.propose, parseSyncLevel("propose").?);
    try std.testing.expectEqual(antfly_client.types.SyncLevel.write, parseSyncLevel("write").?);
    try std.testing.expectEqual(antfly_client.types.SyncLevel.query, parseSyncLevel("query").?);
    try std.testing.expectEqual(antfly_client.types.SyncLevel.enrichments, parseSyncLevel("enrichments").?);
    try std.testing.expectEqual(antfly_client.types.SyncLevel.full_index, parseSyncLevel("full_index").?);
    try std.testing.expect(parseSyncLevel("full_text") == null);
    try std.testing.expect(parseSyncLevel("aknn") == null);
    try std.testing.expect(parseSyncLevel("full-text") == null);
}

test "checkpoint validation rejects changed source and load config" {
    const cp = LoadCheckpoint{
        .source_path = "a.ndjson",
        .source_size = 10,
        .source_mtime_ns = 99,
        .table_name = "t",
        .id_field = "id",
        .sync_level = "write",
        .offset = 4,
    };
    try validateCheckpoint(cp, .{
        .table_name = "t",
        .file_path = "a.ndjson",
        .id_field = "id",
        .sync_level = .write,
    }, .{ .size = 10, .mtime_ns = 99 });
    try std.testing.expectError(error.InvalidLoadCheckpoint, validateCheckpoint(cp, .{
        .table_name = "other",
        .file_path = "a.ndjson",
        .id_field = "id",
        .sync_level = .write,
    }, .{ .size = 10, .mtime_ns = 99 }));
    try std.testing.expectError(error.InvalidLoadCheckpoint, validateCheckpoint(cp, .{
        .table_name = "t",
        .file_path = "a.ndjson",
        .id_field = null,
        .sync_level = .write,
    }, .{ .size = 10, .mtime_ns = 99 }));
    try std.testing.expectError(error.InvalidLoadCheckpoint, validateCheckpoint(cp, .{
        .table_name = "t",
        .file_path = "a.ndjson",
        .id_field = "id",
        .sync_level = .query,
    }, .{ .size = 10, .mtime_ns = 99 }));

    const template_cp = LoadCheckpoint{
        .source_path = "a.ndjson",
        .source_size = 10,
        .source_mtime_ns = 99,
        .table_name = "t",
        .id_template = "{{tenant}}:{{id}}",
        .offset = 4,
    };
    try validateCheckpoint(template_cp, .{
        .table_name = "t",
        .file_path = "a.ndjson",
        .id_template = "{{tenant}}:{{id}}",
    }, .{ .size = 10, .mtime_ns = 99 });
    try std.testing.expectError(error.InvalidLoadCheckpoint, validateCheckpoint(template_cp, .{
        .table_name = "t",
        .file_path = "a.ndjson",
        .id_template = "{{id}}",
    }, .{ .size = 10, .mtime_ns = 99 }));
}

test "load checkpoint owns strings independently of read buffer" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/loader.checkpoint", .{tmp.sub_path});

    {
        var file = try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = true });
        defer file.close(io);
        var buf: [4096]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(
            "{\"version\":1,\"source_path\":\"a.ndjson\",\"source_size\":10,\"source_mtime_ns\":99,\"table_name\":\"docs\",\"id_template\":\"{{tenant}}:{{id}}\",\"sync_level\":\"write\",\"offset\":4,\"line_number\":1}",
        );
        try writer.end();
    }

    var parsed = try loadCheckpoint(alloc, io, path);
    defer parsed.deinit();

    const scratch = try alloc.alloc(u8, 4096);
    defer alloc.free(scratch);
    @memset(scratch, 'x');

    try validateCheckpoint(parsed.value, .{
        .table_name = "docs",
        .file_path = "a.ndjson",
        .id_template = "{{tenant}}:{{id}}",
        .sync_level = .write,
    }, .{ .size = 10, .mtime_ns = 99 });
    try std.testing.expectEqual(@as(u64, 4), parsed.value.offset);
    try std.testing.expectEqualStrings("docs", parsed.value.table_name);
}

test "dry-run load streams boundary-straddling ndjson without bogus invalid json" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/loader-boundary.ndjson", .{tmp.sub_path});

    {
        var file = try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = true });
        defer file.close(io);
        var buf: [4096]u8 = undefined;
        var writer = file.writer(io, &buf);
        for (0..2000) |i| {
            try writer.interface.print(
                "{{\"id\":\"doc-{d}\",\"body\":\"boundary text value {d} with enough bytes to cross small buffers\"}}",
                .{ i, i },
            );
            if (i + 1 < 2000) try writer.interface.writeByte('\n');
        }
        try writer.end();
    }

    const stats = try runLoad(alloc, io, null, .{
        .table_name = "docs",
        .file_path = path,
        .batch_size = 37,
        .max_batches = 1000,
        .id_field = "id",
        .read_buffer_bytes = 31,
        .max_line_bytes = 1024,
        .batch_bytes = 2048,
        .checkpoint_enabled = false,
        .dry_run = true,
    }, null);

    try std.testing.expectEqual(@as(u64, 2000), stats.loaded);
    try std.testing.expectEqual(@as(u64, 2000), stats.sent);
    try std.testing.expectEqual(@as(u64, 0), stats.committed);
    try std.testing.expectEqual(@as(u64, 0), stats.invalid_json);
    try std.testing.expectEqual(@as(u64, 0), stats.bad_id);
    try std.testing.expectEqual(@as(u64, 0), stats.too_long);
}
