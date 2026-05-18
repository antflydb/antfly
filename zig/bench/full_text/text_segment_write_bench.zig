// Copyright 2026 Antfly, Inc.
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
const antfly = @import("antfly_text_bench");

const Allocator = std.mem.Allocator;
const Storage = antfly.lsm_backend.Storage;

const vocabulary = [_][]const u8{
    "alpha",  "bravo",  "charlie", "delta",   "echo",   "foxtrot",  "golf",  "hotel",
    "india",  "juliet", "kilo",    "lima",    "micro",  "november", "oscar", "papa",
    "query",  "rank",   "search",  "token",   "update", "vector",   "write", "zulu",
    "common", "merge",  "segment", "posting", "field",  "stored",   "index", "disk",
};

const Config = struct {
    samples: usize = 3,
    docs: usize = 20_000,
    batch_size: usize = 1_000,
    terms_per_doc: usize = 12,
    merge_width: usize = 8,
    storage_mode: StorageSelection = .host,
};

const StorageSelection = enum {
    host,
    native,
    memory,
    both,
};

const StorageCounters = struct {
    read_file: u64 = 0,
    read_bytes: u64 = 0,
    write_file: u64 = 0,
    write_bytes: u64 = 0,
    rename: u64 = 0,
    delete_file: u64 = 0,
    delete_tree: u64 = 0,

    fn delta(after: StorageCounters, before: StorageCounters) StorageCounters {
        return .{
            .read_file = after.read_file - before.read_file,
            .read_bytes = after.read_bytes - before.read_bytes,
            .write_file = after.write_file - before.write_file,
            .write_bytes = after.write_bytes - before.write_bytes,
            .rename = after.rename - before.rename,
            .delete_file = after.delete_file - before.delete_file,
            .delete_tree = after.delete_tree - before.delete_tree,
        };
    }
};

const StorageHarness = struct {
    const CountingStorage = struct {
        backing: Storage,
        counters: StorageCounters = .{},

        fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.createDirPath(path);
        }

        fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counters.read_file += 1;
            const bytes = try self.backing.readFileAlloc(allocator, path, max_bytes);
            self.counters.read_bytes += bytes.len;
            return bytes;
        }

        fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const bytes = try self.backing.readFileRangeAlloc(allocator, path, offset, len);
            self.counters.read_bytes += bytes.len;
            return bytes;
        }

        fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.fileSize(path);
        }

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const bytes = try self.backing.readFileTrailerAlloc(allocator, path, len);
            self.counters.read_bytes += bytes.len;
            return bytes;
        }

        fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counters.write_file += 1;
            self.counters.write_bytes += contents.len;
            return self.backing.writeFileAbsolute(path, contents);
        }

        fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counters.rename += 1;
            return self.backing.renameAbsolute(old_path, new_path);
        }

        fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counters.delete_file += 1;
            return self.backing.deleteFileAbsolute(path);
        }

        fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.counters.delete_tree += 1;
            return self.backing.deleteTree(path);
        }

        fn nowNs(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.nowNs();
        }
    };

    const counting_vtable: Storage.VTable = .{
        .create_dir_path = CountingStorage.createDirPath,
        .read_file_alloc = CountingStorage.readFileAlloc,
        .read_file_range_alloc = CountingStorage.readFileRangeAlloc,
        .file_size = CountingStorage.fileSize,
        .read_file_trailer_alloc = CountingStorage.readFileTrailerAlloc,
        .write_file_absolute = CountingStorage.writeFileAbsolute,
        .rename_absolute = CountingStorage.renameAbsolute,
        .delete_file_absolute = CountingStorage.deleteFileAbsolute,
        .delete_tree = CountingStorage.deleteTree,
        .now_ns = CountingStorage.nowNs,
    };

    allocator: Allocator,
    mode: StorageSelection,
    memory_backing: ?*antfly.lsm_backend.MemoryStorage = null,
    native_backing: ?*antfly.lsm_backend.storage_io.NativeStorage = null,
    counting_ctx: ?*CountingStorage = null,

    fn init(allocator: Allocator, mode: StorageSelection) !StorageHarness {
        var harness = StorageHarness{
            .allocator = allocator,
            .mode = mode,
        };
        switch (mode) {
            .host, .memory => {
                const backing = try allocator.create(antfly.lsm_backend.MemoryStorage);
                errdefer allocator.destroy(backing);
                backing.* = antfly.lsm_backend.MemoryStorage.init(allocator);
                errdefer backing.deinit();
                harness.memory_backing = backing;
                if (mode == .host) {
                    const counting_ctx = try allocator.create(CountingStorage);
                    errdefer allocator.destroy(counting_ctx);
                    counting_ctx.* = .{ .backing = backing.storage() };
                    harness.counting_ctx = counting_ctx;
                }
            },
            .native => {
                const backing = try allocator.create(antfly.lsm_backend.storage_io.NativeStorage);
                errdefer allocator.destroy(backing);
                backing.* = try antfly.lsm_backend.storage_io.NativeStorage.init(allocator, .threaded);
                errdefer backing.deinit();
                harness.native_backing = backing;

                const counting_ctx = try allocator.create(CountingStorage);
                errdefer allocator.destroy(counting_ctx);
                counting_ctx.* = .{ .backing = backing.storage() };
                harness.counting_ctx = counting_ctx;
            },
            .both => unreachable,
        }
        return harness;
    }

    fn deinit(self: *StorageHarness) void {
        if (self.counting_ctx) |ctx| self.allocator.destroy(ctx);
        if (self.native_backing) |backing| {
            backing.deinit();
            self.allocator.destroy(backing);
        }
        if (self.memory_backing) |backing| {
            backing.deinit();
            self.allocator.destroy(backing);
        }
        self.* = undefined;
    }

    fn storage(self: *StorageHarness) Storage {
        return switch (self.mode) {
            .host, .native => antfly.lsm_backend.HostStorage.init(self.counting_ctx.?, &counting_vtable).storage(),
            .memory => self.memory_backing.?.storage(),
            .both => unreachable,
        };
    }

    fn snapshotCounters(self: *const StorageHarness) StorageCounters {
        if (self.counting_ctx) |ctx| return ctx.counters;
        return .{};
    }
};

const SegmentRef = struct {
    path: []u8,
    bytes: usize,
    docs: usize,

    fn deinit(self: *SegmentRef, allocator: Allocator) void {
        allocator.free(self.path);
        self.* = undefined;
    }
};

const Snapshot = struct {
    storage: StorageCounters,
    segment_count: usize,
    segment_bytes: usize,
    docs: usize,
};

const Scenario = struct {
    allocator: Allocator,
    cfg: Config,
    sample_index: usize,
    storage_kind: StorageSelection,
    label: []u8,
    root_dir: []u8,
    storage_harness: StorageHarness,
    segments: std.ArrayListUnmanaged(SegmentRef) = .empty,
    next_segment_id: usize = 1,

    fn init(allocator: Allocator, cfg: Config, sample_index: usize, storage_kind: StorageSelection) !Scenario {
        var storage_harness = try StorageHarness.init(allocator, storage_kind);
        errdefer storage_harness.deinit();

        const label = try std.fmt.allocPrint(allocator, "{s}", .{@tagName(storage_kind)});
        errdefer allocator.free(label);
        const root_dir = try std.fmt.allocPrint(allocator, "{s}/text-segment-bench-{s}-{d}", .{
            if (storage_kind == .native) "/tmp" else "",
            label,
            sample_index,
        });
        errdefer allocator.free(root_dir);

        try storage_harness.storage().createDirPath(root_dir);

        return .{
            .allocator = allocator,
            .cfg = cfg,
            .sample_index = sample_index,
            .storage_kind = storage_kind,
            .label = label,
            .root_dir = root_dir,
            .storage_harness = storage_harness,
        };
    }

    fn deinit(self: *Scenario) void {
        self.storage_harness.storage().deleteTree(self.root_dir) catch {};
        for (self.segments.items) |*segment_ref| segment_ref.deinit(self.allocator);
        self.segments.deinit(self.allocator);
        self.storage_harness.deinit();
        self.allocator.free(self.root_dir);
        self.allocator.free(self.label);
        self.* = undefined;
    }

    fn snapshot(self: *const Scenario) Snapshot {
        var segment_bytes: usize = 0;
        var docs: usize = 0;
        for (self.segments.items) |segment_ref| {
            segment_bytes += segment_ref.bytes;
            docs += segment_ref.docs;
        }
        return .{
            .storage = self.storage_harness.snapshotCounters(),
            .segment_count = self.segments.items.len,
            .segment_bytes = segment_bytes,
            .docs = docs,
        };
    }

    fn allocateSegmentPath(self: *Scenario) ![]u8 {
        const id = self.next_segment_id;
        self.next_segment_id += 1;
        return try std.fmt.allocPrint(self.allocator, "{s}/{d}.seg", .{ self.root_dir, id });
    }
};

fn nanotime() u64 {
    return antfly.platform_time.monotonicNs();
}

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    const cfg = try parseArgs(alloc, init.minimal.args);

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;

    try out.print(
        "text segment write bench samples={d} docs={d} batch_size={d} terms_per_doc={d} merge_width={d} storage={s}\n",
        .{ cfg.samples, cfg.docs, cfg.batch_size, cfg.terms_per_doc, cfg.merge_width, @tagName(cfg.storage_mode) },
    );
    try stdout_writer.flush();

    const storage_modes: []const StorageSelection = switch (cfg.storage_mode) {
        .host => &[_]StorageSelection{.host},
        .native => &[_]StorageSelection{.native},
        .memory => &[_]StorageSelection{.memory},
        .both => &[_]StorageSelection{ .host, .native, .memory },
    };

    for (storage_modes) |storage_mode| {
        for (0..cfg.samples) |sample_index| {
            var scenario = try Scenario.init(alloc, cfg, sample_index, storage_mode);
            defer scenario.deinit();

            try runBuildSegments(out, &stdout_writer, &scenario);
            try runMergeOnce(out, &stdout_writer, &scenario);
            try runForceMerge(out, &stdout_writer, &scenario);
        }
    }
    try stdout_writer.flush();
}

fn runBuildSegments(writer: anytype, stdout_writer: anytype, scenario: *Scenario) !void {
    const before = scenario.snapshot();
    const start_ns = nanotime();
    var build_ns: u64 = 0;
    var publish_ns: u64 = 0;

    var doc_start: usize = 0;
    while (doc_start < scenario.cfg.docs) {
        const doc_count = @min(scenario.cfg.batch_size, scenario.cfg.docs - doc_start);
        const build_start = nanotime();
        const segment_bytes = try buildSegment(scenario.allocator, doc_start, doc_count, scenario.cfg.terms_per_doc);
        build_ns += nanotime() - build_start;
        defer scenario.allocator.free(segment_bytes);

        const path = try scenario.allocateSegmentPath();
        errdefer scenario.allocator.free(path);
        const publish_start = nanotime();
        try publishBytes(scenario.storage_harness.storage(), scenario.allocator, path, segment_bytes);
        publish_ns += nanotime() - publish_start;

        try scenario.segments.append(scenario.allocator, .{
            .path = path,
            .bytes = segment_bytes.len,
            .docs = doc_count,
        });
        doc_start += doc_count;
    }

    const after = scenario.snapshot();
    try printResult(writer, scenario, "build_segments", scenario.cfg.docs, nanotime() - start_ns, build_ns, 0, publish_ns, before, after);
    try stdout_writer.flush();
}

fn runMergeOnce(writer: anytype, stdout_writer: anytype, scenario: *Scenario) !void {
    if (scenario.segments.items.len < 2) return;
    const before = scenario.snapshot();
    const start_ns = nanotime();
    var merge_ns: u64 = 0;
    var publish_ns: u64 = 0;
    var merged = std.ArrayListUnmanaged(SegmentRef).empty;
    errdefer {
        for (merged.items) |*segment_ref| segment_ref.deinit(scenario.allocator);
        merged.deinit(scenario.allocator);
    }

    var index: usize = 0;
    while (index < scenario.segments.items.len) {
        const end = @min(index + scenario.cfg.merge_width, scenario.segments.items.len);
        if (end - index == 1) {
            try merged.append(scenario.allocator, scenario.segments.items[index]);
            scenario.segments.items[index] = undefined;
            index = end;
            continue;
        }
        const merged_ref = try mergeSegmentRange(scenario, scenario.segments.items[index..end], &merge_ns, &publish_ns);
        try merged.append(scenario.allocator, merged_ref);
        index = end;
    }

    scenario.segments.clearRetainingCapacity();
    try scenario.segments.appendSlice(scenario.allocator, merged.items);
    merged.items.len = 0;
    merged.deinit(scenario.allocator);

    const after = scenario.snapshot();
    try printResult(writer, scenario, "merge_once", before.docs, nanotime() - start_ns, 0, merge_ns, publish_ns, before, after);
    try stdout_writer.flush();
}

fn runForceMerge(writer: anytype, stdout_writer: anytype, scenario: *Scenario) !void {
    if (scenario.segments.items.len < 2) return;
    const before = scenario.snapshot();
    const start_ns = nanotime();
    var merge_ns: u64 = 0;
    var publish_ns: u64 = 0;
    while (scenario.segments.items.len > 1) {
        const end = @min(scenario.cfg.merge_width, scenario.segments.items.len);
        const merged_ref = try mergeSegmentRange(scenario, scenario.segments.items[0..end], &merge_ns, &publish_ns);
        std.mem.copyForwards(SegmentRef, scenario.segments.items[0 .. scenario.segments.items.len - end], scenario.segments.items[end..]);
        scenario.segments.items.len -= end;
        try scenario.segments.append(scenario.allocator, merged_ref);
    }
    const after = scenario.snapshot();
    try printResult(writer, scenario, "force_merge", before.docs, nanotime() - start_ns, 0, merge_ns, publish_ns, before, after);
    try stdout_writer.flush();
}

fn mergeSegmentRange(scenario: *Scenario, source: []SegmentRef, merge_ns: *u64, publish_ns: *u64) !SegmentRef {
    var segments = try scenario.allocator.alloc([]u8, source.len);
    defer scenario.allocator.free(segments);
    for (source, 0..) |segment_ref, i| {
        segments[i] = try scenario.storage_harness.storage().readFileAlloc(scenario.allocator, segment_ref.path, 512 * 1024 * 1024);
    }
    defer for (segments) |bytes| scenario.allocator.free(bytes);

    const merge_start = nanotime();
    const merged_bytes = try antfly.segment.mergeSegments(scenario.allocator, segments);
    merge_ns.* += nanotime() - merge_start;
    defer scenario.allocator.free(merged_bytes);

    const path = try scenario.allocateSegmentPath();
    errdefer scenario.allocator.free(path);
    const publish_start = nanotime();
    try publishBytes(scenario.storage_harness.storage(), scenario.allocator, path, merged_bytes);
    publish_ns.* += nanotime() - publish_start;

    var docs: usize = 0;
    for (source) |*segment_ref| {
        docs += segment_ref.docs;
        scenario.storage_harness.storage().deleteFileAbsolute(segment_ref.path) catch {};
        scenario.allocator.free(segment_ref.path);
    }
    return .{
        .path = path,
        .bytes = merged_bytes.len,
        .docs = docs,
    };
}

fn buildSegment(allocator: Allocator, doc_start: usize, doc_count: usize, terms_per_doc: usize) ![]u8 {
    var inv_builder = antfly.inverted.InvertedIndexBuilder.init(allocator, .{});
    defer inv_builder.deinit();

    var seg_writer = antfly.segment.SegmentWriter.init(allocator);
    defer seg_writer.deinit();
    const field_idx = try seg_writer.addField("body");

    const hit_count = @min(@max(terms_per_doc, 1), vocabulary.len);
    const hits = try allocator.alloc(antfly.inverted.InvertedIndexBuilder.TermHit, hit_count);
    defer allocator.free(hits);

    for (0..doc_count) |local_doc| {
        const global_doc = doc_start + local_doc;
        const doc_id = try std.fmt.allocPrint(allocator, "doc:{d}", .{global_doc});
        defer allocator.free(doc_id);
        const stored = try std.fmt.allocPrint(allocator, "{{\"id\":{d},\"body\":\"common segment merge token {d}\"}}", .{
            global_doc,
            global_doc % vocabulary.len,
        });
        defer allocator.free(stored);

        try seg_writer.addStoredDoc(doc_id, stored);
        for (hits, 0..) |*hit, i| {
            hit.* = .{
                .term = vocabulary[(global_doc + i) % vocabulary.len],
                .freq = 1,
                .norm = @intCast(hit_count),
            };
        }
        try inv_builder.addDocument(@intCast(local_doc), hits);
    }

    const inverted_bytes = try inv_builder.build();
    defer allocator.free(inverted_bytes);
    try seg_writer.addSection(field_idx, .inverted_text, inverted_bytes);
    return try seg_writer.build();
}

fn publishBytes(storage: Storage, allocator: Allocator, path: []const u8, bytes: []const u8) !void {
    var writer = try storage.beginAtomicWrite(allocator, path);
    var active = true;
    defer if (active) writer.abort();
    try writer.appendSlice(bytes);
    active = false;
    try writer.finish();
}

fn printResult(
    writer: anytype,
    scenario: *const Scenario,
    workload: []const u8,
    docs: usize,
    ns: u64,
    build_ns: u64,
    merge_ns: u64,
    publish_ns: u64,
    before: Snapshot,
    after: Snapshot,
) !void {
    const storage_delta = StorageCounters.delta(after.storage, before.storage);
    const ns_per_doc = @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(@max(docs, 1)));
    try writer.print(
        "{{\"scenario\":\"{s}\",\"storage\":\"{s}\",\"sample\":{d},\"workload\":\"{s}\",\"docs\":{d},\"ns\":{d},\"ns_per_doc\":{d:.2},\"build_ns\":{d},\"merge_ns\":{d},\"publish_ns\":{d}",
        .{ scenario.label, @tagName(scenario.storage_kind), scenario.sample_index, workload, docs, ns, ns_per_doc, build_ns, merge_ns, publish_ns },
    );
    try writer.print(
        ",\"segments_before\":{d},\"segments_after\":{d},\"segment_bytes_before\":{d},\"segment_bytes_after\":{d}",
        .{ before.segment_count, after.segment_count, before.segment_bytes, after.segment_bytes },
    );
    try writer.print(
        ",\"storage_read_file\":{d},\"storage_read_bytes\":{d},\"storage_write_file\":{d},\"storage_write_bytes\":{d},\"storage_rename\":{d},\"storage_delete_file\":{d},\"storage_delete_tree\":{d}}}\n",
        .{
            storage_delta.read_file,
            storage_delta.read_bytes,
            storage_delta.write_file,
            storage_delta.write_bytes,
            storage_delta.rename,
            storage_delta.delete_file,
            storage_delta.delete_tree,
        },
    );
}

fn parseArgs(alloc: Allocator, proc_args: std.process.Args) !Config {
    var cfg = Config{};
    var args = try std.process.Args.Iterator.initAllocator(proc_args, alloc);
    defer args.deinit();
    _ = args.next();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--samples")) {
            cfg.samples = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--docs")) {
            cfg.docs = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            cfg.batch_size = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--terms-per-doc")) {
            cfg.terms_per_doc = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--merge-width")) {
            cfg.merge_width = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--storage")) {
            const value = args.next() orelse return error.InvalidArgument;
            cfg.storage_mode = std.meta.stringToEnum(StorageSelection, value) orelse return error.InvalidArgument;
        } else {
            return error.InvalidArgument;
        }
    }
    if (cfg.docs == 0) return error.InvalidArgument;
    if (cfg.batch_size == 0) return error.InvalidArgument;
    if (cfg.merge_width < 2) return error.InvalidArgument;
    return cfg;
}

fn parseNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const value = args.next() orelse return error.InvalidArgument;
    return std.fmt.parseInt(usize, value, 10) catch {
        std.debug.print("invalid value for {s}: {s}\n", .{ flag, value });
        return error.InvalidArgument;
    };
}
