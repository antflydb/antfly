// Copyright 2026 Antfly, Inc.
// Licensed under the Apache License, Version 2.0.

//! Opt-in, bounded diagnostic capture. Inputs are private corpus data: never
//! print them to ordinary logs. The operator must create the destination.
const std = @import("std");
const platform = @import("antfly_platform");

const max_records = 64;
const max_input_bytes = 256 * 1024;
var direct_sequence = std.atomic.Value(u64).init(0);
var http_sequence = std.atomic.Value(u64).init(0);

pub fn now() u64 {
    return platform.time.monotonicNs();
}

pub const Shape = struct {
    items: usize,
    token_lengths: [32]usize = @splat(0),
    max_tokens: usize,
    active_tokens: usize,
    padded_tokens: usize,
};

pub const Trace = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    directory: []const u8,
    id: u64,
    started_ns: u64,
    path: []const u8,
    model: []const u8,
    inputs: []const []const u8,
    task_type: []const u8,
    instruction: ?[]const u8,
    backend: []const u8 = "unknown",
    admission_ns: u64 = 0,
    resolve_manifest_ns: u64 = 0,
    acquire_ns: u64 = 0,
    asset_prepare_ns: u64 = 0,
    tokenize_ns: u64 = 0,
    execution_lock_ns: u64 = 0,
    execute_pool_normalize_ns: u64 = 0,
    attempts: usize = 0,
    shapes: [64]Shape = undefined,
    shape_count: usize = 0,
    shapes_truncated: bool = false,

    pub fn begin(allocator: std.mem.Allocator, io: std.Io, path: []const u8, model: []const u8, inputs: []const []const u8, task_type: []const u8, instruction: ?[]const u8) ?Trace {
        const directory = platform.env.getenv("ANTFLY_EMBED_TRACE_DIR") orelse return null;
        if (!std.fs.path.isAbsolute(directory) or !captureFits(inputs, instruction)) return null;
        const sequence = if (std.mem.eql(u8, path, "managed_direct")) &direct_sequence else &http_sequence;
        const id = sequence.fetchAdd(1, .monotonic);
        if (id >= max_records) return null;
        return .{ .allocator = allocator, .io = io, .directory = directory, .id = id, .started_ns = now(), .path = path, .model = model, .inputs = inputs, .task_type = task_type, .instruction = instruction };
    }

    pub fn shape(self: *Trace, lengths: []const usize, padded_items: usize, padded_length: usize) void {
        if (self.shape_count == self.shapes.len or lengths.len > 32) {
            self.shapes_truncated = true;
            return;
        }
        var value = Shape{ .items = lengths.len, .max_tokens = padded_length, .active_tokens = 0, .padded_tokens = padded_items * padded_length };
        for (lengths, 0..) |length, i| {
            value.token_lengths[i] = length;
            value.active_tokens += length;
        }
        self.shapes[self.shape_count] = value;
        self.shape_count += 1;
    }

    pub fn finish(self: *Trace, vectors: ?[]const []const f32) void {
        self.write(vectors) catch {
            // Diagnostics must never make an otherwise valid embedding fail.
            std.log.warn("embedding trace could not be written; check the opt-in capture directory", .{});
        };
    }

    fn write(self: *Trace, vectors: ?[]const []const f32) !void {
        const elapsed = now() -| self.started_ns;
        const data = try std.json.Stringify.valueAlloc(self.allocator, .{
            .version = 1,
            .started_ns = self.started_ns,
            .path = self.path,
            .model = self.model,
            .input = self.inputs,
            .task_type = self.task_type,
            .instruction = self.instruction,
            .backend = self.backend,
            .success = vectors != null,
            .vectors = vectors,
            .attempts = self.attempts,
            .shapes = self.shapes[0..self.shape_count],
            .shapes_truncated = self.shapes_truncated,
            .timing_ns = .{
                .total = elapsed,
                .admission = self.admission_ns,
                .resolve_manifest = self.resolve_manifest_ns,
                .model_acquire = self.acquire_ns,
                .asset_prepare = self.asset_prepare_ns,
                .tokenize = self.tokenize_ns,
                .execution_lock = self.execution_lock_ns,
                .execute_pool_normalize = self.execute_pool_normalize_ns,
            },
        }, .{});
        defer self.allocator.free(data);
        if (data.len > 2 * 1024 * 1024) return;
        var dir = try std.Io.Dir.openDirAbsolute(self.io, self.directory, .{});
        defer dir.close(self.io);
        var name_buffer: [128]u8 = undefined;
        const name = try std.fmt.bufPrint(&name_buffer, "embed-{x}-{x}.json", .{ self.started_ns, self.id });
        var file = try dir.createFile(self.io, name, .{
            .exclusive = true,
            .truncate = false,
            .permissions = if (std.Io.File.Permissions.has_executable_bit) .fromMode(0o600) else .default_file,
        });
        defer file.close(self.io);
        try file.writeStreamingAll(self.io, data);
    }
};

fn captureFits(inputs: []const []const u8, instruction: ?[]const u8) bool {
    if (inputs.len == 0 or inputs.len > 32) return false;
    var bytes: usize = if (instruction) |value| value.len else 0;
    for (inputs) |input| {
        bytes = std.math.add(usize, bytes, input.len) catch return false;
        if (bytes > max_input_bytes) return false;
    }
    return true;
}

test "embedding trace bounds private inputs and records actual padding" {
    try std.testing.expect(captureFits(&.{ "one", "two" }, null));
    try std.testing.expect(!captureFits(&.{}, null));
    const large = try std.testing.allocator.alloc(u8, max_input_bytes + 1);
    defer std.testing.allocator.free(large);
    try std.testing.expect(!captureFits(&.{large}, null));
    var trace = Trace{ .allocator = std.testing.allocator, .io = std.testing.io, .directory = "", .id = 0, .started_ns = 0, .path = "test", .model = "test", .inputs = &.{}, .task_type = "RETRIEVAL_DOCUMENT", .instruction = null };
    trace.shape(&.{ 20, 100 }, 2, 100);
    try std.testing.expectEqual(@as(usize, 120), trace.shapes[0].active_tokens);
    try std.testing.expectEqual(@as(usize, 200), trace.shapes[0].padded_tokens);
}

test "embedding trace writes replayable private JSON without overwriting" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path_len = try tmp.dir.realPath(std.testing.io, &path_buffer);
    var trace = Trace{ .allocator = std.testing.allocator, .io = std.testing.io, .directory = path_buffer[0..path_len], .id = 2, .started_ns = 1, .path = "managed_direct", .model = "test", .inputs = &.{"quotes \" and newline\n한국"}, .task_type = "RETRIEVAL_DOCUMENT", .instruction = null };
    try trace.write(&.{&.{ 0.5, 0.5 }});
    try std.testing.expectError(error.PathAlreadyExists, trace.write(null));
    const data = try tmp.dir.readFileAlloc(std.testing.io, "embed-1-2.json", std.testing.allocator, .limited(4096));
    defer std.testing.allocator.free(data);
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, data, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings(trace.inputs[0], parsed.value.object.get("input").?.array.items[0].string);
    try std.testing.expect(parsed.value.object.get("success").?.bool);
}
