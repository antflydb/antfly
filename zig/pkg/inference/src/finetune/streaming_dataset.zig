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

// ---------------------------------------------------------------------------
// Streaming JSONL reader with shuffle buffer, sharding, and resume semantics.
//
// This reader iterates JSONL records line-by-line from an in-memory byte
// buffer, optionally through a fixed-size shuffle buffer, optionally
// restricted to a single DDP shard, and supports resume-from-step via a
// tiny binary state blob.
//
// The reader does not parse JSON — it emits raw record bytes so downstream
// code can decide how to interpret each line.
//
// Slurps the whole file into memory on init (via `Dir.readFileAlloc`); the
// Zig 0.16 Io interface does not support seekable streaming, making
// in-memory scanning the simplest correct design.
// ---------------------------------------------------------------------------

/// Maximum bytes a single JSONL line may occupy. Long chat records can be
/// quite large; 10 MiB is well above typical instruction examples.
pub const max_line_bytes: usize = 10 * 1024 * 1024;

/// Magic number used by save/load of resume state. Arbitrary marker that
/// lets `loadResumeState` detect a corrupted or wrong-type blob.
pub const resume_magic: u64 = 0x5452554D_53524541;

pub const StreamConfig = struct {
    /// Shuffle buffer size (number of records held in memory for random
    /// sampling). 0 disables shuffling (sequential read).
    shuffle_buffer: usize = 8192,
    /// Shard index in [0, num_shards). Only records where
    /// `record_index % num_shards == shard_id` are emitted.
    shard_id: u32 = 0,
    num_shards: u32 = 1,
    /// Seed for the shuffle buffer PRNG.
    seed: u64 = 42,
    /// If true, loop the file forever (many epochs). Otherwise EOF is final.
    loop: bool = true,
    /// How many records to skip from the start (for resume). These are consumed
    /// before sharding — sharding and shuffling happen on the post-skip stream.
    skip_records: u64 = 0,
};

pub const StreamReader = struct {
    allocator: std.mem.Allocator,
    /// Whole-file backing store. Owned by StreamReader.
    data: []u8,
    /// Byte offset into `data` of the next line to read.
    pos: usize,
    config: StreamConfig,

    /// Shuffle buffer: owned record bytes with their source indices (for resume).
    buf_records: std.ArrayList([]u8),
    buf_indices: std.ArrayList(u64),
    rng: std.Random.DefaultPrng,

    /// Total number of records read from file (not emitted) — used for resume.
    records_read: u64 = 0,
    /// Number of records emitted so far.
    records_emitted: u64 = 0,
    /// Set once the file reports EOF.
    eof: bool = false,

    /// Open `path` (relative to the current working directory) and slurp its
    /// contents into memory. The returned StreamReader owns the byte buffer.
    pub fn init(
        allocator: std.mem.Allocator,
        path: []const u8,
        config: StreamConfig,
    ) !StreamReader {
        return initAt(allocator, std.Io.Dir.cwd(), std.testing.io, path, config);
    }

    /// Variant that reads the file from a caller-supplied `Io.Dir` using a
    /// caller-supplied `Io`. Tests open files under a temporary directory and
    /// pass `testing.io`; production code should pass the application's Io.
    pub fn initAt(
        allocator: std.mem.Allocator,
        dir: std.Io.Dir,
        io: std.Io,
        sub_path: []const u8,
        config: StreamConfig,
    ) !StreamReader {
        std.debug.assert(config.num_shards >= 1);
        std.debug.assert(config.shard_id < config.num_shards);

        const data = try dir.readFileAlloc(io, sub_path, allocator, .unlimited);
        errdefer allocator.free(data);

        return StreamReader{
            .allocator = allocator,
            .data = data,
            .pos = 0,
            .config = config,
            .buf_records = .empty,
            .buf_indices = .empty,
            .rng = std.Random.DefaultPrng.init(config.seed),
            .records_read = 0,
            .records_emitted = 0,
            .eof = false,
        };
    }

    pub fn deinit(self: *StreamReader) void {
        for (self.buf_records.items) |rec| self.allocator.free(rec);
        self.buf_records.deinit(self.allocator);
        self.buf_indices.deinit(self.allocator);
        self.allocator.free(self.data);
        self.* = undefined;
    }

    /// Rewind to the start of the in-memory buffer. Clears the shuffle buffer.
    pub fn rewind(self: *StreamReader) !void {
        self.pos = 0;
        for (self.buf_records.items) |rec| self.allocator.free(rec);
        self.buf_records.clearRetainingCapacity();
        self.buf_indices.clearRetainingCapacity();
        self.eof = false;
    }

    /// Read one raw line from `data`, returning an owned slice (caller must
    /// free) or null at EOF. Handles LF and CRLF line endings and tolerates
    /// a final line without a trailing newline.
    fn readRawLineOwned(self: *StreamReader) !?[]u8 {
        if (self.pos >= self.data.len) return null;

        const rest = self.data[self.pos..];
        var end: usize = 0;
        while (end < rest.len and rest[end] != '\n') : (end += 1) {}
        // Enforce max_line_bytes against the un-stripped length — callers
        // already get this guard for free against pathological inputs.
        if (end > max_line_bytes) return error.LineTooLong;

        var line_end = end;
        // Strip trailing \r for CRLF files.
        if (line_end > 0 and rest[line_end - 1] == '\r') line_end -= 1;

        const line = try self.allocator.dupe(u8, rest[0..line_end]);
        // Advance past the newline if we found one.
        self.pos += end + @as(usize, if (end < rest.len) 1 else 0);
        return line;
    }

    /// Internal: try to advance the file by one line and return an owned
    /// record, handling `skip_records`, sharding, and looping. Returns null
    /// only when EOF is reached and looping is disabled.
    ///
    /// The returned record has already passed the shard filter; the caller
    /// may place it in the shuffle buffer or emit it directly.
    fn fetchOne(self: *StreamReader) !?struct { bytes: []u8, source_index: u64 } {
        while (true) {
            const maybe = self.readRawLineOwned() catch |err| return err;
            if (maybe == null) {
                // EOF.
                self.eof = true;
                if (self.config.loop) {
                    try self.rewind();
                    continue;
                }
                return null;
            }
            const line = maybe.?;

            // Resume skip: consumed against the absolute source stream,
            // before sharding. This matches the spec: skip is independent
            // of which shard we belong to.
            if (self.config.skip_records > 0) {
                self.config.skip_records -= 1;
                self.records_read += 1;
                self.allocator.free(line);
                continue;
            }

            const source_index = self.records_read;
            self.records_read += 1;

            // Apply shard filter.
            if (self.config.num_shards > 1) {
                const mod: u64 = source_index % @as(u64, self.config.num_shards);
                if (mod != @as(u64, self.config.shard_id)) {
                    self.allocator.free(line);
                    continue;
                }
            }

            return .{ .bytes = line, .source_index = source_index };
        }
    }

    /// Read the next record. Returns an owned slice the caller must free.
    /// Returns null when the stream is exhausted (only if `loop = false`).
    pub fn next(self: *StreamReader) !?[]u8 {
        if (self.config.shuffle_buffer == 0) {
            const got = try self.fetchOne();
            if (got) |g| {
                self.records_emitted += 1;
                return g.bytes;
            }
            return null;
        }

        // Refill shuffle buffer up to capacity. If looping is disabled and
        // we hit EOF mid-fill, fetchOne will return null and we stop filling.
        while (self.buf_records.items.len < self.config.shuffle_buffer) {
            const got = try self.fetchOne();
            if (got) |g| {
                self.buf_records.append(self.allocator, g.bytes) catch |err| {
                    self.allocator.free(g.bytes);
                    return err;
                };
                self.buf_indices.append(self.allocator, g.source_index) catch |err| {
                    // Roll back the record append to keep the two lists in
                    // lock-step. The slice was just appended at the end.
                    const last = self.buf_records.pop().?;
                    self.allocator.free(last);
                    return err;
                };
            } else {
                // EOF + no loop. Stop filling and fall through to drain.
                break;
            }
        }

        if (self.buf_records.items.len == 0) {
            // Truly nothing left.
            return null;
        }

        // Pick a random slot and swap-remove. Ownership of the bytes
        // transfers to the caller.
        const rand = self.rng.random();
        const idx = rand.uintLessThan(usize, self.buf_records.items.len);
        const bytes = self.buf_records.swapRemove(idx);
        _ = self.buf_indices.swapRemove(idx);
        self.records_emitted += 1;
        return bytes;
    }

    /// Save a small resume state (magic + records_read) into an owned slice.
    pub fn saveResumeStateAlloc(self: *const StreamReader, allocator: std.mem.Allocator) ![]u8 {
        const buf = try allocator.alloc(u8, 16);
        std.mem.writeInt(u64, buf[0..8], resume_magic, .little);
        std.mem.writeInt(u64, buf[8..16], self.records_read, .little);
        return buf;
    }

    /// Advance an existing reader to a resume point using a saved blob.
    /// Does NOT preserve exact shuffle-buffer contents (the buffer is rebuilt
    /// as the stream replays); but the deterministic stream offset is restored.
    pub fn loadResumeStateFromBytes(self: *StreamReader, state: []const u8) !void {
        if (state.len < 16) return error.InvalidResumeState;
        const magic = std.mem.readInt(u64, state[0..8], .little);
        if (magic != resume_magic) return error.InvalidResumeMagic;
        const target_records_read = std.mem.readInt(u64, state[8..16], .little);

        try self.rewind();
        self.config.skip_records = target_records_read;
        self.records_read = 0;
        self.records_emitted = 0;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

fn writeSyntheticJsonl(
    dir: std.Io.Dir,
    io: std.Io,
    name: []const u8,
    n: usize,
) !void {
    var file = try dir.createFile(io, name, .{});
    defer file.close(io);

    var offset: u64 = 0;
    var buf: [128]u8 = undefined;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const line = try std.fmt.bufPrint(&buf, "{{\"idx\":{d},\"text\":\"record-{d}\"}}\n", .{ i, i });
        try file.writePositionalAll(io, line, offset);
        offset += line.len;
    }
}

fn collectAll(reader: *StreamReader, allocator: std.mem.Allocator) !std.ArrayList([]u8) {
    var out: std.ArrayList([]u8) = .empty;
    errdefer {
        for (out.items) |r| allocator.free(r);
        out.deinit(allocator);
    }
    while (try reader.next()) |rec| {
        try out.append(allocator, rec);
    }
    return out;
}

fn freeAll(allocator: std.mem.Allocator, list: *std.ArrayList([]u8)) void {
    for (list.items) |r| allocator.free(r);
    list.deinit(allocator);
}

/// Extract the "idx" field from a synthetic record like
/// `{"idx":42,"text":"record-42"}`. Returns the parsed integer.
fn extractIdx(record: []const u8) !usize {
    const needle = "\"idx\":";
    const start = std.mem.indexOf(u8, record, needle) orelse return error.Malformed;
    const after = start + needle.len;
    var end = after;
    while (end < record.len and record[end] >= '0' and record[end] <= '9') : (end += 1) {}
    return try std.fmt.parseInt(usize, record[after..end], 10);
}

test "sequential read, no sharding" {
    const n = 100;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeSyntheticJsonl(tmp.dir, testing.io, "data.jsonl", n);

    var reader = try StreamReader.initAt(testing.allocator, tmp.dir, testing.io, "data.jsonl", .{
        .shuffle_buffer = 0,
        .loop = false,
    });
    defer reader.deinit();

    var all = try collectAll(&reader, testing.allocator);
    defer freeAll(testing.allocator, &all);

    try testing.expectEqual(@as(usize, n), all.items.len);
    for (all.items, 0..) |rec, i| {
        const idx = try extractIdx(rec);
        try testing.expectEqual(i, idx);
    }
}

test "sharding shard 0 of 2 reads even records" {
    const n = 100;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeSyntheticJsonl(tmp.dir, testing.io, "data.jsonl", n);

    var reader = try StreamReader.initAt(testing.allocator, tmp.dir, testing.io, "data.jsonl", .{
        .shuffle_buffer = 0,
        .loop = false,
        .num_shards = 2,
        .shard_id = 0,
    });
    defer reader.deinit();

    var all = try collectAll(&reader, testing.allocator);
    defer freeAll(testing.allocator, &all);

    try testing.expectEqual(@as(usize, n / 2), all.items.len);
    for (all.items, 0..) |rec, i| {
        const idx = try extractIdx(rec);
        try testing.expectEqual(i * 2, idx);
    }
}

test "sharding shard 1 of 2 reads odd records" {
    const n = 100;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeSyntheticJsonl(tmp.dir, testing.io, "data.jsonl", n);

    var reader = try StreamReader.initAt(testing.allocator, tmp.dir, testing.io, "data.jsonl", .{
        .shuffle_buffer = 0,
        .loop = false,
        .num_shards = 2,
        .shard_id = 1,
    });
    defer reader.deinit();

    var all = try collectAll(&reader, testing.allocator);
    defer freeAll(testing.allocator, &all);

    try testing.expectEqual(@as(usize, n / 2), all.items.len);
    for (all.items, 0..) |rec, i| {
        const idx = try extractIdx(rec);
        try testing.expectEqual(i * 2 + 1, idx);
    }
}

test "shuffle buffer returns same set (possibly reordered)" {
    const n = 100;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeSyntheticJsonl(tmp.dir, testing.io, "data.jsonl", n);

    var reader = try StreamReader.initAt(testing.allocator, tmp.dir, testing.io, "data.jsonl", .{
        .shuffle_buffer = 16,
        .loop = false,
    });
    defer reader.deinit();

    var all = try collectAll(&reader, testing.allocator);
    defer freeAll(testing.allocator, &all);

    try testing.expectEqual(@as(usize, n), all.items.len);

    var seen = @as([n]bool, @splat(false));
    for (all.items) |rec| {
        const idx = try extractIdx(rec);
        try testing.expect(idx < n);
        try testing.expect(!seen[idx]);
        seen[idx] = true;
    }
    for (seen) |s| try testing.expect(s);
}

test "loop = false exhausts and returns null" {
    const n = 10;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeSyntheticJsonl(tmp.dir, testing.io, "data.jsonl", n);

    var reader = try StreamReader.initAt(testing.allocator, tmp.dir, testing.io, "data.jsonl", .{
        .shuffle_buffer = 0,
        .loop = false,
    });
    defer reader.deinit();

    var count: usize = 0;
    while (try reader.next()) |rec| {
        testing.allocator.free(rec);
        count += 1;
    }
    try testing.expectEqual(@as(usize, n), count);
    // Subsequent calls remain null.
    try testing.expect((try reader.next()) == null);
}

test "loop = true yields indefinitely" {
    const n = 10;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeSyntheticJsonl(tmp.dir, testing.io, "data.jsonl", n);

    var reader = try StreamReader.initAt(testing.allocator, tmp.dir, testing.io, "data.jsonl", .{
        .shuffle_buffer = 0,
        .loop = true,
    });
    defer reader.deinit();

    var count: usize = 0;
    while (count < 2 * n) : (count += 1) {
        const rec = (try reader.next()) orelse return error.UnexpectedNull;
        defer testing.allocator.free(rec);
        const idx = try extractIdx(rec);
        // Each lap should produce ids 0..n-1 in order.
        try testing.expectEqual(count % n, idx);
    }
}

test "long line ~1 MB" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const long_len: usize = 1 * 1024 * 1024;
    const payload = try testing.allocator.alloc(u8, long_len);
    defer testing.allocator.free(payload);
    @memset(payload, 'x');

    {
        var file = try tmp.dir.createFile(testing.io, "big.jsonl", .{});
        defer file.close(testing.io);
        var offset: u64 = 0;
        try file.writePositionalAll(testing.io, "{\"a\":1}\n", offset);
        offset += "{\"a\":1}\n".len;
        try file.writePositionalAll(testing.io, payload, offset);
        offset += payload.len;
        try file.writePositionalAll(testing.io, "\n", offset);
        offset += 1;
        try file.writePositionalAll(testing.io, "{\"c\":3}\n", offset);
    }

    var reader = try StreamReader.initAt(testing.allocator, tmp.dir, testing.io, "big.jsonl", .{
        .shuffle_buffer = 0,
        .loop = false,
    });
    defer reader.deinit();

    const r0 = (try reader.next()) orelse return error.UnexpectedNull;
    defer testing.allocator.free(r0);
    try testing.expectEqualStrings("{\"a\":1}", r0);

    const r1 = (try reader.next()) orelse return error.UnexpectedNull;
    defer testing.allocator.free(r1);
    try testing.expectEqual(long_len, r1.len);
    try testing.expect(r1[0] == 'x' and r1[long_len - 1] == 'x');

    const r2 = (try reader.next()) orelse return error.UnexpectedNull;
    defer testing.allocator.free(r2);
    try testing.expectEqualStrings("{\"c\":3}", r2);

    try testing.expect((try reader.next()) == null);
}

test "resume: save state after K records and restore" {
    const n = 50;
    const k: usize = 17;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeSyntheticJsonl(tmp.dir, testing.io, "data.jsonl", n);

    // Read K records.
    var reader = try StreamReader.initAt(testing.allocator, tmp.dir, testing.io, "data.jsonl", .{
        .shuffle_buffer = 0,
        .loop = false,
    });
    defer reader.deinit();

    var i: usize = 0;
    while (i < k) : (i += 1) {
        const rec = (try reader.next()) orelse return error.UnexpectedNull;
        testing.allocator.free(rec);
    }

    // Save resume state into memory.
    const state = try reader.saveResumeStateAlloc(testing.allocator);
    defer testing.allocator.free(state);

    // Spin up a second reader and jump to the saved point.
    var reader2 = try StreamReader.initAt(testing.allocator, tmp.dir, testing.io, "data.jsonl", .{
        .shuffle_buffer = 0,
        .loop = false,
    });
    defer reader2.deinit();

    try reader2.loadResumeStateFromBytes(state);

    // From here, we should see records k, k+1, ..., n-1 in order.
    var j: usize = k;
    while (try reader2.next()) |rec| {
        defer testing.allocator.free(rec);
        const idx = try extractIdx(rec);
        try testing.expectEqual(j, idx);
        j += 1;
    }
    try testing.expectEqual(@as(usize, n), j);
}
