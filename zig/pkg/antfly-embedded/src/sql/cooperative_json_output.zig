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

//! Compact JSON under borrowed authority. No admission or deadline is created.
const std = @import("std");
pub const Control = struct {
    ptr: *anyopaque,
    check: *const fn (*anyopaque) anyerror!void,
};
const A = std.mem.Allocator;
const E = std.Io.Writer.Error;

pub const Options = struct {
    emit_null_optional_fields: bool = true,
    emit_nonportable_numbers_as_strings: bool = false,
};

/// Never use remap/realloc: physical copies and final compaction poll in chunks.
pub const Buffer = struct {
    allocator: A,
    control: Control,
    storage: []u8 = &.{},
    len: usize = 0,
    work: usize = 0,
    pub fn init(allocator: A, control: Control) Buffer {
        return .{ .allocator = allocator, .control = control };
    }
    pub fn deinit(self: *Buffer) void {
        self.allocator.free(self.storage);
        self.* = undefined;
    }
    fn poll(self: *Buffer) !void {
        if (self.work % 64 == 0) try self.control.check(self.control.ptr);
        self.work +%= 1;
    }
    fn copy(self: *Buffer, to: []u8, from: []const u8) !void {
        var offset: usize = 0;
        while (offset < from.len) {
            try self.control.check(self.control.ptr);
            const end = offset + @min(@as(usize, 4096), from.len - offset);
            @memcpy(to[offset..end], from[offset..end]);
            offset = end;
        }
    }
    pub fn append(self: *Buffer, bytes: []const u8) !void {
        try self.poll();
        const end = std.math.add(usize, self.len, bytes.len) catch return error.OutOfMemory;
        if (end > self.storage.len) {
            const capacity = @max(end, self.storage.len +| self.storage.len / 2 +| 64);
            const replacement = try self.allocator.alloc(u8, capacity);
            errdefer self.allocator.free(replacement);
            try self.copy(replacement[0..self.len], self.storage[0..self.len]);
            self.allocator.free(self.storage);
            self.storage = replacement;
        }
        // Small fragments need not poll individually, but no copy exceeds 4 KiB.
        if (bytes.len <= 4096) @memcpy(self.storage[self.len..end], bytes) else try self.copy(self.storage[self.len..end], bytes);
        self.len = end;
    }
    pub fn takeOwned(self: *Buffer) ![]u8 {
        try self.control.check(self.control.ptr);
        if (self.len == self.storage.len) {
            const result = self.storage;
            self.storage = &.{};
            self.len = 0;
            return result;
        }
        const result = try self.allocator.alloc(u8, self.len);
        errdefer self.allocator.free(result);
        try self.copy(result, self.storage[0..self.len]);
        self.allocator.free(self.storage);
        self.storage = &.{};
        self.len = 0;
        try self.control.check(self.control.ptr);
        return result;
    }
};

const Sink = struct {
    bytes: Buffer,
    writer: std.Io.Writer = .{ .vtable = &.{ .drain = drain }, .buffer = &.{} },
    failure: ?anyerror = null,
    count_only: bool = false,
    count: usize = 0,
    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) E!usize {
        const self: *Sink = @fieldParentPtr("writer", w);
        if (self.failure != null) return error.WriteFailed;
        var count: usize = 0;
        for (data, 0..) |part, index| {
            const repeats = if (index + 1 == data.len) splat else 1;
            for (0..repeats) |_| {
                if (self.count_only) {
                    self.bytes.poll() catch |err| {
                        self.failure = err;
                        return error.WriteFailed;
                    };
                    self.count = std.math.add(usize, self.count, part.len) catch {
                        self.failure = error.OutOfMemory;
                        return error.WriteFailed;
                    };
                } else self.bytes.append(part) catch |err| {
                    self.failure = err;
                    return error.WriteFailed;
                };
                count += part.len;
            }
        }
        return count;
    }
};

pub const Encoder = struct {
    pub const Error = E;

    sink: Sink,
    writer: *std.Io.Writer,
    options: Options,
    control: Control,
    punctuation: enum { beginning, none, comma, colon } = .beginning,
    work: usize = 0,
    failure: ?anyerror = null,

    /// Initialize at the final address before custom jsonStringify borrows it.
    pub fn init(self: *Encoder, allocator: A, control: Control, options: Options) void {
        self.* = .{ .sink = .{ .bytes = .init(allocator, control) }, .writer = undefined, .control = control, .options = options };
        self.writer = &self.sink.writer;
    }
    pub fn deinit(self: *Encoder) void {
        self.sink.bytes.deinit();
        self.* = undefined;
    }
    pub fn original(self: *const Encoder, err: anyerror) anyerror {
        if (err == error.WriteFailed) {
            if (self.failure) |cause| return cause;
            if (self.sink.failure) |cause| return cause;
        }
        return err;
    }
    fn fail(self: *Encoder, err: anyerror) E {
        if (err != error.WriteFailed) self.failure = err;
        return error.WriteFailed;
    }
    fn poll(self: *Encoder) E!void {
        if (self.failure != null or self.sink.failure != null) return error.WriteFailed;
        if (self.work % 64 == 0) self.control.check(self.control.ptr) catch |err| return self.fail(err);
        self.work +%= 1;
    }
    fn prefix(self: *Encoder) E!void {
        try self.poll();
        switch (self.punctuation) {
            .comma => try self.writer.writeByte(','),
            .colon => try self.writer.writeByte(':'),
            else => {},
        }
    }
    pub fn beginArray(self: *Encoder) E!void {
        try self.prefix();
        try self.writer.writeByte('[');
        self.punctuation = .none;
    }
    pub fn endArray(self: *Encoder) E!void {
        try self.poll();
        try self.writer.writeByte(']');
        self.punctuation = .comma;
    }
    pub fn beginObject(self: *Encoder) E!void {
        try self.prefix();
        try self.writer.writeByte('{');
        self.punctuation = .none;
    }
    pub fn endObject(self: *Encoder) E!void {
        try self.poll();
        try self.writer.writeByte('}');
        self.punctuation = .comma;
    }
    pub fn beginWriteRaw(self: *Encoder) E!void {
        try self.prefix();
    }
    pub fn endWriteRaw(self: *Encoder) void {
        self.punctuation = .comma;
    }
    pub fn objectField(self: *Encoder, key: []const u8) E!void {
        try self.prefix();
        try self.stringRaw(key);
        self.punctuation = .colon;
    }
    fn stringRaw(self: *Encoder, bytes: []const u8) E!void {
        try self.writer.writeByte('"');
        var offset: usize = 0;
        while (offset < bytes.len) {
            self.control.check(self.control.ptr) catch |err| return self.fail(err);
            const end = offset + @min(@as(usize, 4096), bytes.len - offset);
            // Unicode escapes are disabled: splitting UTF-8 byte sequences is
            // safe because unescaped bytes are copied verbatim by this routine.
            try std.json.Stringify.encodeJsonStringChars(bytes[offset..end], .{}, self.writer);
            offset = end;
        }
        try self.writer.writeByte('"');
    }
    fn validUtf8(self: *Encoder, bytes: []const u8) E!bool {
        var offset: usize = 0;
        var checked: usize = 0;
        while (offset < bytes.len) {
            if (offset >= checked) {
                self.control.check(self.control.ptr) catch |err| return self.fail(err);
                checked = offset +| 4096;
            }
            const width = std.unicode.utf8ByteSequenceLength(bytes[offset]) catch return false;
            if (width > bytes.len - offset) return false;
            _ = std.unicode.utf8Decode(bytes[offset..][0..width]) catch return false;
            offset += width;
        }
        return true;
    }
    pub fn print(self: *Encoder, comptime fmt: []const u8, args: anytype) E!void {
        try self.prefix();
        try self.writer.print(fmt, args);
        self.punctuation = .comma;
    }
    pub fn write(self: *Encoder, value: anytype) E!void {
        const T = @TypeOf(value);
        try self.poll();
        if (T == std.json.Value) return self.writeValue(value);
        switch (@typeInfo(T)) {
            .int, .comptime_int, .float, .comptime_float, .bool, .null => {
                try self.prefix();
                try std.json.Stringify.value(value, .{ .emit_nonportable_numbers_as_strings = self.options.emit_nonportable_numbers_as_strings }, self.writer);
                self.punctuation = .comma;
            },
            .optional => if (value) |inner| try self.write(inner) else try self.write(null),
            .@"enum" => if (std.meta.hasFn(T, "jsonStringify")) try value.jsonStringify(self) else try self.write(@tagName(value)),
            .enum_literal => try self.write(@tagName(value)),
            .error_set => try self.write(@errorName(value)),
            .pointer => |pointer| switch (pointer.size) {
                .one => switch (@typeInfo(pointer.child)) {
                    .array => try self.write(@as([]const std.meta.Elem(pointer.child), value)),
                    else => try self.write(value.*),
                },
                .slice => {
                    if (pointer.child == u8 and try self.validUtf8(value)) {
                        try self.prefix();
                        try self.stringRaw(value);
                        self.punctuation = .comma;
                    } else {
                        try self.beginArray();
                        for (value) |item| try self.write(item);
                        try self.endArray();
                    }
                },
                else => @compileError("JSON output requires a sized pointer"),
            },
            .array => try self.write(&value),
            .vector => |vector| {
                const array: [vector.len]vector.child = value;
                try self.write(&array);
            },
            .@"struct" => |info| {
                if (std.meta.hasFn(T, "jsonStringify")) return value.jsonStringify(self);
                if (info.is_tuple) try self.beginArray() else try self.beginObject();
                inline for (info.field_names, info.field_types) |name, FT| {
                    if (FT == void) continue;
                    var emit = true;
                    if (@typeInfo(FT) == .optional) {
                        if (!self.options.emit_null_optional_fields and @field(value, name) == null) emit = false;
                    }
                    if (emit) {
                        if (!info.is_tuple) try self.objectField(name);
                        try self.write(@field(value, name));
                    }
                }
                if (info.is_tuple) try self.endArray() else try self.endObject();
            },
            .@"union" => |info| {
                if (std.meta.hasFn(T, "jsonStringify")) return value.jsonStringify(self);
                const Tag = info.tag_type orelse @compileError("JSON output requires a tagged union");
                try self.beginObject();
                inline for (info.field_names, info.field_types) |name, FT| if (value == @field(Tag, name)) {
                    try self.objectField(name);
                    if (FT == void) {
                        try self.beginObject();
                        try self.endObject();
                    } else try self.write(@field(value, name));
                };
                try self.endObject();
            },
            else => @compileError("Unsupported JSON output type: " ++ @typeName(T)),
        }
    }
    const Frame = struct { values: []const std.json.Value, keys: ?[]const []const u8 = null, index: usize = 0 };
    fn push(self: *Encoder, frames: *std.ArrayList(Frame), frame: Frame) E!void {
        if (frames.items.len == frames.capacity) {
            const replacement = self.sink.bytes.allocator.alloc(Frame, @max(@as(usize, 16), frames.capacity * 2)) catch |err| return self.fail(err);
            errdefer self.sink.bytes.allocator.free(replacement);
            var offset: usize = 0;
            while (offset < frames.items.len) {
                self.control.check(self.control.ptr) catch |err| return self.fail(err);
                const end = offset + @min(@as(usize, 64), frames.items.len - offset);
                @memcpy(replacement[offset..end], frames.items[offset..end]);
                offset = end;
            }
            self.sink.bytes.allocator.free(frames.allocatedSlice());
            frames.* = .{ .items = replacement[0..frames.items.len], .capacity = replacement.len, .pointer_stability = .{} };
        }
        frames.appendAssumeCapacity(frame);
    }
    fn writeValue(self: *Encoder, first: std.json.Value) E!void {
        var frames: std.ArrayList(Frame) = .empty;
        defer frames.deinit(self.sink.bytes.allocator);
        var pending: ?std.json.Value = first;
        while (pending != null or frames.items.len != 0) {
            try self.poll();
            if (pending) |value| {
                pending = null;
                switch (value) {
                    .array => |array| {
                        try self.beginArray();
                        try self.push(&frames, .{ .values = array.items });
                    },
                    .object => |object| {
                        try self.beginObject();
                        try self.push(&frames, .{ .values = object.values(), .keys = object.keys() });
                    },
                    .number_string => |number| {
                        try self.beginWriteRaw();
                        try self.writer.writeAll(number);
                        self.endWriteRaw();
                    },
                    .string => |text| try self.write(text),
                    .integer => |integer| try self.write(integer),
                    .float => |float| try self.write(float),
                    .bool => |boolean| try self.write(boolean),
                    .null => try self.write(null),
                }
                continue;
            }
            const frame = &frames.items[frames.items.len - 1];
            if (frame.index == frame.values.len) {
                if (frame.keys != null) try self.endObject() else try self.endArray();
                _ = frames.pop();
                continue;
            }
            if (frame.keys) |keys| try self.objectField(keys[frame.index]);
            pending = frame.values[frame.index];
            frame.index += 1;
        }
    }
    /// Preflight uses the same traversal/escaping and original authority, but
    /// retains no output bytes and never changes the destination punctuation.
    pub fn countValue(self: *Encoder, value: anytype) !usize {
        if (self.failure != null or self.sink.failure != null) return self.original(error.WriteFailed);
        var counter: Encoder = undefined;
        counter.init(self.sink.bytes.allocator, self.control, self.options);
        counter.sink.count_only = true;
        defer counter.deinit();
        counter.write(value) catch |err| return counter.original(err);
        if (counter.failure != null or counter.sink.failure != null) return counter.original(error.WriteFailed);
        try self.control.check(self.control.ptr);
        return counter.sink.count;
    }

    pub fn toOwnedSlice(self: *Encoder) ![]u8 {
        if (self.failure != null or self.sink.failure != null) return self.original(error.WriteFailed);
        return self.sink.bytes.takeOwned();
    }
};

/// Copy into the transport's pinned allocation scope on the original worker.
/// Refuse publication and free partial storage after any checkpoint failure.
pub fn copyAlloc(allocator: A, source: []const u8, control: Control) ![]u8 {
    try control.check(control.ptr);
    const result = try allocator.alloc(u8, source.len);
    errdefer allocator.free(result);
    var offset: usize = 0;
    while (offset < source.len) {
        try control.check(control.ptr);
        const end = offset + @min(@as(usize, 4096), source.len - offset);
        @memcpy(result[offset..end], source[offset..end]);
        offset = end;
    }
    try control.check(control.ptr);
    return result;
}

pub fn valueAlloc(allocator: A, value: anytype, options: Options, control: Control) ![]u8 {
    var encoder: Encoder = undefined;
    encoder.init(allocator, control, options);
    defer encoder.deinit();
    encoder.write(value) catch |err| return encoder.original(err);
    return encoder.toOwnedSlice();
}

const TestControl = struct {
    checks: usize = 0,
    fail_at: usize = 0,
    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.checks += 1;
        if (self.fail_at != 0 and self.checks == self.fail_at) return error.Canceled;
    }
    fn control(self: *@This()) Control {
        return .{ .ptr = self, .check = check };
    }
};

test "SQL JSON output matches compact standard encoding for values keys invalid UTF8 and custom wire types" {
    const alloc = std.testing.allocator;
    const Custom = struct {
        pub fn jsonStringify(_: @This(), writer: anytype) E!void {
            try writer.beginObject();
            try writer.objectField("value");
            try writer.write("λ\"\\\n");
            try writer.endObject();
        }
    };
    const Union = union(enum) { empty, text: []const u8 };
    const input = .{ .custom = Custom{}, .number = @as(i64, 9007199254740993), .invalid = @as([]const u8, &.{ 0xff, 0x80 }), .absent = @as(?u8, null), .tagged = Union{ .text = "text" }, .tuple = .{ true, @as(u8, 3) } };
    for ([_]bool{ false, true }) |portable| {
        var control: TestControl = .{};
        const expected = try std.json.Stringify.valueAlloc(alloc, input, .{ .emit_null_optional_fields = false, .emit_nonportable_numbers_as_strings = portable });
        defer alloc.free(expected);
        const actual = try valueAlloc(alloc, input, .{ .emit_null_optional_fields = false, .emit_nonportable_numbers_as_strings = portable }, control.control());
        defer alloc.free(actual);
        try std.testing.expectEqualStrings(expected, actual);
    }
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"key\\u03bb\":[null,true,9007199254740993,1.25,\"\\u0000\"]}", .{ .parse_numbers = false });
    defer parsed.deinit();
    var control: TestControl = .{};
    const expected = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    defer alloc.free(expected);
    const actual = try valueAlloc(alloc, parsed.value, .{}, control.control());
    defer alloc.free(actual);
    try std.testing.expectEqualStrings(expected, actual);
}

test "SQL JSON output interruption at every ownership boundary frees partial output" {
    const alloc = std.testing.allocator;
    const text = try alloc.alloc(u8, 16384);
    defer alloc.free(text);
    @memset(text, 'x');
    text[4095] = 0xce;
    text[4096] = 0xbb;
    text[8192] = '\n';
    text[8193] = '"';
    var reference: TestControl = .{};
    const complete = try valueAlloc(alloc, .{ .value = text, .rows = [_]u64{ 1, 2, 3 } }, .{}, reference.control());
    defer alloc.free(complete);
    for (1..reference.checks + 1) |at| {
        var control: TestControl = .{ .fail_at = at };
        try std.testing.expectError(error.Canceled, valueAlloc(alloc, .{ .value = text, .rows = [_]u64{ 1, 2, 3 } }, .{}, control.control()));
    }
    var control: TestControl = .{};
    const expected = try std.json.Stringify.valueAlloc(alloc, .{ .value = text, .rows = [_]u64{ 1, 2, 3 } }, .{});
    defer alloc.free(expected);
    const actual = try valueAlloc(alloc, .{ .value = text, .rows = [_]u64{ 1, 2, 3 } }, .{}, control.control());
    defer alloc.free(actual);
    try std.testing.expectEqualStrings(expected, actual);
}

test "SQL JSON output uses allocator-owned iterative frames for deeply nested values" {
    const alloc = std.testing.allocator;
    const values = try alloc.alloc(std.json.Value, 4097);
    defer alloc.free(values);
    values[4096] = .null;
    for (values[0..4096], 0..) |*value, index| value.* = .{ .array = std.json.Array.fromOwnedSlice(alloc, values[index + 1 .. index + 2]) };
    var control: TestControl = .{};
    const encoded = try valueAlloc(alloc, values[0], .{}, control.control());
    defer alloc.free(encoded);
    try std.testing.expectEqual(@as(usize, 8196), encoded.len);
    try std.testing.expectEqualStrings("null", encoded[4096..4100]);
    try std.testing.expect(control.checks > 64);
}

test "SQL JSON output allocation failures preserve allocator identity and retire frame and buffer storage" {
    const Fixture = struct {
        fn run(alloc: A) !void {
            const text: [8192]u8 = @splat('x');
            var values = [_]std.json.Value{.{ .string = &text }};
            const array: std.json.Value = .{ .array = std.json.Array.fromOwnedSlice(alloc, &values) };
            var control: TestControl = .{};
            const output = try valueAlloc(alloc, array, .{}, control.control());
            defer alloc.free(output);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL JSON output refuses publication when a custom serializer swallows a failed checkpoint" {
    const Custom = struct {
        pub fn jsonStringify(_: @This(), writer: anytype) E!void {
            writer.beginObject() catch {};
            writer.objectField("value") catch {};
            writer.write("text") catch {};
            writer.endObject() catch {};
        }
    };
    var control: TestControl = .{ .fail_at = 3 };
    try std.testing.expectError(error.Canceled, valueAlloc(std.testing.allocator, Custom{}, .{}, control.control()));
}

test "SQL JSON output controlled count retains no payload and leaves destination state unchanged" {
    var control: TestControl = .{};
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var encoder: Encoder = undefined;
    encoder.init(failing.allocator(), control.control(), .{});
    defer encoder.deinit();
    const text: [32768]u8 = @splat('x');
    try std.testing.expectEqual(@as(usize, text.len + 2), try encoder.countValue(&text));
    try std.testing.expectEqual(@as(usize, 0), encoder.sink.bytes.len);
    try std.testing.expectEqual(@as(usize, 0), encoder.sink.bytes.storage.len);
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
    try std.testing.expectEqual(.beginning, encoder.punctuation);
    try std.testing.expect(control.checks > 8);
}

test "transport copy checks every chunk and final publication and frees refused output" {
    const Probe = struct {
        calls: usize = 0,
        refuse: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls == self.refuse) return error.Canceled;
        }
        fn control(self: *@This()) Control {
            return .{ .ptr = self, .check = check };
        }
    };
    const a = std.testing.allocator;
    const source = try a.alloc(u8, 4096 * 3 + 1);
    defer a.free(source);
    for (source, 0..) |*byte, i| byte.* = @truncate(i);
    var probe: Probe = .{};
    const copy = try copyAlloc(a, source, probe.control());
    defer a.free(copy);
    try std.testing.expectEqualSlices(u8, source, copy);
    try std.testing.expect(copy.ptr != source.ptr);
    try std.testing.expectEqual(@as(usize, 6), probe.calls);
    for (1..probe.calls + 1) |failure| {
        var interrupted: Probe = .{ .refuse = failure };
        try std.testing.expectError(error.Canceled, copyAlloc(a, source, interrupted.control()));
    }
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, copyAlloc(failing.allocator(), source, probe.control()));
}
