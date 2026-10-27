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

//! Disposable bounded object-path authority, bound to immutable primary bytes.
//! Old/missing authorities fall back to a full read. No primary format changes.
const std = @import("std");
const A = std.mem.Allocator;
pub const minimum_bytes = 16 * 1024;
pub const maximum_bytes = 32 * 1024;
const header_size = 47;
const prefix = "\x00\x00__metadata__:document_projection_v1:";
pub const Key = [prefix.len + 32]u8;
pub fn keyForRevision(key: anytype) Key {
    var result: Key = undefined;
    @memcpy(result[0..prefix.len], prefix);
    @memcpy(result[prefix.len..], key[key.len - 32 ..]);
    return result;
}
pub const FieldValue = struct { name: []const u8, value: []const u8 };
pub const Source = union(enum) { bytes: []const u8, fields: []const FieldValue };
pub const no_parent: u16 = 511;
pub const maximum_fields = 512;
pub const maximum_object_depth = 128;
const offset_mask: u64 = (@as(u64, 1) << 53) - 1;
pub const Kind = enum(u2) { subtree = 0, leaf = 1, object = 2 };
pub const Field = struct {
    name: []const u8,
    offset: u64,
    length: u64,
    digest: [32]u8,
    index: u16 = 0,
    parent: u16 = no_parent,
    kind: Kind = .subtree,
    first_child: u16 = 0,
    child_count: u16 = 0,
};
fn less(_: void, left: Field, right: Field) bool {
    return std.mem.order(u8, left.name, right.name) == .lt;
}

/// Optional leaf consumer shares the successfully bounded directory walk. It
/// observes final packed indices only, never nodes discarded by width fallback.
pub const LeafObserver = struct {
    ctx: *anyopaque,
    visit: *const fn (*anyopaque, u16, []const u8) anyerror!void,
};
pub fn build(a: A, bytes: []const u8, kind: u8, digest: [32]u8) !?[]u8 {
    return buildObserved(a, bytes, kind, digest, null);
}
fn buildObserved(a: A, bytes: []const u8, kind: u8, digest: [32]u8, observer: ?LeafObserver) !?[]u8 {
    if (bytes.len < minimum_bytes or bytes.len > offset_mask) return null;
    var arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(a));
    defer arena.deinit();
    const temp = arena.allocator();
    const Sink = struct {
        a: A,
        bytes: []const u8,
        fields: std.ArrayList(Field) = .empty,
        size: usize = header_size,
        limit: usize,
        fn member(raw: *anyopaque, name: []const u8, value: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fields.items.len == 512 or name.len > std.math.maxInt(u16) or name.len + 50 > self.limit -| self.size) return error.ProjectionDirectoryTooLarge;
            var hash: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(value, &hash, .{});
            try self.fields.append(self.a, .{ .name = try self.a.dupe(u8, name), .offset = @intFromPtr(value.ptr) - @intFromPtr(self.bytes.ptr), .length = value.len, .digest = hash });
            self.size += 50 + name.len;
        }
    };
    var sink: Sink = .{ .a = temp, .bytes = bytes, .limit = @min(maximum_bytes, bytes.len / 16) };
    var scanner = std.json.Scanner.initCompleteInput(temp, bytes);
    defer scanner.deinit();
    if ((scanner.peekNextTokenType() catch |err| {
        if (err == error.OutOfMemory) return err;
        return null;
    }) != .object_begin) return null;
    @import("antfly-json").scanValue(arena.child_allocator, &scanner, bytes.len, .{ .ctx = &sink, .member_fn = Sink.member }) catch |err| {
        if (err == error.OutOfMemory) return err;
        return null;
    };
    if ((scanner.next() catch |err| {
        if (err == error.OutOfMemory) return err;
        return null;
    }) != .end_of_document) return null;
    std.mem.sort(Field, sink.fields.items, {}, less);
    // Capture only object paths, never individual array elements. The full
    // source has already passed strict validation above. A fixed linear work
    // budget bounds object revisits and hashing independently of depth. Small
    // deep branches remain useful while large/deep objects become opaque.
    var tree = Tree{ .a = temp, .bytes = bytes, .limit = sink.limit, .size = sink.size, .remaining_work = @as(u64, @intCast(bytes.len)) * 3, .validator = .init(arena.child_allocator) };
    defer tree.validator.deinit();
    for (sink.fields.items) |field| try tree.nodes.append(temp, .{ .field = field });
    const roots = tree.nodes.items.len;
    for (0..roots) |i| try tree.capture(i, 1);
    // The v2 packed descriptor retains the 50-byte per-field overhead. Budget
    // fallbacks attest opaque subtrees and never manufacture missing members.
    return try encodeTree(a, bytes, kind, digest, &tree, observer);
}

const Tree = struct {
    const Node = struct { field: Field, first: ?usize = null, last: ?usize = null, next: ?usize = null };
    a: A,
    bytes: []const u8,
    limit: usize,
    size: usize,
    remaining_work: u64,
    nodes: std.ArrayList(Node) = .empty,
    validator: @import("antfly-json").ValueScanner,
    fn capture(self: *@This(), index: usize, depth: usize) anyerror!void {
        const field = self.nodes.items[index].field;
        const raw = self.bytes[@intCast(field.offset)..][0..@intCast(field.length)];
        const trimmed = std.mem.trimStart(u8, raw, " \t\r\n");
        if (trimmed.len == 0) return error.SyntaxError;
        if (trimmed[0] == '[') {
            self.nodes.items[index].field.kind = .subtree;
            return;
        }
        if (trimmed[0] != '{') {
            self.nodes.items[index].field.kind = .leaf;
            return;
        }
        if (depth >= maximum_object_depth or field.length > self.remaining_work) {
            self.nodes.items[index].field.kind = .subtree;
            return;
        }
        // Reserve complete coverage before descending. Reservations are not
        // refunded after a width fallback: even discarded work stays charged.
        self.remaining_work -= field.length;
        const count_before = self.nodes.items.len;
        const size_before = self.size;
        self.nodes.items[index].field.kind = .object;
        self.captureObject(index, depth, raw) catch |err| switch (err) {
            error.ProjectionDirectoryTooLarge => {
                self.nodes.items.len = count_before;
                self.size = size_before;
                self.nodes.items[index].first = null;
                self.nodes.items[index].last = null;
                self.nodes.items[index].field.kind = .subtree;
            },
            else => return err,
        };
    }
    fn captureObject(self: *@This(), index: usize, depth: usize, raw: []const u8) anyerror!void {
        const field = self.nodes.items[index].field;
        var scanner = std.json.Scanner.initCompleteInput(self.a, raw);
        defer scanner.deinit();
        if (try scanner.next() != .object_begin) return error.SyntaxError;
        while (try scanner.peekNextTokenType() != .object_end) {
            const token = try scanner.nextAllocMax(self.a, .alloc_if_needed, raw.len);
            const name = switch (token) {
                .string, .allocated_string => |value| value,
                else => return error.SyntaxError,
            };
            var begin = scanner.cursor;
            while (begin < raw.len and (std.ascii.isWhitespace(raw[begin]) or raw[begin] == ':')) : (begin += 1) {}
            try self.validator.scan(&scanner, raw.len, null);
            const value = raw[begin..scanner.cursor];
            if (self.nodes.items.len == maximum_fields or name.len > std.math.maxInt(u16) or name.len + 50 > self.limit -| self.size) return error.ProjectionDirectoryTooLarge;
            var hash: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(value, &hash, .{});
            const child = self.nodes.items.len;
            try self.nodes.append(self.a, .{ .field = .{ .name = name, .offset = field.offset + begin, .length = value.len, .digest = hash, .parent = @intCast(index) } });
            self.size += 50 + name.len;
            if (self.nodes.items[index].last) |last| self.nodes.items[last].next = child else self.nodes.items[index].first = child;
            self.nodes.items[index].last = child;
            try self.capture(child, depth + 1);
        }
        _ = try scanner.next();
        if (try scanner.next() != .end_of_document) return error.SyntaxError;
    }
};
fn encodeTree(a: A, source: []const u8, kind: u8, digest: [32]u8, tree: *Tree, observer: ?LeafObserver) ![]u8 {
    var order: [maximum_fields]usize = undefined;
    var mapped: [maximum_fields]u16 = undefined;
    var count: usize = 0;
    for (tree.nodes.items, 0..) |node, i| if (node.field.parent == no_parent) {
        order[count] = i;
        count += 1;
    };
    var cursor: usize = 0;
    while (cursor < count) : (cursor += 1) {
        const index = order[cursor];
        mapped[index] = @intCast(cursor);
        const start = count;
        var child = tree.nodes.items[index].first;
        while (child) |i| {
            order[count] = i;
            count += 1;
            child = tree.nodes.items[i].next;
        }
        std.mem.sort(usize, order[start..count], tree.nodes.items, struct {
            fn less(nodes: []Tree.Node, l: usize, r: usize) bool {
                return std.mem.order(u8, nodes[l].field.name, nodes[r].field.name) == .lt;
            }
        }.less);
    }
    const output = try a.alloc(u8, tree.size);
    errdefer a.free(output);
    @memcpy(output[0..4], "DPD2");
    std.mem.writeInt(u64, output[4..12], source.len, .little);
    @memcpy(output[12..44], &digest);
    output[44] = kind;
    std.mem.writeInt(u16, output[45..47], @intCast(count), .little);
    var pos: usize = header_size;
    for (order[0..count]) |i| {
        const field = tree.nodes.items[i].field;
        if (observer) |sink| if (field.kind == .leaf)
            try sink.visit(sink.ctx, mapped[i], source[@intCast(field.offset)..][0..@intCast(field.length)]);
        const parent = if (field.parent == no_parent) no_parent else mapped[field.parent];
        const descriptor = field.offset | (@as(u64, parent) << 53) | (@as(u64, @backingInt(field.kind)) << 62);
        std.mem.writeInt(u16, output[pos..][0..2], @intCast(field.name.len), .little);
        pos += 2;
        @memcpy(output[pos..][0..field.name.len], field.name);
        pos += field.name.len;
        std.mem.writeInt(u64, output[pos..][0..8], descriptor, .little);
        pos += 8;
        std.mem.writeInt(u64, output[pos..][0..8], field.length, .little);
        pos += 8;
        @memcpy(output[pos..][0..32], &field.digest);
        pos += 32;
    }
    return output;
}

/// Remember unsupported large values too, so bounded migration/restarts do
/// not repeatedly read binary values or objects exceeding directory limits.
pub fn buildAuthority(a: A, bytes: []const u8, kind: u8, digest: [32]u8) !?[]u8 {
    return buildAuthorityObserved(a, bytes, kind, digest, null);
}
pub fn buildAuthorityObserved(a: A, bytes: []const u8, kind: u8, digest: [32]u8, observer: ?LeafObserver) !?[]u8 {
    if (kind != @import("internal_keys.zig").primary_kind or bytes.len < minimum_bytes) return null;
    if (try buildObserved(a, bytes, kind, digest, observer)) |encoded| return encoded;
    const output = try a.alloc(u8, header_size);
    @memcpy(output[0..4], "DPX1");
    std.mem.writeInt(u64, output[4..12], bytes.len, .little);
    @memcpy(output[12..44], &digest);
    output[44] = kind;
    std.mem.writeInt(u16, output[45..47], 0, .little);
    return output;
}
pub fn isCurrent(a: A, bytes: []const u8, proof: anytype) !bool {
    if (bytes.len == header_size and std.mem.eql(u8, bytes[0..4], "DPX1") and std.mem.readInt(u64, bytes[4..12], .little) == proof.bytes and bytes[44] == proof.kind and std.mem.eql(u8, bytes[12..44], &proof.digest) and std.mem.readInt(u16, bytes[45..47], .little) == 0) return true;
    const directory = (try Directory.open(a, bytes, proof)) orelse return false;
    a.free(directory.fields);
    return directory.version == 2;
}

pub const Directory = struct {
    fields: []const Field,
    root_count: usize,
    version: u8,
    pub fn open(a: A, bytes: []const u8, proof: anytype) !?Directory {
        if (bytes.len < header_size or bytes.len > maximum_bytes or bytes.len > proof.bytes / 16) return null;
        const version: u8 = if (std.mem.eql(u8, bytes[0..4], "DPD2")) 2 else if (std.mem.eql(u8, bytes[0..4], "DPD1")) 1 else return null;
        if (version == 2 and proof.bytes > offset_mask) return null;
        if (std.mem.readInt(u64, bytes[4..12], .little) != proof.bytes or bytes[44] != proof.kind or !std.mem.eql(u8, bytes[12..44], &proof.digest)) return null;
        const count = std.mem.readInt(u16, bytes[45..47], .little);
        if (count > maximum_fields) return null;
        var pos: usize = header_size;
        for (0..count) |_| {
            if (bytes.len -| pos < 2) return null;
            const len = std.mem.readInt(u16, bytes[pos..][0..2], .little);
            pos += 2;
            if (bytes.len -| pos < @as(usize, len) + 48) return null;
            const descriptor = std.mem.readInt(u64, bytes[pos + len ..][0..8], .little);
            const offset = if (version == 2) descriptor & offset_mask else descriptor;
            const size = std.mem.readInt(u64, bytes[pos + len + 8 ..][0..8], .little);
            if (size == 0 or offset > proof.bytes or size > proof.bytes - offset) return null;
            if (version == 2 and descriptor >> 62 == 3) return null;
            pos += @as(usize, len) + 48;
        }
        if (pos != bytes.len) return null;
        const fields = try a.alloc(Field, count);
        var success = false;
        defer if (!success) a.free(fields);
        pos = header_size;
        var roots: usize = 0;
        var depths: [maximum_fields]u8 = undefined;
        var previous_parent: ?u16 = null;
        for (fields, 0..) |*field, i| {
            const len = std.mem.readInt(u16, bytes[pos..][0..2], .little);
            pos += 2;
            const descriptor = std.mem.readInt(u64, bytes[pos + len ..][0..8], .little);
            field.* = .{ .name = bytes[pos..][0..len], .offset = if (version == 2) descriptor & offset_mask else descriptor, .length = std.mem.readInt(u64, bytes[pos + len + 8 ..][0..8], .little), .digest = bytes[pos + len + 16 ..][0..32].*, .index = @intCast(i), .parent = if (version == 2) @intCast((descriptor >> 53) & 511) else no_parent, .kind = if (version == 2) @fromBackingInt(@intCast(@as(u2, @intCast(descriptor >> 62)))) else .subtree };
            if (field.parent == no_parent) {
                if (previous_parent != null or (i != 0 and !less({}, fields[i - 1], field.*))) return null;
                roots += 1;
                depths[i] = 1;
            } else {
                if (field.parent >= i or fields[field.parent].kind != .object) return null;
                depths[i] = depths[field.parent] + 1;
                if (depths[i] > maximum_object_depth) return null;
                if (previous_parent) |parent| if (field.parent < parent or (field.parent == parent and !less({}, fields[i - 1], field.*))) return null;
                previous_parent = field.parent;
                const parent = &fields[field.parent];
                if (field.offset <= parent.offset) return null;
                const delta = field.offset - parent.offset;
                if (delta >= parent.length or field.length >= parent.length - delta) return null;
                if (parent.child_count == 0) parent.first_child = @intCast(i) else if (i != @as(usize, parent.first_child) + parent.child_count) return null;
                parent.child_count += 1;
            }
            pos += @as(usize, len) + 48;
        }
        success = true;
        return .{ .fields = fields, .root_count = roots, .version = version };
    }
    fn within(self: Directory, first: usize, count: usize, name: []const u8) ?Field {
        var low = first;
        var high = first + count;
        while (low < high) {
            const mid = low + (high - low) / 2;
            switch (std.mem.order(u8, name, self.fields[mid].name)) {
                .eq => return self.fields[mid],
                .lt => high = mid,
                .gt => low = mid + 1,
            }
        }
        return null;
    }
    /// Root lookup stays compatible with SQL projection, including literal dots.
    pub fn find(self: Directory, name: []const u8) ?Field {
        return self.within(0, self.root_count, name);
    }
    pub const Selection = struct { field: Field, covered: usize };
    pub const Location = struct { value: ?Selection = null, objects: usize = 0 };
    /// Structural paths preserve literal dots and empty object keys. Opaque
    /// ancestors retain their subtree. Proven absence also retains complete
    /// object ancestry, needed by ordinary projection's empty-object behavior.
    pub fn locate(self: Directory, parts: []const []const u8) Location {
        if (parts.len == 0) return .{};
        var field = self.find(parts[0]) orelse return .{};
        var covered: usize = 1;
        var objects: usize = 0;
        while (covered < parts.len) {
            switch (field.kind) {
                .subtree => return .{ .value = .{ .field = field, .covered = covered }, .objects = objects },
                .leaf => return .{ .objects = objects },
                .object => {
                    objects = covered;
                    field = self.within(field.first_child, field.child_count, parts[covered]) orelse return .{ .objects = objects };
                },
            }
            covered += 1;
        }
        return .{ .value = .{ .field = field, .covered = covered }, .objects = objects };
    }
    pub fn select(self: Directory, parts: []const []const u8) ?Selection {
        return self.locate(parts).value;
    }
    /// Logical dot paths descend through proven objects. Arrays, legacy roots,
    /// and bounded opaque objects retain their entire subtree and boundaries.
    pub fn resolve(self: Directory, path: []const u8) ?Field {
        var remaining = path;
        var parent: ?Field = null;
        while (true) {
            const dot = std.mem.indexOfScalar(u8, remaining, '.');
            const name = if (dot) |i| remaining[0..i] else remaining;
            const field = (if (parent) |p| self.within(p.first_child, p.child_count, name) else self.find(name)) orelse return null;
            if (dot == null) return field;
            remaining = remaining[dot.? + 1 ..];
            if (remaining.len == 0 or field.kind == .subtree) return field;
            if (field.kind == .leaf) return null;
            parent = field;
        }
    }
};

test "logical value nested authority bounds depth width corruption and allocation" {
    const Fixture = struct {
        fn run(a: A) !void {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const temp = arena.allocator();
            const ignored = try temp.alloc(u8, minimum_bytes);
            @memset(ignored, 'x');
            const body = try std.fmt.allocPrint(temp, "{{\"ignored\":\"{s}\",\"\\u0062ody\":{{\"z\":[1,null]}},\"one\":{{\"two\":{{\"three\":{{\"four\":{{\"leaf\":\"needle\"}}}}}}}},\"body.z\":\"literal\"}}", .{ignored});
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(body, &digest, .{});
            const proof = .{ .bytes = @as(u64, body.len), .kind = @as(u8, 16), .digest = digest };
            const encoded = (try build(temp, body, 16, digest)).?;
            const directory = (try Directory.open(temp, encoded, proof)).?;
            try std.testing.expectEqual(@as(u8, 2), directory.version);
            try std.testing.expect(try isCurrent(temp, encoded, proof));
            try std.testing.expectEqualStrings("body.z", directory.find("body.z").?.name);
            const array = directory.resolve("body.z.member").?;
            try std.testing.expectEqualStrings("[1,null]", body[@intCast(array.offset)..][0..@intCast(array.length)]);
            try std.testing.expect(directory.resolve("body.missing") == null);
            const deep_field = directory.resolve("one.two.three.four.leaf").?;
            try std.testing.expectEqualStrings("leaf", deep_field.name);
            try std.testing.expectEqual(Kind.leaf, deep_field.kind);
            // A dominant nested payload consumes the finite coverage budget;
            // the unresolved subtree remains readable rather than truncated.
            const dominant = try std.fmt.allocPrint(temp, "{{\"one\":{{\"two\":{{\"three\":{{\"four\":{{\"leaf\":\"{s}\"}}}}}}}}}}", .{ignored});
            var dominant_hash: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(dominant, &dominant_hash, .{});
            const dominant_encoded = (try build(temp, dominant, 16, dominant_hash)).?;
            const bounded_depth = (try Directory.open(temp, dominant_encoded, .{ .bytes = @as(u64, dominant.len), .kind = @as(u8, 16), .digest = dominant_hash })).?;
            const opaque_field = bounded_depth.resolve("one.two.three.four.leaf").?;
            try std.testing.expectEqualStrings("four", opaque_field.name);
            try std.testing.expectEqual(Kind.subtree, opaque_field.kind);
            // Every truncation is either rejected before allocation or leaves
            // a properly retired directory. No partial hierarchy can escape.
            for (0..encoded.len) |end| try std.testing.expect(try Directory.open(temp, encoded[0..end], proof) == null);
            const corrupted = try temp.dupe(u8, encoded);
            var pos: usize = header_size;
            for (directory.fields) |field| {
                const name_len = std.mem.readInt(u16, encoded[pos..][0..2], .little);
                pos += 2 + @as(usize, name_len);
                const descriptor = std.mem.readInt(u64, encoded[pos..][0..8], .little);
                std.mem.writeInt(u64, corrupted[pos..][0..8], descriptor | (@as(u64, 3) << 62), .little);
                try std.testing.expect(try Directory.open(temp, corrupted, proof) == null);
                @memcpy(corrupted, encoded);
                if (field.parent != no_parent) {
                    const self_parent = (descriptor & ~(@as(u64, 511) << 53)) | (@as(u64, field.index) << 53);
                    std.mem.writeInt(u64, corrupted[pos..][0..8], self_parent, .little);
                    try std.testing.expect(try Directory.open(temp, corrupted, proof) == null);
                    @memcpy(corrupted, encoded);
                }
                std.mem.writeInt(u64, corrupted[pos + 8 ..][0..8], 0, .little);
                try std.testing.expect(try Directory.open(temp, corrupted, proof) == null);
                @memcpy(corrupted, encoded);
                pos += 48;
            }
            var wide: std.Io.Writer.Allocating = .init(temp);
            try wide.writer.print("{{\"ignored\":\"{s}\",\"wide\":{{", .{ignored});
            for (0..600) |i| {
                if (i != 0) try wide.writer.writeByte(',');
                try wide.writer.print("\"k{d}\":0", .{i});
            }
            try wide.writer.writeAll("}}");
            std.crypto.hash.sha2.Sha256.hash(wide.written(), &digest, .{});
            const bounded = (try build(temp, wide.written(), 16, digest)).?;
            const coarse = (try Directory.open(temp, bounded, .{ .bytes = @as(u64, wide.written().len), .kind = @as(u8, 16), .digest = digest })).?;
            try std.testing.expectEqual(@as(usize, 2), coarse.fields.len);
            try std.testing.expectEqual(Kind.subtree, coarse.resolve("wide.k0").?.kind);
            try std.testing.expect(try isCurrent(temp, bounded, .{ .bytes = @as(u64, wide.written().len), .kind = @as(u8, 16), .digest = digest }));
        }
    };
    try Fixture.run(std.testing.allocator);
    var failing: std.testing.FailingAllocator = .init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try @import("antfly_platform").allocator.checkAllAllocationFailures(failing.allocator(), Fixture.run, .{});
}

/// Encode a bounded root directory from already validated field values. Used
/// by immutable-payload patches, which cannot rescan absent large JSON bytes.
pub fn encodeRoots(a: A, bytes: usize, kind: u8, digest: [32]u8, fields: []const Field) ![]u8 {
    for (fields) |field| if (field.parent != no_parent or field.kind == .object) return error.InvalidArgument;
    return encodeFields(a, bytes, kind, digest, fields);
}
/// Packed breadth-first fields, including complete retained object branches.
/// All offsets and parent/child relationships are checked before publication.
pub fn encodeFields(a: A, bytes: usize, kind: u8, digest: [32]u8, fields: []const Field) ![]u8 {
    if (fields.len > maximum_fields) return error.ProjectionDirectoryTooLarge;
    var size: usize = header_size;
    for (fields) |field| {
        if (field.name.len > std.math.maxInt(u16)) return error.InvalidArgument;
        size = try std.math.add(usize, size, 50 + field.name.len);
    }
    if (size > maximum_bytes) return error.ProjectionDirectoryTooLarge;
    const out = try a.alloc(u8, size);
    errdefer a.free(out);
    @memcpy(out[0..4], "DPD2");
    std.mem.writeInt(u64, out[4..12], bytes, .little);
    @memcpy(out[12..44], &digest);
    out[44] = kind;
    std.mem.writeInt(u16, out[45..47], @intCast(fields.len), .little);
    var pos: usize = header_size;
    for (fields) |field| {
        std.mem.writeInt(u16, out[pos..][0..2], @intCast(field.name.len), .little);
        pos += 2;
        @memcpy(out[pos..][0..field.name.len], field.name);
        pos += field.name.len;
        if (field.offset > offset_mask) return error.InvalidArgument;
        const descriptor = field.offset | (@as(u64, field.parent) << 53) | (@as(u64, @backingInt(field.kind)) << 62);
        std.mem.writeInt(u64, out[pos..][0..8], descriptor, .little);
        std.mem.writeInt(u64, out[pos + 8 ..][0..8], field.length, .little);
        @memcpy(out[pos + 16 ..][0..32], &field.digest);
        pos += 48;
    }
    const checked = (try Directory.open(a, out, .{ .bytes = @as(u64, bytes), .kind = kind, .digest = digest })) orelse return error.InvalidArgument;
    a.free(checked.fields);
    return out;
}
