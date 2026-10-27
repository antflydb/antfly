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

//! Versioned logical JSON commitments, independent of bytes, layout, projection
//! coverage and caches. Objects sort decoded keys; arrays preserve order. Large
//! plain strings hash borrowed canonical bytes without building a JSON DOM.
const std = @import("std");
const json = @import("antfly-json");
const scalar = @import("source_value_identity.zig");
const projection = @import("document_projection.zig");
const A = std.mem.Allocator;
pub const Digest = [32]u8;
pub const algorithm = "logical-json-v1";
pub const Algorithm = enum { sha256_json, logical_json_v1 };
const domain = "antfly.document.logical-json/1\x00";
pub const Member = struct { name: []const u8, digest: Digest };
pub const maximum_depth = json.max_raw_nesting;
/// Immutable navigation and canonical identities share one bounded field tree.
/// Its byte ranges follow this physical layout; logical digests do not.
pub const FieldTree = struct {
    raw: []u8,
    directory: projection.Directory,
    canonical: []Digest,
    pub fn deinit(self: FieldTree, a: A) void {
        a.free(self.raw);
        a.free(self.directory.fields);
        a.free(self.canonical);
    }
};
pub const Root = struct {
    digest: Digest,
    members: []Member,
    tree: ?FieldTree = null,
    pub fn deinit(self: Root, a: A) void {
        for (self.members) |member| a.free(member.name);
        a.free(self.members);
        if (self.tree) |tree| tree.deinit(a);
    }
};
fn hashStart(kind: []const u8) std.crypto.hash.sha2.Sha256 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(domain);
    hash.update(kind);
    hash.update(&.{0});
    return hash;
}
fn hashLength(hash: *std.crypto.hash.sha2.Sha256, length: usize) void {
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, length, .little);
    hash.update(&raw);
}
fn less(_: void, l: Member, r: Member) bool {
    return std.mem.order(u8, l.name, r.name) == .lt;
}
pub fn objectDigest(members: []const Member) !Digest {
    var hash = hashStart("object");
    hashLength(&hash, members.len);
    for (members, 0..) |member, index| {
        if (!std.unicode.utf8ValidateSlice(member.name) or (index != 0 and !less({}, members[index - 1], member))) return error.InvalidDocumentCommitment;
        hashLength(&hash, member.name.len);
        hash.update(member.name);
        hash.update(&member.digest);
    }
    return hash.finalResult();
}

const Parser = struct {
    a: A,
    scanner: std.json.Scanner,
    keys: json.ObjectKeyWorkspace(bool),
    root_members: std.ArrayList(Member) = .empty,
    fn value(self: *@This(), depth: usize) anyerror!Digest {
        if (depth > maximum_depth) return error.DocumentNestingTooDeep;
        const kind = try self.scanner.peekNextTokenType();
        switch (kind) {
            .object_begin => {
                _ = try self.scanner.next();
                try self.keys.enter(depth, true);
                var members: std.ArrayList(Member) = .empty;
                defer members.deinit(self.a);
                while (true) {
                    const token = try self.keys.nextKey(depth, &self.scanner, self.scanner.input.len);
                    const name = switch (token) {
                        .string, .allocated_string => |name| name,
                        .object_end => break,
                        else => return error.UnexpectedToken,
                    };
                    try self.keys.remember(depth, name, false);
                    const digest = try self.value(depth + 1);
                    try members.append(self.a, .{ .name = name, .digest = digest });
                }
                std.mem.sort(Member, members.items, {}, less);
                const digest = try objectDigest(members.items);
                if (depth == 0) for (members.items) |member| {
                    const name = try self.a.dupe(u8, member.name);
                    errdefer self.a.free(name);
                    try self.root_members.append(self.a, .{ .name = name, .digest = member.digest });
                };
                try self.keys.finishFrame(depth);
                return digest;
            },
            .array_begin => {
                _ = try self.scanner.next();
                try self.keys.enter(depth, false);
                var hash = hashStart("array");
                var count: usize = 0;
                while (try self.scanner.peekNextTokenType() != .array_end) {
                    hash.update(&try self.value(depth + 1));
                    count = try std.math.add(usize, count, 1);
                }
                _ = try self.scanner.next();
                try self.keys.finishFrame(depth);
                hashLength(&hash, count);
                return hash.finalResult();
            },
            .string, .number, .true, .false, .null => {
                var begin = self.scanner.cursor;
                while (begin < self.scanner.input.len and (std.ascii.isWhitespace(self.scanner.input[begin]) or self.scanner.input[begin] == ':' or self.scanner.input[begin] == ',')) : (begin += 1) {}
                if (kind != .string or !try json.value_strings.skip(&self.scanner)) try self.scanner.skipValue();
                return (try scalar.validatedScalarDigest(self.a, self.scanner.input[begin..self.scanner.cursor])) orelse error.InvalidDocumentCommitment;
            },
            else => return error.UnexpectedToken,
        }
    }
};
pub fn build(a: A, bytes: []const u8) !Root {
    var parser: Parser = .{ .a = a, .scanner = .initCompleteInput(a, bytes), .keys = .init(a) };
    defer parser.scanner.deinit();
    defer parser.keys.deinit();
    errdefer {
        for (parser.root_members.items) |member| a.free(member.name);
        parser.root_members.deinit(a);
    }
    const digest = try parser.value(0);
    if (try parser.scanner.next() != .end_of_document) return error.UnexpectedToken;
    return .{ .digest = digest, .members = try parser.root_members.toOwnedSlice(a) };
}
/// Reuse canonical scalar commitments produced by the writer's directory pass.
/// The directory and certificate must belong to these exact validated bytes;
/// opaque arrays/subtrees alone use the reference parser. No payload gets a
/// second canonicalization pass merely because its physical layout may change.
pub fn fromPublication(a: A, bytes: []const u8, kind: u8, authority: Digest, publication: scalar.Publication) !?Root {
    const encoded = publication.directory orelse return null;
    const certificate = publication.identities orelse return null;
    const proof = .{ .bytes = @as(u64, bytes.len), .kind = kind, .digest = authority };
    const directory = (try projection.Directory.open(a, encoded, proof)) orelse return null;
    defer a.free(directory.fields);
    const identities = scalar.Certificate.open(certificate, encoded, proof) orelse return null;
    var digests: [projection.maximum_fields]Digest = undefined;
    var cursor = directory.fields.len;
    while (cursor > 0) {
        cursor -= 1;
        const field = directory.fields[cursor];
        if (field.kind == .object) {
            const children = try a.alloc(Member, field.child_count);
            defer a.free(children);
            for (directory.fields[field.first_child..][0..field.child_count], children) |child, *member| member.* = .{ .name = child.name, .digest = digests[child.index] };
            digests[field.index] = try objectDigest(children);
        } else if (field.kind == .leaf and identities.find(field.index) != null) {
            digests[field.index] = identities.find(field.index).?;
        } else digests[field.index] = try compute(a, bytes[@intCast(field.offset)..][0..@intCast(field.length)]);
    }
    const members = try a.alloc(Member, directory.root_count);
    errdefer a.free(members);
    var initialized: usize = 0;
    errdefer for (members[0..initialized]) |member| a.free(member.name);
    for (directory.fields[0..directory.root_count], members) |field, *member| {
        member.* = .{ .name = try a.dupe(u8, field.name), .digest = digests[field.index] };
        initialized += 1;
    }
    const digest = try objectDigest(members);
    // The scalar certificate is consumed here, not duplicated into the durable
    // manifest. Canonical digests for every retained node serve both providers
    // and selective immutable-source reads.
    const raw = try a.dupe(u8, encoded);
    errdefer a.free(raw);
    @memcpy(raw[12..44], &digest);
    const owned_directory = (try projection.Directory.open(a, raw, .{ .bytes = @as(u64, bytes.len), .kind = kind, .digest = digest })) orelse return error.InvalidDocumentCommitment;
    errdefer a.free(owned_directory.fields);
    const canonical = try a.dupe(Digest, digests[0..directory.fields.len]);
    return .{ .digest = digest, .members = members, .tree = .{ .raw = raw, .directory = owned_directory, .canonical = canonical } };
}

pub fn compute(a: A, bytes: []const u8) !Digest {
    const root = try build(a, bytes);
    defer root.deinit(a);
    return root.digest;
}

/// Bounded metadata for incremental top-level patches. Its root must equal the
/// current revision-bound logical authority before any child can be reused.
pub fn encode(a: A, root: Root) ![]u8 {
    if (root.tree) |tree| return encodeTree(a, root.digest, tree.raw, tree.canonical);
    if (root.members.len > 512 or !std.mem.eql(u8, &try objectDigest(root.members), &root.digest)) return error.InvalidDocumentCommitment;
    var length: usize = 6;
    for (root.members) |member| {
        if (member.name.len > std.math.maxInt(u16)) return error.InvalidDocumentCommitment;
        length = try std.math.add(usize, length, member.name.len + 34);
    }
    if (length > 64 * 1024) return error.InvalidDocumentCommitment;
    const out = try a.alloc(u8, length);
    @memcpy(out[0..4], "DLC1");
    std.mem.writeInt(u16, out[4..6], @intCast(root.members.len), .little);
    var position: usize = 6;
    for (root.members) |member| {
        std.mem.writeInt(u16, out[position..][0..2], @intCast(member.name.len), .little);
        position += 2;
        @memcpy(out[position..][0..member.name.len], member.name);
        position += member.name.len;
        @memcpy(out[position..][0..32], &member.digest);
        position += 32;
    }
    return out;
}
pub fn decode(a: A, raw: []const u8, expected: Digest) !Root {
    if (std.mem.startsWith(u8, raw, "DLC2")) return decodeTree(a, raw, expected);
    if (raw.len < 6 or raw.len > 64 * 1024 or !std.mem.eql(u8, raw[0..4], "DLC1")) return error.InvalidDocumentCommitment;
    const count = std.mem.readInt(u16, raw[4..6], .little);
    if (count > 512) return error.InvalidDocumentCommitment;
    const members = try a.alloc(Member, count);
    defer a.free(members);
    var position: usize = 6;
    for (members) |*member| {
        if (raw.len -| position < 2) return error.InvalidDocumentCommitment;
        const length = std.mem.readInt(u16, raw[position..][0..2], .little);
        position += 2;
        if (raw.len -| position < @as(usize, length) + 32) return error.InvalidDocumentCommitment;
        member.* = .{ .name = raw[position..][0..length], .digest = raw[position + length ..][0..32].* };
        position += length + 32;
    }
    if (position != raw.len or !std.mem.eql(u8, &try objectDigest(members), &expected)) return error.InvalidDocumentCommitment;
    const owned = try a.alloc(Member, count);
    var initialized: usize = 0;
    errdefer {
        for (owned[0..initialized]) |member| a.free(member.name);
        a.free(owned);
    }
    for (members, owned) |member, *copy| {
        copy.* = .{ .name = try a.dupe(u8, member.name), .digest = member.digest };
        initialized += 1;
    }
    return .{ .digest = expected, .members = owned };
}

/// DLC2 stores the directory once plus one canonical digest per node. Root
/// members are a runtime view, not another serialized directory. DLC1 readers
/// remain available for old manifests; new writes use the bounded tree.
pub fn encodeTree(a: A, expected: Digest, directory: []const u8, canonical: []const Digest) ![]u8 {
    const size = try std.math.add(usize, 10 + directory.len, canonical.len * 32);
    if (size > 64 * 1024 or canonical.len > projection.maximum_fields or directory.len < 47) return error.InvalidDocumentCommitment;
    const checked = (try projection.Directory.open(a, directory, .{ .bytes = std.mem.readInt(u64, directory[4..12], .little), .kind = directory[44], .digest = expected })) orelse return error.InvalidDocumentCommitment;
    defer a.free(checked.fields);
    try validateTree(a, checked, canonical, expected);
    const raw = try a.alloc(u8, size);
    @memcpy(raw[0..4], "DLC2");
    std.mem.writeInt(u32, raw[4..8], @intCast(directory.len), .little);
    std.mem.writeInt(u16, raw[8..10], @intCast(canonical.len), .little);
    @memcpy(raw[10..][0..directory.len], directory);
    for (canonical, 0..) |digest, i| @memcpy(raw[10 + directory.len + i * 32 ..][0..32], &digest);
    return raw;
}
fn validateTree(a: A, directory: projection.Directory, canonical: []const Digest, expected: Digest) !void {
    if (directory.fields.len != canonical.len) return error.InvalidDocumentCommitment;
    var members: std.ArrayList(Member) = .empty;
    defer members.deinit(a);
    for (directory.fields) |field| if (field.kind == .object) {
        members.clearRetainingCapacity();
        for (directory.fields[field.first_child..][0..field.child_count]) |child|
            try members.append(a, .{ .name = child.name, .digest = canonical[child.index] });
        if (!std.mem.eql(u8, &try objectDigest(members.items), &canonical[field.index])) return error.InvalidDocumentCommitment;
    };
    members.clearRetainingCapacity();
    for (directory.fields[0..directory.root_count]) |field|
        try members.append(a, .{ .name = field.name, .digest = canonical[field.index] });
    if (!std.mem.eql(u8, &try objectDigest(members.items), &expected)) return error.InvalidDocumentCommitment;
}
pub const TreeView = struct {
    raw: []const u8,
    directory: projection.Directory,
    canonical: []const Digest,
    pub fn deinit(self: TreeView, a: A) void {
        a.free(self.directory.fields);
    }
};
/// Borrow the authenticated manifest owner. Opening navigation allocates only
/// the bounded descriptor array; names and canonical digests are not copied.
pub fn openTree(a: A, raw: []const u8, expected: Digest) !?TreeView {
    if (!std.mem.startsWith(u8, raw, "DLC2")) return null;
    if (raw.len < 10 or raw.len > 64 * 1024) return error.InvalidDocumentCommitment;
    const directory_length = std.mem.readInt(u32, raw[4..8], .little);
    const count = std.mem.readInt(u16, raw[8..10], .little);
    if (directory_length < 47 or directory_length > projection.maximum_bytes or count > projection.maximum_fields or raw.len != 10 + @as(usize, directory_length) + @as(usize, count) * 32) return error.InvalidDocumentCommitment;
    const directory_raw = raw[10..][0..directory_length];
    const directory = (try projection.Directory.open(a, directory_raw, .{ .bytes = std.mem.readInt(u64, directory_raw[4..12], .little), .kind = directory_raw[44], .digest = expected })) orelse return error.InvalidDocumentCommitment;
    errdefer a.free(directory.fields);
    const canonical = std.mem.bytesAsSlice(Digest, raw[10 + directory_length ..]);
    try validateTree(a, directory, canonical, expected);
    return .{ .raw = directory_raw, .directory = directory, .canonical = canonical };
}
/// Consumers that need root dependency identities must not clone the complete
/// immutable navigation tree. Names remain owned across later backend reads.
pub fn decodeMembers(a: A, raw: []const u8, expected: Digest) !Root {
    const view = (try openTree(a, raw, expected)) orelse return decode(a, raw, expected);
    defer view.deinit(a);
    return .{ .digest = expected, .members = try ownMembers(a, view.directory, view.canonical) };
}
fn ownMembers(a: A, directory: projection.Directory, canonical: []const Digest) ![]Member {
    const members = try a.alloc(Member, directory.root_count);
    var initialized: usize = 0;
    errdefer {
        for (members[0..initialized]) |member| a.free(member.name);
        a.free(members);
    }
    for (directory.fields[0..directory.root_count], members) |field, *member| {
        member.* = .{ .name = try a.dupe(u8, field.name), .digest = canonical[field.index] };
        initialized += 1;
    }
    return members;
}
fn decodeTree(a: A, raw: []const u8, expected: Digest) !Root {
    const view = (try openTree(a, raw, expected)).?;
    defer view.deinit(a);
    const directory_raw = try a.dupe(u8, view.raw);
    errdefer a.free(directory_raw);
    const directory = (try projection.Directory.open(a, directory_raw, .{ .bytes = std.mem.readInt(u64, directory_raw[4..12], .little), .kind = directory_raw[44], .digest = expected })).?;
    errdefer a.free(directory.fields);
    const canonical = try a.dupe(Digest, view.canonical);
    errdefer a.free(canonical);
    const members = try ownMembers(a, directory, canonical);
    return .{ .digest = expected, .members = members, .tree = .{ .raw = directory_raw, .directory = directory, .canonical = canonical } };
}

test "document payload logical commitments ignore layout and object ordering without losing array order" {
    const a = std.testing.allocator;
    const examples = [_][]const u8{ "{\"body\":\"hello\",\"meta\":{\"b\":true,\"a\":[1,2]}}", " { \"meta\": {\"a\":[1, 2],\"b\":true}, \"b\\u006fdy\":\"hello\" } " };
    try std.testing.expectEqualSlices(u8, &try compute(a, examples[0]), &try compute(a, examples[1]));
    try std.testing.expect(!std.mem.eql(u8, &try compute(a, "[1,2]"), &try compute(a, "[2,1]")));
    try std.testing.expectError(error.DuplicateField, compute(a, "{\"x\":1,\"\\u0078\":2}"));
    const Case = struct {
        fn run(alloc: A, input: []const u8) !void {
            const root = try build(alloc, input);
            defer root.deinit(alloc);
            const encoded = try encode(alloc, root);
            defer alloc.free(encoded);
            const decoded = try decode(alloc, encoded, root.digest);
            defer decoded.deinit(alloc);
            try std.testing.expectEqualSlices(u8, &root.digest, &decoded.digest);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Case.run, .{examples[0]});
}

test "document payload fused canonical commitments equal recursive reference with nested values" {
    const Fixture = struct {
        fn run(a: A) !void {
            const text = try a.alloc(u8, 32 * 1024);
            defer a.free(text);
            @memset(text, 'a');
            const body = try std.fmt.allocPrint(a, "{{\"nested\":{{\"body\":\"{s}\",\"empty\":{{}},\"n\":1.0,\"u\":\"\\u0061\"}},\"array\":[2,{{\"x\":true}}],\"null\":null}}", .{text});
            defer a.free(body);
            const previous: Digest = @splat(0);
            const publication = try scalar.buildAuthority(a, body, 16, previous);
            defer if (publication.directory) |directory| a.free(directory);
            defer if (publication.identities) |identities| a.free(identities);
            const root = (try fromPublication(a, body, 16, previous, publication)).?;
            defer root.deinit(a);
            try std.testing.expectEqualSlices(u8, &try compute(a, body), &root.digest);
            const encoded = try encode(a, root);
            defer a.free(encoded);
            try std.testing.expect(std.mem.startsWith(u8, encoded, "DLC2"));
            const decoded = try decode(a, encoded, root.digest);
            defer decoded.deinit(a);
            try std.testing.expectEqualSlices(u8, &root.digest, &decoded.digest);
            const members_only = try decodeMembers(a, encoded, root.digest);
            defer members_only.deinit(a);
            try std.testing.expect(members_only.tree == null);
            try std.testing.expectEqual(root.members.len, members_only.members.len);
            for (root.members, members_only.members) |expected_member, member| {
                try std.testing.expectEqualStrings(expected_member.name, member.name);
                try std.testing.expectEqualSlices(u8, &expected_member.digest, &member.digest);
            }
            const view = (try openTree(a, encoded, root.digest)).?;
            defer view.deinit(a);
            const selected = view.directory.select(&.{ "nested", "u" }).?;
            try std.testing.expectEqual(@as(usize, 2), selected.covered);
            try std.testing.expectEqualSlices(u8, &try compute(a, "\"a\""), &view.canonical[selected.field.index]);
            const changed = try a.dupe(u8, encoded);
            defer a.free(changed);
            changed[changed.len - 1] ^= 1;
            if (decode(a, changed, root.digest)) |valid| {
                valid.deinit(a);
                return error.ExpectedInvalidDocumentCommitment;
            } else |err| if (err != error.InvalidDocumentCommitment) return err;
            for ([_]usize{ 0, 3, 4, 9, 10, encoded.len - 1 }) |length|
                try std.testing.expectError(error.InvalidDocumentCommitment, decode(a, encoded[0..length], root.digest));
            try scalar.rebindPublication(publication, previous, root.digest);
            try std.testing.expect(scalar.Certificate.open(publication.identities.?, publication.directory.?, .{ .bytes = body.len, .kind = @as(u8, 16), .digest = root.digest }) != null);
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}
