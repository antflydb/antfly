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

//! Immutable field/resource inventory beneath a held native family root.
//! Resource and input digests identify bytes; only actual graph navigation from
//! a native reader lease authenticates membership. All edges are direct, so a
//! successor can reuse fields without retaining a historical family generation.
const std = @import("std");
const graph = @import("immutable_graph.zig");
const publication = @import("index_publication.zig");
const segment = @import("../segment.zig");
pub const Digest = [32]u8;
const resource_bytes = 60;
const field_header_bytes = 124;
const section_bytes = 23;
pub const maximum_fields = 65536;
pub const maximum_name_bytes = 1024;

pub const Resource = struct {
    file: u64,
    payload_bytes: u64,
    physical_bytes: u64,
    identity: Digest,
    pub fn encode(self: Resource) ![resource_bytes]u8 {
        if (self.file == 0 or self.payload_bytes == 0 or self.payload_bytes >= self.physical_bytes or std.mem.allEqual(u8, &self.identity, 0)) return error.InvalidIndexFieldResource;
        const pages = self.payload_bytes / (64 * 1024) + @intFromBool(self.payload_bytes % (64 * 1024) != 0);
        const overhead = try std.math.add(u64, 32, try std.math.mul(u64, pages, 32));
        if (try std.math.add(u64, self.payload_bytes, overhead) != self.physical_bytes) return error.InvalidIndexFieldResource;
        var raw: [resource_bytes]u8 = undefined;
        @memcpy(raw[0..4], "IFR1");
        std.mem.writeInt(u64, raw[4..12], self.file, .little);
        std.mem.writeInt(u64, raw[12..20], self.payload_bytes, .little);
        std.mem.writeInt(u64, raw[20..28], self.physical_bytes, .little);
        @memcpy(raw[28..60], &self.identity);
        return raw;
    }
    pub fn decode(raw: []const u8) !Resource {
        if (raw.len != resource_bytes or !std.mem.eql(u8, raw[0..4], "IFR1")) return error.InvalidIndexFieldResource;
        const value: Resource = .{ .file = std.mem.readInt(u64, raw[4..12], .little), .payload_bytes = std.mem.readInt(u64, raw[12..20], .little), .physical_bytes = std.mem.readInt(u64, raw[20..28], .little), .identity = raw[28..60].* };
        _ = try value.encode();
        return value;
    }
};

pub fn stageResource(writer: anytype, owner: Digest, resource: Resource, limits: graph.Limits) !Digest {
    return graph.stage(writer, owner, .{ .kind = .leaf, .payload = &try resource.encode() }, limits);
}

pub const Section = struct {
    kind: segment.SectionType,
    resource: u8,
    offset: u64,
    length: u64,
    checksum: u32,
};
pub const Field = struct {
    name: []const u8,
    documents: u32,
    ordinal_inventory: Digest,
    configuration: Digest,
    inputs: Digest,
    stats: ?segment.SegmentReader.InvertedFieldStats = null,
    sections: []const Section,
};

fn writeSection(raw: *[section_bytes]u8, section: Section) void {
    std.mem.writeInt(u16, raw[0..2], @backingInt(section.kind), .little);
    raw[2] = section.resource;
    std.mem.writeInt(u64, raw[3..11], section.offset, .little);
    std.mem.writeInt(u64, raw[11..19], section.length, .little);
    std.mem.writeInt(u32, raw[19..23], section.checksum, .little);
}
fn readSection(raw: *const [section_bytes]u8) !Section {
    return .{ .kind = std.enums.fromInt(segment.SectionType, std.mem.readInt(u16, raw[0..2], .little)) orelse return error.InvalidIndexFieldGeneration, .resource = raw[2], .offset = std.mem.readInt(u64, raw[3..11], .little), .length = std.mem.readInt(u64, raw[11..19], .little), .checksum = std.mem.readInt(u32, raw[19..23], .little) };
}

/// Every section of a field (including norms/positions and typed values) moves
/// together. Resources must already be forced before the root can be published.
/// The producer derives inputs/configuration from its closed compiled read set;
/// neither this descriptor nor a matching digest is a read or skip-work grant.
pub fn stageField(writer: anytype, owner: Digest, field: Field, resources: []const Digest, limits: graph.Limits) !Digest {
    errdefer writer.poison();
    if (field.name.len == 0 or field.name.len > maximum_name_bytes or field.documents == 0 or resources.len == 0 or resources.len > graph.fanout or field.sections.len == 0 or field.sections.len > @typeInfo(segment.SectionType).@"enum".field_names.len) return error.InvalidIndexFieldGeneration;
    if (std.mem.allEqual(u8, &field.ordinal_inventory, 0) or std.mem.allEqual(u8, &field.configuration, 0)) return error.InvalidIndexFieldGeneration;
    if (field.stats) |stats| if (stats.doc_count > field.documents) return error.InvalidIndexFieldGeneration;
    var payloads: [graph.fanout]Resource = undefined;
    for (resources, 0..) |id, n| {
        for (resources[0..n]) |prior| if (std.mem.eql(u8, &prior, &id)) return error.InvalidIndexFieldGeneration;
        const node = try graph.loadNode(writer, owner, id);
        if (node.kind() != .leaf or node.childCount() != 0) return error.InvalidIndexFieldResource;
        payloads[n] = try Resource.decode(node.payload());
    }
    var kinds: u16 = 0;
    var used: u64 = 0;
    for (field.sections) |section| {
        if (section.resource >= resources.len) return error.InvalidIndexFieldGeneration;
        const resource = payloads[section.resource];
        if (section.length == 0 or section.offset > resource.payload_bytes or section.length > resource.payload_bytes - section.offset) return error.InvalidIndexFieldGeneration;
        const bit = @as(u16, 1) << @as(u4, @intCast(@backingInt(section.kind)));
        if (kinds & bit != 0) return error.InvalidIndexFieldGeneration;
        kinds |= bit;
        used |= @as(u64, 1) << @as(u6, @intCast(section.resource));
    }
    // Do not pin unrelated physical packs via unused child slots.
    if (@popCount(used) != resources.len or (field.stats != null and kinds & (@as(u16, 1) << @backingInt(segment.SectionType.inverted_text)) == 0)) return error.InvalidIndexFieldGeneration;
    var raw: [field_header_bytes + maximum_name_bytes + section_bytes * @typeInfo(segment.SectionType).@"enum".field_names.len]u8 = undefined;
    @memset(raw[0..field_header_bytes], 0);
    @memcpy(raw[0..4], "IFG1");
    std.mem.writeInt(u32, raw[4..8], field.documents, .little);
    std.mem.writeInt(u16, raw[8..10], @intCast(field.name.len), .little);
    std.mem.writeInt(u16, raw[10..12], @intCast(field.sections.len), .little);
    @memcpy(raw[12..44], &field.ordinal_inventory);
    @memcpy(raw[44..76], &field.configuration);
    @memcpy(raw[76..108], &field.inputs);
    if (field.stats) |stats| {
        raw[108] = 1;
        std.mem.writeInt(u32, raw[112..116], stats.doc_count, .little);
        std.mem.writeInt(u64, raw[116..124], stats.total_field_len, .little);
    }
    @memcpy(raw[field_header_bytes..][0..field.name.len], field.name);
    var offset = field_header_bytes + field.name.len;
    for (field.sections) |section| {
        writeSection(raw[offset..][0..section_bytes], section);
        offset += section_bytes;
    }
    return graph.stage(writer, owner, .{ .kind = .branch, .level = 1, .children = resources, .payload = raw[0..offset] }, limits);
}

pub const Descriptor = struct {
    raw: []const u8,
    pub fn decode(raw: []const u8) !Descriptor {
        if (raw.len < field_header_bytes or !std.mem.eql(u8, raw[0..4], "IFG1") or std.mem.readInt(u32, raw[4..8], .little) == 0 or raw[108] > 1 or !std.mem.allEqual(u8, raw[109..112], 0)) return error.InvalidIndexFieldGeneration;
        const name_bytes = std.mem.readInt(u16, raw[8..10], .little);
        const sections = std.mem.readInt(u16, raw[10..12], .little);
        if (name_bytes == 0 or name_bytes > maximum_name_bytes or sections == 0 or sections > @typeInfo(segment.SectionType).@"enum".field_names.len or raw.len != field_header_bytes + @as(usize, name_bytes) + @as(usize, sections) * section_bytes) return error.InvalidIndexFieldGeneration;
        if (std.mem.allEqual(u8, raw[12..44], 0) or std.mem.allEqual(u8, raw[44..76], 0)) return error.InvalidIndexFieldGeneration;
        if (raw[108] == 0 and !std.mem.allEqual(u8, raw[112..124], 0)) return error.InvalidIndexFieldGeneration;
        if (raw[108] == 1 and std.mem.readInt(u32, raw[112..116], .little) > std.mem.readInt(u32, raw[4..8], .little)) return error.InvalidIndexFieldGeneration;
        var kinds: u16 = 0;
        var inverted = false;
        const result: Descriptor = .{ .raw = raw };
        for (0..sections) |n| {
            const part = try result.section(n);
            if (part.length == 0 or part.resource >= graph.fanout) return error.InvalidIndexFieldGeneration;
            const bit = @as(u16, 1) << @as(u4, @intCast(@backingInt(part.kind)));
            if (kinds & bit != 0) return error.InvalidIndexFieldGeneration;
            kinds |= bit;
            inverted = inverted or part.kind == .inverted_text;
        }
        if (raw[108] == 1 and !inverted) return error.InvalidIndexFieldGeneration;
        return result;
    }
    pub fn name(self: Descriptor) []const u8 {
        return self.raw[field_header_bytes..][0..std.mem.readInt(u16, self.raw[8..10], .little)];
    }
    pub fn documents(self: Descriptor) u32 {
        return std.mem.readInt(u32, self.raw[4..8], .little);
    }
    pub fn ordinalInventory(self: Descriptor) Digest {
        return self.raw[12..44].*;
    }
    pub fn configuration(self: Descriptor) Digest {
        return self.raw[44..76].*;
    }
    pub fn inputs(self: Descriptor) Digest {
        return self.raw[76..108].*;
    }
    pub fn sectionCount(self: Descriptor) u16 {
        return std.mem.readInt(u16, self.raw[10..12], .little);
    }
    pub fn section(self: Descriptor, ordinal: usize) !Section {
        if (ordinal >= self.sectionCount()) return error.InvalidIndexFieldGeneration;
        const offset = field_header_bytes + self.name().len + ordinal * section_bytes;
        return readSection(self.raw[offset..][0..section_bytes]);
    }
    pub fn stats(self: Descriptor) ?segment.SegmentReader.InvertedFieldStats {
        if (self.raw[108] == 0) return null;
        return .{ .doc_count = std.mem.readInt(u32, self.raw[112..116], .little), .total_field_len = std.mem.readInt(u64, self.raw[116..124], .little) };
    }
};

fn descriptorForNode(node: *const graph.OwnedNode) !Descriptor {
    if (node.kind() != .branch or node.level() != 1 or node.childCount() == 0) return error.InvalidIndexFieldGeneration;
    const field = try Descriptor.decode(node.payload());
    var used: u64 = 0;
    for (0..field.sectionCount()) |ordinal| {
        const part = try field.section(ordinal);
        if (part.resource >= node.childCount()) return error.InvalidIndexFieldGeneration;
        used |= @as(u64, 1) << @as(u6, @intCast(part.resource));
    }
    if (@popCount(used) != node.childCount()) return error.InvalidIndexFieldGeneration;
    return field;
}

pub const HeldField = struct {
    node: graph.HeldNode,
    pub fn descriptor(self: *const HeldField) !Descriptor {
        _ = try self.node.metadata();
        return descriptorForNode(&self.node.node);
    }
    /// Return actual membership-bearing resource navigation, not a plain digest.
    /// The enclosing native family lease must remain live while this is used.
    pub fn resource(self: *const HeldField, reader: anytype, section: Section) !HeldResource {
        const field = try self.descriptor();
        const listed = for (0..field.sectionCount()) |ordinal| {
            if (std.meta.eql(try field.section(ordinal), section)) break true;
        } else false;
        if (!listed) return error.InvalidIndexFieldGeneration;
        const node = try self.node.child(reader, section.resource);
        if (node.node.kind() != .leaf or node.node.childCount() != 0) return error.InvalidIndexFieldResource;
        const value = try Resource.decode(try node.metadata());
        if (section.offset > value.payload_bytes or section.length > value.payload_bytes - section.offset) return error.InvalidIndexFieldResource;
        return .{ .node = node };
    }
};
pub const HeldResource = struct {
    node: graph.HeldNode,
    pub fn descriptor(self: *const HeldResource) !Resource {
        if (self.node.node.kind() != .leaf or self.node.node.childCount() != 0) return error.InvalidIndexFieldResource;
        return Resource.decode(try self.node.metadata());
    }
};

const directory_bytes = 76;
pub const maximum_directory_bytes = directory_bytes + 32 + 37;
const directory_level = 6;
const maximum_tree_levels = 4;
pub const stored_inventory_field = "\x00__antfly_stored_inventory";
pub const StoredInventory = struct {
    bytes: u64,
    metadata_bytes: u64,
    header: [21]u8,
    fn validate(self: StoredInventory, documents: u32) !void {
        const admission: segment.SegmentReader.Admission = .{
            .bytes = try std.math.add(u64, self.bytes, 56),
            .stored_offset = 0,
            .stored_length = self.bytes,
            .stored_metadata_length = self.metadata_bytes,
            .stored_header = self.header,
            .index_offset = self.bytes,
            .doc_count = documents,
            .fields = &.{},
        };
        try admission.validate();
    }
};
pub const Directory = struct {
    fields: u32,
    documents: u32,
    ordinal_inventory: Digest,
    configuration: Digest,
    publication_id: ?Digest = null,
    stored: ?StoredInventory = null,
    fn encode(self: Directory) ![directory_bytes]u8 {
        if ((self.fields == 0 and self.stored == null) or self.fields > maximum_fields or self.documents == 0 or std.mem.allEqual(u8, &self.ordinal_inventory, 0) or std.mem.allEqual(u8, &self.configuration, 0)) return error.InvalidIndexFieldDirectory;
        if (self.stored) |stored| {
            if (self.publication_id == null) return error.InvalidIndexFieldDirectory;
            try stored.validate(self.documents);
        }
        var raw: [directory_bytes]u8 = undefined;
        @memcpy(raw[0..4], "IFD1");
        std.mem.writeInt(u32, raw[4..8], self.fields, .little);
        std.mem.writeInt(u32, raw[8..12], self.documents, .little);
        @memcpy(raw[12..44], &self.ordinal_inventory);
        @memcpy(raw[44..76], &self.configuration);
        return raw;
    }
    pub fn decode(raw: []const u8) !Directory {
        const with_stored = raw.len == maximum_directory_bytes and std.mem.eql(u8, raw[0..4], "IFD3");
        const retained = with_stored or (raw.len == directory_bytes + 32 and std.mem.eql(u8, raw[0..4], "IFD2"));
        if (!retained and (raw.len != directory_bytes or !std.mem.eql(u8, raw[0..4], "IFD1"))) return error.InvalidIndexFieldDirectory;
        const value: Directory = .{ .fields = std.mem.readInt(u32, raw[4..8], .little), .documents = std.mem.readInt(u32, raw[8..12], .little), .ordinal_inventory = raw[12..44].*, .configuration = raw[44..76].*, .publication_id = if (retained) raw[76..108].* else null, .stored = if (with_stored) .{ .bytes = std.mem.readInt(u64, raw[108..116], .little), .metadata_bytes = std.mem.readInt(u64, raw[116..124], .little), .header = raw[124..145].* } else null };
        _ = try value.encode();
        if (value.publication_id) |id| if (std.mem.allEqual(u8, &id, 0)) return error.InvalidIndexFieldDirectory;
        return value;
    }
    pub fn metadata(self: Directory, buffer: *[maximum_directory_bytes]u8) ![]const u8 {
        @memcpy(buffer[0..directory_bytes], &try self.encode());
        if (self.publication_id) |id| {
            if (std.mem.allEqual(u8, &id, 0)) return error.InvalidIndexFieldDirectory;
            @memcpy(buffer[0..4], "IFD2");
            @memcpy(buffer[directory_bytes..][0..32], &id);
            if (self.stored) |stored| {
                @memcpy(buffer[0..4], "IFD3");
                std.mem.writeInt(u64, buffer[108..116], stored.bytes, .little);
                std.mem.writeInt(u64, buffer[116..124], stored.metadata_bytes, .little);
                @memcpy(buffer[124..145], &stored.header);
                return buffer;
            }
            return buffer[0 .. directory_bytes + 32];
        }
        return buffer[0..directory_bytes];
    }
};

/// Streaming construction retains one bounded fanout stack, never an array of
/// every field or resource. Names are strictly ordered; ordinal/configuration
/// closure is checked against each actual staged field node before admission.
pub const DirectoryBuilder = struct {
    owner: Digest,
    directory: Directory,
    count: u32 = 0,
    buckets: [maximum_tree_levels][graph.fanout]Digest = undefined,
    counts: [maximum_tree_levels]u8 = @splat(0),
    previous_name: [maximum_name_bytes]u8 = undefined,
    previous_name_len: usize = 0,
    finished: bool = false,
    stored_node: ?Digest = null,
    pub fn init(owner: Digest, directory: Directory) !DirectoryBuilder {
        if (directory.stored != null) return error.InvalidIndexFieldDirectory;
        _ = try directory.encode();
        return .{ .owner = owner, .directory = directory };
    }
    pub fn initWithStored(owner: Digest, directory: Directory, stored: Digest) !DirectoryBuilder {
        if (directory.stored == null) return error.InvalidIndexFieldDirectory;
        _ = try directory.encode();
        return .{ .owner = owner, .directory = directory, .stored_node = stored };
    }
    fn push(self: *DirectoryBuilder, writer: anytype, initial: Digest, initial_level: usize, limits: graph.Limits) !void {
        var id = initial;
        var level = initial_level;
        while (true) : (level += 1) {
            if (level >= self.buckets.len) return error.InvalidIndexFieldDirectory;
            self.buckets[level][self.counts[level]] = id;
            self.counts[level] += 1;
            if (self.counts[level] < graph.fanout) return;
            id = try graph.stage(writer, self.owner, .{ .kind = .branch, .level = @intCast(level + 2), .children = &self.buckets[level] }, limits);
            self.counts[level] = 0;
        }
    }
    pub fn append(self: *DirectoryBuilder, writer: anytype, field: Digest, limits: graph.Limits) !void {
        errdefer writer.poison();
        if (self.finished or self.count >= self.directory.fields) return error.InvalidIndexFieldDirectory;
        const node = try graph.loadNode(writer, self.owner, field);
        const descriptor = try descriptorForNode(&node);
        if (descriptor.documents() != self.directory.documents or !std.mem.eql(u8, &descriptor.ordinalInventory(), &self.directory.ordinal_inventory) or !std.mem.eql(u8, &descriptor.configuration(), &self.directory.configuration)) return error.InvalidIndexFieldDirectory;
        const name = descriptor.name();
        if (self.count != 0 and std.mem.order(u8, self.previous_name[0..self.previous_name_len], name) != .lt) return error.InvalidIndexFieldDirectory;
        try self.push(writer, field, 0, limits);
        @memcpy(self.previous_name[0..name.len], name);
        self.previous_name_len = name.len;
        self.count += 1;
    }
    pub fn finish(self: *DirectoryBuilder, writer: anytype, limits: graph.Limits) !Digest {
        errdefer writer.poison();
        if (self.finished or self.count != self.directory.fields) return error.InvalidIndexFieldDirectory;
        if (self.stored_node) |id| {
            const node = try graph.loadNode(writer, self.owner, id);
            try validateStoredField(try descriptorForNode(&node), self.directory);
        }
        if (self.directory.fields == 0) {
            const inventory = self.stored_node orelse return error.InvalidIndexFieldDirectory;
            var metadata: [maximum_directory_bytes]u8 = undefined;
            const root = try graph.stage(writer, self.owner, .{ .kind = .retained, .level = directory_level, .children = &.{inventory}, .payload = try self.directory.metadata(&metadata) }, limits);
            self.finished = true;
            return root;
        }
        for (0..self.buckets.len) |level| {
            const n = self.counts[level];
            if (n == 0) continue;
            var higher = false;
            for (self.counts[level + 1 ..]) |other| higher = higher or other != 0;
            if (!higher and n == 1) {
                const tree = self.buckets[level][0];
                var metadata: [maximum_directory_bytes]u8 = undefined;
                const children = [_]Digest{ tree, self.stored_node orelse @splat(0) };
                const root = try graph.stage(writer, self.owner, .{ .kind = if (self.directory.publication_id != null) .retained else .branch, .level = directory_level, .children = children[0..if (self.stored_node != null) @as(usize, 2) else 1], .payload = try self.directory.metadata(&metadata) }, limits);
                self.finished = true;
                return root;
            }
            const branch = try graph.stage(writer, self.owner, .{ .kind = .branch, .level = @intCast(level + 2), .children = self.buckets[level][0..n] }, limits);
            self.counts[level] = 0;
            try self.push(writer, branch, level + 1, limits);
        }
        return error.InvalidIndexFieldDirectory;
    }
};

pub const Selection = struct {
    directory: Directory,
    field: HeldField,
    path: [6]u8,
    path_length: u8,
};

/// The field directory must be selected beneath the caller's actual held
/// family. A copied digest, descriptor or bare ordinal cannot construct a pin.
pub fn select(reader: anytype, family: *const graph.HeldNode, child: u8, ordinal: u32) !Selection {
    _ = try family.metadata();
    if (family.node.kind() != .family) return error.InvalidIndexFieldDirectory;
    const directory_node = try family.child(reader, child);
    var result = try selectDirectory(reader, &directory_node, ordinal);
    if (result.path_length >= result.path.len) return error.InvalidIndexFieldDirectory;
    std.mem.copyBackwards(u8, result.path[1 .. result.path_length + 1], result.path[0..result.path_length]);
    result.path[0] = child;
    result.path_length += 1;
    return result;
}

pub fn selectFields(reader: anytype, root: *const graph.HeldNode, ordinal: u32) !Selection {
    if (root.node.kind() != .retained) return error.InvalidIndexFieldDirectory;
    const result = try selectDirectory(reader, root, ordinal);
    const id = result.directory.publication_id orelse return error.IndexPublicationFenceMismatch;
    if (!std.mem.eql(u8, &id, &root.binding.publication)) return error.IndexPublicationFenceMismatch;
    return result;
}

fn directoryForNode(node: *const graph.OwnedNode) !Directory {
    if ((node.kind() != .branch and node.kind() != .retained) or node.level() != directory_level) return error.InvalidIndexFieldDirectory;
    const directory = try Directory.decode(node.payload());
    const children: usize = @intFromBool(directory.fields != 0) + @as(usize, @intFromBool(directory.stored != null));
    if (children != node.childCount() or (directory.publication_id != null) != (node.kind() == .retained)) return error.InvalidIndexFieldDirectory;
    return directory;
}

pub fn describeFields(root: *const graph.HeldNode) !Directory {
    _ = try root.metadata();
    if (root.node.kind() != .retained) return error.InvalidIndexFieldDirectory;
    const directory = try directoryForNode(&root.node);
    const id = directory.publication_id orelse return error.IndexPublicationFenceMismatch;
    if (!std.mem.eql(u8, &id, &root.binding.publication)) return error.IndexPublicationFenceMismatch;
    return directory;
}

fn validateStoredField(field: Descriptor, directory: Directory) !void {
    const stored = directory.stored orelse return error.InvalidIndexFieldDirectory;
    if (!std.mem.eql(u8, field.name(), stored_inventory_field) or field.documents() != directory.documents or field.sectionCount() != 1 or !std.mem.eql(u8, &field.ordinalInventory(), &directory.ordinal_inventory) or !std.mem.eql(u8, &field.configuration(), &directory.configuration)) return error.InvalidIndexFieldDirectory;
    const part = try field.section(0);
    if (part.kind != .columnar_stored or part.length != stored.bytes) return error.InvalidIndexFieldDirectory;
}

pub fn selectStoredFields(reader: anytype, root: *const graph.HeldNode) !HeldField {
    const directory = try describeFields(root);
    return selectStored(reader, root, directory);
}

fn selectStored(reader: anytype, root: *const graph.HeldNode, directory: Directory) !HeldField {
    if (directory.stored == null) return error.InvalidIndexFieldDirectory;
    const held: HeldField = .{ .node = try root.child(reader, @intFromBool(directory.fields != 0)) };
    try validateStoredField(try held.descriptor(), directory);
    return held;
}

fn selectDirectory(reader: anytype, directory_node: *const graph.HeldNode, ordinal: u32) !Selection {
    _ = try directory_node.metadata();
    const directory = try directoryForNode(&directory_node.node);
    if (ordinal >= directory.fields) return error.InvalidIndexFieldDirectory;
    var result: Selection = .{ .directory = directory, .field = undefined, .path = undefined, .path_length = 1 };
    result.path[0] = 0;
    var node = try directory_node.child(reader, 0);
    var capacity: u64 = 1;
    var level: u8 = 1;
    while (capacity < directory.fields) : (level += 1) capacity *= graph.fanout;
    var offset: u64 = ordinal;
    var remaining: u64 = directory.fields;
    while (level > 1) : (level -= 1) {
        if (node.node.kind() != .branch or node.node.level() != level or node.node.payload().len != 0) return error.InvalidIndexFieldDirectory;
        capacity /= graph.fanout;
        if ((remaining - 1) / capacity + 1 != node.node.childCount()) return error.InvalidIndexFieldDirectory;
        const selected: usize = @intCast(offset / capacity);
        if (result.path_length >= result.path.len) return error.InvalidIndexFieldDirectory;
        result.path[result.path_length] = @intCast(selected);
        result.path_length += 1;
        node = try node.child(reader, selected);
        offset %= capacity;
        remaining = @min(capacity, remaining - selected * capacity);
    }
    result.field = .{ .node = node };
    const descriptor = try result.field.descriptor();
    if (descriptor.documents() != directory.documents or !std.mem.eql(u8, &descriptor.ordinalInventory(), &directory.ordinal_inventory) or !std.mem.eql(u8, &descriptor.configuration(), &directory.configuration)) return error.InvalidIndexFieldDirectory;
    return result;
}

/// Prepare one field replacement from an actual held directory selection.
/// Preserve ordinal identity, field position/name and compiler configuration;
/// changing any of those requires a new inventory. The caller stages the native
/// publication receipt and expected family-root CAS in this same transaction.
pub fn replace(writer: anytype, family: *const graph.HeldNode, selected: *const Selection, replacement: Digest, limits: graph.Limits) !Digest {
    return replaceWithPublication(writer, family, selected, replacement, null, limits);
}

pub fn replacePublished(writer: anytype, root: *const graph.HeldNode, selected: *const Selection, replacement: Digest, next: publication.Binding, limits: graph.Limits) !Digest {
    return replaceWithPublication(writer, root, selected, replacement, next, limits);
}

fn replaceWithPublication(writer: anytype, family: *const graph.HeldNode, selected: *const Selection, replacement: Digest, next_publication: ?publication.Binding, limits: graph.Limits) !Digest {
    errdefer writer.poison();
    const previous = try selected.field.descriptor();
    if (selected.path_length == 0 or selected.path_length > selected.path.len) return error.IndexPublicationFenceMismatch;
    if (!std.meta.eql(family.binding, selected.field.node.binding) or family.lease != selected.field.node.lease) return error.IndexPublicationFenceMismatch;
    const node = try graph.loadNode(writer, family.binding.owner, replacement);
    const next = try descriptorForNode(&node);
    if (previous.documents() != next.documents() or !std.mem.eql(u8, previous.name(), next.name()) or !std.mem.eql(u8, &previous.ordinalInventory(), &next.ordinalInventory()) or !std.mem.eql(u8, &previous.configuration(), &next.configuration())) return error.InvalidIndexFieldGeneration;
    // Re-select through this exact held root; a copied Selection cannot supply
    // an unrelated authenticated node plus a forged path to another field.
    var cursor = family.*;
    for (selected.path[0..selected.path_length]) |step| cursor = try cursor.child(writer, step);
    if (!std.mem.eql(u8, &cursor.id, &selected.field.node.id)) return error.IndexPublicationFenceMismatch;
    if (selected.directory.publication_id != null) {
        const next_binding = next_publication orelse return error.IndexPublicationFenceMismatch;
        if (family.node.kind() != .retained or !next_binding.valid() or !std.mem.eql(u8, &next_binding.owner, &family.binding.owner) or std.mem.eql(u8, &next_binding.publication, &family.binding.publication)) return error.IndexPublicationFenceMismatch;
        var directory = selected.directory;
        directory.publication_id = next_binding.publication;
        var metadata: [maximum_directory_bytes]u8 = undefined;
        return graph.replacePathsWithMetadata(writer, family, &.{.{ .path = selected.path[0..selected.path_length], .replacement = replacement }}, try directory.metadata(&metadata), limits);
    }
    if (next_publication != null) return error.IndexPublicationFenceMismatch;
    return graph.replacePath(writer, family, selected.path[0..selected.path_length], replacement, limits);
}

/// Bind a separately pinned physical field generation under the native index
/// owner fence. The coordinator forces resources first, and publishes this
/// receipt, its reverse edge and the logical family CAS in one index transaction.
pub fn stagePublication(a: std.mem.Allocator, writer: anytype, binding: publication.Binding, root: Digest, limits: graph.Limits) !void {
    errdefer writer.poison();
    const epochs = @import("index_source_epoch.zig");
    const owner = try epochs.Owner.decode(try writer.get(epochs.owner_key));
    if (!owner.fields or !owner.families) return error.IndexSourceOwnerMismatch;
    const epoch = try owner.admitPublication(binding);
    const node = try graph.loadNode(writer, binding.owner, root);
    if (node.kind() != .retained) return error.InvalidIndexFieldDirectory;
    const directory = try directoryForNode(&node);
    const id = directory.publication_id orelse return error.IndexPublicationFenceMismatch;
    if (!std.mem.eql(u8, &id, &binding.publication)) return error.IndexPublicationFenceMismatch;
    _ = try publication.stagePublishRoleInEpoch(a, writer, binding, root, &.{}, epoch, .fields);
    try graph.bindResource(writer, binding.owner, binding.publication, root, limits);
}

pub fn checkPublication(reader: anytype, owner: Digest, root: Digest) !Directory {
    const node = try graph.loadNode(reader, owner, root);
    if (node.kind() != .retained) return error.InvalidIndexFieldDirectory;
    const directory = try directoryForNode(&node);
    const id = directory.publication_id orelse return error.IndexPublicationFenceMismatch;
    const binding: publication.Binding = .{ .owner = owner, .publication = id };
    const decision = (try publication.decision(reader, binding)) orelse return error.IndexPublicationMissing;
    try decision.requireRole(.fields);
    try decision.require(binding, .published);
    if (!std.mem.eql(u8, &decision.root, &root)) return error.IndexPublicationFenceMismatch;
    if (!std.mem.eql(u8, &((try graph.resourceNode(reader, owner, id)) orelse return error.IndexPublicationFenceMismatch), &root)) return error.IndexPublicationFenceMismatch;
    return directory;
}

/// Completion probe after the resource publication is retired and its actual
/// physical readers have drained. A copied drain token or still-owned root
/// cannot release descendants. The collector reclaims the retained node next.
pub fn completeRetirement(writer: anytype, node: Digest, decision: publication.Decision, drained: publication.ReaderPins.Drained) !void {
    errdefer writer.poison();
    try decision.requireRole(.fields);
    try decision.require(decision.binding, .retired);
    try publication.ReaderPins.requireDrained(drained, decision.binding);
    if (!std.mem.eql(u8, &decision.root, &node) or try graph.referenceCount(writer, decision.binding.owner, node) != 0) return error.IndexPublicationStillActive;
    const owned = try graph.loadNode(writer, decision.binding.owner, node);
    if (owned.kind() != .retained) return error.InvalidIndexFieldDirectory;
    const directory = try Directory.decode(owned.payload());
    const id = directory.publication_id orelse return error.IndexPublicationFenceMismatch;
    if (!std.mem.eql(u8, &id, &decision.binding.publication)) return error.IndexPublicationFenceMismatch;
    try graph.unbindResource(writer, decision.binding.owner, decision.binding.publication, node);
}

pub const TransferLimits = struct { resources: usize = 65536 };

/// Rebase a complete held inventory into another owner. The importer receives
/// native membership-bearing resource navigation and owns prepared physical
/// resources until the coordinator commits (or discards) its destination root.
/// It must force each new pack before returning its staged destination node.
/// This routine copies metadata only, deduplicates identical packs and never
/// opens a path or manufactures authority from a serialized digest.
pub fn rebase(a: std.mem.Allocator, donor: anytype, family: *const graph.HeldNode, child: u8, destination: anytype, owner: Digest, importer: anytype, limits: TransferLimits, graph_limits: graph.Limits) !Digest {
    return rebaseWithPublication(a, donor, family, child, destination, owner, importer, null, limits, graph_limits);
}

pub fn rebasePublished(a: std.mem.Allocator, donor: anytype, family: *const graph.HeldNode, child: u8, destination: anytype, binding: publication.Binding, importer: anytype, limits: TransferLimits, graph_limits: graph.Limits) !Digest {
    if (!binding.valid()) return error.IndexPublicationFenceMismatch;
    return rebaseWithPublication(a, donor, family, child, destination, binding.owner, importer, binding.publication, limits, graph_limits);
}

const CopiedResource = struct { source: Resource, output: Digest };
const CopiedResources = std.AutoHashMapUnmanaged(Digest, CopiedResource);

fn rebaseWithPublication(a: std.mem.Allocator, donor: anytype, family: *const graph.HeldNode, child: u8, destination: anytype, owner: Digest, importer: anytype, publication_id: ?Digest, limits: TransferLimits, graph_limits: graph.Limits) !Digest {
    errdefer destination.poison();
    _ = try family.metadata();
    if (family.node.kind() != .family) return error.InvalidIndexFieldDirectory;
    const directory_node = try family.child(donor, child);
    _ = try directory_node.metadata();
    var directory = try directoryForNode(&directory_node.node);
    if (directory.publication_id != null and publication_id == null) return error.IndexPublicationFenceMismatch;
    var copied: CopiedResources = .empty;
    defer copied.deinit(a);
    const stored = if (directory.stored != null) try rebaseField(a, donor, &try selectStored(donor, &directory_node, directory), destination, owner, importer, &copied, limits, graph_limits) else null;
    directory.publication_id = publication_id;
    var builder = if (stored) |node| try DirectoryBuilder.initWithStored(owner, directory, node) else try DirectoryBuilder.init(owner, directory);
    for (0..directory.fields) |ordinal| {
        const selected = try selectDirectory(donor, &directory_node, @intCast(ordinal));
        const next = try rebaseField(a, donor, &selected.field, destination, owner, importer, &copied, limits, graph_limits);
        try builder.append(destination, next, graph_limits);
    }
    return builder.finish(destination, graph_limits);
}

fn rebaseField(a: std.mem.Allocator, donor: anytype, selected: *const HeldField, destination: anytype, owner: Digest, importer: anytype, copied: *CopiedResources, limits: TransferLimits, graph_limits: graph.Limits) !Digest {
    const field = try selected.descriptor();
    if (selected.node.node.childCount() > graph.fanout) return error.InvalidIndexFieldGeneration;
    var resources: [graph.fanout]Digest = undefined;
    var sections: [@typeInfo(segment.SectionType).@"enum".field_names.len]Section = undefined;
    for (sections[0..field.sectionCount()], 0..) |*part, index| part.* = try field.section(index);
    for (resources[0..selected.node.node.childCount()], 0..) |*output, slot| {
        const part = for (sections[0..field.sectionCount()]) |candidate| {
            if (candidate.resource == slot) break candidate;
        } else return error.InvalidIndexFieldGeneration;
        const held = try selected.resource(donor, part);
        const source = try held.descriptor();
        if (copied.get(source.identity)) |existing| {
            if (source.payload_bytes != existing.source.payload_bytes or source.physical_bytes != existing.source.physical_bytes) return error.InvalidIndexFieldResource;
            output.* = existing.output;
            continue;
        }
        if (copied.count() >= limits.resources) return error.ResourceBudgetExceeded;
        try copied.ensureUnusedCapacity(a, 1);
        const imported = try importer.importResource(a, &held);
        const imported_node = try graph.loadNode(destination, owner, imported);
        if (imported_node.kind() != .leaf or imported_node.childCount() != 0) return error.InvalidIndexFieldResource;
        const actual = try Resource.decode(imported_node.payload());
        if (!std.mem.eql(u8, &actual.identity, &source.identity) or actual.payload_bytes != source.payload_bytes or actual.physical_bytes != source.physical_bytes) return error.InvalidIndexFieldResource;
        copied.putAssumeCapacity(source.identity, .{ .source = source, .output = imported });
        output.* = imported;
    }
    return stageField(destination, owner, .{
        .name = field.name(),
        .documents = field.documents(),
        .ordinal_inventory = field.ordinalInventory(),
        .configuration = field.configuration(),
        .inputs = field.inputs(),
        .stats = field.stats(),
        .sections = sections[0..field.sectionCount()],
    }, resources[0..selected.node.node.childCount()], graph_limits);
}
