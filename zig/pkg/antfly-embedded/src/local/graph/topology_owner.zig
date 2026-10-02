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

//! Durable immutable topology ownership. Numerical jobs hold explicit pins;
//! neither their page attempt numbers nor their cleanup namespaces own tiles.
const std = @import("std");
const Allocator = std.mem.Allocator;
pub const epoch: u64 = 1;
pub const catalog_prefix = "meta:metric_topology:owners/";
pub const gc_cursor_key = "meta:metric_topology:gc-cursor";
pub const Id = [64]u8;
pub const Digest = [32]u8;
pub const State = enum(u8) { building, sealed, deleting };

pub const Record = struct {
    // The envelope stays decodable across topology format epochs so obsolete
    // owners can be reclaimed without opening their membership or tile data.
    format_epoch: u64 = epoch,
    state: State = .building,
    generation: u64,
    filter: Digest,
    identity: Digest,
    bidirectional: bool,

    pub fn encode(self: @This()) [114]u8 {
        var raw: [114]u8 = @splat(0);
        std.mem.writeInt(u64, raw[0..8], self.format_epoch, .little);
        raw[8] = @intFromEnum(self.state);
        raw[9] = @intFromBool(self.bidirectional);
        std.mem.writeInt(u64, raw[10..18], self.generation, .little);
        @memcpy(raw[18..50], &self.filter);
        @memcpy(raw[50..82], &self.identity);
        std.crypto.hash.sha2.Sha256.hash(raw[0..82], raw[82..114], .{});
        return raw;
    }

    pub fn decode(raw: []const u8) !@This() {
        if (raw.len != 114 or std.mem.readInt(u64, raw[0..8], .little) == 0 or raw[9] > 1)
            return error.InvalidGraphMetricBuildManifest;
        var checksum: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw[0..82], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, raw[82..114])) return error.InvalidGraphMetricBuildManifest;
        return .{
            .format_epoch = std.mem.readInt(u64, raw[0..8], .little),
            .state = std.enums.fromInt(State, raw[8]) orelse return error.InvalidGraphMetricBuildManifest,
            .generation = std.mem.readInt(u64, raw[10..18], .little),
            .filter = raw[18..50].*,
            .identity = raw[50..82].*,
            .bidirectional = raw[9] != 0,
        };
    }
};

pub const Binding = struct {
    id: Id,
    adopted: bool,

    pub fn encode(self: @This()) [97]u8 {
        var raw: [97]u8 = undefined;
        raw[0] = @intFromBool(self.adopted);
        @memcpy(raw[1..65], &self.id);
        std.crypto.hash.sha2.Sha256.hash(raw[0..65], raw[65..97], .{});
        return raw;
    }

    pub fn decode(raw: []const u8) !@This() {
        if (raw.len != 97 or raw[0] > 1) return error.InvalidGraphMetricBuildManifest;
        var checksum: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw[0..65], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, raw[65..97])) return error.InvalidGraphMetricBuildManifest;
        for (raw[1..65]) |c| if (!std.ascii.isHex(c)) return error.InvalidGraphMetricBuildManifest;
        return .{ .adopted = raw[0] != 0, .id = raw[1..65].* };
    }
};

pub fn filterDigest(alloc: Allocator, filter: anytype) !Digest {
    const types = try alloc.dupe([]const u8, filter.types);
    defer alloc.free(types);
    std.mem.sort([]const u8, types, {}, struct {
        fn less(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.order(u8, a, b) == .lt;
        }
    }.less);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(@tagName(filter.mode));
    for (types) |name| {
        var len: [8]u8 = undefined;
        std.mem.writeInt(u64, &len, name.len, .little);
        hash.update(&len);
        hash.update(name);
    }
    return hash.finalResult();
}

pub fn identity(filter: Digest, partition_plan: []const u8) Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var version: [8]u8 = undefined;
    std.mem.writeInt(u64, &version, epoch, .little);
    hash.update(&version);
    hash.update(&filter);
    hash.update(partition_plan);
    return hash.finalResult();
}

pub fn ownerId(digest: Digest, producer_namespace: []const u8, score_generation: u64) Id {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(&digest);
    hash.update(producer_namespace);
    var generation: [8]u8 = undefined;
    std.mem.writeInt(u64, &generation, score_generation, .little);
    hash.update(&generation);
    return std.fmt.bytesToHex(hash.finalResult(), .lower);
}

pub fn catalogKey(alloc: Allocator, id: Id) ![]u8 {
    return std.fmt.allocPrint(alloc, "{s}{s}", .{ catalog_prefix, id });
}

pub fn dataPrefix(alloc: Allocator, id: Id) ![]u8 {
    return std.fmt.allocPrint(alloc, "meta:metric_topology:data/{s}/", .{id});
}

pub fn pinsPrefix(alloc: Allocator, id: Id) ![]u8 {
    return std.fmt.allocPrint(alloc, "meta:metric_topology:pins/{s}/", .{id});
}

pub fn readyKey(alloc: Allocator, digest: Digest, bidirectional: bool) ![]u8 {
    return std.fmt.allocPrint(alloc, "meta:metric_topology:ready/{s}/{d}", .{ std.fmt.bytesToHex(digest, .lower), @intFromBool(bidirectional) });
}

test "topology receipts reject corruption and binding traversal" {
    const record = Record{ .generation = 7, .filter = @splat(1), .identity = @splat(2), .bidirectional = true };
    var raw = record.encode();
    try std.testing.expectEqualDeep(record, try Record.decode(&raw));
    raw[18] ^= 1;
    try std.testing.expectError(error.InvalidGraphMetricBuildManifest, Record.decode(&raw));
    var binding = (Binding{ .id = @splat('a'), .adopted = true }).encode();
    binding[12] = '/';
    try std.testing.expectError(error.InvalidGraphMetricBuildManifest, Binding.decode(&binding));
}
