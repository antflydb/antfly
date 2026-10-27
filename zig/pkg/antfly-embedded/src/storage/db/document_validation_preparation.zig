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

//! Request-owned native validation effects. Unknown rules, missing attestation
//! and unprepared writes retain complete validation. No source bytes or pointers
//! from this preparation enter a public request or the durable journal.
const std = @import("std");
const validation = @import("../document_validation.zig");
const payload = @import("../document_payload.zig");
const logical = @import("../document_commitment.zig");
const projection = @import("../document_projection.zig");
const schema = @import("../../schema/mod.zig");
const keys = @import("../internal_keys.zig");
const A = std.mem.Allocator;

pub const Prepared = struct {
    const Entry = struct { physical: []u8, value: []const u8 };
    entries: []Entry = &.{},
    stamp: validation.Stamp = .{ .rules = @splat(0), .schema_version = 0, .key = @splat(0) },
    full: usize = 0,
    reused: usize = 0,

    pub fn deinit(self: *Prepared, a: A) void {
        for (self.entries) |entry| a.free(entry.physical);
        if (self.entries.len != 0) a.free(self.entries);
        self.* = .{};
    }
    pub fn provider(self: *const Prepared) @import("../commit_participant.zig").DocumentValidations {
        const Bridge = struct {
            fn find(ptr: *const anyopaque, physical: []const u8, value: []const u8) ?validation.Stamp {
                const owner: *const Prepared = @ptrCast(@alignCast(ptr));
                var low: usize = 0;
                var high = owner.entries.len;
                while (low < high) {
                    const mid = low + (high - low) / 2;
                    const entry = owner.entries[mid];
                    switch (std.mem.order(u8, physical, entry.physical)) {
                        .lt => high = mid,
                        .gt => low = mid + 1,
                        .eq => return if (value.len == entry.value.len and (value.ptr == entry.value.ptr or std.mem.eql(u8, value, entry.value))) owner.stamp else null,
                    }
                }
                return null;
            }
        };
        return .{ .ptr = self, .find = Bridge.find };
    }
};

pub fn prepare(a: A, compiled: schema.CompiledTableValidator, version: u32, authority: ?[32]u8, store: *@import("../docstore.zig").DocStore, comptime isMetadata: anytype, writes: anytype, patches: anytype, snapshot: anytype) !Prepared {
    const rules = compiled.rule_fingerprint orelse @as([32]u8, @splat(0));
    var result: Prepared = .{ .stamp = .{ .rules = rules, .schema_version = version, .key = authority orelse @splat(0) } };
    errdefer result.deinit(a);
    var entries: std.ArrayList(Prepared.Entry) = .empty;
    errdefer {
        for (entries.items) |entry| a.free(entry.physical);
        entries.deinit(a);
    }
    const reusable_rules = authority != null and compiled.supportsRootValidationReuse();
    var read: ?@import("../docstore.zig").DocStore.Txn = null;
    defer if (read) |*txn| txn.abort();
    for (writes, 0..) |write, index| {
        if (isMetadata(write.key)) continue;
        const eligible = reusable_rules and write.value.len >= projection.minimum_bytes;
        const physical = if (eligible) try keys.documentKeyAlloc(a, write.key) else null;
        var transferred = false;
        defer if (!transferred) if (physical) |key| a.free(key);
        var reused = false;
        if (eligible) if (patches.forKey(write.key)) |patch| {
            if (patch.sameInput(write.value)) if (snapshot.positions.get(write.key)) |position| {
                if (snapshot.payloads.get(position)) |source| {
                    if (read == null) read = try store.beginProbeTxn();
                    if (try validation.load(&read.?, physical.?, authority.?)) |certificate| {
                        if (certificate.matches(source.proof, rules, version, source.manifest[source.manifest.len - 32 ..][0..32].*))
                            reused = try validateChanged(a, compiled, source, patch.*);
                    }
                }
            };
        };
        if (!reused) {
            try compiled.validateWrites(a, writes[index .. index + 1]);
            result.full += 1;
        } else result.reused += 1;
        // Only semantically non-mutating validators may attest the original
        // input. SQL/default/UUID preparation retains the ordinary path.
        if (eligible) {
            try entries.append(a, .{ .physical = physical.?, .value = write.value });
            transferred = true;
        }
    }
    std.mem.sort(Prepared.Entry, entries.items, {}, struct {
        fn less(_: void, l: Prepared.Entry, r: Prepared.Entry) bool {
            return std.mem.order(u8, l.physical, r.physical) == .lt;
        }
    }.less);
    result.entries = try entries.toOwnedSlice(a);
    return result;
}

/// The shadow contains every real root name, but only changed values and the
/// discriminator. It is used solely by the root-composable validator, never by
/// extraction, index builders, callbacks or source publication.
pub fn validateChanged(a: A, compiled: schema.CompiledTableValidator, source: payload.SourceMetadata, after: payload.PreparedWrite) !bool {
    var arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(a));
    defer arena.deinit();
    const temp = arena.allocator();
    const old = try payload.Manifest.decode(temp, source.manifest);
    if (old.logical_record.len == 0) return false;
    const before = try logical.decodeMembers(temp, old.logical_record, source.proof.digest);
    const next = try logical.decodeMembers(temp, after.patch.logical_record, after.patch.proof.digest);
    const directory = (try projection.Directory.open(temp, after.patch.directory, after.patch.proof)) orelse return false;
    var unchanged: std.StringHashMapUnmanaged(void) = .empty;
    var shadow: std.json.Value = .{ .object = std.json.ObjectMap.empty };
    var cursor: usize = 0;
    var bytes: usize = 0;
    for (next.members) |member| {
        while (cursor < before.members.len and std.mem.order(u8, before.members[cursor].name, member.name) == .lt) : (cursor += 1) {}
        const same = cursor < before.members.len and std.mem.eql(u8, member.name, before.members[cursor].name) and std.mem.eql(u8, &member.digest, &before.members[cursor].digest);
        const discriminator = std.mem.eql(u8, member.name, "_type");
        if (discriminator and !same) return false;
        if (same) try unchanged.put(temp, member.name, {});
        var value: std.json.Value = .null;
        if (!same or discriminator) {
            const field = directory.find(member.name) orelse return false;
            bytes = std.math.add(usize, bytes, @intCast(field.length)) catch return false;
            if (bytes > 32 * 1024 or field.offset > after.expected_json.len or field.length > after.expected_json.len - field.offset) return false;
            value = try std.json.parseFromSliceLeaky(std.json.Value, temp, after.expected_json[@intCast(field.offset)..][0..@intCast(field.length)], .{ .parse_numbers = false });
        }
        try shadow.object.put(temp, member.name, value);
    }
    try compiled.validateChangedRoots(temp, &shadow, &unchanged);
    return true;
}
