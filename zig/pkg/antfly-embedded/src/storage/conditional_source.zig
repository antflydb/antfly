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

//! Bounded authenticated source metadata captured under a pinned read transaction.
const std = @import("std");
const A = std.mem.Allocator;
const projection = @import("document_projection.zig");
const revision = @import("document_mutation_revision.zig");
const identities_mod = @import("source_value_identity.zig");
const logical = @import("logical_source.zig");
pub const Authority = struct {
    source: logical.Authority = .{},
    certificate: ?identities_mod.Certificate = null,
    repair: bool = false,
};
/// At most 64 rows and 128 bounded metadata values. Decode once while the scope
/// is live; copy certificate records/directory names before further reads can
/// retire backend buffers. The owner must keep the pinned transaction alive.
pub const Window = struct {
    rows: [64]Authority = @splat(.{}),
    pub fn prepare(scratch: A, scope: anytype, physical: []const []const u8, ids: []const []const u8) !Window {
        if (physical.len > 64 or physical.len != ids.len) return error.InvalidBatch;
        var out: Window = .{};
        var proofs: [64]?revision.Content = @splat(null);
        var timestamps: [64]u64 = undefined;
        try revision.loadWindow(scope, scratch, physical, ids, proofs[0..ids.len], timestamps[0..ids.len]);
        const Entry = struct {
            key: []const u8,
            row: usize,
            certificate: bool,
            fn less(_: void, left: @This(), right: @This()) bool {
                return std.mem.order(u8, left.key, right.key) == .lt;
            }
        };
        var entries: [128]Entry = undefined;
        var directories: [64]projection.Key = undefined;
        var certificates: [64]identities_mod.Key = undefined;
        var count: usize = 0;
        for (proofs[0..ids.len], physical, 0..) |proof, key, i| {
            out.rows[i].source.proof = proof;
            const current = proof orelse {
                out.rows[i].repair = true;
                continue;
            };
            if (current.kind != @import("internal_keys.zig").primary_kind or current.bytes < projection.minimum_bytes) continue;
            const mutation = revision.keyForPhysical(key).?;
            directories[i] = projection.keyForRevision(mutation);
            certificates[i] = identities_mod.keyForRevision(mutation);
            entries[count] = .{ .key = &directories[i], .row = i, .certificate = false };
            entries[count + 1] = .{ .key = &certificates[i], .row = i, .certificate = true };
            count += 2;
        }
        std.mem.sort(Entry, entries[0..count], {}, Entry.less);
        var keys: [128][]const u8 = undefined;
        var values: [128]?[]const u8 = undefined;
        var slots: [128]usize = undefined;
        var distinct: usize = 0;
        for (entries[0..count], 0..) |entry, i| {
            if (distinct == 0 or !std.mem.eql(u8, keys[distinct - 1], entry.key)) {
                keys[distinct] = entry.key;
                distinct += 1;
            }
            slots[i] = distinct - 1;
        }
        if (distinct != 0) try scope.getManySorted(keys[0..distinct], values[0..distinct]);
        var raw_directories: [64]?[]const u8 = @splat(null);
        var raw_certificates: [64]?[]const u8 = @splat(null);
        for (entries[0..count], slots[0..count]) |entry, slot| {
            if (entry.certificate) raw_certificates[entry.row] = values[slot] else raw_directories[entry.row] = values[slot];
        }
        for (proofs[0..ids.len], 0..) |proof, i| {
            const current = proof orelse continue;
            if (current.kind != @import("internal_keys.zig").primary_kind or current.bytes < projection.minimum_bytes) continue;
            const raw = raw_directories[i] orelse {
                out.rows[i].repair = true;
                continue;
            };
            // Bounds are checked before copying even malformed metadata.
            if (raw.len > projection.maximum_bytes) {
                out.rows[i].repair = true;
                continue;
            }
            const owned = try scratch.dupe(u8, raw);
            const directory = (try projection.Directory.open(scratch, owned, current)) orelse {
                out.rows[i].repair = !try projection.isCurrent(scratch, owned, current);
                continue;
            };
            out.rows[i].source.directory = directory;
            const encoded = raw_certificates[i] orelse {
                out.rows[i].repair = true;
                continue;
            };
            const certificate = identities_mod.Certificate.open(encoded, owned, current) orelse {
                out.rows[i].repair = true;
                continue;
            };
            out.rows[i].repair = out.rows[i].repair or certificate.needsCoverageRepair(directory);
            out.rows[i].certificate = .{ .records = try scratch.dupe(u8, certificate.records), .opaque_coverage = certificate.opaque_coverage };
        }
        return out;
    }
};
