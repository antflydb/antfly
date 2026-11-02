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

//! Compact, read-only acceptance observations for owning-table retirement.
//! These identify durable effects; they grant no owner execution authority.
const std = @import("std");
const root = @import("table_retirement_ledger.zig");

pub const Request = struct {
    lookup: root.Lookup,
    effect: union(enum) {
        admission: struct { incarnation: [32]u8, effects: @import("table_retirement_manifest.zig").Reference, scope: root.Scope },
        cancellation: u64,
        physical_plan: [32]u8,
        owner: struct { ordinal: u16, step: root.physical.Step, evidence: [32]u8 },
        publication: u64,
    },
    pub fn fromInput(input: *const root.Input) !@This() {
        const value: @This() = switch (input.command) {
            .begin => .{ .lookup = input.prepared.?.head.lookup, .effect = .{ .admission = .{ .incarnation = input.prepared.?.head.incarnation, .effects = input.prepared.?.head.effects, .scope = input.prepared.?.head.scope } } },
            .cancel => |r| .{ .lookup = r.lookup, .effect = .{ .cancellation = r.expected_revision } },
            .seal => |r| .{ .lookup = r.lookup, .effect = .{ .physical_plan = input.prepared_physical.?.summary.plan_digest } },
            .ack => |r| .{ .lookup = r.lookup, .effect = .{ .owner = .{ .ordinal = r.ordinal, .step = r.step, .evidence = r.evidence.digest() } } },
            .publish => |r| .{ .lookup = r.lookup, .effect = .{ .publication = r.expected_revision } },
        };
        try value.validate();
        return value;
    }
    pub fn validate(self: @This()) !void {
        try self.lookup.validate();
        switch (self.effect) {
            .admission => |value| {
                try value.effects.validate();
                try value.scope.validate();
                if (value.effects.root_table != self.lookup.table or std.mem.allEqual(u8, &value.incarnation, 0)) return error.InvalidCatalogRecord;
            },
            .cancellation, .publication => |revision| if (revision == 0 or revision == std.math.maxInt(u64)) return error.InvalidCatalogRecord,
            .physical_plan => |digest| if (std.mem.allEqual(u8, &digest, 0)) return error.InvalidCatalogRecord,
            .owner => |owner| if (owner.ordinal >= @import("table_retirement_manifest.zig").max_ranges or std.mem.allEqual(u8, &owner.evidence, 0)) return error.InvalidCatalogRecord,
        }
    }
};

pub const Observation = struct {
    request: Request,
    head: ?root.Head,
    accepted: bool,
    pub fn requireFor(self: @This(), expected: Request) !void {
        try expected.validate();
        try self.request.validate();
        if (!std.meta.eql(expected, self.request)) return error.InvalidCatalogRecord;
        if (self.head) |value| {
            try value.validate();
            if (!std.meta.eql(value.lookup, expected.lookup)) return error.InvalidCatalogRecord;
        } else if (self.accepted) return error.InvalidCatalogRecord;
    }
};

/// One head and at most one fixed owner receipt. Neither terminal plan history
/// nor the manifest is decoded to resolve ambiguous admission/ACK delivery.
pub fn observe(reader: anytype, request: Request) !Observation {
    try request.validate();
    const current = try root.head(reader, request.lookup);
    var value: Observation = .{ .request = request, .head = current, .accepted = false };
    const head = current orelse return value;
    value.accepted = switch (request.effect) {
        .admission => |expected| std.mem.eql(u8, &head.incarnation, &expected.incarnation) and std.meta.eql(head.effects, expected.effects) and std.meta.eql(head.scope, expected.scope),
        .cancellation => |revision| head.phase == .canceled and head.revision == revision + 1,
        .physical_plan => |digest| if (head.physical) |physical| std.mem.eql(u8, &physical.summary.plan_digest, &digest) else false,
        .publication => |revision| head.phase == .published and head.direct_catalog_revision != 0 and head.revision == revision + 1,
        .owner => |expected| blk: {
            var buffer: [256]u8 = undefined;
            const key = try root.physical.key(&buffer, request.lookup, .receipt, expected.ordinal, expected.step);
            const bytes = reader.get(key) catch |err| {
                if (err == error.NotFound) break :blk false;
                return err;
            };
            const accepted = try root.physical.ReceiptRecord.decode(bytes);
            if (!std.meta.eql(accepted.root, request.lookup) or accepted.ordinal != expected.ordinal or accepted.step != expected.step) return error.InvalidCatalogRecord;
            break :blk std.mem.eql(u8, &accepted.evidence.digest(), &expected.evidence);
        },
    };
    try value.requireFor(request);
    return value;
}
