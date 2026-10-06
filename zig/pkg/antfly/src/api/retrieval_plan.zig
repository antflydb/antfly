// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Private execution plans. Public queries contain no navigation policy.
const std = @import("std");
const api = @import("antfly_metadata_openapi");

// Exactly one start source can be active. Keys are literal IDs; selectors
// retain ranked traversal's root/CSV/prior-result semantics.
pub const TreeStart = union(enum) {
    seed_results,
    key: []const u8,
    selector: []const u8,

    pub fn fromSelector(value: ?[]const u8) TreeStart {
        return if (value) |text| .{ .selector = text } else .seed_results;
    }

    pub fn literalKey(self: TreeStart) ?[]const u8 {
        return switch (self) {
            .key => |key| key,
            else => null,
        };
    }

    pub fn selectorText(self: TreeStart) ?[]const u8 {
        return switch (self) {
            .selector => |text| text,
            else => null,
        };
    }
};

pub const TreeSearchConfig = struct {
    index: []const u8,
    start: TreeStart = .seed_results,
    max_depth: ?i64 = null,
    beam_width: ?i64 = null,

    pub fn forBranch(self: TreeSearchConfig, key: []const u8, max_depth: i64) TreeSearchConfig {
        var branch = self;
        branch.start = .{ .key = key };
        branch.max_depth = max_depth;
        return branch;
    }
};

pub const Query = blk: {
    const base = @typeInfo(api.QueryRequest).@"struct";
    const extra = @typeInfo(struct { tree_search: ?TreeSearchConfig = null }).@"struct";
    break :blk @Struct(.auto, null, base.field_names ++ extra.field_names, base.field_types ++ extra.field_types, base.field_attrs ++ extra.field_attrs);
};

pub const Request = blk: {
    const original = @typeInfo(api.RetrievalAgentRequest).@"struct";
    var types: [original.field_types.len]type = undefined;
    @memcpy(&types, original.field_types);
    for (original.field_names, 0..) |name, index| {
        if (std.mem.eql(u8, name, "queries")) types[index] = []const Query;
    }
    break :blk @Struct(.auto, null, original.field_names, &types, original.field_attrs);
};

pub fn fromPublic(alloc: std.mem.Allocator, input: api.RetrievalAgentRequest) !Request {
    var result: Request = undefined;
    inline for (comptime std.meta.fieldNames(api.RetrievalAgentRequest)) |reflected_name| {
        if (comptime std.mem.eql(u8, reflected_name, "queries")) {
            const queries = try alloc.alloc(Query, input.queries.len);
            for (input.queries, queries) |source, *dest| {
                dest.* = .{};
                inline for (comptime std.meta.fieldNames(api.QueryRequest)) |qfield_name| @field(dest, qfield_name) = @field(source, qfield_name);
            }
            result.queries = queries;
        } else @field(result, reflected_name) = @field(input, reflected_name);
    }
    return result;
}
