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

const std = @import("std");
const graph_filter = @import("../storage/db/query/graph_exec.zig");

const Allocator = std.mem.Allocator;

// Row authorization and replication routing deliberately use the storage
// engine's filter compiler. Keeping one executable grammar prevents policies
// from being admitted by a lightweight SDK/server dialect and rejected later
// on a protected read.
pub const PreparedPatternFilter = graph_filter.PreparedPatternFilter;

pub fn storedDocMatchesPatternFilter(
    alloc: Allocator,
    key: []const u8,
    stored: []const u8,
    filter_query_json: []const u8,
) !bool {
    return try graph_filter.storedDocMatchesPatternFilter(
        alloc,
        key,
        stored,
        filter_query_json,
    );
}

pub fn jsonDocMatchesPatternFilter(
    alloc: Allocator,
    key: []const u8,
    doc: std.json.Value,
    filter_query: std.json.Value,
) !bool {
    return try graph_filter.jsonDocMatchesPatternFilter(
        alloc,
        key,
        doc,
        filter_query,
    );
}

test "stored row filters use the canonical storage compiler" {
    const alloc = std.testing.allocator;
    const stored = "{\"tenant\":\"acme\",\"tier\":\"gold\",\"region\":\"west\",\"acl\":{\"roles\":[\"group:eng\",\"role:tenant_reader\"]}}";

    try std.testing.expect(try storedDocMatchesPatternFilter(
        alloc,
        "doc:gold",
        stored,
        "{\"term\":{\"tier\":\"gold\"}}",
    ));
    try std.testing.expect(try storedDocMatchesPatternFilter(
        alloc,
        "doc:gold",
        stored,
        "{\"terms\":{\"acl.roles\":[\"role:tenant_reader\",\"role:admin\"]}}",
    ));
    try std.testing.expect(try storedDocMatchesPatternFilter(
        alloc,
        "doc:gold",
        stored,
        "{\"conjuncts\":[{\"doc_id\":{\"ids\":[\"doc:gold\"]}},{\"term\":{\"tier\":\"gold\"}}]}",
    ));
    try std.testing.expect(try storedDocMatchesPatternFilter(
        alloc,
        "doc:gold",
        stored,
        "{\"bool\":{\"should\":[{\"term\":{\"tier\":\"gold\"}},{\"term\":{\"region\":\"west\"}},{\"term\":{\"tenant\":\"other\"}}],\"minimum_should_match\":2}}",
    ));
    try std.testing.expectError(
        error.InvalidRegex,
        storedDocMatchesPatternFilter(
            alloc,
            "doc:gold",
            stored,
            "{\"bool\":{\"must\":[{\"term\":{\"tenant\":\"acme\"}}],\"should\":[{\"regexp\":{\"tier\":\"[\"}}],\"minimum_should_match\":0}}",
        ),
    );
    try std.testing.expectError(
        error.UnsupportedQueryRequest,
        storedDocMatchesPatternFilter(
            alloc,
            "doc:gold",
            stored,
            "{\"match\":{\"tier\":\"gold\"}}",
        ),
    );
}
