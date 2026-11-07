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

//! Source-compatible request and ranked-batch values. No physical Reader state.
const std = @import("std");
const Allocator = std.mem.Allocator;
const tuples = @import("db/relational_index_keys.zig");
const predicates = @import("db/relational_predicate.zig");
const codec = @import("db/algebraic/relational_row_codec.zig");
pub const TypedLayout = @import("../sql/catalog.zig").Row.TypedLayout;
pub const OrdinalRowView = codec.OrdinalRowView;
pub const Hydrated = struct { rows: []const ?@import("read_session_values.zig").Row, consumed: usize, output_bytes: usize };

pub const Value = tuples.Value;
pub const Condition = predicates.Condition;
/// Bounds use index order, including descending components. A partial tuple
/// denotes the whole matching left prefix, not an implicit NULL suffix.
pub const Bound = struct { values: []const Value, inclusive: bool = true };
pub const PrimaryBound = struct { key: []const u8, inclusive: bool = true };
/// Internal authorization/legacy-filter adapter. Context is borrowed for the
/// reader's lifetime; it sees the verified full typed row before projection.
pub const RowFilter = struct {
    context: *anyopaque,
    matches: *const fn (*anyopaque, Allocator, []const u8, codec.OrdinalRowView) anyerror!bool,
    callback_dispatch: @import("named_callback.zig").Dispatch = NativeBoundary.local_dispatch,
    const NativeCallbacks = struct { matches: @FieldType(RowFilter, "matches") };
    const NativeBoundary = @import("named_callback.zig").Boundary(NativeCallbacks);
    pub fn matchesRow(self: RowFilter, alloc: Allocator, key: []const u8, row: codec.OrdinalRowView) !bool {
        return NativeBoundary.call("matches", self.callback_dispatch, self.matches, .{ self.context, alloc, key, row });
    }
};
pub const Request = struct {
    include_primary_digest: bool = false,
    /// Stateful internal readers already retain a cursor; public stateless
    /// pages opt in to owned external continuation tokens.
    include_cursor: bool = false,
    /// Null selects primary-key order. Named indexes must have a durable
    /// ready proof for this exact generation and owned range.
    index: ?[]const u8 = null,
    /// Bounded planner hint for equality prefixes, range suffixes and proven partial indexes.
    /// Callers requiring primary-key order must leave this disabled.
    auto_index: bool = false,
    after: ?[]const u8 = null,
    lower: ?Bound = null,
    upper: ?Bound = null,
    primary_lower: ?PrimaryBound = null,
    primary_upper: ?PrimaryBound = null,
    expected_schema_version: ?u32 = null,
    row_filter: ?RowFilter = null,
    /// Explicit column names. Empty means an empty projection, not SELECT *.
    fields: []const []const u8 = &.{},
    /// Conjunction of typed comparisons; SQL UNKNOWN does not match WHERE.
    conditions: []const Condition = &.{},
};
