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

//! Source binding metadata for serverless sidecar segments built over RowSource.

const std = @import("std");
const rowsource = @import("../../storage/rowsource/types.zig");

pub const SidecarKind = enum(u8) {
    text = 1,
    vector = 2,
    sparse = 3,
    graph = 4,
    algebraic = 5,
};

pub const RowRefKind = enum(u8) {
    relational_key = 1,
    serverless = 2,
    external = 3,
};

pub const Binding = struct {
    sidecar_kind: SidecarKind,
    source_kind: rowsource.SourceKind,
    row_ref_kind: RowRefKind,
    source_id: []const u8 = &.{},
    snapshot_id: []const u8,
    schema_fingerprint: []const u8,
    column_bindings: []const []const u8 = &.{},
    index_config_hash: []const u8,

    pub fn validate(self: Binding) !void {
        if (self.snapshot_id.len == 0) return error.InvalidSidecarSourceBinding;
        if (self.schema_fingerprint.len == 0) return error.InvalidSidecarSourceBinding;
        if (self.index_config_hash.len == 0) return error.InvalidSidecarSourceBinding;
        for (self.column_bindings) |column| {
            if (column.len == 0) return error.InvalidSidecarSourceBinding;
        }
        switch (self.source_kind) {
            .external_parquet, .external_iceberg, .external_lance => {
                if (self.row_ref_kind != .external) return error.InvalidSidecarSourceBinding;
                if (self.source_id.len == 0) return error.InvalidSidecarSourceBinding;
            },
            .serverless_fragment => {
                if (self.row_ref_kind != .serverless) return error.InvalidSidecarSourceBinding;
            },
            .relational_store, .json_materialized => {
                if (self.row_ref_kind != .relational_key) return error.InvalidSidecarSourceBinding;
            },
        }
    }
};

pub fn sameSourceSnapshot(a: Binding, b: Binding) bool {
    return a.source_kind == b.source_kind and
        a.row_ref_kind == b.row_ref_kind and
        std.mem.eql(u8, a.source_id, b.source_id) and
        std.mem.eql(u8, a.snapshot_id, b.snapshot_id) and
        std.mem.eql(u8, a.schema_fingerprint, b.schema_fingerprint);
}

test "sidecar source binding validates serverless and external sources" {
    const serverless_binding = Binding{
        .sidecar_kind = .text,
        .source_kind = .serverless_fragment,
        .row_ref_kind = .serverless,
        .snapshot_id = "manifest-1",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"body"},
        .index_config_hash = "sha256:text",
    };
    try serverless_binding.validate();

    const external_binding = Binding{
        .sidecar_kind = .vector,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-123",
        .schema_fingerprint = "schema-v2",
        .column_bindings = &[_][]const u8{"embedding"},
        .index_config_hash = "sha256:vector",
    };
    try external_binding.validate();
    try std.testing.expect(!sameSourceSnapshot(serverless_binding, external_binding));

    var invalid = external_binding;
    invalid.row_ref_kind = .serverless;
    try std.testing.expectError(error.InvalidSidecarSourceBinding, invalid.validate());

    const invalid_relational = Binding{
        .sidecar_kind = .algebraic,
        .source_kind = .relational_store,
        .row_ref_kind = .external,
        .snapshot_id = "relational-1",
        .schema_fingerprint = "schema-v3",
        .column_bindings = &[_][]const u8{"tenant"},
        .index_config_hash = "sha256:algebraic",
    };
    try std.testing.expectError(error.InvalidSidecarSourceBinding, invalid_relational.validate());
}
