// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Data-only callback contract for resolving semantic search text to a vector.

const std = @import("std");
const db_types = @import("../storage/db/types.zig");

pub const SemanticResolver = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        resolve_dense_query: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
            semantic_search: []const u8,
            embedding_template: ?[]const u8,
            limit: u32,
        ) anyerror!db_types.DenseKnnQuery,
    };

    pub fn resolveDenseQuery(
        self: SemanticResolver,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        semantic_search: []const u8,
        embedding_template: ?[]const u8,
        limit: u32,
    ) !db_types.DenseKnnQuery {
        return try self.vtable.resolve_dense_query(self.ptr, alloc, table_name, index_name, semantic_search, embedding_template, limit);
    }
};
