// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at https://www.antfly.io/licensing/ELv2-license.

//! Wire-owned payload for creating a table. Parsing and validation stay in
//! tables.zig; the runtime callback boundary depends only on this data shape.

const std = @import("std");

pub const CreateTableRequest = struct {
    num_shards: ?u32 = null,
    description: ?[]u8 = null,
    indexes_json: ?[]u8 = null,
    schema_json: ?[]u8 = null,
    replication_sources_json: ?[]u8 = null,

    pub fn deinit(self: *CreateTableRequest, alloc: std.mem.Allocator) void {
        if (self.description) |value| alloc.free(value);
        if (self.indexes_json) |value| alloc.free(value);
        if (self.schema_json) |value| alloc.free(value);
        if (self.replication_sources_json) |value| alloc.free(value);
        self.* = undefined;
    }
};
