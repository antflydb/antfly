// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Sticky reader refusal, owned by the same Source accepting transaction as
//! its image/marker. A surviving Source image is never invisible to an old
//! reader merely because ordinary transaction records have been collected.
const std = @import("std");
const catalog = @import("table_catalog.zig");
pub fn stage(a: std.mem.Allocator, txn: anytype, fallback: ?catalog.Catalog) !catalog.Catalog {
    const raw = txn.get(catalog.key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    var current = if (raw) |bytes| try catalog.Catalog.decode(bytes) else fallback orelse return error.CatalogSourceUntracked;
    if (current.logical_capture_format >= catalog.replication_source_apply_format) return current;
    current.logical_capture_format = catalog.replication_source_apply_format;
    const encoded = try current.encodeDurable();
    // Batches may borrow put values until commit. Own this beyond callback
    // return, through the caller's already bounded accepting region.
    try txn.put(catalog.key, try a.dupe(u8, encoded.slice()));
    return current;
}
