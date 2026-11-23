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
