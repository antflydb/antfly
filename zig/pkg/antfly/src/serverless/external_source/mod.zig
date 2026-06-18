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

pub const types = @import("types.zig");
pub const codec = @import("codec.zig");
pub const rowsource_bridge = @import("rowsource_bridge.zig");

pub const Format = types.Format;
pub const ColumnChunk = types.ColumnChunk;
pub const RowGroup = types.RowGroup;
pub const FileEntry = types.FileEntry;
pub const Inventory = types.Inventory;
pub const freeInventory = types.freeInventory;
pub const encodeInventoryAlloc = codec.encodeAlloc;
pub const decodeInventoryAlloc = codec.decodeAlloc;
pub const bindingFromInventory = rowsource_bridge.bindingFromInventory;
pub const rowRefForInventoryRow = rowsource_bridge.rowRefForInventoryRow;
pub const validateBatchAgainstInventory = rowsource_bridge.validateBatchAgainstInventory;

test "serverless external source module compiles" {
    _ = types;
    _ = codec;
    _ = rowsource_bridge;
    _ = Format;
    _ = ColumnChunk;
    _ = Inventory;
    _ = encodeInventoryAlloc;
    _ = decodeInventoryAlloc;
    _ = bindingFromInventory;
    _ = rowRefForInventoryRow;
    _ = validateBatchAgainstInventory;
}
