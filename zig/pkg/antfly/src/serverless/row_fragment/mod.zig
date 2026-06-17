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

pub const ColumnKind = types.ColumnKind;
pub const CellValue = types.CellValue;
pub const RowRef = types.RowRef;
pub const Column = types.Column;
pub const Fragment = types.Fragment;
pub const freeFragment = types.freeFragment;
pub const encodeAlloc = codec.encodeAlloc;
pub const decodeAlloc = codec.decodeAlloc;

test "serverless row fragment module compiles" {
    _ = types;
    _ = codec;
    _ = Fragment;
    _ = encodeAlloc;
    _ = decodeAlloc;
}
