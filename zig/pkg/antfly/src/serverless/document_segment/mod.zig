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

pub const Entry = types.Entry;
pub const IndexEntry = types.IndexEntry;
pub const freeEntries = types.freeEntries;
pub const freeIndexEntries = types.freeIndexEntries;
pub const encodeAlloc = codec.encodeAlloc;
pub const decodeAlloc = codec.decodeAlloc;
pub const header_len = codec.header_len;
pub const Header = codec.Header;
pub const decodeHeader = codec.decodeHeader;
pub const decodeIndexAlloc = codec.decodeIndexAlloc;

test "serverless document segment module compiles" {
    _ = types;
    _ = codec;
    _ = Entry;
    _ = IndexEntry;
    _ = freeEntries;
    _ = freeIndexEntries;
    _ = encodeAlloc;
    _ = decodeAlloc;
    _ = decodeHeader;
    _ = decodeIndexAlloc;
}
