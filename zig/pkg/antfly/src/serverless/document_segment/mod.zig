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
