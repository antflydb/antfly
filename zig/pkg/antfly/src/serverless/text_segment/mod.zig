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

pub const Posting = types.Posting;
pub const DocumentEntry = types.DocumentEntry;
pub const TermEntry = types.TermEntry;
pub const Segment = types.Segment;
pub const freeSegment = types.freeSegment;
pub const encodeAlloc = codec.encodeAlloc;
pub const encodedSize = codec.encodedSize;
pub const decodeAlloc = codec.decodeAlloc;
pub const decodeAllocWithLimits = codec.decodeAllocWithLimits;

test "serverless text segment module compiles" {
    _ = types;
    _ = codec;
    _ = Posting;
    _ = DocumentEntry;
    _ = TermEntry;
    _ = Segment;
    _ = freeSegment;
    _ = encodeAlloc;
    _ = encodedSize;
    _ = decodeAlloc;
    _ = decodeAllocWithLimits;
}
