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

pub const Posting = types.Posting;
pub const DocumentEntry = types.DocumentEntry;
pub const TermEntry = types.TermEntry;
pub const Segment = types.Segment;
pub const freeSegment = types.freeSegment;
pub const encodeAlloc = codec.encodeAlloc;
pub const decodeAlloc = codec.decodeAlloc;

test "serverless text segment module compiles" {
    _ = types;
    _ = codec;
    _ = Posting;
    _ = DocumentEntry;
    _ = TermEntry;
    _ = Segment;
    _ = freeSegment;
    _ = encodeAlloc;
    _ = decodeAlloc;
}
