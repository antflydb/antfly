// Copyright 2026 Antfly, Inc.
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

/// Borrowed, allocation-free cancellation source for native JPEG-2000 work.
/// Decoders check it between bounded units such as codeblocks, tiles, and
/// component reconstruction stages.
pub const CancellationProbe = struct {
    context: ?*const anyopaque = null,
    is_cancelled_fn: ?*const fn (?*const anyopaque) bool = null,

    pub fn check(self: CancellationProbe) !void {
        if (self.is_cancelled_fn) |is_cancelled| if (is_cancelled(self.context)) return error.Canceled;
    }
};
