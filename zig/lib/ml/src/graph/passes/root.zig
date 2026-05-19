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

pub const dce = @import("dce.zig");
pub const memory = @import("memory.zig");
pub const fuse = @import("fuse.zig");
pub const const_fold = @import("const_fold.zig");
pub const cse = @import("cse.zig");
pub const pipeline = @import("pipeline.zig");

test {
    _ = dce;
    _ = memory;
    _ = fuse;
    _ = const_fold;
    _ = cse;
    _ = pipeline;
}
