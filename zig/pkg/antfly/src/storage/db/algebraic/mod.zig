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

pub const token = @import("token.zig");
pub const value = @import("value.zig");
pub const algebra = @import("algebra.zig");
pub const law = @import("law.zig");
pub const hll = @import("hll.zig");
pub const fact = @import("fact.zig");
pub const pathfact = @import("pathfact.zig");
pub const lexical = @import("lexical.zig");
pub const tensor = @import("tensor.zig");
pub const vector = @import("vector.zig");
pub const path = @import("path.zig");
pub const adaptive = @import("adaptive.zig");
pub const distributed = @import("distributed.zig");
pub const cylinder = @import("cylinder.zig");
pub const join = @import("join.zig");
pub const ir = @import("ir.zig");
pub const index = @import("index.zig");
pub const planner = @import("planner.zig");
pub const schema_capability = @import("schema_capability.zig");
pub const relational_row_codec = @import("relational_row_codec.zig");
pub const symbol = @import("symbol.zig");

test {
    _ = @import("ownership_test.zig");
    _ = token;
    _ = value;
    _ = algebra;
    _ = law;
    _ = hll;
    _ = fact;
    _ = pathfact;
    _ = lexical;
    _ = tensor;
    _ = vector;
    _ = path;
    _ = adaptive;
    _ = distributed;
    _ = cylinder;
    _ = join;
    _ = ir;
    _ = index;
    _ = planner;
    _ = schema_capability;
    _ = relational_row_codec;
    _ = symbol;
}
