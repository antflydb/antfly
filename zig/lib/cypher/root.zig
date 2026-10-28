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

pub const dependencies = @import("dependencies.zig");
pub const expressions = @import("expressions.zig");
pub const ast = @import("ast.zig");
pub const compiler = @import("compiler.zig");
pub const binder = @import("binder.zig");
pub const lexer = @import("lexer.zig");
pub const value = @import("value.zig");
pub const catalog = @import("catalog.zig");
pub const catalog_guard = @import("catalog_guard.zig");
pub const backend = @import("backend.zig");
pub const native = @import("native.zig");
pub const statistic = @import("statistic.zig");
pub const linear_numeric = @import("linear_numeric.zig");
pub const linear_bounds = @import("linear_bounds.zig");
pub const integer_affine = @import("integer_affine.zig");
pub const multiplicity = @import("multiplicity.zig");
pub const runtime = @import("runtime.zig");
pub const request = @import("request.zig");
pub const wire = @import("wire.zig");
pub const distributed = @import("distributed.zig");
pub const transaction = @import("transaction.zig");
pub const rpc = @import("rpc.zig");
pub const bounded_json = @import("bounded_json.zig");
pub const regex = @import("regex.zig");
pub const snapshot = @import("snapshot.zig");
pub const mutations = @import("mutations.zig");
test {
    _ = @import("fanout_test.zig");
    _ = catalog_guard;
    _ = dependencies;
    _ = runtime;
    _ = integer_affine;
    _ = @import("compiler_test.zig");
    _ = @import("catalog_test.zig");
    _ = @import("runtime_test.zig");
    _ = @import("unicode_string.zig");
    _ = @import("string_work.zig");
    _ = @import("numbers.zig");
    _ = @import("range_pipeline.zig");
    _ = @import("mutation_test.zig");
    _ = @import("native_test.zig");
    _ = @import("postimage_policy_test.zig");
    _ = @import("request.zig");
    _ = wire;
    _ = distributed;
    _ = rpc;
    _ = bounded_json;
    _ = regex;
}
