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

//! Public surface of `lib/ml/tabular`. Reusable, allocator-driven, free of
//! I/O — safe to import from antfly DB indexes / enrichers and from
//! pkg/inference's tabular registry.

pub const ir = @import("ir.zig");
pub const loader = @import("loader.zig");
pub const activation = @import("activation.zig");
pub const tree_engine = @import("tree_engine.zig");
pub const linear_engine = @import("linear_engine.zig");
pub const svm_engine = @import("svm_engine.zig");
pub const preprocess = @import("preprocess.zig");
pub const engine = @import("engine.zig");
pub const optimizer = @import("optimizer/root.zig");
pub const convert = @import("convert/root.zig");

pub const Predictor = engine.Predictor;
pub const TabularModel = ir.TabularModel;

test {
    _ = ir;
    _ = loader;
    _ = activation;
    _ = tree_engine;
    _ = linear_engine;
    _ = svm_engine;
    _ = preprocess;
    _ = engine;
    _ = optimizer;
    _ = convert;
}
