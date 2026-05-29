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

// Backwards-compatibility shim for the pre-rename `blas_compute` module.
//
// The native backend was renamed from `BlasCompute` to `NativeCompute` in
// commit cf90cb1 ("Rename native backend and extract linalg"). Several
// call sites still import by the old module path and type name. This file
// re-exports the new types under the old names so existing callers keep
// compiling without touching every downstream file. New code should import
// `src/ops/native_compute.zig` directly.

const native_compute = @import("native_compute.zig");

pub const BlasCompute = native_compute.NativeCompute;
pub const WeightStore = native_compute.WeightStore;
