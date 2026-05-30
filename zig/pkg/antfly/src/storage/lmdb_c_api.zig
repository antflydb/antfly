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

// Zig 0.17 removed `@cImport`. The `lmdb_c_bindings` module is provided by the
// build system: an `addTranslateC` of `lmdb.h` for the C backend, or
// `lmdb_c_stub.zig` for the Zig backend / no-libc builds.
pub const Bindings = @import("lmdb_c_bindings");
