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

//! Small libc `<stdlib.h>` shim. Zig 0.17 removed the `@cImport`/`@cInclude`
//! builtins, but the only thing the inference backends pulled `stdlib.h` in for
//! was `getenv`. `std.c.getenv` provides the same `extern "c"` declaration, so
//! call sites that used `@cImport(@cInclude("stdlib.h"))` import this instead.
//! Requires the consuming module to link libc.

const std = @import("std");

pub const getenv = std.c.getenv;
