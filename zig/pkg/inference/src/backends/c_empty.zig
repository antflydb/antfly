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

//! Empty C-bindings placeholder. Zig 0.17 removed `@cImport`, so optional C
//! interop modules (`onnx_c`, `ortgenai_c`, `mlx_c`, `blas_c`) are wired by the
//! build system to a real `addTranslateC` module when the corresponding feature
//! is enabled, and to this empty struct otherwise. The importing `.zig` files
//! only dereference these decls in code paths gated behind the matching
//! `build_options.enable_*` flag, so the empty surface is never touched.
