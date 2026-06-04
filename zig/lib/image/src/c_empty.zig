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

//! Empty placeholder for the optional `spng_c` binding. Zig 0.17 removed
//! @cImport; the build wires `spng_c` to a translate-c module when libspng is
//! available, and to this empty struct otherwise. The spng decode paths in the
//! bench/corpus tools are gated behind build_options.enable_spng, so the empty
//! surface is never dereferenced.
