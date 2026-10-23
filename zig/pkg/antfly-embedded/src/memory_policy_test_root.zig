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

//! Versioned memory contracts. No server, model runtime, or extension imports.
test {
    _ = @import("memory/service_test.zig");
    _ = @import("memory/schema.zig");
    _ = @import("memory/recall_windows.zig");
    _ = @import("memory/recall_filters.zig");
    _ = @import("memory/secret_screening.zig");
    _ = @import("memory/persistent_index.zig");
    _ = @import("memory/evaluation.zig");
}
