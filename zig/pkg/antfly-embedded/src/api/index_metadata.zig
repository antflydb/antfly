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

//! Control state stored beside index declarations is opaque to configuration
//! discovery, authorization and rewriting. Do not descend into evidence: its
//! user-defined keys can look exactly like live configuration.
const std = @import("std");

pub fn isOpaqueEntry(name: []const u8) bool {
    return std.mem.eql(u8, name, "_resolver_curation") or
        std.mem.eql(u8, name, "_resolver_namespaces") or
        std.mem.eql(u8, name, "_curation_proposals");
}
