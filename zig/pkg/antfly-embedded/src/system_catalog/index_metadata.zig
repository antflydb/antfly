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

/// Shared classification for catalog identity, discovery and configuration.
/// Enrichment/resolver arrays and opaque evidence are never index objects.
pub fn isReservedEntry(name: []const u8) bool {
    return std.mem.eql(u8, name, "indexes") or std.mem.eql(u8, name, "resolvers") or std.mem.eql(u8, name, "enrichments") or isOpaqueEntry(name);
}

pub fn isOpaqueEntry(name: []const u8) bool {
    return std.mem.eql(u8, name, "_resolver_record_store") or std.mem.eql(u8, name, "_resolver_record_owners") or
        std.mem.eql(u8, name, "_resolver_curation") or
        std.mem.eql(u8, name, "_resolver_namespaces") or
        std.mem.eql(u8, name, "_curation_proposals");
}

/// A native index name is also a storage path component. SQL quoted names
/// must satisfy the same contract as native API names before publication.
pub fn isValidIndexName(name: []const u8) bool {
    return name.len != 0 and name.len <= 255 and std.unicode.utf8ValidateSlice(name) and
        std.mem.indexOfAny(u8, name, "/\\\x00") == null and !std.mem.eql(u8, name, ".") and
        !std.mem.eql(u8, name, "..") and !isReservedEntry(name);
}

test "native index names reject metadata slots and path traversal without restricting Unicode components" {
    for ([_][]const u8{ "indexes", "resolvers", "enrichments", "_resolver_record_store", "_curation_proposals", "../outside", "a/b", "a\\b", ".", "..", "", "a\x00b" }) |name| try std.testing.expect(!isValidIndexName(name));
    for ([_][]const u8{ "status_fts", "quoted index", "索引", "index.v2", "index-v3" }) |name| try std.testing.expect(isValidIndexName(name));
}
