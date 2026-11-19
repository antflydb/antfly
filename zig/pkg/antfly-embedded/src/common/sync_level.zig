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

const std = @import("std");

pub const SyncLevel = enum {
    propose,
    write,
    full_text,
    enrichments,
    full_index,
};

pub fn parsePublicSyncLevelText(text: []const u8) ?SyncLevel {
    if (std.mem.eql(u8, text, "propose")) return .propose;
    if (std.mem.eql(u8, text, "write")) return .write;
    if (std.mem.eql(u8, text, "full_text")) return .full_text;
    if (std.mem.eql(u8, text, "enrichments")) return .enrichments;
    if (std.mem.eql(u8, text, "full_index")) return .full_index;
    return null;
}

pub fn parsePublicSyncLevelJson(value: std.json.Value) ?SyncLevel {
    return switch (value) {
        .string => |text| parsePublicSyncLevelText(text),
        else => null,
    };
}

pub fn publicSyncLevelText(level: SyncLevel) []const u8 {
    return switch (level) {
        .propose => "propose",
        .write => "write",
        .full_text => "full_text",
        .enrichments => "enrichments",
        .full_index => "full_index",
    };
}
