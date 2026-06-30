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

const common_json_helpers = @import("../common/json_helpers.zig");

pub const Allocator = common_json_helpers.Allocator;
pub const ParsedJsonPathValue = common_json_helpers.ParsedJsonPathValue;

pub const parseJsonValueAlloc = common_json_helpers.parseJsonValueAlloc;
pub const parseJsonObjectAlloc = common_json_helpers.parseJsonObjectAlloc;
pub const parseJsonPathValueAlloc = common_json_helpers.parseJsonPathValueAlloc;
pub const parseOwnedJsonValueAlloc = common_json_helpers.parseOwnedJsonValueAlloc;
pub const parseOwnedJsonValueAllocAlways = common_json_helpers.parseOwnedJsonValueAllocAlways;
pub const parseOwnedJsonObjectMapAlloc = common_json_helpers.parseOwnedJsonObjectMapAlloc;
pub const stringifyJsonValueAlloc = common_json_helpers.stringifyJsonValueAlloc;
pub const scalarJsonValueStringAlloc = common_json_helpers.scalarJsonValueStringAlloc;
pub const extractJsonPathValue = common_json_helpers.extractJsonPathValue;
pub const cloneJsonValue = common_json_helpers.cloneJsonValue;
pub const deinitJsonValue = common_json_helpers.deinitJsonValue;
pub const jsonValuesEqual = common_json_helpers.jsonValuesEqual;
