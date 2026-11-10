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

//! Pure delayed-document hydration values. Borrowed source windows expire at
//! the next lookup or explicit releasePage; consumers copy selected cells.
pub const SourceKind = enum { bytes, fields };
pub const FieldValue = struct { name: []const u8, value: []const u8 };
pub const Source = union(SourceKind) { bytes: []const u8, fields: []const FieldValue };
pub const Batch = struct { values: []const ?[]const u8, consumed: usize };
pub const ProjectedBatch = struct { values: []const Source, consumed: usize };
