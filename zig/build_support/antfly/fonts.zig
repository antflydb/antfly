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

/// PDF products embed reviewed Apache-2.0 Roboto fonts, independently of server UI assets.
pub fn create(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.lang.Optimize) *std.Build.Module {
    const files = b.addWriteFiles();
    _ = files.addCopyFile(b.path("lib/pdf/fonts/roboto/Roboto-Regular.ttf"), "lib/pdf/fonts/roboto/Roboto-Regular.ttf");
    _ = files.addCopyFile(b.path("lib/pdf/fonts/roboto/Roboto-Bold.ttf"), "lib/pdf/fonts/roboto/Roboto-Bold.ttf");
    return b.createModule(.{
        .root_source_file = files.add("pdf_standard_fonts.zig", @embedFile("../../pdf_standard_fonts.zig")),
        .target = target,
        .optimize = optimize,
    });
}
