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
    const module = b.createModule(.{
        .root_source_file = files.add("pdf_standard_fonts.zig", @embedFile("../../pdf_standard_fonts.zig")),
        .target = target,
        .optimize = optimize,
    });
    // Native products have several LLVM owners. An authored module alone
    // does not prevent each owner from emitting its embedded font constants.
    // Give these immutable assets one PIC object and reference its symbols.
    if (target.result.os.tag == .macos or target.result.os.tag == .linux) {
        const owner = b.createModule(.{
            .root_source_file = files.add("pdf_font_assets.zig",
                \\const regular = @embedFile("lib/pdf/fonts/roboto/Roboto-Regular.ttf").*;
                \\const bold = @embedFile("lib/pdf/fonts/roboto/Roboto-Bold.ttf").*;
                \\comptime {
                \\    @export(&regular, .{ .name = "antfly_pdf_regular_font_v1", .visibility = .hidden });
                \\    @export(&bold, .{ .name = "antfly_pdf_bold_font_v1", .visibility = .hidden });
                \\}
            ),
            .target = target,
            .optimize = optimize,
            .pic = true,
        });
        const library = b.addLibrary(.{ .name = "antfly-pdf-font-assets", .linkage = .static, .root_module = owner, .use_llvm = @import("../embedded/shared_library.zig").llvmBackend(target.result), .max_rss = 256 * 1024 * 1024 });
        library.link_data_sections = true;
        module.root_source_file = files.add("pdf_standard_fonts_shared.zig", b.fmt(
            \\extern const antfly_pdf_regular_font_v1: [{d}:0]u8;
            \\extern const antfly_pdf_bold_font_v1: [{d}:0]u8;
            \\pub const regular: []const u8 = &antfly_pdf_regular_font_v1;
            \\pub const bold: []const u8 = &antfly_pdf_bold_font_v1;
        , .{ @embedFile("../../lib/pdf/fonts/roboto/Roboto-Regular.ttf").len, @embedFile("../../lib/pdf/fonts/roboto/Roboto-Bold.ttf").len }));
        module.linkLibrary(library);
    }
    return module;
}
