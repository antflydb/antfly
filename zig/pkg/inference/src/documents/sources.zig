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

//! Source adapters retain immutable encoded sources and parse each document
//! once. Page analysis/render buffers live in a resettable per-page arena.
const std = @import("std");
const pdf = @import("antfly_pdf");
const image = @import("antfly_image");
const c = @import("contract.zig");
const ooxml = @import("ooxml.zig");
const slide_parts = @import("slides.zig");
const A = std.mem.Allocator;
pub const Input = @import("antfly_readers").DocumentSource;
// Structural parsing is bounded independently of the selected-page work budget.
pub const max_source_pages = 10000;
fn selectedPage(selection: ?[]const usize, number: usize) bool {
    const pages = selection orelse return true;
    return std.sort.binarySearch(usize, pages, number, struct {
        fn order(key: usize, value: usize) std.math.Order {
            return std.math.order(key, value);
        }
    }.order) != null;
}
pub const Slide = struct { path: []const u8, hidden: bool = false, notes: ?[]const u8 = null, inherited: []const c.InheritedPart = &.{} };
pub const Source = struct {
    input: Input,
    fingerprint: [64]u8,
    bytes: []const u8,
    page_selection: ?[]const usize = null,
    parsed: union(enum) { pdf: pdf.reader.Reader, image: void, slides: struct { archive: ooxml.Archive, slides: []const Slide } },
    pub fn init(a: A, input: Input, bytes: []const u8, limits: c.Limits, control: c.Control) !Source {
        try control.check();
        if (input.id.len == 0 or input.id.len > 256 or bytes.len > limits.max_source_bytes) return error.InvalidDocumentSource;
        const selection: ?[]const usize = if (input.pages) |pages| blk: {
            if (pages.len == 0) return error.InvalidDocumentSelection;
            if (pages.len > limits.max_pages) return error.DocumentPageLimitExceeded;
            const sorted = try a.dupe(usize, pages);
            std.mem.sort(usize, sorted, {}, std.sort.asc(usize));
            for (sorted, 0..) |number, i| {
                if (number == 0 or (i > 0 and sorted[i - 1] == number)) return error.InvalidDocumentSelection;
            }
            break :blk sorted;
        } else null;
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
        const fingerprint = std.fmt.bytesToHex(hash, .lower);
        var result: Source = undefined;
        if (std.mem.eql(u8, input.media_type, "application/pdf")) {
            if (!std.mem.startsWith(u8, bytes, "%PDF-")) return error.DocumentMediaTypeMismatch;
            const Probe = struct {
                fn cancelled(raw: ?*const anyopaque) bool {
                    const ctl: *const c.Control = @ptrCast(@alignCast(raw.?));
                    ctl.check() catch return true;
                    return false;
                }
            };
            const persistent_control = try a.create(c.Control);
            persistent_control.* = control;
            result = .{ .input = input, .bytes = bytes, .fingerprint = fingerprint, .parsed = .{ .pdf = try pdf.reader.Reader.initWithDecodeLimitsAndCancellation(a, bytes, .{}, .{ .context = persistent_control, .is_cancelled_fn = Probe.cancelled }) } };
        } else if (std.mem.eql(u8, input.media_type, "application/vnd.openxmlformats-officedocument.presentationml.presentation")) {
            const archive = try ooxml.Archive.initWithControl(a, bytes, limits.max_source_bytes, control);
            const presentation = try archive.get(a, "ppt/presentation.xml");
            const relationships = try archive.get(a, "ppt/_rels/presentation.xml.rels");
            var slides = std.ArrayList(Slide).empty;
            var inheritance = slide_parts.Cache{};
            var iter = ooxml.Xml{ .bytes = presentation };
            while (try iter.next()) |token| {
                try control.check();
                if (token != .tag or token.tag.closing or !std.mem.eql(u8, token.tag.local(), "sldId")) continue;
                const id = token.tag.attr("r:id") orelse return error.InvalidDocumentXml;
                const path = try relationship(a, relationships, id, "ppt/");
                if (slides.items.len >= max_source_pages) return error.DocumentPageLimitExceeded;
                const number = slides.items.len + 1;
                if (selection == null and number > limits.max_pages) return error.DocumentPageLimitExceeded;
                var slide = Slide{ .path = path };
                // Unselected slides keep their original indexes but do not
                // decompress XML, walk inheritance, or populate part caches.
                if (selectedPage(selection, number)) {
                    const xml = try archive.get(a, path);
                    var slide_iter = ooxml.Xml{ .bytes = xml };
                    while (try slide_iter.next()) |part| {
                        if (part == .tag and !part.tag.closing and std.mem.eql(u8, part.tag.local(), "sld")) {
                            if (part.tag.attr("show")) |show| slide.hidden = std.mem.eql(u8, show, "0") or std.mem.eql(u8, show, "false");
                            break;
                        }
                    }
                    slide.inherited = try inheritance.inherited(a, archive, path, xml);
                }
                try slides.append(a, slide);
            }
            if (slides.items.len == 0) return error.InvalidDocumentSource;
            result = .{ .input = input, .bytes = bytes, .fingerprint = fingerprint, .parsed = .{ .slides = .{ .archive = archive, .slides = try slides.toOwnedSlice(a) } } };
        } else if (std.mem.startsWith(u8, input.media_type, "image/")) {
            const expected = image.inferenceFormatForMimeEssence(input.media_type) orelse return error.UnsupportedDocumentFormat;
            if (image.detectFormat(bytes) != expected) return error.DocumentMediaTypeMismatch;
            const info = try image.inspectEncoded(bytes);
            if (info.width > limits.max_dimension or info.height > limits.max_dimension or try info.pixels() > limits.max_pixels) return error.DocumentPixelLimitExceeded;
            result = .{ .input = input, .bytes = bytes, .fingerprint = fingerprint, .parsed = .image };
        } else return error.UnsupportedDocumentFormat;
        errdefer result.deinit();
        result.page_selection = selection;
        const count = try result.pageCount();
        if (count > max_source_pages) return error.DocumentPageLimitExceeded;
        if (selection) |pages| {
            if (pages[pages.len - 1] > count) return error.InvalidDocumentSelection;
        } else if (count > limits.max_pages) return error.DocumentPageLimitExceeded;
        return result;
    }
    pub fn deinit(self: *Source) void {
        if (self.parsed == .pdf) self.parsed.pdf.deinit();
    }
    pub fn pageCount(self: *Source) !usize {
        return switch (self.parsed) {
            .pdf => |*p| try p.pageCount(),
            .slides => |s| s.slides.len,
            .image => 1,
        };
    }
    pub fn selected(self: Source, number: usize) bool {
        return selectedPage(self.page_selection, number);
    }

    pub fn analyze(self: *Source, a: A, number: usize, control: c.Control) !c.Page {
        try control.check();
        var page = c.Page{ .source_id = self.input.id, .number = number, .kind = .image };
        switch (self.parsed) {
            .pdf => |*p| {
                var analysis = try p.extractPageTextAnalysisAlloc(number);
                defer analysis.deinit(p.alloc);
                page.kind = .page;
                page.coordinate_space = .pdf_points;
                page.native_text = try a.dupe(u8, analysis.text);
                const regions = try a.alloc(c.Region, analysis.runs.len);
                for (regions, analysis.runs, 0..) |*region, run, i| region.* = .{ .id = i, .text = try a.dupe(u8, run.text), .bbox = .{ run.x, run.y, run.x + run.advance_width, run.y + run.font_size }, .origin = .native };
                page.regions = regions;
                if (analysis.has_unrendered_annotations) try @import("representation.zig").finding(a, &page, "pdf_annotation_rendering_unavailable", true);
                if (analysis.outline_fallback) try @import("representation.zig").finding(a, &page, "native_font_fallback", true);
                if (analysis.has_nontext_visual) try @import("representation.zig").finding(a, &page, "visual_content_requires_inspection", true);
            },
            .slides => |s| {
                if (number == 0 or number > s.slides.len) return error.InvalidDocumentSelection;
                const slide = s.slides[number - 1];
                page.kind = .slide;
                page.coordinate_space = .slide_emu;
                const xml = try s.archive.get(a, slide.path);
                page.native_text = try ooxml.text(a, xml);
                page.tables = try ooxml.tables(a, xml);
                // Native extraction alone cannot assert visual completeness.
                if (try ooxml.needsVisualInspection(xml))
                    page.findings = &.{"slide_visual_inspection_unavailable"};
                page.inherited_parts = slide.inherited;
                const ir = @import("representation.zig");
                for (slide.inherited) |part| {
                    if ((part.shapes_visible and part.shape_visual) or (part.background_visible and part.background_visual)) try ir.finding(a, &page, "slide_inherited_visual_inspection_unavailable", true);
                    if (part.conditional_content or (part.shapes_visible and (std.mem.trim(u8, part.text, " \t\r\n").len > 0 or part.tables.len > 0))) try ir.finding(a, &page, "slide_inherited_native_content_requires_composition", true);
                }
                if (slide.hidden and !self.input.include_hidden) return error.HiddenDocumentPage;
                if (self.input.include_notes) {
                    const slash = std.mem.lastIndexOfScalar(u8, slide.path, '/') orelse return error.InvalidDocumentSource;
                    const rel_path = try std.fmt.allocPrint(a, "{s}/_rels/{s}.rels", .{ slide.path[0..slash], slide.path[slash + 1 ..] });
                    if (s.archive.get(a, rel_path)) |rels| {
                        var iter = ooxml.Xml{ .bytes = rels };
                        while (try iter.next()) |token| {
                            if (token != .tag or token.tag.closing) continue;
                            const kind = token.tag.attr("Type") orelse continue;
                            if (!std.mem.endsWith(u8, kind, "/notesSlide")) continue;
                            if (token.tag.attr("TargetMode")) |mode| if (std.mem.eql(u8, mode, "External")) return error.ExternalDocumentPartDenied;
                            const target = token.tag.attr("Target") orelse return error.InvalidDocumentXml;
                            const path = try normalizedPart(a, slide.path[0 .. slash + 1], target);
                            const notes = try ooxml.text(a, try s.archive.get(a, path));
                            // Notes are separate findings/evidence, not visible OCR.
                            page.notes = notes;
                        }
                    } else |err| if (err != error.MissingDocumentPart) return err;
                }
            },
            .image => {},
        }
        return page;
    }
    pub fn render(self: *Source, a: A, number: usize, run: *c.Run) !struct { bytes: []const u8, mime_type: []const u8, width: u32, height: u32, effective_dpi: u16, raster_to_source: c.Transform } {
        try run.control.check();
        switch (self.parsed) {
            .pdf => |*p| {
                const request = pdf.PageRenderRequest{ .page_number = number, .max_pixels = run.limits.max_pixels, .max_dimension = @min(run.limits.max_dimension, 4096), .max_output_bytes = run.limits.max_source_bytes };
                const geometry = try pdf.planParsedPageRenderGeometry(p, request);
                try run.pixels(@as(u64, geometry.width) * geometry.height);
                const rendered = pdf.renderParsedPagePngAdaptiveAlloc(a, p, number, 150, run.limits.max_pixels, @min(run.limits.max_dimension, 4096)) catch |err| switch (err) {
                    error.UnsupportedPdfAnnotationRendering => return error.DocumentRendererUnavailable,
                    else => return err,
                };
                const page_box = try p.extractPageBox(number);
                const rotation = @mod((try p.extractPageRotation(number)) orelse 0, 360);
                const unit = 72.0 / @as(f64, @floatFromInt(rendered.effective_dpi));
                const width: f64 = @floatFromInt(if (rotation == 90 or rotation == 270) rendered.height else rendered.width);
                const height: f64 = @floatFromInt(if (rotation == 90 or rotation == 270) rendered.width else rendered.height);
                const transform: c.Transform = .{ .matrix = switch (rotation) {
                    0 => .{ unit, 0, page_box.min_x, 0, -unit, page_box.min_y + height * unit, 0, 0, 1 },
                    90 => .{ 0, unit, page_box.min_x, unit, 0, page_box.min_y, 0, 0, 1 },
                    180 => .{ -unit, 0, page_box.min_x + width * unit, 0, unit, page_box.min_y, 0, 0, 1 },
                    270 => .{ 0, -unit, page_box.min_x + width * unit, -unit, 0, page_box.min_y + height * unit, 0, 0, 1 },
                    else => return error.InvalidDocumentTransform,
                } };
                return .{ .raster_to_source = transform, .bytes = rendered.png, .mime_type = "image/png", .width = rendered.width, .height = rendered.height, .effective_dpi = rendered.effective_dpi };
            },
            .image => {
                const info = try image.inspectEncoded(self.bytes);
                try run.pixels(try info.pixels());
                return .{ .raster_to_source = .{}, .bytes = self.bytes, .mime_type = self.input.media_type, .width = info.width, .height = info.height, .effective_dpi = 0 };
            },
            .slides => return error.DocumentRendererUnavailable,
        }
    }
};

fn relationship(a: A, xml: []const u8, id: []const u8, base: []const u8) ![]const u8 {
    var iter = ooxml.Xml{ .bytes = xml };
    while (try iter.next()) |token| {
        if (token != .tag or token.tag.closing or !std.mem.eql(u8, token.tag.local(), "Relationship")) continue;
        if (!std.mem.eql(u8, token.tag.attr("Id") orelse continue, id)) continue;
        if (token.tag.attr("TargetMode")) |mode| if (std.mem.eql(u8, mode, "External")) return error.ExternalDocumentPartDenied;
        return normalizedPart(a, base, token.tag.attr("Target") orelse return error.InvalidDocumentXml);
    }
    return error.MissingDocumentPart;
}
const normalizedPart = slide_parts.normalizedPart;

test "document selections spend page budget only on selected PDF pages and slides" {
    inline for (.{ .{ @embedFile("testdata/two-pages.pdf"), "application/pdf" }, .{ @embedFile("testdata/ordered-slides.pptx"), "application/vnd.openxmlformats-officedocument.presentationml.presentation" }, .{ @embedFile("testdata/inherited-slides.pptx"), "application/vnd.openxmlformats-officedocument.presentationml.presentation" } }) |fixture| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var source = try Source.init(a, .{ .id = "source", .url = "", .media_type = fixture[1], .pages = &.{2} }, fixture[0], .{ .max_pages = 1 }, .{});
        defer source.deinit();
        try std.testing.expectEqual(@as(usize, 2), try source.pageCount());
        try std.testing.expect(!source.selected(1));
        try std.testing.expect(source.selected(2));
        const page = try source.analyze(a, 2, .{});
        try std.testing.expectEqual(@as(usize, 2), page.number);
        if (source.parsed == .slides) try std.testing.expectEqual(@as(usize, 0), source.parsed.slides.slides[0].inherited.len);
        try std.testing.expectError(error.DocumentPageLimitExceeded, Source.init(a, .{ .id = "source", .url = "", .media_type = fixture[1] }, fixture[0], .{ .max_pages = 1 }, .{}));
        try std.testing.expectError(error.DocumentPageLimitExceeded, Source.init(a, .{ .id = "source", .url = "", .media_type = fixture[1], .pages = &.{ 1, 2 } }, fixture[0], .{ .max_pages = 1 }, .{}));
        try std.testing.expectError(error.InvalidDocumentSelection, Source.init(a, .{ .id = "source", .url = "", .media_type = fixture[1], .pages = &.{3} }, fixture[0], .{ .max_pages = 1 }, .{}));
        try std.testing.expectError(error.InvalidDocumentSelection, Source.init(a, .{ .id = "source", .url = "", .media_type = fixture[1], .pages = &.{ 1, 1 } }, fixture[0], .{}, .{}));
    }
}
