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
const A = std.mem.Allocator;
pub const Input = @import("antfly_readers").DocumentSource;
pub const Slide = struct { path: []const u8, hidden: bool = false, notes: ?[]const u8 = null };
pub const Source = struct {
    input: Input,
    fingerprint: [64]u8,
    bytes: []const u8,
    parsed: union(enum) { pdf: pdf.reader.Reader, image: void, slides: struct { archive: ooxml.Archive, slides: []const Slide } },
    pub fn init(a: A, input: Input, bytes: []const u8, limits: c.Limits, control: c.Control) !Source {
        try control.check();
        if (input.id.len == 0 or input.id.len > 256 or bytes.len > limits.max_source_bytes) return error.InvalidDocumentSource;
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
            var iter = ooxml.Xml{ .bytes = presentation };
            while (try iter.next()) |token| {
                try control.check();
                if (token != .tag or token.tag.closing or !std.mem.eql(u8, token.tag.local(), "sldId")) continue;
                const id = token.tag.attr("r:id") orelse return error.InvalidDocumentXml;
                const path = try relationship(a, relationships, id, "ppt/");
                const xml = try archive.get(a, path);
                var slide_iter = ooxml.Xml{ .bytes = xml };
                var hidden = false;
                while (try slide_iter.next()) |part| {
                    if (part == .tag and !part.tag.closing and std.mem.eql(u8, part.tag.local(), "sld")) {
                        if (part.tag.attr("show")) |show| hidden = std.mem.eql(u8, show, "0") or std.mem.eql(u8, show, "false");
                        break;
                    }
                }
                if (slides.items.len >= limits.max_pages) return error.DocumentPageLimitExceeded;
                try slides.append(a, .{ .path = path, .hidden = hidden });
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
        const count = try result.pageCount();
        if (count > limits.max_pages) return error.DocumentPageLimitExceeded;
        if (input.pages) |selection| {
            if (selection.len == 0 or selection.len > limits.max_pages) return error.InvalidDocumentSelection;
            for (selection, 0..) |number, i| {
                if (number == 0 or number > count) return error.InvalidDocumentSelection;
                for (selection[0..i]) |prior| if (prior == number) return error.InvalidDocumentSelection;
            }
        }
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
        if (self.input.pages) |selection| {
            for (selection) |n| if (n == number) return true;
            return false;
        }
        return true;
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
                if (analysis.outline_fallback) page.findings = &.{"native_font_fallback"};
                const images = try p.extractPageImageRunsAlloc(number);
                defer {
                    for (images) |*item| item.deinit(p.alloc);
                    p.alloc.free(images);
                }
                if (images.len > 0) page.findings = &.{"visual_content_requires_inspection"};
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
                if (std.mem.indexOf(u8, xml, ":pic") != null or std.mem.indexOf(u8, xml, ":graphicFrame") != null)
                    page.findings = &.{"slide_visual_inspection_unavailable"};
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
                const rendered = try pdf.renderParsedPagePngAdaptiveAlloc(a, p, number, 150, run.limits.max_pixels, @min(run.limits.max_dimension, 4096));
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
fn normalizedPart(a: A, base: []const u8, target: []const u8) ![]const u8 {
    if (target.len == 0 or target[0] == '/' or std.mem.indexOfAny(u8, target, "\\:%?#") != null) return error.ExternalDocumentPartDenied;
    var parts = std.ArrayList([]const u8).empty;
    const combined = try std.mem.concat(a, u8, &.{ base, target });
    var iter = std.mem.splitScalar(u8, combined, '/');
    while (iter.next()) |part| {
        if (part.len == 0 or std.mem.eql(u8, part, ".")) continue;
        if (std.mem.eql(u8, part, "..")) {
            if (parts.items.len == 0) return error.ExternalDocumentPartDenied;
            _ = parts.pop();
        } else try parts.append(a, part);
    }
    const result = try std.mem.join(a, "/", parts.items);
    if (!std.mem.startsWith(u8, result, "ppt/")) return error.ExternalDocumentPartDenied;
    return result;
}
