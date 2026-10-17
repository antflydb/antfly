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
const ValueSpec = struct { name: []const u8, choices: []const u8 = "", path: bool = false, directory: bool = false };
const values = [_]ValueSpec{
    .{ .name = "--engine", .choices = "default auto pcre2" },
    .{ .name = "--decision-context", .choices = "legacy shared-v1" },
    .{ .name = "--backend", .choices = "http antfly typesafe local" },
    .{ .name = "--provider", .choices = "antfly typesafe vercel openrouter opencode local custom" },
    .{ .name = "--agent", .choices = "codex claude-code opencode" },
    .{ .name = "--color", .choices = "auto always never ansi" },
    .{ .name = "--sort", .choices = "none path modified accessed created" },
    .{ .name = "--sortr", .choices = "none path modified accessed created" },
    .{ .name = "--unit", .choices = "line block" },
    .{ .name = "--generate", .choices = "man complete-bash complete-zsh complete-fish complete-powershell" },
    .{ .name = "--encoding", .choices = "auto none utf-8 utf-16le utf-16be utf-32le utf-32be windows-1252 shift_jis euc-jp euc-kr gb18030 big5 iso-2022-jp" },
    .{ .name = "--file", .path = true },
    .{ .name = "--ignore-file", .path = true },
    .{ .name = "--library", .path = true },
    .{ .name = "--dictionary", .path = true },
    .{ .name = "--table-config", .path = true },
    .{ .name = "--table" },
    .{ .name = "--table-filter" },
    .{ .name = "--max-index-documents" },
    .{ .name = "--max-pending-output" },
    .{ .name = "--dictionary-format", .choices = "auto raw brotli brotli-google" },
    .{ .name = "--max-dictionary-size" },
    .{ .name = "--max-decompression-window" },
    .{ .name = "--pre", .path = true },
    .{ .name = "--models-dir", .directory = true },
    .{ .name = "--cache-ttl" },
    .{ .name = "--cache-dir", .directory = true },
};
fn spec(name: []const u8) ?ValueSpec {
    for (values) |value| if (std.mem.eql(u8, value.name, name)) return value;
    return null;
}
fn takesValue(name: []const u8) bool {
    if (spec(name) != null or std.mem.eql(u8, name, "--type") or std.mem.eql(u8, name, "--type-not") or std.mem.eql(u8, name, "--type-clear")) return true;
    const source = @embedFile("options.zig");
    const start = std.mem.indexOf(u8, source, "const value = inline_value").?;
    const end = std.mem.indexOfPos(u8, source, start, "var j: usize").?;
    var buffer: [256]u8 = undefined;
    const quoted = std.fmt.bufPrint(&buffer, "\"{s}\"", .{name}) catch return false;
    return std.mem.indexOf(u8, source[start..end], quoted) != null;
}
fn choices(a: std.mem.Allocator, name: []const u8) ![]u8 {
    if (std.mem.eql(u8, name, "--type") or std.mem.eql(u8, name, "--type-not") or std.mem.eql(u8, name, "--type-clear")) {
        var result: std.Io.Writer.Allocating = .init(a);
        errdefer result.deinit();
        for (@import("filetypes.zig").definitions) |definition| try result.writer.print("{s} ", .{definition.name});
        return result.toOwnedSlice();
    }
    return a.dupe(u8, if (spec(name)) |value| value.choices else "");
}

pub fn generate(a: std.mem.Allocator, kind: []const u8, help: []const u8) ![]u8 {
    var names: std.ArrayList([]const u8) = .empty;
    defer names.deinit(a);
    const source = @embedFile("options.zig");
    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, source, pos, "\"--")) |at| {
        const first = at + 1;
        const end = std.mem.indexOfScalarPos(u8, source, first, '"') orelse break;
        pos = end + 1;
        const name = source[first..end];
        if (name.len < 3 or std.mem.endsWith(u8, name, "-") or std.mem.indexOfScalar(u8, name, '=') != null) continue;
        var duplicate = false;
        for (names.items) |existing| if (std.mem.eql(u8, name, existing)) {
            duplicate = true;
            break;
        };
        if (!duplicate) try names.append(a, name);
    }
    for ([_][]const u8{ "--provider", "--agent", "--stdin", "--global", "--yes" }) |extra| try names.append(a, extra);
    std.mem.sort([]const u8, names.items, {}, struct {
        fn less(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.order(u8, x, y) == .lt;
        }
    }.less);
    var output: std.Io.Writer.Allocating = .init(a);
    errdefer output.deinit();
    if (std.mem.eql(u8, kind, "man")) {
        try output.writer.writeAll(".TH ANTFLY-GREP 1\n.SH NAME\nantfly-grep \\- native exact and decision source search\n.SH SYNOPSIS\n.nf\n");
        var lines = std.mem.splitScalar(u8, help, '\n');
        while (lines.next()) |line| {
            if (line.len > 0 and (line[0] == '.' or line[0] == '\'')) try output.writer.writeAll("\\&");
            for (line) |ch| {
                if (ch == '\\') try output.writer.writeAll("\\e") else try output.writer.writeByte(ch);
            }
            try output.writer.writeByte('\n');
        }
        try output.writer.writeAll(".fi\n.SH OPTIONS\n");
        for (names.items) |name| try output.writer.print(".TP\n.B {s}\nSee antfly-grep --help and the package README for semantics.\n", .{name});
    } else if (std.mem.eql(u8, kind, "complete-bash")) {
        try output.writer.writeAll("_antfly_grep() {\n local candidate cur=\"${COMP_WORDS[COMP_CWORD]}\" prev=\"${COMP_WORDS[COMP_CWORD-1]}\" prefix=''\n COMPREPLY=()\n if [[ $cur == --*=* ]]; then prev=${cur%%=*}; prefix=$prev=; cur=${cur#*=}; fi\n case $prev in\n");
        for (names.items) |name| if (takesValue(name)) {
            const words = try choices(a, name);
            defer a.free(words);
            const value = spec(name);
            try output.writer.print(" {s}) ", .{name});
            if (words.len > 0) try output.writer.print("COMPREPLY=( $(compgen -W '{s}' -- \"$cur\") )", .{words}) else if (value != null and value.?.directory) try output.writer.writeAll("while IFS= read -r candidate; do COMPREPLY+=(\"$candidate\"); done < <(compgen -d -- \"$cur\")") else if (value != null and value.?.path) try output.writer.writeAll("while IFS= read -r candidate; do COMPREPLY+=(\"$candidate\"); done < <(compgen -f -- \"$cur\")") else try output.writer.writeAll("COMPREPLY=()");
            try output.writer.writeAll("; if [[ -n $prefix ]]; then COMPREPLY=( \"${COMPREPLY[@]/#/$prefix}\" ); fi; return;;\n");
        };
        try output.writer.writeAll(" esac\n case ${COMP_WORDS[1]} in cache) COMPREPLY=( $(compgen -W 'clear' -- \"$cur\") ); return;; esac\n local opts='");
        for (names.items) |name| try output.writer.print("{s} ", .{name});
        try output.writer.writeAll("ask files auth doctor skill cache'\n if [[ $cur == -* || $COMP_CWORD == 1 ]]; then COMPREPLY=( $(compgen -W \"$opts\" -- \"$cur\") ); fi\n if [[ ${#COMPREPLY[@]} == 0 && $cur != -* ]]; then while IFS= read -r candidate; do COMPREPLY+=(\"$candidate\"); done < <(compgen -f -- \"$cur\"); fi\n}\ncomplete -o filenames -F _antfly_grep antfly-grep\n");
    } else if (std.mem.eql(u8, kind, "complete-zsh")) {
        try output.writer.writeAll("#compdef antfly-grep\n_arguments -s \\\n");
        for (names.items) |name| {
            const words = try choices(a, name);
            defer a.free(words);
            if (!takesValue(name)) try output.writer.print(" '{s}[{s}]' \\\n", .{ name, name[2..] }) else if (words.len > 0) try output.writer.print(" '{s}[{s}]:value:({s})' \\\n", .{ name, name[2..], words }) else if (spec(name)) |value| try output.writer.print(" '{s}[{s}]:value:{s}' \\\n", .{ name, name[2..], if (value.directory) "_files -/" else if (value.path) "_files" else "" }) else try output.writer.print(" '{s}[{s}]:value:' \\\n", .{ name, name[2..] });
        }
        try output.writer.writeAll(" '1:command or pattern:(ask files auth doctor skill cache)' '*:path:_files'\n");
    } else if (std.mem.eql(u8, kind, "complete-fish")) {
        try output.writer.writeAll("complete -c antfly-grep -n '__fish_use_subcommand' -a 'ask files auth doctor skill cache'\ncomplete -c antfly-grep -n '__fish_seen_subcommand_from cache' -a clear\n");
        for (names.items) |name| {
            try output.writer.print("complete -c antfly-grep -l {s}", .{name[2..]});
            if (takesValue(name)) {
                try output.writer.writeAll(" -r");
                const words = try choices(a, name);
                defer a.free(words);
                if (words.len > 0) try output.writer.print(" -f -a '{s}'", .{words}) else if (spec(name)) |value| {
                    if (value.directory) try output.writer.writeAll(" -a '(__fish_complete_directories)'");
                }
            }
            try output.writer.writeByte('\n');
        }
    } else if (std.mem.eql(u8, kind, "complete-powershell")) {
        try output.writer.writeAll("Register-ArgumentCompleter -Native -CommandName antfly-grep -ScriptBlock { param($wordToComplete, $commandAst, $cursorPosition)\n $elements=@($commandAst.CommandElements | Where-Object { $_.Extent.EndOffset -le $cursorPosition }); $previousIndex=$elements.Count-1; if($wordToComplete -ne ''){$previousIndex--}; $prev=if($previousIndex -ge 1){$elements[$previousIndex].Extent.Text}else{''}; $prefix=''\n if($wordToComplete -match '^(--[^=]+)=(.*)$'){$prev=$Matches[1];$prefix=$prev+'=';$wordToComplete=$Matches[2]}\n $choices=switch($prev){\n");
        for (names.items) |name| if (takesValue(name)) {
            const words = try choices(a, name);
            defer a.free(words);
            try output.writer.print(" '{s}' {{ '{s}'.Split(' ',[System.StringSplitOptions]::RemoveEmptyEntries); break }}\n", .{ name, words });
        };
        try output.writer.writeAll(" default { @(");
        for (names.items) |name| try output.writer.print("'{s}',", .{name});
        try output.writer.writeAll("'ask','files','auth','doctor','skill','cache') }\n }\n $choices | Where-Object { $_.StartsWith($wordToComplete,[System.StringComparison]::OrdinalIgnoreCase) } | ForEach-Object { [System.Management.Automation.CompletionResult]::new($prefix+$_,$_, 'ParameterValue',$_) }\n}\n");
    } else return error.UnknownGenerationKind;
    return output.toOwnedSlice();
}
