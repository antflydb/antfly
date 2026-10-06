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
const trace_file = @import("trace_file.zig");
const antfly_trace_writer = @import("antfly_trace_writer.zig");

/// A std.Io.Writer backed by libc write(2) to a trace output fd.
///
/// `ANTFLY_TRACE_DIR` selects an exclusive file per producer process.
/// Otherwise, when `ANTFLY_TRACE_FILE` is set, output goes to that file (truncated on
/// first open). Otherwise output goes to stderr (fd 2). The Zig test runner
/// reserves stdout for `--listen` IPC, so direct fd-level writes are used.
const trace_vtable: std.Io.Writer.VTable = .{
    .drain = drain,
    .flush = flush,
};

var trace_buf: [4096]u8 = undefined;

var trace_writer_instance: std.Io.Writer = .{
    .buffer = &trace_buf,
    .vtable = &trace_vtable,
};

var trace_fd: std.c.fd_t = -1;
var trace_mutex: std.atomic.Mutex = .unlocked;

fn getTraceFd() std.c.fd_t {
    if (trace_fd >= 0) return trace_fd;

    // A build target may run several test executables concurrently. Never
    // share a truncating descriptor between those producer processes.
    if (std.c.getenv("ANTFLY_TRACE_DIR")) |directory| {
        trace_fd = trace_file.openProcessFile(std.mem.span(directory), std.c.getpid()) catch
            @panic("cannot create process-owned trace file in ANTFLY_TRACE_DIR");
        return trace_fd;
    }

    // Check ANTFLY_TRACE_FILE environment variable
    const path = std.c.getenv("ANTFLY_TRACE_FILE");
    if (path != null) {
        const fd = std.c.open(path.?, .{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true }, @as(std.c.mode_t, 0o644));
        if (fd >= 0) {
            trace_fd = fd;
            return trace_fd;
        }
    }

    // Default to stderr
    trace_fd = std.posix.STDERR_FILENO;
    return trace_fd;
}

fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
    if (w.end > 0) {
        writeAllFd(w.buffer[0..w.end]);
        w.end = 0;
    }
    const pattern = data[data.len - 1];
    var written: usize = 0;
    for (data[0 .. data.len - 1]) |bytes| {
        writeAllFd(bytes);
        written += bytes.len;
    }
    writeAllFd(pattern);
    written += pattern.len;
    if (splat == 0) {
        // Pattern was written once but shouldn't have been; can't undo.
    } else {
        for (1..splat) |_| {
            writeAllFd(pattern);
            written += pattern.len;
        }
    }
    return written;
}

fn flush(w: *std.Io.Writer) std.Io.Writer.Error!void {
    if (w.end > 0) {
        writeAllFd(w.buffer[0..w.end]);
        w.end = 0;
    }
}

fn writeAllFd(data: []const u8) void {
    const fd = getTraceFd();
    var offset: usize = 0;
    while (offset < data.len) {
        const rc = std.c.write(fd, data[offset..].ptr, data.len - offset);
        if (rc < 0) return;
        offset += @intCast(rc);
    }
}

/// Module-level singleton trace writer for Antfly transaction events.
pub fn stderrAntflyTraceWriter() antfly_trace_writer.AntflyTraceWriter {
    const S = struct {
        var ndjson_writer: antfly_trace_writer.AntflyNdjsonTraceWriter = .{ .writer = &trace_writer_instance, .shared_mutex = &trace_mutex };
    };
    return S.ndjson_writer.traceWriter();
}

/// Borrowed process trace sink; adapters share the same output lock.
pub fn traceSink() struct { writer: *std.Io.Writer, mutex: *std.atomic.Mutex } {
    return .{ .writer = &trace_writer_instance, .mutex = &trace_mutex };
}
