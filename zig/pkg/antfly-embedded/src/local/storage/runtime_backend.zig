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

const builtin = @import("builtin");

pub const Backend = enum {
    manual,
    io_threaded,
};

pub fn defaultExecutorBackend() Backend {
    return if (builtin.os.tag == .freestanding) .manual else .io_threaded;
}

pub fn ensureExecutorBackendAvailable(backend: Backend) !void {
    if (builtin.os.tag == .freestanding and backend != .manual) {
        return error.UnsupportedPlatform;
    }
}

pub fn hasBackgroundWorkers(backend: Backend) bool {
    return backend != .manual;
}
