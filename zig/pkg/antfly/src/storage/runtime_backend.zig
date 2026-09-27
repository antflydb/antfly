// Copyright 2026 Antfly, Inc.
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
