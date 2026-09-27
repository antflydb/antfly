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

pub const TestExecutionHook = struct {
    ptr: *anyopaque,
    run: *const fn (ptr: *anyopaque) void,
};

pub var test_before_batch_execution_hook: ?TestExecutionHook = null;

pub var test_before_native_backup_copy_hook: ?TestExecutionHook = null;

pub fn runTestBeforeBatchExecutionHook() void {
    if (comptime builtin.is_test) {
        if (test_before_batch_execution_hook) |hook| hook.run(hook.ptr);
    }
}

pub fn runTestBeforeNativeBackupCopyHook() void {
    if (comptime builtin.is_test) {
        if (test_before_native_backup_copy_hook) |hook| hook.run(hook.ptr);
    }
}
