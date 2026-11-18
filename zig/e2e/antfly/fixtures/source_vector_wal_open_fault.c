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

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int fail_wal_open(const char *path, int flags) {
    const char *arm = getenv("ANTFLY_TEST_WAL_OPEN_FAULT");
    size_t length = strlen(path);
    if (!arm || !(flags & O_CREAT) || (flags & O_TRUNC) ||
        !strstr(path, "/source-vectors/wal-") || length < 5 ||
        strcmp(path + length - 5, ".afvw") != 0 || unlink(arm) != 0) {
        return 0;
    }
    errno = EMFILE;
    return 1;
}

static int fault_open(const char *path, int flags, ...) {
    if (fail_wal_open(path, flags)) return -1;
    if (!(flags & O_CREAT)) return open(path, flags);
    va_list args;
    va_start(args, flags);
    int mode = va_arg(args, int);
    va_end(args);
    return open(path, flags, mode);
}

static int fault_openat(int dirfd, const char *path, int flags, ...) {
    if (fail_wal_open(path, flags)) return -1;
    if (!(flags & O_CREAT)) return openat(dirfd, path, flags);
    va_list args;
    va_start(args, flags);
    int mode = va_arg(args, int);
    va_end(args);
    return openat(dirfd, path, flags, mode);
}

__attribute__((used, section("__DATA,__interpose")))
static const struct { const void *replacement, *original; } interposers[] = {
    { (const void *)fault_open, (const void *)open },
    { (const void *)fault_openat, (const void *)openat },
};
