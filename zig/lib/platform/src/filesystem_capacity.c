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

#include <stdint.h>
#include <sys/statvfs.h>

int antfly_platform_filesystem_capacity(
    const char *path,
    uint64_t *fragment_bytes,
    uint64_t *blocks,
    uint64_t *available_blocks
) {
    struct statvfs stat;
    if (statvfs(path, &stat) != 0) {
        return -1;
    }

    *fragment_bytes = (uint64_t)(stat.f_frsize != 0 ? stat.f_frsize : stat.f_bsize);
    *blocks = (uint64_t)stat.f_blocks;
    *available_blocks = (uint64_t)stat.f_bavail;
    return 0;
}
