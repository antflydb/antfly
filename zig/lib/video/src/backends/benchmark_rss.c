// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#include <sys/resource.h>
#include <stdint.h>
uint64_t av_benchmark_peak_rss(void) {
    struct rusage usage;
    if (getrusage(RUSAGE_SELF, &usage)) return 0;
#ifdef __APPLE__
    return (uint64_t)usage.ru_maxrss;
#else
    return (uint64_t)usage.ru_maxrss * 1024;
#endif
}
