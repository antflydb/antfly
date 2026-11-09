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

#define _DARWIN_C_SOURCE 1
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sys/uio.h>
#include <time.h>

/* Diagnostic interposition only. No payloads or paths leave the process. */
static atomic_uint_fast64_t counters[2][3][6];
static atomic_uint_fast64_t foreground;
static const char *directory;
static size_t directory_len;
__attribute__((constructor)) static void initialize(void) {
    directory = getenv("ANTFLY_IO_PROFILE_DIR");
    directory_len = directory ? strlen(directory) : 0;
}
static int category(int fd) {
    if (!directory_len) return -1;
    char path[PATH_MAX];
    if (fcntl(fd, F_GETPATH, path) != 0 || strncmp(path, directory, directory_len) || path[directory_len] != '/') return -1;
    size_t n = strlen(path);
    if (n >= 4 && !strcmp(path+n-4, "-wal")) return 1;
    if ((n >= 7 && !strcmp(path+n-7, ".aflite")) || (n >= 3 && !strcmp(path+n-3, ".db"))) return 0;
    return 2;
}
static unsigned lane(void) {
    uint64_t id = 0; pthread_threadid_np(NULL, &id);
    return id == atomic_load_explicit(&foreground, memory_order_relaxed) ? 0 : 1;
}
static uint64_t now_ns(void) {
    struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000000000ull + (uint64_t)t.tv_nsec;
}
__attribute__((visibility("default"))) void af_trace_reset(void) {
    for (unsigned a=0;a<2;a++) for(unsigned b=0;b<3;b++) for(unsigned c=0;c<6;c++) atomic_store(&counters[a][b][c],0);
    uint64_t id=0; pthread_threadid_np(NULL,&id); atomic_store(&foreground,id);
}
__attribute__((visibility("default"))) void af_trace_read(uint64_t *output) {
    for (unsigned a=0;a<2;a++) for(unsigned b=0;b<3;b++) for(unsigned c=0;c<6;c++) *output++ = atomic_load(&counters[a][b][c]);
}
static int trace_fsync(int fd) {
    int cat=category(fd);uint64_t began=now_ns();int result=fsync(fd);
    if (cat>=0 && result==0) {unsigned a=lane();atomic_fetch_add(&counters[a][cat][0],1);atomic_fetch_add(&counters[a][cat][5],now_ns()-began);}
    return result;
}
static ssize_t trace_pwrite(int fd,const void *buffer,size_t length,off_t offset) {
    int cat=category(fd);ssize_t result=pwrite(fd,buffer,length,offset);
    if(cat>=0 && result>=0){unsigned a=lane();atomic_fetch_add(&counters[a][cat][1],1);atomic_fetch_add(&counters[a][cat][2],(uint64_t)result);}return result;
}
static ssize_t trace_pwritev(int fd,const struct iovec *vectors,int count,off_t offset) {
    int cat=category(fd);ssize_t result=pwritev(fd,vectors,count,offset);
    if(cat>=0 && result>=0){unsigned a=lane();atomic_fetch_add(&counters[a][cat][1],1);atomic_fetch_add(&counters[a][cat][2],(uint64_t)result);}return result;
}
static ssize_t trace_pread(int fd,void *buffer,size_t length,off_t offset) {
    int cat=category(fd);ssize_t result=pread(fd,buffer,length,offset);
    if(cat>=0 && result>=0){unsigned a=lane();atomic_fetch_add(&counters[a][cat][3],1);atomic_fetch_add(&counters[a][cat][4],(uint64_t)result);}return result;
}
static ssize_t trace_preadv(int fd,const struct iovec *vectors,int count,off_t offset) {
    int cat=category(fd);ssize_t result=preadv(fd,vectors,count,offset);
    if(cat>=0 && result>=0){unsigned a=lane();atomic_fetch_add(&counters[a][cat][3],1);atomic_fetch_add(&counters[a][cat][4],(uint64_t)result);}return result;
}
#define INTERPOSE(replacement,original) \
 __attribute__((used)) static const struct{const void *replace;const void *with;} pair_##original \
 __attribute__((section("__DATA,__interpose")))={(const void *)(uintptr_t)&replacement,(const void *)(uintptr_t)&original};
INTERPOSE(trace_fsync,fsync)
INTERPOSE(trace_pwrite,pwrite)
INTERPOSE(trace_pwritev,pwritev)
INTERPOSE(trace_pread,pread)
INTERPOSE(trace_preadv,preadv)
