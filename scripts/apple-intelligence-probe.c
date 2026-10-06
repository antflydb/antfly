// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Compile this C driver, then link it with zig/lib/apple_native/src/bridge.swift
// using swiftc. Default invocation prints generation/speech availability.
// Alternatively compile with -DANTFLY_APPLE_HOSTED_PROBE and set the absolute
// ANTFLY_APPLE_HOST_LIBRARY path to exercise loading from the real libantfly.
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include "../zig/lib/apple_native/src/loader.h"
#ifdef ANTFLY_APPLE_HOSTED_PROBE
#include <dlfcn.h>
#else

extern int antfly_apple_invoke(int, const uint8_t *, size_t, const uint8_t *, size_t,
                             size_t, void *, int (*)(void *), int (*)(void *, const uint8_t *, size_t));
#endif
static int cancelled(void *context) { return time(NULL) >= *(time_t *)context; }
static int output(void *context, const uint8_t *bytes, size_t length) {
    (void)context;
    return fwrite(bytes, 1, length, stdout) == length ? 0 : 1;
}
int main(int argc, char **argv) {
    const int operation = argc > 1 ? atoi(argv[1]) : 3;
    const char *json = argc > 2 ? argv[2] : "{}";
    uint8_t *audio = NULL;
    size_t size = 0;
    if (argc > 3) {
        FILE *file = fopen(argv[3], "rb");
        if (!file) return 1;
        fseek(file, 0, SEEK_END);
        long length = ftell(file);
        rewind(file);
        if (length <= 0 || length > 128 * 1024 * 1024) { fclose(file); return 1; }
        size = (size_t)length;
        audio = malloc(size);
        if (!audio || fread(audio, 1, size, file) != size) { fclose(file); free(audio); return 1; }
        fclose(file);
    }
    time_t deadline = time(NULL) + 300;
    antfly_apple_invoke_fn invoke;
#ifdef ANTFLY_APPLE_HOSTED_PROBE
    const char *host_path = getenv("ANTFLY_APPLE_HOST_LIBRARY");
    void *host = host_path && host_path[0] == '/' ? dlopen(host_path, RTLD_NOW | RTLD_LOCAL) : NULL;
    invoke = host ? (antfly_apple_invoke_fn)dlsym(host, "antfly_apple_loader_invoke") : NULL;
    if (!invoke) { free(audio); fprintf(stderr, "unable to load Antfly host library\n"); return 1; }
#else
    invoke = antfly_apple_invoke;
#endif
    const int status = invoke(operation, (const uint8_t *)json, strlen(json), audio, size,
                                        8 * 1024 * 1024, &deadline, cancelled, output);
    free(audio);
    fprintf(stderr, "\nstatus=%d\n", status);
    return status == 0 ? 0 : 1;
}
