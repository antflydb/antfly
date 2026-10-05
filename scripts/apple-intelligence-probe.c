// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Compile this C driver, then link it with zig/lib/apple_native/src/bridge.swift
// using swiftc. Default invocation prints generation/speech availability.
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

extern int antfly_apple_invoke(int, const uint8_t *, size_t, const uint8_t *, size_t,
                             size_t, void *, int (*)(void *), int (*)(void *, const uint8_t *, size_t));
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
    const int status = antfly_apple_invoke(operation, (const uint8_t *)json, strlen(json), audio, size,
                                        8 * 1024 * 1024, &deadline, cancelled, output);
    free(audio);
    fprintf(stderr, "\nstatus=%d\n", status);
    return status == 0 ? 0 : 1;
}
