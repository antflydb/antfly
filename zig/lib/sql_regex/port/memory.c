/* Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0 */
#include <stddef.h>
extern int antfly_regex_work(size_t amount);

/* Compile with -ffreestanding -fno-builtin: these routines must never be
 * rewritten into recursive calls to a host libc symbol. */
void *antfly_regex_copy(void *dst, const void *src, size_t n) {
    unsigned char *out = (unsigned char *)dst;
    const unsigned char *in = (const unsigned char *)src;
    for (size_t i = 0; i < n; ++i) out[i] = in[i];
    return dst;
}
void *antfly_regex_set(void *dst, int value, size_t n) {
    unsigned char *out = (unsigned char *)dst;
    for (size_t i = 0; i < n; ++i) out[i] = (unsigned char)value;
    return dst;
}
int antfly_regex_compare(const void *a, const void *b, size_t n) {
    const unsigned char *left = (const unsigned char *)a;
    const unsigned char *right = (const unsigned char *)b;
    for (size_t i = 0; i < n; ++i) if (left[i] != right[i]) return left[i] < right[i] ? -1 : 1;
    return 0;
}
size_t antfly_regex_length(const char *text) {
    size_t n = 0;
    while (text[n]) ++n;
    return n;
}
char *antfly_regex_find_char(const char *text, int character) {
    do { if ((unsigned char)*text == (unsigned char)character) return (char *)text; } while (*text++);
    return NULL;
}
static int exchange(unsigned char *a, unsigned char *b, size_t width) {
    if (!antfly_regex_work(width)) return 0;
    for (size_t i = 0; i < width; ++i) { unsigned char temp = a[i]; a[i] = b[i]; b[i] = temp; }
    return 1;
}
static int sift(unsigned char *base, size_t count, size_t root, size_t width, int (*compare)(const void *, const void *)) {
    while (root < count / 2) {
        if (!antfly_regex_work(2)) return 0;
        size_t child = root * 2 + 1;
        if (child + 1 < count && compare(base + child * width,base + (child + 1) * width) < 0) ++child;
        if (compare(base + root * width,base + child * width) >= 0) return 1;
        if (!exchange(base + root * width,base + child * width,width)) return 0;
        root = child;
    }
    return 1;
}
/* Bounded O(n log n), allocation-free, and independent of host qsort choices. */
int antfly_regex_sort(void *data, size_t count, size_t width, int (*compare)(const void *, const void *)) {
    unsigned char *base = (unsigned char *)data;
    if (count < 2 || width == 0) return 1;
    for (size_t root = count / 2; root > 0;) if (!sift(base,count,--root,width,compare)) return 0;
    for (size_t end = count - 1; end > 0; --end) {
        if (!exchange(base,base + end * width,width)) return 0;
        if (!sift(base,end,0,width,compare)) return 0;
    }
    return 1;
}
