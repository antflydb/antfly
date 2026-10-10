// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// One POSIX thread signal-stack owner for the partitioned native executable.
// Runtime archives disable their private Zig TLS buffers. Keep the main Zig
// startup stack and crash handlers; install a full-size stack before any native
// pthread entry, including threads spawned by linked libraries. TLS destruction
// belongs to libc, so the stack remains valid throughout thread-local cleanup.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define ANTFLY_SIGNAL_STACK_BYTES (256 * 1024)
static _Thread_local unsigned char antfly_signal_stack[ANTFLY_SIGNAL_STACK_BYTES];
typedef int (*create_fn)(pthread_t *, const pthread_attr_t *, void *(*)(void *), void *);
static create_fn native_create;
static pthread_once_t resolve_once = PTHREAD_ONCE_INIT;

static void resolve_create(void) {
    void *symbol = dlsym(RTLD_NEXT, "pthread_create");
    _Static_assert(sizeof(symbol) == sizeof(native_create), "POSIX function pointer size");
    memcpy(&native_create, &symbol, sizeof(symbol));
}

struct entry { void *(*run)(void *); void *argument; };
static void *enter(void *raw) {
    struct entry entry = *(struct entry *)raw;
    free(raw);
    stack_t current;
    if (sigaltstack(NULL, &current) != 0) abort();
    // Preserve a stack installed by the embedding host or another native owner.
    if (current.ss_flags & SS_DISABLE) {
        stack_t stack = { .ss_sp = antfly_signal_stack,
                          .ss_size = sizeof(antfly_signal_stack), .ss_flags = 0 };
        if (sigaltstack(&stack, NULL) != 0) abort();
    }
    return entry.run(entry.argument);
}

int pthread_create(pthread_t *thread, const pthread_attr_t *attributes,
                   void *(*run)(void *), void *argument) {
    int error = pthread_once(&resolve_once, resolve_create);
    if (error) return error;
    if (!native_create) return ENOSYS;
    struct entry *entry = malloc(sizeof(*entry));
    if (!entry) return EAGAIN;
    *entry = (struct entry){ .run = run, .argument = argument };
    error = native_create(thread, attributes, enter, entry);
    if (error) free(entry);
    return error;
}

#ifdef ANTFLY_SIGNAL_STACK_TEST
// A real guard-page overflow must invoke the process handler on our stack.
static void signal_handler(int signal, siginfo_t *info, void *context) {
    (void)signal; (void)info; (void)context;
    unsigned char marker;
    uintptr_t address = (uintptr_t)&marker;
    uintptr_t begin = (uintptr_t)antfly_signal_stack;
    if (address < begin || address >= begin + sizeof(antfly_signal_stack)) _exit(2);
    static const char message[] = "shared signal stack handled worker overflow\n";
    (void)write(STDOUT_FILENO, message, sizeof(message) - 1);
    _exit(0);
}
__attribute__((noinline)) static void overflow(unsigned depth) {
    volatile unsigned char frame[8192];
    frame[depth % sizeof(frame)] = (unsigned char)depth;
    overflow(depth + 1);
    frame[(depth + 1) % sizeof(frame)] = 1;
}
static void *exercise(void *unused) {
    (void)unused;
    stack_t stack;
    if (sigaltstack(NULL, &stack) != 0 || (stack.ss_flags & SS_DISABLE) ||
        stack.ss_size != ANTFLY_SIGNAL_STACK_BYTES) _exit(3);
    overflow(0);
    return NULL;
}
int main(void) {
    alarm(5);
    struct sigaction action = { .sa_sigaction = signal_handler,
                                .sa_flags = SA_SIGINFO | SA_ONSTACK };
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGSEGV, &action, NULL) != 0 || sigaction(SIGBUS, &action, NULL) != 0) return 4;
    pthread_attr_t attributes;
    if (pthread_attr_init(&attributes) != 0 || pthread_attr_setstacksize(&attributes, 2 * 1024 * 1024) != 0) return 5;
    pthread_t thread;
    int error = pthread_create(&thread, &attributes, exercise, NULL);
    pthread_attr_destroy(&attributes);
    if (error) return 6;
    pthread_join(thread, NULL);
    return 7;
}
#endif
