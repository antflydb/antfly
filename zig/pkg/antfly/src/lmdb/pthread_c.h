/* Translate-C shim for the pthread surface used by the Zig LMDB writer lock.
 * Zig 0.17 removed @cImport/@cInclude; the build system runs addTranslateC over
 * this header to produce the `lmdb_pthread` module when linking libc. */
#include <pthread.h>
