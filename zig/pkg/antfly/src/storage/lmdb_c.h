/* Translate-C shim for the C LMDB backend bindings.
 * Zig 0.17 removed @cImport/@cInclude; the build system runs addTranslateC over
 * this header (with lib/lmdb on the include path) to produce the
 * `lmdb_c_bindings` module when the C backend is selected. */
#include "lmdb.h"
