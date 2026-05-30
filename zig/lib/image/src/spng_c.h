/* Translate-C shim for libspng. Zig 0.17 removed @cImport; the build runs
 * addTranslateC over this header (with the spng include dir on the path) to
 * produce the `spng_c` module when spng is available. */
#include "spng.h"
