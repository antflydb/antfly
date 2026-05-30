/* Translate-C shim for system BLAS (CBLAS). Produces the `blas_c` module via
 * addTranslateC when -Dsystem-blas=true. On macOS the Accelerate/vecLib header
 * is used; elsewhere the OpenBLAS cblas.h. */
#if defined(__APPLE__)
#include <vecLib/cblas.h>
#else
#include <cblas.h>
#endif
