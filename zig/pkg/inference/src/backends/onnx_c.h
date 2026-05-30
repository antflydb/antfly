/* Translate-C shim for the ONNX Runtime C API. Zig 0.17 removed @cImport; the
 * build system runs addTranslateC over this header (with the ONNX runtime
 * include dir on the path) to produce the `onnx_c` module when -Donnx=true. */
#include "onnxruntime_c_api.h"
#include "onnxruntime_session_options_config_keys.h"
