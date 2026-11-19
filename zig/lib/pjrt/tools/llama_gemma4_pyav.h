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

// Optional libmtmd decoder for hosts without ffmpeg/ffprobe executables.
// Interpreter/import startup is outside request timing. Opening the encoded
// file, sequential decoding, skipped frames and RGB conversion are timed.
#include <Python.h>
#include <cstring>

static PyObject * reference_pyav_globals = nullptr;
static PyObject * reference_pyav_active = nullptr; // Borrowed until decoder close.
static void reference_pyav_init() {
    Py_Initialize();
    reference_pyav_globals = PyDict_New();
    PyDict_SetItemString(reference_pyav_globals, "__builtins__", PyEval_GetBuiltins());
    const char * source = R"PYAV(
import av

def open_video(path):
    container = av.open(path)
    stream = container.streams.video[0]
    return (container, iter(container.decode(stream)), stream.width, stream.height,
            float(stream.average_rate), stream.frames,
            float(stream.duration * stream.time_base) if stream.duration else -1.0, [])

def read_video(state, width, height, wanted):
    frame = next(state[1], None)
    if frame is None:
        return None
    if not wanted:
        return b''
    state[7].append(float(frame.time))
    if (frame.width, frame.height) != (width, height):
        raise ValueError('Qualification decoder requires original frame dimensions')
    return frame.to_ndarray(format='rgb24').tobytes()
)PYAV";
    auto * result = PyRun_String(source, Py_file_input, reference_pyav_globals, reference_pyav_globals);
    if (!result) { PyErr_Print(); throw std::runtime_error("PyAV initialization failed"); }
    Py_DECREF(result);
}
static void * reference_pyav_open(void *, const char * path, const unsigned char *, size_t,
                                  mtmd_helper_video_stream_info * info) {
    auto * state = PyObject_CallFunction(PyDict_GetItemString(reference_pyav_globals, "open_video"), "s", path);
    if (!state) { PyErr_Print(); return nullptr; }
    info->width = PyLong_AsUnsignedLong(PyTuple_GetItem(state, 2));
    info->height = PyLong_AsUnsignedLong(PyTuple_GetItem(state, 3));
    info->fps = PyFloat_AsDouble(PyTuple_GetItem(state, 4));
    info->n_frames = PyLong_AsLong(PyTuple_GetItem(state, 5));
    info->duration = PyFloat_AsDouble(PyTuple_GetItem(state, 6));
    if (PyErr_Occurred()) { PyErr_Print(); Py_DECREF(state); return nullptr; }
    reference_pyav_active = state;
    return state;
}
static void reference_pyav_capture(const std::filesystem::path & path) {
    if (!reference_pyav_active) throw std::runtime_error("No active video decoder");
    auto * timestamps = PyTuple_GetItem(reference_pyav_active, 7);
    std::ofstream out(path);
    out << std::setprecision(12) << "{\"decoder\":\"PyAV\",\"timestamps_seconds\":[";
    for (Py_ssize_t i = 0; i < PyList_Size(timestamps); ++i) {
        if (i) out << ',';
        out << PyFloat_AsDouble(PyList_GetItem(timestamps, i));
    }
    out << "]}\n";
    if (!out) throw std::runtime_error("Video metadata output failed");
}
static bool reference_pyav_read(void * state, unsigned char * rgb, uint32_t width, uint32_t height) {
    auto * result = PyObject_CallFunction(PyDict_GetItemString(reference_pyav_globals, "read_video"),
                                         "OIIi", static_cast<PyObject *>(state), width, height, rgb != nullptr);
    if (!result) { PyErr_Print(); return false; }
    if (result == Py_None) { Py_DECREF(result); return false; }
    bool ok = PyBytes_Check(result) && (!rgb || size_t(PyBytes_Size(result)) == size_t(width) * height * 3);
    if (ok && rgb) std::memcpy(rgb, PyBytes_AsString(result), size_t(width) * height * 3);
    Py_DECREF(result);
    return ok;
}
static void reference_pyav_close(void * state) {
    reference_pyav_active = nullptr;
    auto * container = PyTuple_GetItem(static_cast<PyObject *>(state), 0);
    auto * result = PyObject_CallMethod(container, "close", nullptr);
    if (!result) PyErr_Print(); else Py_DECREF(result);
    Py_DECREF(static_cast<PyObject *>(state));
}
static const mtmd_helper_video_decoder reference_pyav_decoder = {
    reference_pyav_open, nullptr, reference_pyav_read, nullptr, reference_pyav_close, nullptr,
};
