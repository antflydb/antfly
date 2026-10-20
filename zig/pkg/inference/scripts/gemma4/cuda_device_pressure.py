"""Scoped CUDA allocation pressure for explicit full-model OOM qualification.

Importing this module does not initialize CUDA. The allocation belongs to the
qualification process, never to the inference worker under test.
"""
import contextlib
import ctypes


class CudaDriver:
    def __init__(self, device=0):
        self.library = ctypes.CDLL('libcuda.so.1')
        signatures = {
            'cuInit': [ctypes.c_uint],
            'cuDeviceGet': [ctypes.POINTER(ctypes.c_int), ctypes.c_int],
            'cuDeviceGetUuid_v2': [ctypes.c_void_p, ctypes.c_int],
            'cuCtxCreate_v2': [ctypes.POINTER(ctypes.c_void_p), ctypes.c_uint, ctypes.c_int],
            'cuCtxDestroy_v2': [ctypes.c_void_p],
            'cuMemGetInfo_v2': [ctypes.POINTER(ctypes.c_size_t), ctypes.POINTER(ctypes.c_size_t)],
            'cuMemAlloc_v2': [ctypes.POINTER(ctypes.c_uint64), ctypes.c_size_t],
            'cuMemFree_v2': [ctypes.c_uint64],
        }
        for name, arguments in signatures.items():
            fn = getattr(self.library, name)
            fn.argtypes = arguments
            fn.restype = ctypes.c_int
        self.call('cuInit', 0)
        value = ctypes.c_int()
        self.call('cuDeviceGet', ctypes.byref(value), device)
        self.device = value.value
        uuid = (ctypes.c_ubyte * 16)()
        self.call('cuDeviceGetUuid_v2', ctypes.byref(uuid), self.device)
        value = bytes(uuid).hex()
        self.uuid = f'GPU-{value[:8]}-{value[8:12]}-{value[12:16]}-{value[16:20]}-{value[20:]}'

    def call(self, name, *args):
        code = getattr(self.library, name)(*args)
        if code != 0:
            raise RuntimeError(f'{name} failed with CUDA driver status {code}')

    def create(self):
        context = ctypes.c_void_p()
        self.call('cuCtxCreate_v2', ctypes.byref(context), 0, self.device)
        return context

    def destroy(self, context):
        self.call('cuCtxDestroy_v2', context)

    def memory(self):
        free, total = ctypes.c_size_t(), ctypes.c_size_t()
        self.call('cuMemGetInfo_v2', ctypes.byref(free), ctypes.byref(total))
        return free.value, total.value

    def allocate(self, size):
        pointer = ctypes.c_uint64()
        self.call('cuMemAlloc_v2', ctypes.byref(pointer), size)
        return pointer

    def free(self, pointer):
        self.call('cuMemFree_v2', pointer)


@contextlib.contextmanager
def device_pressure(driver, *, leave_free_bytes=32 * 1024**2):
    if type(leave_free_bytes) is not int or leave_free_bytes <= 0:
        raise ValueError('positive free-memory margin required')
    evidence = None
    context = driver.create()
    try:
        free, total = driver.memory()
        if not 0 < leave_free_bytes < free <= total:
            raise RuntimeError('insufficient free device memory to establish controlled pressure')
        size = free - leave_free_bytes
        pointer = driver.allocate(size)
        try:
            pressured_free, pressured_total = driver.memory()
            if pressured_total != total or pressured_free > leave_free_bytes:
                raise RuntimeError('device pressure changed before the model request')
            evidence = {'device_total_bytes': total, 'free_before_bytes': free,
                        'allocated_pressure_bytes': size, 'free_during_bytes': pressured_free,
                        'leave_free_bytes': leave_free_bytes, 'released': False}
            yield evidence
        finally:
            driver.free(pointer)
    finally:
        driver.destroy(context)
        if evidence is not None:
            evidence['released'] = True
