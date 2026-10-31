# Per-family launch specs

`check_kernels.py <metal|cuda> <family> <old> <new>` looks the family up in
`../kernel_specs.py`, then in `<family>.py` here, which must export `spec`
with the same shape as the dicts in `kernel_specs.py` (`grid`, `kernels`,
optional `shim`). Helpers such as `RAND` can be imported from
`kernel_specs`.
