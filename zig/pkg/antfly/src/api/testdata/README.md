# Native Parquet vector fixtures

Generated with PyArrow 25.0.1. Each file contains 231 rows with columns `body`
(UTF-8), `dense` (`list<float32>`), and `sparse` (UTF-8). Row `i` in file `f`
contains `file-{f}-value-{i}`, `[float(i), 1.0]`, and `{"1":2}`.

```python
import pyarrow as pa
import pyarrow.parquet as pq

for f in range(2):
    table = pa.table({
        "body": [f"file-{f}-value-{i}" for i in range(231)],
        "dense": pa.array([[float(i), 1.0] for i in range(231)],
                          type=pa.list_(pa.float32())),
        "sparse": ['{"1":2}'] * 231,
    })
    pq.write_table(table, f"lake_vectors_{f}.parquet",
                   compression="snappy", data_page_version="1.0" if f == 0 else "2.0",
                   use_dictionary=f == 1, data_page_size=512,
                   write_batch_size=16, store_schema=False)
```

The native execution integration test builds all index families, publishes the
artifacts, and reopens the dense index. The typed case also exercises four-bit
quantization. Reader unit tests separately force vectors across page boundaries,
null/empty lists, null coordinates, and invalid continuations.


The two `lake_double_vectors_*.parquet` fixtures use the same generation options
with `pa.list_(pa.float64())` and values `[float(i) + 1e-8,
1.0000000000000002]`. The integration test checks the original double values at
the SQL/JSON boundary before building and reopening the native index families.
