# Tree-packed Laya: 2026-09-24 benchmark and test evidence

Design and summary: [`zig/pkg/inference/models/laya/LAYA.md`](../../../../zig/pkg/inference/models/laya/LAYA.md).

Host: Apple M4 Max, 36 GiB, macOS 15 (Darwin 24.6.0), Zig 0.16.0. Checkpoint:
`convaiinnovations/laya` at `c5d78730f3493e4fe16d61507ef4b78eef7318cf`, prepared
with `scripts/laya/prepare_laya.py`. The packed session loads the same weights
with `laya.packing = {"mode":"question","max_packed_len":8192}`, so packed
decisions are meaningless and only cost is compared. Median of five warm
requests after one discarded request; ReleaseFast test binary.

```bash
ANTFLY_LAYA_PACKED_BENCH=<prepared laya dir> [ANTFLY_LAYA_BACKEND=metal] \
  zig build test -Doptimize=ReleaseFast -- --test-filter "laya packed benchmark"
```

## Metal, packed RoPE on device (final)

```
{"backend":"metal","state_sentences":1,"questions":1,"unpacked_ms":58.5,"packed_ms":56.5,"unpacked_tokens":55,"packed_tokens":56}
{"backend":"metal","state_sentences":1,"questions":2,"unpacked_ms":67.6,"packed_ms":61.9,"unpacked_tokens":119,"packed_tokens":86}
{"backend":"metal","state_sentences":1,"questions":4,"unpacked_ms":81.4,"packed_ms":70.3,"unpacked_tokens":235,"packed_tokens":134}
{"backend":"metal","state_sentences":1,"questions":8,"unpacked_ms":108.8,"packed_ms":89.1,"unpacked_tokens":479,"packed_tokens":242}
{"backend":"metal","state_sentences":1,"questions":16,"unpacked_ms":165.4,"packed_ms":148.4,"unpacked_tokens":955,"packed_tokens":446}
{"backend":"metal","state_sentences":4,"questions":1,"unpacked_ms":79.4,"packed_ms":72.8,"unpacked_tokens":151,"packed_tokens":152}
{"backend":"metal","state_sentences":4,"questions":2,"unpacked_ms":102.7,"packed_ms":77.6,"unpacked_tokens":311,"packed_tokens":182}
{"backend":"metal","state_sentences":4,"questions":4,"unpacked_ms":149.3,"packed_ms":87.4,"unpacked_tokens":619,"packed_tokens":230}
{"backend":"metal","state_sentences":4,"questions":8,"unpacked_ms":237.2,"packed_ms":114.9,"unpacked_tokens":1247,"packed_tokens":338}
{"backend":"metal","state_sentences":4,"questions":16,"unpacked_ms":410.8,"packed_ms":190.7,"unpacked_tokens":2491,"packed_tokens":542}
{"backend":"metal","state_sentences":12,"questions":1,"unpacked_ms":195.5,"packed_ms":139.2,"unpacked_tokens":407,"packed_tokens":408}
{"backend":"metal","state_sentences":12,"questions":2,"unpacked_ms":280.4,"packed_ms":146.0,"unpacked_tokens":823,"packed_tokens":438}
{"backend":"metal","state_sentences":12,"questions":4,"unpacked_ms":453.6,"packed_ms":165.8,"unpacked_tokens":1643,"packed_tokens":486}
{"backend":"metal","state_sentences":12,"questions":8,"unpacked_ms":800.1,"packed_ms":219.9,"unpacked_tokens":3295,"packed_tokens":594}
{"backend":"metal","state_sentences":12,"questions":16,"unpacked_ms":1534.7,"packed_ms":340.3,"unpacked_tokens":6587,"packed_tokens":798}
```

## Metal, first implementation (packed RoPE rotated on the host)

Superseded. Kept because it shows the cost of per-layer host round trips:
56 device synchronizations per forward add 200–800 ms of fixed overhead.

```
{"backend":"metal","state_sentences":1,"questions":1,"unpacked_ms":73.1,"packed_ms":249.9,"unpacked_tokens":55,"packed_tokens":56}
{"backend":"metal","state_sentences":1,"questions":2,"unpacked_ms":80.1,"packed_ms":296.6,"unpacked_tokens":119,"packed_tokens":86}
{"backend":"metal","state_sentences":1,"questions":4,"unpacked_ms":105.0,"packed_ms":386.9,"unpacked_tokens":235,"packed_tokens":134}
{"backend":"metal","state_sentences":1,"questions":8,"unpacked_ms":123.3,"packed_ms":541.8,"unpacked_tokens":479,"packed_tokens":242}
{"backend":"metal","state_sentences":1,"questions":16,"unpacked_ms":191.3,"packed_ms":868.2,"unpacked_tokens":955,"packed_tokens":446}
{"backend":"metal","state_sentences":4,"questions":1,"unpacked_ms":93.5,"packed_ms":406.7,"unpacked_tokens":151,"packed_tokens":152}
{"backend":"metal","state_sentences":4,"questions":2,"unpacked_ms":124.0,"packed_ms":458.7,"unpacked_tokens":311,"packed_tokens":182}
{"backend":"metal","state_sentences":4,"questions":4,"unpacked_ms":154.6,"packed_ms":506.9,"unpacked_tokens":619,"packed_tokens":230}
{"backend":"metal","state_sentences":4,"questions":8,"unpacked_ms":254.9,"packed_ms":748.7,"unpacked_tokens":1247,"packed_tokens":338}
{"backend":"metal","state_sentences":4,"questions":16,"unpacked_ms":418.9,"packed_ms":875.4,"unpacked_tokens":2491,"packed_tokens":542}
{"backend":"metal","state_sentences":12,"questions":1,"unpacked_ms":215.1,"packed_ms":834.2,"unpacked_tokens":407,"packed_tokens":408}
{"backend":"metal","state_sentences":12,"questions":2,"unpacked_ms":310.3,"packed_ms":808.8,"unpacked_tokens":823,"packed_tokens":438}
{"backend":"metal","state_sentences":12,"questions":4,"unpacked_ms":537.7,"packed_ms":874.9,"unpacked_tokens":1643,"packed_tokens":486}
{"backend":"metal","state_sentences":12,"questions":8,"unpacked_ms":806.5,"packed_ms":921.8,"unpacked_tokens":3295,"packed_tokens":594}
{"backend":"metal","state_sentences":12,"questions":16,"unpacked_ms":1458.9,"packed_ms":1026.9,"unpacked_tokens":6587,"packed_tokens":798}
```

## CPU (native BLAS), final code

```
{"backend":"native","state_sentences":1,"questions":1,"unpacked_ms":336.2,"packed_ms":331.7,"unpacked_tokens":55,"packed_tokens":56}
{"backend":"native","state_sentences":1,"questions":2,"unpacked_ms":446.6,"packed_ms":385.3,"unpacked_tokens":119,"packed_tokens":86}
{"backend":"native","state_sentences":1,"questions":4,"unpacked_ms":606.5,"packed_ms":479.7,"unpacked_tokens":235,"packed_tokens":134}
{"backend":"native","state_sentences":1,"questions":8,"unpacked_ms":972.1,"packed_ms":711.3,"unpacked_tokens":479,"packed_tokens":242}
{"backend":"native","state_sentences":1,"questions":16,"unpacked_ms":1644.2,"packed_ms":1274.3,"unpacked_tokens":955,"packed_tokens":446}
{"backend":"native","state_sentences":4,"questions":1,"unpacked_ms":480.1,"packed_ms":487.8,"unpacked_tokens":151,"packed_tokens":152}
{"backend":"native","state_sentences":4,"questions":2,"unpacked_ms":769.5,"packed_ms":607.1,"unpacked_tokens":311,"packed_tokens":182}
{"backend":"native","state_sentences":4,"questions":4,"unpacked_ms":1427.1,"packed_ms":721.5,"unpacked_tokens":619,"packed_tokens":230}
{"backend":"native","state_sentences":4,"questions":8,"unpacked_ms":2917.7,"packed_ms":1206.3,"unpacked_tokens":1247,"packed_tokens":338}
{"backend":"native","state_sentences":4,"questions":16,"unpacked_ms":5715.8,"packed_ms":1887.6,"unpacked_tokens":2491,"packed_tokens":542}
{"backend":"native","state_sentences":12,"questions":1,"unpacked_ms":1466.6,"packed_ms":1343.6,"unpacked_tokens":407,"packed_tokens":408}
{"backend":"native","state_sentences":12,"questions":2,"unpacked_ms":2279.0,"packed_ms":1420.5,"unpacked_tokens":823,"packed_tokens":438}
{"backend":"native","state_sentences":12,"questions":4,"unpacked_ms":4019.9,"packed_ms":1565.8,"unpacked_tokens":1643,"packed_tokens":486}
{"backend":"native","state_sentences":12,"questions":8,"unpacked_ms":10631.0,"packed_ms":1975.3,"unpacked_tokens":3295,"packed_tokens":594}
{"backend":"native","state_sentences":12,"questions":16,"unpacked_ms":16065.5,"packed_ms":2953.6,"unpacked_tokens":6587,"packed_tokens":798}
```

## CPU, first run (host RoPE; same arithmetic path on CPU)

Run-to-run variation on this laptop is visible between the two CPU runs
(for example 8.6 s vs 10.6 s for eight questions over the longest state).

```
{"backend":"native","state_sentences":1,"questions":1,"unpacked_ms":309.1,"packed_ms":319.1,"unpacked_tokens":55,"packed_tokens":56}
{"backend":"native","state_sentences":1,"questions":2,"unpacked_ms":423.7,"packed_ms":375.2,"unpacked_tokens":119,"packed_tokens":86}
{"backend":"native","state_sentences":1,"questions":4,"unpacked_ms":598.6,"packed_ms":471.3,"unpacked_tokens":235,"packed_tokens":134}
{"backend":"native","state_sentences":1,"questions":8,"unpacked_ms":946.3,"packed_ms":657.8,"unpacked_tokens":479,"packed_tokens":242}
{"backend":"native","state_sentences":1,"questions":16,"unpacked_ms":1565.1,"packed_ms":1268.6,"unpacked_tokens":955,"packed_tokens":446}
{"backend":"native","state_sentences":4,"questions":1,"unpacked_ms":475.7,"packed_ms":478.3,"unpacked_tokens":151,"packed_tokens":152}
{"backend":"native","state_sentences":4,"questions":2,"unpacked_ms":728.2,"packed_ms":544.2,"unpacked_tokens":311,"packed_tokens":182}
{"backend":"native","state_sentences":4,"questions":4,"unpacked_ms":1252.5,"packed_ms":640.8,"unpacked_tokens":619,"packed_tokens":230}
{"backend":"native","state_sentences":4,"questions":8,"unpacked_ms":2410.9,"packed_ms":951.7,"unpacked_tokens":1247,"packed_tokens":338}
{"backend":"native","state_sentences":4,"questions":16,"unpacked_ms":4591.9,"packed_ms":1764.0,"unpacked_tokens":2491,"packed_tokens":542}
{"backend":"native","state_sentences":12,"questions":1,"unpacked_ms":1259.3,"packed_ms":1220.4,"unpacked_tokens":407,"packed_tokens":408}
{"backend":"native","state_sentences":12,"questions":2,"unpacked_ms":2228.6,"packed_ms":1329.8,"unpacked_tokens":823,"packed_tokens":438}
{"backend":"native","state_sentences":12,"questions":4,"unpacked_ms":4175.1,"packed_ms":1521.1,"unpacked_tokens":1643,"packed_tokens":486}
{"backend":"native","state_sentences":12,"questions":8,"unpacked_ms":8643.2,"packed_ms":2284.4,"unpacked_tokens":3295,"packed_tokens":594}
{"backend":"native","state_sentences":12,"questions":16,"unpacked_ms":16678.6,"packed_ms":3724.0,"unpacked_tokens":6587,"packed_tokens":798}
```

## Test output (fixture-backed, native)

```
6/80 finetune.laya.training_test.test.laya training forward objective and every parameter gradient match PyTorch...Laya native: 45 gradient tensors, max absolute error=0.0000023841858
9/80 finetune.laya.training_packed_test.test.laya packed training graph matches packed serving logits, alone and in a padded batch...Laya packed {"mode":"question"}: training-graph vs serving max logit error=0.0000032186508
Laya packed {"mode":"candidate"}: training-graph vs serving max logit error=0.0000069886446
Laya packed export serving vs training max probability error=0.000000059604645
25/80 pipelines.laya_packed_test.test.laya packed encoder on a one-segment tree reproduces the unpacked encoder...Laya packed one-segment encoder max error=0
26/80 pipelines.laya_packed_test.test.laya packed questions are isolated and share one exact trunk encoding...Laya packed {"mode":"question"}: isolation max error=0.0000027418137, trunk max error=0
Laya packed {"mode":"candidate"}: isolation max error=0.0000047683716, trunk max error=0
Laya intermediates encoder_max_error=0.0000005 head_max_error=0.0000014
46 selected; 39 passed; 7 skipped.
```

## PyTorch oracle

```
$ uv run --script laya_packed_reference.py --fixture <ref> --common common.py
{"one_segment_vs_upstream_max_error": 0.0}
```

## State cache, CPU only (superseded later on 2026-09-24)

CPU, ReleaseFast. `packed_ms` is packed with the cache disabled, and
`packed_cached_ms` is packed with the cache on. Every repeated request after
the first hits the cache.

```
{"backend":"native","state_sentences":1,"questions":1,"unpacked_ms":307.0,"packed_ms":307.2,"packed_cached_ms":310.8,"unpacked_tokens":55,"packed_tokens":56}
{"backend":"native","state_sentences":1,"questions":2,"unpacked_ms":436.9,"packed_ms":381.3,"packed_cached_ms":327.9,"unpacked_tokens":119,"packed_tokens":86}
{"backend":"native","state_sentences":1,"questions":4,"unpacked_ms":568.9,"packed_ms":450.5,"packed_cached_ms":425.0,"unpacked_tokens":235,"packed_tokens":134}
{"backend":"native","state_sentences":1,"questions":8,"unpacked_ms":919.0,"packed_ms":675.7,"packed_cached_ms":659.5,"unpacked_tokens":479,"packed_tokens":242}
{"backend":"native","state_sentences":1,"questions":16,"unpacked_ms":1630.0,"packed_ms":1247.4,"packed_cached_ms":1208.5,"unpacked_tokens":955,"packed_tokens":446}
{"backend":"native","state_sentences":4,"questions":1,"unpacked_ms":477.9,"packed_ms":473.5,"packed_cached_ms":364.9,"unpacked_tokens":151,"packed_tokens":152}
{"backend":"native","state_sentences":4,"questions":2,"unpacked_ms":731.5,"packed_ms":546.3,"packed_cached_ms":395.0,"unpacked_tokens":311,"packed_tokens":182}
{"backend":"native","state_sentences":4,"questions":4,"unpacked_ms":1238.3,"packed_ms":634.9,"packed_cached_ms":515.8,"unpacked_tokens":619,"packed_tokens":230}
{"backend":"native","state_sentences":4,"questions":8,"unpacked_ms":2241.4,"packed_ms":907.8,"packed_cached_ms":779.8,"unpacked_tokens":1247,"packed_tokens":338}
{"backend":"native","state_sentences":4,"questions":16,"unpacked_ms":4182.5,"packed_ms":1582.7,"packed_cached_ms":1454.9,"unpacked_tokens":2491,"packed_tokens":542}
{"backend":"native","state_sentences":12,"questions":1,"unpacked_ms":1126.9,"packed_ms":1113.2,"packed_cached_ms":721.1,"unpacked_tokens":407,"packed_tokens":408}
{"backend":"native","state_sentences":12,"questions":2,"unpacked_ms":2051.9,"packed_ms":1207.0,"packed_cached_ms":785.8,"unpacked_tokens":823,"packed_tokens":438}
{"backend":"native","state_sentences":12,"questions":4,"unpacked_ms":3835.5,"packed_ms":1375.2,"packed_cached_ms":972.9,"unpacked_tokens":1643,"packed_tokens":486}
{"backend":"native","state_sentences":12,"questions":8,"unpacked_ms":7487.0,"packed_ms":1813.0,"packed_cached_ms":1393.9,"unpacked_tokens":3295,"packed_tokens":594}
{"backend":"native","state_sentences":12,"questions":16,"unpacked_ms":14696.1,"packed_ms":2703.6,"packed_cached_ms":2287.8,"unpacked_tokens":6587,"packed_tokens":798}
```

Metal: the same benchmark with the cache enabled on Metal, before it was
restricted to CPU. The cache gave no speedup, because the host keys and values
are re-uploaded on every request. A second finding followed: decisions through
a session were wrong (max probability error 0.034 against the oracle), because
the device row concat/gather is not ordered with pending batched command work.
In this output `packed_ms` is uncached and `packed_cached_ms` is cached.

```
{"backend":"metal","state_sentences":1,"questions":1,"unpacked_ms":57.5,"packed_ms":56.3,"packed_cached_ms":68.3,"unpacked_tokens":55,"packed_tokens":56}
{"backend":"metal","state_sentences":1,"questions":2,"unpacked_ms":66.8,"packed_ms":61.2,"packed_cached_ms":68.2,"unpacked_tokens":119,"packed_tokens":86}
{"backend":"metal","state_sentences":1,"questions":4,"unpacked_ms":80.2,"packed_ms":70.4,"packed_cached_ms":79.6,"unpacked_tokens":235,"packed_tokens":134}
{"backend":"metal","state_sentences":1,"questions":8,"unpacked_ms":110.5,"packed_ms":89.2,"packed_cached_ms":99.4,"unpacked_tokens":479,"packed_tokens":242}
{"backend":"metal","state_sentences":1,"questions":16,"unpacked_ms":169.1,"packed_ms":153.0,"packed_cached_ms":162.3,"unpacked_tokens":955,"packed_tokens":446}
{"backend":"metal","state_sentences":4,"questions":1,"unpacked_ms":79.9,"packed_ms":73.3,"packed_cached_ms":78.3,"unpacked_tokens":151,"packed_tokens":152}
{"backend":"metal","state_sentences":4,"questions":2,"unpacked_ms":104.5,"packed_ms":79.2,"packed_cached_ms":83.6,"unpacked_tokens":311,"packed_tokens":182}
{"backend":"metal","state_sentences":4,"questions":4,"unpacked_ms":151.1,"packed_ms":87.7,"packed_cached_ms":93.8,"unpacked_tokens":619,"packed_tokens":230}
{"backend":"metal","state_sentences":4,"questions":8,"unpacked_ms":242.7,"packed_ms":118.2,"packed_cached_ms":125.0,"unpacked_tokens":1247,"packed_tokens":338}
{"backend":"metal","state_sentences":4,"questions":16,"unpacked_ms":441.6,"packed_ms":192.1,"packed_cached_ms":204.9,"unpacked_tokens":2491,"packed_tokens":542}
{"backend":"metal","state_sentences":12,"questions":1,"unpacked_ms":197.8,"packed_ms":138.8,"packed_cached_ms":130.3,"unpacked_tokens":407,"packed_tokens":408}
{"backend":"metal","state_sentences":12,"questions":2,"unpacked_ms":292.6,"packed_ms":148.4,"packed_cached_ms":142.9,"unpacked_tokens":823,"packed_tokens":438}
{"backend":"metal","state_sentences":12,"questions":4,"unpacked_ms":481.3,"packed_ms":169.1,"packed_cached_ms":167.0,"unpacked_tokens":1643,"packed_tokens":486}
{"backend":"metal","state_sentences":12,"questions":8,"unpacked_ms":826.3,"packed_ms":228.5,"packed_cached_ms":217.0,"unpacked_tokens":3295,"packed_tokens":594}
{"backend":"metal","state_sentences":12,"questions":16,"unpacked_ms":1512.8,"packed_ms":347.5,"packed_cached_ms":361.0,"unpacked_tokens":6587,"packed_tokens":798}
```

## State cache on the GPU (final)

After the row-join fix (in-stream flat concat/slice on Metal) and
device-resident entries, the cache is enabled on Metal and CPU. Trunks under
96 tokens are not cached. `packed_ms` is uncached and `packed_cached_ms` is
cached. Metal:

```
{"backend":"metal","state_sentences":1,"questions":1,"unpacked_ms":58.2,"packed_ms":56.3,"packed_cached_ms":56.5,"unpacked_tokens":55,"packed_tokens":56}
{"backend":"metal","state_sentences":1,"questions":2,"unpacked_ms":66.5,"packed_ms":61.2,"packed_cached_ms":60.9,"unpacked_tokens":119,"packed_tokens":86}
{"backend":"metal","state_sentences":1,"questions":4,"unpacked_ms":79.8,"packed_ms":69.9,"packed_cached_ms":69.9,"unpacked_tokens":235,"packed_tokens":134}
{"backend":"metal","state_sentences":1,"questions":8,"unpacked_ms":109.1,"packed_ms":88.8,"packed_cached_ms":88.7,"unpacked_tokens":479,"packed_tokens":242}
{"backend":"metal","state_sentences":1,"questions":16,"unpacked_ms":166.3,"packed_ms":148.6,"packed_cached_ms":148.7,"unpacked_tokens":955,"packed_tokens":446}
{"backend":"metal","state_sentences":4,"questions":1,"unpacked_ms":79.6,"packed_ms":72.0,"packed_cached_ms":66.8,"unpacked_tokens":151,"packed_tokens":152}
{"backend":"metal","state_sentences":4,"questions":2,"unpacked_ms":103.4,"packed_ms":78.2,"packed_cached_ms":72.2,"unpacked_tokens":311,"packed_tokens":182}
{"backend":"metal","state_sentences":4,"questions":4,"unpacked_ms":149.0,"packed_ms":87.3,"packed_cached_ms":82.2,"unpacked_tokens":619,"packed_tokens":230}
{"backend":"metal","state_sentences":4,"questions":8,"unpacked_ms":234.3,"packed_ms":115.7,"packed_cached_ms":109.4,"unpacked_tokens":1247,"packed_tokens":338}
{"backend":"metal","state_sentences":4,"questions":16,"unpacked_ms":411.4,"packed_ms":194.0,"packed_cached_ms":191.4,"unpacked_tokens":2491,"packed_tokens":542}
{"backend":"metal","state_sentences":12,"questions":1,"unpacked_ms":193.5,"packed_ms":137.2,"packed_cached_ms":108.5,"unpacked_tokens":407,"packed_tokens":408}
{"backend":"metal","state_sentences":12,"questions":2,"unpacked_ms":280.1,"packed_ms":147.5,"packed_cached_ms":118.3,"unpacked_tokens":823,"packed_tokens":438}
{"backend":"metal","state_sentences":12,"questions":4,"unpacked_ms":452.0,"packed_ms":168.0,"packed_cached_ms":139.4,"unpacked_tokens":1643,"packed_tokens":486}
{"backend":"metal","state_sentences":12,"questions":8,"unpacked_ms":788.1,"packed_ms":222.4,"packed_cached_ms":192.9,"unpacked_tokens":3295,"packed_tokens":594}
{"backend":"metal","state_sentences":12,"questions":16,"unpacked_ms":1499.9,"packed_ms":972.6,"packed_cached_ms":473.1,"unpacked_tokens":6587,"packed_tokens":798}
```

The uncached 16-question, 12-sentence Metal value (972.6 ms) is an outlier;
three earlier runs measured 340–347 ms.

Intermediate Metal run: device-resident entries, but the branch forward was
still unframed. The cached path was slower than uncached, which showed the
remaining cost was per-op submission rather than uploads.

```
{"backend":"metal","state_sentences":1,"questions":1,"unpacked_ms":58.9,"packed_ms":51.8,"packed_cached_ms":119.4,"unpacked_tokens":55,"packed_tokens":56}
{"backend":"metal","state_sentences":1,"questions":2,"unpacked_ms":65.0,"packed_ms":61.7,"packed_cached_ms":175.8,"unpacked_tokens":119,"packed_tokens":86}
{"backend":"metal","state_sentences":1,"questions":4,"unpacked_ms":81.6,"packed_ms":72.1,"packed_cached_ms":210.8,"unpacked_tokens":235,"packed_tokens":134}
{"backend":"metal","state_sentences":1,"questions":8,"unpacked_ms":111.8,"packed_ms":90.9,"packed_cached_ms":313.4,"unpacked_tokens":479,"packed_tokens":242}
{"backend":"metal","state_sentences":1,"questions":16,"unpacked_ms":167.7,"packed_ms":153.1,"packed_cached_ms":392.0,"unpacked_tokens":955,"packed_tokens":446}
{"backend":"metal","state_sentences":4,"questions":1,"unpacked_ms":82.1,"packed_ms":74.0,"packed_cached_ms":188.2,"unpacked_tokens":151,"packed_tokens":152}
{"backend":"metal","state_sentences":4,"questions":2,"unpacked_ms":105.2,"packed_ms":77.9,"packed_cached_ms":157.3,"unpacked_tokens":311,"packed_tokens":182}
{"backend":"metal","state_sentences":4,"questions":4,"unpacked_ms":150.0,"packed_ms":88.4,"packed_cached_ms":186.6,"unpacked_tokens":619,"packed_tokens":230}
{"backend":"metal","state_sentences":4,"questions":8,"unpacked_ms":236.8,"packed_ms":116.6,"packed_cached_ms":312.6,"unpacked_tokens":1247,"packed_tokens":338}
{"backend":"metal","state_sentences":4,"questions":16,"unpacked_ms":416.6,"packed_ms":199.0,"packed_cached_ms":373.2,"unpacked_tokens":2491,"packed_tokens":542}
{"backend":"metal","state_sentences":12,"questions":1,"unpacked_ms":192.9,"packed_ms":137.8,"packed_cached_ms":292.6,"unpacked_tokens":407,"packed_tokens":408}
{"backend":"metal","state_sentences":12,"questions":2,"unpacked_ms":287.9,"packed_ms":148.2,"packed_cached_ms":266.7,"unpacked_tokens":823,"packed_tokens":438}
{"backend":"metal","state_sentences":12,"questions":4,"unpacked_ms":456.9,"packed_ms":169.0,"packed_cached_ms":332.3,"unpacked_tokens":1643,"packed_tokens":486}
{"backend":"metal","state_sentences":12,"questions":8,"unpacked_ms":797.7,"packed_ms":229.6,"packed_cached_ms":402.5,"unpacked_tokens":3295,"packed_tokens":594}
{"backend":"metal","state_sentences":12,"questions":16,"unpacked_ms":1530.0,"packed_ms":342.2,"packed_cached_ms":448.9,"unpacked_tokens":6587,"packed_tokens":798}
```

CPU:

```
{"backend":"native","state_sentences":1,"questions":1,"unpacked_ms":383.5,"packed_ms":403.1,"packed_cached_ms":401.2,"unpacked_tokens":55,"packed_tokens":56}
{"backend":"native","state_sentences":1,"questions":2,"unpacked_ms":588.3,"packed_ms":497.2,"packed_cached_ms":507.5,"unpacked_tokens":119,"packed_tokens":86}
{"backend":"native","state_sentences":1,"questions":4,"unpacked_ms":824.0,"packed_ms":661.1,"packed_cached_ms":635.9,"unpacked_tokens":235,"packed_tokens":134}
{"backend":"native","state_sentences":1,"questions":8,"unpacked_ms":1341.4,"packed_ms":954.1,"packed_cached_ms":956.9,"unpacked_tokens":479,"packed_tokens":242}
{"backend":"native","state_sentences":1,"questions":16,"unpacked_ms":2218.5,"packed_ms":1218.5,"packed_cached_ms":1218.9,"unpacked_tokens":955,"packed_tokens":446}
{"backend":"native","state_sentences":4,"questions":1,"unpacked_ms":476.0,"packed_ms":471.7,"packed_cached_ms":345.8,"unpacked_tokens":151,"packed_tokens":152}
{"backend":"native","state_sentences":4,"questions":2,"unpacked_ms":725.4,"packed_ms":535.0,"packed_cached_ms":382.0,"unpacked_tokens":311,"packed_tokens":182}
{"backend":"native","state_sentences":4,"questions":4,"unpacked_ms":1204.0,"packed_ms":618.5,"packed_cached_ms":497.9,"unpacked_tokens":619,"packed_tokens":230}
{"backend":"native","state_sentences":4,"questions":8,"unpacked_ms":2250.2,"packed_ms":902.2,"packed_cached_ms":777.3,"unpacked_tokens":1247,"packed_tokens":338}
{"backend":"native","state_sentences":4,"questions":16,"unpacked_ms":4106.4,"packed_ms":1611.1,"packed_cached_ms":1443.4,"unpacked_tokens":2491,"packed_tokens":542}
{"backend":"native","state_sentences":12,"questions":1,"unpacked_ms":1174.7,"packed_ms":1181.9,"packed_cached_ms":740.3,"unpacked_tokens":407,"packed_tokens":408}
{"backend":"native","state_sentences":12,"questions":2,"unpacked_ms":2255.2,"packed_ms":1327.0,"packed_cached_ms":834.1,"unpacked_tokens":823,"packed_tokens":438}
{"backend":"native","state_sentences":12,"questions":4,"unpacked_ms":4157.8,"packed_ms":1675.3,"packed_cached_ms":1034.0,"unpacked_tokens":1643,"packed_tokens":486}
{"backend":"native","state_sentences":12,"questions":8,"unpacked_ms":7401.4,"packed_ms":1824.4,"packed_cached_ms":1408.4,"unpacked_tokens":3295,"packed_tokens":594}
{"backend":"native","state_sentences":12,"questions":16,"unpacked_ms":14821.7,"packed_ms":2756.9,"packed_cached_ms":2324.4,"unpacked_tokens":6587,"packed_tokens":798}
```

## Segment attention (step 1b), 2026-09-24

Metal:

```
{"backend":"metal","state_sentences":1,"questions":1,"unpacked_ms":61.9,"packed_ms":60.7,"packed_cached_ms":59.5,"unpacked_tokens":55,"packed_tokens":56}
{"backend":"metal","state_sentences":1,"questions":2,"unpacked_ms":70.9,"packed_ms":64.6,"packed_cached_ms":63.3,"unpacked_tokens":119,"packed_tokens":86}
{"backend":"metal","state_sentences":1,"questions":4,"unpacked_ms":83.4,"packed_ms":69.9,"packed_cached_ms":69.3,"unpacked_tokens":235,"packed_tokens":134}
{"backend":"metal","state_sentences":1,"questions":8,"unpacked_ms":112.2,"packed_ms":81.8,"packed_cached_ms":81.7,"unpacked_tokens":479,"packed_tokens":242}
{"backend":"metal","state_sentences":1,"questions":16,"unpacked_ms":169.4,"packed_ms":106.7,"packed_cached_ms":106.3,"unpacked_tokens":955,"packed_tokens":446}
{"backend":"metal","state_sentences":1,"questions":64,"unpacked_ms":515.4,"packed_ms":258.6,"packed_cached_ms":259.2,"unpacked_tokens":3835,"packed_tokens":1694}
{"backend":"metal","state_sentences":4,"questions":1,"unpacked_ms":82.5,"packed_ms":74.6,"packed_cached_ms":61.0,"unpacked_tokens":151,"packed_tokens":152}
{"backend":"metal","state_sentences":4,"questions":2,"unpacked_ms":107.4,"packed_ms":78.1,"packed_cached_ms":65.0,"unpacked_tokens":311,"packed_tokens":182}
{"backend":"metal","state_sentences":4,"questions":4,"unpacked_ms":151.6,"packed_ms":84.8,"packed_cached_ms":73.2,"unpacked_tokens":619,"packed_tokens":230}
{"backend":"metal","state_sentences":4,"questions":8,"unpacked_ms":237.7,"packed_ms":102.1,"packed_cached_ms":88.8,"unpacked_tokens":1247,"packed_tokens":338}
{"backend":"metal","state_sentences":4,"questions":16,"unpacked_ms":418.8,"packed_ms":132.2,"packed_cached_ms":119.2,"unpacked_tokens":2491,"packed_tokens":542}
{"backend":"metal","state_sentences":4,"questions":64,"unpacked_ms":1543.2,"packed_ms":316.8,"packed_cached_ms":307.3,"unpacked_tokens":9979,"packed_tokens":1790}
{"backend":"metal","state_sentences":12,"questions":1,"unpacked_ms":207.1,"packed_ms":135.1,"packed_cached_ms":66.2,"unpacked_tokens":407,"packed_tokens":408}
{"backend":"metal","state_sentences":12,"questions":2,"unpacked_ms":301.8,"packed_ms":138.0,"packed_cached_ms":72.5,"unpacked_tokens":823,"packed_tokens":438}
{"backend":"metal","state_sentences":12,"questions":4,"unpacked_ms":477.3,"packed_ms":146.5,"packed_cached_ms":82.9,"unpacked_tokens":1643,"packed_tokens":486}
{"backend":"metal","state_sentences":12,"questions":8,"unpacked_ms":816.7,"packed_ms":170.4,"packed_cached_ms":102.5,"unpacked_tokens":3295,"packed_tokens":594}
{"backend":"metal","state_sentences":12,"questions":16,"unpacked_ms":1544.7,"packed_ms":200.0,"packed_cached_ms":141.2,"unpacked_tokens":6587,"packed_tokens":798}
{"backend":"metal","state_sentences":12,"questions":64,"unpacked_ms":6099.4,"packed_ms":472.2,"packed_cached_ms":382.4,"unpacked_tokens":26363,"packed_tokens":2046}
```

CPU:

```
{"backend":"native","state_sentences":1,"questions":1,"unpacked_ms":317.8,"packed_ms":300.8,"packed_cached_ms":328.5,"unpacked_tokens":55,"packed_tokens":56}
{"backend":"native","state_sentences":1,"questions":2,"unpacked_ms":425.6,"packed_ms":371.4,"packed_cached_ms":373.2,"unpacked_tokens":119,"packed_tokens":86}
{"backend":"native","state_sentences":1,"questions":4,"unpacked_ms":587.0,"packed_ms":450.5,"packed_cached_ms":463.6,"unpacked_tokens":235,"packed_tokens":134}
{"backend":"native","state_sentences":1,"questions":8,"unpacked_ms":884.4,"packed_ms":579.3,"packed_cached_ms":577.8,"unpacked_tokens":479,"packed_tokens":242}
{"backend":"native","state_sentences":1,"questions":16,"unpacked_ms":1560.0,"packed_ms":930.8,"packed_cached_ms":931.7,"unpacked_tokens":955,"packed_tokens":446}
{"backend":"native","state_sentences":1,"questions":64,"unpacked_ms":5776.4,"packed_ms":2913.3,"packed_cached_ms":2862.5,"unpacked_tokens":3835,"packed_tokens":1694}
{"backend":"native","state_sentences":4,"questions":1,"unpacked_ms":514.4,"packed_ms":499.5,"packed_cached_ms":323.5,"unpacked_tokens":151,"packed_tokens":152}
{"backend":"native","state_sentences":4,"questions":2,"unpacked_ms":803.5,"packed_ms":523.9,"packed_cached_ms":318.3,"unpacked_tokens":311,"packed_tokens":182}
{"backend":"native","state_sentences":4,"questions":4,"unpacked_ms":1210.8,"packed_ms":595.0,"packed_cached_ms":431.2,"unpacked_tokens":619,"packed_tokens":230}
{"backend":"native","state_sentences":4,"questions":8,"unpacked_ms":2183.0,"packed_ms":782.3,"packed_cached_ms":609.5,"unpacked_tokens":1247,"packed_tokens":338}
{"backend":"native","state_sentences":4,"questions":16,"unpacked_ms":4248.2,"packed_ms":1191.2,"packed_cached_ms":994.7,"unpacked_tokens":2491,"packed_tokens":542}
{"backend":"native","state_sentences":4,"questions":64,"unpacked_ms":16224.0,"packed_ms":3399.2,"packed_cached_ms":3229.0,"unpacked_tokens":9979,"packed_tokens":1790}
{"backend":"native","state_sentences":12,"questions":1,"unpacked_ms":1162.1,"packed_ms":1144.8,"packed_cached_ms":331.2,"unpacked_tokens":407,"packed_tokens":408}
{"backend":"native","state_sentences":12,"questions":2,"unpacked_ms":2071.5,"packed_ms":1220.0,"packed_cached_ms":375.9,"unpacked_tokens":823,"packed_tokens":438}
{"backend":"native","state_sentences":12,"questions":4,"unpacked_ms":3889.6,"packed_ms":1340.0,"packed_cached_ms":521.1,"unpacked_tokens":1643,"packed_tokens":486}
{"backend":"native","state_sentences":12,"questions":8,"unpacked_ms":7427.5,"packed_ms":1593.6,"packed_cached_ms":770.4,"unpacked_tokens":3295,"packed_tokens":594}
{"backend":"native","state_sentences":12,"questions":16,"unpacked_ms":15620.5,"packed_ms":2102.2,"packed_cached_ms":1503.3,"unpacked_tokens":6587,"packed_tokens":798}
{"backend":"native","state_sentences":12,"questions":64,"unpacked_ms":59179.5,"packed_ms":5076.6,"packed_cached_ms":4331.7,"unpacked_tokens":26363,"packed_tokens":2046}
```

## Step 0: typed-decisions accuracy, 2026-09-24

Serving-path evaluation (`finetune eval laya`, Metal) on s0-eval (152 cases, 760 decisions):

```
== released
{"format": "antfly-laya-eval/v1", "records_sha256": "0996cec5994f3c05e233556b30e674627aa6730f28d84b34b8877f50fc193c2c", "packing": "none", "backend": "metal", "overall": {"decisions": 760, "accuracy": 0.3868421052631579, "soft_ce": 1.30795638096163, "ece": 0.1579487887652297, "ordinal_mae": 0.6562302058198264}, "choice": {"decisions": 228, "accuracy": 0.33771929824561403, "soft_ce": 1.5210421520678148, "ece": 0.14279232108802123, "ordinal_mae": null}, "score": {"decisions": 304, "accuracy": 0.3519736842105263, "soft_ce": 1.4879718930366135, "ece": 0.19068649979798416, "ordinal_mae": 0.6562302058198264}, "noul": {"decisions": 228, "accuracy": 0.4824561403508772, "soft_ce": 0.8548499270887835, "ece": 0.19880408832901406, "ordinal_mae": null}, "prompt_tokens": 211125, "seconds": 53.172325}
== released-packed
{"format": "antfly-laya-eval/v1", "records_sha256": "0996cec5994f3c05e233556b30e674627aa6730f28d84b34b8877f50fc193c2c", "packing": "question", "backend": "metal", "overall": {"decisions": 760, "accuracy": 0.3605263157894737, "soft_ce": 1.337135795768793, "ece": 0.132774419063016, "ordinal_mae": 0.666354491148396}, "choice": {"decisions": 228, "accuracy": 0.19736842105263158, "soft_ce": 1.6675512599335334, "ece": 0.2193197396240736, "ordinal_mae": null}, "score": {"decisions": 304, "accuracy": 0.3092105263157895, "soft_ce": 1.4718076766670365, "ece": 0.11344727913015767, "ordinal_mae": 0.666354491148396}, "noul": {"decisions": 228, "accuracy": 0.5921052631578947, "soft_ce": 0.8271578237397333, "ece": 0.17399306673752635, "ordinal_mae": null}, "prompt_tokens": 81745, "seconds": 37.975024}
== upstream-typed-decisions
{"format": "antfly-laya-eval/v1", "records_sha256": "0996cec5994f3c05e233556b30e674627aa6730f28d84b34b8877f50fc193c2c", "packing": "none", "backend": "metal", "overall": {"decisions": 760, "accuracy": 0.7539473684210526, "soft_ce": 0.8853601253877356, "ece": 0.19298428155873945, "ordinal_mae": 0.24779554366681336}, "choice": {"decisions": 228, "accuracy": 0.7236842105263158, "soft_ce": 1.0261173186789596, "ece": 0.2362423859405936, "ordinal_mae": null}, "score": {"decisions": 304, "accuracy": 0.6875, "soft_ce": 1.0553627544088877, "ece": 0.1570452262500399, "ordinal_mae": 0.24779554366681336}, "noul": {"decisions": 228, "accuracy": 0.8728070175438597, "soft_ce": 0.517932760068309, "ece": 0.19764491758848482, "ordinal_mae": null}, "prompt_tokens": 211125, "seconds": 75.273512}
== packed-finetune
{"format": "antfly-laya-eval/v1", "records_sha256": "0996cec5994f3c05e233556b30e674627aa6730f28d84b34b8877f50fc193c2c", "packing": "question", "backend": "metal", "overall": {"decisions": 760, "accuracy": 0.5736842105263158, "soft_ce": 1.026462118628347, "ece": 0.06338861419966349, "ordinal_mae": 0.47073215398124474}, "choice": {"decisions": 228, "accuracy": 0.6052631578947368, "soft_ce": 1.1576869213269179, "ece": 0.12279918450012542, "ordinal_mae": null}, "score": {"decisions": 304, "accuracy": 0.48026315789473684, "soft_ce": 1.2315875480013918, "ece": 0.041850975469539003, "ordinal_mae": 0.47073215398124474}, "noul": {"decisions": 228, "accuracy": 0.6666666666666666, "soft_ce": 0.6217367434323828, "ece": 0.044915111964209053, "ordinal_mae": null}, "prompt_tokens": 81745, "seconds": 92.540653}
== unpacked-finetune
{"format": "antfly-laya-eval/v1", "records_sha256": "0996cec5994f3c05e233556b30e674627aa6730f28d84b34b8877f50fc193c2c", "packing": "none", "backend": "metal", "overall": {"decisions": 760, "accuracy": 0.5723684210526315, "soft_ce": 1.0072466132111706, "ece": 0.055086075279273485, "ordinal_mae": 0.4607488314655041}, "choice": {"decisions": 228, "accuracy": 0.6271929824561403, "soft_ce": 1.1254137490200218, "ece": 0.10696306916182498, "ordinal_mae": null}, "score": {"decisions": 304, "accuracy": 0.46710526315789475, "soft_ce": 1.2132717188316264, "ece": 0.04945284049761921, "ordinal_mae": 0.4607488314655041}, "noul": {"decisions": 228, "accuracy": 0.6578947368421053, "soft_ce": 0.6143793365750404, "ece": 0.08476328745222925, "ordinal_mae": null}, "prompt_tokens": 211125, "seconds": 143.770676}
```

Training reports:

```
{"packing": "question", "train_examples": 400, "train_records": 2000, "temperature": [1.0232930183410645, 1, 1.4454398155212402], "optimizer": {"optimizer_step": 400, "microbatch_step": 400}, "host_peak_bytes": 11167714964, "run_sha256": "af26dc1fdd769317158043b3ba2ab4ccfbe715bb6d9668a974f1f0bc740f9b77"}
{"packing": "none", "train_examples": 2000, "train_records": 2000, "temperature": [1.1481536626815796, 1, 1.5135612487792969], "optimizer": {"optimizer_step": 400, "microbatch_step": 2000}, "host_peak_bytes": 10607865574, "run_sha256": "969a37be6ccbe55f8eed6181331d912e4e9b9a8f6bee55ca051026fb34d4ae8f"}
train question exit 0 seconds 3343
train unpacked exit 0 seconds 11363
```

Packed training throughput by batch size (median step, steps 3+):

```
bs=1 exit 0
  steps 48 median step 5.7s -> 0.88 decisions/s
bs=4 exit 0
  steps 12 median step 17.2s -> 1.16 decisions/s
bs=8 exit 0
  steps 6 median step 36.5s -> 1.10 decisions/s

```
