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
