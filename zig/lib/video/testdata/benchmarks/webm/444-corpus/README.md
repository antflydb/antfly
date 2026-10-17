# Full Sintel AV1 4:4:4 workload

Sintel copyright Blender Foundation / durian.blender.org, CC BY 3.0:
https://download.blender.org/durian/trailer/sintel_trailer-480p.mp4
https://creativecommons.org/licenses/by/3.0/

This 854×480 AV1 High profile derivative retains all 1,253 displayed pictures
from the pinned 52-second 4:2:0 AV1 corpus. Converting it to 4:4:4 does not add
source chroma detail. `manifest.json` records source/coded hashes, encoder and
probe identities, commands, profile, and independent normalized-plane hashes.
AOM's container reports the last picture timestamp as segment duration;
the probe receipt retains this distinction from cadence-derived picture extent.

Generate from the repository root with an offline reference AOM encoder:

```sh
python3 zig/lib/video/scripts/generate_av1_444_benchmark.py \
  --aomenc /path/to/aomenc --output zig/lib/video/testdata/benchmarks/webm/444-corpus
```

The generator verifies the pinned source and coded profile/count, then hashes
all decoded Y-plus-interleaved-UV pictures using independent FFmpeg planes.
Reference tools are not linked into native inference. The maintained qualifier
checks every native output against those hashes. See
`zig/lib/video/testdata/av1-combined-model-validation` for decode and completed
CPU scalar/SIMD/Metal preparation receipts. These shared-host measurements do not
establish pretrained full-pipeline throughput or model usefulness.
