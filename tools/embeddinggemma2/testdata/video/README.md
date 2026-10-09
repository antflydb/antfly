# Pretrained video oracle

These small original synthetic clips and F32 vectors qualify ordered video groups
against `google/embeddinggemma-2` revision `914f7f89142e33e77833254d9c9b90c3cef7303b`.
The checkpoint itself is not included. `video-oracle.json` records source code
hashes, tool versions, clip/RGB hashes, upstream frame selections, complete token
IDs and independently pooled normalized 768-dimensional vectors.

`video-h264.mp4` is an original moving black/white tile pattern at 640×480, with
eight High/CABAC pictures over two seconds and B-picture reordering.
`video-repeat.mov` encodes one picture with a four-second duration; the processor
selects it four times. `video-mjpeg.mov` is the original Apache-2.0 fixture copied
from `zig/lib/video/testdata/mjpeg.mov`, with its existing generator/provenance.
The oracle decodes H.264 RGB with FFmpeg and independently decodes demuxed JPEG
packets with Pillow/libjpeg, matching the native JPEG RGB policy. FFmpeg default
MJPEG IDCT differs by up to three channel values on the colorful fixture; the
oracle records that difference and both RGB hashes. It then uses the pinned
upstream video processor, F32 model and masked mean/L2 pooling. Native pixels or
preparation are not used to generate expected vectors.

Install PyTorch 2.10.0, Torchvision 0.25.0, NumPy, Pillow and Transformers from
commit `92cd495f2720c064bc78eb2d93e28704c5bce51f` in a separate Python environment.
`reference.py` checks source and checkpoint digests before running the reference.
From the repository root:

```sh
python tools/embeddinggemma2/reference.py acquire /tmp/embeddinggemma2/model
python tools/embeddinggemma2/reference.py video-oracle /tmp/embeddinggemma2/model \
  --output /tmp/embeddinggemma2/oracle/video-oracle.json

cd zig/pkg/inference
ANTFLY_EMBEDDINGGEMMA2_MODEL=/tmp/embeddinggemma2/model \
ANTFLY_EMBEDDINGGEMMA2_VIDEO_ORACLE="$PWD/../../../tools/embeddinggemma2/testdata/video/video-oracle.json" \
ANTFLY_EMBEDDINGGEMMA2_VIDEO_REPORT=/tmp/embeddinggemma2/native.json \
zig build test-embeddinggemma2 -Dmetal=false -Doptimize=ReleaseSafe --summary all -- \
  --test-filter 'embeddinggemma2 pretrained video'

ANTFLY_EMBEDDINGGEMMA2_MODEL=/tmp/embeddinggemma2/model \
ANTFLY_EMBEDDINGGEMMA2_VIDEO_ORACLE="$PWD/../../../tools/embeddinggemma2/testdata/video/video-oracle.json" \
ANTFLY_EMBEDDINGGEMMA2_VIDEO_REPORT=/tmp/embeddinggemma2/metal.json \
ANTFLY_EMBEDDINGGEMMA2_METAL=1 \
zig build test-embeddinggemma2 -Dmetal=true -Doptimize=ReleaseSafe --summary all -- \
  --test-filter 'embeddinggemma2 pretrained video'
```

The opt-in regression checks presentation-frame selection (including reordered
packets and duplicates), token counts, normalized embeddings, text/video order,
multiple videos, and an actual HTTP request with normalized 128-dimensional output.
`ANTFLY_EMBEDDINGGEMMA2_CASE` can select one named case for diagnosis. Without
checkpoint/oracle paths the pretrained test skips explicitly.
