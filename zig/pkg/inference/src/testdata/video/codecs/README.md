# MP4 codec qualification fixtures

These fixtures exercise EmbeddingGemma2 CUDA NVDEC with progressive 8-bit SDR
H.264 (`avc1`), HEVC Main (`hvc1`/`hev1`), and AV1 Main (`av01`). The 10-bit
HEVC/AV1 files are rejection controls. `hevc-aac.mp4` carries the same HEVC
pictures with AAC audio: video embedding ignores the audio track, while an
explicit audio request decodes it.

`hevc-hev1-inband.mp4` supplies VPS/SPS/PPS only in timed samples, with no
parameter arrays in `hvcC`. `hevc-avc-tracks.mp4` places the four-second
192x192 HEVC track before a one-second 32x32 AVC track. Contract preflight
must charge at least the HEVC track's pixels even though portable inspection
selects the smaller AVC track. Both fixtures participate in CUDA pixel parity,
codec switching, and cancellation checks. Their receipt rows record their
own generator hash and FFmpeg library versions.

`receipt.json` records file hashes, PyAV/FFmpeg versions, generation source hash,
all reference presentation timestamps, and independent FFmpeg YUV420 hashes for
the frames selected at 1 FPS with a 32-frame cap. The video pattern and AAC-LC
sine waves, silence, quiet tones, transients, and musical sequence are generated
locally. English/Spanish speech inputs come from the repository's synthetic
`zig/e2e/inference/testdata` WAV fixtures, resampled to stereo 48 kHz and cropped
or zero-padded to four seconds before AAC encoding; source hashes and paths are
recorded in the receipt. HE-AAC v1/v2 controls are copied from the
repository's owned audio conformance fixtures; their source paths are recorded
in the receipt. Reference WAV files contain native-rate mono IEEE F32 samples
from FFmpeg, avoiding a second quantization step.

From the repository root, with PyAV installed from the pinned requirements:

```sh
python3 zig/pkg/inference/scripts/embeddinggemma2/generate_mp4_codec_fixtures.py --help
```

The default generates four-second, 192x192 clips. Use `--duration-seconds 60`
with an output directory under `.benchmark-results/` for the longer qualification
cohort. Runtime decoding does not depend on PyAV or FFmpeg.

The HTTP qualification runner compares each AAC embedding against the matching
independent F32 WAV using fixed cosine >= 0.995 and max component error <= 0.01
bounds. It also checks independent-reference top-1 retrieval and rejects
mismatched references across the silence/transient/music/speech cohort. PCM error
is checked separately (mean absolute error <= 1e-4 for all, plus SNR >= 50 dB for
the owned HE-AAC/PS fixtures). Exact native-decoded-PCM embedding equivalence is
an additional check, never a replacement for the independent reference gate.
