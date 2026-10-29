These little-endian float32 tensors were generated with Transformers 5.5.0
and NumPy 2.4.4 by `lib/audio/scripts/generate_whisper_reference.py`, using
`WhisperFeatureExtractor._np_extract_fbank_features` without model downloads.
They are independent of Antfly's FFT and BLAS implementations.

Each filename is `<sample-count>-<mel-bins>-<amplitude-divisor>.f32` and contains
`[mel-bins, 100]` features for a one-second context. The input is deterministic
quantized broadband PCM; 733-sample cases exercise the zero-padded tail, and
16000-sample cases exercise right-edge reflection. Divisors 1 and 16 expose
normalization errors that peak-relative normalization would hide.

The first-party fixture digests are reviewed in
`scripts/embedded_asset_licenses.json`; update those records if regeneration
changes the reference tensors. `zig build test-whisper-frontend` from the
inference package runs both production paths and the independent FFT checks.
