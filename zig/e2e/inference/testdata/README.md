# Inference E2E fixtures

`whisper_quality.wav` is synthetic English speech generated for this test
suite. It says: “The quick brown fox jumps over the lazy dog.” The fixture is
mono, 16-bit PCM at 16 kHz so it exercises transcription quality without a
platform speech-synthesis dependency in Linux CI.
