# Inference E2E fixtures

`whisper_quality.wav` is synthetic English speech generated for this test
suite. It says: “The quick brown fox jumps over the lazy dog.” The fixture is
mono, 16-bit PCM at 16 kHz so it exercises transcription quality without a
platform speech-synthesis dependency in Linux CI.

`whisper_spanish_quality.wav` is synthetic Spanish speech generated for this
suite. It says: “Buenos días. Esta es una prueba de reconocimiento automático
del idioma.” The fixture is mono, unsigned 8-bit PCM at 8 kHz; the runtime
resampling path converts it to Whisper's 16 kHz input while keeping the checked
in regression fixture small.
