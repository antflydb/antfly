# Maintained AAC enhancement core

The PacketVideo scalar algorithm is maintained here as Zig source under its
original Apache-2.0 license. See [source notices](SOURCE_NOTICES.md) for the pin.

The initial offline translation expanded macros independently in each source
unit before merging declarations; this preserves per-file fixed-point scales.
Header prototypes resolve to their one Zig definition. Macro helper modules,
C exports and libc byte operations were removed.

Antfly changes use complex QMF synthesis for stereo and mono, define mathematical
shifts for zero-energy bands with large exponents, and replace pointer-bearing
PS scratch overlays with explicitly owned native arrays. Hybrid history copies
use sample widths rather than the 32-bit source's pointer-width assumption.
TNS bottom bands clamp at zero before table lookup, and malformed additional SBR
elements fail before indexing the single-element enhancement lane.

The shared AAC batch and streaming boundaries route actual SBR/PS payloads here.
The wrapper owns native-aligned context, bounded input/lookahead, output and PS
pointer trees. Fixed-point MAC and QMF sample additions explicitly wrap as in the
reference's ARM arithmetic; syntax/index/control arithmetic retains runtime checks.
Failures retire the decoder before any retry can reuse partially advanced state.

TNS feedback uses a wrapping sum followed by one subtraction from the input.
This preserves the fixed-point recurrence while avoiding an LLVM optimized
subtract-reduction sign inversion observed in macOS AArch64 ReleaseFast builds.
Signed-impulse and independent wide-integer recurrence regressions run in
ReleaseFast through inference's `test-audio-tns` target, including default and
audio-specific test suites.

Five owned 44.1/48 kHz HE-AAC/PS fixtures compare complete trimmed PCM against
independent FFmpeg output at a 50 dB SNR floor. See [AUDIO.md](../../AUDIO.md) for
the bounded profile contract and regression commands. This is conformance evidence
for that subset, not a claim of universal HE-AAC support.
