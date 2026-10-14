# Stateful VP8 sequence qualification

The ordinary video CI and instrumented fuzz roots now import a VP8 sequence
target. It decodes complete packet tapes through the maintained pure Zig
`Vp8Decoder`, retains four independent padded picture outputs, checks their
geometry and full-plane hashes before/after subsequent interframes and failures,
and verifies them after decoder destruction. A failed core is not reentered.
The testing allocator and shared decode budget cover all retained outputs and
core allocations together. Packet, pixel and control-check limits bound mutation.

The accepted twelve-packet 480p textured Sintel stream matches twelve independent
FFmpeg 9.0.2 native VP8 picture hashes in visible planar Y/U/V order. Deterministic
checks skip alternating output while preserving all later oracle pixels, and
cancel the second packet while keeping the first output alive. No runtime codec
dependency is added. [Seed provenance](../sequence-fuzz/provenance.json) retains
commands, source/oracle hashes and CC BY 3.0 attribution.

The updated sequence suite passes 13/13 on macOS ARM64, pinned Linux ARM64
and Linux x86_64 under architecture emulation. The instrumented VP8 campaign
is recorded separately in the [receipt](receipt.json); pending jobs are not qualified. The x86_64 Linux executable is statically linked
and packaged in a scratch image; execution on this ARM64 host uses Docker's
architecture emulation, not native x86 hardware performance qualification.

From `zig/`, reproduce with the repository compiler:

```sh
zig build test-video -Doptimize=Debug -Dvideo-test-filter=sequence --summary all
python3 lib/video/scripts/qualify_sequence_fuzz.py --zig /path/to/zig \
  --target vp8 --iterations 10000 --output /tmp/vp8-sequence-campaign
```

The maintained runner now invokes VP8 separately alongside VP9, AV1 and Annex B.
It hashes all video and image Zig sources, including the maintained VP8 core.
Bounded real-picture campaigns add qualification, not exhaustive stream/tool
coverage. The complete non-CUDA objective remains active.
