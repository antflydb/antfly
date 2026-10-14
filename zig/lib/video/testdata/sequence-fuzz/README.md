# Real-picture sequence seeds

These two IVF files contain twelve stream-copied packets from the textured
portion near second eight of the maintained 854×480 Sintel VP9 and AV1 corpus.
They preserve interframe references and avoid a corpus made only of opening
black pictures. The packet-tape fuzz harness removes only the IVF file header;
it preserves packet lengths and timestamps for mutation.

Sintel is © Blender Foundation / durian.blender.org, licensed under
[CC BY 3.0](https://creativecommons.org/licenses/by/3.0/).
[Provenance](provenance.json) records source and seed hashes and generation
commands. No runtime codec dependency is added.

`golden.json` records SHA-256 hashes for twelve planar Y/U/V pictures per
stream, generated with FFmpeg 9.0.2's native VP9 and libdav1d AV1 decoders.
`provenance.json` records the complete oracle commands and concatenated raw
picture hashes. Ordinary video CI compares each native decoded picture against
these independent hashes without an FFmpeg runtime dependency.
