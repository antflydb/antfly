This directory contains a small checked-in multi-codec audio corpus for
`lib/audio`.

Most fixtures are derived from `../tone.wav` with FFmpeg and intentionally keep
the same 1-second tone content while widening codec/container coverage. The
AAC tool fixtures intentionally use transient/noise sources instead so they can
exercise real PNS/TNS/gain/short-window shapes:

Current status:

- The shared non-MP3 passing/decode-but-not-claimed/unsupported corpus metadata
  lives in `lib/audio/src/conformance.zig` and is wired through
  `lib/audio/src/mod.zig`.
- The shared passing lane currently covers all encoded fixtures checked into
  this directory.
- The shared decode-but-not-claimed lane is currently empty.
- The shared unsupported lane is intentionally separate from this directory and
  currently only covers synthetic unknown bytes in code.
- The `lib/audio` tests verify both that every encoded fixture here is present
  in the shared passing table and that the fixture list documented in this file
  stays aligned with the files on disk.
- `zig build lib-audio-conformance-run` runs the external Xiph-family and
  MP3/AAC/MP4 conformance sweeps against already-fetched corpora. Use
  `zig build lib-audio-conformance-fetch` once to populate the external
  fixtures, or `zig build lib-audio-conformance` to fetch and run them.

- `tone-stereo.aac`
  - generated as ADTS AAC-LC at 16 kHz stereo
- `tone-mono-44k.aac`
  - generated as ADTS AAC-LC at 44.1 kHz mono
- `tone-mono-44k.m4a`
  - same AAC-LC content in an MP4/M4A container at 44.1 kHz mono
- `tone-mono-44k.mp4`
  - same AAC-LC content in a generic `isom` MP4 container at 44.1 kHz mono
- `transient-mono-44k-pns.aac`
  - generated as low-bitrate ADTS AAC-LC at 44.1 kHz mono from a transient
    pulse train
  - this intentionally exercises real perceptual noise substitution (PNS)
    bands in the pure-Zig AAC lane
- `noise-mono-44k-tns-gain.aac`
  - generated as low-bitrate ADTS AAC-LC at 44.1 kHz mono from white noise
  - this intentionally exercises real temporal noise shaping (TNS) frames and
    gain-control payload skipping in the pure-Zig AAC lane
- `noise-stereo-44k-tns.aac`
  - generated as low-bitrate ADTS AAC-LC at 44.1 kHz stereo from white noise
  - this intentionally exercises real low-bitrate stereo CPE TNS alignment in
    the pure-Zig AAC lane
- `transient-mono-44k-short.aac`
  - generated as ADTS AAC-LC at 44.1 kHz mono from a transient pulse train
  - this intentionally exercises real `eight_short` AAC window sequences
- `transient-mono-44k-short.m4a`
  - same transient AAC-LC content in an MP4/M4A container at 44.1 kHz mono
  - this intentionally exercises the same real `eight_short` AAC window
    sequences through the Zig MP4 demux path
- `transient-mono-44k-short.mp4`
  - same transient AAC-LC content in a generic `isom` MP4 container at 44.1
    kHz mono
  - this intentionally exercises the same real `eight_short` AAC window
    sequences through the Zig MP4 demux path without an M4A-specific brand
- `transient-stereo-44k-short.aac`
  - generated as ADTS AAC-LC at 44.1 kHz stereo from a transient pulse train
  - this intentionally exercises real stereo `CPE` `eight_short` AAC window
    sequences
- `transient-stereo-44k-short.m4a`
  - same transient AAC-LC stereo content in an MP4/M4A container at 44.1 kHz
  - this intentionally exercises the same real stereo `CPE` `eight_short`
    window sequences through the Zig MP4 demux path
- `transient-stereo-44k-short.mp4`
  - same transient AAC-LC stereo content in a generic `isom` MP4 container at
    44.1 kHz
  - this intentionally exercises the same real stereo `CPE` `eight_short`
    window sequences through the Zig MP4 demux path without an M4A-specific
    brand
- `tone-stereo.m4a`
  - generated as AAC-LC in an MP4/M4A container at 16 kHz stereo
- `tone-stereo-12k-sbr.m4a`
  - generated as low-bitrate AAC in an MP4/M4A container at 16 kHz stereo
  - this intentionally exposes a real MP4 `AudioSpecificConfig` with
    `sbr_present=true`, while the packetized AAC-LC core still decodes through
    the pure-Zig AAC lane
  - this exercises the narrow supported “LC core plus SBR sync extension”
    compatibility shape, not general HE-AAC reconstruction
- `tone-stereo.mp4`
  - generated as AAC-LC in a generic `isom` MP4 container at 16 kHz stereo
  - this now exercises direct claimed MP4-container support rather than a
    hint-only fallback lane
- `tone-stereo-alac.m4a`
  - generated as ALAC in an MP4/M4A container at 16 kHz stereo
  - this intentionally exercises the Zig MP4 demux path on ALAC, not just AAC
- `tone-stereo-alac-24bit.m4a`
  - generated as 24-bit ALAC in an MP4/M4A container at 16 kHz stereo
  - this intentionally widens the checked-in pure-Zig ALAC lane beyond the 16-bit fixture
- `tone-stereo-alac.mp4`
  - same ALAC content in a generic `isom` MP4 container at 16 kHz stereo
  - this intentionally exercises the same Zig MP4 demux path without an
    M4A-specific brand
- `tone-stereo-alac-24bit.mp4`
  - same 24-bit ALAC content in a generic `isom` MP4 container at 16 kHz stereo
- `tone-stereo-alac-24bit.caf`
  - generated as 24-bit ALAC in CAF at 16 kHz stereo
  - this intentionally widens the checked-in pure-Zig CAF ALAC lane beyond the 16-bit fixture
- `tone-stereo.ogg`
  - generated as Ogg/Vorbis at 16 kHz stereo
- `tone-stereo.oga`
  - same Vorbis payload as the canonical Ogg fixture, but through the `.oga`
    audio-first extension
- `tone-stereo.opus`
  - generated as Ogg/Opus stereo; FFmpeg reports 48 kHz decode output as
    expected for Opus
- `tone-mono-48k.opus`
  - generated as Ogg/Opus mono at 48 kHz
- `probe-celt-mono-48k-5ms.opus`
  - generated as mono Opus at 48 kHz with 5 ms packets
  - libopus switches this into restricted low-delay mode, which intentionally
    exercises the checked-in short-packet CELT lane
- `probe-celt-mono-48k-120ms.opus`
  - generated as mono Opus at 48 kHz with 120 ms packets
  - this intentionally exercises a real long multi-frame CELT packet shape
    instead of only shorter packetized checked-in coverage
- `probe-celt-stereo-48k-2p5ms.opus`
  - generated as stereo Opus at 48 kHz with 2.5 ms packets
  - libopus switches this into restricted low-delay mode, which intentionally
    exercises the checked-in shortest-packet stereo CELT lane
- `probe-celt-stereo-48k-40ms.opus`
  - generated as stereo Opus at 48 kHz with 40 ms packets
  - this intentionally exercises a real multi-frame CELT packet shape instead
    of only the older 20 ms single-frame checked-in lane
- `probe-celt-stereo-48k-60ms.opus`
  - generated as stereo Opus at 48 kHz with 60 ms packets
  - this intentionally exercises a real longer multi-frame CELT packet shape
    beyond the checked-in 40 ms packetized lane
- `tone-stereo-opus.ogg`
  - Opus payload in an `.ogg` filename, to exercise Ogg/Opus alias handling
- `probe-silk-mono-16k.ogg`
  - generated as a low-bitrate mono Ogg/Opus stream that lands on the SILK path
  - this intentionally exercises the pure-Zig SILK runtime lane through a real
    Ogg container, not just packet-level probes
- `probe-silk-mono-16k-fec.ogg`
  - generated as a low-bitrate mono Ogg/Opus stream on the SILK path with
    in-band FEC enabled
  - this intentionally exercises LBRR-bearing SILK packets, where the
    pure-Zig decoder must parse and skip redundant FEC frames before decoding
    the regular frame
- `probe-silk-mono-16k-fec-10ms.ogg`
  - generated as a low-bitrate mono Ogg/Opus stream on the SILK path with
    in-band FEC enabled and 10 ms packet duration
  - this intentionally exercises narrow single-internal-frame SILK packets
    that still carry LBRR/FEC side data
- `probe-silk-mono-16k-fec-40ms.ogg`
  - generated as a low-bitrate mono Ogg/Opus stream on the SILK path with
    in-band FEC enabled and 40 ms packet duration
  - this intentionally exercises multi-internal-frame SILK packets that also
    carry LBRR/FEC side data
- `probe-silk-mono-16k-fec-60ms.ogg`
  - generated as a low-bitrate mono Ogg/Opus stream on the SILK path with
    in-band FEC enabled and 60 ms packet duration
  - this intentionally exercises three-internal-frame SILK packets that also
    carry LBRR/FEC side data
- `probe-silk-mono-16k-10ms.ogg`
  - generated as a low-bitrate mono Ogg/Opus stream on the SILK path with 10 ms
    packet duration
  - this intentionally exercises the pure-Zig 10 ms SILK synthesis/runtime lane
- `probe-silk-mono-16k-60ms.ogg`
  - generated as a low-bitrate mono Ogg/Opus stream on the SILK path with 60 ms
    packet duration
  - this intentionally exercises the pure-Zig multi-internal-frame SILK runtime
    lane through a real Ogg fixture
- `probe-silk-stereo-16k-10ms.ogg`
  - generated as a low-bitrate stereo Ogg/Opus stream on the WB SILK path with
    10 ms packet duration
  - this intentionally exercises the real stereo 10 ms SILK lane through a
    checked-in Ogg fixture
- `probe-silk-stereo-16k.ogg`
  - generated as a low-bitrate stereo Ogg/Opus stream that stays on the WB
    SILK path
  - this intentionally exercises the real stereo SILK runtime lane through a
    checked-in Ogg fixture, not only synthetic packet probes
- `probe-silk-stereo-16k-fec.ogg`
  - generated as a low-bitrate stereo Ogg/Opus stream on the WB SILK path with
    in-band FEC enabled
  - this intentionally exercises stereo LBRR-bearing SILK packets on the
    pure-Zig runtime lane
- `probe-silk-stereo-16k-fec-10ms.ogg`
  - generated as a low-bitrate stereo Ogg/Opus stream on the WB SILK path with
    in-band FEC enabled and 10 ms packet duration
  - this intentionally exercises stereo single-internal-frame SILK packets
    that still carry LBRR/FEC side data
- `probe-silk-stereo-16k-fec-40ms.ogg`
  - generated as a low-bitrate stereo Ogg/Opus stream on the WB SILK path with
    in-band FEC enabled and 40 ms packet duration
  - this intentionally exercises stereo multi-internal-frame SILK packets that
    also carry LBRR/FEC side data
- `probe-silk-stereo-16k-fec-60ms.ogg`
  - generated as a low-bitrate stereo Ogg/Opus stream on the WB SILK path with
    in-band FEC enabled and 60 ms packet duration
  - this intentionally exercises stereo three-internal-frame SILK packets that
    also carry LBRR/FEC side data
- `probe-silk-stereo-16k-40ms.ogg`
  - generated as a low-bitrate stereo Ogg/Opus stream on the WB SILK path with
    40 ms packet duration
  - this intentionally exercises the real two-internal-frame stereo SILK lane
    through a checked-in Ogg fixture
- `probe-hybrid-mono-48k.ogg`
  - generated as a low-bitrate mono Ogg/Opus stream that lands on the hybrid
    SILK+CELT path
  - this intentionally exercises the real mono hybrid runtime lane through a
    checked-in Ogg fixture, not only synthetic packet probes
- `probe-hybrid-mono-48k-10ms.ogg`
  - generated as a low-bitrate mono Ogg/Opus stream on the hybrid SILK+CELT
    path with 10 ms packet duration
  - this intentionally exercises the real mono 10 ms hybrid runtime lane
- `probe-hybrid-mono-48k-fec-10ms.ogg`
  - generated as a low-bitrate mono Ogg/Opus stream on the hybrid SILK+CELT
    path with in-band FEC enabled and 10 ms packet duration
  - this intentionally exercises hybrid packets where the SILK front carries
    LBRR/FEC side data before the CELT highband
- `probe-hybrid-mono-48k-fec.ogg`
  - generated as a low-bitrate mono Ogg/Opus stream on the hybrid SILK+CELT
    path with in-band FEC enabled
  - this intentionally exercises 20 ms hybrid packets where the SILK front
    carries LBRR/FEC side data before the CELT highband
- `probe-hybrid-stereo-48k.ogg`
  - generated as a low-bitrate stereo Ogg/Opus stream that lands on the hybrid
    SILK+CELT path
  - this intentionally exercises the pure-Zig hybrid runtime lane through a
    real Ogg container, not just packet-level probes
- `probe-hybrid-stereo-48k-10ms.ogg`
  - generated as a low-bitrate stereo Ogg/Opus stream on the hybrid SILK+CELT
    path with 10 ms packet duration
  - this intentionally exercises the real stereo 10 ms hybrid runtime lane
- `probe-hybrid-stereo-48k-fec-10ms.ogg`
  - generated as a low-bitrate stereo Ogg/Opus stream on the hybrid SILK+CELT
    path with in-band FEC enabled and 10 ms packet duration
  - this intentionally exercises stereo hybrid packets where the SILK front
    carries LBRR/FEC side data before the CELT highband
- `probe-hybrid-stereo-48k-fec.ogg`
  - generated as a low-bitrate stereo Ogg/Opus stream on the hybrid SILK+CELT
    path with in-band FEC enabled
  - this intentionally exercises stereo 20 ms hybrid packets where the SILK
    front carries LBRR/FEC side data before the CELT highband
- `tone-stereo.flac`
  - generated as FLAC at 16 kHz stereo
- `tone-stereo-24bit.flac`
  - generated as 24-bit FLAC at 16 kHz stereo
- `tone-stereo-flac.ogg`
  - FLAC payload muxed in Ogg, to exercise non-Vorbis/non-Opus Ogg-family
    decode
- `tone-stereo.aiff`
  - generated as AIFF/PCM16 stereo at 16 kHz
- `tone-stereo.caf`
  - generated as CAF/ALAC stereo at 16 kHz

Generation commands used on this machine:

```sh
ffmpeg -y -i lib/audio/testdata/tone.wav -ac 2 -c:a aac -b:a 96k -f adts lib/audio/testdata/codec-corpus/tone-stereo.aac
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 44100 -ac 1 -c:a aac -b:a 96k -f adts lib/audio/testdata/codec-corpus/tone-mono-44k.aac
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 44100 -ac 1 -c:a aac -profile:a aac_low -b:a 96k lib/audio/testdata/codec-corpus/tone-mono-44k.m4a
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 44100 -ac 1 -c:a aac -profile:a aac_low -b:a 96k lib/audio/testdata/codec-corpus/tone-mono-44k.mp4
ffmpeg -y -f lavfi -i "sine=frequency=1000:duration=1:sample_rate=44100" -af "volume='if(lt(mod(t,0.1),0.005),1,0.05)'" -ac 1 -c:a aac -profile:a aac_low -b:a 24k -f adts lib/audio/testdata/codec-corpus/transient-mono-44k-pns.aac
ffmpeg -y -f lavfi -i "anoisesrc=color=white:duration=1:sample_rate=44100" -ac 1 -c:a aac -profile:a aac_low -b:a 16k -f adts lib/audio/testdata/codec-corpus/noise-mono-44k-tns-gain.aac
ffmpeg -y -f lavfi -i "anoisesrc=color=white:duration=1:sample_rate=44100" -ac 2 -c:a aac -profile:a aac_low -b:a 16k -f adts lib/audio/testdata/codec-corpus/noise-stereo-44k-tns.aac
ffmpeg -y -f lavfi -i "aevalsrc='if(lt(mod(t,0.1),0.002),0.95*sin(2*PI*3000*t),0)':s=44100:d=1" -c:a aac -profile:a aac_low -b:a 96k -f adts lib/audio/testdata/codec-corpus/transient-mono-44k-short.aac
ffmpeg -y -f lavfi -i "aevalsrc='if(lt(mod(t,0.1),0.002),0.95*sin(2*PI*3000*t),0)':s=44100:d=1" -c:a aac -profile:a aac_low -b:a 96k lib/audio/testdata/codec-corpus/transient-mono-44k-short.m4a
ffmpeg -y -f lavfi -i "aevalsrc='if(lt(mod(t,0.1),0.002),0.95*sin(2*PI*3000*t),0)':s=44100:d=1" -c:a aac -profile:a aac_low -b:a 96k lib/audio/testdata/codec-corpus/transient-mono-44k-short.mp4
ffmpeg -y -f lavfi -i "aevalsrc='if(lt(mod(t,0.1),0.002),0.95*sin(2*PI*3000*t),0)|if(lt(mod(t,0.1),0.002),0.95*sin(2*PI*2400*t),0)':s=44100:d=1" -c:a aac -profile:a aac_low -b:a 128k -f adts lib/audio/testdata/codec-corpus/transient-stereo-44k-short.aac
ffmpeg -y -f lavfi -i "aevalsrc='if(lt(mod(t,0.1),0.002),0.95*sin(2*PI*3000*t),0)|if(lt(mod(t,0.1),0.002),0.95*sin(2*PI*2400*t),0)':s=44100:d=1" -c:a aac -profile:a aac_low -b:a 128k lib/audio/testdata/codec-corpus/transient-stereo-44k-short.m4a
ffmpeg -y -f lavfi -i "aevalsrc='if(lt(mod(t,0.1),0.002),0.95*sin(2*PI*3000*t),0)|if(lt(mod(t,0.1),0.002),0.95*sin(2*PI*2400*t),0)':s=44100:d=1" -c:a aac -profile:a aac_low -b:a 128k lib/audio/testdata/codec-corpus/transient-stereo-44k-short.mp4
ffmpeg -y -i lib/audio/testdata/tone.wav -ac 2 -c:a aac -b:a 96k lib/audio/testdata/codec-corpus/tone-stereo.m4a
ffmpeg -y -i lib/audio/testdata/tone.wav -ac 2 -c:a aac -b:a 12k lib/audio/testdata/codec-corpus/tone-stereo-12k-sbr.m4a
ffmpeg -y -i lib/audio/testdata/tone.wav -ac 2 -c:a aac -b:a 96k lib/audio/testdata/codec-corpus/tone-stereo.mp4
ffmpeg -y -i lib/audio/testdata/tone.wav -ac 2 -c:a alac lib/audio/testdata/codec-corpus/tone-stereo-alac.m4a
ffmpeg -y -i lib/audio/testdata/tone.wav -ac 2 -c:a alac lib/audio/testdata/codec-corpus/tone-stereo-alac.mp4
ffmpeg -y -i lib/audio/testdata/tone.wav -af aformat=sample_fmts=s32p -ac 2 -c:a alac lib/audio/testdata/codec-corpus/tone-stereo-alac-24bit.m4a
ffmpeg -y -i lib/audio/testdata/tone.wav -af aformat=sample_fmts=s32p -ac 2 -c:a alac lib/audio/testdata/codec-corpus/tone-stereo-alac-24bit.mp4
ffmpeg -y -i lib/audio/testdata/tone.wav -af aformat=sample_fmts=s32p -ac 2 -c:a alac lib/audio/testdata/codec-corpus/tone-stereo-alac-24bit.caf
ffmpeg -y -i lib/audio/testdata/tone.wav -ac 2 -c:a vorbis -strict -2 -q:a 5 lib/audio/testdata/codec-corpus/tone-stereo.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ac 2 -c:a vorbis -strict -2 -q:a 5 lib/audio/testdata/codec-corpus/tone-stereo.oga
ffmpeg -y -i lib/audio/testdata/tone.wav -ac 2 -c:a libopus -b:a 96k lib/audio/testdata/codec-corpus/tone-stereo.opus
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 48000 -ac 1 -c:a libopus -b:a 64k lib/audio/testdata/codec-corpus/tone-mono-48k.opus
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 48000 -ac 1 -c:a libopus -application audio -frame_duration 5 -b:a 64k lib/audio/testdata/codec-corpus/probe-celt-mono-48k-5ms.opus
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 48000 -ac 1 -c:a libopus -application audio -frame_duration 120 -b:a 64k lib/audio/testdata/codec-corpus/probe-celt-mono-48k-120ms.opus
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 48000 -ac 2 -c:a libopus -application audio -frame_duration 2.5 -b:a 96k lib/audio/testdata/codec-corpus/probe-celt-stereo-48k-2p5ms.opus
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 48000 -ac 2 -c:a libopus -application audio -frame_duration 40 -b:a 96k lib/audio/testdata/codec-corpus/probe-celt-stereo-48k-40ms.opus
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 48000 -ac 2 -c:a libopus -application audio -frame_duration 60 -b:a 96k lib/audio/testdata/codec-corpus/probe-celt-stereo-48k-60ms.opus
ffmpeg -y -i lib/audio/testdata/tone.wav -ac 2 -c:a libopus -b:a 96k lib/audio/testdata/codec-corpus/tone-stereo-opus.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 16000 -ac 1 -c:a libopus -application voip -b:a 12k lib/audio/testdata/codec-corpus/probe-silk-mono-16k.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 16000 -ac 1 -c:a libopus -application voip -b:a 12k -vbr off -frame_duration 20 -packet_loss 15 -fec 1 lib/audio/testdata/codec-corpus/probe-silk-mono-16k-fec.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 16000 -ac 1 -c:a libopus -application voip -b:a 12k -vbr off -frame_duration 10 -packet_loss 15 -fec 1 lib/audio/testdata/codec-corpus/probe-silk-mono-16k-fec-10ms.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 16000 -ac 1 -c:a libopus -application voip -b:a 12k -vbr off -frame_duration 40 -packet_loss 15 -fec 1 lib/audio/testdata/codec-corpus/probe-silk-mono-16k-fec-40ms.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 16000 -ac 1 -c:a libopus -application voip -b:a 12k -vbr off -frame_duration 60 -packet_loss 15 -fec 1 lib/audio/testdata/codec-corpus/probe-silk-mono-16k-fec-60ms.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 16000 -ac 1 -c:a libopus -application voip -frame_duration 10 -b:a 12k lib/audio/testdata/codec-corpus/probe-silk-mono-16k-10ms.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 16000 -ac 1 -c:a libopus -application voip -frame_duration 60 -b:a 12k lib/audio/testdata/codec-corpus/probe-silk-mono-16k-60ms.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 16000 -ac 2 -c:a libopus -application voip -frame_duration 10 -b:a 20k lib/audio/testdata/codec-corpus/probe-silk-stereo-16k-10ms.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 16000 -ac 2 -c:a libopus -application voip -b:a 24k lib/audio/testdata/codec-corpus/probe-silk-stereo-16k.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 16000 -ac 2 -af aformat=channel_layouts=stereo -c:a libopus -application voip -b:a 24k -vbr off -frame_duration 20 -packet_loss 15 -fec 1 lib/audio/testdata/codec-corpus/probe-silk-stereo-16k-fec.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 16000 -ac 2 -af aformat=channel_layouts=stereo -c:a libopus -application voip -b:a 24k -vbr off -frame_duration 10 -packet_loss 15 -fec 1 lib/audio/testdata/codec-corpus/probe-silk-stereo-16k-fec-10ms.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 16000 -ac 2 -af aformat=channel_layouts=stereo -c:a libopus -application voip -b:a 24k -vbr off -frame_duration 40 -packet_loss 15 -fec 1 lib/audio/testdata/codec-corpus/probe-silk-stereo-16k-fec-40ms.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 16000 -ac 2 -af aformat=channel_layouts=stereo -c:a libopus -application voip -b:a 24k -vbr off -frame_duration 60 -packet_loss 15 -fec 1 lib/audio/testdata/codec-corpus/probe-silk-stereo-16k-fec-60ms.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 16000 -ac 2 -c:a libopus -application voip -frame_duration 40 -b:a 24k lib/audio/testdata/codec-corpus/probe-silk-stereo-16k-40ms.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 48000 -ac 1 -c:a libopus -application audio -frame_duration 10 -b:a 20k lib/audio/testdata/codec-corpus/probe-hybrid-mono-48k-10ms.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 48000 -ac 1 -c:a libopus -application audio -b:a 20k -vbr off -frame_duration 10 -packet_loss 15 -fec 1 lib/audio/testdata/codec-corpus/probe-hybrid-mono-48k-fec-10ms.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 48000 -ac 1 -c:a libopus -application audio -b:a 20k -vbr off -frame_duration 20 -packet_loss 15 -fec 1 lib/audio/testdata/codec-corpus/probe-hybrid-mono-48k-fec.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 48000 -ac 1 -c:a libopus -application audio -b:a 20k lib/audio/testdata/codec-corpus/probe-hybrid-mono-48k.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 48000 -ac 2 -c:a libopus -application audio -frame_duration 10 -b:a 28k lib/audio/testdata/codec-corpus/probe-hybrid-stereo-48k-10ms.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 48000 -ac 2 -af aformat=channel_layouts=stereo -c:a libopus -application audio -b:a 28k -vbr off -frame_duration 10 -packet_loss 15 -fec 1 lib/audio/testdata/codec-corpus/probe-hybrid-stereo-48k-fec-10ms.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 48000 -ac 2 -af aformat=channel_layouts=stereo -c:a libopus -application audio -b:a 24k -vbr off -frame_duration 20 -packet_loss 15 -fec 1 lib/audio/testdata/codec-corpus/probe-hybrid-stereo-48k-fec.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ar 48000 -ac 2 -c:a libopus -application audio -b:a 24k lib/audio/testdata/codec-corpus/probe-hybrid-stereo-48k.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ac 2 -c:a flac lib/audio/testdata/codec-corpus/tone-stereo.flac
ffmpeg -y -i lib/audio/testdata/tone.wav -ac 2 -sample_fmt s32 -c:a flac lib/audio/testdata/codec-corpus/tone-stereo-24bit.flac
ffmpeg -y -i lib/audio/testdata/tone.wav -ac 2 -c:a flac -f ogg lib/audio/testdata/codec-corpus/tone-stereo-flac.ogg
ffmpeg -y -i lib/audio/testdata/tone.wav -ac 2 -c:a pcm_s16be lib/audio/testdata/codec-corpus/tone-stereo.aiff
ffmpeg -y -i lib/audio/testdata/tone.wav -ac 2 -c:a alac lib/audio/testdata/codec-corpus/tone-stereo.caf
```

The conformance tests in `lib/audio/src/mod.zig` use this corpus to verify:

- format sniffing
- direct claimed MP4/M4A container support for generic `isom` AAC and ALAC
  audio
- MIME and filename aliases for MP4-family audio such as `.m4a`, `.m4b`, and
  `.m4p`
- direct WAV G.711 and RF64/BW64 support through the Zig WAV path, direct signed
  PCM, float, and G.711 companded AIFF/AIFC support through the Zig AIFF path,
  direct AU/SND PCM/float/G.711 support through the Zig AU path, direct
  CAF/LPCM PCM/float and CAF G.711 support through the Zig CAF path, plus
  CAF/ALAC through the Zig CAF demux path, including checked-in pure-Zig ALAC
  cookie parsing, compressed-frame decode, and CAF packet-table remainder
  trimming
- Ogg-family alias/container coverage across Vorbis (`.ogg`, `.oga`), Opus
  (`.opus`, `.ogg`), and FLAC-in-Ogg
- real Ogg/Opus mono/stereo SILK, including checked-in 10 ms SILK synthesis
  coverage plus 40 ms and 60 ms multi-internal-frame SILK runtime coverage,
  plus mono/stereo hybrid runtime coverage including checked-in 10 ms hybrid
  shapes, in addition to the existing CELT-focused Opus corpus
- generated edge coverage across channel/layout and format variants including
  mono AAC, real mono AAC PNS and TNS/gain fixtures, short-window mono AAC in
  ADTS, M4A, and generic MP4, low-bitrate stereo AAC CPE/TNS, short-window
  stereo AAC in ADTS, M4A, and generic MP4, mono long-window AAC in M4A/MP4,
  ALAC in M4A/MP4, mono Opus, 24-bit FLAC, AIFF/PCM, and CAF/ALAC
- generic `lib/audio.decode(...)` dispatch
- interleaved decode shape
- stereo duplication stays coherent
- mono downmix remains close to the original `tone.wav` reference
