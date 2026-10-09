# Third-party notices

## Cisco OpenH264 CAVLC and CABAC tables

The native Zig H.264 decoder includes CAVLC codewords and CABAC context/arithmetic tables generated from
[OpenH264 encoder_data_tables.cpp](https://github.com/cisco/openh264/blob/1a0073f0322c8b74cbcb75ca1bb1c3d19d75538d/codec/encoder/core/src/encoder_data_tables.cpp),
and [OpenH264 common_tables.cpp](https://github.com/cisco/openh264/blob/1a0073f0322c8b74cbcb75ca1bb1c3d19d75538d/codec/common/src/common_tables.cpp),
revision `1a0073f0322c8b74cbcb75ca1bb1c3d19d75538d`.
No OpenH264 runtime is linked.

Copyright (c)  2013, Cisco Systems
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:
* Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in
the documentation and/or other materials provided with the
distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

## H.264 normative mappings

`src/h264_cabac_extensions.zig`, field scans/context increments,
`src/h264_chroma422.zig`, scaling fallback rules and FMO maps are factual mappings
from [ITU-T H.264 (08/2021)](https://www.itu.int/rec/T-REC-H.264-202108-I/en).
The independently written Zig implementation and synthetic fixture generators
remain Apache-2.0. Extended CABAC initial states are aliases into the existing
pinned OpenH264 base table above; that table retains its BSD-2-Clause notice.

## Sintel qualification assets

`testdata/h264-sintel-original.mp4` and `testdata/h264-jm-*.mp4` contain an excerpt
or resized/re-encoded derivatives of Sintel © Blender Foundation /
[durian.blender.org](https://durian.blender.org/about/), under
[Creative Commons Attribution 3.0](https://creativecommons.org/licenses/by/3.0/).
The original excerpt copies video and removes audio; JM derivatives resize frames
and re-encode them for codec qualification. Source hashes, modification details,
attribution and offline regeneration instructions are in [testdata/README.md](testdata/README.md).
These media assets retain CC BY 3.0 rather than the code's Apache-2.0 license.
The official JM reference software is invoked offline but is not vendored or linked.
