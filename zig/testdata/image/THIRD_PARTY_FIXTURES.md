# Third-Party Image Fixtures

This directory contains a small number of upstream reference fixtures used for
image decoder conformance. Generated fixtures and first-party fixtures are
listed in `manifest.zon`; the imported upstream fixtures below retain their
upstream license terms.

## WebP Fixtures From libwebp

Files:

- `webp/upstream/libwebp/test.webp`

Source:

- `examples/test.webp` from `webmproject/libwebp`

License:

```text
Copyright (c) 2010, Google Inc. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

  * Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer.

  * Redistributions in binary form must reproduce the above copyright
    notice, this list of conditions and the following disclaimer in
    the documentation and/or other materials provided with the
    distribution.

  * Neither the name of Google nor the names of its contributors may
    be used to endorse or promote products derived from this software
    without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```

## WebP Fixtures From Go x/image

Files:

- `webp/upstream/go_image/blue-purple-pink.lossless.webp`
- `webp/upstream/go_image/blue-purple-pink.lossy.webp`
- `webp/upstream/go_image/blue-purple-pink-large.no-filter.lossy.webp`
- `webp/upstream/go_image/blue-purple-pink-large.normal-filter.lossy.webp`
- `webp/upstream/go_image/blue-purple-pink-large.simple-filter.lossy.webp`
- `webp/upstream/go_image/video-001.lossy.webp`
- `webp/upstream/go_image/yellow_rose.lossy-with-alpha.webp`
- `webp/upstream/go_image/yellow_rose.lossy.webp`

Source:

- WebP testdata from `golang.org/x/image`

License:

```text
Copyright 2009 The Go Authors.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
   * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
   * Neither the name of Google LLC nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```

## U.S. Treasury Bulletin JPEG 2000 Regression Fixture

File:

- `jpeg2000/regression/officeqa-1985-page1-2319x3253.j2k`
- SHA-256:
  `7200669239a3fc847d685f6b8e7efbcc556a7b61db9755b1e3d07e0c5c6b9359`

Source:

- Page 1 image XObject from the March 1985 *Treasury Bulletin*, distributed by
  the Federal Reserve Bank of St. Louis:
  `https://fraser.stlouisfed.org/files/docs/publications/tbulletin/1985_03_treasurybulletin.pdf`

License:

- Public-domain work of the United States federal government. The fixture is
  the JPEG 2000 codestream extracted without modification from the source PDF.
