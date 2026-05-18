# libjpeg-turbo Upstream JPEG Fixtures

This directory holds the small checked-in subset of official JPEG fixtures
imported from the `libjpeg-turbo` source tree:

- `testorig.jpg`
- `testimgint.jpg`
- `testimgari.jpg`

Primary source:

- <https://github.com/libjpeg-turbo/libjpeg-turbo/tree/main/testimages>

Raw source URLs used for this import:

- <https://raw.githubusercontent.com/libjpeg-turbo/libjpeg-turbo/main/testimages/testorig.jpg>
- <https://raw.githubusercontent.com/libjpeg-turbo/libjpeg-turbo/main/testimages/testimgint.jpg>
- <https://raw.githubusercontent.com/libjpeg-turbo/libjpeg-turbo/main/testimages/testimgari.jpg>

Notes:

- `testimgint.jpg` is baseline, not progressive.
- `testimgint.jpg` and `testimgari.jpg` decode to the same RGBA pixels in the
  current corpus, which makes them a useful upstream baseline/arithmetic pair.
