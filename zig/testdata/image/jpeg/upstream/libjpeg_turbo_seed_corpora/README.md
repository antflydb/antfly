# libjpeg-turbo Seed-Corpora JPEG Fixtures

This directory holds a small checked-in subset of official JPEG fixtures
imported from the `libjpeg-turbo/seed-corpora` repository.

Primary source:

- <https://github.com/libjpeg-turbo/seed-corpora>

Imported source paths:

- `bugs/decompress/8bit/testorig_grayscale_q80_rst8.jpg`
- `bugs/decompress/8bit/testorig_vflip_410_q10_baseline.jpg`
- `bugs/decompress/8bit/2x2_410.jpg`
- `bugs/decompress/8bit/2x2_420.jpg`
- `bugs/decompress/8bit/8x8_420.jpg`
- `bugs/decompress/8bit/1x1_420.jpg`
- `bugs/decompress/8bit/1x1_444.jpg`
- `bugs/decompress/8bit/testorig_rgb_444_floatdct_q100_test3icc.jpg`
- `bugs/decompress/8bit/testorig_hflip_2x4_q1_rst16.jpg`
- `bugs/decompress/8bit/testorig_rot90_441_q25_scan_script.jpg`
- `bugs/decompress/8bit/testorig_rot270_420_fastdct_q75_prog_rst100.jpg`
- `bugs/decompress/8bit/testorig_transpose_440_q90_ari_rst1.jpg`
- `bugs/decompress/8bit/testorig_transverse_422_q95_prog_ari.jpg`
- `bugs/decompress/8bit/testorig_rot180_cmyk_411_q50_opt_rst1B_test1icc.jpg`
- `bugs/decompress/mozilla_891693_CVE-2013-6629_CVE-2013-6630/kitty2.jpg`
- `bugs/decompress/github_347/overflow1.jpg`
- `bugs/decompress/github_347/overflow2.jpg`
- `bugs/decompress/LJPGT-PT-23-01/test1-8.jpg`
- `bugs/decompress/12bit/random12_100x91_islow_4x1,2x2,1x2_Q100,99,98_rst2.jpg`
- `bugs/decompress/12bit/random12_99x92_ifast_rgb_420_Q90,80,70_smooth50.jpg`
- `bugs/decompress/lossless/random2_92x99_lossless_psv5_pt1.jpg`
- `bugs/decompress/lossless/random6_97x94_lossless_psv2_pt2.jpg`
- `bugs/decompress/lossless/random8_93x98_lossless_psv2_pt0.jpg`
- `bugs/decompress/lossless/random10_100x91_lossless_psv6_pt1.jpg`
- `bugs/decompress/lossless/random10_97x94_lossless_psv7_pt9.jpg`
- `bugs/decompress/lossless/random11_99x92_lossless_psv6_pt2.jpg`
- `bugs/decompress/lossless/random12_91x100_lossless_psv6_pt0.jpg`
- `bugs/decompress/lossless/random5_96x95_lossless_psv3_pt2.jpg`
- `bugs/decompress/lossless/random13_93x98_lossless_psv4_pt4.jpg`
- `bugs/decompress/lossless/random16_96x95_lossless_psv3_pt0.jpg`
- `bugs/decompress/lossless/random16_98x93_lossless_psv2_pt0.jpg`
- `bugs/decompress/lossless/random16_98x93_lossless_psv7_pt0.jpg`
- `bugs/decompress/lossless/random16_99x92_lossless_psv1_pt6.jpg`
- `bugs/decompress/lossless/random14_99x92_lossless_psv6_pt13.jpg`
- `bugs/decompress/lossless/random15_92x99_lossless_psv5_pt3.jpg`
- `bugs/decompress/lossless/random16_92x99_lossless_psv5_pt0.jpg`

Notes:

- The grayscale restart, baseline `4:1:0`, tiny baseline `4:1:0`,
  tiny baseline `4:2:0`, tiny baseline `4:4:4`, baseline direct-RGB `4:4:4`,
  Adobe APP14 transform-2 YCCK `4:1:1`, extended-sequential with 16-bit quant tables,
  baseline `1:2:0`, the Mozilla `kitty2` baseline `4:2:0` regression,
  12-bit extended-sequential odd-sampling YCbCr, 12-bit
  extended-sequential direct RGB, progressive `4:4:1`, progressive restart,
  arithmetic `4:4:0` restart, and arithmetic-progressive `4:2:2` seeds are
  all checked-in as success fixtures.
- The checked-in upstream subset now also includes representative Huffman
  lossless JPEG seeds at 2-bit, 5-bit, 6-bit, 8-bit, 10-bit, 11-bit, 12-bit,
  13-bit, 14-bit, 15-bit, and 16-bit
  precision with nontrivial predictor-selection and point-transform
  combinations, including predictor-selection values `1`, `2`, `3`, `4`, `5`,
  `6`, and `7` in the checked-in subset.
- The checked-in upstream subset also includes the two large valid baseline
  `github_347` overflow regression images, which require widening the baseline
  DC predictor/coefficient path beyond signed 32-bit intermediates.
- The two 12-bit fixtures are pinned to this repo's reduced-to-`rgba8` decode
  contract, and their current hashes now match scalar `djpeg -dct int`
  parity after keeping native sample precision through plane write,
  upsampling, and final color conversion.
