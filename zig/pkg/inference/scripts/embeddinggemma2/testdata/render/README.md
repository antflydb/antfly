These first-party Apache-2.0 fixtures qualify the explicit version-3 inference
rendering policy, extending the pinned processor with presentation orientation,
SAR-controlled target geometry and deterministic HDR conversion. They do not
assert upstream Transformers automatically honors these metadata policies.

`rotation-90.mov` and `sar-2-1.mov` preserve every compressed sample from
`../video/video-mjpeg.mov`. `video_policy_qualification.variants` changes only
MP4 metadata. The independent reference uses Pillow/libjpeg RGB and rotates
pixels clockwise, or passes twice the coded width into the pinned aspect-ratio
geometry function. SAR uses one direct resample, without an intermediate raster.

`pq-2020.mp4` and `hlg-2020.mp4` encode the synthetic 48x48 10-bit limited-range
4:2:0 ramp in `zig/lib/video/testdata/hdr-10bit-nv12.bin`, repeated eight times
at 4 FPS. Interleaved UV becomes planar U/V for FFmpeg input. Encode with
`libx264 -preset ultrafast -qp 0 -bf 0 -g 8 -x264-params
colorprim=bt2020:transfer=smpte2084:colormatrix=bt2020nc` (HLG uses
`transfer=arib-std-b67`). The independent decoder is FFmpeg; decoded native samples
also match the source ramp exactly. Only frames `[0, 4]` feed the model.

The rendering policy uses BT.2100 PQ or HLG (1000-nit display, gamma 1.2, zero
black), D65 BT.2020-to-BT.709 conversion, fixed 100-nit exposure, global luminance
Reinhard `L/(1+L)`, then sRGB encoding and RGB8 quantization. Mastering and dynamic
HDR metadata do not change this policy. `render_reference.py` implements the
independent float64 reference and generates the library RGB8 fixtures.

`pytorch-cpu.json` and `pytorch-mps.json` retain full vectors, source hashes,
checkpoint/processor pins, versions, selected-frame RGB hashes and prompt token
counts. Generate with `video_render_qualification.py --backend cpu|mps`, then
compare the production native/Metal HTTP routes with `--backend native|metal`.
The scripts fail on changed assets, recipe, checkpoint, processor source or vector
agreement outside the existing `1e-4` coordinate / `0.9999` cosine thresholds.
