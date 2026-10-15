This full 52-second Sintel trailer is © Blender Foundation, durian.blender.org,
licensed under [CC BY 3.0](https://creativecommons.org/licenses/by/3.0/).
Modifications: resized to 480×320 and re-encoded as H.264 High, 24 FPS, with
CABAC, two B-frames, four references and deblocking. The manifest records the
original URL/hash, encoding command and resulting clip hash. The original
source and pretrained weights are not committed.

The four queries are unlabelled comparison inputs. A one-document corpus tests
vector/score preservation and full-pipeline resource use; its rankings are
trivial. Use the six-document corpus in `../retrieval` to assess rank preservation.
The pinned upstream processor selects 32 frames uniformly across the clip.
`video_compare.py` streams independent FFmpeg RGB, retaining only those selected
frames, and disables resampling in the processor so selection occurs once.
Completed timings include decode, preparation, model execution and final
readback; native also includes HTTP. Reports retain host conditions and sampled
server RSS. They do not establish clean-host production capacity.
