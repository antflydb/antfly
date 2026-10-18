These six two-second, 480×320, 24 FPS clips are derived from the Sintel
trailer, © Blender Foundation, durian.blender.org, licensed under
[CC BY 3.0](https://creativecommons.org/licenses/by/3.0/).
The manifest records the original download URL, SHA-256, excerpt boundaries,
encoding commands and each resulting clip hash. Modifications: three excerpts
starting at 6, 20 and 38 seconds, resized and encoded as H.264 High and MJPEG.

The four queries are unlabelled comparison inputs. They test preservation of
reference scores and rankings, not whether the pretrained model serves an end
user's retrieval task. The original source and weights are not committed.

Use `video_corpus.py --source <original> --output-dir <directory>` to reproduce
inputs with the recorded encoder configuration; changed encoder output fails
its hash check. `video_compare.py --corpus corpus.json` verifies every clip
before measuring. Pin the checkpoint and Transformers revisions using
`reference.py`, run CPU as the reference, then compare MPS, native and Metal
reports against the same corpus and checkpoint receipt. Timing scopes differ:
native includes HTTP; PyTorch excludes it. Runs on a busy host are qualification
measurements, not production capacity estimates.
