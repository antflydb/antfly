# Copyright 2026 the HuggingFace Team. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
# Source-method snapshot observed 2026-10-08, without edits to the method body:
# https://github.com/huggingface/transformers/blob/main/src/transformers/models/embedding_gemma2/video_processing_embedding_gemma2.py
# This is a conformance oracle, not part of the Antfly runtime.
class EmbeddingGemma2VideoProcessor:
    def sample_frames(
        self,
        metadata: VideoMetadata,
        fps: int | float | None = None,
        max_frames: int | None = None,
        overflow_strategy: str | None = None,
        **kwargs,
    ) -> np.ndarray:
        if kwargs.get("num_frames") is not None:
            raise ValueError(
                f"Sampling with `num_frames` is not supported for {self.__class__.__name__}. "
                "Please use `fps` and `max_frames` to control video sampling."
            )
        # 1) Sample to match the target `fps` if it is set, otherwise keep the whole video.
        # A decoded array carries no frame rate, and neither `fps` nor `duration` can be inferred
        # from one, so rate-based sampling is simply not applicable to that input. Skip it rather
        # than guess a source rate: a wrong guess silently discards frames.
        if fps is not None and (metadata.fps is None or metadata.duration is None):
            logger.warning_once(
                "Asked to sample uniformly with `fps`, but the video metadata has no `fps` or `duration`. "
                "Keeping every frame and applying only the `max_frames` budget. Pass a `VideoMetadata` "
                "object with a valid `fps` and `duration` to sample at a target frame rate."
            )
            fps = None
        if fps is None:
            indices = np.arange(metadata.total_num_frames, dtype=int)
        else:
            step = metadata.fps / fps  # native frames per sampled frame
            num_sampled = max(1, int(metadata.duration * fps))
            indices = np.array(
                [
                    min(metadata.total_num_frames - 1, int(i * step))
                    for i in range(num_sampled)
                ],
                dtype=int,
            )
        # 2) Cap total number of frames to `max_frames` checking the input `overflow_strategy`
        if overflow_strategy is not None:
            if max_frames is None:
                raise ValueError(
                    f"You must pass `max_frames` when requesting an overflow_strategy={overflow_strategy}!"
                )
            # If video is too short, do no accidentally pad inputs when trying to re-sample
            if len(indices) <= max_frames:
                pass
            elif overflow_strategy == "truncate":
                indices = indices[:max_frames]
            elif overflow_strategy == "uniform":
                linspace_idx = np.linspace(0, len(indices) - 1, max_frames, dtype=int)
                indices = np.array([indices[i] for i in linspace_idx], dtype=int)
            else:
                raise ValueError(
                    f"You passed `overflow_strategy={overflow_strategy}` but expected one of ['truncate', 'uniform']"
                )
        return indices
