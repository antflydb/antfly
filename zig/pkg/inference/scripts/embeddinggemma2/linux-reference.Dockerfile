# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
# Qualification tooling only; no production codec/model dependencies.
FROM python@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg ca-certificates && rm -rf /var/lib/apt/lists/*
RUN python -m pip install --no-cache-dir --only-binary=:all: torch==2.10.0 torchvision==0.25.0 --index-url https://download.pytorch.org/whl/cpu
RUN python -m pip install --no-cache-dir numpy==2.5.4 pillow==12.3.0 huggingface-hub==2.2.0 packaging==26.3 pyyaml==6.0.3 regex==2026.9.29 tokenizers==0.23.3 typer==0.27.3 safetensors==0.8.0 tqdm==4.70.1 jinja2==3.1.6
COPY transformers /usr/local/lib/python3.12/site-packages/transformers
COPY transformers.dist-info /usr/local/lib/python3.12/site-packages/transformers-5.19.0.dev0.dist-info
ENV HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1

# Match the decoder/color-conversion release used by the pinned RGB oracle.
# This minimal offline video tool supports MOV/MP4, WebM, TS and JPEG inputs.
COPY ffmpeg-9.0.2.tar.xz /tmp/ffmpeg-9.0.2.tar.xz
RUN apt-get update && apt-get install -y --no-install-recommends build-essential pkg-config xz-utils libdav1d-dev && rm -rf /var/lib/apt/lists/*
RUN echo "8c3850283eb25fa026482078a04051e0be17347b09ef81a0849bec15a96e002e  /tmp/ffmpeg-9.0.2.tar.xz" | sha256sum -c - \
    && tar -xf /tmp/ffmpeg-9.0.2.tar.xz -C /tmp \
    && cd /tmp/ffmpeg-9.0.2 \
    && ./configure --disable-everything --disable-doc --disable-debug --disable-network --disable-autodetect \
        --enable-ffmpeg --enable-ffprobe --enable-libdav1d \
        --enable-protocol=file,pipe --enable-demuxer=mov,matroska,mpegts,image2 \
        --enable-decoder=h264,mjpeg,vp8,vp9,rawvideo,libdav1d \
        --enable-parser=h264,vp8,vp9,av1,mjpeg \
        --enable-encoder=rawvideo --enable-muxer=rawvideo,image2 \
        --enable-filter=buffer,buffersink,scale,format \
    && make -j4 && make install \
    && rm -rf /tmp/ffmpeg-9.0.2 /tmp/ffmpeg-9.0.2.tar.xz /var/lib/apt/lists/*
