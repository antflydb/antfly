// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");

pub const tasks = @import("tasks.zig");
pub const documents = @import("documents.zig");
pub const adapters = @import("adapters.zig");

pub const EmbeddingPipeline = @import("embedding.zig").EmbeddingPipeline;
pub const EmbeddingConfig = @import("embedding.zig").EmbeddingConfig;
pub const ChunkingPipeline = @import("chunking.zig").ChunkingPipeline;
pub const ChunkingConfig = @import("chunking.zig").ChunkingConfig;
pub const Chunk = @import("chunking.zig").Chunk;
pub const RerankingPipeline = @import("reranking.zig").RerankingPipeline;
pub const RerankingConfig = @import("reranking.zig").RerankingConfig;
pub const RankedResult = @import("reranking.zig").RankedResult;
pub const ClassificationPipeline = @import("classification.zig").ClassificationPipeline;
pub const ClassificationConfig = @import("classification.zig").ClassificationConfig;
pub const ClassificationResult = @import("classification.zig").ClassificationResult;
pub const NerPipeline = @import("ner.zig").NerPipeline;
pub const NerConfig = @import("ner.zig").NerConfig;
pub const Entity = @import("ner.zig").Entity;
pub const GenerationPipeline = @import("generation.zig").GenerationPipeline;
pub const GenerationConfig = @import("generation.zig").GenerationConfig;
pub const GenerationResult = @import("generation.zig").GenerationResult;
pub const GenerationMessage = @import("generation.zig").Message;
pub const EncoderDecoderPipeline = @import("encoder_decoder.zig").EncoderDecoderPipeline;
pub const DecoderConfig = @import("encoder_decoder.zig").DecoderConfig;
pub const EncoderDecoderResult = @import("encoder_decoder.zig").EncoderDecoderResult;
pub const RewritingPipeline = @import("rewriting.zig").RewritingPipeline;
pub const RewriteConfig = @import("rewriting.zig").RewriteConfig;
pub const RebelPipeline = @import("rebel.zig").RebelPipeline;
pub const RebelConfig = @import("rebel.zig").RebelConfig;
pub const ReadingPipeline = @import("reading.zig").ReadingPipeline;
pub const ReadConfig = @import("reading.zig").ReadConfig;
pub const TextRegion = @import("multistage_ocr.zig").TextRegion;
pub const RecognizedRegion = @import("multistage_ocr.zig").RecognizedRegion;
pub const LayoutRegion = @import("multistage_ocr.zig").LayoutRegion;
pub const assembleMultiStageText = @import("multistage_ocr.zig").assembleFullText;
pub const sortRegionsByReadingOrder = @import("multistage_ocr.zig").sortRegionsByReadingOrder;
pub const MultiStageOCRPipeline = @import("multistage_ocr.zig").MultiStageOCRPipeline;
pub const TranscriptionPipeline = @import("transcription.zig").TranscriptionPipeline;
pub const TranscribeConfig = @import("transcription.zig").TranscribeConfig;
pub const image = @import("image.zig");
pub const crop = @import("crop.zig");
pub const ctc_decode = @import("ctc_decode.zig");
pub const connected_components = @import("connected_components.zig");
pub const multistage_ocr = @import("multistage_ocr.zig");
pub const audio = @import("audio.zig");
pub const grammar = @import("grammar.zig");
pub const tool_parser = @import("tool_parser.zig");
pub const multimodal_reranker = tasks.multimodal_reranker;
pub const document_preprocessing = documents.preprocessing;
pub const document_classification = documents.classification;
pub const document_token_classification = documents.token_classification;
pub const OnnxDecoderOnlyVlmPipeline = @import("onnx_decoder_only_vlm.zig").Pipeline;
pub const JsonGrammar = grammar.JsonGrammar;

test {
    _ = @import("embedding.zig");
    _ = @import("chunking.zig");
    _ = @import("reranking.zig");
    _ = @import("classification.zig");
    _ = @import("ner.zig");
    _ = @import("generation.zig");
    _ = @import("encoder_decoder.zig");
    _ = @import("rewriting.zig");
    _ = @import("rebel.zig");
    _ = @import("reading.zig");
    _ = @import("multistage_ocr.zig");
    _ = @import("crop.zig");
    _ = @import("ctc_decode.zig");
    _ = @import("connected_components.zig");
    _ = @import("transcription.zig");
    _ = @import("image.zig");
    _ = @import("audio.zig");
    _ = @import("grammar.zig");
    _ = @import("tool_parser.zig");
    _ = @import("tasks.zig");
    _ = @import("documents.zig");
    _ = @import("multimodal_reranker.zig");
    _ = @import("document_preprocessing.zig");
    _ = @import("document_classification.zig");
    _ = @import("document_token_classification.zig");
    _ = @import("adapters.zig");
    _ = @import("onnx_decoder_only_vlm.zig");
}
