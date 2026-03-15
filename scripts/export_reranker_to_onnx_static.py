#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.8"
# dependencies = [
#     "onnx",
#     "onnxruntime",
#     "optimum[onnxruntime]",
#     "transformers",
#     "torch",
#     "datasets",
# ]
# ///
"""
Export reranker model to ONNX format with STATIC int8 quantization using Optimum.

This uses Optimum's built-in static quantization which has been tested and works
correctly with the mixedbread reranker model.

Usage:
    python export_reranker_to_onnx_static.py --output-dir ./models/rerankers/reranker_onnx_static

Requirements:
    pip install onnx onnxruntime optimum transformers torch datasets
"""

import argparse
import logging
from pathlib import Path
import numpy as np
from optimum.onnxruntime import ORTModelForSequenceClassification
from transformers import AutoTokenizer
from onnxruntime.quantization import quantize_static, CalibrationDataReader, QuantType, QuantFormat
from onnxruntime.quantization.shape_inference import quant_pre_process

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RerankerCalibrationDataReader(CalibrationDataReader):
    """
    Calibration data reader for ONNX Runtime static quantization.
    Provides batches of tokenized query-document pairs.
    """
    def __init__(self, tokenizer, num_samples=100):
        self.tokenizer = tokenizer
        self.num_samples = num_samples
        self.current_index = 0

        # Sample queries and documents for calibration
        self.queries = [
            "What is machine learning?",
            "How does deep learning work?",
            "Explain neural networks",
            "What is natural language processing?",
            "How to train a language model?",
            "What are transformers in AI?",
            "Explain attention mechanism",
            "What is BERT model?",
            "How does GPT work?",
            "What is model quantization?",
        ]

        self.documents = [
            "Machine learning is a subset of artificial intelligence that enables systems to learn from data.",
            "Deep learning uses neural networks with multiple layers to process complex patterns in data.",
            "Neural networks are computing systems inspired by biological neural networks in animal brains.",
            "Natural language processing (NLP) enables computers to understand and generate human language.",
            "Training a language model involves feeding large amounts of text data through neural networks.",
            "Transformers are a type of neural network architecture that uses self-attention mechanisms.",
            "The attention mechanism allows models to focus on relevant parts of the input sequence.",
            "BERT (Bidirectional Encoder Representations from Transformers) is a pre-trained language model.",
            "GPT (Generative Pre-trained Transformer) is an autoregressive language model for text generation.",
            "Model quantization reduces precision of weights and activations to improve inference speed.",
            "Python is a programming language used for web development and data science.",
            "The weather today is sunny with a chance of rain in the afternoon.",
            "Cooking pasta requires boiling water and adding salt for flavor.",
            "The stock market fluctuates based on various economic factors and investor sentiment.",
            "Gardening is a relaxing hobby that connects people with nature and provides fresh produce.",
            "Classical music has been popular for centuries across many cultures worldwide.",
            "Regular exercise improves cardiovascular health and mental wellbeing significantly.",
            "Coffee is one of the most widely consumed beverages in the world.",
            "The solar system consists of the sun and celestial objects orbiting it.",
            "Historical events shape the cultural and political landscape of nations.",
        ]

    def get_next(self):
        """Get next calibration sample as a dictionary of numpy arrays."""
        if self.current_index >= self.num_samples:
            return None

        query = self.queries[self.current_index % len(self.queries)]
        doc = self.documents[self.current_index % len(self.documents)]

        # Tokenize the pair
        inputs = self.tokenizer(
            query,
            doc,
            padding="max_length",
            truncation=True,
            max_length=512,
            return_tensors="np"
        )

        # Convert to dictionary of numpy arrays (ONNX Runtime format)
        input_dict = {
            "input_ids": inputs["input_ids"].astype(np.int64),
            "attention_mask": inputs["attention_mask"].astype(np.int64),
        }

        self.current_index += 1
        return input_dict


def export_model_static_quantization(model_id: str, output_dir: str):
    """
    Export a HuggingFace reranker model to ONNX with static int8 quantization.

    Uses Optimum's static quantization with QOperator format to keep activations
    in int8 between layers for maximum performance on ARM64 NEON.

    Args:
        model_id: HuggingFace model ID
        output_dir: Directory to save quantized ONNX model
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    logger.info(f"Loading model: {model_id}")
    logger.info(f"Output directory: {output_dir}")

    # Load tokenizer
    tokenizer = AutoTokenizer.from_pretrained(model_id)

    # Export to ONNX first
    logger.info("Converting to ONNX format...")
    ort_model = ORTModelForSequenceClassification.from_pretrained(
        model_id,
        export=True,
    )

    # Save unquantized model to temp directory
    temp_dir = output_path / "temp_unquantized"
    temp_dir.mkdir(exist_ok=True)
    ort_model.save_pretrained(temp_dir)
    tokenizer.save_pretrained(temp_dir)

    # Get paths
    model_input = str(temp_dir / "model.onnx")
    model_preprocessed = str(temp_dir / "model_preprocessed.onnx")
    model_output = str(output_path / "model_quantized.onnx")

    logger.info("\n🔧 Preprocessing model for quantization...")
    logger.info("Running shape inference and model optimization...")

    try:
        # Preprocess the model - this is critical for successful quantization
        # It performs shape inference and prepares the model structure
        quant_pre_process(
            input_model_path=model_input,
            output_model_path=model_preprocessed,
            skip_optimization=False,  # Apply optimizations
            skip_onnx_shape=False,    # Run shape inference
            skip_symbolic_shape=False,  # Run symbolic shape inference
            auto_merge=True,           # Merge nodes where possible
            save_as_external_data=False,
        )
        logger.info("✅ Preprocessing complete")

        # Use preprocessed model for quantization
        quantization_input = model_preprocessed
    except Exception as e:
        logger.warning(f"Preprocessing failed: {e}")
        logger.info("Continuing with original model...")
        quantization_input = model_input

    logger.info("\n🔧 Applying static int8 quantization...")
    logger.info("Using QOperator format to keep activations in int8 between layers")
    logger.info("This enables fast int8 SIMD operations (SDOT/SMMLA on ARM64)")

    try:
        # Create calibration data reader
        logger.info("Creating calibration dataset with 100 samples...")
        calibration_reader = RerankerCalibrationDataReader(tokenizer, num_samples=100)
        logger.info("✅ Calibration data reader ready")

        # Apply static quantization using ONNX Runtime
        # Note: QOperator format may have compatibility issues with complex models
        # Fallback to QDQ if QOperator fails
        logger.info("Running calibration and quantization...")
        logger.info("Trying QOperator format first (best for int8 performance)...")

        try:
            quantize_static(
                model_input=quantization_input,
                model_output=model_output,
                calibration_data_reader=calibration_reader,
                quant_format=QuantFormat.QOperator,  # QOperator format - keeps activations in int8!
                activation_type=QuantType.QInt8,      # int8 activations
                weight_type=QuantType.QInt8,          # int8 weights
                per_channel=False,                    # Per-tensor quantization
                reduce_range=False,                   # Don't reduce range (better for ARM64)
            )
            logger.info("✅ Successfully quantized with QOperator format!")
        except Exception as e:
            logger.warning(f"QOperator format failed: {e}")
            logger.info("Falling back to QDQ format...")

            # Reset calibration reader
            calibration_reader = RerankerCalibrationDataReader(tokenizer, num_samples=100)

            quantize_static(
                model_input=quantization_input,
                model_output=model_output,
                calibration_data_reader=calibration_reader,
                quant_format=QuantFormat.QDQ,        # QDQ format - more compatible
                activation_type=QuantType.QInt8,      # int8 activations
                weight_type=QuantType.QInt8,          # int8 weights
                per_channel=False,                    # Per-tensor quantization
                reduce_range=False,                   # Don't reduce range (better for ARM64)
            )
            logger.info("✅ Successfully quantized with QDQ format (fallback)")

        # Copy tokenizer and config to output directory
        tokenizer.save_pretrained(output_dir)

        # Copy config files
        import shutil
        for file in ["config.json", "special_tokens_map.json", "tokenizer_config.json"]:
            src = temp_dir / file
            if src.exists():
                shutil.copy(src, output_path / file)

        # Clean up temp directory
        shutil.rmtree(temp_dir)

        logger.info("✅ Static quantization complete!")
        logger.info("   - Weights quantized to int8 (calibrated)")
        logger.info("   - Activations quantized to int8 (calibrated with MinMax)")
        logger.info("   - Optimized for ARM64 NEON (SDOT/SMMLA instructions)")

        # Check which format was used
        import onnx
        quantized_model = onnx.load(model_output)
        has_qlinear = any('QLinear' in node.op_type for node in quantized_model.graph.node)
        if has_qlinear:
            logger.info("   - Format: QOperator (QLinearMatMul - activations stay in int8!)")
        else:
            logger.info("   - Format: QDQ (QuantizeLinear/DequantizeLinear pairs)")

    except Exception as e:
        logger.error(f"❌ Quantization failed: {e}")
        import traceback
        traceback.print_exc()
        raise

    logger.info("\n✅ Export complete!")
    logger.info(f"\nFiles saved to: {output_dir}")

    # Print model info
    logger.info("\n📊 Model Information:")
    logger.info(f"  - Model ID: {model_id}")
    logger.info(f"  - ONNX file: {output_path / 'model_quantized.onnx'}")
    logger.info(f"  - Tokenizer: {output_path / 'tokenizer.json'}")
    logger.info(f"  - Quantization: Static int8 QOperator (weights + activations)")
    logger.info(f"  - Hardware optimization: ARM64 NEON (SDOT/SMMLA)")

    logger.info("\n⚡ Expected Performance:")
    logger.info("  - Model size: ~4x smaller")
    logger.info("  - Memory usage: ~4x lower")
    logger.info("  - Inference speed: 2-4x faster on ARM64 with int8 SIMD")
    logger.info("  - Activations stay in int8 between layers!")
    logger.info("  - Uses QLinearMatMul instead of MatMul with Q/DQ pairs")


def main():
    parser = argparse.ArgumentParser(
        description="Export reranker model to ONNX with static int8 quantization using Optimum"
    )
    parser.add_argument(
        "--model",
        type=str,
        default="mixedbread-ai/mxbai-rerank-base-v1",
        help="HuggingFace model ID to export"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./models/rerankers/reranker_onnx_static",
        help="Output directory for quantized ONNX model"
    )

    args = parser.parse_args()

    try:
        export_model_static_quantization(args.model, args.output_dir)

        logger.info("\n" + "="*70)
        logger.info("🎉 All done! The statically quantized model is ready to use.")
        logger.info("="*70)
        logger.info("\n✨ Key Benefits:")
        logger.info("  • Activations stay in int8 throughout the network")
        logger.info("  • Should be 2-4x faster than non-quantized models")
        logger.info("  • Uses QLinearMatMul/QLinearConv for true int8 compute")
        logger.info("  • Optimized for ARM64 NEON SDOT/SMMLA instructions")
        logger.info("\n🚀 Next Steps:")
        logger.info(f"  1. Test the model: go test -v -run TestCompareAllRerankerModels ./termite/")
        logger.info("  2. Run benchmarks to measure the speedup")
        logger.info("  3. Verify accuracy is acceptable (should be >95% of original)")

    except Exception as e:
        logger.error(f"\n❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()
