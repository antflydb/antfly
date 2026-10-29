# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

from array import array
import math
import unittest

from qualify_mp4_codecs import ffmpeg_embedding_acceptance, pcm_comparison


class PcmQualificationTests(unittest.TestCase):
    def test_exact_pcm_and_small_error(self):
        expected = array("f", [0.25, -0.5, 0.125, -0.25])
        self.assertEqual(pcm_comparison(expected, expected)["max_abs"], 0)
        actual = array("f", (sample + 0.00001 for sample in expected))
        measured = pcm_comparison(actual, expected)
        self.assertGreater(measured["snr_db"], 50)
        self.assertLess(measured["mean_abs"], 0.0001)

    def test_errors_exceeding_pcm_contract_are_visible(self):
        expected = array("f", [0.25, -0.5, 0.125, -0.25])
        actual = array("f", (sample + 0.01 for sample in expected))
        measured = pcm_comparison(actual, expected)
        self.assertLess(measured["snr_db"], 50)

    def test_truncated_empty_and_nonfinite_pcm_are_rejected(self):
        for actual, expected in (
            ([0.25], [0.25, 0.25]),
            ([], []),
            ([math.nan], [0.25]),
            ([0.25], [math.inf]),
        ):
            with self.subTest(actual=actual, expected=expected):
                with self.assertRaises(ValueError):
                    pcm_comparison(array("f", actual), array("f", expected))


class IndependentEmbeddingAcceptanceTests(unittest.TestCase):
    @staticmethod
    def unit(first, second=0):
        norm = math.hypot(first, second)
        return [first / norm, second / norm] + [0.0] * 766

    def test_accepts_exact_and_bounded_approximation(self):
        reference = self.unit(1)
        self.assertEqual(
            ffmpeg_embedding_acceptance(reference, reference)["max_abs"], 0
        )
        ffmpeg_embedding_acceptance(self.unit(1, 0.005), reference)

    def test_rejects_wrong_reference_and_component_drift(self):
        reference = self.unit(1)
        for actual in (self.unit(0, 1), self.unit(1, 0.02)):
            with self.assertRaisesRegex(ValueError, "FFmpeg embedding acceptance"):
                ffmpeg_embedding_acceptance(actual, reference)

    def test_cosine_bound_is_enforced_even_with_small_component_errors(self):
        # Spread drift over 767 components so each stays below .01 while the
        # accumulated angular error exceeds the independent cosine bound.
        actual = [math.sqrt(1 - 767 * 0.004**2)] + [0.004] * 767
        with self.assertRaisesRegex(ValueError, "FFmpeg embedding acceptance"):
            ffmpeg_embedding_acceptance(actual, self.unit(1))

    def test_invalid_dimension_nonfinite_and_unnormalized_vectors_fail(self):
        for actual in ([1.0], [math.nan] * 768, [0.0] * 768):
            with self.assertRaises(ValueError):
                ffmpeg_embedding_acceptance(actual, self.unit(1))


if __name__ == "__main__":
    unittest.main()
