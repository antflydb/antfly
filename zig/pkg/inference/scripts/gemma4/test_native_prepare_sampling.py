import copy
import unittest
from qualify_native_prepare_sampling import validate_export


class PreparationSamplingTests(unittest.TestCase):
    def fixture(self):
        return {
            "metadata": {
                "logical_tokens": 8189,
                "outcome": "compacted",
                "checkpoint_bytes": 100,
            },
            "identity": {"weights": "a" * 64},
            "file_sha256": "b" * 64,
        }

    def test_exact_qualified_export_is_accepted(self):
        expected = self.fixture()
        validate_export(copy.deepcopy(expected), expected, 8189)

    def test_fallback_identity_bytes_and_metadata_changes_fail(self):
        for mutate in [
            lambda x: x["metadata"].update(outcome="unchanged_insufficient_savings"),
            lambda x: x["metadata"].update(logical_tokens=32752),
            lambda x: x["metadata"].update(checkpoint_bytes=200),
            lambda x: x.update(file_sha256="c" * 64),
            lambda x: x["identity"].update(weights="d" * 64),
        ]:
            expected = self.fixture()
            actual = copy.deepcopy(expected)
            mutate(actual)
            with self.assertRaises(AssertionError):
                validate_export(actual, expected, 8189)


if __name__ == "__main__":
    unittest.main()
