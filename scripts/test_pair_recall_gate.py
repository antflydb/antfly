import json
import tempfile
import unittest
from pathlib import Path

from run_posting_locality_ab import validate_pair_recall


class PairRecallTest(unittest.TestCase):
    def check(self, after=0.98, count=1000, *, invalid_live=None):
        with tempfile.TemporaryDirectory() as directory:
            roots = [Path(directory) / name for name in ("control", "candidate")]
            for root, recall in zip(roots, (0.99, after), strict=True):
                root.mkdir()
                live = (
                    invalid_live
                    if root == roots[1] and invalid_live is not None
                    else recall
                )
                (root / "qualification-summary.json").write_text(
                    json.dumps(
                        {
                            "runs": [
                                {"label": "online-live", "recall": live},
                                {"label": "reopened-warm", "recall": recall},
                            ]
                        }
                    )
                )
                (root / "public-query-profile.json").write_text(
                    json.dumps({"count": count, "recall": recall})
                )
            return validate_pair_recall(*roots, 1000)

    def test_one_percentage_point_boundary_passes(self):
        self.assertEqual(self.check()["fixed-profile"], [0.99, 0.98])

    def test_loss_and_missing_work_fail_closed(self):
        with self.assertRaises(RuntimeError):
            self.check(0.9799)
        with self.assertRaises(RuntimeError):
            self.check(count=10)
        for value in (float("nan"), -1, 0, 1.1, True):
            with self.assertRaises(RuntimeError):
                self.check(invalid_live=value)


if __name__ == "__main__":
    unittest.main()
