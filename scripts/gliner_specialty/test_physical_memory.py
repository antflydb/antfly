"""Offline contract tests for process-group physical-memory aggregation."""

import unittest

import physical_memory as memory


def row(pid, start, footprint, lifetime, rss=None):
    return {
        "pid": pid,
        "start_abstime": start,
        "rss_bytes": footprint if rss is None else rss,
        "phys_footprint_bytes": footprint,
        "lifetime_max_phys_footprint_bytes": lifetime,
    }


class PhysicalMemorySamplerTest(unittest.TestCase):
    def test_listing_filters_group_and_rejects_malformed_or_duplicate_rows(self):
        self.assertEqual([7, 9], memory.parse_process_group_listing("2 8\n1 9\n1 7\n", 1))
        for listing in ("1\n", "x 2\n", "0 2\n", "1 2\n3 2\n"):
            with self.subTest(listing=listing), self.assertRaises(ValueError):
                memory.parse_process_group_listing(listing, 1)
        for group in (0, -1, True, "1"):
            with self.subTest(group=group), self.assertRaises(ValueError):
                memory.parse_process_group_listing("1 2\n", group)

    def test_pid_reuse_creates_a_distinct_conservative_identity(self):
        samples = iter((row(41, 100, 60, 100), row(41, 200, 50, 80)))
        sampler = memory.ProcessGroupPhysicalMemorySampler(
            libproc=object(), sample_fn=lambda _lib, _pid: next(samples),
            listing_fn=lambda: "9 41\n",
        )
        first = sampler.snapshot(9)
        second = sampler.snapshot(9)
        self.assertEqual(
            100, first["conservative_summed_lifetime_max_phys_footprint_bytes"]
        )
        self.assertEqual(
            180, second["conservative_summed_lifetime_max_phys_footprint_bytes"]
        )
        self.assertEqual(2, second["observed_process_identities"])
        self.assertIn("not_concurrent_group_peak", second["bound_semantics"])

    def test_identity_maximum_is_monotonic_and_summed_across_workers(self):
        values = {
            11: iter((row(11, 1, 60, 100), row(11, 1, 70, 120))),
            12: iter((row(12, 2, 30, 50), row(12, 2, 20, 50))),
        }
        sampler = memory.ProcessGroupPhysicalMemorySampler(
            libproc=object(), sample_fn=lambda _lib, pid: next(values[pid]),
            listing_fn=lambda: "4 12\n4 11\n8 99\n",
        )
        first = sampler.snapshot(4)
        second = sampler.snapshot(4)
        self.assertEqual([11, 12], [sample["pid"] for sample in first["rows"]])
        self.assertEqual(
            150, first["conservative_summed_lifetime_max_phys_footprint_bytes"]
        )
        self.assertEqual(
            170, second["conservative_summed_lifetime_max_phys_footprint_bytes"]
        )
        self.assertEqual(90, second["live_phys_footprint_bytes"])

    def test_transient_underreport_resamples_and_retains_observed_current_peak(self):
        values = iter((row(5, 10, 101, 100), row(5, 10, 80, 101)))
        sampler = memory.ProcessGroupPhysicalMemorySampler(
            libproc=object(), sample_fn=lambda _lib, _pid: next(values),
            listing_fn=lambda: "3 5\n",
        )
        snapshot = sampler.snapshot(3)
        sampled = snapshot["rows"][0]
        self.assertEqual(2, sampled["consistency_sample_attempts"])
        self.assertEqual(
            101, sampled["identity_sampled_current_max_phys_footprint_bytes"]
        )
        self.assertEqual(101, sampled["identity_effective_peak_bound_bytes"])
        self.assertEqual(
            101, snapshot["summed_sampled_current_max_phys_footprint_bytes"]
        )
        self.assertEqual(101, snapshot["conservative_summed_identity_peak_bound_bytes"])
        self.assertEqual(
            101, snapshot["conservative_summed_lifetime_max_phys_footprint_bytes"]
        )
        self.assertEqual(1, len(sampled["transient_kernel_peak_lag_observations"]))

    def test_underreport_exhaustion_fails_with_counter_evidence(self):
        values = iter(
            (
                row(5, 10, 101, 100),
                row(5, 10, 80, 100),
                row(5, 10, 70, 100),
            )
        )
        sampler = memory.ProcessGroupPhysicalMemorySampler(
            libproc=object(), sample_fn=lambda _lib, _pid: next(values),
            listing_fn=lambda: "3 5\n",
        )
        with self.assertRaisesRegex(
            RuntimeError,
            r"pid=5 start=10 current=70 lifetime=100.*first_current=101.*"
            r"sampled_current_high_water=101.*attempts=3",
        ):
            sampler.snapshot(3)

    def test_decreased_kernel_peak_resamples_or_fails_closed(self):
        values = iter(
            (
                row(5, 10, 50, 100),
                row(5, 10, 50, 99),
                row(5, 10, 40, 100),
            )
        )
        sampler = memory.ProcessGroupPhysicalMemorySampler(
            libproc=object(), sample_fn=lambda _lib, _pid: next(values),
            listing_fn=lambda: "3 5\n",
        )
        sampler.snapshot(3)
        recovered = sampler.snapshot(3)
        self.assertEqual(2, recovered["rows"][0]["consistency_sample_attempts"])
        self.assertEqual(
            100, recovered["conservative_summed_lifetime_max_phys_footprint_bytes"]
        )

    def test_resampling_requires_stable_identity_and_live_process(self):
        cases = (
            (
                iter((row(5, 10, 101, 100), row(5, 11, 80, 101))),
                "identity changed",
            ),
            (iter((row(5, 10, 101, 100), None)), "disappeared"),
        )
        for values, expected in cases:
            with self.subTest(expected=expected):
                sampler = memory.ProcessGroupPhysicalMemorySampler(
                    libproc=object(), sample_fn=lambda _lib, _pid: next(values),
                    listing_fn=lambda: "3 5\n",
                )
                with self.assertRaisesRegex(RuntimeError, expected):
                    sampler.snapshot(3)

    def test_consistency_attempts_must_be_positive_integer(self):
        for attempts in (0, -1, True, "3"):
            with self.subTest(attempts=attempts), self.assertRaises(ValueError):
                memory.ProcessGroupPhysicalMemorySampler(
                    libproc=object(), sample_fn=lambda _lib, _pid: None,
                    listing_fn=lambda: "",
                    consistency_attempts=attempts,
                )

    def test_esrch_disappearance_is_the_only_empty_sample(self):
        sampler = memory.ProcessGroupPhysicalMemorySampler(
            libproc=object(), sample_fn=lambda _lib, _pid: None,
            listing_fn=lambda: "6 70\n",
        )
        snapshot = sampler.snapshot(6)
        self.assertEqual([], snapshot["rows"])
        self.assertEqual(0, snapshot["conservative_summed_lifetime_max_phys_footprint_bytes"])

    def test_wrong_or_uninspectable_sample_fails_closed(self):
        wrong = memory.ProcessGroupPhysicalMemorySampler(
            libproc=object(), sample_fn=lambda _lib, _pid: row(8, 1, 1, 1),
            listing_fn=lambda: "1 7\n",
        )
        with self.assertRaisesRegex(RuntimeError, "identity mismatch"):
            wrong.snapshot(1)

        def unavailable(_lib, _pid):
            raise OSError(1, "not permitted")

        unavailable_sampler = memory.ProcessGroupPhysicalMemorySampler(
            libproc=object(), sample_fn=unavailable, listing_fn=lambda: "1 7\n"
        )
        with self.assertRaises(OSError):
            unavailable_sampler.snapshot(1)


if __name__ == "__main__":
    unittest.main()
