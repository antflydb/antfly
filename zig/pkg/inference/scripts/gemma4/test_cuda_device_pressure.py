import unittest
from cuda_device_pressure import device_pressure


class Driver:
    def __init__(self, fail=None):
        self.events = []
        self.fail = fail
        self.pressured = False

    def create(self):
        self.events.append("create")
        return "ctx"

    def destroy(self, context):
        self.events.append(("destroy", context))

    def memory(self):
        self.events.append("memory")
        return (10 if self.pressured else 100), 200

    def allocate(self, size):
        self.events.append(("allocate", size))
        if self.fail == "allocate":
            raise RuntimeError("allocation failure")
        self.pressured = True
        return "allocation"

    def free(self, pointer):
        self.events.append(("free", pointer))
        if self.fail == "free":
            raise RuntimeError("free failure")
        self.pressured = False


class PressureCleanupTests(unittest.TestCase):
    def test_model_failure_releases_pressure_and_context(self):
        driver = Driver()
        with self.assertRaisesRegex(RuntimeError, "model failure"):
            with device_pressure(driver, leave_free_bytes=10) as evidence:
                raise RuntimeError("model failure")
        self.assertEqual(
            driver.events[-2:], [("free", "allocation"), ("destroy", "ctx")]
        )
        self.assertFalse(driver.pressured)
        self.assertTrue(evidence["released"])

    def test_allocation_failure_destroys_context_without_invalid_free(self):
        driver = Driver("allocate")
        with self.assertRaisesRegex(RuntimeError, "allocation failure"):
            with device_pressure(driver, leave_free_bytes=10):
                self.fail("must not yield")
        self.assertEqual(driver.events[-1], ("destroy", "ctx"))
        self.assertNotIn(("free", "allocation"), driver.events)

    def test_free_failure_still_destroys_context(self):
        driver = Driver("free")
        with self.assertRaisesRegex(RuntimeError, "free failure"):
            with device_pressure(driver, leave_free_bytes=10):
                pass
        self.assertEqual(driver.events[-1], ("destroy", "ctx"))

    def test_unestablished_pressure_rejected_and_cleaned(self):
        driver = Driver()
        with self.assertRaisesRegex(RuntimeError, "device pressure changed"):
            with device_pressure(driver, leave_free_bytes=5):
                self.fail("must not yield")
        self.assertEqual(
            driver.events[-2:], [("free", "allocation"), ("destroy", "ctx")]
        )

    def test_success_records_exact_allocation_and_release(self):
        driver = Driver()
        with device_pressure(driver, leave_free_bytes=10) as evidence:
            self.assertEqual(evidence["allocated_pressure_bytes"], 90)
            self.assertFalse(evidence["released"])
        self.assertTrue(evidence["released"])
        self.assertFalse(driver.pressured)


if __name__ == "__main__":
    unittest.main()
