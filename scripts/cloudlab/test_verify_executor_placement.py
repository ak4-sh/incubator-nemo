#!/usr/bin/env python3

import importlib.util
import pathlib
import unittest


SCRIPT_PATH = pathlib.Path(__file__).with_name("verify_executor_placement.py")
SPEC = importlib.util.spec_from_file_location("verify_executor_placement", SCRIPT_PATH)
VERIFY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VERIFY)


def row(executor_type, executor_id, host, passed="true"):
    return {
        "executorType": executor_type,
        "executorId": executor_id,
        "containerId": f"container_{executor_id}",
        "physicalHost": host,
        "placementPassed": passed,
    }


class VerifyExecutorPlacementTest(unittest.TestCase):
    def setUp(self):
        self.source_hosts = VERIFY.parse_hosts("node5-link-1")
        self.compute_hosts = VERIFY.parse_hosts(
            "node9-link-1,node10-link-1,node11-link-1,node12-link-1"
        )

    def test_accepts_expected_layout(self):
        rows = [
            row("Source", "e1", "node5-link-1"),
            row("Compute", "e2", "node9-link-1"),
            row("Compute", "e3", "node10-link-1"),
            row("Compute", "e4", "node11-link-1"),
            row("Compute", "e5", "node12-link-1"),
        ]
        result = VERIFY.verify(rows, self.source_hosts, self.compute_hosts, True)
        self.assertTrue(result["passed"])

    def test_rejects_compute_on_source_host(self):
        rows = [
            row("Source", "e1", "node5-link-1"),
            row("Compute", "e2", "node5-link-1"),
            row("Compute", "e3", "node10-link-1"),
            row("Compute", "e4", "node11-link-1"),
            row("Compute", "e5", "node12-link-1"),
        ]
        result = VERIFY.verify(rows, self.source_hosts, self.compute_hosts, True)
        self.assertFalse(result["passed"])
        self.assertTrue(any("Source host" in failure for failure in result["failures"]))

    def test_rejects_duplicate_compute_host_and_missing_node(self):
        rows = [
            row("Source", "e1", "node5-link-1"),
            row("Compute", "e2", "node9-link-1"),
            row("Compute", "e3", "node9-link-1"),
            row("Compute", "e4", "node11-link-1"),
            row("Compute", "e5", "node12-link-1"),
        ]
        result = VERIFY.verify(rows, self.source_hosts, self.compute_hosts, True)
        self.assertFalse(result["passed"])
        self.assertTrue(any("2 Compute" in failure for failure in result["failures"]))
        self.assertTrue(any("node10-link-1" in failure for failure in result["failures"]))

    def test_unrestricted_mode_passes(self):
        rows = [
            row("Source", "e1", "node0-link-1"),
            row("Compute", "e2", "node4-link-1"),
        ]
        result = VERIFY.verify(rows, [], [], False)
        self.assertTrue(result["passed"])


if __name__ == "__main__":
    unittest.main()
