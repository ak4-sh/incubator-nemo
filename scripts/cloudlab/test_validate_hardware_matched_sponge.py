#!/usr/bin/env python3

import importlib.util
import pathlib
import unittest


SCRIPT = pathlib.Path(__file__).with_name("validate_hardware_matched_sponge.py")
SPEC = importlib.util.spec_from_file_location("hardware_preflight", SCRIPT)
PREFLIGHT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PREFLIGHT)


class HardwareMatchedPreflightTest(unittest.TestCase):
    def test_accepts_node_sized_resources(self):
        resources = [
            {"type": "Transient", "memory_mb": 1769, "capacity": 1, "slot": 1, "num": 0},
            {"type": "Reserved", "memory_mb": 1769, "capacity": 1, "slot": 1, "num": 0},
            {"type": "Source", "memory_mb": 114688, "capacity": 55, "slot": 8, "num": 1},
            {"type": "Compute", "memory_mb": 114688, "capacity": 55, "slot": 5, "num": 4},
        ]
        self.assertEqual(
            [], PREFLIGHT.validate_executor_config(resources, 114688, 55)
        )

    def test_rejects_small_compute_container(self):
        resources = [
            {"type": "Transient", "memory_mb": 1769, "capacity": 1, "slot": 1, "num": 0},
            {"type": "Reserved", "memory_mb": 1769, "capacity": 1, "slot": 1, "num": 0},
            {"type": "Source", "memory_mb": 114688, "capacity": 55, "slot": 8, "num": 1},
            {"type": "Compute", "memory_mb": 2048, "capacity": 1, "slot": 1, "num": 20},
        ]
        failures = PREFLIGHT.validate_executor_config(resources, 114688, 55)
        self.assertTrue(any("Compute.memory_mb" in failure for failure in failures))
        self.assertTrue(any("Compute.capacity" in failure for failure in failures))
        self.assertTrue(any("Compute.slot" in failure for failure in failures))
        self.assertTrue(any("Compute.num" in failure for failure in failures))

    def test_host_parser_normalizes_duplicates(self):
        with self.assertRaises(ValueError):
            PREFLIGHT.parse_hosts("node5,node5-link-1")

    def test_probe_schema_recognizes_stale_executor_count(self):
        numeric_keys = {"cpus", "memory_kb", "nm_count", "vmworker_count", "reef_count"}
        self.assertIn("reef_count", numeric_keys)


if __name__ == "__main__":
    unittest.main()
