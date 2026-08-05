import unittest
from pathlib import Path

from checks.architecture import run as architecture
from checks.config import get_config
from checks.route_contract import normalize


class QualityChecksTest(unittest.TestCase):
    def test_config_has_quality_limits(self):
        config = get_config()
        self.assertGreater(config.max_go_lines, 0)
        self.assertGreater(config.max_function_lines, 0)
        self.assertGreater(config.max_decisions, 0)

    def test_route_parameter_names_are_normalized(self):
        self.assertEqual(normalize("/api/v1/events/{eventID}"), "/api/v1/events/{}")

    def test_architecture_is_clean(self):
        self.assertEqual(architecture(), 0)


if __name__ == "__main__":
    unittest.main()

