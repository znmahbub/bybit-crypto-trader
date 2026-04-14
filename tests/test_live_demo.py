from __future__ import annotations

import os
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bybit_trader.verification import verify_private_demo_order_lifecycle, verify_private_demo_topics


@unittest.skipUnless(os.environ.get("BYBIT_TRADER_RUN_DEMO_LIVE_TESTS") == "1", "Live demo Bybit checks are opt-in.")
class LiveDemoIntegrationTests(unittest.TestCase):
    def test_private_demo_topics(self) -> None:
        result = verify_private_demo_topics()
        self.assertEqual(result.status, "passed")

    def test_private_demo_order_lifecycle(self) -> None:
        result = verify_private_demo_order_lifecycle()
        self.assertEqual(result.status, "passed")


if __name__ == "__main__":
    unittest.main()
