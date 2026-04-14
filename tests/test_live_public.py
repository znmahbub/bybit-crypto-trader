from __future__ import annotations

from datetime import UTC, datetime
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bybit_trader.data import ResearchWarehouse
from bybit_trader.historical import HistoricalMarketFetcher
from bybit_trader.session import NotebookSession
from bybit_trader.verification import verify_public_rest, verify_public_websocket


@unittest.skipUnless(os.environ.get("BYBIT_TRADER_RUN_LIVE_TESTS") == "1", "Live public Bybit checks are opt-in.")
class LivePublicIntegrationTests(unittest.TestCase):
    def test_public_rest_verification(self) -> None:
        result = verify_public_rest()
        self.assertEqual(result.status, "passed")

    def test_public_websocket_verification(self) -> None:
        result = verify_public_websocket()
        self.assertEqual(result.status, "passed")

    def test_real_data_strategy_analytics_smoke(self) -> None:
        with TemporaryDirectory() as temp_dir:
            warehouse = ResearchWarehouse(root=temp_dir)
            fetcher = HistoricalMarketFetcher(warehouse=warehouse)
            portal, imported = fetcher.build_research_portal(
                symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"],
                lookback_days=14,
                interval_minutes=60,
                include_option_snapshots=False,
                end_time=datetime(2026, 4, 15, tzinfo=UTC),
            )
            session = NotebookSession(data_portal=portal, warehouse=warehouse)
            for strategy_name in [
                "perp_trend",
                "perp_mean_reversion",
                "carry_basket",
                "momentum_rotation",
                "volatility_breakout",
            ]:
                report = session.backtest_analytics(strategy_name, symbols=imported["symbols"])
                self.assertIn("ending_equity", report.as_dict())
                self.assertGreaterEqual(report.trade_count, 0)


if __name__ == "__main__":
    unittest.main()
