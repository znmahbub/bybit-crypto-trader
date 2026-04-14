from __future__ import annotations

from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bybit_trader.config import UniverseConfig
from bybit_trader.data import build_sample_portal
from bybit_trader.exchange import InstrumentRegistry, PaperBroker
from bybit_trader.models import InstrumentMeta, OrderIntent
from bybit_trader.session import NotebookSession


class InstrumentRegistryTests(unittest.TestCase):
    def test_dynamic_universe_filters_arbitrary_symbols(self) -> None:
        config = UniverseConfig(
            dynamic=True,
            symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
            min_24h_turnover=5_000_000,
            max_spread_bps=15,
            min_listing_days=30,
            require_funding_observations=3,
        )
        registry = InstrumentRegistry(config)
        registry.upsert(
            [
                InstrumentMeta(symbol="BTCUSDT", category="linear", base_coin="BTC", quote_coin="USDT", turnover24h=1_000_000, spread_bps=40),
                InstrumentMeta(symbol="ETHUSDT", category="linear", base_coin="ETH", quote_coin="USDT", turnover24h=1_000_000, spread_bps=40),
                InstrumentMeta(symbol="SOLUSDT", category="linear", base_coin="SOL", quote_coin="USDT", turnover24h=1_000_000, spread_bps=40),
                InstrumentMeta(symbol="XRPUSDT", category="linear", base_coin="XRP", quote_coin="USDT", turnover24h=10_000_000, spread_bps=8),
                InstrumentMeta(symbol="LOWUSDT", category="linear", base_coin="LOW", quote_coin="USDT", turnover24h=10_000, spread_bps=8),
            ]
        )
        selected = registry.linear_universe(funding_counts={"XRPUSDT": 4, "LOWUSDT": 4})
        symbols = [item.symbol for item in selected]
        self.assertIn("BTCUSDT", symbols)
        self.assertIn("ETHUSDT", symbols)
        self.assertIn("SOLUSDT", symbols)
        self.assertIn("XRPUSDT", symbols)
        self.assertNotIn("LOWUSDT", symbols)


class BacktestAndSessionTests(unittest.TestCase):
    def test_backtest_runs_and_produces_summary(self) -> None:
        session = NotebookSession.with_sample_data()
        result = session.backtest("perp_trend")
        summary = result.summary()
        self.assertGreater(len(result.equity_curve), 0)
        self.assertIn("total_return", summary)
        self.assertGreaterEqual(summary["trade_count"], 0.0)

    def test_paper_broker_realizes_pnl_on_round_trip(self) -> None:
        broker = PaperBroker(initial_cash=10_000, fee_bps=0, slippage_bps=0)
        broker.submit_orders([OrderIntent(symbol="BTCUSDT", quantity=1.0)], market_prices={"BTCUSDT": 100.0})
        reports = broker.submit_orders([OrderIntent(symbol="BTCUSDT", quantity=-1.0)], market_prices={"BTCUSDT": 110.0})
        self.assertAlmostEqual(reports[0].realized_pnl, 10.0)

    def test_option_strategy_backtest_has_access_to_option_prices(self) -> None:
        session = NotebookSession.with_sample_data()
        result = session.backtest("option_iv_hv_long_gamma")
        self.assertGreater(len(result.equity_curve), 0)

    def test_session_research_and_runner_are_notebook_friendly(self) -> None:
        session = NotebookSession.with_sample_data()
        research = session.research(["BTCUSDT", "ETHUSDT"])
        self.assertIn("BTCUSDT", research)
        runner = session.build_runner("momentum_rotation")
        payload = runner.run_cycle()
        self.assertIn("monitor", payload)
        self.assertIn("orders_submitted", payload)


if __name__ == "__main__":
    unittest.main()
