from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import os
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bybit_trader.analytics import build_backtest_analytics, build_option_research_summary
from bybit_trader.data import InMemoryDataPortal, ResearchWarehouse, build_sample_portal
from bybit_trader.demo_credentials import load_demo_credentials
from bybit_trader.historical import HistoricalMarketFetcher
from bybit_trader.models import AccountState, BacktestResult, EquityPoint, ExecutionReport, OptionChainSnapshot, OptionQuote
from bybit_trader.session import NotebookSession


class DemoCredentialTests(unittest.TestCase):
    def test_local_loader_succeeds_and_repr_masks_secrets(self) -> None:
        credentials = load_demo_credentials(required=True)
        self.assertEqual(credentials.source, "local_source")
        rendered = repr(credentials)
        self.assertNotIn(credentials.api_key, rendered)
        self.assertNotIn(credentials.api_secret, rendered)
        self.assertIn("...", credentials.masked()["api_key"])

    def test_env_fallback_works_when_local_source_is_disabled(self) -> None:
        with patch.dict(
            os.environ,
            {
                "BYBIT_TRADER_DISABLE_LOCAL_DEMO_CREDS": "1",
                "BYBIT_DEMO_API_KEY": "env_key_1234",
                "BYBIT_DEMO_API_SECRET": "env_secret_5678",
            },
            clear=False,
        ):
            credentials = load_demo_credentials(required=True)
        self.assertEqual(credentials.source, "environment")
        self.assertEqual(credentials.api_key, "env_key_1234")

    def test_session_can_attach_default_demo_client(self) -> None:
        session = NotebookSession.with_sample_data()
        session.attach_default_demo_client()
        self.assertIsNotNone(session.bybit_client)
        self.assertTrue(session.bybit_client.demo)


class HistoricalFetcherTests(unittest.TestCase):
    def test_instrument_pagination_and_kline_dedup_persist_to_warehouse(self) -> None:
        calls: list[tuple[str, dict]] = []

        def fake_get_json(path: str, params: dict) -> dict:
            calls.append((path, dict(params)))
            if path == "/v5/market/instruments-info":
                if params.get("cursor") is None:
                    return {
                        "retCode": 0,
                        "result": {
                            "list": [
                                {
                                    "symbol": "BTCUSDT",
                                    "baseCoin": "BTC",
                                    "quoteCoin": "USDT",
                                    "status": "Trading",
                                    "launchTime": "1704067200000",
                                    "priceFilter": {"tickSize": "0.1"},
                                    "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001"},
                                    "leverageFilter": {"maxLeverage": "100"},
                                }
                            ],
                            "nextPageCursor": "cursor-2",
                        },
                    }
                return {
                    "retCode": 0,
                    "result": {
                        "list": [
                            {
                                "symbol": "XRPUSDT",
                                "baseCoin": "XRP",
                                "quoteCoin": "USDT",
                                "status": "Trading",
                                "launchTime": "1704067200000",
                                "priceFilter": {"tickSize": "0.0001"},
                                "lotSizeFilter": {"qtyStep": "1", "minOrderQty": "1"},
                                "leverageFilter": {"maxLeverage": "50"},
                            }
                        ],
                        "nextPageCursor": "",
                    },
                }
            if path == "/v5/market/kline":
                if params["end"] == 1714608000000:
                    return {
                        "retCode": 0,
                        "result": {
                            "list": [
                                ["1714525200000", "101", "106", "100", "102", "11", "1122"],
                                ["1714525200000", "101", "106", "100", "102", "11", "1122"],
                                ["1714528800000", "102", "107", "101", "103", "12", "1236"],
                            ]
                        },
                    }
                if params["end"] == 1714521600000:
                    return {
                        "retCode": 0,
                        "result": {
                            "list": [
                                ["1714521600000", "100", "105", "95", "101", "10", "1010"],
                            ]
                        },
                    }
                return {"retCode": 0, "result": {"list": []}}
            raise AssertionError(f"Unexpected path {path}")

        with TemporaryDirectory() as temp_dir:
            warehouse = ResearchWarehouse(root=temp_dir, config=None)
            fetcher = HistoricalMarketFetcher(warehouse=warehouse, public_get_json=fake_get_json)
            instruments = fetcher.fetch_linear_instruments()
            bars = fetcher.fetch_linear_klines(
                "BTCUSDT",
                lookback_days=1,
                interval_minutes=60,
                end_time=datetime(2024, 5, 2, 0, 0, tzinfo=UTC),
            )

            self.assertEqual([instrument.symbol for instrument in instruments], ["BTCUSDT", "XRPUSDT"])
            self.assertEqual([bar.close for bar in bars], [101.0, 102.0, 103.0])
            self.assertEqual(len(warehouse.read_dataset("linear_instruments")), 2)
            self.assertEqual(len(warehouse.read_dataset("kline_BTCUSDT")), 3)
            self.assertTrue(any(path == "/v5/market/instruments-info" for path, _ in calls))


class AnalyticsTests(unittest.TestCase):
    def test_backtest_analytics_metrics_include_funding_and_symbol_contributions(self) -> None:
        result = BacktestResult(
            strategy_name="demo_strategy",
            initial_cash=1000.0,
            ending_equity=1085.0,
            equity_curve=[
                EquityPoint(ts=datetime(2025, 1, 1, tzinfo=UTC), equity=1000.0, cash=1000.0, gross_notional=100.0, drawdown=0.0),
                EquityPoint(ts=datetime(2025, 1, 2, tzinfo=UTC), equity=1085.0, cash=900.0, gross_notional=200.0, drawdown=0.05),
            ],
            trades=[
                ExecutionReport(
                    order_id="1",
                    symbol="BTCUSDT",
                    filled_qty=1.0,
                    avg_price=100.0,
                    fees=1.0,
                    status="Filled",
                    ts=datetime(2025, 1, 1, tzinfo=UTC),
                    realized_pnl=15.0,
                )
            ],
            funding_events=[
                ExecutionReport(
                    order_id="funding-1",
                    symbol="BTCUSDT",
                    filled_qty=0.0,
                    avg_price=0.0,
                    fees=0.0,
                    status="Funding",
                    ts=datetime(2025, 1, 1, tzinfo=UTC),
                    realized_pnl=-2.0,
                )
            ],
            final_account=AccountState(),
            final_prices={},
            metadata={"symbols": ["BTCUSDT"]},
        )
        report = build_backtest_analytics(result)
        payload = report.as_dict()
        self.assertEqual(payload["trade_count"], 1)
        self.assertAlmostEqual(payload["funding_contribution"], -2.0)
        self.assertEqual(payload["best_symbol"], "BTCUSDT")

    def test_option_research_summary_stays_research_only(self) -> None:
        snapshot = OptionChainSnapshot(
            ts=datetime(2025, 1, 1, tzinfo=UTC),
            base_coin="BTC",
            historical_volatility=0.5,
            quotes=(
                OptionQuote(
                    ts=datetime(2025, 1, 1, tzinfo=UTC),
                    symbol="BTC-01JAN25-70000-C-USDT",
                    underlying_symbol="BTCUSDT",
                    option_type="Call",
                    strike=70000.0,
                    expiry=datetime(2025, 1, 31, tzinfo=UTC),
                    bid=1000.0,
                    ask=1100.0,
                    mark_price=1050.0,
                    mark_iv=0.45,
                    delta=0.5,
                    gamma=0.01,
                    vega=12.0,
                    theta=-1.0,
                    underlying_price=70200.0,
                ),
            ),
        )
        summary = build_option_research_summary(snapshot)
        self.assertTrue(summary.research_only)
        self.assertIn("Research-only", summary.notes)

    def test_session_real_data_hook_can_swap_in_a_portal(self) -> None:
        portal = build_sample_portal(symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"), periods=220)
        session = NotebookSession()
        with patch("bybit_trader.session.HistoricalMarketFetcher.build_research_portal", return_value=(portal, {"symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]})):
            imported = session.fetch_real_market_data()
        self.assertEqual(imported["symbols"][0], "BTCUSDT")
        self.assertEqual(session.resolve_universe()[:2], ["BTCUSDT", "ETHUSDT"])

    def test_session_backtest_analytics_returns_notebook_friendly_report(self) -> None:
        session = NotebookSession.with_sample_data(periods=240)
        report = session.backtest_analytics("momentum_rotation")
        self.assertIn("ending_equity", report.as_dict())
        self.assertIn("trade_count", report.as_dict())


if __name__ == "__main__":
    unittest.main()
