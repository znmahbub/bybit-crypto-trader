from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

from .analytics import BacktestAnalyticsReport, build_backtest_analytics, build_option_research_summary
from .backtest import BacktestEngine
from .config import AppConfig, StrategyConfig
from .data import InMemoryDataPortal, ResearchWarehouse, build_sample_portal
from .demo_credentials import load_demo_credentials
from .exchange import BybitBrokerAdapter, BybitClient, InstrumentRegistry, PaperBroker
from .historical import HistoricalMarketFetcher
from .risk import RiskEngine
from .strategies import STRATEGY_LIBRARY, build_strategy


class NotebookSession:
    def __init__(
        self,
        *,
        config: AppConfig | None = None,
        data_portal: InMemoryDataPortal | None = None,
        warehouse: ResearchWarehouse | None = None,
        instrument_registry: InstrumentRegistry | None = None,
        risk_engine: RiskEngine | None = None,
        broker=None,
        bybit_client: BybitClient | None = None,
    ) -> None:
        self.config = config or AppConfig()
        self.data_portal = data_portal or InMemoryDataPortal()
        self.warehouse = warehouse or ResearchWarehouse(self.config.storage)
        self.instrument_registry = instrument_registry or InstrumentRegistry(self.config.universe)
        self.risk_engine = risk_engine or RiskEngine(self.config.risk)
        self.bybit_client = bybit_client
        self.broker = broker or PaperBroker()
        self._sync_registry_from_portal()

    @classmethod
    def from_config(cls, path: str | Path) -> "NotebookSession":
        config = AppConfig.load(path)
        return cls(config=config)

    @classmethod
    def with_sample_data(
        cls,
        *,
        config_path: str | Path | None = None,
        symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"),
        periods: int = 300,
    ) -> "NotebookSession":
        config = AppConfig.load(config_path) if config_path else AppConfig()
        portal = build_sample_portal(symbols=symbols, periods=periods)
        return cls(config=config, data_portal=portal, broker=PaperBroker())

    def attach_live_client(self, api_key: str, api_secret: str, *, testnet: bool = False, demo: bool = True) -> "NotebookSession":
        self.bybit_client = BybitClient(api_key=api_key, api_secret=api_secret, testnet=testnet, demo=demo)
        self.broker = BybitBrokerAdapter(self.bybit_client)
        return self

    def attach_default_demo_client(self) -> "NotebookSession":
        credentials = load_demo_credentials(required=True)
        return self.attach_live_client(
            credentials.api_key,
            credentials.api_secret,
            testnet=credentials.testnet,
            demo=credentials.demo,
        )

    def list_strategies(self) -> list[str]:
        return sorted(STRATEGY_LIBRARY.keys())

    def resolve_universe(self, symbols: list[str] | None = None) -> list[str]:
        if symbols:
            return symbols
        funding_counts = {symbol: len(self.data_portal.get_funding(symbol)) for symbol in self.data_portal.list_symbols("linear")}
        eligible = self.instrument_registry.linear_universe(funding_counts=funding_counts)
        if eligible:
            return [item.symbol for item in eligible]
        return self.data_portal.list_symbols("linear")

    def research(self, symbols: list[str] | None = None, lookback: int = 100) -> dict[str, dict[str, float]]:
        output: dict[str, dict[str, float]] = {}
        for symbol in self.resolve_universe(symbols):
            bars = self.data_portal.get_bars(symbol, limit=lookback)
            if len(bars) < 2:
                continue
            closes = [bar.close for bar in bars]
            returns = (closes[-1] / closes[0]) - 1.0
            avg_volume = sum(bar.volume for bar in bars) / len(bars)
            funding = self.data_portal.get_funding(symbol, limit=10)
            avg_funding = sum(item.rate for item in funding) / len(funding) if funding else 0.0
            output[symbol] = {
                "last_price": closes[-1],
                "period_return": returns,
                "avg_volume": avg_volume,
                "avg_funding": avg_funding,
            }
        return output

    def backtest(
        self,
        strategy_name: str,
        *,
        strategy_params: dict[str, Any] | None = None,
        symbols: list[str] | None = None,
        initial_cash: float = 100_000.0,
    ):
        config_params = self._strategy_params(strategy_name)
        merged = {**config_params, **(strategy_params or {})}
        strategy = build_strategy(strategy_name, merged)
        broker = PaperBroker(initial_cash=initial_cash)
        engine = BacktestEngine(self.data_portal, broker=broker, risk_engine=RiskEngine(self.config.risk), initial_cash=initial_cash)
        return engine.run(strategy, self.resolve_universe(symbols))

    def backtest_analytics(
        self,
        strategy_name: str,
        *,
        strategy_params: dict[str, Any] | None = None,
        symbols: list[str] | None = None,
        initial_cash: float = 100_000.0,
    ) -> BacktestAnalyticsReport:
        result = self.backtest(
            strategy_name,
            strategy_params=strategy_params,
            symbols=symbols,
            initial_cash=initial_cash,
        )
        return build_backtest_analytics(result)

    def paper_trade(
        self,
        strategy_name: str,
        *,
        strategy_params: dict[str, Any] | None = None,
        symbols: list[str] | None = None,
        as_of=None,
    ):
        config_params = self._strategy_params(strategy_name)
        merged = {**config_params, **(strategy_params or {})}
        strategy = build_strategy(strategy_name, merged)
        universe = self.resolve_universe(symbols)
        latest_timestamps = [bars[-1].ts for symbol in universe if (bars := self.data_portal.get_bars(symbol))]
        if not latest_timestamps:
            return []
        as_of = as_of or max(latest_timestamps)
        account = self.broker.reconcile_state()
        prices = self.data_portal.latest_prices(universe, as_of=as_of)
        risk_state = self.risk_engine.evaluate(account, prices, as_of=as_of)
        state = self.data_portal.strategy_state(
            as_of=as_of,
            universe=universe,
            account_state=account,
            risk_state=risk_state,
            run_mode=self.config.execution.run_mode,
            bar_limit=max(strategy.min_history + 5, 200),
        )
        targets = strategy.generate_targets(state)
        targets = self.risk_engine.filter_targets(targets, state.prices, state.risk_state)
        orders = strategy.rebalance(targets, account, state.prices)
        orders = self.risk_engine.allow_orders(orders, state.prices)
        reports = self.broker.submit_orders(orders, market_prices=state.prices, timestamp=as_of)
        self.data_portal.add_execution_reports(reports)
        return reports

    def live_monitor(self) -> dict[str, Any]:
        account = self.broker.reconcile_state()
        prices = self.data_portal.latest_prices(self.resolve_universe())
        risk = self.risk_engine.evaluate(account, prices)
        return {
            "universe": self.resolve_universe(),
            "positions": {symbol: asdict(position) for symbol, position in account.positions.items()},
            "cash": account.cash,
            "equity": risk.equity,
            "drawdown": risk.drawdown,
            "last_registry_refresh": self.instrument_registry.last_refresh.isoformat() if self.instrument_registry.last_refresh else None,
        }

    def promote_strategy(
        self,
        result,
        *,
        min_total_return: float = 0.0,
        max_drawdown: float = 0.18,
        min_trade_count: int = 5,
    ) -> dict[str, Any]:
        summary = result.summary()
        approved = (
            summary["total_return"] >= min_total_return
            and summary["max_drawdown"] <= max_drawdown
            and summary["trade_count"] >= min_trade_count
        )
        return {
            "approved": approved,
            "summary": summary,
            "criteria": {
                "min_total_return": min_total_return,
                "max_drawdown": max_drawdown,
                "min_trade_count": min_trade_count,
            },
        }

    def fetch_real_market_data(
        self,
        *,
        symbols: list[str] | None = None,
        lookback_days: int = 180,
        interval_minutes: int = 60,
        include_option_snapshots: bool = True,
        end_time=None,
    ) -> dict[str, Any]:
        fetcher = HistoricalMarketFetcher(warehouse=self.warehouse)
        portal, imported = fetcher.build_research_portal(
            symbols=symbols,
            lookback_days=lookback_days,
            interval_minutes=interval_minutes,
            include_option_snapshots=include_option_snapshots,
            end_time=end_time,
        )
        self.data_portal = portal
        self._sync_registry_from_portal()
        return imported

    def option_research_summary(self, *, base_coins: tuple[str, ...] = ("BTC", "ETH")) -> list[dict[str, Any]]:
        summaries: list[dict[str, Any]] = []
        for base_coin in base_coins:
            snapshot = self.data_portal.get_option_chain(base_coin)
            if snapshot is None:
                continue
            summaries.append(build_option_research_summary(snapshot).as_dict())
        return summaries

    def run_default_perp_analytics(
        self,
        *,
        strategy_names: list[str] | None = None,
        symbols: list[str] | None = None,
        initial_cash: float = 100_000.0,
    ) -> list[dict[str, Any]]:
        selected = strategy_names or [
            "perp_trend",
            "perp_mean_reversion",
            "carry_basket",
            "momentum_rotation",
            "volatility_breakout",
        ]
        reports: list[dict[str, Any]] = []
        for strategy_name in selected:
            report = self.backtest_analytics(strategy_name, symbols=symbols, initial_cash=initial_cash)
            reports.append(report.as_dict())
        return reports

    def build_runner(self, strategy_name: str, *, strategy_params: dict[str, Any] | None = None):
        from .runner import TradingRunner

        config_params = self._strategy_params(strategy_name)
        merged = {**config_params, **(strategy_params or {})}
        return TradingRunner(session=self, strategy_name=strategy_name, strategy_params=merged)

    def _strategy_params(self, strategy_name: str) -> dict[str, Any]:
        config_item: StrategyConfig | None = self.config.strategy(strategy_name)
        return {} if config_item is None else dict(config_item.params)

    def _sync_registry_from_portal(self) -> None:
        instruments = self.data_portal.get_instruments()
        if instruments:
            self.instrument_registry.upsert(instruments)
