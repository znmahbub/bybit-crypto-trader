from __future__ import annotations

from datetime import datetime

from .data import InMemoryDataPortal
from .exchange import PaperBroker
from .models import BacktestResult, EquityPoint, RunMode
from .risk import RiskEngine
from .strategies import Strategy


class BacktestEngine:
    def __init__(
        self,
        data_portal: InMemoryDataPortal,
        *,
        broker: PaperBroker | None = None,
        risk_engine: RiskEngine | None = None,
        initial_cash: float = 100_000.0,
    ) -> None:
        self.data_portal = data_portal
        self.broker = broker or PaperBroker(initial_cash=initial_cash)
        self.risk_engine = risk_engine
        self.initial_cash = initial_cash
        self._applied_funding: set[tuple[str, datetime]] = set()
        self._funding_events = []

    def run(self, strategy: Strategy, symbols: list[str] | None = None) -> BacktestResult:
        self._applied_funding.clear()
        self._funding_events = []
        universe = tuple(symbols or self.data_portal.list_symbols("linear"))
        if strategy.category == "option":
            universe = tuple()
        if strategy.category == "mixed":
            universe = tuple(symbols or self.data_portal.list_symbols("linear"))
        strategy.prepare_features(self.data_portal)

        timestamps = self._timeline(universe, include_options=strategy.category in {"option", "mixed"})
        trades = []
        equity_curve: list[EquityPoint] = []
        last_prices: dict[str, float] = {}
        warmup = max(0, strategy.min_history - 1)
        for index, ts in enumerate(timestamps):
            prices = self.data_portal.latest_prices(universe, as_of=ts)
            last_prices = dict(prices)
            if index < warmup:
                continue
            account_state = self.broker.reconcile_state()
            risk_state = self.risk_engine.evaluate(account_state, prices, as_of=ts) if self.risk_engine else None
            if risk_state is None:
                from .models import RiskState

                equity = account_state.mark_to_market(prices)
                risk_state = RiskState(
                    equity=equity,
                    peak_equity=equity,
                    drawdown=0.0,
                    gross_notional=0.0,
                    net_notional=0.0,
                    leverage=0.0,
                )
            state = self.data_portal.strategy_state(
                as_of=ts,
                universe=universe,
                account_state=account_state,
                risk_state=risk_state,
                run_mode=RunMode.BACKTEST,
                bar_limit=max(strategy.min_history + 5, 200),
            )
            if not state.prices:
                continue
            targets = strategy.generate_targets(state)
            if self.risk_engine:
                targets = self.risk_engine.filter_targets(targets, state.prices, state.risk_state)
            orders = strategy.rebalance(targets, account_state, state.prices)
            if self.risk_engine:
                orders = self.risk_engine.allow_orders(orders, state.prices)
            trades.extend(self.broker.submit_orders(orders, market_prices=state.prices, timestamp=ts))
            self._apply_funding(ts, universe, state.prices)
            account_state = self.broker.reconcile_state()
            updated_risk = self.risk_engine.evaluate(account_state, state.prices, as_of=ts) if self.risk_engine else risk_state
            equity_curve.append(
                EquityPoint(
                    ts=ts,
                    equity=account_state.mark_to_market(state.prices),
                    cash=account_state.cash,
                    gross_notional=updated_risk.gross_notional,
                    drawdown=updated_risk.drawdown,
                )
            )

        ending_equity = equity_curve[-1].equity if equity_curve else self.initial_cash
        return BacktestResult(
            strategy_name=strategy.name,
            initial_cash=self.initial_cash,
            ending_equity=ending_equity,
            equity_curve=equity_curve,
            trades=trades,
            funding_events=list(self._funding_events),
            final_account=self.broker.reconcile_state(),
            final_prices=last_prices,
            metadata={"symbols": list(universe)},
        )

    def _apply_funding(self, ts: datetime, universe: tuple[str, ...], prices: dict[str, float]) -> None:
        if not hasattr(self.broker, "apply_funding"):
            return
        for symbol in universe:
            for observation in self.data_portal.get_funding(symbol, as_of=ts):
                marker = (symbol, observation.ts)
                if observation.ts == ts and marker not in self._applied_funding:
                    payment = self.broker.apply_funding(symbol, observation.rate, prices.get(symbol, 0.0))
                    if payment:
                        from .models import ExecutionReport

                        self._funding_events.append(
                            ExecutionReport(
                                order_id=f"funding-{symbol}-{int(observation.ts.timestamp())}",
                                symbol=symbol,
                                filled_qty=0.0,
                                avg_price=prices.get(symbol, 0.0),
                                fees=0.0,
                                status="Funding",
                                ts=observation.ts,
                                realized_pnl=-payment,
                                metadata={"rate": observation.rate},
                            )
                        )
                    self._applied_funding.add(marker)

    def _timeline(self, universe: tuple[str, ...], *, include_options: bool) -> list[datetime]:
        timestamps = set()
        for symbol in universe:
            timestamps.update(bar.ts for bar in self.data_portal.get_bars(symbol))
        if include_options:
            for base_coin in {instrument.base_coin for instrument in self.data_portal.get_instruments("option")}:
                snapshot = self.data_portal.get_option_chain(base_coin)
                if snapshot is not None:
                    timestamps.add(snapshot.ts)
        return sorted(timestamps)
