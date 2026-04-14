from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
import json
import time
from typing import Any

from .models import RunMode
from .strategies import build_strategy


@dataclass(slots=True)
class TradingRunner:
    session: Any
    strategy_name: str
    strategy_params: dict[str, Any] = field(default_factory=dict)

    def bootstrap(self) -> dict[str, Any]:
        self.session._sync_registry_from_portal()
        return {
            "strategy": self.strategy_name,
            "run_mode": self.session.config.execution.run_mode.value,
            "execution_mode": self.session.config.execution.execution_mode.value,
            "universe": self.session.resolve_universe(),
        }

    def run_cycle(self, *, as_of: datetime | None = None) -> dict[str, Any]:
        strategy = build_strategy(self.strategy_name, self.strategy_params)
        universe = self.session.resolve_universe()
        latest_timestamps = [bars[-1].ts for symbol in universe if (bars := self.session.data_portal.get_bars(symbol))]
        as_of = as_of or (max(latest_timestamps) if latest_timestamps else datetime.now(UTC))
        account = self.session.broker.reconcile_state()
        prices = self.session.data_portal.latest_prices(universe, as_of=as_of)
        risk_state = self.session.risk_engine.evaluate(account, prices, as_of=as_of)
        state = self.session.data_portal.strategy_state(
            as_of=as_of,
            universe=universe,
            account_state=account,
            risk_state=risk_state,
            run_mode=self.session.config.execution.run_mode if hasattr(self.session.config.execution, "run_mode") else RunMode.BACKTEST,
            bar_limit=max(strategy.min_history + 5, 200),
        )
        targets = strategy.generate_targets(state)
        targets = self.session.risk_engine.filter_targets(targets, state.prices, state.risk_state)
        orders = strategy.rebalance(targets, account, state.prices)
        orders = self.session.risk_engine.allow_orders(orders, state.prices)
        reports = self.session.broker.submit_orders(orders, market_prices=state.prices, timestamp=as_of)
        return {
            "as_of": as_of.isoformat(),
            "orders_submitted": len(orders),
            "reports": [asdict(report) for report in reports],
            "monitor": self.session.live_monitor(),
        }

    def run_forever(self, *, cycles: int | None = None) -> list[dict[str, Any]]:
        outputs: list[dict[str, Any]] = []
        executed = 0
        while cycles is None or executed < cycles:
            outputs.append(self.run_cycle())
            executed += 1
            if cycles is not None and executed >= cycles:
                break
            time.sleep(self.session.config.execution.poll_interval_seconds)
        return outputs


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Bybit trader service loop.")
    parser.add_argument("--config", default="configs/default.toml", help="Path to the TOML config file.")
    parser.add_argument("--strategy", default="perp_trend", help="Strategy name to run.")
    parser.add_argument("--cycles", type=int, default=1, help="Number of cycles to execute.")
    parser.add_argument("--sample-data", action="store_true", help="Use synthetic sample data instead of exchange connectivity.")
    args = parser.parse_args()

    from .session import NotebookSession

    if args.sample_data:
        session = NotebookSession.with_sample_data(config_path=args.config)
    else:
        session = NotebookSession.from_config(args.config)
    runner = session.build_runner(args.strategy)
    runner.bootstrap()
    outputs = runner.run_forever(cycles=args.cycles)
    for payload in outputs:
        print(json.dumps(payload, default=str))


if __name__ == "__main__":
    main()
