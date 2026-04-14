from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from random import Random
import json
from typing import Iterable

from .config import StorageConfig
from .models import (
    AccountState,
    ExecutionReport,
    FundingObservation,
    InstrumentMeta,
    MarketBar,
    OptionChainSnapshot,
    OptionQuote,
    RiskState,
    RunMode,
    StrategyState,
    ensure_utc,
    utc_now,
)


class InMemoryDataPortal:
    def __init__(self) -> None:
        self._bars: dict[str, list[MarketBar]] = defaultdict(list)
        self._funding: dict[str, list[FundingObservation]] = defaultdict(list)
        self._trades: dict[str, list] = defaultdict(list)
        self._instruments: dict[str, InstrumentMeta] = {}
        self._option_chains: dict[str, list[OptionChainSnapshot]] = defaultdict(list)
        self._account_replay: list[ExecutionReport] = []

    def add_bars(self, bars: Iterable[MarketBar]) -> None:
        for bar in bars:
            self._bars[bar.symbol].append(bar)
        for symbol in self._bars:
            self._bars[symbol].sort(key=lambda item: item.ts)

    def add_funding(self, observations: Iterable[FundingObservation]) -> None:
        for item in observations:
            self._funding[item.symbol].append(item)
        for symbol in self._funding:
            self._funding[symbol].sort(key=lambda item: item.ts)

    def add_instruments(self, instruments: Iterable[InstrumentMeta]) -> None:
        for instrument in instruments:
            self._instruments[instrument.symbol] = instrument

    def add_option_chain(self, snapshot: OptionChainSnapshot) -> None:
        self._option_chains[snapshot.base_coin].append(snapshot)
        self._option_chains[snapshot.base_coin].sort(key=lambda item: item.ts)

    def add_execution_reports(self, reports: Iterable[ExecutionReport]) -> None:
        self._account_replay.extend(reports)
        self._account_replay.sort(key=lambda item: item.ts)

    def get_bars(self, symbol: str, limit: int | None = None, as_of: datetime | None = None) -> list[MarketBar]:
        bars = self._bars.get(symbol, [])
        if as_of is not None:
            cutoff = ensure_utc(as_of)
            bars = [bar for bar in bars if bar.ts <= cutoff]
        if limit is None:
            return list(bars)
        return list(bars[-limit:])

    def get_funding(self, symbol: str, limit: int | None = None, as_of: datetime | None = None) -> list[FundingObservation]:
        funding = self._funding.get(symbol, [])
        if as_of is not None:
            cutoff = ensure_utc(as_of)
            funding = [item for item in funding if item.ts <= cutoff]
        if limit is None:
            return list(funding)
        return list(funding[-limit:])

    def get_trades(self, symbol: str, limit: int | None = None) -> list:
        trades = self._trades.get(symbol, [])
        if limit is None:
            return list(trades)
        return list(trades[-limit:])

    def get_instruments(self, category: str | None = None) -> list[InstrumentMeta]:
        items = list(self._instruments.values())
        if category is None:
            return items
        return [item for item in items if item.category == category]

    def get_option_chain(self, base_coin: str, as_of: datetime | None = None) -> OptionChainSnapshot | None:
        snapshots = self._option_chains.get(base_coin, [])
        if not snapshots:
            return None
        if as_of is None:
            return snapshots[-1]
        cutoff = ensure_utc(as_of)
        eligible = [snapshot for snapshot in snapshots if snapshot.ts <= cutoff]
        return eligible[-1] if eligible else None

    def get_account_replay(self) -> list[ExecutionReport]:
        return list(self._account_replay)

    def list_symbols(self, category: str = "linear") -> list[str]:
        return [item.symbol for item in self.get_instruments(category)]

    def latest_prices(self, universe: Iterable[str], as_of: datetime | None = None) -> dict[str, float]:
        prices: dict[str, float] = {}
        for symbol in universe:
            bars = self.get_bars(symbol, limit=1, as_of=as_of)
            if bars:
                prices[symbol] = bars[-1].close
        return prices

    def strategy_state(
        self,
        *,
        as_of: datetime,
        universe: Iterable[str],
        account_state: AccountState,
        risk_state: RiskState,
        run_mode: RunMode,
        bar_limit: int | None = None,
    ) -> StrategyState:
        selected = tuple(universe)
        bars = {symbol: self.get_bars(symbol, limit=bar_limit, as_of=as_of) for symbol in selected}
        funding = {symbol: self.get_funding(symbol, as_of=as_of) for symbol in selected}
        prices = self.latest_prices(selected, as_of=as_of)
        option_chains = {
            base_coin: snapshot
            for base_coin in {instrument.base_coin for instrument in self.get_instruments("option")}
            if (snapshot := self.get_option_chain(base_coin, as_of=as_of)) is not None
        }
        for snapshot in option_chains.values():
            for quote in snapshot.quotes:
                prices.setdefault(quote.underlying_symbol, quote.underlying_price)
                prices[quote.symbol] = quote.mid
        return StrategyState(
            as_of=ensure_utc(as_of) or utc_now(),
            run_mode=run_mode,
            universe=selected,
            bars=bars,
            funding=funding,
            option_chains=option_chains,
            prices=prices,
            account_state=account_state,
            risk_state=risk_state,
        )


class ResearchWarehouse:
    def __init__(self, config: StorageConfig | None = None, root: str | Path | None = None) -> None:
        if config is None:
            config = StorageConfig()
        default_config = StorageConfig()
        self.root = Path(root or config.root)
        if root is not None and config.raw_path == default_config.raw_path:
            self.raw_path = self.root / "raw"
        else:
            self.raw_path = Path(config.raw_path)
        if root is not None and config.normalized_path == default_config.normalized_path:
            self.normalized_path = self.root / "normalized"
        else:
            self.normalized_path = Path(config.normalized_path)
        self.root.mkdir(parents=True, exist_ok=True)
        self.raw_path.mkdir(parents=True, exist_ok=True)
        self.normalized_path.mkdir(parents=True, exist_ok=True)

    def append_raw(self, stream_name: str, payload: dict) -> Path:
        target = self.raw_path / f"{stream_name}.jsonl"
        with target.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, default=str))
            handle.write("\n")
        return target

    def append_normalized(self, dataset_name: str, rows: Iterable[dict]) -> Path:
        target = self.normalized_path / f"{dataset_name}.jsonl"
        with target.open("a", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, default=str))
                handle.write("\n")
        return target

    def record_option_chain(self, snapshot: OptionChainSnapshot) -> Path:
        rows = []
        for quote in snapshot.quotes:
            row = asdict(quote)
            row["ts"] = quote.ts.isoformat()
            row["expiry"] = quote.expiry.isoformat()
            row["base_coin"] = snapshot.base_coin
            row["historical_volatility"] = snapshot.historical_volatility
            rows.append(row)
        self.append_raw("option_chain_raw", {"base_coin": snapshot.base_coin, "ts": snapshot.ts.isoformat(), "size": len(rows)})
        return self.append_normalized("option_chain", rows)

    def dataset_path(self, dataset_name: str, *, raw: bool = False) -> Path:
        base = self.raw_path if raw else self.normalized_path
        return base / f"{dataset_name}.jsonl"

    def read_dataset(self, dataset_name: str, *, raw: bool = False) -> list[dict]:
        target = self.dataset_path(dataset_name, raw=raw)
        if not target.exists():
            return []
        rows = []
        with target.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    rows.append(json.loads(line))
        return rows


def build_sample_portal(
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"),
    periods: int = 300,
    seed: int = 7,
) -> InMemoryDataPortal:
    rng = Random(seed)
    start = datetime(2025, 1, 1, tzinfo=UTC)
    portal = InMemoryDataPortal()
    instruments: list[InstrumentMeta] = []
    bars: list[MarketBar] = []
    funding: list[FundingObservation] = []
    latest_prices: dict[str, float] = {}

    base_prices = {
        "BTCUSDT": 70_000.0,
        "ETHUSDT": 3_600.0,
        "SOLUSDT": 180.0,
        "XRPUSDT": 0.65,
        "DOGEUSDT": 0.15,
    }
    trends = {
        "BTCUSDT": 0.0012,
        "ETHUSDT": 0.0010,
        "SOLUSDT": 0.0018,
        "XRPUSDT": 0.0007,
        "DOGEUSDT": 0.0015,
    }
    vol = {
        "BTCUSDT": 0.012,
        "ETHUSDT": 0.016,
        "SOLUSDT": 0.028,
        "XRPUSDT": 0.020,
        "DOGEUSDT": 0.030,
    }

    for index, symbol in enumerate(symbols):
        launch_time = start - timedelta(days=365 + index * 10)
        instruments.append(
            InstrumentMeta(
                symbol=symbol,
                category="linear",
                base_coin=symbol.replace("USDT", ""),
                quote_coin="USDT",
                launch_time=launch_time,
                max_leverage=10.0,
                min_order_qty=0.001,
                qty_step=0.001,
                turnover24h=10_000_000 + (index * 2_000_000),
                spread_bps=5 + index,
            )
        )
        price = base_prices[symbol]
        for step in range(periods):
            ts = start + timedelta(hours=step)
            shock = rng.uniform(-1.0, 1.0) * vol[symbol]
            drift = trends[symbol]
            price = max(0.0001, price * (1.0 + drift + shock))
            high = price * (1.0 + abs(shock) * 0.75)
            low = price * (1.0 - abs(shock) * 0.75)
            open_price = price / (1.0 + shock * 0.25)
            volume = 1_000.0 + 400.0 * (1.0 + rng.random()) * (1 + index * 0.2)
            bars.append(
                MarketBar(
                    ts=ts,
                    symbol=symbol,
                    open=open_price,
                    high=high,
                    low=low,
                    close=price,
                    volume=volume,
                    turnover=volume * price,
                )
            )
            if step % 8 == 0:
                regime_shift = ((step // 24) % 5) - 2
                funding.append(
                    FundingObservation(
                        ts=ts,
                        symbol=symbol,
                        rate=(0.00005 * regime_shift) + rng.uniform(-0.00003, 0.00003),
                        interval_hours=8,
                    )
                )
        latest_prices[symbol] = price

    option_snapshot_time = start + timedelta(hours=periods - 1)
    for base_coin, symbol in (("BTC", "BTCUSDT"), ("ETH", "ETHUSDT")):
        option_quotes: list[OptionQuote] = []
        price = latest_prices[symbol]
        for strike_offset, option_type in ((-0.1, "Put"), (0.0, "Call"), (0.1, "Call")):
            strike = round(price * (1.0 + strike_offset), 2)
            option_quotes.append(
                OptionQuote(
                    ts=option_snapshot_time,
                    symbol=f"{base_coin}-{(option_snapshot_time + timedelta(days=30)).strftime('%d%b%y').upper()}-{int(strike)}-{option_type[0]}",
                    underlying_symbol=f"{base_coin}USDT",
                    option_type=option_type,
                    strike=strike,
                    expiry=option_snapshot_time + timedelta(days=30),
                    bid=price * 0.015,
                    ask=price * 0.018,
                    mark_price=price * 0.0165,
                    mark_iv=0.45 + strike_offset * 0.2,
                    delta=0.5 if option_type == "Call" else -0.5,
                    gamma=0.02,
                    vega=25.0,
                    theta=-5.0,
                    underlying_price=price,
                    volume24h=40.0,
                    turnover24h=200_000.0,
                )
            )
        portal.add_option_chain(
            OptionChainSnapshot(
                ts=option_snapshot_time,
                base_coin=base_coin,
                quotes=tuple(option_quotes),
                historical_volatility=0.55,
            )
        )
        portal.add_instruments(
            [
                InstrumentMeta(
                    symbol=f"{base_coin}-OPT",
                    category="option",
                    base_coin=base_coin,
                    quote_coin="USDT",
                    launch_time=start - timedelta(days=100),
                    turnover24h=2_000_000.0,
                    spread_bps=12.0,
                )
            ]
        )

    portal.add_instruments(instruments)
    portal.add_bars(bars)
    portal.add_funding(funding)
    return portal
