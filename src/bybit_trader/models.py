from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import Enum
from math import isnan
from typing import Any


def utc_now() -> datetime:
    return datetime.now(UTC)


def ensure_utc(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


class RunMode(str, Enum):
    BACKTEST = "backtest"
    DEMO = "demo"
    LIVE = "live"


class ExecutionMode(str, Enum):
    MANUAL = "manual"
    GATED = "gated"
    AUTO = "auto"


class ProductType(str, Enum):
    LINEAR = "linear"
    INVERSE = "inverse"
    OPTION = "option"
    SPOT = "spot"


@dataclass(slots=True)
class InstrumentMeta:
    symbol: str
    category: str
    base_coin: str
    quote_coin: str
    status: str = "Trading"
    launch_time: datetime | None = None
    tick_size: float | None = None
    qty_step: float | None = None
    min_order_qty: float | None = None
    max_leverage: float | None = None
    turnover24h: float | None = None
    spread_bps: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def listing_age_days(self, now: datetime | None = None) -> float:
        if self.launch_time is None:
            return float("inf")
        now = ensure_utc(now) or utc_now()
        return max(0.0, (now - ensure_utc(self.launch_time)).total_seconds() / 86400.0)


@dataclass(slots=True)
class MarketBar:
    ts: datetime
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    turnover: float = 0.0


@dataclass(slots=True)
class FundingObservation:
    ts: datetime
    symbol: str
    rate: float
    interval_hours: int = 8


@dataclass(slots=True)
class TradeTick:
    ts: datetime
    symbol: str
    price: float
    size: float
    side: str


@dataclass(slots=True)
class OrderBookSnapshot:
    ts: datetime
    symbol: str
    bid_price: float
    ask_price: float
    bid_size: float = 0.0
    ask_size: float = 0.0

    @property
    def mid_price(self) -> float:
        return (self.bid_price + self.ask_price) / 2.0

    @property
    def spread_bps(self) -> float:
        if self.mid_price <= 0:
            return 0.0
        return ((self.ask_price - self.bid_price) / self.mid_price) * 10000.0


@dataclass(slots=True)
class OptionQuote:
    ts: datetime
    symbol: str
    underlying_symbol: str
    option_type: str
    strike: float
    expiry: datetime
    bid: float
    ask: float
    mark_price: float
    mark_iv: float
    delta: float
    gamma: float
    vega: float
    theta: float
    underlying_price: float
    volume24h: float = 0.0
    turnover24h: float = 0.0

    @property
    def mid(self) -> float:
        if self.bid > 0 and self.ask > 0:
            return (self.bid + self.ask) / 2.0
        return self.mark_price


@dataclass(slots=True)
class OptionChainSnapshot:
    ts: datetime
    base_coin: str
    quotes: tuple[OptionQuote, ...]
    historical_volatility: float | None = None


@dataclass(slots=True)
class Signal:
    ts: datetime
    strategy_name: str
    symbol: str
    score: float
    direction: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TargetPosition:
    symbol: str
    quantity: float | None = None
    target_notional: float | None = None
    category: str = ProductType.LINEAR.value
    reason: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class OrderIntent:
    symbol: str
    quantity: float
    category: str = ProductType.LINEAR.value
    order_type: str = "Market"
    limit_price: float | None = None
    reduce_only: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def side(self) -> str:
        return "Buy" if self.quantity > 0 else "Sell"


@dataclass(slots=True)
class ExecutionReport:
    order_id: str
    symbol: str
    filled_qty: float
    avg_price: float
    fees: float
    status: str
    ts: datetime
    realized_pnl: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Position:
    symbol: str
    quantity: float
    average_price: float
    realized_pnl: float = 0.0
    category: str = ProductType.LINEAR.value

    def market_value(self, price: float) -> float:
        return self.quantity * price


@dataclass(slots=True)
class AccountState:
    cash: float = 0.0
    positions: dict[str, Position] = field(default_factory=dict)
    open_orders: list[OrderIntent] = field(default_factory=list)
    realized_pnl: float = 0.0

    def position_qty(self, symbol: str) -> float:
        position = self.positions.get(symbol)
        return 0.0 if position is None else position.quantity

    def mark_to_market(self, prices: dict[str, float]) -> float:
        return self.cash + sum(
            position.market_value(prices.get(symbol, position.average_price))
            for symbol, position in self.positions.items()
        )


@dataclass(slots=True)
class RiskState:
    equity: float
    peak_equity: float
    drawdown: float
    gross_notional: float
    net_notional: float
    leverage: float
    stale_data_symbols: tuple[str, ...] = ()
    rejected_order_count: int = 0
    option_delta: float = 0.0
    option_gamma: float = 0.0
    option_vega: float = 0.0


@dataclass(slots=True)
class StrategyState:
    as_of: datetime
    run_mode: RunMode
    universe: tuple[str, ...]
    bars: dict[str, list[MarketBar]]
    funding: dict[str, list[FundingObservation]]
    option_chains: dict[str, OptionChainSnapshot]
    prices: dict[str, float]
    account_state: AccountState
    risk_state: RiskState
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class EquityPoint:
    ts: datetime
    equity: float
    cash: float
    gross_notional: float
    drawdown: float


@dataclass(slots=True)
class BacktestResult:
    strategy_name: str
    initial_cash: float
    ending_equity: float
    equity_curve: list[EquityPoint]
    trades: list[ExecutionReport]
    funding_events: list[ExecutionReport] = field(default_factory=list)
    final_account: AccountState | None = None
    final_prices: dict[str, float] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def summary(self) -> dict[str, float]:
        total_return = 0.0
        if self.initial_cash:
            total_return = (self.ending_equity / self.initial_cash) - 1.0
        max_drawdown = max((point.drawdown for point in self.equity_curve), default=0.0)
        trade_count = float(len(self.trades))
        avg_trade_pnl = 0.0
        if self.trades:
            avg_trade_pnl = sum(trade.realized_pnl - trade.fees for trade in self.trades) / len(self.trades)
        return {
            "ending_equity": round(self.ending_equity, 2),
            "total_return": round(total_return, 6),
            "max_drawdown": round(max_drawdown, 6),
            "trade_count": trade_count,
            "avg_trade_pnl": round(avg_trade_pnl, 6),
        }

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["summary"] = self.summary()
        return payload


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if isnan(parsed):
        return default
    return parsed
