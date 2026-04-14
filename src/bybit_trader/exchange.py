from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import count
from math import copysign
from typing import Any, Callable

from .config import UniverseConfig
from .data import ResearchWarehouse
from .models import (
    AccountState,
    ExecutionReport,
    InstrumentMeta,
    OptionChainSnapshot,
    OptionQuote,
    OrderIntent,
    Position,
    ProductType,
    ensure_utc,
    safe_float,
)


def _coerce_datetime(raw: Any) -> datetime | None:
    if raw is None:
        return None
    try:
        if isinstance(raw, (int, float, str)) and str(raw).isdigit():
            return datetime.fromtimestamp(int(raw) / 1000.0, tz=UTC)
    except (TypeError, ValueError):
        return None
    return ensure_utc(raw)


class InstrumentRegistry:
    def __init__(self, config: UniverseConfig) -> None:
        self.config = config
        self._instruments: dict[str, InstrumentMeta] = {}
        self._last_refresh: datetime | None = None

    @property
    def last_refresh(self) -> datetime | None:
        return self._last_refresh

    def upsert(self, instruments: Iterable[InstrumentMeta]) -> None:
        for instrument in instruments:
            self._instruments[instrument.symbol] = instrument
        self._last_refresh = datetime.now(UTC)

    def refresh_from_pages(
        self,
        fetch_page: Callable[[str | None], tuple[list[InstrumentMeta], str | None]],
    ) -> None:
        cursor: str | None = None
        combined: list[InstrumentMeta] = []
        while True:
            page, cursor = fetch_page(cursor)
            combined.extend(page)
            if not cursor:
                break
        self.upsert(combined)

    def instruments(self, category: str | None = None) -> list[InstrumentMeta]:
        values = list(self._instruments.values())
        if category is None:
            return values
        return [item for item in values if item.category == category]

    def linear_universe(
        self,
        *,
        now: datetime | None = None,
        extra_turnover: dict[str, float] | None = None,
        extra_spread_bps: dict[str, float] | None = None,
        funding_counts: dict[str, int] | None = None,
    ) -> list[InstrumentMeta]:
        now = ensure_utc(now) or datetime.now(UTC)
        extra_turnover = extra_turnover or {}
        extra_spread_bps = extra_spread_bps or {}
        funding_counts = funding_counts or {}
        selected: list[InstrumentMeta] = []
        explicit = set(self.config.symbols)
        base_filter = set(self.config.base_coins)
        for instrument in self.instruments(ProductType.LINEAR.value):
            turnover = extra_turnover.get(instrument.symbol, instrument.turnover24h or 0.0)
            spread_bps = extra_spread_bps.get(instrument.symbol, instrument.spread_bps or 0.0)
            age_days = instrument.listing_age_days(now)
            if instrument.status != "Trading":
                continue
            if instrument.quote_coin != self.config.quote_coin:
                continue
            if base_filter and instrument.base_coin not in base_filter:
                continue
            if not self.config.dynamic and explicit and instrument.symbol not in explicit:
                continue
            if explicit and instrument.symbol in explicit:
                selected.append(instrument)
                continue
            if turnover < self.config.min_24h_turnover:
                continue
            if spread_bps > self.config.max_spread_bps:
                continue
            if age_days < self.config.min_listing_days:
                continue
            if funding_counts.get(instrument.symbol, 0) < self.config.require_funding_observations:
                continue
            selected.append(instrument)
        deduped = {item.symbol: item for item in selected}
        return sorted(deduped.values(), key=lambda item: item.symbol)


class BrokerAdapter(ABC):
    @abstractmethod
    def submit_orders(
        self,
        orders: Iterable[OrderIntent],
        *,
        market_prices: dict[str, float] | None = None,
        timestamp: datetime | None = None,
    ) -> list[ExecutionReport]:
        raise NotImplementedError

    @abstractmethod
    def amend_orders(self, orders: Iterable[OrderIntent]) -> list[ExecutionReport]:
        raise NotImplementedError

    @abstractmethod
    def cancel_orders(self, symbols: Iterable[str]) -> list[ExecutionReport]:
        raise NotImplementedError

    @abstractmethod
    def stream_private_events(self) -> Iterator[ExecutionReport]:
        raise NotImplementedError

    @abstractmethod
    def reconcile_state(self) -> AccountState:
        raise NotImplementedError


class PaperBroker(BrokerAdapter):
    def __init__(self, initial_cash: float = 100_000.0, fee_bps: float = 5.0, slippage_bps: float = 1.0) -> None:
        self.account_state = AccountState(cash=initial_cash)
        self.fee_bps = fee_bps
        self.slippage_bps = slippage_bps
        self._order_ids = count(1)

    def submit_orders(
        self,
        orders: Iterable[OrderIntent],
        *,
        market_prices: dict[str, float] | None = None,
        timestamp: datetime | None = None,
    ) -> list[ExecutionReport]:
        market_prices = market_prices or {}
        timestamp = ensure_utc(timestamp) or datetime.now(UTC)
        reports: list[ExecutionReport] = []
        for order in orders:
            price = market_prices.get(order.symbol)
            if price is None:
                continue
            slippage_multiplier = 1.0 + copysign(self.slippage_bps / 10_000.0, order.quantity)
            fill_price = order.limit_price or (price * slippage_multiplier)
            notional = abs(order.quantity) * fill_price
            fee = notional * (self.fee_bps / 10_000.0)
            realized_pnl = self._apply_fill(order.symbol, order.quantity, fill_price)
            self.account_state.cash -= (order.quantity * fill_price) + fee
            self.account_state.realized_pnl += realized_pnl - fee
            report = ExecutionReport(
                order_id=f"paper-{next(self._order_ids)}",
                symbol=order.symbol,
                filled_qty=order.quantity,
                avg_price=fill_price,
                fees=fee,
                status="Filled",
                ts=timestamp,
                realized_pnl=realized_pnl,
                metadata={"side": order.side, "category": order.category},
            )
            reports.append(report)
        return reports

    def amend_orders(self, orders: Iterable[OrderIntent]) -> list[ExecutionReport]:
        reports = []
        now = datetime.now(UTC)
        for order in orders:
            reports.append(
                ExecutionReport(
                    order_id=f"paper-amend-{next(self._order_ids)}",
                    symbol=order.symbol,
                    filled_qty=0.0,
                    avg_price=order.limit_price or 0.0,
                    fees=0.0,
                    status="Amended",
                    ts=now,
                )
            )
        return reports

    def cancel_orders(self, symbols: Iterable[str]) -> list[ExecutionReport]:
        now = datetime.now(UTC)
        return [
            ExecutionReport(
                order_id=f"paper-cancel-{next(self._order_ids)}",
                symbol=symbol,
                filled_qty=0.0,
                avg_price=0.0,
                fees=0.0,
                status="Cancelled",
                ts=now,
            )
            for symbol in symbols
        ]

    def stream_private_events(self) -> Iterator[ExecutionReport]:
        return iter(())

    def reconcile_state(self) -> AccountState:
        positions = {
            symbol: Position(
                symbol=position.symbol,
                quantity=position.quantity,
                average_price=position.average_price,
                realized_pnl=position.realized_pnl,
                category=position.category,
            )
            for symbol, position in self.account_state.positions.items()
        }
        return AccountState(
            cash=self.account_state.cash,
            positions=positions,
            open_orders=list(self.account_state.open_orders),
            realized_pnl=self.account_state.realized_pnl,
        )

    def apply_funding(self, symbol: str, rate: float, mark_price: float) -> float:
        position = self.account_state.positions.get(symbol)
        if position is None or position.quantity == 0:
            return 0.0
        payment = position.quantity * mark_price * rate
        self.account_state.cash -= payment
        self.account_state.realized_pnl -= payment
        return payment

    def _apply_fill(self, symbol: str, fill_qty: float, fill_price: float) -> float:
        realized_pnl = 0.0
        current = self.account_state.positions.get(symbol)
        if current is None:
            self.account_state.positions[symbol] = Position(symbol=symbol, quantity=fill_qty, average_price=fill_price)
            return 0.0
        if current.quantity == 0 or (current.quantity > 0 and fill_qty > 0) or (current.quantity < 0 and fill_qty < 0):
            total_qty = current.quantity + fill_qty
            weighted_cost = (current.average_price * abs(current.quantity)) + (fill_price * abs(fill_qty))
            current.quantity = total_qty
            current.average_price = weighted_cost / abs(total_qty)
            return 0.0

        closing_qty = min(abs(current.quantity), abs(fill_qty))
        realized_pnl += closing_qty * (fill_price - current.average_price) * (1 if current.quantity > 0 else -1)
        new_qty = current.quantity + fill_qty
        if abs(new_qty) < 1e-12:
            del self.account_state.positions[symbol]
            return realized_pnl
        if current.quantity * new_qty > 0:
            current.quantity = new_qty
            return realized_pnl
        current.quantity = new_qty
        current.average_price = fill_price
        return realized_pnl


class BybitClient:
    def __init__(self, api_key: str = "", api_secret: str = "", *, testnet: bool = False, demo: bool = False) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.demo = demo
        self._rest = None

    def _session(self):
        if self._rest is None:
            try:
                from pybit.unified_trading import HTTP
            except ImportError as exc:
                raise RuntimeError("pybit is required to use the Bybit client.") from exc
            self._rest = HTTP(
                api_key=self.api_key,
                api_secret=self.api_secret,
                testnet=self.testnet,
                demo=self.demo,
            )
        return self._rest

    def get_linear_instruments(self, cursor: str | None = None, limit: int = 500) -> tuple[list[InstrumentMeta], str | None]:
        payload = self._session().get_instruments_info(category="linear", limit=limit, cursor=cursor)
        result = payload.get("result", {})
        instruments = [self._normalize_instrument(item, category="linear") for item in result.get("list", [])]
        return instruments, result.get("nextPageCursor") or None

    def get_option_instruments(self, base_coin: str = "BTC", cursor: str | None = None, limit: int = 500) -> tuple[list[InstrumentMeta], str | None]:
        payload = self._session().get_instruments_info(category="option", baseCoin=base_coin, limit=limit, cursor=cursor)
        result = payload.get("result", {})
        instruments = [self._normalize_instrument(item, category="option") for item in result.get("list", [])]
        return instruments, result.get("nextPageCursor") or None

    def get_tickers(self, *, category: str, symbol: str | None = None, base_coin: str | None = None) -> list[dict[str, Any]]:
        payload = self._session().get_tickers(category=category, symbol=symbol, baseCoin=base_coin)
        return payload.get("result", {}).get("list", [])

    def get_positions(self, *, category: str, settle_coin: str | None = None) -> list[dict[str, Any]]:
        payload = self._session().get_positions(category=category, settleCoin=settle_coin)
        return payload.get("result", {}).get("list", [])

    def get_open_orders(self, *, category: str, settle_coin: str | None = None) -> list[dict[str, Any]]:
        payload = self._session().get_open_orders(category=category, settleCoin=settle_coin)
        return payload.get("result", {}).get("list", [])

    def get_execution_history(self, *, category: str, symbol: str | None = None) -> list[dict[str, Any]]:
        payload = self._session().get_executions(category=category, symbol=symbol)
        return payload.get("result", {}).get("list", [])

    def get_wallet_balance(self, *, account_type: str = "UNIFIED", coin: str | None = "USDT") -> list[dict[str, Any]]:
        payload = self._session().get_wallet_balance(accountType=account_type, coin=coin)
        return payload.get("result", {}).get("list", [])

    def place_order(self, order: OrderIntent) -> dict[str, Any]:
        return self._session().place_order(
            category=order.category,
            symbol=order.symbol,
            side=order.side,
            orderType=order.order_type,
            qty=abs(order.quantity),
            price=order.limit_price,
            reduceOnly=order.reduce_only,
            **order.metadata,
        )

    def amend_order(self, order: OrderIntent) -> dict[str, Any]:
        payload = dict(order.metadata)
        if order.limit_price is not None:
            payload["price"] = order.limit_price
        payload["qty"] = abs(order.quantity)
        return self._session().amend_order(category=order.category, symbol=order.symbol, **payload)

    def cancel_order(self, symbol: str, category: str = ProductType.LINEAR.value, **kwargs: Any) -> dict[str, Any]:
        return self._session().cancel_order(category=category, symbol=symbol, **kwargs)

    def fetch_option_chain(self, base_coin: str) -> OptionChainSnapshot:
        rows = self.get_tickers(category="option", base_coin=base_coin)
        now = datetime.now(UTC)
        quotes = []
        for row in rows:
            symbol = row.get("symbol", "")
            pieces = symbol.split("-")
            strike = safe_float(pieces[2] if len(pieces) > 2 else row.get("strike"))
            option_type = "Call" if symbol.endswith("-C") else "Put"
            expiry = datetime.strptime(pieces[1], "%d%b%y").replace(tzinfo=UTC) if len(pieces) > 1 else now
            quotes.append(
                OptionQuote(
                    ts=now,
                    symbol=symbol,
                    underlying_symbol=f"{base_coin}USDT",
                    option_type=option_type,
                    strike=strike,
                    expiry=expiry,
                    bid=safe_float(row.get("bid1Price")),
                    ask=safe_float(row.get("ask1Price")),
                    mark_price=safe_float(row.get("markPrice")),
                    mark_iv=safe_float(row.get("markIv")),
                    delta=safe_float(row.get("delta")),
                    gamma=safe_float(row.get("gamma")),
                    vega=safe_float(row.get("vega")),
                    theta=safe_float(row.get("theta")),
                    underlying_price=safe_float(row.get("underlyingPrice")),
                    volume24h=safe_float(row.get("volume24h")),
                    turnover24h=safe_float(row.get("turnover24h")),
                )
            )
        return OptionChainSnapshot(ts=now, base_coin=base_coin, quotes=tuple(quotes))

    def _normalize_instrument(self, item: dict[str, Any], category: str) -> InstrumentMeta:
        price_filter = item.get("priceFilter", {})
        lot_size = item.get("lotSizeFilter", {})
        leverage_filter = item.get("leverageFilter", {})
        return InstrumentMeta(
            symbol=item.get("symbol", ""),
            category=category,
            base_coin=item.get("baseCoin", ""),
            quote_coin=item.get("quoteCoin", "USDT"),
            status=item.get("status", "Trading"),
            launch_time=_coerce_datetime(item.get("launchTime")),
            tick_size=safe_float(price_filter.get("tickSize")),
            qty_step=safe_float(lot_size.get("qtyStep")),
            min_order_qty=safe_float(lot_size.get("minOrderQty")),
            max_leverage=safe_float(leverage_filter.get("maxLeverage")),
            metadata=item,
        )


class BybitBrokerAdapter(BrokerAdapter):
    def __init__(self, client: BybitClient, category: str = ProductType.LINEAR.value) -> None:
        self.client = client
        self.category = category

    def submit_orders(
        self,
        orders: Iterable[OrderIntent],
        *,
        market_prices: dict[str, float] | None = None,
        timestamp: datetime | None = None,
    ) -> list[ExecutionReport]:
        reports: list[ExecutionReport] = []
        ts = ensure_utc(timestamp) or datetime.now(UTC)
        for order in orders:
            response = self.client.place_order(order)
            reports.append(
                ExecutionReport(
                    order_id=response.get("result", {}).get("orderId", "unknown"),
                    symbol=order.symbol,
                    filled_qty=0.0,
                    avg_price=order.limit_price or market_prices.get(order.symbol, 0.0) if market_prices else 0.0,
                    fees=0.0,
                    status=response.get("retMsg", "Submitted"),
                    ts=ts,
                    metadata=response,
                )
            )
        return reports

    def amend_orders(self, orders: Iterable[OrderIntent]) -> list[ExecutionReport]:
        reports = []
        now = datetime.now(UTC)
        for order in orders:
            response = self.client.amend_order(order)
            reports.append(
                ExecutionReport(
                    order_id=response.get("result", {}).get("orderId", "unknown"),
                    symbol=order.symbol,
                    filled_qty=0.0,
                    avg_price=order.limit_price or 0.0,
                    fees=0.0,
                    status=response.get("retMsg", "Amended"),
                    ts=now,
                    metadata=response,
                )
            )
        return reports

    def cancel_orders(self, symbols: Iterable[str]) -> list[ExecutionReport]:
        reports = []
        now = datetime.now(UTC)
        for symbol in symbols:
            response = self.client.cancel_order(symbol, category=self.category)
            reports.append(
                ExecutionReport(
                    order_id=response.get("result", {}).get("orderId", "unknown"),
                    symbol=symbol,
                    filled_qty=0.0,
                    avg_price=0.0,
                    fees=0.0,
                    status=response.get("retMsg", "Cancelled"),
                    ts=now,
                    metadata=response,
                )
            )
        return reports

    def stream_private_events(self) -> Iterator[ExecutionReport]:
        return iter(())

    def reconcile_state(self) -> AccountState:
        positions_payload = self.client.get_positions(category=self.category, settle_coin="USDT")
        positions: dict[str, Position] = {}
        for row in positions_payload:
            quantity = safe_float(row.get("size"))
            if quantity == 0:
                continue
            if row.get("side", "Buy") == "Sell":
                quantity *= -1.0
            positions[row.get("symbol", "")] = Position(
                symbol=row.get("symbol", ""),
                quantity=quantity,
                average_price=safe_float(row.get("avgPrice")),
                realized_pnl=safe_float(row.get("cumRealisedPnl")),
                category=self.category,
            )
        return AccountState(cash=0.0, positions=positions)


@dataclass(slots=True)
class OptionChainRecorder:
    client: BybitClient
    warehouse: ResearchWarehouse
    base_coins: tuple[str, ...] = ("BTC", "ETH")

    def capture_once(self) -> dict[str, OptionChainSnapshot]:
        snapshots: dict[str, OptionChainSnapshot] = {}
        for base_coin in self.base_coins:
            snapshot = self.client.fetch_option_chain(base_coin)
            self.warehouse.record_option_chain(snapshot)
            snapshots[base_coin] = snapshot
        return snapshots
