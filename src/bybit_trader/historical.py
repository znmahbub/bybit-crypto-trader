from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime, timedelta
import json
from math import sqrt
import ssl
from statistics import pstdev
from typing import Any, Callable
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from .data import InMemoryDataPortal, ResearchWarehouse
from .models import FundingObservation, InstrumentMeta, MarketBar, OptionChainSnapshot, OptionQuote, ensure_utc, safe_float

PUBLIC_REST_BASE = "https://api.bybit.com"


def _public_get_json(path: str, params: dict[str, Any]) -> dict[str, Any]:
    query = urlencode({key: value for key, value in params.items() if value is not None})
    url = f"{PUBLIC_REST_BASE}{path}?{query}"
    try:
        with urlopen(url, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))
    except URLError as exc:
        reason = getattr(exc, "reason", None)
        cert_error = isinstance(reason, ssl.SSLCertVerificationError) or "CERTIFICATE_VERIFY_FAILED" in str(exc)
        if not cert_error:
            raise
        insecure_context = ssl._create_unverified_context()
        with urlopen(url, timeout=20, context=insecure_context) as response:
            return json.loads(response.read().decode("utf-8"))


def _require_ret_code_zero(payload: dict[str, Any], label: str) -> dict[str, Any]:
    if payload.get("retCode") != 0:
        raise RuntimeError(f"{label} failed with retCode={payload.get('retCode')} retMsg={payload.get('retMsg')}")
    return payload


def _parse_expiry(raw: str, fallback: datetime) -> datetime:
    try:
        return datetime.strptime(raw, "%d%b%y").replace(tzinfo=UTC)
    except ValueError:
        return fallback


def _returns(closes: list[float]) -> list[float]:
    if len(closes) < 2:
        return []
    return [(current / previous) - 1.0 for previous, current in zip(closes[:-1], closes[1:]) if previous > 0]


def _realized_volatility_from_bars(bars: list[MarketBar]) -> float | None:
    returns = _returns([bar.close for bar in bars])
    if len(returns) < 24:
        return None
    return pstdev(returns) * sqrt(24 * 365)


class HistoricalMarketFetcher:
    def __init__(
        self,
        *,
        warehouse: ResearchWarehouse | None = None,
        public_get_json: Callable[[str, dict[str, Any]], dict[str, Any]] | None = None,
    ) -> None:
        self.warehouse = warehouse or ResearchWarehouse()
        self.public_get_json = public_get_json or _public_get_json

    def fetch_linear_instruments(self, *, limit: int = 500) -> list[InstrumentMeta]:
        cursor: str | None = None
        instruments: dict[str, InstrumentMeta] = {}
        while True:
            payload = _require_ret_code_zero(
                self.public_get_json("/v5/market/instruments-info", {"category": "linear", "limit": limit, "cursor": cursor}),
                "Linear instruments",
            )
            result = payload.get("result", {})
            rows = result.get("list", [])
            self.warehouse.append_raw("linear_instruments_raw", {"cursor": cursor, "rows": rows})
            for row in rows:
                instrument = self._normalize_instrument(row, category="linear")
                instruments[instrument.symbol] = instrument
            cursor = result.get("nextPageCursor") or None
            if not cursor:
                break
        self.warehouse.append_normalized(
            "linear_instruments",
            [
                {
                    "symbol": instrument.symbol,
                    "category": instrument.category,
                    "base_coin": instrument.base_coin,
                    "quote_coin": instrument.quote_coin,
                    "status": instrument.status,
                    "launch_time": instrument.launch_time.isoformat() if instrument.launch_time else None,
                    "tick_size": instrument.tick_size,
                    "qty_step": instrument.qty_step,
                    "min_order_qty": instrument.min_order_qty,
                    "max_leverage": instrument.max_leverage,
                }
                for instrument in instruments.values()
            ],
        )
        return sorted(instruments.values(), key=lambda item: item.symbol)

    def fetch_linear_tickers(self) -> list[dict[str, Any]]:
        payload = _require_ret_code_zero(self.public_get_json("/v5/market/tickers", {"category": "linear"}), "Linear tickers")
        rows = payload.get("result", {}).get("list", [])
        self.warehouse.append_raw("linear_tickers_raw", {"count": len(rows)})
        return rows

    def discover_analytics_universe(
        self,
        *,
        default_symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT"),
        extra_symbols: int = 1,
        min_history_days: int = 180,
        as_of: datetime | None = None,
    ) -> list[str]:
        as_of = ensure_utc(as_of) or datetime.now(UTC)
        instruments = self.fetch_linear_instruments()
        tickers = {row.get("symbol", ""): row for row in self.fetch_linear_tickers()}
        selected = list(default_symbols)
        excluded = set(default_symbols)
        candidates: list[tuple[float, str]] = []
        for instrument in instruments:
            if instrument.symbol in excluded or instrument.status != "Trading" or instrument.quote_coin != "USDT":
                continue
            if instrument.listing_age_days(as_of) < float(min_history_days):
                continue
            ticker = tickers.get(instrument.symbol, {})
            turnover = safe_float(ticker.get("turnover24h"), default=instrument.turnover24h or 0.0)
            if turnover <= 0:
                continue
            candidates.append((turnover, instrument.symbol))
        candidates.sort(reverse=True)
        for _, symbol in candidates[:extra_symbols]:
            if symbol not in selected:
                selected.append(symbol)
        return selected

    def fetch_linear_klines(
        self,
        symbol: str,
        *,
        interval_minutes: int = 60,
        lookback_days: int = 180,
        end_time: datetime | None = None,
        limit: int = 1000,
    ) -> list[MarketBar]:
        end_time = ensure_utc(end_time) or datetime.now(UTC)
        start_time = end_time - timedelta(days=lookback_days)
        interval_ms = interval_minutes * 60 * 1000
        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)
        cursor_end_ms = end_ms
        deduped: dict[int, MarketBar] = {}

        while cursor_end_ms >= start_ms:
            payload = _require_ret_code_zero(
                self.public_get_json(
                    "/v5/market/kline",
                    {
                        "category": "linear",
                        "symbol": symbol,
                        "interval": str(interval_minutes),
                        "start": start_ms,
                        "end": cursor_end_ms,
                        "limit": limit,
                    },
                ),
                f"Kline {symbol}",
            )
            rows = payload.get("result", {}).get("list", [])
            self.warehouse.append_raw(f"kline_{symbol}_raw", {"symbol": symbol, "start": start_ms, "end": cursor_end_ms, "rows": rows})
            if not rows:
                break
            page_min_ts = cursor_end_ms
            for row in rows:
                ts_ms = int(row[0])
                page_min_ts = min(page_min_ts, ts_ms)
                deduped[ts_ms] = MarketBar(
                    ts=datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC),
                    symbol=symbol,
                    open=safe_float(row[1]),
                    high=safe_float(row[2]),
                    low=safe_float(row[3]),
                    close=safe_float(row[4]),
                    volume=safe_float(row[5]),
                    turnover=safe_float(row[6]) if len(row) > 6 else safe_float(row[5]) * safe_float(row[4]),
                )
            next_cursor = page_min_ts - interval_ms
            if next_cursor >= cursor_end_ms:
                break
            cursor_end_ms = next_cursor

        bars = [deduped[key] for key in sorted(deduped)]
        self.warehouse.append_normalized(
            f"kline_{symbol}",
            [
                {
                    "ts": bar.ts.isoformat(),
                    "symbol": bar.symbol,
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                    "turnover": bar.turnover,
                }
                for bar in bars
            ],
        )
        return bars

    def fetch_funding_history(
        self,
        symbol: str,
        *,
        lookback_days: int = 180,
        end_time: datetime | None = None,
        limit: int = 200,
    ) -> list[FundingObservation]:
        end_time = ensure_utc(end_time) or datetime.now(UTC)
        start_time = end_time - timedelta(days=lookback_days)
        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)
        cursor_end_ms = end_ms
        deduped: dict[int, FundingObservation] = {}

        while cursor_end_ms >= start_ms:
            payload = _require_ret_code_zero(
                self.public_get_json(
                    "/v5/market/funding/history",
                    {
                        "category": "linear",
                        "symbol": symbol,
                        "startTime": start_ms,
                        "endTime": cursor_end_ms,
                        "limit": limit,
                    },
                ),
                f"Funding history {symbol}",
            )
            rows = payload.get("result", {}).get("list", [])
            self.warehouse.append_raw(f"funding_{symbol}_raw", {"symbol": symbol, "start": start_ms, "end": cursor_end_ms, "rows": rows})
            if not rows:
                break
            page_min_ts = cursor_end_ms
            for row in rows:
                ts_ms = int(row.get("fundingRateTimestamp", "0"))
                page_min_ts = min(page_min_ts, ts_ms)
                deduped[ts_ms] = FundingObservation(
                    ts=datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC),
                    symbol=row.get("symbol", symbol),
                    rate=safe_float(row.get("fundingRate")),
                )
            next_cursor = page_min_ts - 1
            if next_cursor >= cursor_end_ms:
                break
            cursor_end_ms = next_cursor

        funding = [deduped[key] for key in sorted(deduped)]
        self.warehouse.append_normalized(
            f"funding_{symbol}",
            [{"ts": item.ts.isoformat(), "symbol": item.symbol, "rate": item.rate, "interval_hours": item.interval_hours} for item in funding],
        )
        return funding

    def fetch_option_chain(self, base_coin: str, *, underlying_bars: list[MarketBar] | None = None) -> OptionChainSnapshot:
        now = datetime.now(UTC)
        payload = _require_ret_code_zero(
            self.public_get_json("/v5/market/tickers", {"category": "option", "baseCoin": base_coin}),
            f"Option tickers {base_coin}",
        )
        rows = payload.get("result", {}).get("list", [])
        quotes: list[OptionQuote] = []
        for row in rows:
            symbol = row.get("symbol", "")
            pieces = symbol.split("-")
            expiry = _parse_expiry(pieces[1], now) if len(pieces) > 1 else now
            option_flag = pieces[3] if len(pieces) > 3 else ("C" if "-C-" in symbol else "P")
            option_type = "Call" if option_flag == "C" else "Put"
            quotes.append(
                OptionQuote(
                    ts=now,
                    symbol=symbol,
                    underlying_symbol=f"{base_coin}USDT",
                    option_type=option_type,
                    strike=safe_float(pieces[2] if len(pieces) > 2 else row.get("strike")),
                    expiry=expiry,
                    bid=safe_float(row.get("bid1Price")),
                    ask=safe_float(row.get("ask1Price")),
                    mark_price=safe_float(row.get("markPrice")),
                    mark_iv=safe_float(row.get("markIv")),
                    delta=safe_float(row.get("delta")),
                    gamma=safe_float(row.get("gamma")),
                    vega=safe_float(row.get("vega")),
                    theta=safe_float(row.get("theta")),
                    underlying_price=safe_float(row.get("underlyingPrice") or row.get("indexPrice")),
                    volume24h=safe_float(row.get("volume24h")),
                    turnover24h=safe_float(row.get("turnover24h")),
                )
            )
        snapshot = OptionChainSnapshot(
            ts=now,
            base_coin=base_coin,
            quotes=tuple(quotes),
            historical_volatility=self.fetch_historical_volatility(base_coin) or _realized_volatility_from_bars(underlying_bars or []),
        )
        self.warehouse.record_option_chain(snapshot)
        return snapshot

    def fetch_historical_volatility(self, base_coin: str, *, period: int = 7) -> float | None:
        payload = _require_ret_code_zero(
            self.public_get_json("/v5/market/historical-volatility", {"category": "option", "baseCoin": base_coin, "period": period}),
            f"Historical volatility {base_coin}",
        )
        result = payload.get("result", [])
        if isinstance(result, dict):
            rows = result.get("list", [])
        else:
            rows = result
        if not rows:
            return None
        latest = rows[-1]
        if isinstance(latest, dict):
            for key in ("value", "historicalVolatility", "volatility", "hv"):
                value = safe_float(latest.get(key), default=float("nan"))
                if value == value and value > 0:
                    return value
        if isinstance(latest, (list, tuple)) and latest:
            for item in reversed(latest):
                value = safe_float(item, default=float("nan"))
                if value == value and value > 0:
                    return value
        return None

    def build_research_portal(
        self,
        *,
        symbols: list[str] | None = None,
        lookback_days: int = 180,
        interval_minutes: int = 60,
        include_option_snapshots: bool = True,
        end_time: datetime | None = None,
    ) -> tuple[InMemoryDataPortal, dict[str, Any]]:
        portal = InMemoryDataPortal()
        instruments = self.fetch_linear_instruments()
        portal.add_instruments(instruments)

        selected_symbols = symbols or self.discover_analytics_universe(min_history_days=lookback_days, as_of=end_time)
        imported = {"symbols": selected_symbols, "bar_counts": {}, "funding_counts": {}}
        underlying_bar_map: dict[str, list[MarketBar]] = {}
        for symbol in selected_symbols:
            bars = self.fetch_linear_klines(symbol, interval_minutes=interval_minutes, lookback_days=lookback_days, end_time=end_time)
            funding = self.fetch_funding_history(symbol, lookback_days=lookback_days, end_time=end_time)
            portal.add_bars(bars)
            portal.add_funding(funding)
            imported["bar_counts"][symbol] = len(bars)
            imported["funding_counts"][symbol] = len(funding)
            underlying_bar_map[symbol] = bars

        if include_option_snapshots:
            option_instruments = [
                InstrumentMeta(symbol=f"{base_coin}-OPTIONS", category="option", base_coin=base_coin, quote_coin="USDT")
                for base_coin in ("BTC", "ETH")
            ]
            portal.add_instruments(option_instruments)
            for base_coin in ("BTC", "ETH"):
                portal.add_option_chain(self.fetch_option_chain(base_coin, underlying_bars=underlying_bar_map.get(f"{base_coin}USDT", [])))

        return portal, imported

    def _normalize_instrument(self, item: dict[str, Any], *, category: str) -> InstrumentMeta:
        price_filter = item.get("priceFilter", {})
        lot_size_filter = item.get("lotSizeFilter", {})
        leverage_filter = item.get("leverageFilter", {})
        launch_time_raw = item.get("launchTime")
        launch_time = None
        if launch_time_raw:
            launch_time = datetime.fromtimestamp(int(launch_time_raw) / 1000.0, tz=UTC)
        return InstrumentMeta(
            symbol=item.get("symbol", ""),
            category=category,
            base_coin=item.get("baseCoin", ""),
            quote_coin=item.get("quoteCoin", "USDT"),
            status=item.get("status", "Trading"),
            launch_time=launch_time,
            tick_size=safe_float(price_filter.get("tickSize")),
            qty_step=safe_float(lot_size_filter.get("qtyStep")),
            min_order_qty=safe_float(lot_size_filter.get("minOrderQty")),
            max_leverage=safe_float(leverage_filter.get("maxLeverage")),
            turnover24h=safe_float(item.get("turnover24h")),
            spread_bps=None,
            metadata=item,
        )
