from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from statistics import mean, pstdev
from typing import Any

from .models import AccountState, OptionQuote, OrderIntent, StrategyState, TargetPosition


def _returns(closes: list[float]) -> list[float]:
    if len(closes) < 2:
        return []
    return [(current / previous) - 1.0 for previous, current in zip(closes[:-1], closes[1:])]


def _rolling_mean(values: list[float]) -> float:
    return mean(values) if values else 0.0


def _rolling_std(values: list[float]) -> float:
    return pstdev(values) if len(values) > 1 else 0.0


def _atr_fraction(highs: list[float], lows: list[float], closes: list[float]) -> float:
    if len(closes) < 2:
        return 0.0
    ranges = []
    previous_close = closes[0]
    for high, low, close in zip(highs[1:], lows[1:], closes[1:]):
        ranges.append(max(high - low, abs(high - previous_close), abs(low - previous_close)) / max(close, 1e-9))
        previous_close = close
    return _rolling_mean(ranges)


@dataclass(slots=True)
class Strategy(ABC):
    name: str
    params: dict[str, Any] = field(default_factory=dict)
    category: str = "linear"
    min_history: int = 50

    def prepare_features(self, data_portal) -> None:
        return None

    @abstractmethod
    def generate_targets(self, state: StrategyState) -> list[TargetPosition]:
        raise NotImplementedError

    def rebalance(self, targets: list[TargetPosition], broker_state: AccountState, prices: dict[str, float]) -> list[OrderIntent]:
        orders: list[OrderIntent] = []
        for target in targets:
            price = prices.get(target.symbol)
            if not price:
                continue
            target_qty = target.quantity
            if target_qty is None and target.target_notional is not None:
                target_qty = target.target_notional / price
            if target_qty is None:
                continue
            current_qty = broker_state.position_qty(target.symbol)
            delta_qty = target_qty - current_qty
            if abs(delta_qty) * price < 25.0:
                continue
            orders.append(
                OrderIntent(
                    symbol=target.symbol,
                    quantity=delta_qty,
                    category=target.category,
                    metadata={"reason": target.reason, **target.metadata},
                )
            )
        return orders


@dataclass(slots=True)
class PerpTrendFundingStrategy(Strategy):
    name: str = "perp_trend"
    min_history: int = 100

    def generate_targets(self, state: StrategyState) -> list[TargetPosition]:
        lookback_fast = int(self.params.get("lookback_fast", 24))
        lookback_slow = int(self.params.get("lookback_slow", 96))
        vol_lookback = int(self.params.get("vol_lookback", 48))
        funding_cap = float(self.params.get("funding_cap", 0.0008))
        target_vol = float(self.params.get("target_vol", 0.15))
        targets: list[TargetPosition] = []
        risk_budget = max(state.risk_state.equity, 1.0) * target_vol
        for symbol in state.universe:
            bars = state.bars.get(symbol, [])
            if len(bars) < max(lookback_slow, vol_lookback) + 2:
                continue
            closes = [bar.close for bar in bars]
            fast_return = (closes[-1] / closes[-lookback_fast]) - 1.0
            slow_return = (closes[-1] / closes[-lookback_slow]) - 1.0
            realized_vol = _rolling_std(_returns(closes[-vol_lookback:])) * (24**0.5)
            realized_vol = max(realized_vol, 0.01)
            recent_funding = state.funding.get(symbol, [])
            avg_funding = _rolling_mean([item.rate for item in recent_funding[-3:]])
            direction = 0.0
            if fast_return > 0 and slow_return > 0 and avg_funding < funding_cap:
                direction = 1.0
            elif fast_return < 0 and slow_return < 0 and avg_funding > -funding_cap:
                direction = -1.0
            if direction == 0.0:
                targets.append(TargetPosition(symbol=symbol, target_notional=0.0, reason="flat"))
                continue
            notional = min(risk_budget / realized_vol, state.risk_state.equity * 0.75)
            targets.append(
                TargetPosition(
                    symbol=symbol,
                    target_notional=direction * notional,
                    reason="trend_with_funding_filter",
                    metadata={"fast_return": fast_return, "slow_return": slow_return, "funding": avg_funding},
                )
            )
        return targets


@dataclass(slots=True)
class PerpMeanReversionStrategy(Strategy):
    name: str = "perp_mean_reversion"
    min_history: int = 72

    def generate_targets(self, state: StrategyState) -> list[TargetPosition]:
        lookback = int(self.params.get("lookback", 48))
        entry_z = float(self.params.get("entry_z", 2.0))
        exit_z = float(self.params.get("exit_z", 0.6))
        funding_shock = float(self.params.get("funding_shock", 0.001))
        targets: list[TargetPosition] = []
        for symbol in state.universe:
            bars = state.bars.get(symbol, [])
            if len(bars) < lookback + 2:
                continue
            closes = [bar.close for bar in bars[-lookback:]]
            avg = _rolling_mean(closes)
            deviation = _rolling_std(closes)
            if deviation <= 0:
                continue
            z_score = (closes[-1] - avg) / deviation
            last_return = (closes[-1] / closes[-2]) - 1.0
            funding = _rolling_mean([item.rate for item in state.funding.get(symbol, [])[-2:]])
            current_qty = state.account_state.position_qty(symbol)
            if abs(z_score) < exit_z:
                targets.append(TargetPosition(symbol=symbol, target_notional=0.0, reason="mean_reversion_exit"))
                continue
            if z_score >= entry_z and funding >= funding_shock and last_return < 0:
                target = -0.2 * state.risk_state.equity
            elif z_score <= -entry_z and funding <= -funding_shock and last_return > 0:
                target = 0.2 * state.risk_state.equity
            else:
                target = current_qty * state.prices.get(symbol, 0.0)
            targets.append(
                TargetPosition(
                    symbol=symbol,
                    target_notional=target,
                    reason="leverage_flush_reversion",
                    metadata={"z_score": z_score, "funding": funding},
                )
            )
        return targets


@dataclass(slots=True)
class CrossSectionalCarryBasketStrategy(Strategy):
    name: str = "carry_basket"
    min_history: int = 16

    def generate_targets(self, state: StrategyState) -> list[TargetPosition]:
        lookback = int(self.params.get("lookback", 10))
        basket_size = int(self.params.get("basket_size", 2))
        gross_notional = float(self.params.get("gross_notional", max(state.risk_state.equity * 0.6, 0.0)))
        scored = []
        for symbol in state.universe:
            funding = state.funding.get(symbol, [])
            if len(funding) < lookback:
                continue
            avg_funding = _rolling_mean([item.rate for item in funding[-lookback:]])
            scored.append((symbol, avg_funding))
        if len(scored) < basket_size * 2:
            return []
        scored.sort(key=lambda item: item[1])
        long_bucket = scored[:basket_size]
        short_bucket = scored[-basket_size:]
        leg_notional = gross_notional / max(basket_size * 2, 1)
        targets = []
        for symbol, score in long_bucket:
            targets.append(TargetPosition(symbol=symbol, target_notional=leg_notional, reason="carry_long", metadata={"funding": score}))
        for symbol, score in short_bucket:
            targets.append(TargetPosition(symbol=symbol, target_notional=-leg_notional, reason="carry_short", metadata={"funding": score}))
        return targets


@dataclass(slots=True)
class CrossSectionalMomentumRotationStrategy(Strategy):
    name: str = "momentum_rotation"
    min_history: int = 180

    def generate_targets(self, state: StrategyState) -> list[TargetPosition]:
        short_horizon = int(self.params.get("short_horizon", 24))
        medium_horizon = int(self.params.get("medium_horizon", 72))
        long_horizon = int(self.params.get("long_horizon", 168))
        top_n = int(self.params.get("top_n", 2))
        bottom_n = int(self.params.get("bottom_n", 2))
        gross_notional = float(self.params.get("gross_notional", max(state.risk_state.equity * 0.75, 0.0)))
        ranked: list[tuple[str, float, float]] = []
        for symbol in state.universe:
            bars = state.bars.get(symbol, [])
            if len(bars) <= long_horizon:
                continue
            closes = [bar.close for bar in bars]
            score = (
                0.5 * ((closes[-1] / closes[-short_horizon]) - 1.0)
                + 0.3 * ((closes[-1] / closes[-medium_horizon]) - 1.0)
                + 0.2 * ((closes[-1] / closes[-long_horizon]) - 1.0)
            )
            vol = _rolling_std(_returns(closes[-medium_horizon:])) or 0.01
            ranked.append((symbol, score / vol, vol))
        if len(ranked) < top_n + bottom_n:
            return []
        ranked.sort(key=lambda item: item[1])
        long_bucket = ranked[-top_n:]
        short_bucket = ranked[:bottom_n]
        denominator = max(top_n + bottom_n, 1)
        base_notional = gross_notional / denominator
        targets: list[TargetPosition] = []
        for symbol, score, vol in long_bucket:
            targets.append(TargetPosition(symbol=symbol, target_notional=base_notional / max(vol, 0.01), reason="momentum_long", metadata={"score": score}))
        for symbol, score, vol in short_bucket:
            targets.append(TargetPosition(symbol=symbol, target_notional=-(base_notional / max(vol, 0.01)), reason="momentum_short", metadata={"score": score}))
        return targets


@dataclass(slots=True)
class VolatilityCompressionBreakoutStrategy(Strategy):
    name: str = "volatility_breakout"
    min_history: int = 48

    def generate_targets(self, state: StrategyState) -> list[TargetPosition]:
        atr_lookback = int(self.params.get("atr_lookback", 20))
        compression_window = int(self.params.get("compression_window", 12))
        breakout_window = int(self.params.get("breakout_window", 24))
        volume_multiple = float(self.params.get("volume_multiple", 1.3))
        targets: list[TargetPosition] = []
        for symbol in state.universe:
            bars = state.bars.get(symbol, [])
            if len(bars) < max(atr_lookback, breakout_window) + 2:
                continue
            recent = bars[-max(atr_lookback, breakout_window):]
            highs = [bar.high for bar in recent]
            lows = [bar.low for bar in recent]
            closes = [bar.close for bar in recent]
            volumes = [bar.volume for bar in recent]
            atr_now = _atr_fraction(highs[-atr_lookback:], lows[-atr_lookback:], closes[-atr_lookback:])
            atr_comp = _atr_fraction(highs[-compression_window:], lows[-compression_window:], closes[-compression_window:])
            highest = max(closes[-breakout_window:-1])
            lowest = min(closes[-breakout_window:-1])
            volume_ratio = volumes[-1] / max(_rolling_mean(volumes[-compression_window:-1]), 1e-9)
            target_notional = 0.0
            if atr_comp < atr_now * 0.75 and volume_ratio > volume_multiple and closes[-1] > highest:
                target_notional = 0.25 * state.risk_state.equity
            elif atr_comp < atr_now * 0.75 and volume_ratio > volume_multiple and closes[-1] < lowest:
                target_notional = -0.25 * state.risk_state.equity
            targets.append(
                TargetPosition(
                    symbol=symbol,
                    target_notional=target_notional,
                    reason="volatility_breakout",
                    metadata={"atr_now": atr_now, "atr_comp": atr_comp, "volume_ratio": volume_ratio},
                )
            )
        return targets


@dataclass(slots=True)
class OptionIVHVLongGammaStrategy(Strategy):
    name: str = "option_iv_hv_long_gamma"
    category: str = "option"
    min_history: int = 1

    def generate_targets(self, state: StrategyState) -> list[TargetPosition]:
        base_coins = self.params.get("base_coins", ["BTC", "ETH"])
        iv_discount = float(self.params.get("iv_discount", 0.85))
        targets: list[TargetPosition] = []
        for base_coin in base_coins:
            chain = state.option_chains.get(base_coin)
            if chain is None or chain.historical_volatility is None:
                continue
            hv = chain.historical_volatility
            near_atm = sorted(chain.quotes, key=lambda quote: abs(quote.strike - quote.underlying_price))[:2]
            for quote in near_atm:
                if quote.mark_iv < hv * iv_discount:
                    targets.append(
                        TargetPosition(
                            symbol=quote.symbol,
                            quantity=1.0,
                            category="option",
                            reason="iv_below_hv",
                            metadata={"base_coin": base_coin, "mark_iv": quote.mark_iv, "hv": hv},
                        )
                    )
        return targets


@dataclass(slots=True)
class OptionRichPremiumFadeStrategy(Strategy):
    name: str = "option_premium_fade"
    category: str = "option"
    min_history: int = 1

    def generate_targets(self, state: StrategyState) -> list[TargetPosition]:
        targets: list[TargetPosition] = []
        for base_coin, chain in state.option_chains.items():
            if chain.historical_volatility is None:
                continue
            rich_quotes = [quote for quote in chain.quotes if quote.mark_iv > chain.historical_volatility * 1.2]
            if not rich_quotes:
                continue
            rich_quotes.sort(key=lambda item: item.mark_iv, reverse=True)
            short_leg = rich_quotes[0]
            hedge_candidates = sorted(chain.quotes, key=lambda item: abs(item.strike - short_leg.strike))
            if len(hedge_candidates) < 2:
                continue
            hedge_leg = hedge_candidates[1]
            targets.append(TargetPosition(symbol=short_leg.symbol, quantity=-1.0, category="option", reason="rich_short_gamma", metadata={"base_coin": base_coin}))
            targets.append(TargetPosition(symbol=hedge_leg.symbol, quantity=1.0, category="option", reason="defined_risk_hedge", metadata={"base_coin": base_coin}))
        return targets


@dataclass(slots=True)
class ProtectiveOptionOverlayStrategy(Strategy):
    name: str = "protective_option_overlay"
    category: str = "mixed"
    min_history: int = 1

    def generate_targets(self, state: StrategyState) -> list[TargetPosition]:
        targets: list[TargetPosition] = []
        perp_exposure = {symbol: position for symbol, position in state.account_state.positions.items() if symbol.endswith("USDT") and position.quantity > 0}
        for symbol, position in perp_exposure.items():
            base_coin = symbol.replace("USDT", "")
            chain = state.option_chains.get(base_coin)
            if chain is None:
                continue
            puts = [quote for quote in chain.quotes if quote.option_type == "Put" and quote.strike < quote.underlying_price]
            if not puts:
                continue
            selected = sorted(puts, key=lambda quote: abs(quote.strike - quote.underlying_price * 0.9))[0]
            hedge_size = max(1.0, abs(position.quantity) * 0.1)
            targets.append(
                TargetPosition(
                    symbol=selected.symbol,
                    quantity=hedge_size,
                    category="option",
                    reason="protective_overlay",
                    metadata={"underlying": symbol, "hedge_ratio": 0.1},
                )
            )
        return targets


STRATEGY_LIBRARY = {
    "perp_trend": PerpTrendFundingStrategy,
    "perp_mean_reversion": PerpMeanReversionStrategy,
    "carry_basket": CrossSectionalCarryBasketStrategy,
    "momentum_rotation": CrossSectionalMomentumRotationStrategy,
    "volatility_breakout": VolatilityCompressionBreakoutStrategy,
    "option_iv_hv_long_gamma": OptionIVHVLongGammaStrategy,
    "option_premium_fade": OptionRichPremiumFadeStrategy,
    "protective_option_overlay": ProtectiveOptionOverlayStrategy,
}


def build_strategy(name: str, params: dict[str, Any] | None = None) -> Strategy:
    params = params or {}
    strategy_class = STRATEGY_LIBRARY.get(name)
    if strategy_class is None:
        raise KeyError(f"Unknown strategy: {name}")
    return strategy_class(params=params)

