from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

from .config import RiskLimits
from .models import AccountState, RiskState, TargetPosition, ensure_utc


class RiskEngine:
    def __init__(self, limits: RiskLimits) -> None:
        self.limits = limits
        self.peak_equity = 0.0

    def evaluate(
        self,
        account_state: AccountState,
        prices: dict[str, float],
        *,
        as_of: datetime | None = None,
        stale_symbols: tuple[str, ...] = (),
        option_delta: float = 0.0,
        option_gamma: float = 0.0,
        option_vega: float = 0.0,
    ) -> RiskState:
        _ = ensure_utc(as_of) or datetime.now(UTC)
        gross_notional = sum(abs(position.quantity) * prices.get(symbol, position.average_price) for symbol, position in account_state.positions.items())
        net_notional = sum(position.quantity * prices.get(symbol, position.average_price) for symbol, position in account_state.positions.items())
        equity = account_state.mark_to_market(prices)
        self.peak_equity = max(self.peak_equity, equity)
        peak = max(self.peak_equity, equity, 1.0)
        drawdown = max(0.0, (peak - equity) / peak)
        leverage = 0.0 if equity <= 0 else gross_notional / equity
        return RiskState(
            equity=equity,
            peak_equity=peak,
            drawdown=drawdown,
            gross_notional=gross_notional,
            net_notional=net_notional,
            leverage=leverage,
            stale_data_symbols=stale_symbols,
            option_delta=option_delta,
            option_gamma=option_gamma,
            option_vega=option_vega,
        )

    def filter_targets(self, targets: list[TargetPosition], prices: dict[str, float], state: RiskState) -> list[TargetPosition]:
        if state.drawdown > self.limits.max_drawdown:
            return []
        if state.option_vega > self.limits.option_vega_limit:
            return []

        filtered: list[TargetPosition] = []
        aggregate_notional = 0.0
        for target in targets:
            price = prices.get(target.symbol)
            if not price:
                continue
            capped = target
            if target.target_notional is not None:
                target_notional = max(
                    -self.limits.max_single_symbol_notional,
                    min(self.limits.max_single_symbol_notional, target.target_notional),
                )
                capped = replace(target, target_notional=target_notional)
                aggregate_notional += abs(target_notional)
            elif target.quantity is not None:
                quantity = target.quantity
                target_notional = quantity * price
                if abs(target_notional) > self.limits.max_single_symbol_notional:
                    quantity = self.limits.max_single_symbol_notional / price * (1 if quantity > 0 else -1)
                capped = replace(target, quantity=quantity)
                aggregate_notional += abs(quantity * price)
            filtered.append(capped)
        max_gross_notional = state.equity * self.limits.max_gross_leverage if state.equity > 0 else 0.0
        if aggregate_notional > max_gross_notional and max_gross_notional > 0:
            scale = max_gross_notional / aggregate_notional
            scaled: list[TargetPosition] = []
            for target in filtered:
                if target.target_notional is not None:
                    scaled.append(replace(target, target_notional=target.target_notional * scale))
                elif target.quantity is not None:
                    scaled.append(replace(target, quantity=target.quantity * scale))
            return scaled
        return filtered

    def allow_orders(self, orders, prices: dict[str, float]) -> list:
        approved = []
        for order in orders:
            price = prices.get(order.symbol)
            if price is None:
                continue
            if abs(order.quantity * price) > self.limits.max_order_notional:
                scale = self.limits.max_order_notional / abs(order.quantity * price)
                order.quantity *= scale
            approved.append(order)
        return approved

