from __future__ import annotations

from dataclasses import dataclass, field
from statistics import mean
from typing import Any

from .models import BacktestResult, OptionChainSnapshot, Position


def _position_unrealized(position: Position, final_price: float) -> float:
    return position.quantity * (final_price - position.average_price)


@dataclass(slots=True)
class BacktestAnalyticsReport:
    strategy_name: str
    universe: tuple[str, ...]
    ending_equity: float
    total_return: float
    max_drawdown: float
    trade_count: int
    average_trade_pnl: float
    win_rate: float
    gross_turnover: float
    turnover_ratio: float
    average_gross_exposure: float
    funding_contribution: float
    best_symbol: str | None
    best_symbol_pnl: float
    worst_symbol: str | None
    worst_symbol_pnl: float
    symbol_contributions: dict[str, float] = field(default_factory=dict)
    notes: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "strategy_name": self.strategy_name,
            "universe": list(self.universe),
            "ending_equity": round(self.ending_equity, 2),
            "total_return": round(self.total_return, 6),
            "max_drawdown": round(self.max_drawdown, 6),
            "trade_count": self.trade_count,
            "average_trade_pnl": round(self.average_trade_pnl, 6),
            "win_rate": round(self.win_rate, 6),
            "gross_turnover": round(self.gross_turnover, 2),
            "turnover_ratio": round(self.turnover_ratio, 6),
            "average_gross_exposure": round(self.average_gross_exposure, 2),
            "funding_contribution": round(self.funding_contribution, 6),
            "best_symbol": self.best_symbol,
            "best_symbol_pnl": round(self.best_symbol_pnl, 6),
            "worst_symbol": self.worst_symbol,
            "worst_symbol_pnl": round(self.worst_symbol_pnl, 6),
            "symbol_contributions": {symbol: round(value, 6) for symbol, value in sorted(self.symbol_contributions.items())},
            "notes": self.notes,
        }

    def render_summary(self) -> str:
        payload = self.as_dict()
        lines = [
            f"{payload['strategy_name']} on {', '.join(payload['universe'])}",
            f"ending_equity={payload['ending_equity']:.2f} total_return={payload['total_return']:.4%} max_drawdown={payload['max_drawdown']:.4%}",
            f"trade_count={payload['trade_count']} win_rate={payload['win_rate']:.2%} avg_trade_pnl={payload['average_trade_pnl']:.2f}",
            f"gross_turnover={payload['gross_turnover']:.2f} turnover_ratio={payload['turnover_ratio']:.4f} avg_gross_exposure={payload['average_gross_exposure']:.2f}",
            f"funding_contribution={payload['funding_contribution']:.2f} best_symbol={payload['best_symbol']}({payload['best_symbol_pnl']:.2f}) worst_symbol={payload['worst_symbol']}({payload['worst_symbol_pnl']:.2f})",
        ]
        if self.notes:
            lines.append(self.notes)
        return "\n".join(lines)


@dataclass(slots=True)
class OptionResearchSummary:
    base_coin: str
    snapshot_ts: str
    quote_count: int
    liquid_quote_count: int
    historical_volatility: float | None
    atm_mark_iv: float | None
    iv_to_hv_ratio: float | None
    cheapest_iv_discount_ratio: float | None
    richest_iv_premium_ratio: float | None
    research_only: bool = True
    notes: str = "Research-only snapshot. Native option history is still too shallow for a full fill-accurate backtest."

    def as_dict(self) -> dict[str, Any]:
        return {
            "base_coin": self.base_coin,
            "snapshot_ts": self.snapshot_ts,
            "quote_count": self.quote_count,
            "liquid_quote_count": self.liquid_quote_count,
            "historical_volatility": None if self.historical_volatility is None else round(self.historical_volatility, 6),
            "atm_mark_iv": None if self.atm_mark_iv is None else round(self.atm_mark_iv, 6),
            "iv_to_hv_ratio": None if self.iv_to_hv_ratio is None else round(self.iv_to_hv_ratio, 6),
            "cheapest_iv_discount_ratio": None
            if self.cheapest_iv_discount_ratio is None
            else round(self.cheapest_iv_discount_ratio, 6),
            "richest_iv_premium_ratio": None
            if self.richest_iv_premium_ratio is None
            else round(self.richest_iv_premium_ratio, 6),
            "research_only": self.research_only,
            "notes": self.notes,
        }


def build_backtest_analytics(result: BacktestResult) -> BacktestAnalyticsReport:
    summary = result.summary()
    pnl_after_fees = [(trade.realized_pnl - trade.fees) for trade in result.trades]
    winning_trades = [value for value in pnl_after_fees if value > 0]
    trade_count = len(result.trades)
    win_rate = len(winning_trades) / trade_count if trade_count else 0.0
    gross_turnover = sum(abs(trade.filled_qty * trade.avg_price) for trade in result.trades)
    turnover_ratio = gross_turnover / result.initial_cash if result.initial_cash else 0.0
    average_gross_exposure = mean(point.gross_notional for point in result.equity_curve) if result.equity_curve else 0.0
    funding_contribution = sum(event.realized_pnl for event in result.funding_events)

    symbol_contributions: dict[str, float] = {}
    for trade in result.trades:
        symbol_contributions[trade.symbol] = symbol_contributions.get(trade.symbol, 0.0) + (trade.realized_pnl - trade.fees)
    for funding_event in result.funding_events:
        symbol_contributions[funding_event.symbol] = symbol_contributions.get(funding_event.symbol, 0.0) + funding_event.realized_pnl
    if result.final_account is not None:
        for symbol, position in result.final_account.positions.items():
            final_price = result.final_prices.get(symbol, position.average_price)
            symbol_contributions[symbol] = symbol_contributions.get(symbol, 0.0) + _position_unrealized(position, final_price)

    best_symbol = None
    best_symbol_pnl = 0.0
    worst_symbol = None
    worst_symbol_pnl = 0.0
    if symbol_contributions:
        best_symbol, best_symbol_pnl = max(symbol_contributions.items(), key=lambda item: item[1])
        worst_symbol, worst_symbol_pnl = min(symbol_contributions.items(), key=lambda item: item[1])

    return BacktestAnalyticsReport(
        strategy_name=result.strategy_name,
        universe=tuple(result.metadata.get("symbols", [])),
        ending_equity=summary["ending_equity"],
        total_return=summary["total_return"],
        max_drawdown=summary["max_drawdown"],
        trade_count=trade_count,
        average_trade_pnl=summary["avg_trade_pnl"],
        win_rate=win_rate,
        gross_turnover=gross_turnover,
        turnover_ratio=turnover_ratio,
        average_gross_exposure=average_gross_exposure,
        funding_contribution=funding_contribution,
        best_symbol=best_symbol,
        best_symbol_pnl=best_symbol_pnl,
        worst_symbol=worst_symbol,
        worst_symbol_pnl=worst_symbol_pnl,
        symbol_contributions=symbol_contributions,
    )


def build_option_research_summary(snapshot: OptionChainSnapshot) -> OptionResearchSummary:
    quotes = list(snapshot.quotes)
    liquid_quotes = [quote for quote in quotes if quote.bid > 0 and quote.ask > 0]
    atm_quote = min(liquid_quotes or quotes, key=lambda quote: abs(quote.strike - quote.underlying_price), default=None)
    historical_volatility = snapshot.historical_volatility
    atm_mark_iv = None if atm_quote is None else atm_quote.mark_iv
    iv_to_hv_ratio = None
    cheapest_iv_discount_ratio = None
    richest_iv_premium_ratio = None
    if historical_volatility and historical_volatility > 0:
        if atm_mark_iv is not None:
            iv_to_hv_ratio = atm_mark_iv / historical_volatility
        ratios = [quote.mark_iv / historical_volatility for quote in liquid_quotes if quote.mark_iv > 0]
        if ratios:
            cheapest_iv_discount_ratio = min(ratios)
            richest_iv_premium_ratio = max(ratios)
    return OptionResearchSummary(
        base_coin=snapshot.base_coin,
        snapshot_ts=snapshot.ts.isoformat(),
        quote_count=len(quotes),
        liquid_quote_count=len(liquid_quotes),
        historical_volatility=historical_volatility,
        atm_mark_iv=atm_mark_iv,
        iv_to_hv_ratio=iv_to_hv_ratio,
        cheapest_iv_discount_ratio=cheapest_iv_discount_ratio,
        richest_iv_premium_ratio=richest_iv_premium_ratio,
    )
