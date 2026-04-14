from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import tomllib

from .models import ExecutionMode, RunMode


def _enum_value(enum_type: type[RunMode] | type[ExecutionMode], raw: str, default: str) -> Any:
    try:
        return enum_type(raw)
    except ValueError:
        return enum_type(default)


@dataclass(slots=True)
class StorageConfig:
    root: str = "data"
    raw_path: str = "data/raw"
    normalized_path: str = "data/normalized"
    notebook_path: str = "notebooks"

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> "StorageConfig":
        return cls(**{field_name: payload.get(field_name, getattr(cls(), field_name)) for field_name in cls.__dataclass_fields__})


@dataclass(slots=True)
class UniverseConfig:
    dynamic: bool = True
    quote_coin: str = "USDT"
    symbols: list[str] = field(default_factory=lambda: ["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    base_coins: list[str] = field(default_factory=list)
    min_24h_turnover: float = 5_000_000.0
    max_spread_bps: float = 25.0
    min_listing_days: int = 21
    require_funding_observations: int = 5

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> "UniverseConfig":
        instance = cls()
        for field_name in cls.__dataclass_fields__:
            setattr(instance, field_name, payload.get(field_name, getattr(instance, field_name)))
        return instance


@dataclass(slots=True)
class RiskLimits:
    max_gross_leverage: float = 1.8
    max_single_symbol_notional: float = 25_000.0
    max_order_notional: float = 10_000.0
    max_drawdown: float = 0.18
    max_daily_loss: float = 0.05
    stale_after_seconds: int = 90
    option_vega_limit: float = 2_000.0

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> "RiskLimits":
        instance = cls()
        for field_name in cls.__dataclass_fields__:
            setattr(instance, field_name, payload.get(field_name, getattr(instance, field_name)))
        return instance


@dataclass(slots=True)
class ExecutionSettings:
    run_mode: RunMode = RunMode.BACKTEST
    execution_mode: ExecutionMode = ExecutionMode.MANUAL
    use_demo: bool = True
    poll_interval_seconds: int = 60
    confirm_before_send: bool = False

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> "ExecutionSettings":
        return cls(
            run_mode=_enum_value(RunMode, payload.get("run_mode", RunMode.BACKTEST.value), RunMode.BACKTEST.value),
            execution_mode=_enum_value(
                ExecutionMode,
                payload.get("execution_mode", ExecutionMode.MANUAL.value),
                ExecutionMode.MANUAL.value,
            ),
            use_demo=payload.get("use_demo", True),
            poll_interval_seconds=payload.get("poll_interval_seconds", 60),
            confirm_before_send=payload.get("confirm_before_send", False),
        )


@dataclass(slots=True)
class StrategyConfig:
    name: str
    enabled: bool = True
    params: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> "StrategyConfig":
        return cls(
            name=payload["name"],
            enabled=payload.get("enabled", True),
            params=payload.get("params", {}),
        )


@dataclass(slots=True)
class AppConfig:
    storage: StorageConfig = field(default_factory=StorageConfig)
    universe: UniverseConfig = field(default_factory=UniverseConfig)
    risk: RiskLimits = field(default_factory=RiskLimits)
    execution: ExecutionSettings = field(default_factory=ExecutionSettings)
    strategies: list[StrategyConfig] = field(default_factory=list)

    @classmethod
    def load(cls, path: str | Path) -> "AppConfig":
        config_path = Path(path)
        payload = tomllib.loads(config_path.read_text())
        strategies = [StrategyConfig.from_mapping(entry) for entry in payload.get("strategies", [])]
        return cls(
            storage=StorageConfig.from_mapping(payload.get("storage", {})),
            universe=UniverseConfig.from_mapping(payload.get("universe", {})),
            risk=RiskLimits.from_mapping(payload.get("risk", {})),
            execution=ExecutionSettings.from_mapping(payload.get("execution", {})),
            strategies=strategies,
        )

    def strategy(self, name: str) -> StrategyConfig | None:
        for item in self.strategies:
            if item.name == name:
                return item
        return None

