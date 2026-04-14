# Bybit Crypto Trader

Python-first research and execution scaffold for Bybit USDT perpetuals and Bybit options.

## What is included

- Shared domain types for research, backtesting, demo, and live execution
- Dynamic linear-perp universe discovery with configurable liquidity and spread filters
- A deterministic backtest engine with fees, slippage, funding, and risk caps
- Notebook-facing APIs for research, backtests, paper trading, and runner bootstrap
- Local demo-credential loading for notebook and verification flows on this private machine
- Real Bybit public-data ingestion for 1h perp backtests and limited options research summaries
- Strategy implementations for trend, mean reversion, carry, momentum rotation, breakout, and options overlays
- A Bybit client and broker adapter scaffold that is ready to be wired to real credentials

## Project layout

```text
.
├── configs/default.toml
├── notebooks/
├── src/bybit_trader/
└── tests/
```

## Quickstart

1. Create a virtual environment with Python 3.11+.
2. Install the package:

```bash
python3 -m pip install -e .
```

3. Run the unit tests:

```bash
python3 -m unittest discover -s tests
```

4. Open the example notebook:

```bash
jupyter lab notebooks/01_research_and_backtest.ipynb
```

5. Run the verification smoke suite:

```bash
PYTHONPATH=src python3 -m bybit_trader.verification --mode smoke
```

6. Run the demo websocket and order-lifecycle checks:

```bash
PYTHONPATH=src python3 -m bybit_trader.verification --mode private-demo-topics
PYTHONPATH=src python3 -m bybit_trader.verification --mode private-demo-order-lifecycle
```

## Minimal notebook usage

```python
from bybit_trader.session import NotebookSession

session = NotebookSession.with_sample_data()
result = session.backtest("perp_trend")
result.summary()
```

## Real-data analytics workflow

```python
from bybit_trader.session import NotebookSession

session = NotebookSession.from_config("configs/default.toml")
session.fetch_real_market_data(lookback_days=180, interval_minutes=60)
analytics = session.run_default_perp_analytics()
option_research = session.option_research_summary()
```

## Notes

- The Bybit adapter uses lazy imports so the package can still be inspected and unit-tested before external dependencies are installed.
- Options backtesting is deliberately conservative until native option-chain history has been recorded by the included recorder workflow.
- The local demo credential loader is intentionally private to this machine; verification and notebook helpers use it automatically and only fall back to environment variables when local credentials are disabled or absent.
- Live trading support is scaffolded with restart-safe reconciliation hooks and a runner loop, but you should still add alerting, deployment hardening, and account-level safeguards before production use.
- Public websocket verification needs the `websockets` dependency. Demo private verification uses the local demo credential source by default.
