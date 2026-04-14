from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN, ROUND_UP
import hashlib
import hmac
import importlib.util
import json
import os
from pathlib import Path
import ssl
import subprocess
import sys
import time
from typing import Any, Callable
from urllib.parse import urlencode
from urllib.error import URLError
from urllib.request import urlopen

from .demo_credentials import load_demo_credentials
from .exchange import BybitClient
from .models import OrderIntent

PUBLIC_REST_BASE = "https://api.bybit.com"
PUBLIC_LINEAR_WS = "wss://stream.bybit.com/v5/public/linear"
PUBLIC_OPTION_WS = "wss://stream.bybit.com/v5/public/option"
DEMO_PRIVATE_WS = "wss://stream-demo.bybit.com/v5/private"


class VerificationFailure(RuntimeError):
    pass


@dataclass(slots=True)
class VerificationResult:
    name: str
    status: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    duration_seconds: float = 0.0

    @property
    def ok(self) -> bool:
        return self.status == "passed"

    def to_line(self) -> str:
        detail = f" details={json.dumps(self.details, sort_keys=True, default=str)}" if self.details else ""
        return f"[{self.status.upper()}] {self.name}: {self.message}{detail}"

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "message": self.message,
            "details": self.details,
            "duration_seconds": self.duration_seconds,
        }


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _timed(name: str, fn: Callable[..., VerificationResult], *args: Any, **kwargs: Any) -> VerificationResult:
    started = time.monotonic()
    try:
        result = fn(*args, **kwargs)
    except VerificationFailure as exc:
        result = VerificationResult(name=name, status="failed", message=str(exc))
    except Exception as exc:  # pragma: no cover - defensive wrapper
        result = VerificationResult(name=name, status="failed", message=f"Unexpected error: {exc}")
    result.duration_seconds = round(time.monotonic() - started, 3)
    return result


def _module_available(module_name: str) -> bool:
    return importlib.util.find_spec(module_name) is not None


def _require_modules(*module_names: str) -> None:
    missing = [name for name in module_names if not _module_available(name)]
    if missing:
        raise VerificationFailure(
            "Missing required Python modules: "
            + ", ".join(missing)
            + ". Install project dependencies before running this verification target."
        )


def _require_demo_credentials() -> tuple[str, str]:
    credentials = load_demo_credentials()
    if credentials is None:
        raise VerificationFailure(
            "Missing demo credentials. Add the local demo credential source or set BYBIT_DEMO_API_KEY and BYBIT_DEMO_API_SECRET."
        )
    return credentials.api_key, credentials.api_secret


def _public_get_json(path: str, params: dict[str, Any]) -> dict[str, Any]:
    query = urlencode(params)
    url = f"{PUBLIC_REST_BASE}{path}?{query}"
    try:
        with urlopen(url, timeout=15) as response:
            return json.loads(response.read().decode("utf-8"))
    except URLError as exc:
        reason = getattr(exc, "reason", None)
        cert_error = isinstance(reason, ssl.SSLCertVerificationError) or "CERTIFICATE_VERIFY_FAILED" in str(exc)
        if not cert_error:
            raise
        insecure_context = ssl._create_unverified_context()
        with urlopen(url, timeout=15, context=insecure_context) as response:
            return json.loads(response.read().decode("utf-8"))


def _network_ssl_context() -> ssl.SSLContext:
    certifi_spec = importlib.util.find_spec("certifi")
    if certifi_spec is not None:
        import certifi

        return ssl.create_default_context(cafile=certifi.where())
    return ssl._create_unverified_context()


def _require_ret_code_zero(payload: dict[str, Any], label: str) -> dict[str, Any]:
    if payload.get("retCode") != 0:
        raise VerificationFailure(f"{label} failed with retCode={payload.get('retCode')}, retMsg={payload.get('retMsg')}")
    return payload


def _decimal_step(value: str | float, step: str | float, *, rounding: str) -> str:
    value_decimal = Decimal(str(value))
    step_decimal = Decimal(str(step))
    if step_decimal <= 0:
        return str(value_decimal)
    quantized = (value_decimal / step_decimal).to_integral_value(rounding=ROUND_DOWN if rounding == "down" else ROUND_UP)
    normalized = quantized * step_decimal
    return format(normalized.normalize(), "f")


def run_runner_module_smoke() -> VerificationResult:
    root = project_root()
    env = os.environ.copy()
    existing_path = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(root / "src") + (os.pathsep + existing_path if existing_path else "")
    command = [
        sys.executable,
        "-m",
        "bybit_trader.runner",
        "--sample-data",
        "--cycles",
        "1",
        "--strategy",
        "momentum_rotation",
    ]
    completed = subprocess.run(
        command,
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    combined_output = "\n".join(part for part in [completed.stdout.strip(), completed.stderr.strip()] if part).strip()
    if completed.returncode != 0:
        raise VerificationFailure(f"Runner module exited with code {completed.returncode}. Output: {combined_output or '<empty>'}")
    if not completed.stdout.strip():
        raise VerificationFailure("Runner module produced no stdout; expected at least one emitted payload.")
    payload_line = completed.stdout.strip().splitlines()[-1]
    try:
        payload = json.loads(payload_line)
    except json.JSONDecodeError as exc:
        raise VerificationFailure(f"Runner stdout was not parseable as JSON: {payload_line}") from exc
    if "orders_submitted" not in payload:
        raise VerificationFailure(f"Runner payload is missing 'orders_submitted': {payload}")
    return VerificationResult(
        name="runner-module-smoke",
        status="passed",
        message="Module invocation produced a runner payload.",
        details={"orders_submitted": payload.get("orders_submitted"), "strategy": payload.get("monitor", {}).get("universe", [])[:3]},
    )


def verify_public_rest() -> VerificationResult:
    ticker_payload = _require_ret_code_zero(
        _public_get_json("/v5/market/tickers", {"category": "linear", "symbol": "BTCUSDT"}),
        "Tickers",
    )
    kline_payload = _require_ret_code_zero(
        _public_get_json("/v5/market/kline", {"category": "linear", "symbol": "BTCUSDT", "interval": "60", "limit": 3}),
        "Kline",
    )
    instruments_payload = _require_ret_code_zero(
        _public_get_json("/v5/market/instruments-info", {"category": "linear", "limit": 3}),
        "Instruments info",
    )
    funding_payload = _require_ret_code_zero(
        _public_get_json("/v5/market/funding/history", {"category": "linear", "symbol": "BTCUSDT", "limit": 3}),
        "Funding history",
    )

    ticker_row = ticker_payload.get("result", {}).get("list", [{}])[0]
    kline_rows = kline_payload.get("result", {}).get("list", [])
    instruments_rows = instruments_payload.get("result", {}).get("list", [])
    funding_rows = funding_payload.get("result", {}).get("list", [])
    if ticker_row.get("symbol") != "BTCUSDT" or not ticker_row.get("lastPrice"):
        raise VerificationFailure(f"Ticker response did not include a valid BTCUSDT last price: {ticker_row}")
    if len(kline_rows) < 1:
        raise VerificationFailure("Kline response was empty.")
    if len(instruments_rows) < 1:
        raise VerificationFailure("Instruments response was empty.")
    if len(funding_rows) < 1:
        raise VerificationFailure("Funding history response was empty.")
    return VerificationResult(
        name="public-rest",
        status="passed",
        message="Public REST endpoints returned valid market data.",
        details={
            "ticker_last_price": ticker_row.get("lastPrice"),
            "kline_count": len(kline_rows),
            "instrument_symbols": [row.get("symbol") for row in instruments_rows[:3]],
            "funding_samples": len(funding_rows),
        },
    )


async def _ws_subscribe_and_wait(uri: str, args: list[str], predicate: Callable[[dict[str, Any]], bool], timeout_seconds: float = 10.0) -> dict[str, Any]:
    import websockets

    async with websockets.connect(
        uri,
        ping_interval=None,
        close_timeout=2,
        max_size=2_000_000,
        ssl=_network_ssl_context(),
    ) as websocket:
        await websocket.send(json.dumps({"op": "subscribe", "args": args}))
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            raw = await asyncio.wait_for(websocket.recv(), timeout=deadline - time.monotonic())
            message = json.loads(raw)
            if predicate(message):
                return message
            if message.get("success") is False:
                raise VerificationFailure(f"WebSocket subscription failed: {message}")
        raise VerificationFailure(f"Timed out waiting for a matching websocket message on {uri} for args={args}")


async def _ws_authenticate_and_subscribe(uri: str, api_key: str, api_secret: str, topics: list[str], timeout_seconds: float = 10.0) -> dict[str, Any]:
    import websockets

    expires = int((time.time() + 10) * 1000)
    signature = hmac.new(
        api_secret.encode("utf-8"),
        f"GET/realtime{expires}".encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()
    async with websockets.connect(
        uri,
        ping_interval=None,
        close_timeout=2,
        max_size=2_000_000,
        ssl=_network_ssl_context(),
    ) as websocket:
        await websocket.send(json.dumps({"op": "auth", "args": [api_key, expires, signature]}))
        auth_message = json.loads(await asyncio.wait_for(websocket.recv(), timeout=timeout_seconds))
        if not auth_message.get("success"):
            raise VerificationFailure(f"Private websocket auth failed: {auth_message}")
        await websocket.send(json.dumps({"op": "subscribe", "args": topics}))
        subscription_message = json.loads(await asyncio.wait_for(websocket.recv(), timeout=timeout_seconds))
        if subscription_message.get("success") is False:
            raise VerificationFailure(f"Private websocket subscription failed: {subscription_message}")
        await websocket.send(json.dumps({"op": "ping"}))
        pong_message = json.loads(await asyncio.wait_for(websocket.recv(), timeout=timeout_seconds))
        if pong_message.get("op") != "pong":
            raise VerificationFailure(f"Expected private websocket pong, got: {pong_message}")
        return {"auth": auth_message, "subscribe": subscription_message, "pong": pong_message}


def verify_public_websocket() -> VerificationResult:
    _require_modules("websockets")
    option_payload = _require_ret_code_zero(
        _public_get_json("/v5/market/tickers", {"category": "option", "baseCoin": "BTC"}),
        "Option tickers",
    )
    option_rows = option_payload.get("result", {}).get("list", [])
    if not option_rows:
        raise VerificationFailure("Option ticker REST bootstrap returned no rows.")
    option_symbol = option_rows[0]["symbol"]

    async def _run() -> dict[str, Any]:
        linear_message = await _ws_subscribe_and_wait(
            PUBLIC_LINEAR_WS,
            ["tickers.BTCUSDT"],
            lambda message: message.get("topic") == "tickers.BTCUSDT" and bool(message.get("data")),
        )
        option_message = await _ws_subscribe_and_wait(
            PUBLIC_OPTION_WS,
            [f"tickers.{option_symbol}"],
            lambda message: message.get("topic") == f"tickers.{option_symbol}" and bool(message.get("data")),
        )
        linear_data = linear_message.get("data", {})
        option_data = option_message.get("data", {})
        return {
            "linear_topic": linear_message.get("topic"),
            "linear_last_price": linear_data.get("lastPrice"),
            "option_topic": option_message.get("topic"),
            "option_mark_price": option_data.get("markPrice"),
        }

    details = asyncio.run(_run())
    return VerificationResult(
        name="public-websocket",
        status="passed",
        message="Public linear and option websocket feeds delivered market snapshots.",
        details=details,
    )


def verify_private_demo_topics() -> VerificationResult:
    _require_modules("websockets")
    api_key, api_secret = _require_demo_credentials()
    topics = ["order", "position", "wallet", "execution.fast", "greeks"]
    try:
        details = asyncio.run(
            _ws_authenticate_and_subscribe(
                DEMO_PRIVATE_WS,
                api_key,
                api_secret,
                topics,
            )
        )
    except VerificationFailure as exc:
        if "execution.fast" not in str(exc):
            raise
        topics = ["order", "position", "wallet", "execution", "greeks"]
        details = asyncio.run(
            _ws_authenticate_and_subscribe(
                DEMO_PRIVATE_WS,
                api_key,
                api_secret,
                topics,
            )
        )
    return VerificationResult(
        name="private-demo-topics",
        status="passed",
        message="Demo private websocket authenticated and subscribed successfully.",
        details={"conn_id": details["auth"].get("conn_id"), "topics": topics},
    )


def verify_private_demo_order_lifecycle() -> VerificationResult:
    _require_modules("pybit", "websockets")
    api_key, api_secret = _require_demo_credentials()

    market_ticker = _require_ret_code_zero(
        _public_get_json("/v5/market/tickers", {"category": "linear", "symbol": "BTCUSDT"}),
        "Tickers",
    )
    market_instrument = _require_ret_code_zero(
        _public_get_json("/v5/market/instruments-info", {"category": "linear", "symbol": "BTCUSDT"}),
        "Instruments info",
    )
    ticker_row = market_ticker["result"]["list"][0]
    instrument_row = market_instrument["result"]["list"][0]
    tick_size = instrument_row["priceFilter"]["tickSize"]
    min_qty = instrument_row["lotSizeFilter"]["minOrderQty"]
    qty_step = instrument_row["lotSizeFilter"]["qtyStep"]
    last_price = Decimal(ticker_row["lastPrice"])
    bid_price = Decimal(ticker_row.get("bid1Price") or ticker_row["lastPrice"])

    qty = _decimal_step(min_qty, qty_step, rounding="up")
    price = _decimal_step(min(bid_price, last_price) * Decimal("0.995"), tick_size, rounding="down")
    order_link_id = f"codex-{int(time.time())}"
    client = BybitClient(api_key=api_key, api_secret=api_secret, demo=True)
    initial_open_orders = client.get_open_orders(category="linear", settle_coin="USDT")
    initial_positions = client.get_positions(category="linear", settle_coin="USDT")
    initial_wallet = client.get_wallet_balance(account_type="UNIFIED", coin="USDT")

    async def _run() -> dict[str, Any]:
        import websockets

        expires = int((time.time() + 10) * 1000)
        signature = hmac.new(
            api_secret.encode("utf-8"),
            f"GET/realtime{expires}".encode("utf-8"),
            digestmod=hashlib.sha256,
        ).hexdigest()
        async with websockets.connect(
            DEMO_PRIVATE_WS,
            ping_interval=None,
            close_timeout=2,
            max_size=2_000_000,
            ssl=_network_ssl_context(),
        ) as websocket:
            await websocket.send(json.dumps({"op": "auth", "args": [api_key, expires, signature]}))
            auth_message = json.loads(await asyncio.wait_for(websocket.recv(), timeout=10))
            if not auth_message.get("success"):
                raise VerificationFailure(f"Private websocket auth failed: {auth_message}")
            await websocket.send(json.dumps({"op": "subscribe", "args": ["order"]}))
            subscribe_message = json.loads(await asyncio.wait_for(websocket.recv(), timeout=10))
            if subscribe_message.get("success") is False:
                raise VerificationFailure(f"Order subscription failed: {subscribe_message}")

            create_response = client.place_order(
                OrderIntent(
                    symbol="BTCUSDT",
                    quantity=float(qty),
                    order_type="Limit",
                    limit_price=float(price),
                    metadata={"timeInForce": "PostOnly", "orderLinkId": order_link_id},
                )
            )
            order_id = create_response.get("result", {}).get("orderId")
            if not order_id:
                raise VerificationFailure(f"REST order placement did not return orderId: {create_response}")

            created_event: dict[str, Any] | None = None
            cancelled_event: dict[str, Any] | None = None
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline and (created_event is None or cancelled_event is None):
                raw = await asyncio.wait_for(websocket.recv(), timeout=deadline - time.monotonic())
                message = json.loads(raw)
                if message.get("topic") != "order":
                    continue
                for row in message.get("data", []):
                    if row.get("orderLinkId") != order_link_id:
                        continue
                    if created_event is None and row.get("orderStatus") in {"New", "PartiallyFilled"}:
                        created_event = row
                        cancel_response = client.cancel_order("BTCUSDT", category="linear", orderId=order_id)
                        if cancel_response.get("retCode") != 0:
                            raise VerificationFailure(f"REST cancel failed: {cancel_response}")
                    elif row.get("orderStatus") == "Cancelled":
                        cancelled_event = row
                        break

            if created_event is None:
                raise VerificationFailure("Did not observe the created order event for the demo order.")
            if cancelled_event is None:
                raise VerificationFailure("Did not observe the cancelled order event for the demo order.")

            open_orders = client.get_open_orders(category="linear", settle_coin="USDT")
            lingering = [row for row in open_orders if row.get("orderLinkId") == order_link_id]
            if lingering:
                raise VerificationFailure(f"Order still appears in realtime open orders after cancel: {lingering}")
            reconciled_positions = client.get_positions(category="linear", settle_coin="USDT")
            reconciled_wallet = client.get_wallet_balance(account_type="UNIFIED", coin="USDT")
            return {
                "order_id": order_id,
                "created_status": created_event.get("orderStatus"),
                "cancelled_status": cancelled_event.get("orderStatus"),
                "positions_before": len(initial_positions),
                "positions_after": len(reconciled_positions),
                "open_orders_before": len(initial_open_orders),
                "wallet_rows_before": len(initial_wallet),
                "wallet_rows_after": len(reconciled_wallet),
            }

    details = asyncio.run(_run())
    return VerificationResult(
        name="private-demo-order-lifecycle",
        status="passed",
        message="Demo REST order placement and private-stream reconciliation completed.",
        details=details,
    )


def _suite_for_mode(mode: str) -> list[Callable[[], VerificationResult]]:
    if mode == "smoke":
        return [run_runner_module_smoke, verify_public_rest]
    if mode == "public-rest":
        return [verify_public_rest]
    if mode == "public-ws":
        return [verify_public_websocket]
    if mode == "private-demo-topics":
        return [verify_private_demo_topics]
    if mode == "private-demo-order-lifecycle":
        return [verify_private_demo_order_lifecycle]
    if mode == "full":
        suite: list[Callable[[], VerificationResult]] = [
            run_runner_module_smoke,
            verify_public_rest,
            verify_public_websocket,
        ]
        if load_demo_credentials() is not None:
            suite.extend([verify_private_demo_topics, verify_private_demo_order_lifecycle])
        return suite
    raise ValueError(f"Unsupported verification mode: {mode}")


def run_verification_suite(mode: str = "smoke") -> list[VerificationResult]:
    results = []
    for fn in _suite_for_mode(mode):
        results.append(_timed(fn.__name__.replace("_", "-"), fn))
    if mode == "full" and load_demo_credentials() is None:
        results.append(
            VerificationResult(
                name="private-demo-gate",
                status="skipped",
                message="Demo private verification was skipped because demo credentials are not configured.",
            )
        )
    return results


def _exit_code(results: list[VerificationResult]) -> int:
    return 0 if all(result.status in {"passed", "skipped"} for result in results) else 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Run verification and smoke checks for the Bybit trader project.")
    parser.add_argument(
        "--mode",
        default="smoke",
        choices=["smoke", "public-rest", "public-ws", "private-demo-topics", "private-demo-order-lifecycle", "full"],
        help="Verification target to execute.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON instead of text lines.")
    args = parser.parse_args()

    results = run_verification_suite(args.mode)
    if args.json:
        print(json.dumps([result.as_dict() for result in results], default=str))
    else:
        for result in results:
            print(result.to_line())
    raise SystemExit(_exit_code(results))


if __name__ == "__main__":
    main()
