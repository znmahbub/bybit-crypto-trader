from __future__ import annotations

from pathlib import Path
import io
import json
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bybit_trader.verification import VerificationResult, main, run_runner_module_smoke, run_verification_suite, verify_public_rest


class VerificationTests(unittest.TestCase):
    def test_runner_module_smoke_emits_payload(self) -> None:
        result = run_runner_module_smoke()
        self.assertEqual(result.status, "passed")
        self.assertIn("orders_submitted", result.message + str(result.details))

    @patch("bybit_trader.verification._public_get_json")
    def test_public_rest_verification_accepts_valid_payloads(self, mocked_get_json) -> None:
        mocked_get_json.side_effect = [
            {"retCode": 0, "result": {"list": [{"symbol": "BTCUSDT", "lastPrice": "70000"}]}},
            {"retCode": 0, "result": {"list": [["1", "2", "3", "4", "5"]]}},
            {"retCode": 0, "result": {"list": [{"symbol": "BTCUSDT"}, {"symbol": "ETHUSDT"}]}},
            {"retCode": 0, "result": {"list": [{"symbol": "BTCUSDT", "fundingRate": "0.0001", "fundingRateTimestamp": "1"}]}},
        ]
        result = verify_public_rest()
        self.assertEqual(result.status, "passed")
        self.assertEqual(result.details["ticker_last_price"], "70000")
        self.assertEqual(result.details["funding_samples"], 1)

    @patch("bybit_trader.verification._module_available", return_value=False)
    def test_public_ws_dependency_check_fails_clearly(self, _mocked_module_available) -> None:
        result = run_verification_suite("public-ws")[0]
        self.assertEqual(result.status, "failed")
        self.assertIn("Missing required Python modules", result.message)

    @patch("bybit_trader.verification.run_verification_suite")
    def test_verification_cli_can_emit_json(self, mocked_suite) -> None:
        mocked_suite.return_value = [VerificationResult(name="smoke", status="passed", message="ok", details={"x": 1})]
        stdout = io.StringIO()
        with patch.object(sys, "argv", ["verification", "--mode", "smoke", "--json"]), patch("sys.stdout", stdout):
            with self.assertRaises(SystemExit) as raised:
                main()
        self.assertEqual(raised.exception.code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload[0]["status"], "passed")
        self.assertEqual(payload[0]["details"]["x"], 1)


if __name__ == "__main__":
    unittest.main()
