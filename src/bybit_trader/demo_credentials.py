from __future__ import annotations

from dataclasses import dataclass, field
import importlib
import os


def mask_secret(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


@dataclass(slots=True)
class DemoCredentials:
    api_key: str = field(repr=False)
    api_secret: str = field(repr=False)
    source: str = "unknown"
    demo: bool = True
    testnet: bool = False

    def masked(self) -> dict[str, str]:
        return {
            "api_key": mask_secret(self.api_key),
            "api_secret": mask_secret(self.api_secret),
            "source": self.source,
        }


def _load_local_demo_credentials() -> DemoCredentials | None:
    if os.environ.get("BYBIT_TRADER_DISABLE_LOCAL_DEMO_CREDS", "").strip() == "1":
        return None
    try:
        module = importlib.import_module("bybit_trader._local_demo_credentials")
    except ModuleNotFoundError:
        return None
    api_key = str(getattr(module, "BYBIT_DEMO_API_KEY", "")).strip()
    api_secret = str(getattr(module, "BYBIT_DEMO_API_SECRET", "")).strip()
    if not api_key or not api_secret:
        return None
    return DemoCredentials(api_key=api_key, api_secret=api_secret, source="local_source")


def _load_env_demo_credentials() -> DemoCredentials | None:
    api_key = os.environ.get("BYBIT_DEMO_API_KEY", "").strip()
    api_secret = os.environ.get("BYBIT_DEMO_API_SECRET", "").strip()
    if not api_key or not api_secret:
        return None
    return DemoCredentials(api_key=api_key, api_secret=api_secret, source="environment")


def load_demo_credentials(*, required: bool = False) -> DemoCredentials | None:
    credentials = _load_local_demo_credentials() or _load_env_demo_credentials()
    if credentials is not None:
        return credentials
    if required:
        raise RuntimeError(
            "Demo credentials are not configured. Add the local credential source or set BYBIT_DEMO_API_KEY and BYBIT_DEMO_API_SECRET."
        )
    return None
