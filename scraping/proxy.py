"""
Proxy pool loader for scrapers.

Loads proxy definitions from a JSON file (default: scraping/cookies/proxy_config.json).
Set PROXY_CONFIG_PATH in the environment to override the file location.

Each scrape session should pick one proxy and reuse the same aiohttp session so
Reddit pagination (?after=) stays on a consistent egress IP.
"""

from __future__ import annotations

import json
import os
import random
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator, List, Optional
from urllib.parse import quote

import aiohttp
from aiohttp_socks import ProxyConnector
from pydantic import BaseModel, Field, field_validator

SUPPORTED_PROTOCOLS = frozenset({"http", "https", "socks4", "socks5"})

_DEFAULT_PROXY_CONFIG = Path(__file__).resolve().parent / "cookies" / "proxy_config.json"


class ProxyEntry(BaseModel):
    """Single proxy endpoint."""

    protocol: str = Field(description="http, https, socks4, or socks5")
    ip: str = Field(description="Proxy host or IP address")
    port: int = Field(gt=0, le=65535)
    username: Optional[str] = None
    password: Optional[str] = None

    @field_validator("protocol")
    @classmethod
    def normalize_protocol(cls, value: str) -> str:
        normalized = value.lower().strip()
        if normalized not in SUPPORTED_PROTOCOLS:
            supported = ", ".join(sorted(SUPPORTED_PROTOCOLS))
            raise ValueError(
                f"Unsupported proxy protocol '{value}'. Supported: {supported}"
            )
        return normalized

    def to_url(self) -> str:
        """Build a proxy URL suitable for aiohttp-socks ProxyConnector."""
        if self.username is not None and self.password is not None:
            user = quote(self.username, safe="")
            pwd = quote(self.password, safe="")
            auth = f"{user}:{pwd}@"
        else:
            auth = ""
        return f"{self.protocol}://{auth}{self.ip}:{self.port}"

    def display_address(self) -> str:
        """Host:port without credentials (for logs)."""
        return f"{self.ip}:{self.port}"

    def log_label(self) -> str:
        """Safe label for error logs (no credentials)."""
        return f"{self.protocol}://{self.display_address()}"


class ProxyConfigFile(BaseModel):
    proxies: List[ProxyEntry] = Field(default_factory=list)


class ProxyPool:
    """Pool of proxies; pick one per scrape session."""

    def __init__(self, proxies: List[ProxyEntry]):
        self._proxies = list(proxies)

    @property
    def size(self) -> int:
        return len(self._proxies)

    def pick_one(self) -> Optional[ProxyEntry]:
        if not self._proxies:
            return None
        return random.choice(self._proxies)

    @classmethod
    def from_file(cls, path: str | Path) -> "ProxyPool":
        config_path = Path(path)
        with config_path.open(encoding="utf-8") as handle:
            raw = json.load(handle)
        config = ProxyConfigFile.model_validate(raw)
        return cls(config.proxies)

    @classmethod
    def from_env(cls) -> Optional["ProxyPool"]:
        resolved = resolve_proxy_config_path()
        if resolved is None:
            return None
        return cls.from_file(resolved)


def resolve_proxy_config_path() -> Optional[Path]:
    """
    Resolve proxy config path from PROXY_CONFIG_PATH or the default under scraping/cookies/.
    Returns None if no file exists (proxies disabled).
    """
    env_path = os.getenv("PROXY_CONFIG_PATH")
    if env_path:
        candidate = Path(env_path).expanduser()
        if not candidate.is_absolute():
            candidate = Path.cwd() / candidate
        return candidate if candidate.is_file() else None

    if _DEFAULT_PROXY_CONFIG.is_file():
        return _DEFAULT_PROXY_CONFIG
    return None


@asynccontextmanager
async def proxy_client_session(
    proxy: Optional[ProxyEntry],
    user_agent: str,
    timeout_seconds: float = 10,
) -> AsyncIterator[aiohttp.ClientSession]:
    """
    Open an aiohttp ClientSession optionally routed through one proxy.

    Use one session (and thus one proxy) for an entire organic or on-demand scrape
  so ?after= pagination shares the same egress.
    """
    headers = {"User-Agent": user_agent}
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    session: Optional[aiohttp.ClientSession] = None
    try:
        if proxy is not None:
            connector = ProxyConnector.from_url(proxy.to_url())
            session = aiohttp.ClientSession(
                headers=headers,
                timeout=timeout,
                connector=connector,
            )
        else:
            session = aiohttp.ClientSession(headers=headers, timeout=timeout)
        yield session
    finally:
        if session is not None:
            await session.close()
