"""
X/Twitter cookie pool for Playwright scraping.

Supports multiple logged-in accounts in one file. Pick one account per scrape job
(organic scrape or on-demand request) so pagination stays on the same session.

File formats (COOKIES_PATH):

1) Multi-account (recommended):
{
  "accounts": [
    {"label": "acct_1", "cookies": [ {...}, {...} ]},
    {"label": "acct_2", "cookies": [ {...}, {...} ]}
  ]
}

2) Legacy single account — flat Cookie-Editor export array:
[ {"domain": ".x.com", "name": "auth_token", ...}, ... ]

3) Legacy wrapper:
{"cookies": [ ... ]}
"""

from __future__ import annotations

import json
import logging
import os
import random
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

_DEFAULT_COOKIES_PATH = Path(__file__).resolve().parent / "cookies" / "cookies.json"


class CookieAccount(BaseModel):
    """One X login session (Cookie-Editor export)."""

    cookies: List[dict] = Field(default_factory=list)
    label: str = Field(default="default")

    @property
    def size(self) -> int:
        return len(self.cookies)

    def log_label(self) -> str:
        """Safe label for error logs (no cookie values)."""
        return f"{self.label} ({self.size} cookies)"


class CookiePool:
    """Pool of X cookie sets; pick one per Playwright scrape job."""

    def __init__(self, accounts: List[CookieAccount]):
        self._accounts = [a for a in accounts if a.cookies]

    @property
    def size(self) -> int:
        return len(self._accounts)

    def pick_one(self) -> Optional[CookieAccount]:
        if not self._accounts:
            return None
        return random.choice(self._accounts)

    @classmethod
    def from_file(cls, path: str | Path) -> "CookiePool":
        accounts = parse_cookie_accounts(Path(path))
        return cls(accounts)

    @classmethod
    def from_env(cls) -> Optional["CookiePool"]:
        resolved = resolve_cookies_path()
        if resolved is None:
            return None
        try:
            return cls.from_file(resolved)
        except Exception:
            logger.error("Failed to load cookie pool from %s", resolved, exc_info=True)
            return None


def resolve_cookies_path() -> Optional[Path]:
    env_path = os.getenv("COOKIES_PATH")
    if env_path:
        candidate = Path(env_path).expanduser()
        if not candidate.is_absolute():
            candidate = Path.cwd() / candidate
        return candidate if candidate.is_file() else None

    if _DEFAULT_COOKIES_PATH.is_file():
        return _DEFAULT_COOKIES_PATH
    return None


def parse_cookie_accounts(path: Path) -> List[CookieAccount]:
    with path.open(encoding="utf-8") as handle:
        raw = json.load(handle)
    return parse_cookie_accounts_from_data(raw)


def parse_cookie_accounts_from_data(raw: object) -> List[CookieAccount]:
    if isinstance(raw, list):
        if not raw:
            return []
        if _looks_like_cookie_list(raw):
            return [CookieAccount(label="default", cookies=raw)]
        accounts: List[CookieAccount] = []
        for index, item in enumerate(raw):
            accounts.extend(_parse_account_entry(item, default_label=f"account_{index + 1}"))
        return accounts

    if isinstance(raw, dict):
        if "accounts" in raw and isinstance(raw["accounts"], list):
            accounts = []
            for index, item in enumerate(raw["accounts"]):
                accounts.extend(
                    _parse_account_entry(item, default_label=f"account_{index + 1}")
                )
            return accounts
        if "cookies" in raw and isinstance(raw["cookies"], list):
            label = str(raw.get("label") or raw.get("name") or "default")
            return [CookieAccount(label=label, cookies=raw["cookies"])]

    raise ValueError(
        "Unsupported cookies file format. Use a Cookie-Editor array, "
        '{"cookies": [...]}, or {"accounts": [...]}.'
    )


def _parse_account_entry(item: object, default_label: str) -> List[CookieAccount]:
    if isinstance(item, list):
        return [CookieAccount(label=default_label, cookies=item)]
    if isinstance(item, dict):
        if "cookies" in item and isinstance(item["cookies"], list):
            label = str(item.get("label") or item.get("name") or default_label)
            return [CookieAccount(label=label, cookies=item["cookies"])]
        if _looks_like_cookie_dict(item):
            return [CookieAccount(label=default_label, cookies=[item])]
    return []


def _looks_like_cookie_list(items: list) -> bool:
    return bool(items) and all(_looks_like_cookie_dict(x) for x in items if isinstance(x, dict))


def _looks_like_cookie_dict(item: dict) -> bool:
    return "name" in item and "value" in item and ("domain" in item or "url" in item)


def normalize_cookie_editor_export(cookies: List[dict]) -> List[dict]:
    """Convert Cookie-Editor JSON into Playwright add_cookies() format."""
    normalized = []
    for cookie in cookies:
        if not isinstance(cookie, dict):
            continue

        name = cookie.get("name")
        value = cookie.get("value")
        if not name or value is None:
            continue

        domain = cookie.get("domain")
        path = cookie.get("path") or "/"

        out = {
            "name": name,
            "value": str(value),
            "path": path,
        }

        if domain:
            out["domain"] = domain
        elif cookie.get("url"):
            out["url"] = cookie["url"]

        expires = cookie.get("expires", cookie.get("expirationDate"))
        if isinstance(expires, (int, float)):
            out["expires"] = expires
        if "httpOnly" in cookie:
            out["httpOnly"] = bool(cookie["httpOnly"])
        if "secure" in cookie:
            out["secure"] = bool(cookie["secure"])

        same_site = cookie.get("sameSite")
        if isinstance(same_site, str):
            ss = same_site.strip().capitalize()
            if ss == "No_restriction":
                ss = "None"
            if ss in {"Lax", "None", "Strict"}:
                out["sameSite"] = ss

        normalized.append(out)
    return normalized
