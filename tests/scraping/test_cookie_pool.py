import json
import tempfile
import unittest
from pathlib import Path

from scraping.cookie_pool import (
    CookiePool,
    normalize_cookie_editor_export,
    parse_cookie_accounts_from_data,
)


SAMPLE_COOKIE = {
    "domain": ".x.com",
    "name": "auth_token",
    "path": "/",
    "value": "abc123",
    "secure": True,
}


class TestCookiePool(unittest.TestCase):
    def test_log_label(self):
        account = parse_cookie_accounts_from_data([SAMPLE_COOKIE])[0]
        self.assertEqual(account.log_label(), "default (1 cookies)")

    def test_legacy_flat_array(self):
        accounts = parse_cookie_accounts_from_data([SAMPLE_COOKIE, SAMPLE_COOKIE])
        self.assertEqual(len(accounts), 1)
        self.assertEqual(len(accounts[0].cookies), 2)

    def test_multi_account_format(self):
        raw = {
            "accounts": [
                {"label": "a1", "cookies": [SAMPLE_COOKIE]},
                {"label": "a2", "cookies": [SAMPLE_COOKIE, SAMPLE_COOKIE]},
            ]
        }
        accounts = parse_cookie_accounts_from_data(raw)
        self.assertEqual(len(accounts), 2)
        self.assertEqual(accounts[0].label, "a1")
        self.assertEqual(accounts[1].size, 2)

    def test_pool_pick_one(self):
        raw = {
            "accounts": [
                {"label": "a1", "cookies": [SAMPLE_COOKIE]},
                {"label": "a2", "cookies": [SAMPLE_COOKIE]},
            ]
        }
        pool = CookiePool(parse_cookie_accounts_from_data(raw))
        picked = pool.pick_one()
        self.assertIsNotNone(picked)
        self.assertIn(picked.label, ("a1", "a2"))

    def test_normalize_expiration_date(self):
        cookie = {
            **SAMPLE_COOKIE,
            "expirationDate": 1805425665.166837,
            "sameSite": "no_restriction",
        }
        out = normalize_cookie_editor_export([cookie])
        self.assertEqual(out[0]["expires"], 1805425665.166837)
        self.assertEqual(out[0]["sameSite"], "None")

    def test_from_file(self):
        payload = {"accounts": [{"label": "x", "cookies": [SAMPLE_COOKIE]}]}
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tmp:
            json.dump(payload, tmp)
            path = tmp.name
        pool = CookiePool.from_file(path)
        self.assertEqual(pool.size, 1)


if __name__ == "__main__":
    unittest.main()
