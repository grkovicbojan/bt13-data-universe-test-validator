"""One-off proxy checker for Reddit JSON API. Run: python scripts/check_proxies.py"""
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scraping.proxy import ProxyPool, ProxyEntry, proxy_client_session

TEST_URL = "https://www.reddit.com/r/gcse/top.json?limit=1&raw_json=1"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


async def test_entry(label: str, proxy: ProxyEntry | None) -> None:
    try:
        async with proxy_client_session(proxy, USER_AGENT, timeout_seconds=25) as session:
            async with session.get(TEST_URL) as resp:
                body = await resp.text()
                ok = resp.status == 200 and "children" in body
                status = "OK" if ok else "FAIL"
                print(
                    f"{label:28} {status:4}  http={resp.status}  bytes={len(body)}"
                )
                if not ok:
                    print(f"  preview: {body[:120]!r}")
    except Exception as exc:
        print(f"{label:28} FAIL  err={type(exc).__name__}: {exc}")


async def main() -> None:
    config_path = ROOT / "scraping" / "cookies" / "proxy_config.json"
    print(f"Config: {config_path}")
    print(f"Target: {TEST_URL}\n")

    await test_entry("direct (no proxy)", None)

    pool = ProxyPool.from_file(config_path)
    print("-" * 72)
    for i, entry in enumerate(pool._proxies, start=1):
        label = f"#{i} {entry.protocol}://{entry.display_address()}"
        await test_entry(label, entry)


if __name__ == "__main__":
    asyncio.run(main())
