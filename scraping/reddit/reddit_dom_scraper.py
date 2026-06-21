import asyncio
import re
import traceback
import datetime as dt
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncIterator, Iterator, List, Optional, Tuple
from urllib.parse import quote_plus, urljoin, urlparse

import aiohttp
import bittensor as bt
from bs4 import BeautifulSoup, Tag

from scraping.proxy import ProxyEntry, ProxyPool, proxy_client_session
from common.data import DataEntity, DataLabel
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult
from scraping.reddit.model import RedditContent, RedditDataType, DELETED_USER
from scraping.reddit.utils import (
    is_valid_reddit_url,
    validate_reddit_content,
    normalize_label,
    normalize_permalink,
)
from common.protocol import KeywordMode


class RedditDomScraper(Scraper):
    """
    Scrapes Reddit data by parsing HTML from old.reddit.com (no JSON API).
    """

    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    BASE_URL = "https://old.reddit.com"
    PUBLIC_URL = "https://www.reddit.com"

    REQUEST_TIMEOUT = 10
    MAX_RETRIES = 3
    RETRY_DELAY = 2
    PAGE_DELAY = 2

    def __init__(self):
        self._proxy_pool: Optional[ProxyPool] = None
        self._proxy_pool_loaded = False
        self._session_proxy: Optional[ProxyEntry] = None

    def _proxy_error_context(self) -> str:
        if self._session_proxy is None:
            return " [reddit dom proxy=none/direct]"
        return f" [reddit dom proxy={self._session_proxy.log_label()}]"

    @contextmanager
    def _track_proxy_session(self, proxy: Optional[ProxyEntry]) -> Iterator[None]:
        self._session_proxy = proxy
        try:
            yield
        finally:
            self._session_proxy = None

    def _get_proxy_pool(self) -> Optional[ProxyPool]:
        if not self._proxy_pool_loaded:
            try:
                self._proxy_pool = ProxyPool.from_env()
            except Exception:
                bt.logging.error(
                    f"Failed to load proxy config: {traceback.format_exc()}"
                )
                self._proxy_pool = None
            self._proxy_pool_loaded = True
        if self._proxy_pool is None or self._proxy_pool.size == 0:
            return None
        return self._proxy_pool

    def _pick_proxy_for_session(self) -> Optional[ProxyEntry]:
        pool = self._get_proxy_pool()
        if pool is None:
            return None
        proxy = pool.pick_one()
        if proxy is not None:
            bt.logging.info(
                f"Reddit DOM scrape session using proxy "
                f"{proxy.protocol}://{proxy.display_address()}"
            )
        return proxy

    def _is_proxy_connect_error(self, exc: Exception) -> bool:
        name = type(exc).__name__
        if name in ("ProxyError", "ProxyConnectionError", "ProxyTimeoutError"):
            return True
        msg = str(exc).lower()
        return "proxy" in msg or "ruleset" in msg or "socks" in msg

    @asynccontextmanager
    async def _direct_fallback_session(
        self, failed_proxy: ProxyEntry
    ) -> AsyncIterator[aiohttp.ClientSession]:
        bt.logging.warning(
            f"Reddit DOM proxy {failed_proxy.log_label()} failed; "
            f"retrying via direct (local) connection"
        )
        with self._track_proxy_session(None):
            async with proxy_client_session(
                None, self.USER_AGENT, self.REQUEST_TIMEOUT
            ) as session:
                yield session

    def _to_scrape_url(self, url: str) -> str:
        parsed = urlparse(url.strip())
        if parsed.netloc and "reddit.com" in parsed.netloc:
            path = parsed.path or "/"
            if parsed.query:
                path = f"{path}?{parsed.query}"
            return urljoin(self.BASE_URL, path)
        return url

    def _public_url(self, permalink: str) -> str:
        return f"{self.PUBLIC_URL}{normalize_permalink(permalink)}"

    def _extract_score(self, elem: Tag) -> Optional[int]:
        data_score = elem.get("data-score")
        if data_score is not None:
            try:
                return int(data_score)
            except ValueError:
                pass

        score_elem = elem.find("div", class_="score")
        if score_elem and score_elem.get_text(strip=True):
            text = score_elem.get_text(strip=True)
            if text in ("•", "score hidden"):
                return None
            try:
                return int(text)
            except ValueError:
                pass

        tagline = elem.find("p", class_="tagline")
        if tagline:
            for span in tagline.find_all("span", class_="unvoted"):
                text = span.get_text(strip=True)
                match = re.search(r"(-?\d+)\s+point", text)
                if match:
                    return int(match.group(1))
        return None

    def _extract_body(self, elem: Tag) -> str:
        body_elem = elem.find("div", class_="usertext-body")
        if body_elem:
            return body_elem.get_text("\n", strip=True)
        return ""

    def _extract_created_at(self, elem: Tag) -> Optional[dt.datetime]:
        timestamp = elem.get("data-timestamp")
        if timestamp:
            try:
                return dt.datetime.fromtimestamp(
                    int(timestamp) / 1000, tz=dt.timezone.utc
                )
            except (TypeError, ValueError):
                pass

        time_elem = elem.find("time")
        if time_elem and time_elem.get("datetime"):
            try:
                parsed = dt.datetime.fromisoformat(time_elem["datetime"])
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=dt.timezone.utc)
                return parsed
            except ValueError:
                pass
        return None

    def _is_self_post(self, elem: Tag) -> bool:
        classes = elem.get("class", [])
        domain = elem.get("data-domain", "")
        return "self" in classes or str(domain).startswith("self.")

    def _should_skip_thing(self, elem: Tag) -> bool:
        classes = elem.get("class", [])
        if "promoted" in classes or "promotedlink" in classes:
            return True
        if "stickied" in classes:
            return True
        if elem.get("data-promoted") == "true":
            return True
        return False

    def _parse_media_urls(self, elem: Tag) -> Optional[List[str]]:
        media_urls: List[str] = []
        thumbnail = elem.find("a", class_="thumbnail")
        if thumbnail and thumbnail.get("href"):
            href = thumbnail["href"]
            if any(
                ext in href.lower()
                for ext in [".jpg", ".jpeg", ".png", ".gif", ".mp4", ".webm"]
            ) or any(domain in href for domain in ["i.redd.it", "v.redd.it"]):
                media_urls.append(href.split("?")[0])

        data_url = elem.get("data-url")
        if data_url:
            if any(
                ext in data_url.lower()
                for ext in [".jpg", ".jpeg", ".png", ".gif", ".mp4", ".webm"]
            ) or any(domain in data_url for domain in ["i.redd.it", "v.redd.it"]):
                clean = urljoin(self.PUBLIC_URL, data_url).split("?")[0]
                if clean not in media_urls:
                    media_urls.append(clean)

        return media_urls or None

    def _parse_thing(self, elem: Tag) -> Optional[RedditContent]:
        if self._should_skip_thing(elem):
            return None

        fullname = elem.get("data-fullname")
        if not fullname:
            return None

        data_type = elem.get("data-type", "")
        classes = elem.get("class", [])
        is_comment = data_type == "comment" or "comment" in classes
        is_post = data_type in ("link", "") or "link" in classes

        if not is_comment and not is_post:
            return None

        permalink = elem.get("data-permalink", "")
        if not permalink:
            title_elem = elem.find("a", class_="title")
            if title_elem and title_elem.get("href"):
                permalink = title_elem["href"]
        if not permalink:
            return None

        username = elem.get("data-author") or DELETED_USER
        if username == "[deleted]":
            username = DELETED_USER

        community = elem.get("data-subreddit-prefixed") or ""
        if community and not community.startswith("r/"):
            subreddit = elem.get("data-subreddit") or community
            community = f"r/{subreddit}"

        created_at = self._extract_created_at(elem)
        if created_at is None:
            return None

        body = self._extract_body(elem)
        title = None
        parent_id = None

        if is_comment:
            if not body:
                return None
            title = None
        else:
            title_elem = elem.find("a", class_="title")
            title = title_elem.get_text(strip=True) if title_elem else ""
            if self._is_self_post(elem) and not body:
                body = ""

        score = self._extract_score(elem)
        num_comments = None
        if not is_comment:
            comments_count = elem.get("data-comments-count")
            if comments_count is not None:
                try:
                    num_comments = int(comments_count)
                except ValueError:
                    num_comments = None

        is_nsfw = elem.get("data-nsfw") == "true"
        media = self._parse_media_urls(elem) if not is_comment else None

        return RedditContent(
            id=fullname,
            url=self._public_url(permalink),
            username=username,
            communityName=community,
            body=body,
            createdAt=created_at,
            dataType=RedditDataType.COMMENT if is_comment else RedditDataType.POST,
            title=title,
            parentId=parent_id,
            media=media,
            is_nsfw=is_nsfw,
            score=score,
            upvote_ratio=None,
            num_comments=num_comments,
            scrapedAt=dt.datetime.now(dt.timezone.utc),
        )

    async def _fetch_soup_with_session(
        self, session: aiohttp.ClientSession, url: str
    ) -> Tuple[Optional[BeautifulSoup], bool]:
        proxy_failed = False
        for attempt in range(self.MAX_RETRIES):
            try:
                async with session.get(url, timeout=self.REQUEST_TIMEOUT) as response:
                    if response.status == 429:
                        retry_after = int(
                            response.headers.get("Retry-After", self.RETRY_DELAY)
                        )
                        bt.logging.warning(
                            f"Rate limited, waiting {retry_after}s before retry..."
                            f"{self._proxy_error_context()}"
                        )
                        await asyncio.sleep(retry_after)
                        continue

                    if response.status != 200:
                        bt.logging.warning(
                            f"Got status {response.status} from {url}"
                            f"{self._proxy_error_context()}"
                        )
                        if attempt < self.MAX_RETRIES - 1:
                            await asyncio.sleep(self.RETRY_DELAY)
                            continue
                        return None, False

                    html = await response.text()
                    return BeautifulSoup(html, "html.parser"), False

            except asyncio.TimeoutError:
                bt.logging.warning(
                    f"Timeout fetching {url}, attempt {attempt + 1}/{self.MAX_RETRIES}"
                    f"{self._proxy_error_context()}"
                )
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY)
                    continue
            except Exception as e:
                if self._is_proxy_connect_error(e):
                    proxy_failed = True
                bt.logging.error(
                    f"Error fetching {url}: {e}{self._proxy_error_context()}"
                )
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY)
                    continue

        return None, proxy_failed

    async def _fetch_soup(
        self, session: aiohttp.ClientSession, url: str
    ) -> Optional[BeautifulSoup]:
        soup, proxy_failed = await self._fetch_soup_with_session(session, url)
        if soup is not None:
            return soup

        failed_proxy = self._session_proxy
        if proxy_failed and failed_proxy is not None:
            async with self._direct_fallback_session(failed_proxy) as direct_session:
                soup, _ = await self._fetch_soup_with_session(direct_session, url)
                return soup
        return None

    async def _enrich_self_post_body(
        self, session: aiohttp.ClientSession, content: RedditContent
    ) -> RedditContent:
        if content.data_type != RedditDataType.POST or content.body:
            return content

        soup = await self._fetch_soup(session, self._to_scrape_url(content.url))
        if soup is None:
            return content

        for elem in soup.find_all("div", class_="thing"):
            if elem.get("data-fullname") == content.id:
                body = self._extract_body(elem)
                if body:
                    return content.copy(update={"body": body})
                break
        return content

    async def _scrape_listing(
        self,
        session: aiohttp.ClientSession,
        start_url: str,
        limit: int,
        enrich_self_posts: bool = True,
    ) -> List[RedditContent]:
        contents: List[RedditContent] = []
        url = start_url

        while len(contents) < limit:
            soup = await self._fetch_soup(session, url)
            if soup is None:
                break

            for elem in soup.find_all("div", class_="thing"):
                content = self._parse_thing(elem)
                if content:
                    contents.append(content)
                if len(contents) >= limit:
                    break

            next_button = soup.find("a", rel="next")
            if not next_button or len(contents) >= limit:
                break

            url = urljoin(self.BASE_URL, next_button.get("href", ""))
            await asyncio.sleep(self.PAGE_DELAY)

        contents = contents[:limit]

        if enrich_self_posts:
            enriched: List[RedditContent] = []
            for content in contents:
                if content.data_type == RedditDataType.POST and not content.body:
                    content = await self._enrich_self_post_body(session, content)
                    await asyncio.sleep(0.5)
                enriched.append(content)
            return enriched

        return contents

    async def validate(self, entities: List[DataEntity]) -> List[ValidationResult]:
        if not entities:
            return []

        results: List[ValidationResult] = []
        session_proxy = self._pick_proxy_for_session()

        with self._track_proxy_session(session_proxy):
            async with proxy_client_session(
                session_proxy, self.USER_AGENT, self.REQUEST_TIMEOUT
            ) as session:
                for entity in entities:
                    if not is_valid_reddit_url(entity.uri):
                        results.append(
                            ValidationResult(
                                is_valid=False,
                                reason="Invalid URI.",
                                content_size_bytes_validated=entity.content_size_bytes,
                            )
                        )
                        continue

                    try:
                        ent_content = RedditContent.from_data_entity(entity)
                    except Exception:
                        results.append(
                            ValidationResult(
                                is_valid=False,
                                reason="Failed to decode data entity.",
                                content_size_bytes_validated=entity.content_size_bytes,
                            )
                        )
                        continue

                    try:
                        live_content = await self._fetch_content_from_url(
                            session, ent_content.url, ent_content.data_type, ent_content.id
                        )
                    except Exception as e:
                        bt.logging.error(
                            f"Failed to retrieve content for {entity.uri}: {e}"
                            f"{self._proxy_error_context()}"
                        )
                        results.append(
                            ValidationResult(
                                is_valid=False,
                                reason="Failed to retrieve submission/comment from Reddit.",
                                content_size_bytes_validated=entity.content_size_bytes,
                            )
                        )
                        continue

                    if not live_content:
                        results.append(
                            ValidationResult(
                                is_valid=False,
                                reason="Reddit content not found or invalid.",
                                content_size_bytes_validated=entity.content_size_bytes,
                            )
                        )
                        continue

                    results.append(
                        validate_reddit_content(
                            actual_content=live_content,
                            entity_to_validate=entity,
                        )
                    )

        return results

    async def scrape(self, scrape_config: ScrapeConfig) -> List[DataEntity]:
        bt.logging.trace(
            f"Reddit DOM scraper performing scrape with config: {scrape_config}."
        )

        assert (
            not scrape_config.labels or len(scrape_config.labels) <= 1
        ), "Can only scrape 1 subreddit at a time."

        subreddit_name = (
            normalize_label(scrape_config.labels[0]) if scrape_config.labels else "all"
        )

        limit = min(scrape_config.entity_limit or 100, 100)
        sort = self._get_sort_for_date_range(scrape_config.date_range.end)

        contents: List[RedditContent] = []
        session_proxy = self._pick_proxy_for_session()
        try:
            with self._track_proxy_session(session_proxy):
                async with proxy_client_session(
                    session_proxy, self.USER_AGENT, self.REQUEST_TIMEOUT
                ) as session:
                    url = f"{self.BASE_URL}/r/{subreddit_name}/{sort}/"
                    contents = await self._scrape_listing(session, url, limit)
        except Exception:
            bt.logging.error(
                f"Failed to scrape reddit using subreddit {subreddit_name}: "
                f"{traceback.format_exc()}.{self._proxy_error_context()}"
            )
            return []

        filtered_contents = []
        for content in contents:
            if content.is_nsfw and content.media:
                bt.logging.trace(f"Skipping NSFW content with media: {content.url}")
                continue
            filtered_contents.append(content)

        bt.logging.success(
            f"Completed DOM scrape for subreddit {subreddit_name}. Scraped {len(filtered_contents)} items "
            f"(filtered out {len(contents) - len(filtered_contents)} NSFW+media posts)."
        )

        return [
            RedditContent.to_data_entity(content=content)
            for content in filtered_contents
        ]

    async def on_demand_scrape(
        self,
        usernames: List[str] = None,
        subreddit: str = "all",
        keywords: List[str] = None,
        keyword_mode: KeywordMode = "all",
        start_datetime: dt.datetime = None,
        end_datetime: dt.datetime = None,
        limit: int = 100,
    ) -> List[DataEntity]:
        if all(
            param is None
            for param in [usernames, keywords, start_datetime, end_datetime]
        ) and subreddit == "all":
            bt.logging.trace("All search parameters are None, returning empty list")
            return []

        bt.logging.trace(
            f"On-demand DOM scrape with usernames={usernames}, subreddit={subreddit}, "
            f"keywords={keywords}, keyword_mode={keyword_mode}, start={start_datetime}, end={end_datetime}"
        )

        limit = min(limit, 100)
        contents: List[RedditContent] = []
        session_proxy = self._pick_proxy_for_session()

        try:
            with self._track_proxy_session(session_proxy):
                async with proxy_client_session(
                    session_proxy, self.USER_AGENT, self.REQUEST_TIMEOUT
                ) as session:
                    if usernames:
                        bt.logging.info(f"----mode usernames----:")
                        for username in usernames:
                            try:
                                for suffix in ("submitted", "comments"):
                                    user_url = (
                                        f"{self.BASE_URL}/user/{username}/{suffix}/?sort=new"
                                    )
                                    page_contents = await self._scrape_listing(
                                        session, user_url, limit, enrich_self_posts=True
                                    )
                                    for content in page_contents:
                                        if self._matches_criteria(
                                            content,
                                            keywords,
                                            keyword_mode,
                                            start_datetime,
                                            end_datetime,
                                        ):
                                            contents.append(content)
                            except Exception as e:
                                bt.logging.warning(
                                    f"Failed to scrape user '{username}': {e}"
                                    f"{self._proxy_error_context()}"
                                )
                                continue
                    elif subreddit and subreddit != "all":
                        bt.logging.info(f"----mode subreddit----: {subreddit}, {keywords}, {keyword_mode}")
                        if keywords is None:
                            keywords = [subreddit]
                        else:
                            keywords.append(subreddit)
                        for keyword in keywords:
                            subreddit_name = (
                                keyword.removeprefix("r/")
                                if keyword.startswith("r/")
                                else keyword
                            )
                            url = f"{self.BASE_URL}/r/{subreddit_name}/new/"

                            page_contents = await self._scrape_listing_paginated(
                                session,
                                url,
                                limit,
                                keywords,
                                keyword_mode,
                                start_datetime,
                                end_datetime,
                            )
                            contents.extend(page_contents)
                    else:
                        bt.logging.info(f"----mode keywords----:")
                        if keywords:
                            search_query = self._build_search_query(
                                keywords, keyword_mode
                            )
                            url = (
                                f"{self.BASE_URL}/search"
                                f"?q={quote_plus(search_query)}&sort=new"
                            )
                        else:
                            url = f"{self.BASE_URL}/new/"
                        bt.logging.info(f"----url----: {url}")
                        page_contents = await self._scrape_listing_paginated(
                            session,
                            url,
                            limit,
                            keywords,
                            keyword_mode,
                            start_datetime,
                            end_datetime,
                        )
                        contents.extend(page_contents)

                    filtered_contents = []
                    for content in contents:
                        if content.is_nsfw and content.media:
                            continue
                        filtered_contents.append(content)
                        if len(filtered_contents) >= limit:
                            break

                    bt.logging.success(
                        f"On-demand DOM scrape completed. Found {len(filtered_contents)} items"
                    )

                    return [
                        RedditContent.to_data_entity(content=content)
                        for content in filtered_contents[:limit]
                    ]
        except Exception as e:
            bt.logging.error(
                f"Failed to perform on-demand DOM scrape: {e}{self._proxy_error_context()}"
            )
            bt.logging.error(traceback.format_exc())
            return []

    async def _scrape_listing_paginated(
        self,
        session: aiohttp.ClientSession,
        start_url: str,
        limit: int,
        keywords: Optional[List[str]],
        keyword_mode: KeywordMode,
        start_datetime: Optional[dt.datetime],
        end_datetime: Optional[dt.datetime],
    ) -> List[RedditContent]:
        matched: List[RedditContent] = []
        url = start_url

        while len(matched) < limit:
            soup = await self._fetch_soup(session, url)
            if soup is None:
                break

            page_matched: List[RedditContent] = []
            continue_pagination = True
            start = None
            if start_datetime:
                start = start_datetime
                if start.tzinfo is None:
                    start = start.replace(tzinfo=dt.timezone.utc)

            for elem in soup.find_all("div", class_="thing"):
                content = self._parse_thing(elem)
                if content is None or content.created_at is None:
                    continue
                if start and content.created_at < start:
                    continue_pagination = False
                    break
                if self._matches_criteria(
                    content, keywords, keyword_mode, start_datetime, end_datetime
                ):
                    if content.data_type == RedditDataType.POST and not content.body:
                        content = await self._enrich_self_post_body(session, content)
                    page_matched.append(content)

            matched.extend(page_matched)
            if not continue_pagination or len(matched) >= limit:
                break

            next_button = soup.find("a", rel="next")
            if not next_button:
                break

            url = urljoin(self.BASE_URL, next_button.get("href", ""))
            await asyncio.sleep(self.PAGE_DELAY)

        return matched[:limit]

    async def _fetch_content_from_url(
        self,
        session: aiohttp.ClientSession,
        url: str,
        data_type: RedditDataType,
        content_id: str,
    ) -> Optional[RedditContent]:
        scrape_url = self._to_scrape_url(url)

        if data_type == RedditDataType.POST:
            soup = await self._fetch_soup(session, scrape_url)
            if soup is None:
                return None
            for elem in soup.find_all("div", class_="thing"):
                if elem.get("data-fullname") == content_id:
                    content = self._parse_thing(elem)
                    if content and not content.body:
                        body = self._extract_body(elem)
                        if body:
                            content = content.copy(update={"body": body})
                    return content
            return None

        while True:
            soup = await self._fetch_soup(session, scrape_url)
            if soup is None:
                return None

            for elem in soup.find_all("div", class_="thing"):
                if elem.get("data-fullname") == content_id:
                    return self._parse_thing(elem)

            next_button = soup.find("a", rel="next")
            if not next_button:
                break
            scrape_url = urljoin(self.BASE_URL, next_button.get("href", ""))
            await asyncio.sleep(self.PAGE_DELAY)

        return None

    def _build_search_query(
        self, keywords: List[str], keyword_mode: KeywordMode
    ) -> str:
        if keyword_mode == "all":
            return " AND ".join(f'"{keyword}"' for keyword in keywords)
        return " OR ".join(f'"{keyword}"' for keyword in keywords)

    def _matches_criteria(
        self,
        content: RedditContent,
        keywords: List[str] = None,
        keyword_mode: KeywordMode = "all",
        start_datetime: dt.datetime = None,
        end_datetime: dt.datetime = None,
    ) -> bool:
        if start_datetime:
            if start_datetime.tzinfo is None:
                start_datetime = start_datetime.replace(tzinfo=dt.timezone.utc)
            if content.created_at < start_datetime:
                return False

        if end_datetime:
            if end_datetime.tzinfo is None:
                end_datetime = end_datetime.replace(tzinfo=dt.timezone.utc)
            if content.created_at > end_datetime:
                return False

        if keywords:
            post_community = content.community
            if post_community:
                post_community = post_community.lower().removeprefix("r/")
                subreddit_match = any(
                    keyword.lower().removeprefix("r/") == post_community
                    for keyword in keywords
                )
            else:
                subreddit_match = False

            if subreddit_match == True:
                # bt.logging.info(f"----subreddit match----: {keywords}, {post_community}")
                return True

            searchable_text = ""
            if content.title:
                searchable_text += content.title.lower() + " "
            if content.body:
                searchable_text += content.body.lower()

            if keyword_mode == "all":
                if not all(keyword.lower() in searchable_text for keyword in keywords):
                    return False
            else:
                if not any(keyword.lower() in searchable_text for keyword in keywords):
                    return False

        return True

    def _get_sort_for_date_range(self, end_date: dt.datetime) -> str:
        now = dt.datetime.now(tz=dt.timezone.utc)
        days_ago = (now - end_date).days
        if days_ago <= 1:
            return "new"
        return "top"


async def test_scrape():
    scraper = RedditDomScraper()
    entities = await scraper.scrape(
        ScrapeConfig(
            entity_limit=5,
            date_range=DateRange(
                start=dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=1),
                end=dt.datetime.now(tz=dt.timezone.utc),
            ),
            labels=[DataLabel(value="r/python")],
        )
    )
    print(f"Scraped r/python: {len(entities)} entities")
    if entities:
        print(f"Sample URI: {entities[0].uri}")


async def test_on_demand_scrape():
    scraper = RedditDomScraper()
    entities = await scraper.on_demand_scrape(subreddit="r/python", limit=5)
    print(f"On-demand r/python: {len(entities)} entities")
    if entities:
        print(f"Sample URI: {entities[0].uri}")


if __name__ == "__main__":
    asyncio.run(test_scrape())
    asyncio.run(test_on_demand_scrape())
