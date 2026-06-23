import datetime as dt
import unittest

from scraping.reddit.apify_dataset import (
    apify_reddit_item_fields,
    is_raw_apify_reddit_dataset_item,
    reddit_content_from_apify_dataset_item,
    reddit_content_from_cache_payload,
)
from scraping.reddit.model import RedditDataType


RAW_REDDIT_ITEM = {
    "id": "t3_abc123",
    "url": "https://www.reddit.com/r/bittensor_/comments/abc123/test_post/",
    "username": "miner_user",
    "communityName": "r/bittensor_",
    "body": "Hello #bittensor world",
    "createdAt": "2026-06-17T23:56:05+00:00",
    "dataType": "post",
    "title": "Test post",
    "score": 42,
    "num_comments": 3,
    "isNsfw": False,
    "extraApifyField": "ignored",
}

LEGACY_CACHE = {
    "id": "t3_abc123",
    "url": "https://www.reddit.com/r/bittensor_/comments/abc123/test_post/",
    "username": "miner_user",
    "communityName": "r/bittensor_",
    "body": "Hello #bittensor world",
    "createdAt": "2026-06-17T23:56:05+00:00",
    "dataType": "post",
    "title": "Test post",
    "score": 42,
    "num_comments": 3,
    "is_nsfw": False,
    "scrapedAt": "2026-06-18T00:00:00+00:00",
}


class TestRedditApifyDataset(unittest.TestCase):
    def test_is_raw_apify_reddit_dataset_item(self):
        self.assertTrue(is_raw_apify_reddit_dataset_item(RAW_REDDIT_ITEM))
        self.assertFalse(is_raw_apify_reddit_dataset_item({"url": "u"}))

    def test_apify_reddit_item_fields_strips_extras_and_normalizes_nsfw(self):
        fields = apify_reddit_item_fields(RAW_REDDIT_ITEM)
        self.assertNotIn("extraApifyField", fields)
        self.assertNotIn("isNsfw", fields)
        self.assertFalse(fields["is_nsfw"])
        self.assertEqual(fields["communityName"], "r/bittensor_")

    def test_reddit_content_from_apify_dataset_item(self):
        content = reddit_content_from_apify_dataset_item(
            RAW_REDDIT_ITEM,
            scraped_at=dt.datetime(2026, 6, 18, tzinfo=dt.timezone.utc),
        )
        self.assertEqual(content.data_type, RedditDataType.POST)
        self.assertEqual(content.score, 42)

    def test_reddit_content_from_cache_payload_raw(self):
        content = reddit_content_from_cache_payload(RAW_REDDIT_ITEM)
        self.assertIsNotNone(content)
        assert content is not None
        self.assertEqual(content.url, RAW_REDDIT_ITEM["url"])

    def test_reddit_content_from_cache_payload_legacy(self):
        content = reddit_content_from_cache_payload(LEGACY_CACHE)
        self.assertIsNotNone(content)
        assert content is not None
        self.assertEqual(content.num_comments, 3)


if __name__ == "__main__":
    unittest.main()
