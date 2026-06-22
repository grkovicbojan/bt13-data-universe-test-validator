import datetime as dt
import unittest

from scraping.x.apify_dataset import (
    coerce_scrape_cache_payload_to_apify_item,
    is_raw_apify_x_dataset_item,
    is_xcontent_cache_payload,
    xcontent_cache_payload_to_apify_item,
)


RAW_APIFY_ITEM = {
    "id": "2067402130730705293",
    "url": "https://x.com/subnetradarcom/status/2067402130730705293",
    "text": "hello https://t.co/abc",
    "createdAt": "Thu Jun 18 00:20:53 +0000 2026",
    "author": {"userName": "subnetradarcom", "name": "Subnetradar"},
}

XCONTENT_CACHE_ITEM = {
    "username": "subnetradarcom",
    "text": "hello https://t.co/abc",
    "url": "https://x.com/subnetradarcom/status/2067402130730705293",
    "timestamp": "2026-06-18T00:20:53+00:00",
    "tweet_hashtags": ["#bittensor", "$TAO"],
    "tweet_id": "2067402130730705293",
    "user_verified": False,
    "user_blue_verified": True,
    "like_count": 1,
    "view_count": 191,
}


class TestApifyDataset(unittest.TestCase):
    def test_shape_detection(self):
        self.assertTrue(is_raw_apify_x_dataset_item(RAW_APIFY_ITEM))
        self.assertTrue(is_xcontent_cache_payload(XCONTENT_CACHE_ITEM))
        self.assertFalse(is_xcontent_cache_payload(RAW_APIFY_ITEM))

    def test_coerce_raw_payload_is_identity(self):
        self.assertIs(
            coerce_scrape_cache_payload_to_apify_item(RAW_APIFY_ITEM), RAW_APIFY_ITEM
        )

    def test_xcontent_cache_rebuilds_apify_row(self):
        rebuilt = xcontent_cache_payload_to_apify_item(XCONTENT_CACHE_ITEM)
        self.assertIsNotNone(rebuilt)
        assert rebuilt is not None
        self.assertEqual(rebuilt["url"], XCONTENT_CACHE_ITEM["url"])
        self.assertEqual(rebuilt["author"]["userName"], "subnetradarcom")
        self.assertEqual(rebuilt["author"]["isBlueVerified"], True)
        self.assertEqual(rebuilt["author"]["isVerified"], False)
        self.assertEqual(rebuilt["createdAt"], "Thu Jun 18 00:20:53 +0000 2026")
        self.assertEqual(rebuilt["entities"]["hashtags"][0]["text"], "bittensor")
        self.assertEqual(rebuilt["entities"]["symbols"][0]["text"], "TAO")

    def test_coerce_xcontent_cache_payload(self):
        coerced = coerce_scrape_cache_payload_to_apify_item(XCONTENT_CACHE_ITEM)
        self.assertIsNotNone(coerced)
        assert coerced is not None
        self.assertTrue(is_raw_apify_x_dataset_item(coerced))


if __name__ == "__main__":
    unittest.main()
