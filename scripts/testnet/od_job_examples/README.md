# On-Demand Job File Examples

`Create from External File` accepts **only** a JSON array of on-demand job definitions
matching the constellation OD API (`OnDemandJobPayloadX` / `OnDemandJobPayloadReddit`).

## File format

Each array element is one job:

```json
{
  "job": {
    "platform": "reddit",
    "subreddit": null,
    "usernames": null,
    "keywords": ["todayilearned"]
  },
  "start_date": "2026-05-22T00:00:00+00:00",
  "end_date": "2026-05-26T00:00:00+00:00",
  "limit": 3,
  "keyword_mode": "any",
  "ttl_minutes": 2
}
```

### Reddit job (`platform: "reddit"`)

Required: at least one of `subreddit`, `keywords`, or `usernames`.

```json
{
  "job": {
    "platform": "reddit",
    "keywords": ["todayilearned"]
  },
  "limit": 3,
  "keyword_mode": "any",
  "ttl_minutes": 2
}
```

### X job (`platform: "x"`)

Required: at least one of `keywords`, `usernames`, or `url`.

```json
{
  "job": {
    "platform": "x",
    "usernames": ["eliz883"]
  },
  "limit": 100,
  "keyword_mode": "any",
  "ttl_minutes": 2
}
```

### Optional fields

| Field | Default if omitted |
|-------|-------------------|
| `start_date` | 7 days ago (UTC) |
| `end_date` | now (UTC) |
| `limit` | 50 |
| `keyword_mode` | `any` |
| `ttl_minutes` | 30 |

`keywords` max 5 per job. `ttl_minutes` min 1 (local testnet).

## Example files

| File | Contents |
|------|----------|
| `example_od_jobs.json` | Mixed Reddit + X (matches production-style jobs) |
| `example_reddit_jobs.json` | Reddit-only |
| `example_x_jobs.json` | X-only |

## Dashboard usage

1. **On-Demand Jobs** → **Create from External File**
2. Path example:
   ```
   /mnt/kdu_work/bittensor/data-universe/scripts/testnet/od_job_examples/example_od_jobs.json
   ```
3. **Use example file** or **Preview Jobs**
4. Set **Start index** / **Job count**, then **Create Jobs from File**

For **Automatic OD Jobs** with keyword source = External label file, the same JSON path is used;
the scheduler rotates through entries using each job's own parameters.
