# Testnet Validator Dashboard

Real-time web UI for monitoring miner evaluation on testnet without access to the remote Macrocosmos S3 API.

## Architecture

```
┌─────────────────┐     P2P (dendrite)      ┌──────────────┐
│   Validator     │ ───────────────────────▶│    Miner     │
│  (testnet)      │                           │  (testnet)   │
└────────┬────────┘                           └──────┬───────┘
         │                                           │
         │  local API (:8100)                      │ --s3_auth_url
         ▼                                           ▼
┌─────────────────────────────────────────────────────────────┐
│              Local Data Universe API                         │
│  Replaces S3 presigned URLs with local filesystem storage   │
│  - Parquet uploads (S3 validation)                          │
│  - On-demand job queue + submissions                        │
└─────────────────────────────────────────────────────────────┘
         │
         │  SSE events
         ▼
┌─────────────────┐
│   Dashboard     │  http://localhost:8080/dashboard/
│   Web UI        │
└─────────────────┘
```

## Quick Start

### 1. Start validator with dashboard

```bash
python scripts/start_testnet_dashboard.py \
  --wallet.name my-wallet \
  --wallet.hotkey my-hotkey \
  --subtensor.network test \
  --netuid 254
```

### 2. Start miner pointing at local API

```bash
python -m neurons.miner \
  --wallet.name miner-wallet \
  --wallet.hotkey miner-hotkey \
  --subtensor.network test \
  --netuid 254 \
  --s3_auth_url http://localhost:8100
```

### 3. Open dashboard

Navigate to **http://localhost:8080/dashboard/**

## Dashboard Settings

All options are configurable from the web UI without restarting:

| Option | Description |
|--------|-------------|
| Target Miner(s) | Evaluate only selected UIDs (empty = all miners) |
| Skip S3 validation | Disable parquet/S3 validation phase |
| Pause evaluation | Stop the evaluation loop |
| Eval batch size | Miners evaluated per cycle |
| Local API URL | Address of the local Data Universe API |
| Auto OD interval | Minutes between auto-generated on-demand jobs (0 = off) |
| Auto OD platform/keywords | Template for scheduled OD jobs |

## On-Demand Jobs

Without external Constellation API access, create OD jobs via:

1. **Dashboard UI** — "On-Demand Jobs" tab → Create Manual OD Job
2. **Auto scheduler** — set "Auto OD interval" > 0 in settings
3. **Direct API** — `POST http://localhost:8100/on-demand/constellation/jobs`

## Evaluation Events (SSE)

The live feed shows these event types:

- `eval_started` — evaluation begun for a miner
- `eval_p2p_complete` — P2P bucket validation finished
- `eval_s3_complete` — local parquet validation finished
- `eval_od_complete` — on-demand submission validation finished
- `eval_complete` — full cycle done with updated score
- `eval_failed` — validation failed at a specific phase

## Flags Reference

| Flag | Default | Description |
|------|---------|-------------|
| `--neuron.dashboard_on` | false | Enable web dashboard |
| `--neuron.dashboard_port` | 8080 | Dashboard port |
| `--neuron.local_api_on` | false | Start local API server |
| `--neuron.local_api_port` | 8100 | Local API port |
| `--neuron.local_api_data_dir` | `<repo>/local_api_data` | OD jobs + miner submissions |
| `--s3_auth_url` | (remote) | Overridden to localhost when local_api_on |
