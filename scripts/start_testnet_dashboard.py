#!/usr/bin/env python3
"""
Start the validator with testnet dashboard and local API enabled.

This launcher enables:
  - Local Data Universe API (replaces remote S3 / Macrocosmos API)
  - Real-time evaluation dashboard web UI
  - On-demand job auto-generation (configurable via dashboard)

Usage:
  python scripts/start_testnet_dashboard.py \\
    --wallet.name my-wallet \\
    --wallet.hotkey my-hotkey \\
    --subtensor.network test \\
    --netuid 254

Then open: http://localhost:8080/dashboard/

Point your miner at the same local API:
  --s3_auth_url http://localhost:8100
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOCAL_API_DATA_DIR = ROOT / "local_api_data"

DEFAULT_ARGS = [
    "--neuron.dashboard_on",
    "--neuron.local_api_on",
    "--wandb.off",
    "--neuron.disable_set_weights",
    f"--neuron.local_api_data_dir={LOCAL_API_DATA_DIR}",
]


def main():
    extra = sys.argv[1:]
    cmd = [
        sys.executable,
        "-m",
        "neurons.validator",
        *DEFAULT_ARGS,
        *extra,
    ]
    print("Starting testnet validator with dashboard...")
    print("  Dashboard:  http://localhost:8080/dashboard/")
    print("  Local API:  http://localhost:8100")
    print(f"  OD storage: {LOCAL_API_DATA_DIR}")
    print("  Miner flag: --s3_auth_url http://localhost:8100")
    print()
    subprocess.run(cmd, cwd=ROOT, check=False)


if __name__ == "__main__":
    main()
