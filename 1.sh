#!/usr/bin/env bash
# Force-stop local testnet validator (dashboard) and miner processes.
#
# Use when Ctrl+Z / Ctrl+C left neurons.miner or neurons.validator running.
#
# Targets:
#   - python3 -m neurons.miner  (testHotkey2, axon 8093, netuid 254)
#   - python3 scripts/start_testnet_dashboard.py / neurons.validator (axon 8092)
#   - listeners on ports 8080 (dashboard), 8100 (local API), 8092, 8093
#
# Usage:
#   ./scripts/kill_testnet_processes.sh
#   ./scripts/kill_testnet_processes.sh --dry-run

set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DRY_RUN=0
TESTNET_RPC="${TESTNET_RPC:-wss://lb.nodies.app/v2/bittensor-testnet-archival?apikey=a1dd9b8e-ae6e-4ef4-b346-13cac5635338}"

if [[ "${1:-}" == "--dry-run" || "${1:-}" == "-n" ]]; then
  DRY_RUN=1
fi

# ---------------------------------------------------------------------------
# Collect PIDs
# ---------------------------------------------------------------------------

declare -A SEEN=()
PIDS=()

add_pid() {
  local pid="$1"
  if [[ -z "$pid" || "$pid" == "$$" || "$pid" == "$PPID" ]]; then
    return
  fi
  if [[ -z "${SEEN[$pid]+x}" ]]; then
    SEEN[$pid]=1
    PIDS+=("$pid")
  fi
}

pids_for_pattern() {
  local pattern="$1"
  pgrep -f "$pattern" 2>/dev/null || true
}

pids_for_port() {
  local port="$1"
  if command -v fuser >/dev/null 2>&1; then
    # fuser prints "8092/tcp: 1234 5678" — extract numbers only
    fuser -n tcp "$port" 2>/dev/null | grep -oE '[0-9]+' || true
  elif command -v lsof >/dev/null 2>&1; then
    lsof -t -iTCP:"$port" -sTCP:LISTEN 2>/dev/null || true
  fi
}

# Miner: neurons.miner (netuid 254 / testHotkey2 / axon 8093)
while read -r pid; do
  [[ -n "$pid" ]] && add_pid "$pid"
done < <(pids_for_pattern "python.*-m neurons\.miner.*(netuid.?254|testHotkey2|8093)")

# Validator launcher + neurons.validator (dashboard testnet, netuid 254 / axon 8092)
while read -r pid; do
  [[ -n "$pid" ]] && add_pid "$pid"
done < <(pids_for_pattern "start_testnet_dashboard\.py")
while read -r pid; do
  [[ -n "$pid" ]] && add_pid "$pid"
done < <(pids_for_pattern "python.*-m neurons\.validator.*(netuid.?254|8092|dashboard_on|local_api_on)")

# Ports used by testnet dashboard / local API / axons
for port in 8092 8093 8080 8100; do
  while read -r pid; do
    [[ -n "$pid" ]] && add_pid "$pid"
  done < <(pids_for_port "$port")
done

# Include child processes (threads/subprocess workers)
if [[ ${#PIDS[@]} -gt 0 ]]; then
  for pid in "${PIDS[@]}"; do
    while read -r child; do
      [[ -n "$child" ]] && add_pid "$child"
    done < <(pgrep -P "$pid" 2>/dev/null || true)
  done
fi

if [[ ${#PIDS[@]} -eq 0 ]]; then
  echo "No matching testnet miner/validator processes found."

  cd ../bt13-data-universe
  source venv/bin/activate
  cd ../data-universe

  # Pass custom RPC as --subtensor.network (not chain_endpoint): bittensor's
  # setup_config overwrites an explicit chain_endpoint when network is also set.
  python3 scripts/start_testnet_dashboard.py \
  --wallet.name testWallet \
  --wallet.hotkey testHotkey \
  --subtensor.network "$TESTNET_RPC" \
  --netuid 254 \
  --axon.port 8092 \
  --vpermit_rao_limit 0 

  exit 0
fi

echo "Found ${#PIDS[@]} process(es) to stop:"
for pid in "${PIDS[@]}"; do
  if [[ -r "/proc/$pid/cmdline" ]]; then
    cmd="$(tr '\0' ' ' < "/proc/$pid/cmdline" | sed 's/ $//')"
    echo "  PID $pid — ${cmd:-?}"
  else
    echo "  PID $pid"
  fi
done

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "(dry-run: no signals sent)"
  exit 0
fi

# ---------------------------------------------------------------------------
# SIGTERM then SIGKILL
# ---------------------------------------------------------------------------

still_alive=()
for pid in "${PIDS[@]}"; do
  if kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid" 2>/dev/null || true
    still_alive+=("$pid")
  fi
done

if [[ ${#still_alive[@]} -gt 0 ]]; then
  sleep 2
fi

for pid in "${still_alive[@]}"; do
  if kill -0 "$pid" 2>/dev/null; then
    echo "PID $pid still running — sending SIGKILL"
    kill -KILL "$pid" 2>/dev/null || true
  fi
done

# Re-check ports
sleep 1
leftover=0
for port in 8092 8093 8080 8100; do
  if pids_for_port "$port" | grep -q .; then
    echo "Warning: port $port may still be in use"
    leftover=1
  fi
done

if [[ "$leftover" -eq 0 ]]; then
  echo "Testnet miner/validator processes stopped."
else
  echo "Done with warnings — re-run or inspect with: ss -lntp | grep -E '8092|8093|8080|8100'"
fi

cd ../bt13-data-universe
source venv/bin/activate
cd ../data-universe

python3 scripts/start_testnet_dashboard.py \
  --wallet.name testWallet \
  --wallet.hotkey testHotkey \
  --subtensor.network "$TESTNET_RPC" \
  --netuid 254 \
  --axon.port 8092 \
  --vpermit_rao_limit 0 
exit 0

