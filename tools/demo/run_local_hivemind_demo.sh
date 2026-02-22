#!/usr/bin/env bash
set -euo pipefail

DEMO_ROOT="${DEMO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/local-demo" && pwd)}"
BIND_HOST="${BIND_HOST:-127.0.0.1}"
HIVEMIND_PORT="${HIVEMIND_PORT:-12020}"
BRAINHOST_PORT="${BRAINHOST_PORT:-12010}"
REGIONHOST_PORT="${REGIONHOST_PORT:-12040}"
REGION_ID="${REGION_ID:-1}"
SHARD_INDEX="${SHARD_INDEX:-0}"
ROUTER_ID="${ROUTER_ID:-demo-router}"
RUN_ENERGY_SCENARIO="${RUN_ENERGY_SCENARIO:-false}"
IO_ADDRESS="${IO_ADDRESS:-}"
IO_ID="${IO_ID:-io-gateway}"
SCENARIO_CREDIT="${SCENARIO_CREDIT:-500}"
SCENARIO_RATE="${SCENARIO_RATE:-3}"
SCENARIO_PLASTICITY_RATE="${SCENARIO_PLASTICITY_RATE:-0.05}"
SCENARIO_PROBABILISTIC="${SCENARIO_PROBABILISTIC:-true}"

usage() {
  echo "Usage: $(basename "$0") [--demo-root PATH] [--bind-host HOST] [--hivemind-port PORT] [--brainhost-port PORT] [--regionhost-port PORT]"
  echo "       [--region-id ID] [--shard-index IDX] [--router-id NAME]"
  echo "       [--run-energy-scenario true|false --io-address host:port --io-id name]"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --demo-root)
      DEMO_ROOT="$2"; shift 2;;
    --bind-host)
      BIND_HOST="$2"; shift 2;;
    --hivemind-port)
      HIVEMIND_PORT="$2"; shift 2;;
    --brainhost-port)
      BRAINHOST_PORT="$2"; shift 2;;
    --regionhost-port)
      REGIONHOST_PORT="$2"; shift 2;;
    --region-id)
      REGION_ID="$2"; shift 2;;
    --shard-index)
      SHARD_INDEX="$2"; shift 2;;
    --router-id)
      ROUTER_ID="$2"; shift 2;;
    --run-energy-scenario)
      RUN_ENERGY_SCENARIO="$2"; shift 2;;
    --io-address)
      IO_ADDRESS="$2"; shift 2;;
    --io-id)
      IO_ID="$2"; shift 2;;
    --scenario-credit)
      SCENARIO_CREDIT="$2"; shift 2;;
    --scenario-rate)
      SCENARIO_RATE="$2"; shift 2;;
    --scenario-plasticity-rate)
      SCENARIO_PLASTICITY_RATE="$2"; shift 2;;
    --scenario-probabilistic)
      SCENARIO_PROBABILISTIC="$2"; shift 2;;
    -h|--help)
      usage; exit 0;;
    *)
      echo "Unknown argument: $1"; usage; exit 1;;
  esac
 done

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
RUN_ROOT="$DEMO_ROOT/$(date +%Y%m%d_%H%M%S)"
ARTIFACT_ROOT="$RUN_ROOT/artifacts"
LOG_ROOT="$RUN_ROOT/logs"

mkdir -p "$ARTIFACT_ROOT" "$LOG_ROOT"

BRAIN_ID=$(python3 - <<'PY'
import uuid
print(uuid.uuid4())
PY
)

HIVE_ADDR="${BIND_HOST}:${HIVEMIND_PORT}"
BRAIN_ADDR="${BIND_HOST}:${BRAINHOST_PORT}"
REGION_ADDR="${BIND_HOST}:${REGIONHOST_PORT}"

printf "Demo root: %s\n" "$RUN_ROOT"
printf "BrainId: %s\n" "$BRAIN_ID"

pkill -f "Nbn.Runtime.HiveMind" 2>/dev/null || true
pkill -f "Nbn.Runtime.RegionHost" 2>/dev/null || true
pkill -f "Nbn.Tools.DemoHost" 2>/dev/null || true

DOTNET_NOLOGO=1 dotnet build "$REPO_ROOT/src/Nbn.Runtime.HiveMind/Nbn.Runtime.HiveMind.csproj" -c Release >/dev/null
DOTNET_NOLOGO=1 dotnet build "$REPO_ROOT/src/Nbn.Runtime.RegionHost/Nbn.Runtime.RegionHost.csproj" -c Release >/dev/null
DOTNET_NOLOGO=1 dotnet build "$REPO_ROOT/tools/Nbn.Tools.DemoHost/Nbn.Tools.DemoHost.csproj" -c Release >/dev/null

HIVE_EXE="$REPO_ROOT/src/Nbn.Runtime.HiveMind/bin/Release/net8.0/Nbn.Runtime.HiveMind"
REGION_EXE="$REPO_ROOT/src/Nbn.Runtime.RegionHost/bin/Release/net8.0/Nbn.Runtime.RegionHost"
DEMO_EXE="$REPO_ROOT/tools/Nbn.Tools.DemoHost/bin/Release/net8.0/Nbn.Tools.DemoHost"

if [[ -x "$DEMO_EXE" ]]; then
  artifact_json=$("$DEMO_EXE" init-artifacts --artifact-root "$ARTIFACT_ROOT" --json | grep -E '^\{.*\}$' | tail -n 1 || true)
else
  artifact_json=$(DOTNET_NOLOGO=1 dotnet run --project "$REPO_ROOT/tools/Nbn.Tools.DemoHost" -c Release --no-build -- init-artifacts --artifact-root "$ARTIFACT_ROOT" --json | grep -E '^\{.*\}$' | tail -n 1 || true)
fi
if [[ -z "$artifact_json" ]]; then
  echo "DemoHost did not return JSON output." >&2
  exit 1
fi

read -r NBN_SHA NBN_SIZE <<<"$(python3 - <<'PY' "$artifact_json"
import json, sys
payload = json.loads(sys.argv[1])
print(payload["nbn_sha256"], payload["nbn_size"])
PY
)"

HIVE_LOG="$LOG_ROOT/hivemind.log"
BRAIN_LOG="$LOG_ROOT/brainhost.log"
REGION_LOG="$LOG_ROOT/regionhost.log"
HIVE_ERR="$LOG_ROOT/hivemind.err.log"
BRAIN_ERR="$LOG_ROOT/brainhost.err.log"
REGION_ERR="$LOG_ROOT/regionhost.err.log"
SCENARIO_LOG="$LOG_ROOT/energy-plasticity-scenario.log"

cleanup() {
  for pid in "${REGION_PID:-}" "${BRAIN_PID:-}" "${HIVE_PID:-}"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT

if [[ -x "$HIVE_EXE" ]]; then
  ("$HIVE_EXE" --bind-host "$BIND_HOST" --port "$HIVEMIND_PORT" >"$HIVE_LOG" 2>"$HIVE_ERR") &
else
  (DOTNET_NOLOGO=1 dotnet run --project "$REPO_ROOT/src/Nbn.Runtime.HiveMind" -c Release --no-build -- --bind-host "$BIND_HOST" --port "$HIVEMIND_PORT" >"$HIVE_LOG" 2>"$HIVE_ERR") &
fi
HIVE_PID=$!

sleep 1

if [[ -x "$DEMO_EXE" ]]; then
  ("$DEMO_EXE" run-brain --bind-host "$BIND_HOST" --port "$BRAINHOST_PORT" --brain-id "$BRAIN_ID" --hivemind-address "$HIVE_ADDR" --hivemind-id "HiveMind" --router-id "$ROUTER_ID" >"$BRAIN_LOG" 2>"$BRAIN_ERR") &
else
  (DOTNET_NOLOGO=1 dotnet run --project "$REPO_ROOT/tools/Nbn.Tools.DemoHost" -c Release --no-build -- run-brain --bind-host "$BIND_HOST" --port "$BRAINHOST_PORT" --brain-id "$BRAIN_ID" --hivemind-address "$HIVE_ADDR" --hivemind-id "HiveMind" --router-id "$ROUTER_ID" >"$BRAIN_LOG" 2>"$BRAIN_ERR") &
fi
BRAIN_PID=$!

sleep 1

if [[ -x "$REGION_EXE" ]]; then
  ("$REGION_EXE" --bind-host "$BIND_HOST" --port "$REGIONHOST_PORT" --brain-id "$BRAIN_ID" --region "$REGION_ID" --neuron-start 0 --neuron-count 1 --shard-index "$SHARD_INDEX" --router-address "$BRAIN_ADDR" --router-id "$ROUTER_ID" --tick-address "$HIVE_ADDR" --tick-id "HiveMind" --nbn-sha256 "$NBN_SHA" --nbn-size "$NBN_SIZE" --artifact-root "$ARTIFACT_ROOT" >"$REGION_LOG" 2>"$REGION_ERR") &
else
  (DOTNET_NOLOGO=1 dotnet run --project "$REPO_ROOT/src/Nbn.Runtime.RegionHost" -c Release --no-build -- --bind-host "$BIND_HOST" --port "$REGIONHOST_PORT" --brain-id "$BRAIN_ID" --region "$REGION_ID" --neuron-start 0 --neuron-count 1 --shard-index "$SHARD_INDEX" --router-address "$BRAIN_ADDR" --router-id "$ROUTER_ID" --tick-address "$HIVE_ADDR" --tick-id "HiveMind" --nbn-sha256 "$NBN_SHA" --nbn-size "$NBN_SIZE" --artifact-root "$ARTIFACT_ROOT" >"$REGION_LOG" 2>"$REGION_ERR") &
fi
REGION_PID=$!

printf "HiveMind: %s (pid %s)\n" "$HIVE_ADDR" "$HIVE_PID"
printf "BrainHost: %s (pid %s)\n" "$BRAIN_ADDR" "$BRAIN_PID"
printf "RegionHost: %s (pid %s)\n" "$REGION_ADDR" "$REGION_PID"
printf "Logs: %s\n" "$LOG_ROOT"

deadline=$((SECONDS+20))
while [[ $SECONDS -lt $deadline ]]; do
  hive_ready=false
  brain_ready=false
  region_ready=false
  [[ -s "$HIVE_LOG" ]] && hive_ready=true
  [[ -s "$BRAIN_LOG" ]] && brain_ready=true
  [[ -s "$REGION_LOG" ]] && region_ready=true
  if $hive_ready && $brain_ready && $region_ready; then
    break
  fi
  sleep 0.25
 done

if [[ "${RUN_ENERGY_SCENARIO,,}" == "true" ]]; then
  if [[ -z "$IO_ADDRESS" ]]; then
    echo "Energy/plasticity scenario skipped: --io-address is required." >&2
  else
    scenario_args=(
      io-scenario
      --io-address "$IO_ADDRESS"
      --io-id "$IO_ID"
      --brain-id "$BRAIN_ID"
      --credit "$SCENARIO_CREDIT"
      --rate "$SCENARIO_RATE"
      --cost-enabled true
      --energy-enabled true
      --plasticity-enabled true
      --plasticity-rate "$SCENARIO_PLASTICITY_RATE"
      --probabilistic "$SCENARIO_PROBABILISTIC"
      --json
    )

    if [[ -x "$DEMO_EXE" ]]; then
      "$DEMO_EXE" "${scenario_args[@]}" | tee "$SCENARIO_LOG"
    else
      DOTNET_NOLOGO=1 dotnet run --project "$REPO_ROOT/tools/Nbn.Tools.DemoHost" -c Release --no-build -- "${scenario_args[@]}" | tee "$SCENARIO_LOG"
    fi
  fi
fi

printf "Press Enter to stop the demo.\n"
read -r
