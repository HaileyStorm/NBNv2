#!/usr/bin/env bash
set -euo pipefail

DEMO_ROOT="${DEMO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/local-demo" && pwd)}"
BIND_HOST="${BIND_HOST:-127.0.0.1}"
HIVEMIND_PORT="${HIVEMIND_PORT:-12020}"
BRAINHOST_PORT="${BRAINHOST_PORT:-12010}"   # legacy compatibility; ignored
REGIONHOST_PORT="${REGIONHOST_PORT:-12040}" # legacy compatibility; used to derive worker base when --worker-port not set
WORKER_PORT="${WORKER_PORT:-}"
WORKER_COUNT="${WORKER_COUNT:-3}"
IO_PORT="${IO_PORT:-12050}"
REPRO_PORT="${REPRO_PORT:-12070}"
OBS_PORT="${OBS_PORT:-12060}"
SETTINGS_PORT="${SETTINGS_PORT:-12010}"

RUN_ENERGY_SCENARIO="${RUN_ENERGY_SCENARIO:-false}"
RUN_REPRO_SCENARIO="${RUN_REPRO_SCENARIO:-false}"
RUN_REPRO_SUITE="${RUN_REPRO_SUITE:-false}"
SCENARIO_CREDIT="${SCENARIO_CREDIT:-500}"
SCENARIO_RATE="${SCENARIO_RATE:-3}"
SCENARIO_PLASTICITY_RATE="${SCENARIO_PLASTICITY_RATE:-0.05}"
SCENARIO_PROBABILISTIC="${SCENARIO_PROBABILISTIC:-true}"
REPRO_SEED="${REPRO_SEED:-12345}"
REPRO_SPAWN_POLICY="${REPRO_SPAWN_POLICY:-never}"
REPRO_STRENGTH_SOURCE="${REPRO_STRENGTH_SOURCE:-base}"
REPRO_CLIENT_PORT="${REPRO_CLIENT_PORT:-12072}"

usage() {
  echo "Usage: $(basename "$0") [--demo-root PATH] [--bind-host HOST]"
  echo "       [--settings-port PORT] [--worker-port PORT] [--worker-count N]"
  echo "       [--hivemind-port PORT] [--io-port PORT] [--repro-port PORT] [--obs-port PORT]"
  echo "       [--run-energy-scenario true|false]"
  echo "       [--run-repro-scenario true|false --repro-seed N --repro-spawn-policy default|never|always --repro-strength-source base|live]"
  echo "       [--run-repro-suite true|false --repro-seed N]"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --demo-root)
      DEMO_ROOT="$2"; shift 2;;
    --bind-host)
      BIND_HOST="$2"; shift 2;;
    --settings-port)
      SETTINGS_PORT="$2"; shift 2;;
    --worker-port)
      WORKER_PORT="$2"; shift 2;;
    --worker-count)
      WORKER_COUNT="$2"; shift 2;;
    --hivemind-port)
      HIVEMIND_PORT="$2"; shift 2;;
    --io-port)
      IO_PORT="$2"; shift 2;;
    --repro-port)
      REPRO_PORT="$2"; shift 2;;
    --obs-port)
      OBS_PORT="$2"; shift 2;;
    --brainhost-port)
      BRAINHOST_PORT="$2"; shift 2;;
    --regionhost-port)
      REGIONHOST_PORT="$2"; shift 2;;
    --run-energy-scenario)
      RUN_ENERGY_SCENARIO="$2"; shift 2;;
    --run-repro-scenario)
      RUN_REPRO_SCENARIO="$2"; shift 2;;
    --run-repro-suite)
      RUN_REPRO_SUITE="$2"; shift 2;;
    --scenario-credit)
      SCENARIO_CREDIT="$2"; shift 2;;
    --scenario-rate)
      SCENARIO_RATE="$2"; shift 2;;
    --scenario-plasticity-rate)
      SCENARIO_PLASTICITY_RATE="$2"; shift 2;;
    --scenario-probabilistic)
      SCENARIO_PROBABILISTIC="$2"; shift 2;;
    --repro-seed)
      REPRO_SEED="$2"; shift 2;;
    --repro-spawn-policy)
      REPRO_SPAWN_POLICY="$2"; shift 2;;
    --repro-strength-source)
      REPRO_STRENGTH_SOURCE="$2"; shift 2;;
    --repro-client-port)
      REPRO_CLIENT_PORT="$2"; shift 2;;
    -h|--help)
      usage; exit 0;;
    *)
      echo "Unknown argument: $1"; usage; exit 1;;
  esac
done

if [[ -z "$WORKER_PORT" ]]; then
  derived_worker_port=$((REGIONHOST_PORT - 100))
  if [[ "$derived_worker_port" -lt 1 ]]; then
    derived_worker_port=1
  fi
  WORKER_PORT="$derived_worker_port"
fi

if [[ "$WORKER_COUNT" -lt 1 ]]; then
  echo "worker-count must be >= 1" >&2
  exit 1
fi

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
RUN_ROOT="$DEMO_ROOT/$(date +%Y%m%d_%H%M%S)"
ARTIFACT_ROOT="$RUN_ROOT/artifacts"
LOG_ROOT="$RUN_ROOT/logs"
SETTINGS_DB="$DEMO_ROOT/settingsmonitor.db"

mkdir -p "$ARTIFACT_ROOT" "$LOG_ROOT"
rm -f "$SETTINGS_DB" "${SETTINGS_DB}-wal" "${SETTINGS_DB}-shm"

HIVE_ADDR="${BIND_HOST}:${HIVEMIND_PORT}"
IO_ADDR="${BIND_HOST}:${IO_PORT}"
REPRO_ADDR="${BIND_HOST}:${REPRO_PORT}"
OBS_ADDR="${BIND_HOST}:${OBS_PORT}"
SETTINGS_ADDR="${BIND_HOST}:${SETTINGS_PORT}"

echo "Demo root: $RUN_ROOT"
echo "Worker-node-first topology enabled."
echo "Legacy BrainHost/RegionHost arguments are ignored."

pkill -f "Nbn.Runtime.SettingsMonitor" 2>/dev/null || true
pkill -f "Nbn.Runtime.WorkerNode" 2>/dev/null || true
pkill -f "Nbn.Runtime.HiveMind" 2>/dev/null || true
pkill -f "Nbn.Runtime.IO" 2>/dev/null || true
pkill -f "Nbn.Runtime.Reproduction" 2>/dev/null || true
pkill -f "Nbn.Runtime.Observability" 2>/dev/null || true
pkill -f "Nbn.Tools.DemoHost" 2>/dev/null || true

DOTNET_NOLOGO=1 dotnet build "$REPO_ROOT/src/Nbn.Runtime.SettingsMonitor/Nbn.Runtime.SettingsMonitor.csproj" -c Release --disable-build-servers >/dev/null
DOTNET_NOLOGO=1 dotnet build "$REPO_ROOT/src/Nbn.Runtime.WorkerNode/Nbn.Runtime.WorkerNode.csproj" -c Release --disable-build-servers >/dev/null
DOTNET_NOLOGO=1 dotnet build "$REPO_ROOT/src/Nbn.Runtime.HiveMind/Nbn.Runtime.HiveMind.csproj" -c Release --disable-build-servers >/dev/null
DOTNET_NOLOGO=1 dotnet build "$REPO_ROOT/src/Nbn.Runtime.IO/Nbn.Runtime.IO.csproj" -c Release --disable-build-servers >/dev/null
DOTNET_NOLOGO=1 dotnet build "$REPO_ROOT/src/Nbn.Runtime.Reproduction/Nbn.Runtime.Reproduction.csproj" -c Release --disable-build-servers >/dev/null
DOTNET_NOLOGO=1 dotnet build "$REPO_ROOT/src/Nbn.Runtime.Observability/Nbn.Runtime.Observability.csproj" -c Release --disable-build-servers >/dev/null
DOTNET_NOLOGO=1 dotnet build "$REPO_ROOT/tools/Nbn.Tools.DemoHost/Nbn.Tools.DemoHost.csproj" -c Release --disable-build-servers >/dev/null

SETTINGS_EXE="$REPO_ROOT/src/Nbn.Runtime.SettingsMonitor/bin/Release/net8.0/Nbn.Runtime.SettingsMonitor"
WORKER_EXE="$REPO_ROOT/src/Nbn.Runtime.WorkerNode/bin/Release/net8.0/Nbn.Runtime.WorkerNode"
HIVE_EXE="$REPO_ROOT/src/Nbn.Runtime.HiveMind/bin/Release/net8.0/Nbn.Runtime.HiveMind"
IO_EXE="$REPO_ROOT/src/Nbn.Runtime.IO/bin/Release/net8.0/Nbn.Runtime.IO"
REPRO_EXE="$REPO_ROOT/src/Nbn.Runtime.Reproduction/bin/Release/net8.0/Nbn.Runtime.Reproduction"
OBS_EXE="$REPO_ROOT/src/Nbn.Runtime.Observability/bin/Release/net8.0/Nbn.Runtime.Observability"
DEMO_EXE="$REPO_ROOT/tools/Nbn.Tools.DemoHost/bin/Release/net8.0/Nbn.Tools.DemoHost"

if [[ -x "$DEMO_EXE" ]]; then
  artifact_json=$("$DEMO_EXE" init-artifacts --artifact-root "$ARTIFACT_ROOT" --json | grep -E '^\{.*\}$' | tail -n 1 || true)
else
  artifact_json=$(DOTNET_NOLOGO=1 dotnet run --project "$REPO_ROOT/tools/Nbn.Tools.DemoHost" -c Release --no-build -- init-artifacts --artifact-root "$ARTIFACT_ROOT" --json | grep -E '^\{.*\}$' | tail -n 1 || true)
fi
if [[ -z "$artifact_json" ]]; then
  echo "DemoHost did not return artifact JSON output." >&2
  exit 1
fi

read -r NBN_SHA NBN_SIZE <<<"$(python3 - <<'PY' "$artifact_json"
import json, sys
payload = json.loads(sys.argv[1])
print(payload["nbn_sha256"], payload["nbn_size"])
PY
)"

SETTINGS_LOG="$LOG_ROOT/settingsmonitor.log"
SETTINGS_ERR="$LOG_ROOT/settingsmonitor.err.log"
HIVE_LOG="$LOG_ROOT/hivemind.log"
HIVE_ERR="$LOG_ROOT/hivemind.err.log"
IO_LOG="$LOG_ROOT/io.log"
IO_ERR="$LOG_ROOT/io.err.log"
REPRO_LOG="$LOG_ROOT/reproduction.log"
REPRO_ERR="$LOG_ROOT/reproduction.err.log"
OBS_LOG="$LOG_ROOT/observability.log"
OBS_ERR="$LOG_ROOT/observability.err.log"
SPAWN_LOG="$LOG_ROOT/spawn.log"
SCENARIO_LOG="$LOG_ROOT/energy-plasticity-scenario.log"
REPRO_SCENARIO_LOG="$LOG_ROOT/repro-scenario.log"
REPRO_SUITE_LOG="$LOG_ROOT/repro-suite.log"

WORKER_LOGS=()
WORKER_ERRS=()
WORKER_PIDS=()

cleanup() {
  for pid in "${WORKER_PIDS[@]:-}" "${OBS_PID:-}" "${REPRO_PID:-}" "${IO_PID:-}" "${HIVE_PID:-}" "${SETTINGS_PID:-}"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT

if [[ -x "$SETTINGS_EXE" ]]; then
  ("$SETTINGS_EXE" --db "$SETTINGS_DB" --bind-host "$BIND_HOST" --port "$SETTINGS_PORT" >"$SETTINGS_LOG" 2>"$SETTINGS_ERR") &
else
  (DOTNET_NOLOGO=1 dotnet run --project "$REPO_ROOT/src/Nbn.Runtime.SettingsMonitor" -c Release --no-build -- --db "$SETTINGS_DB" --bind-host "$BIND_HOST" --port "$SETTINGS_PORT" >"$SETTINGS_LOG" 2>"$SETTINGS_ERR") &
fi
SETTINGS_PID=$!
sleep 1

for ((i=0; i<WORKER_COUNT; i++)); do
  worker_num=$((i + 1))
  worker_port=$((WORKER_PORT + i))
  worker_log="$LOG_ROOT/worker-${worker_num}.log"
  worker_err="$LOG_ROOT/worker-${worker_num}.err.log"
  worker_logical="nbn.worker.${worker_num}"
  worker_root="worker-node-${worker_num}"

  WORKER_LOGS+=("$worker_log")
  WORKER_ERRS+=("$worker_err")

  if [[ -x "$WORKER_EXE" ]]; then
    (NBN_ARTIFACT_ROOT="$ARTIFACT_ROOT" "$WORKER_EXE" \
      --bind-host "$BIND_HOST" --port "$worker_port" \
      --logical-name "$worker_logical" --root-name "$worker_root" \
      --settings-host "$BIND_HOST" --settings-port "$SETTINGS_PORT" --settings-name "SettingsMonitor" \
      --service-roles all \
      --cpu-pct 100 --ram-pct 100 --storage-pct 100 --gpu-pct 100 >"$worker_log" 2>"$worker_err") &
  else
    (NBN_ARTIFACT_ROOT="$ARTIFACT_ROOT" DOTNET_NOLOGO=1 dotnet run --project "$REPO_ROOT/src/Nbn.Runtime.WorkerNode" -c Release --no-build -- \
      --bind-host "$BIND_HOST" --port "$worker_port" \
      --logical-name "$worker_logical" --root-name "$worker_root" \
      --settings-host "$BIND_HOST" --settings-port "$SETTINGS_PORT" --settings-name "SettingsMonitor" \
      --service-roles all \
      --cpu-pct 100 --ram-pct 100 --storage-pct 100 --gpu-pct 100 >"$worker_log" 2>"$worker_err") &
  fi
  WORKER_PIDS+=("$!")
  sleep 0.4
done

if [[ -x "$HIVE_EXE" ]]; then
  ("$HIVE_EXE" --bind-host "$BIND_HOST" --port "$HIVEMIND_PORT" --settings-db "$SETTINGS_DB" --settings-host "$BIND_HOST" --settings-port "$SETTINGS_PORT" --settings-name "SettingsMonitor" >"$HIVE_LOG" 2>"$HIVE_ERR") &
else
  (DOTNET_NOLOGO=1 dotnet run --project "$REPO_ROOT/src/Nbn.Runtime.HiveMind" -c Release --no-build -- --bind-host "$BIND_HOST" --port "$HIVEMIND_PORT" --settings-db "$SETTINGS_DB" --settings-host "$BIND_HOST" --settings-port "$SETTINGS_PORT" --settings-name "SettingsMonitor" >"$HIVE_LOG" 2>"$HIVE_ERR") &
fi
HIVE_PID=$!
sleep 1

if [[ -x "$IO_EXE" ]]; then
  ("$IO_EXE" --bind-host "$BIND_HOST" --port "$IO_PORT" --settings-host "$BIND_HOST" --settings-port "$SETTINGS_PORT" --settings-name "SettingsMonitor" --hivemind-address "$HIVE_ADDR" --hivemind-name "HiveMind" --repro-address "$REPRO_ADDR" --repro-name "ReproductionManager" >"$IO_LOG" 2>"$IO_ERR") &
else
  (DOTNET_NOLOGO=1 dotnet run --project "$REPO_ROOT/src/Nbn.Runtime.IO" -c Release --no-build -- --bind-host "$BIND_HOST" --port "$IO_PORT" --settings-host "$BIND_HOST" --settings-port "$SETTINGS_PORT" --settings-name "SettingsMonitor" --hivemind-address "$HIVE_ADDR" --hivemind-name "HiveMind" --repro-address "$REPRO_ADDR" --repro-name "ReproductionManager" >"$IO_LOG" 2>"$IO_ERR") &
fi
IO_PID=$!
sleep 1

if [[ -x "$REPRO_EXE" ]]; then
  ("$REPRO_EXE" --bind-host "$BIND_HOST" --port "$REPRO_PORT" --settings-host "$BIND_HOST" --settings-port "$SETTINGS_PORT" --settings-name "SettingsMonitor" --io-address "$IO_ADDR" --io-name "io-gateway" >"$REPRO_LOG" 2>"$REPRO_ERR") &
else
  (DOTNET_NOLOGO=1 dotnet run --project "$REPO_ROOT/src/Nbn.Runtime.Reproduction" -c Release --no-build -- --bind-host "$BIND_HOST" --port "$REPRO_PORT" --settings-host "$BIND_HOST" --settings-port "$SETTINGS_PORT" --settings-name "SettingsMonitor" --io-address "$IO_ADDR" --io-name "io-gateway" >"$REPRO_LOG" 2>"$REPRO_ERR") &
fi
REPRO_PID=$!
sleep 1

if [[ -x "$OBS_EXE" ]]; then
  ("$OBS_EXE" --bind-host "$BIND_HOST" --port "$OBS_PORT" --settings-host "$BIND_HOST" --settings-port "$SETTINGS_PORT" --settings-name "SettingsMonitor" --enable-debug --enable-viz >"$OBS_LOG" 2>"$OBS_ERR") &
else
  (DOTNET_NOLOGO=1 dotnet run --project "$REPO_ROOT/src/Nbn.Runtime.Observability" -c Release --no-build -- --bind-host "$BIND_HOST" --port "$OBS_PORT" --settings-host "$BIND_HOST" --settings-port "$SETTINGS_PORT" --settings-name "SettingsMonitor" --enable-debug --enable-viz >"$OBS_LOG" 2>"$OBS_ERR") &
fi
OBS_PID=$!

echo "SettingsMonitor: $SETTINGS_ADDR (pid $SETTINGS_PID)"
for ((i=0; i<WORKER_COUNT; i++)); do
  worker_num=$((i + 1))
  worker_port=$((WORKER_PORT + i))
  echo "WorkerNode $worker_num: ${BIND_HOST}:${worker_port} (pid ${WORKER_PIDS[$i]})"
done
echo "HiveMind: $HIVE_ADDR (pid $HIVE_PID)"
echo "IO Gateway: $IO_ADDR (pid $IO_PID)"
echo "Reproduction: $REPRO_ADDR (pid $REPRO_PID)"
echo "Observability: $OBS_ADDR (pid $OBS_PID)"
echo "Logs: $LOG_ROOT"

deadline=$((SECONDS+30))
while [[ $SECONDS -lt $deadline ]]; do
  settings_ready=false
  workers_ready=true
  hive_ready=false
  io_ready=false
  repro_ready=false
  obs_ready=false

  [[ -s "$SETTINGS_LOG" ]] && settings_ready=true
  [[ -s "$HIVE_LOG" ]] && hive_ready=true
  [[ -s "$IO_LOG" ]] && io_ready=true
  [[ -s "$REPRO_LOG" ]] && repro_ready=true
  [[ -s "$OBS_LOG" ]] && obs_ready=true

  for worker_log in "${WORKER_LOGS[@]}"; do
    if [[ ! -s "$worker_log" ]]; then
      workers_ready=false
      break
    fi
  done

  if $settings_ready && $workers_ready && $hive_ready && $io_ready && $repro_ready && $obs_ready; then
    break
  fi
  sleep 0.25
done

sleep 2

spawn_args=(
  spawn-brain
  --io-address "$IO_ADDR"
  --io-id io-gateway
  --port "$((REPRO_CLIENT_PORT + 2))"
  --nbn-sha256 "$NBN_SHA"
  --nbn-size "$NBN_SIZE"
  --store-uri "$ARTIFACT_ROOT"
  --timeout-seconds 70
  --wait-seconds 30
  --json
)

if [[ -x "$DEMO_EXE" ]]; then
  spawn_output=$("$DEMO_EXE" "${spawn_args[@]}")
else
  spawn_output=$(DOTNET_NOLOGO=1 dotnet run --project "$REPO_ROOT/tools/Nbn.Tools.DemoHost" -c Release --no-build -- "${spawn_args[@]}")
fi
printf "%s\n" "$spawn_output" >"$SPAWN_LOG"

spawn_json=$(printf "%s\n" "$spawn_output" | grep -E '^\{.*\}$' | tail -n 1 || true)
if [[ -z "$spawn_json" ]]; then
  echo "Spawn command did not emit JSON output. See $SPAWN_LOG." >&2
  exit 1
fi

read -r BRAIN_ID REG_STATUS FAIL_REASON FAIL_MESSAGE <<<"$(python3 - <<'PY' "$spawn_json"
import json, sys
payload = json.loads(sys.argv[1])
ack = payload.get("spawn_ack") or {}
print(
    (ack.get("brain_id") or ""),
    (payload.get("registration_status") or ""),
    (payload.get("failure_reason_code") or ""),
    (payload.get("failure_message") or "")
)
PY
)"

if [[ -z "$BRAIN_ID" ]]; then
  echo "Spawn failed: ${FAIL_REASON} ${FAIL_MESSAGE}" >&2
  exit 1
fi

if [[ "$REG_STATUS" != "registered" ]]; then
  echo "Warning: spawned brain $BRAIN_ID but registration status is '$REG_STATUS'. Continuing; check $SPAWN_LOG if follow-on scenarios report brain_not_found." >&2
fi

echo "Spawned brain: $BRAIN_ID"
echo "Spawn JSON: $spawn_json"

if [[ "${RUN_ENERGY_SCENARIO,,}" == "true" ]]; then
  scenario_args=(
    io-scenario
    --io-address "$IO_ADDR"
    --io-id io-gateway
    --port "$((REPRO_CLIENT_PORT - 1))"
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

if [[ "${RUN_REPRO_SCENARIO,,}" == "true" ]]; then
  repro_args=(
    repro-scenario
    --io-address "$IO_ADDR"
    --io-id io-gateway
    --port "$REPRO_CLIENT_PORT"
    --parent-a-sha256 "$NBN_SHA"
    --parent-a-size "$NBN_SIZE"
    --parent-b-sha256 "$NBN_SHA"
    --parent-b-size "$NBN_SIZE"
    --store-uri "$ARTIFACT_ROOT"
    --seed "$REPRO_SEED"
    --spawn-policy "$REPRO_SPAWN_POLICY"
    --strength-source "$REPRO_STRENGTH_SOURCE"
    --json
  )

  if [[ -x "$DEMO_EXE" ]]; then
    "$DEMO_EXE" "${repro_args[@]}" | tee "$REPRO_SCENARIO_LOG"
  else
    DOTNET_NOLOGO=1 dotnet run --project "$REPO_ROOT/tools/Nbn.Tools.DemoHost" -c Release --no-build -- "${repro_args[@]}" | tee "$REPRO_SCENARIO_LOG"
  fi
fi

if [[ "${RUN_REPRO_SUITE,,}" == "true" ]]; then
  suite_args=(
    repro-suite
    --io-address "$IO_ADDR"
    --io-id io-gateway
    --port "$((REPRO_CLIENT_PORT + 1))"
    --parent-a-sha256 "$NBN_SHA"
    --parent-a-size "$NBN_SIZE"
    --store-uri "$ARTIFACT_ROOT"
    --seed "$REPRO_SEED"
    --json
  )

  if [[ -x "$DEMO_EXE" ]]; then
    "$DEMO_EXE" "${suite_args[@]}" | tee "$REPRO_SUITE_LOG"
  else
    DOTNET_NOLOGO=1 dotnet run --project "$REPO_ROOT/tools/Nbn.Tools.DemoHost" -c Release --no-build -- "${suite_args[@]}" | tee "$REPRO_SUITE_LOG"
  fi
fi

printf "Press Enter to stop the demo.\n"
read -r
