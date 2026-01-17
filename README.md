# NothingButNeurons v2 (NBNv2)

Distributed neural simulation framework based on the NBNv2 design spec in
`NBNv2_Design.md`. This repo includes:

- `src/` runtime services and shared libraries
- `tools/` Workbench (Avalonia UI)
- `tests/` format and simulation tests

The solution file is `NBNv2.sln`.

## HiveMind quickstart (CLI)

```bash
dotnet run --project src/Nbn.Runtime.HiveMind -- \
  --bind-host 127.0.0.1 --port 12020 \
  --tick-hz 30 --min-tick-hz 5 \
  --compute-timeout-ms 200 --deliver-timeout-ms 200 \
  --enable-otel --otel-metrics --otel-traces \
  --otel-endpoint http://localhost:4317 \
  --otel-service-name nbn.hivemind
```

OpenTelemetry env vars (alternatives to CLI flags):
- `NBN_HIVE_OTEL_ENABLED`
- `NBN_HIVE_OTEL_METRICS_ENABLED`
- `NBN_HIVE_OTEL_TRACES_ENABLED`
- `NBN_HIVE_OTEL_CONSOLE`
- `NBN_HIVE_OTEL_ENDPOINT` (falls back to `OTEL_EXPORTER_OTLP_ENDPOINT`)
- `NBN_HIVE_OTEL_SERVICE_NAME` (falls back to `OTEL_SERVICE_NAME`)

## RegionHost quickstart (CLI)

Example invocation (replace IDs/ports/sha/size with real values):

```bash
dotnet run --project src/Nbn.Runtime.RegionHost -- \
  --bind-host 127.0.0.1 --port 12040 \
  --brain-id 11111111-2222-3333-4444-555555555555 \
  --region 9 --neuron-start 0 --neuron-count 0 --shard-index 0 \
  --router-address 127.0.0.1:12010 --router-id brain-router \
  --tick-address 127.0.0.1:12000 --tick-id HiveMind \
  --output-address 127.0.0.1:12020 --output-id output-coordinator \
  --nbn-sha256 <nbn_sha256_hex> --nbn-size <nbn_size_bytes> \
  --nbs-sha256 <nbs_sha256_hex> --nbs-size <nbs_size_bytes> \
  --artifact-root <artifact_store_path>
```

Notes:
- `--nbs-*` flags are optional (omit for no snapshot overlays).
- Output region shards (`region 31`) require a valid `--output-*` PID.
- RegionHost registers/unregisters its shard with the HiveMind PID provided via `--tick-*`.
