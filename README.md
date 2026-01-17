# NothingButNeurons v2 (NBNv2)

Distributed neural simulation framework based on the NBNv2 design spec in
`NBNv2_Design.md`. This repo includes:

- `src/` runtime services and shared libraries
- `tools/` Workbench (Avalonia UI)
- `tests/` format and simulation tests

The solution file is `NBNv2.sln`.

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
