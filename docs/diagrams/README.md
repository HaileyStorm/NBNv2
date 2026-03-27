# NBN diagrams

This directory keeps the documentation diagrams as paired SVG source and PNG renders.

Current diagrams:

- `runtime-service-topology`
- `tick-compute-deliver-pipeline`
- `sharding-and-placement`
- `snapshot-and-recovery-lifecycle`
- `reproduction-flow`
- `artifact-store-partial-fetch`

Regenerate them with:

```bash
npm install --prefix docs/branding
python docs/diagrams/generate_diagrams.py
```

PNG rendering reuses `docs/branding/render_png.mjs` so the docs and branding assets stay on the same SVG-to-PNG path.
