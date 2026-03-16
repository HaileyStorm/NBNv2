# NBN Branding

This folder now keeps only the two retained identity candidates and the reproducible asset generator that emits the canonical SVG, PNG, and ICO files.

## Adopted mark

`Soft + Gold Right N` is the adopted primary NBN mark.

![Soft + Gold Right N](png/nbn-soft-gold-right-n-logo.png)

- [Icon SVG](svg/nbn-soft-gold-right-n-icon.svg)
- [Icon PNG](png/nbn-soft-gold-right-n-icon.png)
- [Icon ICO](ico/nbn-soft-gold-right-n-icon.ico)
- [Logo SVG](svg/nbn-soft-gold-right-n-logo.svg)
- [Logo PNG](png/nbn-soft-gold-right-n-logo.png)

## Secondary candidate

`Diamond Wide Gate` remains the secondary candidate and now uses the same adopted wordmark treatment.

![Diamond Wide Gate](png/nbn-diamond-wide-gate-logo.png)

- [Icon SVG](svg/nbn-diamond-wide-gate-icon.svg)
- [Icon PNG](png/nbn-diamond-wide-gate-icon.png)
- [Icon ICO](ico/nbn-diamond-wide-gate-icon.ico)
- [Logo SVG](svg/nbn-diamond-wide-gate-logo.svg)
- [Logo PNG](png/nbn-diamond-wide-gate-logo.png)

## Usage

- Primary logo: repo README, docs entrypoints, and Workbench Orchestrator.
- Primary icon: .NET project icon metadata and PerfProbe reports.
- Secondary mark: kept only as the alternate finalist.

## Regeneration

```powershell
npm install --prefix docs/branding
python docs/branding/generate_assets.py
```
