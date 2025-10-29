# Changelog

All notable changes to this project will be documented in this file.

## v2.1-pro (2025-10-29)
- ArcGIS Pro pipeline: use temporary FileGDB (`carg_temp.gdb`) for intermediates (no `shape/`).
- Output directory fallback when `output/` is locked (creates `output_YYYYMMDD_HHMMSS`).
- Per-layer field standardization and post-merge standardization; removed global final pass.
- Exclude `Shape_Length`/`Shape_Area` from final shapefiles.
- `CalculateField` set to `PYTHON3` for ArcGIS Pro scripts.
- Domain mappings cached to reduce I/O.
- Safer handling of `Direzione` type conversions.
- Log final output directory in the summary.

## v2.0 (previous)
- Initial ArcGIS Pro compatibility for GPKG and GeoDB scripts.
- Domain mapping and field standardization rules.
- Geometry diagnostics and CSV report.

## v1.0
- First public release
- Documentation