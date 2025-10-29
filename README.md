# CARG Data Conversion Tools — GPKG/GeoDB to Shapefile

Version: 2.1 (Pro)

Author: Patrizio Petricca (patrizio.petricca@isprambiente.it)

Python tools for ArcGIS Pro (and legacy ArcMap) that convert CARG-coded GeoPackage (.gpkg) or File Geodatabase (.gdb) into standardized shapefiles.

## What’s New in 2.1 (Pro)

- Intermediate processing in a temporary FileGDB (no more `shape/` folder).
- Automatic fallback when `output/` is locked (e.g., OneDrive): creates `output_YYYYMMDD_HHMMSS`.
- Field standardization applied per-layer and after merge only (no global final pass).
- `Shape_Length`/`Shape_Area` excluded from final shapefiles.
- `CalculateField` uses PYTHON3 (ArcGIS Pro), domain mappings cached, robust handling of the “Direzione” field.

## Requirements

- ArcGIS Pro 3.x with ArcPy (recommended for the PRO scripts)
- ArcMap 10.x (legacy scripts available: BDgpkg2shape.py and BDgeoDB2shape.py)

## Input Structure

```
workspace/
  input.gpkg or input.gdb         # CARG GeoPackage or FileGDB
  domini/                         # Domain tables (.dbf)
    d_10_tipo.dbf
    d_11_tipo.dbf
    d_12_tipo.dbf
    d_13_tipo.dbf
    d_19_tipo.dbf
    d_foglio.dbf
    d_st018_line.dbf
    d_st021.dbf
    d_1000_tipo.dbf
    d_2000_SiglaTipo.dbf
    d_t2000_eta.dbf
    d_t3000.dbf
    d_st018_contorno.dbf
    d_st018_affiora.dbf
    d_tipologia.dbf
    d_stato.dbf
    d_fase.dbf
    d_verso.dbf
    d_asimmetria.dbf
```

## Supported Layers (CARG)

| CARG Layer   | Output Shapefile              | Notes                                   |
|--------------|-------------------------------|-----------------------------------------|
| ST010Point   | geomorfologia_punti.shp       | Geomorphology points                    |
| ST011Polygon | geomorfologia_poligoni.shp    | Geomorphology polygons                  |
| ST012Polyline| geomorfologia_linee.shp       | Geomorphology lines                     |
| ST013Point   | risorse_prospezioni.shp       | Resources and prospecting points        |
| ST018Polyline| geologia_linee.shp            | Geology lines                           |
| ST018Polygon | geologia_poligoni.shp         | Geology polygons                        |
| ST019Point   | geologia_punti.shp            | Geology point measurements              |
| ST021Polyline| merged into geologia_linee.shp| Folds lines appended to geology lines   |

## Usage (ArcGIS Pro)

1. Add a Script Tool and select:
   - BDgpkg2shapePRO.py for .gpkg inputs
   - BDgeoDB2shapePRO.py for .gdb inputs
2. Set parameter 0 to the input file path (.gpkg or .gdb)

Programmatic example:

```python
from BDgpkg2shapePRO import main   # for .gpkg (ArcGIS Pro)
from BDgeoDB2shapePRO import main  # for .gdb  (ArcGIS Pro)

import arcpy
arcpy.SetParameterAsText(0, r"C:\path\to\your\data.gpkg")
main()
```

Or using the class API:

```python
from BDgpkg2shapePRO import CARGProcessor
processor = CARGProcessor(r"C:\path\to\your\data.gpkg")
processor.process_all_optimized()
```

## Output

Generated into `output/` (if not writable, a fallback `output_YYYYMMDD_HHMMSS` is used):

- geomorfologia_punti.shp
- geomorfologia_linee.shp
- geomorfologia_poligoni.shp
- geologia_punti.shp
- geologia_linee.shp (includes folds)
- geologia_poligoni.shp
- risorse_prospezioni.shp

Quality report:

- `F[FOGLIO]_geometry_issues.csv` (geometry check summary)

## Technical Notes

- Domains mapped from `domini/*.dbf`; mappings cached for performance.
- Per-layer field standardization plus a final pass after lines merge.
- Temporary FileGDB used for intermediate steps (faster and safer than shapefiles).

## Known Notes

1. Compatibility: PRO scripts target ArcGIS Pro (Python 3). ArcMap scripts remain as legacy.
2. Spatial reference is preserved from the input.
3. Ensure the `domini/` folder is present and complete.

