# OMOP CDM Standardization Pipeline

A Nextflow pipeline that transforms raw clinical/claims data into
[OMOP Common Data Model v5.4](https://ohdsi.github.io/CommonDataModel/)
using **DuckDB** as the local database engine.

## Pipeline Overview

```
Raw CSVs + OMOP Vocabulary
        │
        ▼
┌─────────────────┐
│   1. ETL        │  Map source codes → standard concepts (DuckDB)
│   OMOP_ETL      │  Outputs: omop_cdm.duckdb, mapping_log.tsv
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
┌──────────┐  ┌──────────────────────┐
│ 2. Prof- │  │ 3. Reverse Engineer  │
│   iling  │  │   OMOP_REVERSE_ENG.. │
│          │  │                      │
│ Coverage │  │ Concept → source     │
│ charts,  │  │ audit trail, lineage │
│ unmapped │  │ HTML explorer        │
│ codes,   │  │ audit_trail.tsv      │
│ HTML rpt │  │ concept_map.tsv      │
└──────────┘  └──────────────────────┘
         │
         ▼
┌─────────────────┐
│   4. Update     │  Incorporate vocabulary updates incrementally
│   OMOP_UPDATE   │  Outputs: omop_cdm_updated.duckdb, diff report
└─────────────────┘
```

## Modules

| Module | Script | Purpose |
|--------|--------|---------|
| `OMOP_ETL` | `scripts/etl.py` | Load raw data + vocabulary, map source codes to standard concepts, populate CDM tables |
| `OMOP_PROFILING` | `scripts/profiling.py` | Visualize mapping coverage, domain distribution, top unmapped codes |
| `OMOP_REVERSE_ENGINEER` | `scripts/reverse_engineering.py` | Trace every standard concept back to its raw source value with full audit trail |
| `OMOP_UPDATE` | `scripts/update.py` | Incorporate vocabulary updates and new concept IDs incrementally |

## Mapping Strategy

Source codes are resolved to standard OMOP concepts in priority order:

1. **SOURCE_TO_CONCEPT_MAP** — custom mappings in the vocabulary (exact, confidence 1.0)
2. **CONCEPT_DIRECT** — direct lookup by `concept_code` + `vocabulary_id` where `standard_concept = 'S'` (confidence 1.0)
3. **MAPS_TO** — traverse `CONCEPT_RELATIONSHIP` with `relationship_id = 'Maps to'` (confidence 0.9)
4. **UNMAPPED** — `concept_id = 0` (confidence 0.0)

## Quick Start

### Prerequisites

```bash
pip install -r omop/requirements.txt
# Nextflow >= 23.04
# Java 11+
```

### Run with test data

```bash
cd omop
nextflow run main.nf -profile local,test
```

### Run with real data

```bash
nextflow run omop/main.nf \
  --raw_data_dir      /path/to/raw_data/ \
  --vocabulary_dir    /path/to/omop_vocab/ \
  --source_mapping_file omop/test/source_mapping.json \
  --outdir            results/omop \
  -profile            local
```

### With vocabulary update

```bash
nextflow run omop/main.nf \
  --raw_data_dir         /path/to/raw_data/ \
  --vocabulary_dir       /path/to/omop_vocab_v20240101/ \
  --source_mapping_file  omop/test/source_mapping.json \
  --vocabulary_update_dir /path/to/omop_vocab_v20240601/ \
  --outdir               results/omop_updated \
  -profile               local
```

## Inputs

### `--raw_data_dir`
Directory containing raw source CSV/TSV files. File names must match
`source_file` entries in the mapping config.

### `--vocabulary_dir`
Directory containing OMOP vocabulary files downloaded from
[Athena](https://athena.ohdsi.org/):
- `CONCEPT.csv`
- `CONCEPT_RELATIONSHIP.csv`
- `CONCEPT_SYNONYM.csv`
- `VOCABULARY.csv`
- `SOURCE_TO_CONCEPT_MAP.csv`

### `--source_mapping_file`
JSON file describing how each source table maps to a CDM table.
See `test/source_mapping.json` for a full example covering:
- `person` (from patients with gender/race/ethnicity codes)
- `condition_occurrence` (from diagnoses with ICD-10-CM codes)
- `drug_exposure` (from medications with RxNorm codes)
- `measurement` (from lab results with LOINC codes)

## Outputs

```
results/omop/
├── cdm/
│   ├── omop_cdm.duckdb           # Populated CDM database
│   └── omop_cdm_updated.duckdb   # After vocabulary update
├── etl_logs/
│   ├── mapping_log.tsv           # Every source code mapping attempt
│   └── etl_summary.json          # Row counts and coverage %
├── profiling/
│   ├── profiling_report.html     # Interactive mapping report
│   ├── coverage_stats.tsv        # Coverage % per source column
│   ├── unmapped_codes.tsv        # Top-N unmapped source codes
│   └── plots/                    # PNG charts
├── reverse_engineering/
│   ├── lineage_report.html       # Interactive concept lineage explorer
│   ├── audit_trail.tsv           # Full concept → source audit trail
│   ├── audit_trail.parquet       # Parquet version (for analytics)
│   └── concept_map.tsv           # Concept → all source values reference
└── update_logs/
    ├── update_report.html        # Vocabulary diff report
    ├── update_log.tsv            # Changed mappings
    └── update_summary.json       # Update statistics
```

## Key Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `cdm_version` | `5.4` | OMOP CDM version |
| `mapping_warn_threshold` | `80` | Warn if coverage < this % |
| `profiling_top_n` | `20` | Top N unmapped codes to report |
| `audit_parquet` | `true` | Export audit trail as Parquet |
| `update_full_remap` | `false` | Full re-ETL vs incremental update |
| `duckdb_memory` | `16GB` | DuckDB memory limit |
| `duckdb_threads` | `4` | DuckDB thread count |

## Querying the CDM

The output `omop_cdm.duckdb` can be queried directly:

```python
import duckdb
con = duckdb.connect("results/omop/cdm/omop_cdm.duckdb", read_only=True)

# Count conditions by domain
con.execute("""
    SELECT c.concept_name, COUNT(*) AS n
    FROM condition_occurrence co
    JOIN concept c ON c.concept_id = co.condition_concept_id
    GROUP BY c.concept_name
    ORDER BY n DESC
    LIMIT 10
""").df()

# Full audit trail for a specific source code
con.execute("""
    SELECT * FROM _etl_mapping_log
    WHERE source_value = 'E11.9'
""").df()
```
