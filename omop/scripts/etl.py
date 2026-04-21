#!/usr/bin/env python3
"""
OMOP CDM ETL Script
====================
Loads raw source data and OMOP vocabulary into DuckDB, maps source codes
to standard concepts via CONCEPT_RELATIONSHIP, and populates CDM tables.

Mapping strategy (in priority order):
  0. PLAIN_TEXT_MAP     (static lookup for plain-text values like Synthea Race/Ethnicity/Gender)
  1. SOURCE_TO_CONCEPT_MAP  (custom mappings in the vocabulary)
  2. CONCEPT table direct lookup by concept_code + vocabulary_id
  3. CONCEPT_RELATIONSHIP 'Maps to' traversal
  4. Mark as unmapped (concept_id = 0)

Synthea compatibility:
  - UUID person/encounter IDs are hash-mapped to stable integers
  - ISO 8601 datetime strings (e.g. 1998-02-02T17:16:12Z) are truncated to dates
  - person_source_value / visit_source_value columns carry the original UUIDs
  - procedure_occurrence CDM table is supported
  - Race/Ethnicity/Gender plain-text strings resolved via PLAIN_TEXT_CONCEPT_MAP
"""

import argparse
import hashlib
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ── CDM table DDL (OMOP v5.4 core clinical tables) ───────────────────────────

CDM_DDL = """
-- Vocabulary tables (loaded from files)
CREATE TABLE IF NOT EXISTS concept (
    concept_id          INTEGER NOT NULL,
    concept_name        VARCHAR NOT NULL,
    domain_id           VARCHAR NOT NULL,
    vocabulary_id       VARCHAR NOT NULL,
    concept_class_id    VARCHAR NOT NULL,
    standard_concept    VARCHAR,
    concept_code        VARCHAR NOT NULL,
    valid_start_date    DATE NOT NULL,
    valid_end_date      DATE NOT NULL,
    invalid_reason      VARCHAR
);

CREATE TABLE IF NOT EXISTS concept_relationship (
    concept_id_1        INTEGER NOT NULL,
    concept_id_2        INTEGER NOT NULL,
    relationship_id     VARCHAR NOT NULL,
    valid_start_date    DATE NOT NULL,
    valid_end_date      DATE NOT NULL,
    invalid_reason      VARCHAR
);

CREATE TABLE IF NOT EXISTS concept_synonym (
    concept_id              INTEGER NOT NULL,
    concept_synonym_name    VARCHAR NOT NULL,
    language_concept_id     INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS vocabulary (
    vocabulary_id           VARCHAR NOT NULL,
    vocabulary_name         VARCHAR NOT NULL,
    vocabulary_reference    VARCHAR,
    vocabulary_version      VARCHAR,
    vocabulary_concept_id   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS source_to_concept_map (
    source_code             VARCHAR NOT NULL,
    source_concept_id       INTEGER NOT NULL,
    source_vocabulary_id    VARCHAR NOT NULL,
    source_code_description VARCHAR,
    target_concept_id       INTEGER NOT NULL,
    target_vocabulary_id    VARCHAR NOT NULL,
    valid_start_date        DATE NOT NULL,
    valid_end_date          DATE NOT NULL,
    invalid_reason          VARCHAR
);

-- CDM clinical tables
CREATE TABLE IF NOT EXISTS person (
    person_id                   BIGINT NOT NULL,
    gender_concept_id           INTEGER NOT NULL,
    year_of_birth               INTEGER NOT NULL,
    month_of_birth              INTEGER,
    day_of_birth                INTEGER,
    birth_datetime              TIMESTAMP,
    race_concept_id             INTEGER NOT NULL,
    ethnicity_concept_id        INTEGER NOT NULL,
    location_id                 BIGINT,
    provider_id                 BIGINT,
    care_site_id                BIGINT,
    person_source_value         VARCHAR,
    gender_source_value         VARCHAR,
    gender_source_concept_id    INTEGER,
    race_source_value           VARCHAR,
    race_source_concept_id      INTEGER,
    ethnicity_source_value      VARCHAR,
    ethnicity_source_concept_id INTEGER
);

CREATE TABLE IF NOT EXISTS condition_occurrence (
    condition_occurrence_id         BIGINT NOT NULL,
    person_id                       BIGINT NOT NULL,
    condition_concept_id            INTEGER NOT NULL,
    condition_start_date            DATE NOT NULL,
    condition_start_datetime        TIMESTAMP,
    condition_end_date              DATE,
    condition_end_datetime          TIMESTAMP,
    condition_type_concept_id       INTEGER NOT NULL,
    condition_status_concept_id     INTEGER,
    stop_reason                     VARCHAR,
    provider_id                     BIGINT,
    visit_occurrence_id             BIGINT,
    visit_detail_id                 BIGINT,
    condition_source_value          VARCHAR,
    condition_source_concept_id     INTEGER,
    condition_status_source_value   VARCHAR
);

CREATE TABLE IF NOT EXISTS drug_exposure (
    drug_exposure_id                BIGINT NOT NULL,
    person_id                       BIGINT NOT NULL,
    drug_concept_id                 INTEGER NOT NULL,
    drug_exposure_start_date        DATE NOT NULL,
    drug_exposure_start_datetime    TIMESTAMP,
    drug_exposure_end_date          DATE,
    drug_exposure_end_datetime      TIMESTAMP,
    verbatim_end_date               DATE,
    drug_type_concept_id            INTEGER NOT NULL,
    stop_reason                     VARCHAR,
    refills                         INTEGER,
    quantity                        DOUBLE,
    days_supply                     INTEGER,
    sig                             VARCHAR,
    route_concept_id                INTEGER,
    lot_number                      VARCHAR,
    provider_id                     BIGINT,
    visit_occurrence_id             BIGINT,
    visit_detail_id                 BIGINT,
    drug_source_value               VARCHAR,
    drug_source_concept_id          INTEGER,
    route_source_value              VARCHAR,
    dose_unit_source_value          VARCHAR
);

CREATE TABLE IF NOT EXISTS measurement (
    measurement_id                  BIGINT NOT NULL,
    person_id                       BIGINT NOT NULL,
    measurement_concept_id          INTEGER NOT NULL,
    measurement_date                DATE NOT NULL,
    measurement_datetime            TIMESTAMP,
    measurement_time                VARCHAR,
    measurement_type_concept_id     INTEGER NOT NULL,
    operator_concept_id             INTEGER,
    value_as_number                 DOUBLE,
    value_as_concept_id             INTEGER,
    unit_concept_id                 INTEGER,
    range_low                       DOUBLE,
    range_high                      DOUBLE,
    provider_id                     BIGINT,
    visit_occurrence_id             BIGINT,
    visit_detail_id                 BIGINT,
    measurement_source_value        VARCHAR,
    measurement_source_concept_id   INTEGER,
    unit_source_value               VARCHAR,
    unit_source_concept_id          INTEGER,
    value_source_value              VARCHAR,
    measurement_event_id            BIGINT,
    meas_event_field_concept_id     INTEGER
);

CREATE TABLE IF NOT EXISTS observation (
    observation_id                  BIGINT NOT NULL,
    person_id                       BIGINT NOT NULL,
    observation_concept_id          INTEGER NOT NULL,
    observation_date                DATE NOT NULL,
    observation_datetime            TIMESTAMP,
    observation_type_concept_id     INTEGER NOT NULL,
    value_as_number                 DOUBLE,
    value_as_string                 VARCHAR,
    value_as_concept_id             INTEGER,
    qualifier_concept_id            INTEGER,
    unit_concept_id                 INTEGER,
    provider_id                     BIGINT,
    visit_occurrence_id             BIGINT,
    visit_detail_id                 BIGINT,
    observation_source_value        VARCHAR,
    observation_source_concept_id   INTEGER,
    unit_source_value               VARCHAR,
    qualifier_source_value          VARCHAR,
    value_source_value              VARCHAR,
    observation_event_id            BIGINT,
    obs_event_field_concept_id      INTEGER
);

CREATE TABLE IF NOT EXISTS visit_occurrence (
    visit_occurrence_id             BIGINT NOT NULL,
    person_id                       BIGINT NOT NULL,
    visit_concept_id                INTEGER NOT NULL,
    visit_start_date                DATE NOT NULL,
    visit_start_datetime            TIMESTAMP,
    visit_end_date                  DATE,
    visit_end_datetime              TIMESTAMP,
    visit_type_concept_id           INTEGER NOT NULL,
    provider_id                     BIGINT,
    care_site_id                    BIGINT,
    visit_source_value              VARCHAR,
    visit_source_concept_id         INTEGER,
    admitted_from_concept_id        INTEGER,
    admitted_from_source_value      VARCHAR,
    discharged_to_concept_id        INTEGER,
    discharged_to_source_value      VARCHAR,
    preceding_visit_occurrence_id   BIGINT
);

CREATE TABLE IF NOT EXISTS procedure_occurrence (
    procedure_occurrence_id         BIGINT NOT NULL,
    person_id                       BIGINT NOT NULL,
    procedure_concept_id            INTEGER NOT NULL,
    procedure_date                  DATE NOT NULL,
    procedure_datetime              TIMESTAMP,
    procedure_end_date              DATE,
    procedure_end_datetime          TIMESTAMP,
    procedure_type_concept_id       INTEGER NOT NULL,
    modifier_concept_id             INTEGER,
    quantity                        INTEGER,
    provider_id                     BIGINT,
    visit_occurrence_id             BIGINT,
    visit_detail_id                 BIGINT,
    procedure_source_value          VARCHAR,
    procedure_source_concept_id     INTEGER,
    modifier_source_value           VARCHAR
);

-- ETL metadata table (internal — not part of CDM spec)
CREATE TABLE IF NOT EXISTS _etl_mapping_log (
    log_id              BIGINT,
    run_timestamp       TIMESTAMP,
    source_table        VARCHAR,
    source_column       VARCHAR,
    source_value        VARCHAR,
    source_vocabulary   VARCHAR,
    mapped_concept_id   INTEGER,
    mapped_concept_name VARCHAR,
    mapped_domain       VARCHAR,
    mapping_method      VARCHAR,   -- 'SOURCE_TO_CONCEPT_MAP' | 'CONCEPT_DIRECT' | 'MAPS_TO' | 'UNMAPPED'
    mapping_confidence  DOUBLE,    -- 1.0 = exact, 0.0 = unmapped
    row_count           BIGINT
);
"""


# ── Vocabulary file names expected in vocabulary_dir ─────────────────────────

VOCAB_FILES = {
    "concept":              "CONCEPT.csv",
    "concept_relationship": "CONCEPT_RELATIONSHIP.csv",
    "concept_synonym":      "CONCEPT_SYNONYM.csv",
    "vocabulary":           "VOCABULARY.csv",
    "source_to_concept_map":"SOURCE_TO_CONCEPT_MAP.csv",
}


# ── UUID ↔ integer helpers ────────────────────────────────────────────────────

def uuid_to_int(uuid_str: str) -> int:
    """
    Deterministically map a UUID string to a positive 64-bit integer.
    Uses the first 15 hex digits of the MD5 hash so the result fits in BIGINT.
    """
    if not uuid_str or uuid_str == "nan":
        return 0
    h = hashlib.md5(uuid_str.encode()).hexdigest()
    return int(h[:15], 16)


def normalise_date(val: str) -> str:
    """
    Accept ISO 8601 datetime strings (e.g. '1998-02-02T17:16:12Z') or plain
    dates ('1998-02-02') and return a plain 'YYYY-MM-DD' string.
    Returns the original value unchanged if it doesn't match either pattern.
    """
    if not val or val == "nan":
        return val
    # Strip time component if present
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", str(val))
    return m.group(1) if m else val


def build_uuid_map(series: pd.Series) -> dict[str, int]:
    """Build a {uuid: integer_id} lookup for all unique non-null values."""
    return {v: uuid_to_int(v) for v in series.dropna().unique()}


# ── Vocabulary loading ────────────────────────────────────────────────────────

def load_vocabulary(con: duckdb.DuckDBPyConnection, vocab_dir: Path) -> None:
    """Load OMOP vocabulary CSV files into DuckDB vocabulary tables.

    Handles Athena format (YYYYMMDD integer dates, tab-delimited) automatically.
    """
    log.info("Loading OMOP vocabulary files from %s", vocab_dir)

    # Tables that have date columns needing potential YYYYMMDD → DATE conversion
    date_col_tables = {
        "concept":               ["valid_start_date", "valid_end_date"],
        "concept_relationship":  ["valid_start_date", "valid_end_date"],
        "source_to_concept_map": ["valid_start_date", "valid_end_date"],
    }

    for table, filename in VOCAB_FILES.items():
        fpath = vocab_dir / filename
        if not fpath.exists():
            log.warning("Vocabulary file not found, skipping: %s", fpath)
            continue
        log.info("  Loading %s → table '%s'", filename, table)

        # Detect Athena YYYYMMDD date format from first data row
        athena_dates = False
        if table in date_col_tables:
            try:
                with open(fpath, encoding="utf-8") as fh:
                    header_cols = fh.readline().strip().split("\t")
                    first_vals  = fh.readline().strip().split("\t")
                row = dict(zip(header_cols, first_vals))
                sample_date = row.get("valid_start_date", "")
                athena_dates = len(sample_date) == 8 and sample_date.isdigit()
            except Exception:
                athena_dates = False

        try:
            # Always use pandas for tables with date columns to handle format safely
            if table in date_col_tables:
                log.info("    Reading via pandas (date conversion: %s) ...",
                         "YYYYMMDD" if athena_dates else "auto")
                # Use chunked reading for large files to avoid OOM
                chunk_size = 500_000
                total_inserted = 0
                reader = pd.read_csv(
                    fpath, sep="\t", dtype=str, low_memory=False,
                    keep_default_na=False, na_values=[""],
                    chunksize=chunk_size
                )
                for chunk in reader:
                    if athena_dates:
                        for dc in date_col_tables[table]:
                            if dc in chunk.columns:
                                chunk[dc] = pd.to_datetime(
                                    chunk[dc], format="%Y%m%d", errors="coerce"
                                ).dt.date
                    con.register("_vocab_staging", chunk)
                    con.execute(f"INSERT INTO {table} SELECT * FROM _vocab_staging")
                    con.unregister("_vocab_staging")
                    total_inserted += len(chunk)
                    if total_inserted % 2_000_000 == 0:
                        log.info("    ... %d rows loaded so far", total_inserted)
            else:
                fpath_str = str(fpath).replace("\\", "/")
                con.execute(f"""
                    INSERT INTO {table}
                    SELECT * FROM read_csv_auto(
                        '{fpath_str}',
                        header=true, sep='\\t',
                        ignore_errors=true, quote=''
                    )
                """)
        except Exception as exc:
            log.error("    Failed to load %s: %s", filename, exc)
            continue

        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        log.info("    Loaded %d rows", count)


# ── Static plain-text → concept_id lookup (Synthea & similar sources) ────────
# Synthea encodes Race, Ethnicity, and Gender as plain English strings rather
# than numeric concept codes.  These never match Athena's concept_code column,
# so we resolve them here before hitting the database.
#
# concept_id values are from the standard OMOP vocabulary:
#   Gender   : 8507 M, 8532 F
#   Race     : 8527 white, 8516 black, 8515 asian, 8657 native, 8522 other
#   Ethnicity: 38003563 hispanic, 38003564 nonhispanic

PLAIN_TEXT_CONCEPT_MAP: dict[tuple[str, str], tuple[int, str, str]] = {
    # (source_value_lower, vocabulary_id) → (concept_id, concept_name, domain_id)

    # Gender
    ("m",           "Gender"): (8507,     "MALE",                              "Gender"),
    ("male",        "Gender"): (8507,     "MALE",                              "Gender"),
    ("f",           "Gender"): (8532,     "FEMALE",                            "Gender"),
    ("female",      "Gender"): (8532,     "FEMALE",                            "Gender"),

    # Race
    ("white",                       "Race"): (8527, "White",                                    "Race"),
    ("black",                       "Race"): (8516, "Black or African American",                "Race"),
    ("black or african american",   "Race"): (8516, "Black or African American",                "Race"),
    ("asian",                       "Race"): (8515, "Asian",                                    "Race"),
    ("native",                      "Race"): (8657, "Native Hawaiian or Other Pacific Islander","Race"),
    ("pacific islander",            "Race"): (8657, "Native Hawaiian or Other Pacific Islander","Race"),
    ("native hawaiian",             "Race"): (8657, "Native Hawaiian or Other Pacific Islander","Race"),
    ("hawaiian",                    "Race"): (8657, "Native Hawaiian or Other Pacific Islander","Race"),
    ("american indian",             "Race"): (8657, "Native Hawaiian or Other Pacific Islander","Race"),
    ("other",                       "Race"): (8522, "Other Race",                               "Race"),
    ("hispanic",                    "Race"): (8522, "Other Race",                               "Race"),

    # Ethnicity
    ("hispanic",        "Ethnicity"): (38003563, "Hispanic or Latino",     "Ethnicity"),
    ("nonhispanic",     "Ethnicity"): (38003564, "Not Hispanic or Latino", "Ethnicity"),
    ("non-hispanic",    "Ethnicity"): (38003564, "Not Hispanic or Latino", "Ethnicity"),
    ("not hispanic",    "Ethnicity"): (38003564, "Not Hispanic or Latino", "Ethnicity"),
    ("not hispanic or latino", "Ethnicity"): (38003564, "Not Hispanic or Latino", "Ethnicity"),
    ("hispanic or latino",     "Ethnicity"): (38003563, "Hispanic or Latino",     "Ethnicity"),
}


def resolve_concept(
    con: duckdb.DuckDBPyConnection,
    source_value: str,
    source_vocabulary: str,
) -> tuple[int, str, str, str, float]:
    """
    Resolve a source code to a standard OMOP concept_id.
    Returns (concept_id, concept_name, domain_id, method, confidence).
    """
    # 0. Plain-text static lookup (handles Synthea Race/Ethnicity/Gender strings)
    key = (source_value.strip().lower(), source_vocabulary)
    if key in PLAIN_TEXT_CONCEPT_MAP:
        cid, cname, domain = PLAIN_TEXT_CONCEPT_MAP[key]
        return cid, cname, domain, "PLAIN_TEXT_MAP", 1.0

    # 1. SOURCE_TO_CONCEPT_MAP
    row = con.execute("""
        SELECT target_concept_id, c.concept_name, c.domain_id
        FROM   source_to_concept_map s
        JOIN   concept c ON c.concept_id = s.target_concept_id
        WHERE  s.source_code = ?
          AND  s.source_vocabulary_id = ?
          AND  s.invalid_reason IS NULL
          AND  c.invalid_reason IS NULL
        LIMIT 1
    """, [source_value, source_vocabulary]).fetchone()
    if row:
        return row[0], row[1], row[2], "SOURCE_TO_CONCEPT_MAP", 1.0

    # 2. Direct concept lookup (standard concept)
    row = con.execute("""
        SELECT concept_id, concept_name, domain_id
        FROM   concept
        WHERE  concept_code    = ?
          AND  vocabulary_id   = ?
          AND  standard_concept = 'S'
          AND  invalid_reason IS NULL
        LIMIT 1
    """, [source_value, source_vocabulary]).fetchone()
    if row:
        return row[0], row[1], row[2], "CONCEPT_DIRECT", 1.0

    # 3. CONCEPT_RELATIONSHIP 'Maps to' traversal
    row = con.execute("""
        SELECT c2.concept_id, c2.concept_name, c2.domain_id
        FROM   concept c1
        JOIN   concept_relationship cr
               ON  cr.concept_id_1   = c1.concept_id
               AND cr.relationship_id = 'Maps to'
               AND cr.invalid_reason IS NULL
        JOIN   concept c2
               ON  c2.concept_id     = cr.concept_id_2
               AND c2.standard_concept = 'S'
               AND c2.invalid_reason IS NULL
        WHERE  c1.concept_code  = ?
          AND  c1.vocabulary_id = ?
          AND  c1.invalid_reason IS NULL
        LIMIT 1
    """, [source_value, source_vocabulary]).fetchone()
    if row:
        return row[0], row[1], row[2], "MAPS_TO", 0.9

    # 4. Unmapped
    return 0, "Unmapped", "Unknown", "UNMAPPED", 0.0


def map_table(
    con: duckdb.DuckDBPyConnection,
    source_df: pd.DataFrame,
    table_config: dict,
    run_ts: datetime,
    log_rows: list,
) -> pd.DataFrame:
    """
    Apply concept mapping to a source DataFrame according to table_config.
    table_config keys:
      source_table, cdm_table, column_mappings: [{source_col, target_col,
        source_vocabulary, is_concept_col}], id_col, date_col, person_id_col
    """
    source_table = table_config["source_table"]
    col_maps     = table_config.get("column_mappings", [])

    # Build a concept cache to avoid repeated DB lookups for the same code
    concept_cache: dict[tuple, tuple] = {}

    for cm in col_maps:
        if not cm.get("is_concept_col", False):
            continue
        src_col  = cm["source_col"]
        vocab_id = cm.get("source_vocabulary", "")
        tgt_col  = cm["target_col"]

        if src_col not in source_df.columns:
            log.warning("Column '%s' not found in source table '%s', skipping", src_col, source_table)
            continue

        concept_ids   = []
        for val in source_df[src_col].fillna("").astype(str):
            key = (val, vocab_id)
            if key not in concept_cache:
                concept_cache[key] = resolve_concept(con, val, vocab_id)
            cid, cname, domain, method, conf = concept_cache[key]
            concept_ids.append(cid)

            # Aggregate into log (count occurrences)
            log_rows.append({
                "run_timestamp":       run_ts,
                "source_table":        source_table,
                "source_column":       src_col,
                "source_value":        val,
                "source_vocabulary":   vocab_id,
                "mapped_concept_id":   cid,
                "mapped_concept_name": cname,
                "mapped_domain":       domain,
                "mapping_method":      method,
                "mapping_confidence":  conf,
                "row_count":           1,
            })

        source_df[tgt_col] = concept_ids

    return source_df


def run_etl(args: argparse.Namespace) -> None:
    raw_dir    = Path(args.raw_data_dir)
    vocab_dir  = Path(args.vocabulary_dir)
    mapping_cfg = json.loads(Path(args.mapping_config).read_text())

    log.info("Connecting to DuckDB: %s", args.output_db)
    con = duckdb.connect(args.output_db)
    con.execute(f"SET memory_limit='{args.duckdb_memory}'")
    con.execute(f"SET threads={args.duckdb_threads}")

    # Create schema
    log.info("Creating CDM schema (OMOP v%s)", args.cdm_version)
    con.execute(CDM_DDL)

    # Load vocabulary
    load_vocabulary(con, vocab_dir)

    run_ts   = datetime.utcnow()
    log_rows = []
    summary  = {"run_timestamp": run_ts.isoformat(), "tables": {}}

    # Process each source table defined in the mapping config
    for tbl_cfg in mapping_cfg.get("tables", []):
        source_table = tbl_cfg["source_table"]
        cdm_table    = tbl_cfg["cdm_table"]
        source_file  = raw_dir / tbl_cfg["source_file"]

        if not source_file.exists():
            log.warning("Source file not found, skipping: %s", source_file)
            continue

        log.info("Processing source table: %s → CDM table: %s", source_table, cdm_table)
        delimiter = tbl_cfg.get("delimiter", args.delimiter)
        df = pd.read_csv(source_file, sep=delimiter, dtype=str, low_memory=False)
        log.info("  Loaded %d rows from %s", len(df), source_file.name)

        # ── Normalise ISO datetime columns to plain dates (before rename) ─────
        date_target_cols = {
            "condition_start_date", "condition_end_date",
            "drug_exposure_start_date", "drug_exposure_end_date",
            "visit_start_date", "visit_end_date",
            "procedure_date", "measurement_date", "observation_date",
            "birth_datetime",
        }
        for cm in tbl_cfg.get("column_mappings", []):
            if cm.get("target_col") in date_target_cols and cm["source_col"] in df.columns:
                df[cm["source_col"]] = df[cm["source_col"]].apply(normalise_date)

        # ── Map concept columns FIRST (uses original source column names) ─────
        df = map_table(con, df, tbl_cfg, run_ts, log_rows)

        # ── Apply column renames ──────────────────────────────────────────────
        rename_map = {
            cm["source_col"]: cm["target_col"]
            for cm in tbl_cfg.get("column_mappings", [])
            if not cm.get("is_concept_col", False) and "target_col" in cm
        }
        df = df.rename(columns=rename_map)

        # ── Resolve UUID person/visit IDs → stable integers ───────────────────
        if "person_source_value" in df.columns:
            uuid_map = build_uuid_map(df["person_source_value"])
            df["person_id"] = df["person_source_value"].map(uuid_map).fillna(0).astype("int64")

        if "visit_source_value" in df.columns and cdm_table == "visit_occurrence":
            uuid_map_v = build_uuid_map(df["visit_source_value"])
            df["visit_occurrence_id"] = df["visit_source_value"].map(uuid_map_v).fillna(0).astype("int64")

        # ── Generate surrogate PKs for tables that need them ──────────────────
        pk_col_map = {
            "condition_occurrence": "condition_occurrence_id",
            "drug_exposure":        "drug_exposure_id",
            "procedure_occurrence": "procedure_occurrence_id",
            "measurement":          "measurement_id",
            "observation":          "observation_id",
        }
        if cdm_table in pk_col_map:
            pk_col = pk_col_map[cdm_table]
            if pk_col not in df.columns:
                offset = con.execute(
                    f"SELECT COALESCE(MAX({pk_col}), 0) FROM {cdm_table}"
                ).fetchone()[0]
                df[pk_col] = range(offset + 1, offset + 1 + len(df))

        # ── person table: derive year/month/day from birth_datetime ───────────
        if cdm_table == "person":
            if "birth_datetime" in df.columns:
                bd = pd.to_datetime(df["birth_datetime"], errors="coerce")
                df["year_of_birth"]  = bd.dt.year.fillna(0).astype("int32")
                df["month_of_birth"] = bd.dt.month.fillna(0).astype("int32")
                df["day_of_birth"]   = bd.dt.day.fillna(0).astype("int32")
            if "person_source_value" in df.columns and "person_id" not in df.columns:
                uuid_map = build_uuid_map(df["person_source_value"])
                df["person_id"] = df["person_source_value"].map(uuid_map).fillna(0).astype("int64")
            for col, default in [
                ("gender_concept_id", 0), ("race_concept_id", 0),
                ("ethnicity_concept_id", 0), ("year_of_birth", 0),
            ]:
                if col not in df.columns:
                    df[col] = default

        # ── Fill required NOT NULL type concept IDs with EHR defaults ─────────
        # OMOP concept 32817 = EHR (type concept used when source doesn't specify)
        # OMOP concept 9202  = Outpatient Visit (default visit type)
        type_concept_defaults = {
            "condition_type_concept_id":    32817,
            "drug_type_concept_id":         32817,
            "procedure_type_concept_id":    32817,
            "measurement_type_concept_id":  32817,
            "observation_type_concept_id":  32817,
            "visit_type_concept_id":        9202,
        }
        for col, default_val in type_concept_defaults.items():
            if col not in df.columns:
                df[col] = default_val
            else:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(default_val).astype("int64")

        # ── Clamp source_concept_id columns to INT32 range ────────────────────
        # Some SNOMED codes exceed INT32 max (2147483647); store 0 for those
        int32_max = 2_147_483_647
        source_concept_cols = [c for c in df.columns if c.endswith("_source_concept_id")]
        for col in source_concept_cols:
            numeric = pd.to_numeric(df[col], errors="coerce").fillna(0)
            df[col] = numeric.where(numeric <= int32_max, 0).astype("int64")

        # Get CDM column list and types from information_schema
        schema_rows = con.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{cdm_table}'
            ORDER BY ordinal_position
        """).fetchall()
        cdm_col_types = {r[0]: r[1] for r in schema_rows}

        # Build a staging DataFrame with only columns present in both df and CDM
        df_insert = df.copy()

        # Cast date columns from string to proper Python date objects
        date_type_cols = {c for c, t in cdm_col_types.items() if "DATE" in t.upper() or "TIMESTAMP" in t.upper()}
        for col in date_type_cols:
            if col in df_insert.columns:
                df_insert[col] = pd.to_datetime(df_insert[col], errors="coerce")

        # Cast integer columns
        int_type_cols = {c for c, t in cdm_col_types.items() if "INT" in t.upper() or "BIGINT" in t.upper()}
        for col in int_type_cols:
            if col in df_insert.columns:
                df_insert[col] = pd.to_numeric(df_insert[col], errors="coerce").fillna(0).astype("int64")

        # Keep only CDM columns; add missing ones as None
        for col in cdm_col_types:
            if col not in df_insert.columns:
                df_insert[col] = None
        df_insert = df_insert[[c for c in cdm_col_types if c in df_insert.columns]]

        try:
            con.register("_staging", df_insert)
            con.execute(f"INSERT INTO {cdm_table} SELECT * FROM _staging")
            con.unregister("_staging")
            row_count = con.execute(f"SELECT COUNT(*) FROM {cdm_table}").fetchone()[0]
            log.info("  Inserted → %s now has %d rows", cdm_table, row_count)
            summary["tables"][cdm_table] = {"source_rows": len(df), "cdm_rows": row_count}
        except Exception as exc:
            log.error("  Failed to insert into %s: %s", cdm_table, exc)
            summary["tables"][cdm_table] = {"error": str(exc)}

    # Write mapping log
    log.info("Writing mapping log → %s", args.mapping_log)
    log_df = pd.DataFrame(log_rows)
    if not log_df.empty:
        # Aggregate duplicate (source_value, source_vocabulary, method) rows
        log_df = (
            log_df.groupby(
                ["source_table", "source_column", "source_value",
                 "source_vocabulary", "mapped_concept_id",
                 "mapped_concept_name", "mapped_domain",
                 "mapping_method", "mapping_confidence"],
                dropna=False,
            )
            .agg(row_count=("row_count", "sum"))
            .reset_index()
        )
        log_df.insert(0, "log_id", range(1, len(log_df) + 1))
        log_df.insert(1, "run_timestamp", run_ts)
        log_df.to_csv(args.mapping_log, sep="\t", index=False)

        # Persist log into DuckDB for downstream queries
        con.register("_log_staging", log_df)
        con.execute("INSERT INTO _etl_mapping_log SELECT * FROM _log_staging")
        con.unregister("_log_staging")

        # Coverage stats
        total   = len(log_df)
        mapped  = len(log_df[log_df["mapped_concept_id"] != 0])
        pct     = round(mapped / total * 100, 2) if total > 0 else 0
        summary["mapping_coverage_pct"] = pct
        log.info("Mapping coverage: %d/%d (%.1f%%)", mapped, total, pct)
        if pct < args.warn_threshold:
            log.warning("Coverage %.1f%% is below threshold %.1f%%", pct, args.warn_threshold)
    else:
        log.warning("No mapping log entries generated — check source files and mapping config")
        summary["mapping_coverage_pct"] = 0.0

    # Write ETL summary
    log.info("Writing ETL summary → %s", args.etl_summary)
    Path(args.etl_summary).write_text(json.dumps(summary, indent=2, default=str))

    con.close()
    log.info("ETL complete. CDM database: %s", args.output_db)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="OMOP CDM ETL using DuckDB")
    p.add_argument("--raw-data-dir",    required=True)
    p.add_argument("--vocabulary-dir",  required=True)
    p.add_argument("--mapping-config",  required=True)
    p.add_argument("--output-db",       required=True)
    p.add_argument("--mapping-log",     required=True)
    p.add_argument("--etl-summary",     required=True)
    p.add_argument("--cdm-version",     default="5.4")
    p.add_argument("--delimiter",       default=",")
    p.add_argument("--duckdb-memory",   default="16GB")
    p.add_argument("--duckdb-threads",  type=int, default=4)
    p.add_argument("--warn-threshold",  type=float, default=80.0)
    p.add_argument("--report-unmapped", action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_etl(args)
