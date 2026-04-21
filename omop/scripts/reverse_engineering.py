#!/usr/bin/env python3
"""
OMOP CDM Reverse Engineering Script
=====================================
For every standard concept present in the CDM, traces back to:
  - The original raw source value
  - The source table and column it came from
  - The vocabulary and mapping path used
  - The full concept hierarchy (domain → concept class → concept)
  - Any intermediate concepts traversed (e.g., non-standard → standard)

Outputs:
  - audit_trail.tsv   : full lineage table (one row per source_value → concept)
  - concept_map.tsv   : flat reference table (concept_id → all source values)
  - lineage_report.html : interactive explorer with search and drill-down
  - audit_trail.parquet : (optional) Parquet version for downstream analytics
"""

import argparse
import logging
from pathlib import Path

import duckdb
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Queries ───────────────────────────────────────────────────────────────────

Q_AUDIT_TRAIL = """
-- Full lineage: source value → standard concept, with vocabulary path
SELECT
    l.source_table,
    l.source_column,
    l.source_vocabulary                         AS source_vocabulary_id,
    l.source_value,
    l.row_count                                 AS source_occurrence_count,
    l.mapping_method,
    l.mapping_confidence,
    -- Source concept (non-standard, if exists)
    sc.concept_id                               AS source_concept_id,
    sc.concept_name                             AS source_concept_name,
    sc.concept_code                             AS source_concept_code,
    sc.vocabulary_id                            AS source_concept_vocabulary,
    sc.concept_class_id                         AS source_concept_class,
    -- Standard (target) concept
    l.mapped_concept_id                         AS standard_concept_id,
    tc.concept_name                             AS standard_concept_name,
    tc.concept_code                             AS standard_concept_code,
    tc.vocabulary_id                            AS standard_vocabulary_id,
    tc.concept_class_id                         AS standard_concept_class,
    tc.domain_id                                AS standard_domain,
    tc.valid_start_date                         AS concept_valid_start,
    tc.valid_end_date                           AS concept_valid_end,
    tc.invalid_reason                           AS concept_invalid_reason,
    -- Relationship used (if MAPS_TO path)
    cr.relationship_id                          AS mapping_relationship,
    l.run_timestamp
FROM _etl_mapping_log l
-- Join source concept (may not exist for all source codes)
LEFT JOIN concept sc
    ON  sc.concept_code   = l.source_value
    AND sc.vocabulary_id  = l.source_vocabulary
-- Join target/standard concept
LEFT JOIN concept tc
    ON  tc.concept_id = l.mapped_concept_id
-- Join the relationship record used (for MAPS_TO method)
LEFT JOIN concept_relationship cr
    ON  cr.concept_id_1    = sc.concept_id
    AND cr.concept_id_2    = l.mapped_concept_id
    AND cr.relationship_id = 'Maps to'
    AND cr.invalid_reason IS NULL
ORDER BY l.source_table, l.source_column, l.source_value
"""

Q_CONCEPT_MAP = """
-- Flat reference: for each standard concept, list all source values that map to it
SELECT
    tc.concept_id                               AS standard_concept_id,
    tc.concept_name                             AS standard_concept_name,
    tc.domain_id                                AS domain,
    tc.vocabulary_id                            AS vocabulary,
    tc.concept_code                             AS concept_code,
    tc.concept_class_id                         AS concept_class,
    COUNT(DISTINCT l.source_value)              AS distinct_source_values,
    SUM(l.row_count)                            AS total_source_records,
    STRING_AGG(DISTINCT l.source_value, ' | ')  AS source_values,
    STRING_AGG(DISTINCT l.source_table || '.' || l.source_column, ' | ') AS source_locations,
    STRING_AGG(DISTINCT l.mapping_method, ' | ') AS mapping_methods_used
FROM _etl_mapping_log l
JOIN concept tc ON tc.concept_id = l.mapped_concept_id
WHERE l.mapped_concept_id != 0
GROUP BY tc.concept_id, tc.concept_name, tc.domain_id,
         tc.vocabulary_id, tc.concept_code, tc.concept_class_id
ORDER BY total_source_records DESC
"""

Q_DOMAIN_SUMMARY = """
SELECT
    COALESCE(tc.domain_id, 'Unmapped')          AS domain,
    COUNT(DISTINCT l.source_value)              AS unique_source_codes,
    COUNT(DISTINCT l.mapped_concept_id)         AS unique_standard_concepts,
    SUM(l.row_count)                            AS total_records,
    ROUND(AVG(l.mapping_confidence) * 100, 1)  AS avg_confidence_pct
FROM _etl_mapping_log l
LEFT JOIN concept tc ON tc.concept_id = l.mapped_concept_id
GROUP BY COALESCE(tc.domain_id, 'Unmapped')
ORDER BY total_records DESC
"""


def build_lineage_report(
    audit_df: pd.DataFrame,
    concept_map_df: pd.DataFrame,
    domain_summary_df: pd.DataFrame,
) -> str:
    """Build an interactive HTML lineage explorer."""

    try:
        import plotly.express as px
        import plotly.graph_objects as go

        # Domain summary bar chart
        fig_domain = px.bar(
            domain_summary_df,
            x="domain",
            y="total_records",
            color="avg_confidence_pct",
            title="Records by OMOP Domain (colour = avg mapping confidence %)",
            labels={"total_records": "Records", "domain": "Domain"},
            color_continuous_scale="RdYlGn",
            range_color=[0, 100],
        )
        domain_chart_html = fig_domain.to_html(full_html=False, include_plotlyjs=False)

        # Mapping method breakdown
        method_counts = (
            audit_df.groupby("mapping_method")["source_occurrence_count"]
            .sum()
            .reset_index()
        )
        fig_method = px.pie(
            method_counts,
            names="mapping_method",
            values="source_occurrence_count",
            title="Mapping Method Breakdown",
            hole=0.4,
            color_discrete_sequence=["#2ecc71", "#3498db", "#e67e22", "#e74c3c"],
        )
        method_chart_html = fig_method.to_html(full_html=False, include_plotlyjs=False)

    except ImportError:
        domain_chart_html = "<p>Plotly not available for charts.</p>"
        method_chart_html = ""

    # Build searchable audit table (first 2000 rows for HTML performance)
    sample = audit_df.head(2000)
    table_rows = ""
    for _, row in sample.iterrows():
        conf_color = "#2ecc71" if row.get("mapping_confidence", 0) >= 0.9 else (
            "#e67e22" if row.get("mapping_confidence", 0) >= 0.5 else "#e74c3c"
        )
        table_rows += f"""<tr>
            <td>{row.get('source_table','')}.{row.get('source_column','')}</td>
            <td><code>{row.get('source_value','')}</code></td>
            <td>{row.get('source_vocabulary_id','')}</td>
            <td style="color:{conf_color};font-weight:bold">{row.get('mapping_method','')}</td>
            <td>{row.get('standard_concept_id','')}</td>
            <td>{row.get('standard_concept_name','')}</td>
            <td>{row.get('standard_domain','')}</td>
            <td>{row.get('standard_vocabulary_id','')}</td>
            <td>{row.get('source_occurrence_count','')}</td>
        </tr>"""

    total_rows = len(audit_df)
    shown_rows = min(2000, total_rows)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>OMOP CDM Reverse Engineering — Lineage Report</title>
<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
<style>
  body {{ font-family: Arial, sans-serif; margin: 0; background: #f4f6f9; color: #2c3e50; }}
  header {{ background: #1a252f; color: white; padding: 20px 30px; }}
  header h1 {{ margin: 0; font-size: 1.5em; }}
  header p  {{ margin: 4px 0 0; opacity: 0.75; font-size: 0.85em; }}
  .section  {{ padding: 20px 30px; }}
  .charts   {{ display: flex; flex-wrap: wrap; gap: 20px; }}
  .chart    {{ flex: 1; min-width: 400px; background: white;
               border-radius: 8px; padding: 10px; box-shadow: 0 1px 4px rgba(0,0,0,.1); }}
  h2        {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 6px; }}
  input[type=text] {{
    width: 100%; padding: 10px; font-size: 1em; border: 1px solid #bdc3c7;
    border-radius: 4px; margin-bottom: 12px; box-sizing: border-box;
  }}
  table     {{ width: 100%; border-collapse: collapse; font-size: 0.85em; }}
  th        {{ background: #2c3e50; color: white; padding: 8px 10px; text-align: left; }}
  td        {{ padding: 6px 10px; border-bottom: 1px solid #ecf0f1; }}
  tr:hover  {{ background: #eaf4fb; }}
  .badge    {{ display: inline-block; padding: 2px 8px; border-radius: 10px;
               font-size: 0.8em; font-weight: bold; }}
  .note     {{ color: #7f8c8d; font-size: 0.85em; margin-bottom: 8px; }}
</style>
</head>
<body>
<header>
  <h1>OMOP CDM Reverse Engineering — Concept Lineage Report</h1>
  <p>Traces every standard concept back to its original raw source value and mapping path</p>
</header>

<div class="section">
  <h2>Domain &amp; Method Overview</h2>
  <div class="charts">
    <div class="chart">{domain_chart_html}</div>
    <div class="chart">{method_chart_html}</div>
  </div>
</div>

<div class="section">
  <h2>Full Audit Trail
    <span style="font-size:0.7em;font-weight:normal;color:#7f8c8d">
      (showing {shown_rows:,} of {total_rows:,} rows — full data in audit_trail.tsv)
    </span>
  </h2>
  <p class="note">
    Mapping methods:
    <span style="color:#2ecc71">■</span> SOURCE_TO_CONCEPT_MAP (exact custom mapping) &nbsp;
    <span style="color:#3498db">■</span> CONCEPT_DIRECT (standard concept lookup) &nbsp;
    <span style="color:#e67e22">■</span> MAPS_TO (relationship traversal) &nbsp;
    <span style="color:#e74c3c">■</span> UNMAPPED
  </p>
  <input type="text" id="searchBox" onkeyup="filterTable()" placeholder="Search source value, concept name, domain...">
  <table id="auditTable">
    <thead>
      <tr>
        <th>Source Location</th>
        <th>Source Value</th>
        <th>Source Vocabulary</th>
        <th>Mapping Method</th>
        <th>Concept ID</th>
        <th>Standard Concept Name</th>
        <th>Domain</th>
        <th>Vocabulary</th>
        <th>Occurrences</th>
      </tr>
    </thead>
    <tbody id="auditBody">
      {table_rows}
    </tbody>
  </table>
</div>

<script>
function filterTable() {{
  const q = document.getElementById('searchBox').value.toLowerCase();
  const rows = document.getElementById('auditBody').getElementsByTagName('tr');
  for (let r of rows) {{
    r.style.display = r.innerText.toLowerCase().includes(q) ? '' : 'none';
  }}
}}
</script>
</body>
</html>"""
    return html


def run_reverse_engineering(args: argparse.Namespace) -> None:
    log.info("Connecting to CDM DuckDB: %s", args.cdm_db)
    con = duckdb.connect(args.cdm_db, read_only=True)
    con.execute(f"SET memory_limit='{args.duckdb_memory}'")
    con.execute(f"SET threads={args.duckdb_threads}")

    log.info("Building full audit trail...")
    audit_df = con.execute(Q_AUDIT_TRAIL).df()
    log.info("  %d audit trail rows", len(audit_df))

    log.info("Building concept map...")
    concept_map_df = con.execute(Q_CONCEPT_MAP).df()
    log.info("  %d unique standard concepts", len(concept_map_df))

    log.info("Building domain summary...")
    domain_summary_df = con.execute(Q_DOMAIN_SUMMARY).df()

    con.close()

    # Write TSV outputs
    log.info("Writing audit trail → %s", args.audit_trail_tsv)
    audit_df.to_csv(args.audit_trail_tsv, sep="\t", index=False)

    log.info("Writing concept map → %s", args.concept_map_tsv)
    concept_map_df.to_csv(args.concept_map_tsv, sep="\t", index=False)

    # Optional Parquet
    if args.parquet:
        log.info("Writing Parquet audit trail → %s", args.parquet)
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            pq.write_table(pa.Table.from_pandas(audit_df), args.parquet)
        except ImportError:
            log.warning("pyarrow not installed — skipping Parquet output")

    # HTML lineage report
    log.info("Building lineage report → %s", args.lineage_report)
    html = build_lineage_report(audit_df, concept_map_df, domain_summary_df)
    Path(args.lineage_report).write_text(html, encoding="utf-8")

    log.info("Reverse engineering complete.")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="OMOP CDM Reverse Engineering")
    p.add_argument("--cdm-db",           required=True)
    p.add_argument("--mapping-log",      required=True)
    p.add_argument("--audit-trail-tsv",  required=True)
    p.add_argument("--concept-map-tsv",  required=True)
    p.add_argument("--lineage-report",   required=True)
    p.add_argument("--parquet",          default=None)
    p.add_argument("--duckdb-memory",    default="16GB")
    p.add_argument("--duckdb-threads",   type=int, default=4)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_reverse_engineering(args)
