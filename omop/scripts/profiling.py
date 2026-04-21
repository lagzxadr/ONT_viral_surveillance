#!/usr/bin/env python3
"""
OMOP CDM Data Profiling Script
================================
Queries the CDM DuckDB and the ETL mapping log to produce:
  - Mapping coverage by domain and vocabulary
  - Top-N unmapped source codes
  - CDM table row counts
  - Concept distribution per domain
  - An interactive HTML report (Plotly)
  - Supporting TSV exports
"""

import argparse
import json
import logging
from pathlib import Path

import duckdb
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Queries ───────────────────────────────────────────────────────────────────

Q_COVERAGE_BY_DOMAIN = """
SELECT
    mapped_domain                                       AS domain,
    mapping_method,
    SUM(row_count)                                      AS record_count,
    ROUND(SUM(row_count) * 100.0 /
          SUM(SUM(row_count)) OVER (PARTITION BY mapped_domain), 2) AS pct_of_domain
FROM _etl_mapping_log
GROUP BY mapped_domain, mapping_method
ORDER BY domain, record_count DESC
"""

Q_COVERAGE_BY_TABLE = """
SELECT
    source_table,
    source_column,
    COUNT(DISTINCT source_value)                        AS unique_source_codes,
    SUM(CASE WHEN mapped_concept_id != 0 THEN row_count ELSE 0 END) AS mapped_records,
    SUM(row_count)                                      AS total_records,
    ROUND(SUM(CASE WHEN mapped_concept_id != 0 THEN row_count ELSE 0 END)
          * 100.0 / NULLIF(SUM(row_count), 0), 2)      AS coverage_pct
FROM _etl_mapping_log
GROUP BY source_table, source_column
ORDER BY source_table, source_column
"""

Q_UNMAPPED_TOP_N = """
SELECT
    source_table,
    source_column,
    source_vocabulary,
    source_value,
    SUM(row_count)  AS occurrence_count
FROM _etl_mapping_log
WHERE mapped_concept_id = 0
GROUP BY source_table, source_column, source_vocabulary, source_value
ORDER BY occurrence_count DESC
LIMIT ?
"""

Q_CDM_TABLE_COUNTS = """
SELECT table_name, estimated_size AS row_count
FROM duckdb_tables()
WHERE schema_name = 'main'
  AND table_name NOT LIKE '\\_%' ESCAPE '\\'
ORDER BY table_name
"""

Q_CONCEPT_DISTRIBUTION = """
SELECT
    c.domain_id,
    c.vocabulary_id,
    COUNT(DISTINCT l.mapped_concept_id) AS unique_concepts,
    SUM(l.row_count)                    AS total_records
FROM _etl_mapping_log l
JOIN concept c ON c.concept_id = l.mapped_concept_id
WHERE l.mapped_concept_id != 0
GROUP BY c.domain_id, c.vocabulary_id
ORDER BY total_records DESC
"""

Q_MAPPING_METHOD_SUMMARY = """
SELECT
    mapping_method,
    COUNT(DISTINCT source_value) AS unique_codes,
    SUM(row_count)               AS total_records,
    ROUND(AVG(mapping_confidence) * 100, 1) AS avg_confidence_pct
FROM _etl_mapping_log
GROUP BY mapping_method
ORDER BY total_records DESC
"""


def build_html_report(
    coverage_by_domain: pd.DataFrame,
    coverage_by_table: pd.DataFrame,
    unmapped: pd.DataFrame,
    method_summary: pd.DataFrame,
    concept_dist: pd.DataFrame,
    cdm_counts: pd.DataFrame,
    top_n: int,
) -> str:
    """Build a self-contained interactive HTML report using Plotly."""

    figs = []

    # 1. Mapping method donut
    if not method_summary.empty:
        fig1 = px.pie(
            method_summary,
            names="mapping_method",
            values="total_records",
            title="Mapping Method Distribution",
            hole=0.45,
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig1.update_traces(textinfo="percent+label")
        figs.append(("Mapping Methods", fig1))

    # 2. Coverage % per source table/column — horizontal bar
    if not coverage_by_table.empty:
        fig2 = px.bar(
            coverage_by_table.sort_values("coverage_pct"),
            x="coverage_pct",
            y=coverage_by_table["source_table"] + "." + coverage_by_table["source_column"],
            orientation="h",
            title="Mapping Coverage % by Source Column",
            labels={"x": "Coverage (%)", "y": "Source Column"},
            color="coverage_pct",
            color_continuous_scale="RdYlGn",
            range_color=[0, 100],
        )
        fig2.update_layout(yaxis_title="", xaxis_range=[0, 100])
        figs.append(("Coverage by Column", fig2))

    # 3. Domain distribution stacked bar
    if not coverage_by_domain.empty:
        fig3 = px.bar(
            coverage_by_domain,
            x="domain",
            y="record_count",
            color="mapping_method",
            title="Record Count by Domain and Mapping Method",
            labels={"record_count": "Records", "domain": "OMOP Domain"},
            color_discrete_sequence=px.colors.qualitative.Pastel,
        )
        figs.append(("Domain Distribution", fig3))

    # 4. Top-N unmapped codes table
    if not unmapped.empty:
        fig4 = go.Figure(data=[go.Table(
            header=dict(
                values=["Source Table", "Column", "Vocabulary",
                        "Source Value", "Occurrences"],
                fill_color="#2c3e50",
                font=dict(color="white", size=12),
                align="left",
            ),
            cells=dict(
                values=[
                    unmapped["source_table"],
                    unmapped["source_column"],
                    unmapped["source_vocabulary"],
                    unmapped["source_value"],
                    unmapped["occurrence_count"],
                ],
                fill_color=[["#f9f9f9", "#ffffff"] * (len(unmapped) // 2 + 1)],
                align="left",
            ),
        )])
        fig4.update_layout(title=f"Top {top_n} Unmapped Source Codes")
        figs.append((f"Top {top_n} Unmapped", fig4))

    # 5. Concept vocabulary distribution treemap
    if not concept_dist.empty:
        fig5 = px.treemap(
            concept_dist,
            path=["domain_id", "vocabulary_id"],
            values="total_records",
            title="Mapped Concept Distribution by Domain and Vocabulary",
            color="total_records",
            color_continuous_scale="Blues",
        )
        figs.append(("Concept Distribution", fig5))

    # 6. CDM table row counts
    if not cdm_counts.empty:
        fig6 = px.bar(
            cdm_counts.sort_values("row_count", ascending=False),
            x="table_name",
            y="row_count",
            title="CDM Table Row Counts",
            labels={"row_count": "Rows", "table_name": "CDM Table"},
            color="row_count",
            color_continuous_scale="Teal",
        )
        figs.append(("CDM Table Counts", fig6))

    # Assemble HTML
    tab_buttons = ""
    tab_contents = ""
    for i, (title, fig) in enumerate(figs):
        active = "active" if i == 0 else ""
        display = "block" if i == 0 else "none"
        tab_buttons += (
            f'<button class="tablink {active}" '
            f'onclick="openTab(event,\'tab{i}\')">{title}</button>\n'
        )
        tab_contents += (
            f'<div id="tab{i}" class="tabcontent" style="display:{display}">'
            f'{fig.to_html(full_html=False, include_plotlyjs=False)}'
            f'</div>\n'
        )

    plotly_cdn = (
        '<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>'
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>OMOP CDM Data Profiling Report</title>
{plotly_cdn}
<style>
  body {{ font-family: Arial, sans-serif; margin: 0; background: #f4f6f9; }}
  header {{ background: #2c3e50; color: white; padding: 20px 30px; }}
  header h1 {{ margin: 0; font-size: 1.6em; }}
  header p  {{ margin: 4px 0 0; opacity: 0.8; font-size: 0.9em; }}
  .tab-bar {{ background: #34495e; display: flex; flex-wrap: wrap; padding: 0 20px; }}
  .tablink {{
    background: none; border: none; color: #bdc3c7;
    padding: 14px 18px; cursor: pointer; font-size: 0.95em;
    border-bottom: 3px solid transparent;
  }}
  .tablink:hover, .tablink.active {{
    color: white; border-bottom: 3px solid #3498db;
  }}
  .tabcontent {{ padding: 20px 30px; }}
</style>
</head>
<body>
<header>
  <h1>OMOP CDM Data Profiling Report</h1>
  <p>Generated by the OMOP CDM Standardization Pipeline</p>
</header>
<div class="tab-bar">
{tab_buttons}
</div>
{tab_contents}
<script>
function openTab(evt, tabId) {{
  document.querySelectorAll('.tabcontent').forEach(el => el.style.display = 'none');
  document.querySelectorAll('.tablink').forEach(el => el.classList.remove('active'));
  document.getElementById(tabId).style.display = 'block';
  evt.currentTarget.classList.add('active');
}}
</script>
</body>
</html>"""
    return html


def run_profiling(args: argparse.Namespace) -> None:
    log.info("Connecting to CDM DuckDB: %s", args.cdm_db)
    con = duckdb.connect(args.cdm_db, read_only=True)
    con.execute(f"SET memory_limit='{args.duckdb_memory}'")
    con.execute(f"SET threads={args.duckdb_threads}")

    plots_dir = Path(args.plots_dir)
    plots_dir.mkdir(parents=True, exist_ok=True)

    log.info("Querying coverage by domain...")
    coverage_by_domain = con.execute(Q_COVERAGE_BY_DOMAIN).df()

    log.info("Querying coverage by table/column...")
    coverage_by_table = con.execute(Q_COVERAGE_BY_TABLE).df()

    log.info("Querying top-%d unmapped codes...", args.top_n)
    unmapped = con.execute(Q_UNMAPPED_TOP_N, [args.top_n]).df()

    log.info("Querying CDM table counts...")
    try:
        cdm_counts = con.execute(Q_CDM_TABLE_COUNTS).df()
    except Exception:
        cdm_counts = pd.DataFrame(columns=["table_name", "row_count"])

    log.info("Querying concept distribution...")
    concept_dist = con.execute(Q_CONCEPT_DISTRIBUTION).df()

    log.info("Querying mapping method summary...")
    method_summary = con.execute(Q_MAPPING_METHOD_SUMMARY).df()

    con.close()

    # Write TSVs
    log.info("Writing coverage stats → %s", args.coverage_tsv)
    coverage_by_table.to_csv(args.coverage_tsv, sep="\t", index=False)

    log.info("Writing unmapped codes → %s", args.unmapped_tsv)
    unmapped.to_csv(args.unmapped_tsv, sep="\t", index=False)

    # Save individual PNGs if requested
    formats = [f.strip() for f in args.formats.split(",")]
    if "png" in formats:
        try:
            import plotly.io as pio
            if not coverage_by_table.empty:
                fig = px.bar(
                    coverage_by_table.sort_values("coverage_pct"),
                    x="coverage_pct",
                    y=coverage_by_table["source_table"] + "." + coverage_by_table["source_column"],
                    orientation="h",
                    title="Mapping Coverage %",
                    color="coverage_pct",
                    color_continuous_scale="RdYlGn",
                    range_color=[0, 100],
                )
                pio.write_image(fig, str(plots_dir / "coverage_by_column.png"), width=1000, height=600)
            if not coverage_by_domain.empty:
                fig2 = px.bar(
                    coverage_by_domain,
                    x="domain", y="record_count", color="mapping_method",
                    title="Domain Distribution",
                )
                pio.write_image(fig2, str(plots_dir / "domain_distribution.png"), width=1000, height=500)
            log.info("PNG charts saved to %s", plots_dir)
        except Exception as exc:
            log.warning("Could not save PNG charts (kaleido may not be installed): %s", exc)

    # Build and write HTML report
    if "html" in formats:
        log.info("Building HTML report → %s", args.output_report)
        html = build_html_report(
            coverage_by_domain, coverage_by_table, unmapped,
            method_summary, concept_dist, cdm_counts, args.top_n,
        )
        Path(args.output_report).write_text(html, encoding="utf-8")

    log.info("Profiling complete.")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="OMOP CDM Data Profiling")
    p.add_argument("--cdm-db",          required=True)
    p.add_argument("--mapping-log",     required=True)
    p.add_argument("--output-report",   required=True)
    p.add_argument("--coverage-tsv",    required=True)
    p.add_argument("--unmapped-tsv",    required=True)
    p.add_argument("--plots-dir",       required=True)
    p.add_argument("--top-n",           type=int, default=20)
    p.add_argument("--formats",         default="html,tsv")
    p.add_argument("--duckdb-memory",   default="16GB")
    p.add_argument("--duckdb-threads",  type=int, default=4)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_profiling(args)
