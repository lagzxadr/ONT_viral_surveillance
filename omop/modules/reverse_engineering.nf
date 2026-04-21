/*
 * Module: OMOP_REVERSE_ENGINEER
 * Purpose: For every standard concept in the CDM, trace back to
 *          the original raw source value, the vocabulary mapping
 *          path, and the ETL transformation applied.
 *          Produces a full audit trail usable for data lineage,
 *          QA, and regulatory review.
 *
 * Inputs:
 *   cdm_db      - populated DuckDB file from ETL
 *   mapping_log - TSV audit log from ETL
 *
 * Outputs:
 *   audit_trail_tsv     - full concept→source lineage (TSV)
 *   audit_trail_parquet - same data in Parquet (optional)
 *   lineage_report      - HTML report with interactive lineage explorer
 *   concept_map_tsv     - flat concept mapping reference table
 */

process OMOP_REVERSE_ENGINEER {
    label 'process_medium'

    publishDir "${params.outdir}/reverse_engineering", mode: 'copy'

    input:
    path cdm_db
    path mapping_log

    output:
    path "audit_trail.tsv",             emit: audit_trail_tsv
    path "audit_trail.parquet",         emit: audit_trail_parquet, optional: true
    path "lineage_report.html",         emit: lineage_report
    path "concept_map.tsv",             emit: concept_map_tsv
    path "versions.yml",                emit: versions

    script:
    """
    python3 ${projectDir}/omop/scripts/reverse_engineering.py \\
        --cdm-db            ${cdm_db} \\
        --mapping-log       ${mapping_log} \\
        --audit-trail-tsv   audit_trail.tsv \\
        --concept-map-tsv   concept_map.tsv \\
        --lineage-report    lineage_report.html \\
        --duckdb-memory     ${params.duckdb_memory} \\
        --duckdb-threads    ${params.duckdb_threads} \\
        ${params.audit_parquet ? '--parquet audit_trail.parquet' : ''}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python3 --version 2>&1 | sed 's/Python //')
        duckdb: \$(python3 -c "import duckdb; print(duckdb.__version__)")
        pyarrow: \$(python3 -c "import pyarrow; print(pyarrow.__version__)" 2>/dev/null || echo 'n/a')
    END_VERSIONS
    """
}
