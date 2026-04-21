/*
 * Module: OMOP_ETL
 * Purpose: Load raw source data and OMOP vocabulary into DuckDB,
 *          map source codes to standard concepts, and populate
 *          all CDM clinical tables.
 *
 * Inputs:
 *   raw_data_dir   - directory of raw CSV/TSV source tables
 *   vocabulary_dir - directory of OMOP vocabulary files
 *   mapping_config - JSON file describing source→domain mappings
 *
 * Outputs:
 *   cdm_db       - populated DuckDB file (omop_cdm.duckdb)
 *   mapping_log  - TSV audit log of every source code mapping attempt
 *   etl_summary  - JSON summary (row counts, coverage %)
 */

process OMOP_ETL {
    label 'process_high'

    publishDir "${params.outdir}/cdm",     mode: 'copy', pattern: '*.duckdb'
    publishDir "${params.outdir}/etl_logs", mode: 'copy', pattern: '*.{tsv,json}'

    input:
    path raw_data_dir
    path vocabulary_dir
    path mapping_config

    output:
    path "omop_cdm.duckdb",      emit: cdm_db
    path "mapping_log.tsv",      emit: mapping_log
    path "etl_summary.json",     emit: summary
    path "versions.yml",         emit: versions

    script:
    """
    python3 ${projectDir}/omop/scripts/etl.py \\
        --raw-data-dir      ${raw_data_dir} \\
        --vocabulary-dir    ${vocabulary_dir} \\
        --mapping-config    ${mapping_config} \\
        --output-db         omop_cdm.duckdb \\
        --mapping-log       mapping_log.tsv \\
        --etl-summary       etl_summary.json \\
        --cdm-version       ${params.cdm_version} \\
        --delimiter         "${params.source_delimiter}" \\
        --duckdb-memory     ${params.duckdb_memory} \\
        --duckdb-threads    ${params.duckdb_threads} \\
        --warn-threshold    ${params.mapping_warn_threshold} \\
        ${params.report_unmapped ? '--report-unmapped' : ''}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python3 --version 2>&1 | sed 's/Python //')
        duckdb: \$(python3 -c "import duckdb; print(duckdb.__version__)")
    END_VERSIONS
    """
}
