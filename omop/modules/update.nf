/*
 * Module: OMOP_UPDATE
 * Purpose: Incorporate vocabulary updates (new concept IDs,
 *          deprecated concepts, relationship changes) into the
 *          existing CDM DuckDB. Supports both incremental patch
 *          mode (only re-map changed concepts) and full re-ETL.
 *
 * Inputs:
 *   cdm_db         - existing CDM DuckDB from ETL
 *   vocab_update   - directory with updated vocabulary files
 *                    (can be empty [] to skip update)
 *   mapping_log    - original ETL mapping log
 *
 * Outputs:
 *   cdm_db_updated    - updated DuckDB file
 *   update_report     - HTML diff report (old vs new concept assignments)
 *   update_log        - TSV log of all changed/added/deprecated mappings
 *   update_summary    - JSON summary of changes
 */

process OMOP_UPDATE {
    label 'process_high'

    publishDir "${params.outdir}/cdm",          mode: 'copy', pattern: '*_updated.duckdb'
    publishDir "${params.outdir}/update_logs",  mode: 'copy', pattern: '*.{tsv,json,html}'

    input:
    path cdm_db
    path vocab_update   // may be an empty list [] when no update provided
    path mapping_log

    output:
    path "omop_cdm_updated.duckdb",  emit: cdm_db_updated
    path "update_report.html",       emit: update_report
    path "update_log.tsv",           emit: update_log
    path "update_summary.json",      emit: update_summary
    path "versions.yml",             emit: versions

    script:
    def vocab_update_arg = (vocab_update instanceof List && vocab_update.isEmpty())
        ? ''
        : "--vocab-update-dir ${vocab_update}"
    def remap_flag = params.update_full_remap ? '--full-remap' : '--incremental'
    """
    python3 ${projectDir}/omop/scripts/update.py \\
        --cdm-db            ${cdm_db} \\
        --mapping-log       ${mapping_log} \\
        --output-db         omop_cdm_updated.duckdb \\
        --update-report     update_report.html \\
        --update-log        update_log.tsv \\
        --update-summary    update_summary.json \\
        --duckdb-memory     ${params.duckdb_memory} \\
        --duckdb-threads    ${params.duckdb_threads} \\
        ${vocab_update_arg} \\
        ${remap_flag}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python3 --version 2>&1 | sed 's/Python //')
        duckdb: \$(python3 -c "import duckdb; print(duckdb.__version__)")
    END_VERSIONS
    """
}
