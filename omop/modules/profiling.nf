/*
 * Module: OMOP_PROFILING
 * Purpose: Visualize the mapping process — coverage by domain,
 *          top unmapped source codes, concept distribution,
 *          and per-table row counts. Produces an HTML report
 *          and supporting TSV/PNG assets.
 *
 * Inputs:
 *   cdm_db      - populated DuckDB file from ETL
 *   mapping_log - TSV audit log from ETL
 *
 * Outputs:
 *   profiling_report - interactive HTML report
 *   coverage_tsv     - mapping coverage stats per domain/table
 *   unmapped_tsv     - top-N unmapped source codes
 *   plots_dir        - directory of PNG charts
 */

process OMOP_PROFILING {
    label 'process_medium'

    publishDir "${params.outdir}/profiling", mode: 'copy'

    input:
    path cdm_db
    path mapping_log

    output:
    path "profiling_report.html",   emit: report
    path "coverage_stats.tsv",      emit: coverage_tsv
    path "unmapped_codes.tsv",      emit: unmapped_tsv
    path "plots/",                  emit: plots_dir
    path "versions.yml",            emit: versions

    script:
    """
    mkdir -p plots

    python3 ${projectDir}/omop/scripts/profiling.py \\
        --cdm-db            ${cdm_db} \\
        --mapping-log       ${mapping_log} \\
        --output-report     profiling_report.html \\
        --coverage-tsv      coverage_stats.tsv \\
        --unmapped-tsv      unmapped_codes.tsv \\
        --plots-dir         plots/ \\
        --top-n             ${params.profiling_top_n} \\
        --formats           "${params.profiling_formats}" \\
        --duckdb-memory     ${params.duckdb_memory} \\
        --duckdb-threads    ${params.duckdb_threads}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python3 --version 2>&1 | sed 's/Python //')
        duckdb: \$(python3 -c "import duckdb; print(duckdb.__version__)")
        plotly: \$(python3 -c "import plotly; print(plotly.__version__)")
        pandas: \$(python3 -c "import pandas; print(pandas.__version__)")
    END_VERSIONS
    """
}
