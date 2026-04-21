#!/usr/bin/env nextflow

/*
 * ============================================================
 *  OMOP CDM Standardization Pipeline
 * ============================================================
 *
 * Steps:
 *   1. ETL          - Load raw data + vocabulary into DuckDB,
 *                     map source codes → standard OMOP concepts
 *   2. Profiling    - Visualize mapping coverage, domain
 *                     distribution, unmapped codes
 *   3. Reverse Eng  - Trace every standard concept back to its
 *                     source raw value with full audit trail
 *   4. Update       - Incorporate vocabulary updates / new IDs
 *                     and re-standardize incrementally
 */

nextflow.enable.dsl = 2

// ─── Parameter validation ─────────────────────────────────────────────────────

def validateParams() {
    if (!params.raw_data_dir) {
        error "ERROR: --raw_data_dir is required. Path to directory containing raw CSV/TSV source tables."
    }
    if (!params.vocabulary_dir) {
        error "ERROR: --vocabulary_dir is required. Path to OMOP vocabulary files (CONCEPT.csv, CONCEPT_RELATIONSHIP.csv, etc.)."
    }
    if (!params.source_mapping_file) {
        error "ERROR: --source_mapping_file is required. Path to source-to-concept mapping config (JSON)."
    }
}

// ─── Include modules ──────────────────────────────────────────────────────────

include { OMOP_ETL              } from './modules/etl'
include { OMOP_PROFILING        } from './modules/profiling'
include { OMOP_REVERSE_ENGINEER } from './modules/reverse_engineering'
include { OMOP_UPDATE           } from './modules/update'

// ─── Main workflow ────────────────────────────────────────────────────────────

workflow {

    validateParams()

    // ── Inputs ────────────────────────────────────────────────────────────────
    ch_raw_data     = Channel.value(file(params.raw_data_dir,       checkIfExists: true))
    ch_vocab        = Channel.value(file(params.vocabulary_dir,     checkIfExists: true))
    ch_mapping      = Channel.value(file(params.source_mapping_file, checkIfExists: true))

    ch_vocab_update = params.vocabulary_update_dir
        ? Channel.value(file(params.vocabulary_update_dir, checkIfExists: true))
        : Channel.value([])

    // ── 1. ETL: raw data → OMOP CDM DuckDB ───────────────────────────────────
    OMOP_ETL(ch_raw_data, ch_vocab, ch_mapping)

    // ── 2. Profiling: mapping coverage + domain visualizations ───────────────
    OMOP_PROFILING(OMOP_ETL.out.cdm_db, OMOP_ETL.out.mapping_log)

    // ── 3. Reverse engineering: concept → source audit trail ─────────────────
    OMOP_REVERSE_ENGINEER(OMOP_ETL.out.cdm_db, OMOP_ETL.out.mapping_log)

    // ── 4. Update: incorporate vocabulary updates / new concept IDs ──────────
    OMOP_UPDATE(
        OMOP_ETL.out.cdm_db,
        ch_vocab_update,
        OMOP_ETL.out.mapping_log
    )
}

// ─── Completion summary ───────────────────────────────────────────────────────

workflow.onComplete {
    log.info """
    ╔══════════════════════════════════════════════════════════╗
    ║   OMOP CDM Standardization Pipeline - Complete           ║
    ╠══════════════════════════════════════════════════════════╣
    ║  Status   : ${workflow.success ? 'SUCCESS ✓' : 'FAILED ✗'}
    ║  Duration : ${workflow.duration}
    ║  CDM DB   : ${params.outdir}/cdm/omop_cdm.duckdb
    ║  Report   : ${params.outdir}/profiling/profiling_report.html
    ║  Audit    : ${params.outdir}/reverse_engineering/
    ╚══════════════════════════════════════════════════════════╝
    """.stripIndent()
}

workflow.onError {
    log.error "Pipeline failed: ${workflow.errorMessage}"
}
