#!/usr/bin/env nextflow

/*
 * ============================================================
 *  Nanopore Viral Genome Identification Pipeline
 *  for Pathogen Monitoring
 * ============================================================
 *
 * Workflow steps:
 *   1. Raw read QC          - NanoPlot + NanoStat
 *   2. Adapter trimming     - Porechop
 *   3. Read filtering       - Filtlong (length + quality)
 *   4. Post-trim QC         - NanoPlot + NanoStat
 *   5. Host depletion       - Minimap2 + Samtools
 *   6. Viral classification - Kraken2 + Bracken + Krona
 *   7. De novo assembly     - Flye
 *   8. Consensus polishing  - Medaka
 *   9. Genome annotation    - BLAST (nucleotide) + Diamond (protein)
 *  10. Genome coverage      - Pathogen coverage for top N identified species
 *  11. QC report            - MultiQC
 */

nextflow.enable.dsl = 2

// ─── Parameter validation ────────────────────────────────────────────────────

def validateParams() {
    if (!params.input) {
        error "ERROR: --input samplesheet is required. Provide a CSV with columns: sample,fastq_path"
    }
    if (!params.kraken2_db) {
        error "ERROR: --kraken2_db is required. Provide path to a Kraken2 viral/standard database."
    }
    if (!params.blast_db && !params.diamond_db) {
        log.warn "WARNING: Neither --blast_db nor --diamond_db provided. Annotation step will be skipped."
    }
    if (!params.host_genome) {
        log.warn "WARNING: --host_genome not provided. Host depletion step will be skipped."
    }
}

// ─── Include modules ─────────────────────────────────────────────────────────

include { NANOPLOT as NANOPLOT_RAW        } from './modules/nanoplot'
include { NANOPLOT as NANOPLOT_FILTERED   } from './modules/nanoplot'
include { NANOSTAT as NANOSTAT_RAW        } from './modules/nanostat'
include { NANOSTAT as NANOSTAT_FILTERED   } from './modules/nanostat'
include { PORECHOP                        } from './modules/porechop'
include { FILTLONG                        } from './modules/filtlong'
include { MINIMAP2_HOST_DEPLETION         } from './modules/minimap2'
include { SAMTOOLS_FLAGSTAT               } from './modules/samtools'
include { KRAKEN2_CLASSIFY                } from './modules/kraken2'
include { BRACKEN_ABUNDANCE               } from './modules/bracken'
include { KRONA_PLOT                      } from './modules/krona'
include { FLYE_ASSEMBLE                   } from './modules/flye'
include { MEDAKA_POLISH                   } from './modules/medaka'
include { BLAST_BLASTN                    } from './modules/blast'
include { DIAMOND_BLASTX                  } from './modules/diamond'
include { PATHOGEN_COVERAGE               } from './modules/coverage'
include { MULTIQC                         } from './modules/multiqc'

// ─── Helper: parse samplesheet ───────────────────────────────────────────────

def parseSamplesheet(csv_file) {
    Channel
        .fromPath(csv_file, checkIfExists: true)
        .splitCsv(header: true, strip: true)
        .map { row ->
            def meta = [
                id      : row.sample,
                barcode : row.containsKey('barcode') ? row.barcode : 'NA',
                run_id  : row.containsKey('run_id')  ? row.run_id  : 'NA'
            ]
            def fastq = file(row.fastq_path, checkIfExists: true)
            return [ meta, fastq ]
        }
}

// ─── Main workflow ────────────────────────────────────────────────────────────

workflow {

    validateParams()

    // ── 0. Input ──────────────────────────────────────────────────────────────
    ch_reads = parseSamplesheet(params.input)

    // ── 1. Raw QC ─────────────────────────────────────────────────────────────
    NANOPLOT_RAW(ch_reads)
    NANOSTAT_RAW(ch_reads)

    // ── 2. Adapter trimming ───────────────────────────────────────────────────
    PORECHOP(ch_reads)

    // ── 3. Read filtering (length + quality) ──────────────────────────────────
    FILTLONG(PORECHOP.out.reads)

    // ── 4. Post-filter QC ─────────────────────────────────────────────────────
    NANOPLOT_FILTERED(FILTLONG.out.reads)
    NANOSTAT_FILTERED(FILTLONG.out.reads)

    // ── 5. Host depletion (optional) ──────────────────────────────────────────
    if (params.host_genome) {
        ch_host_genome = Channel.value(file(params.host_genome, checkIfExists: true))
        MINIMAP2_HOST_DEPLETION(FILTLONG.out.reads, ch_host_genome)
        SAMTOOLS_FLAGSTAT(MINIMAP2_HOST_DEPLETION.out.bam)
        ch_viral_reads = MINIMAP2_HOST_DEPLETION.out.unmapped_reads
    } else {
        ch_viral_reads = FILTLONG.out.reads
    }

    // ── 6. Viral classification ───────────────────────────────────────────────
    ch_kraken2_db = Channel.value(file(params.kraken2_db, checkIfExists: true))
    KRAKEN2_CLASSIFY(ch_viral_reads, ch_kraken2_db)
    BRACKEN_ABUNDANCE(KRAKEN2_CLASSIFY.out.report, ch_kraken2_db)
    KRONA_PLOT(KRAKEN2_CLASSIFY.out.output)

    // ── 7. De novo assembly ───────────────────────────────────────────────────
    FLYE_ASSEMBLE(ch_viral_reads)

    // ── 8. Consensus polishing ────────────────────────────────────────────────
    // Join reads with their assembly by sample ID
    ch_polish_input = ch_viral_reads.join(FLYE_ASSEMBLE.out.assembly, by: 0)
    MEDAKA_POLISH(ch_polish_input)

    // ── 9. Annotation ─────────────────────────────────────────────────────────
    if (params.blast_db) {
        ch_blast_db = Channel.value(file(params.blast_db, checkIfExists: true))
        BLAST_BLASTN(MEDAKA_POLISH.out.consensus, ch_blast_db)
    }
    if (params.diamond_db) {
        ch_diamond_db = Channel.value(file(params.diamond_db, checkIfExists: true))
        DIAMOND_BLASTX(MEDAKA_POLISH.out.consensus, ch_diamond_db)
    }

    // ── 10. Genome coverage for top N pathogens ───────────────────────────────
    // Join consensus, kraken2 output, bracken output, and viral reads by sample ID
    ch_coverage_input = MEDAKA_POLISH.out.consensus
        .join(KRAKEN2_CLASSIFY.out.output,   by: 0)
        .join(BRACKEN_ABUNDANCE.out.output,  by: 0)
        .join(ch_viral_reads,                by: 0)

    PATHOGEN_COVERAGE(
        ch_coverage_input.map { meta, consensus, k2out, bracken, reads -> [ meta, consensus ] },
        ch_coverage_input.map { meta, consensus, k2out, bracken, reads -> [ meta, k2out     ] },
        ch_coverage_input.map { meta, consensus, k2out, bracken, reads -> [ meta, bracken   ] },
        ch_coverage_input.map { meta, consensus, k2out, bracken, reads -> [ meta, reads     ] }
    )

    // ── 11. Aggregate QC report ───────────────────────────────────────────────
    ch_multiqc_files = Channel.empty()
        .mix(NANOSTAT_RAW.out.stats.map { meta, f -> f })
        .mix(NANOSTAT_FILTERED.out.stats.map { meta, f -> f })
        .mix(NANOPLOT_RAW.out.report.map { meta, f -> f })
        .mix(NANOPLOT_FILTERED.out.report.map { meta, f -> f })
        .mix(KRAKEN2_CLASSIFY.out.report.map { meta, f -> f })
        .mix(BRACKEN_ABUNDANCE.out.report.map { meta, f -> f })
        .mix(PATHOGEN_COVERAGE.out.summary.map { meta, f -> f })

    if (params.host_genome) {
        ch_multiqc_files = ch_multiqc_files
            .mix(SAMTOOLS_FLAGSTAT.out.stats.map { meta, f -> f })
    }

    ch_multiqc_config = params.multiqc_config
        ? Channel.value(file(params.multiqc_config))
        : Channel.empty()

    MULTIQC(
        ch_multiqc_files.collect(),
        ch_multiqc_config.ifEmpty([])
    )
}

// ─── Workflow completion summary ──────────────────────────────────────────────

workflow.onComplete {
    log.info """
    ╔══════════════════════════════════════════════════════════╗
    ║   Nanopore Viral Surveillance Pipeline - Complete        ║
    ╠══════════════════════════════════════════════════════════╣
    ║  Status    : ${workflow.success ? 'SUCCESS ✓' : 'FAILED ✗'}
    ║  Duration  : ${workflow.duration}
    ║  Results   : ${params.outdir}
    ║  Report    : ${params.outdir}/multiqc/multiqc_report.html
    ╚══════════════════════════════════════════════════════════╝
    """.stripIndent()
}

workflow.onError {
    log.error "Pipeline failed: ${workflow.errorMessage}"
}
