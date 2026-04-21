/*
 * Module: NanoPlot
 * Purpose: Generate visual QC plots for nanopore reads
 */

process NANOPLOT {
    tag "${meta.id}"
    label 'process_medium'

    publishDir "${params.outdir}/qc/nanoplot/${meta.id}", mode: 'copy'

    input:
    tuple val(meta), path(reads)

    output:
    tuple val(meta), path("*.html"),                emit: report
    tuple val(meta), path("NanoStats.txt"),          emit: stats
    tuple val(meta), path("*.png"),                  emit: plots, optional: true
    path  "versions.yml",                            emit: versions

    script:
    def prefix = task.ext.prefix ?: "${meta.id}"
    """
    NanoPlot \\
        --fastq ${reads} \\
        --outdir . \\
        --prefix ${prefix}_ \\
        --threads ${task.cpus} \\
        --plots dot \\
        --N50 \\
        --title "${meta.id} - Nanopore Read QC" \\
        --color '#1f77b4' \\
        --format png

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        nanoplot: \$(NanoPlot --version 2>&1 | sed 's/NanoPlot //')
    END_VERSIONS
    """
}
