/*
 * Module: NanoStat
 * Purpose: Generate summary statistics for nanopore reads (MultiQC-compatible)
 */

process NANOSTAT {
    tag "${meta.id}"
    label 'process_low'

    publishDir "${params.outdir}/qc/nanostat/${meta.id}", mode: 'copy'

    input:
    tuple val(meta), path(reads)

    output:
    tuple val(meta), path("${meta.id}_nanostat.txt"), emit: stats
    path  "versions.yml",                             emit: versions

    script:
    """
    NanoStat \\
        --fastq ${reads} \\
        --threads ${task.cpus} \\
        --name "${meta.id}" \\
        > ${meta.id}_nanostat.txt

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        nanostat: \$(NanoStat --version 2>&1 | sed 's/NanoStat //')
    END_VERSIONS
    """
}
