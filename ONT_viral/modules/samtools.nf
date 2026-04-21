/*
 * Module: Samtools Flagstat
 * Purpose: Generate alignment statistics for host depletion BAM (MultiQC-compatible)
 */

process SAMTOOLS_FLAGSTAT {
    tag "${meta.id}"
    label 'process_low'

    publishDir "${params.outdir}/host_depletion/${meta.id}", mode: 'copy'

    input:
    tuple val(meta), path(bam)

    output:
    tuple val(meta), path("${meta.id}_flagstat.txt"), emit: stats
    path  "versions.yml",                             emit: versions

    script:
    """
    samtools flagstat \\
        --threads ${task.cpus} \\
        ${bam} \\
        > ${meta.id}_flagstat.txt

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        samtools: \$(samtools --version 2>&1 | head -1 | sed 's/samtools //')
    END_VERSIONS
    """
}
