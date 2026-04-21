/*
 * Module: Porechop
 * Purpose: Trim Oxford Nanopore adapters from reads
 */

process PORECHOP {
    tag "${meta.id}"
    label 'process_medium'

    publishDir "${params.outdir}/trimmed/${meta.id}", mode: 'copy'

    input:
    tuple val(meta), path(reads)

    output:
    tuple val(meta), path("${meta.id}_trimmed.fastq.gz"), emit: reads
    tuple val(meta), path("${meta.id}_porechop.log"),     emit: log
    path  "versions.yml",                                 emit: versions

    script:
    """
    porechop \\
        --input ${reads} \\
        --output ${meta.id}_trimmed.fastq.gz \\
        --threads ${task.cpus} \\
        --discard_middle \\
        2>&1 | tee ${meta.id}_porechop.log

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        porechop: \$(porechop --version 2>&1)
    END_VERSIONS
    """
}
