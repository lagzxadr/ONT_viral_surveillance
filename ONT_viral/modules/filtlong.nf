/*
 * Module: Filtlong
 * Purpose: Filter nanopore reads by length and quality
 */

process FILTLONG {
    tag "${meta.id}"
    label 'process_low'

    publishDir "${params.outdir}/filtered/${meta.id}", mode: 'copy'

    input:
    tuple val(meta), path(reads)

    output:
    tuple val(meta), path("${meta.id}_filtered.fastq.gz"), emit: reads
    tuple val(meta), path("${meta.id}_filtlong.log"),      emit: log
    path  "versions.yml",                                  emit: versions

    script:
    def min_len = params.min_length ?: 200
    def max_len = params.max_length ?: 50000
    def min_q   = params.min_quality ?: 8
    """
    filtlong \\
        --min_length ${min_len} \\
        --max_length ${max_len} \\
        --min_mean_q ${min_q} \\
        ${reads} \\
        2> ${meta.id}_filtlong.log \\
        | gzip > ${meta.id}_filtered.fastq.gz

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        filtlong: \$(filtlong --version 2>&1 | head -1)
    END_VERSIONS
    """
}
