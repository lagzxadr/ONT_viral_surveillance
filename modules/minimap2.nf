/*
 * Module: Minimap2 - Host Depletion
 * Purpose: Align reads to host genome and extract unmapped (viral) reads
 */

process MINIMAP2_HOST_DEPLETION {
    tag "${meta.id}"
    label 'process_high'

    publishDir "${params.outdir}/host_depletion/${meta.id}", mode: 'copy'

    input:
    tuple val(meta), path(reads)
    path  host_genome

    output:
    tuple val(meta), path("${meta.id}_host_mapped.bam"),     emit: bam
    tuple val(meta), path("${meta.id}_unmapped.fastq.gz"),   emit: unmapped_reads
    tuple val(meta), path("${meta.id}_host_depletion.log"),  emit: log
    path  "versions.yml",                                    emit: versions

    script:
    """
    # Align to host genome using map-ont preset
    minimap2 \\
        -ax map-ont \\
        -t ${task.cpus} \\
        ${host_genome} \\
        ${reads} \\
        2> ${meta.id}_host_depletion.log \\
    | samtools sort -@ ${task.cpus} -o ${meta.id}_host_mapped.bam

    samtools index ${meta.id}_host_mapped.bam

    # Extract unmapped reads (viral candidates)
    samtools fastq \\
        -@ ${task.cpus} \\
        -f 4 \\
        ${meta.id}_host_mapped.bam \\
        | gzip > ${meta.id}_unmapped.fastq.gz

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        minimap2: \$(minimap2 --version 2>&1)
        samtools: \$(samtools --version 2>&1 | head -1 | sed 's/samtools //')
    END_VERSIONS
    """
}
