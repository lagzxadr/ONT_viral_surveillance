/*
 * Module: Kraken2
 * Purpose: Taxonomic classification of viral reads
 */

process KRAKEN2_CLASSIFY {
    tag "${meta.id}"
    label 'process_high'

    publishDir "${params.outdir}/classification/kraken2/${meta.id}", mode: 'copy'

    input:
    tuple val(meta), path(reads)
    path  kraken2_db

    output:
    tuple val(meta), path("${meta.id}_kraken2.output"),  emit: output
    tuple val(meta), path("${meta.id}_kraken2.report"),  emit: report
    tuple val(meta), path("${meta.id}_classified.fastq.gz"),   emit: classified_reads,   optional: true
    tuple val(meta), path("${meta.id}_unclassified.fastq.gz"), emit: unclassified_reads, optional: true
    path  "versions.yml",                                emit: versions

    script:
    def confidence = params.kraken2_confidence ?: 0.05
    """
    kraken2 \\
        --db ${kraken2_db} \\
        --threads ${task.cpus} \\
        --output ${meta.id}_kraken2.output \\
        --report ${meta.id}_kraken2.report \\
        --report-minimizer-data \\
        --confidence ${confidence} \\
        --classified-out ${meta.id}_classified.fastq \\
        --unclassified-out ${meta.id}_unclassified.fastq \\
        --gzip-compressed \\
        ${reads}

    # Compress classified/unclassified outputs
    gzip -f ${meta.id}_classified.fastq   || true
    gzip -f ${meta.id}_unclassified.fastq || true

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        kraken2: \$(kraken2 --version 2>&1 | head -1 | sed 's/Kraken version //')
    END_VERSIONS
    """
}
