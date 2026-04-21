/*
 * Module: Bracken
 * Purpose: Re-estimate species/genus-level abundances from Kraken2 reports
 */

process BRACKEN_ABUNDANCE {
    tag "${meta.id}"
    label 'process_low'

    publishDir "${params.outdir}/classification/bracken/${meta.id}", mode: 'copy'

    input:
    tuple val(meta), path(kraken2_report)
    path  kraken2_db

    output:
    tuple val(meta), path("${meta.id}_bracken.report"),  emit: report
    tuple val(meta), path("${meta.id}_bracken.output"),  emit: output
    path  "versions.yml",                                emit: versions

    script:
    def level     = params.bracken_level     ?: 'S'
    def threshold = params.bracken_threshold ?: 10
    """
    bracken \\
        -d ${kraken2_db} \\
        -i ${kraken2_report} \\
        -o ${meta.id}_bracken.output \\
        -w ${meta.id}_bracken.report \\
        -r 150 \\
        -l ${level} \\
        -t ${threshold}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        bracken: \$(bracken --version 2>&1 | sed 's/Bracken v//')
    END_VERSIONS
    """
}
