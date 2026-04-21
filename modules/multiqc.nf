/*
 * Module: MultiQC
 * Purpose: Aggregate all QC metrics into a single interactive HTML report
 */

process MULTIQC {
    label 'process_medium'

    publishDir "${params.outdir}/multiqc", mode: 'copy'

    input:
    path  qc_files
    path  multiqc_config

    output:
    path "multiqc_report.html",  emit: report
    path "multiqc_data/",        emit: data
    path "versions.yml",         emit: versions

    script:
    def config_arg = multiqc_config ? "--config ${multiqc_config}" : ''
    def title_arg  = params.multiqc_title ? "--title \"${params.multiqc_title}\"" : ''
    """
    multiqc \\
        --force \\
        ${config_arg} \\
        ${title_arg} \\
        --filename multiqc_report.html \\
        --comment "Nanopore Viral Surveillance Pipeline QC Report" \\
        .

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        multiqc: \$(multiqc --version 2>&1 | sed 's/multiqc, version //')
    END_VERSIONS
    """
}
