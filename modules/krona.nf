/*
 * Module: Krona
 * Purpose: Generate interactive taxonomic pie charts from Kraken2 output
 */

process KRONA_PLOT {
    tag "${meta.id}"
    label 'process_low'

    publishDir "${params.outdir}/classification/krona/${meta.id}", mode: 'copy'

    input:
    tuple val(meta), path(kraken2_output)

    output:
    tuple val(meta), path("${meta.id}_krona.html"), emit: html
    path  "versions.yml",                           emit: versions

    script:
    """
    # Convert Kraken2 output to Krona format
    cut -f2,3 ${kraken2_output} > ${meta.id}_krona_input.txt

    ktImportTaxonomy \\
        -t 2 \\
        -s 1 \\
        -o ${meta.id}_krona.html \\
        ${meta.id}_krona_input.txt

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        krona: \$(ktImportTaxonomy 2>&1 | head -2 | tail -1 | sed 's/.*KronaTools //' | sed 's/ .*//')
    END_VERSIONS
    """
}
