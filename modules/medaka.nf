/*
 * Module: Medaka
 * Purpose: Polish assembled viral genomes using nanopore reads
 */

process MEDAKA_POLISH {
    tag "${meta.id}"
    label 'process_high'
    label 'process_long'

    publishDir "${params.outdir}/assembly/medaka/${meta.id}", mode: 'copy'

    input:
    tuple val(meta), path(reads), path(assembly)

    output:
    tuple val(meta), path("${meta.id}_consensus.fasta"), emit: consensus
    tuple val(meta), path("${meta.id}_medaka.log"),      emit: log
    path  "versions.yml",                                emit: versions

    script:
    def model = params.medaka_model ?: 'r941_min_high_g360'
    """
    medaka_consensus \\
        -i ${reads} \\
        -d ${assembly} \\
        -o medaka_out \\
        -t ${task.cpus} \\
        -m ${model} \\
        2>&1 | tee ${meta.id}_medaka.log

    # Rename consensus output
    cp medaka_out/consensus.fasta ${meta.id}_consensus.fasta

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        medaka: \$(medaka --version 2>&1 | sed 's/medaka //')
    END_VERSIONS
    """
}
