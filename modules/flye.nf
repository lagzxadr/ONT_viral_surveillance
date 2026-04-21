/*
 * Module: Flye
 * Purpose: De novo assembly of viral genomes from nanopore reads
 */

process FLYE_ASSEMBLE {
    tag "${meta.id}"
    label 'process_high'
    label 'process_long'

    publishDir "${params.outdir}/assembly/flye/${meta.id}", mode: 'copy'

    input:
    tuple val(meta), path(reads)

    output:
    tuple val(meta), path("${meta.id}_assembly.fasta"),      emit: assembly
    tuple val(meta), path("${meta.id}_assembly_info.txt"),   emit: info
    tuple val(meta), path("${meta.id}_flye.log"),            emit: log
    path  "versions.yml",                                    emit: versions

    script:
    def genome_size  = params.flye_genome_size   ?: '5m'
    def min_overlap  = params.flye_min_overlap   ?: 1000
    """
    flye \\
        --nano-hq ${reads} \\
        --genome-size ${genome_size} \\
        --out-dir flye_out \\
        --threads ${task.cpus} \\
        --min-overlap ${min_overlap} \\
        --meta \\
        2>&1 | tee ${meta.id}_flye.log

    # Rename outputs with sample prefix
    cp flye_out/assembly.fasta         ${meta.id}_assembly.fasta
    cp flye_out/assembly_info.txt      ${meta.id}_assembly_info.txt

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        flye: \$(flye --version 2>&1)
    END_VERSIONS
    """
}
