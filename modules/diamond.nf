/*
 * Module: Diamond (blastx)
 * Purpose: Protein-level annotation of assembled viral genomes
 */

process DIAMOND_BLASTX {
    tag "${meta.id}"
    label 'process_medium'

    publishDir "${params.outdir}/annotation/diamond/${meta.id}", mode: 'copy'

    input:
    tuple val(meta), path(consensus)
    path  diamond_db

    output:
    tuple val(meta), path("${meta.id}_diamond.tsv"), emit: results
    path  "versions.yml",                            emit: versions

    script:
    def evalue  = params.blast_evalue        ?: '1e-5'
    def max_hit = params.blast_max_targets   ?: 5
    """
    diamond blastx \\
        --query ${consensus} \\
        --db ${diamond_db} \\
        --out ${meta.id}_diamond.tsv \\
        --outfmt 6 qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore staxids sscinames \\
        --evalue ${evalue} \\
        --max-target-seqs ${max_hit} \\
        --threads ${task.cpus} \\
        --sensitive \\
        --more-sensitive

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        diamond: \$(diamond --version 2>&1 | head -1 | sed 's/diamond version //')
    END_VERSIONS
    """
}
