/*
 * Module: BLAST (blastn)
 * Purpose: Annotate assembled viral genomes against nucleotide database
 */

process BLAST_BLASTN {
    tag "${meta.id}"
    label 'process_medium'

    publishDir "${params.outdir}/annotation/blast/${meta.id}", mode: 'copy'

    input:
    tuple val(meta), path(consensus)
    path  blast_db

    output:
    tuple val(meta), path("${meta.id}_blastn.txt"),  emit: results
    tuple val(meta), path("${meta.id}_blastn.xml"),  emit: xml
    path  "versions.yml",                            emit: versions

    script:
    def max_targets = params.blast_max_targets   ?: 5
    def evalue      = params.blast_evalue        ?: '1e-5'
    def perc_id     = params.blast_perc_identity ?: 80
    """
    blastn \\
        -query ${consensus} \\
        -db ${blast_db} \\
        -out ${meta.id}_blastn.txt \\
        -outfmt "6 qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore staxids sscinames sblastnames" \\
        -evalue ${evalue} \\
        -perc_identity ${perc_id} \\
        -max_target_seqs ${max_targets} \\
        -num_threads ${task.cpus}

    # Also generate XML for downstream parsing
    blastn \\
        -query ${consensus} \\
        -db ${blast_db} \\
        -out ${meta.id}_blastn.xml \\
        -outfmt 5 \\
        -evalue ${evalue} \\
        -perc_identity ${perc_id} \\
        -max_target_seqs ${max_targets} \\
        -num_threads ${task.cpus}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        blast: \$(blastn -version 2>&1 | head -1 | sed 's/blastn: //')
    END_VERSIONS
    """
}
