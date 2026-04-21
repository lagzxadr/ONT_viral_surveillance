/*
 * Module: PATHOGEN_COVERAGE
 * Purpose: Calculate genome coverage for the top N pathogens identified by Bracken.
 *
 * Strategy:
 *   1. Parse the Bracken output to rank species by estimated read count and
 *      select the top N (default 5).
 *   2. Extract the read IDs assigned to each target taxon from the Kraken2
 *      per-read output using krakentools extract_kraken_reads.py.
 *   3. Map the extracted reads against the polished consensus assembly with
 *      minimap2 (map-ont preset).
 *   4. Compute per-base depth  (samtools depth)  and summary coverage stats
 *      (samtools coverage) for each pathogen.
 *   5. Emit a merged TSV summary across all top pathogens for MultiQC.
 */

process PATHOGEN_COVERAGE {
    tag "${meta.id}"
    label 'process_high'

    publishDir "${params.outdir}/coverage/${meta.id}", mode: 'copy'

    input:
    // Polished consensus assembly for this sample
    tuple val(meta), path(consensus)
    // Kraken2 per-read output  (tab-separated: C/U  readID  taxID  length  kmer_hits)
    tuple val(meta), path(kraken2_output)
    // Bracken species-level output  (tab-separated with header)
    tuple val(meta), path(bracken_output)
    // Viral reads (post host-depletion) to map against the consensus
    tuple val(meta), path(reads)

    output:
    tuple val(meta), path("${meta.id}_top${params.coverage_top_n}_pathogens.tsv"),   emit: summary
    tuple val(meta), path("${meta.id}_coverage_depth.tsv"),                           emit: depth
    tuple val(meta), path("${meta.id}_coverage_stats.tsv"),                           emit: stats
    tuple val(meta), path("${meta.id}_top_pathogens.txt"),                            emit: pathogen_list
    path  "versions.yml",                                                             emit: versions

    script:
    def top_n = params.coverage_top_n ?: 5
    """
    # ── Step 1: Identify top ${top_n} pathogens from Bracken output ──────────
    # Bracken output columns: name  taxonomy_id  taxonomy_lvl  kraken_assigned_reads
    #                         added_reads  new_est_reads  fraction_total_reads
    # Sort by new_est_reads (col 6) descending, skip header, take top N.

    awk 'NR > 1 { print }' ${bracken_output} \\
        | sort -t'\t' -k6,6rn \\
        | head -n ${top_n} \\
        > top_pathogens_raw.tsv

    # Write a human-readable list (name + taxid + estimated reads)
    echo -e "rank\tspecies\ttaxonomy_id\test_reads\tfraction_total" > ${meta.id}_top_pathogens.txt
    awk 'BEGIN{OFS="\t"} { print NR, \$1, \$2, \$6, \$7 }' top_pathogens_raw.tsv \\
        >> ${meta.id}_top_pathogens.txt

    # Collect taxon IDs for read extraction
    awk '{ print \$2 }' top_pathogens_raw.tsv > target_taxids.txt

    # ── Step 2: Extract reads per taxon from Kraken2 output ──────────────────
    # krakentools extract_kraken_reads.py is bundled in the kraken2 biocontainer.
    # We loop over each taxid and extract matching read IDs.

    mkdir -p extracted_reads

    while IFS= read -r taxid; do
        extract_kraken_reads.py \\
            -k ${kraken2_output} \\
            -s ${reads} \\
            -o extracted_reads/taxid_\${taxid}.fastq \\
            -t \${taxid} \\
            --include-children \\
            2>/dev/null || true
        # Compress if non-empty
        if [ -s extracted_reads/taxid_\${taxid}.fastq ]; then
            gzip extracted_reads/taxid_\${taxid}.fastq
        fi
    done < target_taxids.txt

    # ── Step 3 & 4: Map reads → consensus, compute coverage per pathogen ─────
    echo -e "sample\tspecies\ttaxonomy_id\trname\tstartpos\tendpos\tnumreads\tcovbases\tcoverage\tmeandepth\tmeanbaseq\tmeanmapq" \\
        > ${meta.id}_coverage_stats.tsv

    echo -e "sample\tspecies\ttaxonomy_id\tposition\tdepth" \\
        > ${meta.id}_coverage_depth.tsv

    while IFS=\$'\\t' read -r species taxid rest; do
        fq="extracted_reads/taxid_\${taxid}.fastq.gz"
        [ -f "\${fq}" ] || continue

        # Map with minimap2 (map-ont), sort and index
        minimap2 \\
            -ax map-ont \\
            -t ${task.cpus} \\
            ${consensus} \\
            "\${fq}" \\
            2>/dev/null \\
        | samtools sort -@ ${task.cpus} -o "\${taxid}_sorted.bam"
        samtools index "\${taxid}_sorted.bam"

        # Per-base depth
        samtools depth -a "\${taxid}_sorted.bam" \\
            | awk -v s="${meta.id}" -v sp="\${species}" -v t="\${taxid}" \\
                'BEGIN{OFS="\\t"} { print s, sp, t, \$2, \$3 }' \\
            >> ${meta.id}_coverage_depth.tsv

        # Summary coverage stats (one row per reference contig)
        samtools coverage "\${taxid}_sorted.bam" \\
            | awk -v s="${meta.id}" -v sp="\${species}" -v t="\${taxid}" \\
                'NR > 1 { print s, sp, t, \$0 }' OFS='\\t' \\
            >> ${meta.id}_coverage_stats.tsv

    done < top_pathogens_raw.tsv

    # ── Step 5: Build merged summary TSV (one row per pathogen) ──────────────
    echo -e "sample\trank\tspecies\ttaxonomy_id\test_reads\tfraction_total\tmean_coverage\tmean_depth\tcovered_bases\tref_length" \\
        > ${meta.id}_top${top_n}_pathogens.tsv

    awk 'NR > 1' ${meta.id}_top_pathogens.txt | while IFS=\$'\\t' read -r rank species taxid est_reads frac; do
        # Aggregate coverage stats for this taxid across all contigs
        stats=\$(awk -v t="\${taxid}" \\
            'BEGIN{cov=0; depth=0; bases=0; len=0; n=0}
             \$3==t { cov+=\$9; depth+=\$10; bases+=\$8; len+=(\$6-\$5+1); n++ }
             END { if(n>0) printf "%.2f\\t%.2f\\t%d\\t%d", cov/n, depth/n, bases, len }' \\
            ${meta.id}_coverage_stats.tsv)
        [ -z "\${stats}" ] && stats="0.00\t0.00\t0\t0"
        echo -e "${meta.id}\t\${rank}\t\${species}\t\${taxid}\t\${est_reads}\t\${frac}\t\${stats}"
    done >> ${meta.id}_top${top_n}_pathogens.tsv

    # ── Cleanup intermediates ─────────────────────────────────────────────────
    rm -rf extracted_reads top_pathogens_raw.tsv target_taxids.txt *_sorted.bam *_sorted.bam.bai

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        minimap2: \$(minimap2 --version 2>&1)
        samtools: \$(samtools --version 2>&1 | head -1 | sed 's/samtools //')
        krakentools: \$(extract_kraken_reads.py --version 2>&1 | head -1 || echo 'bundled')
    END_VERSIONS
    """
}
