# Nanopore Viral Genome Identification Pipeline

A Nextflow DSL2 pipeline for identifying viral genomes from Oxford Nanopore sequencing data, designed for pathogen monitoring and surveillance.

---

## Pipeline Overview

```
Raw FASTQ reads
      │
      ▼
┌─────────────────────┐
│  1. Raw QC          │  NanoPlot + NanoStat
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  2. Adapter Trim    │  Porechop
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  3. Read Filtering  │  Filtlong (length + quality)
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  4. Post-filter QC  │  NanoPlot + NanoStat
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  5. Host Depletion  │  Minimap2 → Samtools (optional)
└─────────┬───────────┘
          │
          ├──────────────────────────────────────┐
          ▼                                      ▼
┌─────────────────────┐              ┌───────────────────────┐
│  6. Classification  │              │  7. De novo Assembly  │
│  Kraken2 + Bracken  │              │  Flye (--meta)        │
│  + Krona plots      │              └───────────┬───────────┘
└─────────────────────┘                          │
                                                 ▼
                                    ┌───────────────────────┐
                                    │  8. Polishing         │
                                    │  Medaka               │
                                    └───────────┬───────────┘
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │  9. Annotation        │
                                    │  BLAST + Diamond      │
                                    └───────────────────────┘
          │
          ▼
┌─────────────────────┐
│  10. QC Report      │  MultiQC (aggregated HTML report)
└─────────────────────┘
```

---

## Requirements

- [Nextflow](https://www.nextflow.io/) ≥ 23.04.0
- [Docker](https://www.docker.com/) or [Singularity](https://sylabs.io/singularity/) (recommended)
- All tool containers are pulled automatically from [Biocontainers](https://biocontainers.pro/)

---

## Quick Start

### Test run with bundled dataset

A 150 MB test dataset (53,439 ONT reads, run ID `2f9c9c7a`) is included in `data/test_dataset.zip`.

**Prerequisites:**
```bash
# Java 11+
sudo apt install default-jdk

# Nextflow
curl -s https://get.nextflow.io | bash && sudo mv nextflow /usr/local/bin/

# Docker (recommended)
curl -fsSL https://get.docker.com | sh

# Or Singularity
sudo apt install singularity-container
```

**Download a Kraken2 viral database** (required — not bundled due to size):
```bash
# Pre-built viral DB from Ben Langmead's index (~1 GB, fastest option)
mkdir kraken2_viral_db
wget https://genome-idx.s3.amazonaws.com/kraken/k2_viral_20240112.tar.gz
tar -xzf k2_viral_20240112.tar.gz -C kraken2_viral_db/
```

**Run the test:**
```bash
# Using the automated run script (handles setup checks)
export KRAKEN2_DB=/path/to/kraken2_viral_db
bash run_test.sh

# Or run Nextflow directly
nextflow run main.nf \
    -profile docker,test_dataset \
    --kraken2_db /path/to/kraken2_viral_db
```

The `test_dataset` profile pre-configures:
- Input: `test/samplesheet_test_dataset.csv` (53,439 reads, single sample)
- No host depletion (set `--host_genome` to enable)
- No annotation (set `--blast_db` / `--diamond_db` to enable)
- Resource limits: 16 GB RAM, 8 CPUs, 4 h



Create a CSV file (`samplesheet.csv`) with the following columns:

```csv
sample,barcode,run_id,fastq_path
patient_A,barcode01,run_20260420,/data/barcode01/reads.fastq.gz
patient_B,barcode02,run_20260420,/data/barcode02/reads.fastq.gz
```

| Column      | Required | Description                              |
|-------------|----------|------------------------------------------|
| `sample`    | ✓        | Unique sample identifier                 |
| `barcode`   |          | Nanopore barcode label                   |
| `run_id`    |          | Sequencing run identifier                |
| `fastq_path`| ✓        | Absolute or relative path to FASTQ file  |

### 2. Download required databases

**Kraken2 viral database:**
```bash
# Standard viral database (~8 GB)
kraken2-build --download-library viral --db kraken2_viral_db
kraken2-build --build --db kraken2_viral_db

# Or download pre-built from https://benlangmead.github.io/aws-indexes/k2
wget https://genome-idx.s3.amazonaws.com/kraken/k2_viral_20240112.tar.gz
tar -xzf k2_viral_20240112.tar.gz -C kraken2_viral_db/
```

**BLAST viral database:**
```bash
update_blastdb.pl --decompress ref_viruses_rep_genomes
# Or full nt database (large):
update_blastdb.pl --decompress nt
```

**Host genome (for depletion):**
```bash
# Human reference (hg38)
wget https://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/000/001/405/GCA_000001405.15_GRCh38/GRCh38_major_chr.fna.gz
```

### 3. Run the pipeline

```bash
nextflow run main.nf \
    --input samplesheet.csv \
    --outdir results \
    --host_genome /path/to/hg38.fa \
    --kraken2_db /path/to/kraken2_viral_db \
    --blast_db /path/to/blast_viral_db/ref_viruses_rep_genomes \
    --medaka_model r941_min_high_g360 \
    -profile docker
```

**With Singularity (HPC):**
```bash
nextflow run main.nf \
    --input samplesheet.csv \
    --outdir results \
    --host_genome /path/to/hg38.fa \
    --kraken2_db /path/to/kraken2_viral_db \
    --blast_db /path/to/blast_db \
    -profile singularity,slurm
```

---

## Parameters

### Required

| Parameter      | Description                                      |
|----------------|--------------------------------------------------|
| `--input`      | Path to samplesheet CSV                          |
| `--kraken2_db` | Path to Kraken2 database directory               |

### Optional

| Parameter           | Default              | Description                                    |
|---------------------|----------------------|------------------------------------------------|
| `--outdir`          | `results`            | Output directory                               |
| `--host_genome`     | `null`               | Host genome FASTA for depletion                |
| `--blast_db`        | `null`               | BLAST nucleotide database path                 |
| `--diamond_db`      | `null`               | Diamond protein database path                  |
| `--min_length`      | `200`                | Minimum read length (bp)                       |
| `--max_length`      | `50000`              | Maximum read length (bp)                       |
| `--min_quality`     | `8`                  | Minimum mean read quality (Phred)              |
| `--medaka_model`    | `r941_min_high_g360` | Medaka model (match your flow cell)            |
| `--flye_genome_size`| `5m`                 | Expected genome size for Flye assembly         |
| `--kraken2_confidence` | `0.05`           | Kraken2 confidence threshold                   |
| `--bracken_level`   | `S`                  | Bracken taxonomic level (S=Species, G=Genus)   |
| `--multiqc_title`   | Pipeline default     | Custom title for MultiQC report                |

### Medaka models

Select the model matching your flow cell and basecaller version:

| Flow cell | Basecaller | Model                    |
|-----------|------------|--------------------------|
| R9.4.1    | Guppy High | `r941_min_high_g360`     |
| R9.4.1    | Guppy Sup  | `r941_min_sup_g507`      |
| R10.4.1   | Dorado     | `r1041_e82_400bps_hac_v4.2.0` |
| R10.4.1   | Dorado Sup | `r1041_e82_400bps_sup_v4.2.0` |

---

## Output Structure

```
results/
├── qc/
│   ├── nanoplot/
│   │   ├── <sample>_raw/          # Raw read QC plots
│   │   └── <sample>_filtered/     # Post-filter QC plots
│   └── nanostat/
│       ├── <sample>_raw/          # Raw read statistics
│       └── <sample>_filtered/     # Post-filter statistics
├── trimmed/                       # Adapter-trimmed reads
├── filtered/                      # Length/quality-filtered reads
├── host_depletion/                # Host-depleted reads + flagstat
├── classification/
│   ├── kraken2/                   # Kraken2 reports and outputs
│   ├── bracken/                   # Bracken abundance estimates
│   └── krona/                     # Interactive Krona HTML charts
├── assembly/
│   ├── flye/                      # De novo assemblies
│   └── medaka/                    # Polished consensus genomes
├── annotation/
│   ├── blast/                     # BLASTn results (tabular + XML)
│   └── diamond/                   # Diamond blastx results
├── multiqc/
│   ├── multiqc_report.html        # ← Main QC report (open this)
│   └── multiqc_data/              # Raw data tables
└── pipeline_info/
    ├── execution_report.html      # Nextflow execution report
    ├── execution_timeline.html    # Process timeline
    └── execution_trace.txt        # Detailed resource usage
```

---

## QC Report

The MultiQC report (`results/multiqc/multiqc_report.html`) aggregates:

- **Read statistics** — total reads, bases, N50, mean/median length and quality
- **Quality distributions** — read length and quality score histograms
- **Host depletion** — % reads mapped to host vs retained for viral analysis
- **Kraken2 classification** — classified/unclassified read proportions per sample
- **Bracken abundances** — species-level viral abundance estimates

---

## Citation

If you use this pipeline, please cite the underlying tools:

- **NanoPlot/NanoStat**: De Coster et al. (2018) *Bioinformatics*
- **Porechop**: Wick et al. (2017) *GitHub*
- **Filtlong**: Wick (2021) *GitHub*
- **Minimap2**: Li (2018) *Bioinformatics*
- **Kraken2**: Wood et al. (2019) *Genome Biology*
- **Bracken**: Lu et al. (2017) *PeerJ CS*
- **Flye**: Kolmogorov et al. (2019) *Nature Methods*
- **Medaka**: Oxford Nanopore Technologies *GitHub*
- **BLAST**: Camacho et al. (2009) *BMC Bioinformatics*
- **Diamond**: Buchfink et al. (2021) *Nature Methods*
- **MultiQC**: Ewels et al. (2016) *Bioinformatics*
