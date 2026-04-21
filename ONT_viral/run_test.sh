#!/usr/bin/env bash
# =============================================================================
#  ONT Viral Surveillance Pipeline — Test Run Script
#  Uses: test_dataset (53,439 reads, 169 Mb, run 2f9c9c7a)
#  Requires: Nextflow >=23.04, Docker or Singularity, Java 11+
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Colour helpers ────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ── 1. Check prerequisites ────────────────────────────────────────────────────
info "Checking prerequisites..."

command -v java     >/dev/null 2>&1 || error "Java not found. Install Java 11+: sudo apt install default-jdk"
command -v nextflow >/dev/null 2>&1 || {
    warn "Nextflow not found. Installing..."
    curl -s https://get.nextflow.io | bash
    sudo mv nextflow /usr/local/bin/
    info "Nextflow installed: $(nextflow -version 2>&1 | head -1)"
}

# Detect container engine
CONTAINER_PROFILE=""
if command -v docker >/dev/null 2>&1; then
    info "Docker found: $(docker --version)"
    CONTAINER_PROFILE="-profile docker"
elif command -v singularity >/dev/null 2>&1; then
    info "Singularity found: $(singularity --version)"
    CONTAINER_PROFILE="-profile singularity"
else
    error "Neither Docker nor Singularity found. Install one:
  Docker:      curl -fsSL https://get.docker.com | sh
  Singularity: sudo apt install singularity-container"
fi

# ── 2. Check / build required databases ──────────────────────────────────────
info "Checking databases..."

KRAKEN2_DB="${KRAKEN2_DB:-}"
BLAST_DB="${BLAST_DB:-}"
HOST_GENOME="${HOST_GENOME:-}"

if [[ -z "$KRAKEN2_DB" ]]; then
    warn "KRAKEN2_DB not set. Checking for local mini viral DB..."
    MINI_DB="test/data/kraken2_mini_db"
    if [[ ! -d "$MINI_DB" ]]; then
        info "Building minimal Kraken2 viral database (requires ~4GB, ~20 min)..."
        mkdir -p "$MINI_DB"
        kraken2-build --download-taxonomy --db "$MINI_DB" || \
            error "kraken2-build failed. Install kraken2 or set KRAKEN2_DB=/path/to/db"
        kraken2-build --download-library viral --db "$MINI_DB"
        kraken2-build --build --db "$MINI_DB" --threads 4
        info "Kraken2 mini DB built at $MINI_DB"
    fi
    KRAKEN2_DB="$MINI_DB"
fi

# ── 3. Set run parameters ─────────────────────────────────────────────────────
OUTDIR="test_results_$(date +%Y%m%d_%H%M%S)"
SAMPLESHEET="test/samplesheet_test_dataset.csv"

info "Run configuration:"
echo "  Samplesheet : $SAMPLESHEET"
echo "  Kraken2 DB  : $KRAKEN2_DB"
echo "  Host genome : ${HOST_GENOME:-'(skipped)'}"
echo "  BLAST DB    : ${BLAST_DB:-'(skipped)'}"
echo "  Output dir  : $OUTDIR"
echo "  Profile     : $CONTAINER_PROFILE"

# ── 4. Launch pipeline ────────────────────────────────────────────────────────
info "Launching Nextflow pipeline..."

NXF_OPTS="-Xms1g -Xmx4g"

nextflow run main.nf \
    $CONTAINER_PROFILE \
    --input          "$SAMPLESHEET" \
    --kraken2_db     "$KRAKEN2_DB" \
    --outdir         "$OUTDIR" \
    --max_memory     "16.GB" \
    --max_cpus       8 \
    --max_time       "4.h" \
    --min_length     200 \
    --min_quality    8 \
    --coverage_top_n 5 \
    ${HOST_GENOME:+--host_genome "$HOST_GENOME"} \
    ${BLAST_DB:+--blast_db "$BLAST_DB"} \
    -resume \
    -with-report  "$OUTDIR/pipeline_report.html" \
    -with-timeline "$OUTDIR/pipeline_timeline.html" \
    -with-dag      "$OUTDIR/pipeline_dag.svg" \
    2>&1 | tee "$OUTDIR/nextflow.log" || true

# ── 5. Check results ──────────────────────────────────────────────────────────
if [[ -d "$OUTDIR" ]]; then
    info "Pipeline finished. Results in: $OUTDIR"
    echo ""
    echo "Key outputs:"
    find "$OUTDIR" -name "multiqc_report.html"   -exec echo "  MultiQC report  : {}" \;
    find "$OUTDIR" -name "*.krona.html"           -exec echo "  Krona chart     : {}" \;
    find "$OUTDIR" -name "coverage_summary.tsv"   -exec echo "  Coverage summary: {}" \;
    find "$OUTDIR" -name "*.bracken"              -exec echo "  Bracken output  : {}" \;
else
    error "Output directory not created — pipeline may have failed. Check nextflow.log"
fi
