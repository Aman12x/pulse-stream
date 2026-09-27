#!/usr/bin/env bash
# Copy the capture's Parquet archive from the VM into analytics/data/ for dbt.
set -euo pipefail
HOST=${HOST:-opc@157.151.255.207}
cd "$(dirname "$0")/../analytics"
mkdir -p data
ssh "$HOST" 'sg docker -c "docker run --rm -v capture_archive:/archive alpine tar -C /archive -cf - ."' | tar -C data -xf - 2>/dev/null || true
mkdir -p data/archive && find data -maxdepth 1 -name 'topic=*' -exec mv {} data/archive/ \; 2>/dev/null || true
find data/archive -name '*.parquet' | wc -l | xargs echo "parquet files:"
