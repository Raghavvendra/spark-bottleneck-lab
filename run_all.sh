#!/usr/bin/env bash
# Reproduce everything: data -> measured runs -> diagnosis -> figures.
# Usage: ./run_all.sh [CORES] [SCALE]      e.g. ./run_all.sh 4 3
set -euo pipefail
CORES="${1:-1}"; SCALE="${2:-1}"
python -m pytest tests -q
[ -f data/meta.json ] || python -m bottleneck_lab.scenarios setup --data data --scale "$SCALE"
python -m bottleneck_lab.run_lab --reps 3 --cores "$CORES"
python -m bottleneck_lab.plots --results results
