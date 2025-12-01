#!/usr/bin/env bash
set -euo pipefail

# ------------ User-adjustable settings ------------------
MASTER_URL=${MASTER_URL:-spark://vmhadoopmaster.srv.lamsade.dauphine.fr:7077}
PROPERTIES_FILE=${PROPERTIES_FILE:-conf/spark.conf}
JAR_PATH=${JAR_PATH:-flight-assembly.jar}
LOG_DIR=${LOG_DIR:-logs}
DELTA_BASE=${DELTA_BASE:-/students/p6emiasd2025/aduvignac-rosa/delta}
COMMON_ARGS=(--deltaBase "$DELTA_BASE" --sparkMaster="$MASTER_URL")

mkdir -p "$LOG_DIR"

base_cmd=(
  spark-submit
  --master "$MASTER_URL"
  --properties-file "$PROPERTIES_FILE"
  --class com.emiasd.flight.Main
  "$JAR_PATH"
)

submit_stage() {
  local stage="$1"
  shift
  local label="$1"
  shift
  local log_file="$LOG_DIR/${stage}${label}.log"
  printf "[+] Starting stage '%s'%s (log: %s)\n" "$stage" "${label:+ $label}" "$log_file" >&2
  (
    "${base_cmd[@]}" --stage="$stage" "${COMMON_ARGS[@]}" "$@"
  ) >"$log_file" 2>&1 &
  echo $!
}

wait_for() {
  local pid="$1"
  shift || true
  if [[ -n "$pid" ]]; then
    wait "$pid"
  fi
}

# 1. Bronze (sequential)
bronze_pid=$(submit_stage bronze "")
wait_for "$bronze_pid"

# 2. Silver (sequential)
silver_pid=$(submit_stage silver "")
wait_for "$silver_pid"

# 3. Gold thresholds (parallel)
gold_thresholds=(15 30 45 60 90)
gold_pids=()
for th in "${gold_thresholds[@]}"; do
  gold_pids+=("$(submit_stage gold "-th${th}" --th "$th")")
done
for pid in "${gold_pids[@]}"; do
  wait_for "$pid"
done

# 4. ML experiments (parallel example; adjust matrix as needed)
ml_matrix=(
  "D2 60 0 0 Baseline_D2_th60_noWeather"
  "D2 60 7 7 S1_origin7h_dest7h_D2_th60"
  "D2 15 7 7 S2_D2_th15_origin7h_dest7h"
  "D2 30 7 7 S2_D2_th30_origin7h_dest7h"
  "D2 45 7 7 S2_D2_th45_origin7h_dest7h"
  "D2 60 7 7 S2_D2_th60_origin7h_dest7h"
  "D2 90 7 7 S2_D2_th90_origin7h_dest7h"
)

ml_pids=()
for entry in "${ml_matrix[@]}"; do
  read -r ds th origin dest tag <<<"$entry"
  ml_pids+=("$(submit_stage ml "-${tag}" --ds "$ds" --th "$th" --originHours "$origin" --destHours "$dest" --tag "$tag")")
done
for pid in "${ml_pids[@]}"; do
  wait_for "$pid"
done

printf "[✔] Pipeline completed. Logs in %s\n" "$LOG_DIR" >&2
