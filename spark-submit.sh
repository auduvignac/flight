#!/usr/bin/env bash
set -e

# --- Vérification des arguments ---
if [ $# -lt 1 ]; then
  echo "Usage:"
  echo "  $0 <stage> [--ds=D2 --th=60 --originHours=7 --destHours=7 --tag=MyExp]"
  echo "  stage ∈ {bronze, silver, gold, all}"
  exit 1
fi

# --- Paramètres par défaut ---
STAGE=$1
shift  # Supprime le premier argument (stage)
EXTRA_ARGS="$@"

# --- JAR location ---
JAR="/app/flight-assembly.jar"
MAIN_CLASS="com.emiasd.flight.Main"

if [ ! -f "$JAR" ]; then
  echo "❌ Fichier JAR introuvable à l'emplacement $JAR"
  exit 1
fi

# --- Configuration --------------
# --- log configuration ---
LOG_CONF=${LOG_CONFIG_PATH:-/opt/spark/conf/log4j2.properties}
# --- application configuration ---
CFG_FILE=${APPLICATION_CONFIG_PATH:-/opt/config/application.conf}
# --- configuration spark ---
SPARK_CONF=${SPARK_CONFIG_PATH:-/opt/spark/conf/spark.conf}

echo "=============================================="
echo "🚀 Lancement de Spark"
echo "=============================================="
echo "JAR         : $JAR"
echo "Classe      : $MAIN_CLASS"
echo "Log4j conf  : $LOG_CONF"
echo "flight conf : $CFG_FILE"
echo "spark conf  : $SPARK_CONF"
echo "Stage       : $STAGE"
echo "Args        : $EXTRA_ARGS"
echo "=============================================="

spark-submit \
  --properties-file "$SPARK_CONF" \
  --class "$MAIN_CLASS" \
  "$JAR" \
  --stage="$STAGE" \
  $EXTRA_ARGS