#!/usr/bin/env bash
set -e

# ==============================================
# 🚀 Spark Submit Script (Template compatible)
# ==============================================

# --- Vérification des arguments ---
if [ $# -lt 1 ]; then
  echo "Usage:"
  echo "  $0 <stage>"
  echo "  stage ∈ {bronze, silver, gold, all}"
  exit 1
fi

# --- Paramètres par défaut ---
STAGE=${1:-"all"}

# --- JAR location (cf. run-app.sh) ---
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
SPARK_CONF="/opt/spark/conf/spark.conf"

echo "=============================================="
echo "🚀 Lancement de Spark"
echo "=============================================="
echo "JAR         : $JAR"
echo "Classe      : $MAIN_CLASS"
echo "Log4j conf  : $LOG_CONF"
echo "flight conf : $CFG_FILE"
echo "spark conf  : $SPARK_CONF"
echo "stage       : $STAGE"
echo "=============================================="

spark-submit \
  --properties-file "$SPARK_CONF" \
  --class "$MAIN_CLASS" \
  "$JAR" "$STAGE"