#!/usr/bin/env bash
set -e

# ==============================================
# 🚀 Spark Submit Script (Template compatible)
# ==============================================

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

echo "=============================================="
echo "🚀 Lancement de Spark"
echo "=============================================="
echo "🧱 JAR          : $JAR"
echo "🏷️  Classe      : $MAIN_CLASS"
echo "🪵 Log4j conf   : $LOG_CONF"
echo "🪵 flight conf  : $CFG_FILE"
echo "=============================================="

# --- Submit Spark job ---
spark-submit \
  --master spark://spark-master:7077 \
  --class "$MAIN_CLASS" \
  --conf "spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8 -Dlog4j.configuration=$LOG_CONF" \
  --conf "spark.executor.extraJavaOptions=-Dfile.encoding=UTF-8 -Dlog4j.configuration=$LOG_CONF" \
  --conf "spark.app.config=$CFG_FILE" \
  "$JAR"