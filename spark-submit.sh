#!/usr/bin/env bash
set -e

# ==============================================
# 🚀 Spark Submit Script (Template compatible)
# ==============================================

# --- Auto-detect last built JAR ---
JAR=$(find target/scala-* -name "*.jar" | sort -r | head -n 1)
if [ -z "$JAR" ]; then
  echo "❌ Aucun JAR trouvé. Exécutez d'abord : sbt clean package"
  exit 1
fi

# --- Detect package name from Main.scala ---
PACKAGE_PATH=$(find src/main/scala -type f -name "Main.scala" | head -n 1)
if grep -q '^package ' "$PACKAGE_PATH"; then
  PACKAGE=$(grep '^package ' "$PACKAGE_PATH" | awk '{print $2}')
  MAIN_CLASS="$PACKAGE.Main"
else
  MAIN_CLASS="Main"
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