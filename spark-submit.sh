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

echo "=============================================="
echo "🚀 Lancement de Spark"
echo "=============================================="
echo "🧱 JAR          : $JAR"
echo "🏷️  Classe      : $MAIN_CLASS"
echo "🪵 Log4j conf   : $LOG_CONF"
echo "🪵 flight conf  : $CFG_FILE"
echo "=============================================="

# --- Submit Spark job ---
args=(
  --master "spark://spark-master:7077"
  --class "$MAIN_CLASS"
  --conf "spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8 -Dlog4j.configuration=$LOG_CONF"
  --conf "spark.executor.extraJavaOptions=-Dfile.encoding=UTF-8 -Dlog4j.configuration=$LOG_CONF"
  --conf "spark.app.config=$CFG_FILE"

  # Recos mémoire/ressources
  --conf "spark.driver.memory=4g"
  --conf "spark.executor.memory=6g"
  --conf "spark.executor.cores=2"
  --conf "spark.executor.instances=4"
# Overhead natif (shuffle, sérialisation, JNI…)
--conf "spark.executor.memoryOverhead=2048" \
# Off-heap (Tungsten, buffers)
--conf "spark.memory.offHeap.enabled=true" \
--conf "spark.memory.offHeap.size=1024m" \
# Kryo buffers assez grands pour éviter les OOM sérialisation
--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
--conf "spark.kryoserializer.buffer=64m" \
--conf "spark.kryoserializer.buffer.max=1024m" \

  # Disque de shuffle
  --conf "spark.local.dir=${SPARK_LOCAL_DIRS:-/opt/spark/local}"

  # AQE / partitions
  --conf "spark.sql.adaptive.enabled=true"
  --conf "spark.sql.adaptive.coalescePartitions.enabled=true"
  --conf "spark.sql.adaptive.advisoryPartitionSizeInBytes=64MB"
  --conf "spark.sql.adaptive.skewJoin.enabled=true"
  --conf "spark.sql.shuffle.partitions=96"
  --conf "spark.default.parallelism=96"

  # Résilience réseau & shuffle
  --conf "spark.network.timeout=600s"
  --conf "spark.executor.heartbeatInterval=30s"
  --conf "spark.shuffle.compress=true"
  --conf "spark.shuffle.spill.compress=true"

  # Fichiers de sortie
  --conf "spark.sql.files.maxRecordsPerFile=1000000"
)

spark-submit "${args[@]}" "$JAR" -- "$STAGE"
