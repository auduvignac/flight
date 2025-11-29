#!/usr/bin/env bash
set -e

# --- Vérification des arguments ---
if [ $# -lt 1 ]; then
  echo "Usage:"
  echo "  $0 <stage> [--ds=D2 --th=60 --originHours=7 --destHours=7 --tag=MyExp --deltaBase=/app/delta-Exp]"
  echo "  stage ∈ {bronze, silver, gold, ml, all}"
  exit 1
fi

# --- Lecture des arguments ---
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
DEFAULT_SPARK_CONF="/opt/spark/conf/spark-submit.local.conf"
if [ -f "./conf/spark-submit.local.conf" ]; then
  DEFAULT_SPARK_CONF="./conf/spark-submit.local.conf"
fi
SPARK_CONF=${SPARK_CONFIG_PATH:-$DEFAULT_SPARK_CONF}

echo "=============================================="
echo "🚀 Lancement de Spark"
echo "=============================================="
echo "JAR         : $JAR"
echo "Classe      : $MAIN_CLASS"
echo "Log4j conf  : $LOG_CONF"
echo "flight conf : $CFG_FILE"
echo "spark conf  : $SPARK_CONF"
echo "Stage       : $STAGE"
echo "Arguments CLI : $EXTRA_ARGS"
echo "=============================================="

# --- Vérification et journalisation du répertoire Delta ---
if echo "$EXTRA_ARGS" | grep -q -- "--deltaBase="; then
  DELTA_BASE=$(echo "$EXTRA_ARGS" | sed -n 's/.*--deltaBase=\([^ ]*\).*/\1/p')
  echo "📂 Répertoire Delta utilisé : $DELTA_BASE"
  if [ -d "$DELTA_BASE" ]; then
    echo "✅ DeltaBase détecté : $DELTA_BASE"
  else
    echo "⚠️  DeltaBase introuvable localement : $DELTA_BASE"
  fi
fi

# --- Exécution de Spark ---
spark-submit \
  --properties-file "$SPARK_CONF" \
  --class "$MAIN_CLASS" \
  "$JAR" \
  --stage="$STAGE" \
  $EXTRA_ARGS