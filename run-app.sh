#!/usr/bin/env bash
set -e  # Stop on first error

# =========================================================
# Script de build + lancement du cluster Spark + job
# =========================================================

# === Paramètres par défaut ===
BUILD=false
LOCAL=true
RESET=false
STAGE="all"
DATA_DIR_PATH=${DATA_PATH:-./data}

# === Parsing des arguments ===
while [[ $# -gt 0 ]]; do
  case "$1" in
    --build)
    BUILD=true
    ;;
    --local)
    LOCAL=true
    ;;
    --reset)
    RESET=true
    ;;
    --stage=*)
    STAGE="${1#*=}"
    ;;
    *)
    echo "⚠️  Argument inconnu : $1"
    ;;
  esac
  shift
done

if [ "$LOCAL" = true ]; then
  if [ ! -d "$DATA_DIR_PATH" ] || [ -z "$(ls -A $DATA_DIR_PATH)" ]; then
    echo "[run-app] Dataset manquant. Arrêt."
    exit 1
  fi
  echo "[run-app] Mode local détecté : dataset présent, exécution Spark directe."
  ./spark-submit.sh "$STAGE"
fi

ASSEMBLY_JAR="target/scala-2.12/flight-assembly.jar"

# =========================================================
# Étape 0 : Vérification de la présence et rapatriement du dataset
# =========================================================
echo "📁 Vérification du dataset..."
./get-data.sh

if [ $? -ne 0 ]; then
  echo "❌ Erreur lors du téléchargement du dataset."
  exit 1
fi

# =========================================================
# Étape 1 : Compilation du projet Scala (si build, fat JAR)
# =========================================================
if [ "$BUILD" = true ]; then
  echo "🔧 Compilation du projet Scala avec sbt-assembly..."

  # Vérifie que sbt est installé
  if ! command -v sbt &>/dev/null; then
      echo "❌ Erreur : 'sbt' n'est pas installé sur la machine hôte."
      exit 1
  fi

  # Nettoyage et création du jar assemblé
  if sbt clean assembly; then
      echo "✅ Compilation et assembly réussis."
  else
      echo "❌ Échec de la compilation Scala."
      exit 1
  fi

  # Vérifie que le JAR assemblé existe
  if [ ! -f "$ASSEMBLY_JAR" ]; then
      echo "❌ Fichier $ASSEMBLY_JAR introuvable après l'assembly."
      exit 1
  fi
fi

# =========================================================
# Étape 2 : (Re)démarrage du cluster Spark via Docker
# =========================================================
echo "🧹 Arrêt de tout cluster Spark existant..."
docker rm -f spark-submit spark-worker spark-master >/dev/null 2>&1 || true

echo " Création du répertoire local de shuffle..."
mkdir -p ./spark-local   # +++ support shuffle local

echo "🚀 Démarrage du cluster Spark..."
docker compose up -d

echo "⏳ Attente de la disponibilité du Spark Master..."
for i in {1..15}; do
  if docker logs spark-master 2>&1 | grep -q "Starting Spark master"; then
    break
  fi
  echo "⏳ Spark master en préparation..."
  sleep 2
done

if [ "$RESET" = true ]; then
  echo "🧹 Suppression du répertoire delta (via conteneur root)..."
  # Attente du conteneur worker
  worker_found=false
  for i in {1..10}; do
    if docker ps | grep -q spark-worker; then
      worker_found=true
      # Vérifier l'accessibilité du répertoire delta
      if docker compose exec -u root spark-worker bash -c "[ -d /app/delta ]"; then
        if docker compose exec -u root spark-worker bash -c "[ -w /app/delta ]"; then
          docker compose exec -u root spark-worker bash -c "rm -rf /app/delta/* || true"
          echo "✅ Répertoire delta nettoyé."
        else
          echo "❌ Le répertoire /app/delta existe mais n'est pas accessible en écriture dans le conteneur spark-worker."
        fi
      else
        echo "❌ Le répertoire /app/delta n'existe pas dans le conteneur spark-worker."
      fi
      break
    fi
    echo "⏳ Attente du démarrage du spark-worker..."
    sleep 2
  done
  if [ "$worker_found" = false ]; then
    echo "❌ Le conteneur spark-worker n'est pas en cours d'exécution. Impossible de réinitialiser le répertoire delta."
  fi
fi

# =========================================================
# Étape 3 : Copie du JAR dans le conteneur spark-submit
# =========================================================
echo "📦 Copie du jar assemblé dans le conteneur..."
docker cp "$ASSEMBLY_JAR" spark-submit:/app/flight-assembly.jar

# =========================================================
# Étape 4 : Préparation du script spark-submit.sh
# =========================================================
echo "⚙️  Préparation du script spark-submit.sh..."
docker exec spark-submit dos2unix /app/spark-submit.sh >/dev/null 2>&1 || true

# =========================================================
# Étape 5 : Soumission du job Spark
# =========================================================
echo "🚀 Soumission du job Spark..."
echo "----------------------------------------------"
echo "Build : $BUILD"
echo "Stage : $STAGE"
echo "----------------------------------------------"
docker exec spark-submit /app/spark-submit.sh "$STAGE"

echo ""
echo "📜 Logs du conteneur spark-submit :"
docker logs spark-submit

echo ""
echo "✅ Job Spark terminé avec succès."