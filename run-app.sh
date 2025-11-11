#!/usr/bin/env bash
set -e  # Stop on first error

# =========================================================
# Script de build + lancement du cluster Spark + job
# =========================================================

BUILD=false

# === Parsing des arguments ===
while [[ $# -gt 0 ]]; do
  case "$1" in
    --build)
    BUILD=true
    ;;
    *)
    echo "⚠️  Argument inconnu : $1"
    ;;
  esac
  shift
done

ASSEMBLY_JAR="target/scala-2.12/flight-assembly.jar"

# =========================================================
# Étape 0 : Vérification de la présence et rapatriement du dataset
# =========================================================
echo "📁 Vérification du dataset..."
./get-data.sh

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

echo "🚀 Démarrage du cluster Spark..."
docker compose up -d

echo "⏳ Attente de la disponibilité du Spark Master..."
sleep 5

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
docker exec spark-submit chmod +x /app/spark-submit.sh

# =========================================================
# Étape 5 : Soumission du job Spark
# =========================================================
echo "🚀 Soumission du job Spark..."
docker exec spark-submit /app/spark-submit.sh

echo ""
echo "📜 Logs du conteneur spark-submit :"
docker logs spark-submit

echo ""
echo "✅ Job Spark terminé avec succès."