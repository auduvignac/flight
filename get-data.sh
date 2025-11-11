#!/usr/bin/env bash
set -e

DATA_DIR="./data"
FLIGHTS_DIR="$DATA_DIR/flights"
ZIP_FILE="$DATA_DIR/flights.zip"
URL="https://www.dropbox.com/sh/iasq7frk6f58ptq/AAAzSmk6cusSNfqYNYsnLGIXa?dl=1"
REQUIRED_GB=6

mkdir -p "$DATA_DIR"

# Vérifie l'espace disque
AVAILABLE_GB=$(df -BG "$DATA_DIR" | tail -1 | awk '{print $4}' | sed 's/G//')
if (( AVAILABLE_GB < REQUIRED_GB )); then
  echo "❌ Espace disque insuffisant : ${AVAILABLE_GB} Go disponibles, ${REQUIRED_GB} Go requis."
  exit 1
fi

# Vérifie si déjà téléchargé
if [ -d "$FLIGHTS_DIR" ]; then
  echo "✅ Dataset déjà présent dans $FLIGHTS_DIR"
  exit 0
fi

echo "🛰️ Téléchargement du dataset (~5 Go)..."
wget --progress=bar:force -O "$ZIP_FILE" "$URL"

echo "📦 Extraction dans $FLIGHTS_DIR..."
mkdir -p "$FLIGHTS_DIR"
unzip -q "$ZIP_FILE" -d "$FLIGHTS_DIR"

echo "🧹 Nettoyage du fichier ZIP..."
rm -f "$ZIP_FILE"

echo "✅ Dataset prêt dans $FLIGHTS_DIR"
