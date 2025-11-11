#!/usr/bin/env bash
set -e

DATA_DIR="./data"
FLIGHTS_DIR="$DATA_DIR/Flights"
WEATHER_DIR="$DATA_DIR/Weather"
NORMALIZED_FLIGHTS_DIR="$DATA_DIR/flights"
NORMALIZED_WEATHER_DIR="$DATA_DIR/weather"
WBAN_AIRPORT_TIMEZONE="$DATA_DIR/wban_airport_timezone.csv"
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

# Vérifie si le dataset complet est déjà présent
if [ -d "$NORMALIZED_FLIGHTS_DIR" ] && [ -d "$NORMALIZED_WEATHER_DIR" ] && [ -f "$WBAN_AIRPORT_TIMEZONE" ]; then
  echo "✅ Dataset complet déjà présent dans $DATA_DIR"
  exit 0
else
  echo "📂 Dataset incomplet ou absent : téléchargement requis."
fi

echo "🛰️ Téléchargement du dataset (~5 Go)..."
wget --progress=bar:force -O "$ZIP_FILE" "$URL"

echo "📦 Extraction dans $DATA_DIR..."

if command -v timeout &>/dev/null; then
  # Exécute unzip avec timeout
  if ! timeout 10m unzip -o "$ZIP_FILE" -d "$DATA_DIR" 2> >(grep -vE "stripped absolute path|mapname" >&2); then
    if [ $? -eq 124 ]; then
      echo "⚠️ Extraction interrompue après 10 minutes (timeout atteint)"
    else
      echo "⚠️ Unzip a rencontré des erreurs non bloquantes"
    fi
  fi
else
  # Sans timeout
  if ! unzip -o "$ZIP_FILE" -d "$DATA_DIR" 2> >(grep -vE "stripped absolute path|mapname" >&2); then
    echo "⚠️ Unzip a rencontré des erreurs non bloquantes"
  fi
fi

echo "🧹 Nettoyage du fichier ZIP..."
rm -f "$ZIP_FILE"

echo "🧹 Normalisation du nom des répertoires"
if [ -d "$FLIGHTS_DIR" ]; then
  mv "$FLIGHTS_DIR" "$NORMALIZED_FLIGHTS_DIR"
fi

if [ -d "$DATA_DIR/Weather" ]; then
  mv "$WEATHER_DIR" "$NORMALIZED_WEATHER_DIR"
fi

echo "✅ Dataset prêt dans $DATA_DIR"
