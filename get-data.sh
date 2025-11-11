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

# ==============================================
# 🔍 Vérifie l'espace disque (compatible mac/Linux)
# ==============================================
if [[ "$OSTYPE" == "darwin"* ]]; then
  # macOS — df affiche des blocs de 512 octets
  AVAILABLE_GB=$(df -g "$DATA_DIR" | tail -1 | awk '{print $4}')
else
  # Linux — df -BG est supporté
  AVAILABLE_GB=$(df -BG "$DATA_DIR" | tail -1 | awk '{print $4}' | sed 's/G//')
fi

if (( AVAILABLE_GB < REQUIRED_GB )); then
  echo "❌ Espace disque insuffisant : ${AVAILABLE_GB} Go disponibles, ${REQUIRED_GB} Go requis."
  exit 1
fi

# ==============================================
# ✅ Vérifie la présence du dataset complet
# ==============================================
if [ -d "$NORMALIZED_FLIGHTS_DIR" ] && [ -d "$NORMALIZED_WEATHER_DIR" ] && [ -f "$WBAN_AIRPORT_TIMEZONE" ]; then
  echo "✅ Dataset complet déjà présent dans $DATA_DIR"
  exit 0
else
  echo "📂 Dataset incomplet ou absent : téléchargement requis."
fi

# ==============================================
# 🌐 Téléchargement compatible mac/Linux
# ==============================================
echo "🛰️ Téléchargement du dataset (~5 Go)..."

if command -v wget &>/dev/null; then
  wget --progress=bar:force -O "$ZIP_FILE" "$URL"
else
  echo "⚙️ wget non trouvé, utilisation de curl"
  curl -L -o "$ZIP_FILE" "$URL"
fi

# ==============================================
# 📦 Extraction avec gestion du timeout portable
# ==============================================
echo "📦 Extraction dans $DATA_DIR..."

# Utilise timeout (Linux) ou gtimeout (mac)
if command -v timeout &>/dev/null; then
  TIMEOUT_CMD="timeout 10m"
elif command -v gtimeout &>/dev/null; then
  TIMEOUT_CMD="gtimeout 10m"
else
  TIMEOUT_CMD=""
fi

if ! $TIMEOUT_CMD unzip -o "$ZIP_FILE" -d "$DATA_DIR" 2> >(grep -vE "stripped absolute path|mapname" >&2 || true); then
  if [ $? -eq 124 ]; then
    echo "⚠️ Extraction interrompue après 10 minutes (timeout atteint)"
  else
    echo "⚠️ unzip a rencontré des erreurs non bloquantes"
  fi
fi

# ==============================================
# 🧹 Nettoyage et normalisation
# ==============================================
echo "🧹 Nettoyage du fichier ZIP..."
rm -f "$ZIP_FILE"

echo "🧹 Normalisation des noms de répertoires..."
if [ -d "$FLIGHTS_DIR" ]; then
  mv "$FLIGHTS_DIR" "$NORMALIZED_FLIGHTS_DIR"
fi

if [ -d "$WEATHER_DIR" ]; then
  mv "$WEATHER_DIR" "$NORMALIZED_WEATHER_DIR"
fi

echo "✅ Dataset prêt dans $DATA_DIR"