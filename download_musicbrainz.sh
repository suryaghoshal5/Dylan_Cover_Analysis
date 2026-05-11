#!/bin/bash
# MusicBrainz download script with auto-resume

DIR="data/20260502-002346"
BASE_URL="https://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/20260502-002346"

FILES=(
  "mbdump.tar.bz2"
  "mbdump-derived.tar.bz2"
  "mbdump-edit.tar.bz2"
)

echo "Starting download at $(date)"

for file in "${FILES[@]}"; do
  echo ""
  echo "====================================="
  echo "Downloading $file"
  echo "====================================="

  # curl -C - auto-resumes from where it left off
  # -L follows redirects
  # -o specifies output file
  # --retry 10 retries on transient errors
  # --retry-delay 5 waits 5 seconds between retries

  curl -C - -L \
    --retry 10 \
    --retry-delay 5 \
    --retry-max-time 0 \
    -o "$DIR/$file" \
    "$BASE_URL/$file"

  if [ $? -eq 0 ]; then
    echo "✅ Successfully downloaded $file"
  else
    echo "❌ Failed to download $file"
    exit 1
  fi
done

echo ""
echo "====================================="
echo "All downloads complete at $(date)"
echo "====================================="
