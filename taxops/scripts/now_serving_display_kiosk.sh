#!/usr/bin/env bash
# Raspberry Pi Chromium kiosk for TaxOps Now Serving lobby display.
# Usage:
#   export TAXOPS_URL="http://192.168.1.173:5000"
#   ./now_serving_display_kiosk.sh
#
# Optional: TAXOPS_DISPLAY_URL overrides the full path.
set -euo pipefail

TAXOPS_URL="${TAXOPS_URL:-http://192.168.1.173:5000}"
DISPLAY_URL="${TAXOPS_DISPLAY_URL:-${TAXOPS_URL%/}/now-serving/display?autosound=1}"

# Disable screensaver / blanking when possible (Pi OS).
if command -v xset >/dev/null 2>&1; then
  xset s off || true
  xset -dpms || true
  xset s noblank || true
fi

CHROMIUM=""
for c in chromium-browser chromium google-chrome; do
  if command -v "$c" >/dev/null 2>&1; then
    CHROMIUM="$c"
    break
  fi
done

if [[ -z "$CHROMIUM" ]]; then
  echo "Install Chromium: sudo apt install chromium-browser" >&2
  exit 1
fi

exec "$CHROMIUM" \
  --kiosk \
  --noerrdialogs \
  --disable-infobars \
  --disable-session-crashed-bubble \
  --check-for-update-interval=31536000 \
  --autoplay-policy=no-user-gesture-required \
  --disable-features=TranslateUI \
  --lang=en-US \
  "$DISPLAY_URL"
