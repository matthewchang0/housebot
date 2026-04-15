#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/house}"
PYTHON_BIN="${PYTHON_BIN:-python3.13}"

sudo useradd --system --create-home --home-dir /home/house --shell /usr/sbin/nologin house 2>/dev/null || true
sudo mkdir -p "${APP_DIR}"
sudo chown -R "${USER}:$(id -gn)" "${APP_DIR}"

rsync -av --delete \
  --exclude '.git' \
  --exclude '.venv' \
  --exclude '.pytest_cache' \
  ./ "${APP_DIR}/"

cd "${APP_DIR}"
"${PYTHON_BIN}" -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .

# Pre-create writable runtime directories for the systemd service user.
mkdir -p data logs reports
find data logs reports -type d -exec chmod 755 {} \;

install -m 0644 deploy/house.service /tmp/house.service
python3 - "${APP_DIR}" /tmp/house.service <<'PY'
from pathlib import Path
import sys

app_dir = sys.argv[1]
path = Path(sys.argv[2])
path.write_text(path.read_text().replace("/opt/house", app_dir))
PY
sudo mv /tmp/house.service /etc/systemd/system/house.service

install -m 0644 deploy/house-dashboard.service /tmp/house-dashboard.service
python3 - "${APP_DIR}" /tmp/house-dashboard.service <<'PY'
from pathlib import Path
import sys

app_dir = sys.argv[1]
path = Path(sys.argv[2])
path.write_text(path.read_text().replace("/opt/house", app_dir))
PY
sudo mv /tmp/house-dashboard.service /etc/systemd/system/house-dashboard.service

sudo systemctl daemon-reload
sudo systemctl enable house.service
sudo systemctl enable house-dashboard.service

# Keep the git checkout owned by the deploy user so `git pull` works normally,
# but hand runtime storage to the systemd service user.
sudo chown -R house:house "${APP_DIR}/data" "${APP_DIR}/logs" "${APP_DIR}/reports"

echo "Server files installed."
echo "Next:"
echo "  1. Copy your .env into ${APP_DIR}/.env"
echo "  2. sudo systemctl start house.service"
echo "  3. sudo systemctl start house-dashboard.service"
