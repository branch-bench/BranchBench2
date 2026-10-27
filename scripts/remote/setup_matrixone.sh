#!/usr/bin/env bash
# MatrixOne 4.2.5: mo-service, etc/ and its bundled library were rsynced to
# ~/mo/matrixone beforehand (same paths as on the driver, which the binary's
# rpath expects).
set -euo pipefail
D=$HOME/mo/matrixone
[[ -x "$D/mo-service" ]] || { echo "missing $D/mo-service (rsync first)"; exit 1; }
mkdir -p "$D/mo-data"
sudo -n tee /etc/systemd/system/matrixone.service >/dev/null <<UNIT
[Unit]
Description=MatrixOne 4.2.5 (db-fork benchmark)
After=network.target
[Service]
User=ubuntu
WorkingDirectory=$D
ExecStart=$D/mo-service -launch $D/etc/launch/launch.toml
StandardOutput=append:$HOME/mo/mo-service.log
StandardError=append:$HOME/mo/mo-service.log
Restart=on-failure
LimitNOFILE=655360
[Install]
WantedBy=multi-user.target
UNIT
sudo -n systemctl daemon-reload && sudo -n systemctl enable --now matrixone.service
for i in $(seq 1 90); do ss -ltn | grep -q ':6001 ' && break; sleep 2; done
systemctl is-active matrixone.service; ss -ltn | grep ':6001 ' && echo "matrixone listening"
