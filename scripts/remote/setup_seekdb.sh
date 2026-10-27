#!/usr/bin/env bash
# SeekDB 1.4.0: the binary was rsynced to ~/seekdb/bin beforehand; a fresh
# store is bootstrapped here. Runs in the foreground under systemd.
set -euo pipefail
D=$HOME/seekdb
[[ -x "$D/bin/seekdb" ]] || { echo "missing $D/bin/seekdb (rsync first)"; exit 1; }
mkdir -p "$D/store" "$D/log" "$D/run" "$D/etc"
sudo -n tee /etc/systemd/system/seekdb.service >/dev/null <<UNIT
[Unit]
Description=SeekDB 1.4.0 (db-fork benchmark)
After=network.target
[Service]
User=ubuntu
WorkingDirectory=$D
ExecStart=$D/bin/seekdb --nodaemon
Restart=on-failure
LimitNOFILE=655360
LimitCORE=0
[Install]
WantedBy=multi-user.target
UNIT
sudo -n systemctl daemon-reload && sudo -n systemctl enable --now seekdb.service
for i in $(seq 1 90); do ss -ltn | grep -q ':2881 ' && break; sleep 2; done
systemctl is-active seekdb.service; ss -ltn | grep ':2881 ' && echo "seekdb listening (bootstrap may continue for a while)"
