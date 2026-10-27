#!/usr/bin/env bash
# Doltgres 1.4.0 release binary as a systemd service on :5432, bound to all
# interfaces (the security group limits who can reach it).
set -euo pipefail
D=$HOME/doltgres-1.4.0
mkdir -p "$D/dl" "$D/databases"
if [[ ! -x "$D/dl/doltgresql-linux-amd64/bin/doltgres" ]]; then
    curl -sL -o "$D/dl/dg.tar.gz" https://github.com/dolthub/doltgresql/releases/download/v1.4.0/doltgresql-linux-amd64.tar.gz
    tar xzf "$D/dl/dg.tar.gz" -C "$D/dl"
fi
printf 'listener:\n  host: 0.0.0.0\n  port: 5432\ndata_dir: %s\n' "$D/databases" > "$D/config.yaml"
mkdir -p ~/.dolt
[[ -f ~/.dolt/config_global.json ]] || echo '{"user.email":"benchmark@example.com","user.name":"benchmark"}' > ~/.dolt/config_global.json
sudo -n tee /etc/systemd/system/doltgres.service >/dev/null <<UNIT
[Unit]
Description=Doltgres 1.4.0 (db-fork benchmark)
After=network.target
[Service]
User=ubuntu
WorkingDirectory=$D
ExecStart=$D/dl/doltgresql-linux-amd64/bin/doltgres -config $D/config.yaml
Restart=on-failure
LimitNOFILE=65536
[Install]
WantedBy=multi-user.target
UNIT
sudo -n systemctl daemon-reload && sudo -n systemctl enable --now doltgres.service
sleep 5; systemctl is-active doltgres.service; ss -ltn | grep ':5432 ' && echo "doltgres up: $($D/dl/doltgresql-linux-amd64/bin/doltgres --version)"
