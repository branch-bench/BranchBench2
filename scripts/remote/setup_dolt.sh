#!/usr/bin/env bash
# Dolt 2.4.2 release binary as a systemd service (sql-server on :3306).
set -euo pipefail
D=$HOME/dolt
mkdir -p "$D/bin" "$D/databases" "$D/download"
if [[ ! -x "$D/bin/dolt" ]]; then
    curl -sL -o "$D/download/dolt.tar.gz" https://github.com/dolthub/dolt/releases/download/v2.4.2/dolt-linux-amd64.tar.gz
    tar xzf "$D/download/dolt.tar.gz" -C "$D/download"
    cp "$D/download/dolt-linux-amd64/bin/dolt" "$D/bin/dolt"
fi
"$D/bin/dolt" config --global --add user.name benchmark >/dev/null
"$D/bin/dolt" config --global --add user.email benchmark@example.com >/dev/null
sudo -n tee /etc/systemd/system/dolt.service >/dev/null <<UNIT
[Unit]
Description=Dolt 2.4.2 sql-server (db-fork benchmark)
After=network.target
[Service]
User=ubuntu
WorkingDirectory=$D
ExecStart=$D/bin/dolt sql-server --host 0.0.0.0 --port 3306 --data-dir $D/databases
Restart=on-failure
LimitNOFILE=65536
[Install]
WantedBy=multi-user.target
UNIT
sudo -n systemctl daemon-reload && sudo -n systemctl enable --now dolt.service
sleep 5; systemctl is-active dolt.service; ss -ltn | grep ':3306 ' && echo "dolt up: $($D/bin/dolt version | head -1)"
# The default root user is root@localhost only; the driver connects from
# another machine.
(cd "$D/databases" && "$D/bin/dolt" sql -q "CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY ''; GRANT ALL ON *.* TO 'root'@'%' WITH GRANT OPTION;" >/dev/null) && echo "root@% created"
