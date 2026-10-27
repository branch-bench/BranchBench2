#!/usr/bin/env bash
# PostgreSQL 18 (PGDG) on a 150 GB sparse XFS loop image with reflink, the
# same settings as the driver's server, listening on :5434 for the driver.
# Args: <driver private ip>; the postgres password is read from ~/pgpw (600).
set -euo pipefail
DRIVER_IP="$1"; PWFILE=$HOME/pgpw
[[ -s "$PWFILE" ]] || { echo "missing $PWFILE"; exit 1; }
export DEBIAN_FRONTEND=noninteractive
if [[ ! -x /usr/lib/postgresql/18/bin/postgres ]]; then
    sudo -n install -d /usr/share/postgresql-common/pgdg
    sudo -n curl -s -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc https://www.postgresql.org/media/keys/ACCC4CF8.asc
    echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt noble-pgdg main" | sudo -n tee /etc/apt/sources.list.d/pgdg.list >/dev/null
    sudo -n apt-get update -qq >/dev/null
    sudo -n apt-get install -y -qq postgresql-18 >/dev/null
fi
# The package's default cluster (on :5432) is not used.
sudo -n pg_dropcluster 18 main --stop 2>/dev/null || true
sudo -n systemctl disable --now postgresql 2>/dev/null || true
IMG=$HOME/pgbench.img; MNT=/mnt/pgbench
if [[ ! -f "$IMG" ]]; then
    truncate -s 150G "$IMG"
    mkfs.xfs -q -m reflink=1 "$IMG"
fi
sudo -n mkdir -p "$MNT"
mountpoint -q "$MNT" || sudo -n mount -o loop "$IMG" "$MNT"
grep -q "$MNT" /etc/fstab || echo "$IMG $MNT xfs loop,nofail 0 0" | sudo -n tee -a /etc/fstab >/dev/null
sudo -n chown postgres:postgres "$MNT"
PGBIN=/usr/lib/postgresql/18/bin; PGDATA=$MNT/pgdata
if [[ ! -f "$PGDATA/PG_VERSION" ]]; then
    sudo -n cp "$PWFILE" /tmp/pgpw && sudo -n chown postgres /tmp/pgpw && sudo -n chmod 600 /tmp/pgpw
    sudo -n -u postgres "$PGBIN/initdb" -D "$PGDATA" --data-checksums --locale=C.UTF-8 -E UTF8 \
        --auth-local=scram-sha-256 --auth-host=scram-sha-256 --pwfile=/tmp/pgpw >/dev/null
    sudo -n rm -f /tmp/pgpw
    sudo -n -u postgres tee -a "$PGDATA/postgresql.conf" >/dev/null <<CONF

# db-fork benchmark settings (same as the driver's local server)
port = 5434
listen_addresses = '*'
shared_buffers = 4GB
max_connections = 400
file_copy_method = clone
work_mem = 64MB
maintenance_work_mem = 512MB
max_wal_size = 4GB
checkpoint_completion_target = 0.5
deadlock_timeout = 2s
autovacuum_worker_slots = 16
CONF
    echo "host    all             all             $DRIVER_IP/32          scram-sha-256" | sudo -n -u postgres tee -a "$PGDATA/pg_hba.conf" >/dev/null
    # The server also connects to itself at its VPC address (postgres_fdw
    # servers are defined with the host the driver uses).
    OWN_IP=$(hostname -I | awk '{print $1}')
    echo "host    all             all             $OWN_IP/32          scram-sha-256" | sudo -n -u postgres tee -a "$PGDATA/pg_hba.conf" >/dev/null
fi
sudo -n tee /etc/systemd/system/pg18-bench.service >/dev/null <<UNIT
[Unit]
Description=PostgreSQL 18 on $MNT (db-fork benchmark)
After=network.target
RequiresMountsFor=$MNT
[Service]
Type=forking
User=postgres
ExecStart=$PGBIN/pg_ctl -D $PGDATA -l $MNT/pg.log -w start
ExecStop=$PGBIN/pg_ctl -D $PGDATA -m fast -t 60 -w stop
PIDFile=$PGDATA/postmaster.pid
LimitNOFILE=65536
[Install]
WantedBy=multi-user.target
UNIT
sudo -n systemctl daemon-reload && sudo -n systemctl enable --now pg18-bench.service
sleep 3; systemctl is-active pg18-bench.service; ss -ltn | grep ':5434 ' && echo "pg18 up: $($PGBIN/postgres --version)"
rm -f "$PWFILE"
