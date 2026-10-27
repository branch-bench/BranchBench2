#!/usr/bin/env bash
# Provision one backend's server machine from the driver:
#   scripts/remote/provision.sh <backend>     (dolt dolt_mysql seekdb matrixone file_copy chronos)
# Reads hosts.env, installs the driver's ssh key, copies builds where needed
# and runs the matching setup_*.sh on the machine. Idempotent.
set -euo pipefail
cd "$(dirname "$0")/../.."
source scripts/remote/hosts.env
b="$1"; var="HOST_$b"; host="${!var}"
SSH=(ssh -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 "$SSH_USER@$host")
RSYNC=(rsync -az --info=progress2 -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=accept-new")
echo "== $b on $host"
"${SSH[@]}" "bash -s -- '$(cat ~/.ssh/id_ed25519.pub)'" < scripts/remote/common.sh
# Never let unattended-upgrades restart a database service mid-sweep.
"${SSH[@]}" bash -s < scripts/remote/disable_auto_upgrades.sh
case "$b" in
    dolt)       "${SSH[@]}" bash -s < scripts/remote/setup_doltgres.sh ;;
    dolt_mysql) "${SSH[@]}" bash -s < scripts/remote/setup_dolt.sh ;;
    seekdb)
        "${SSH[@]}" mkdir -p ~/seekdb
        "${RSYNC[@]}" ~/seekdb/bin "$SSH_USER@$host:seekdb/"
        "${SSH[@]}" bash -s < scripts/remote/setup_seekdb.sh ;;
    matrixone)
        "${SSH[@]}" mkdir -p ~/mo/matrixone/thirdparties/install
        "${RSYNC[@]}" ~/mo/matrixone/mo-service ~/mo/matrixone/etc "$SSH_USER@$host:mo/matrixone/"
        "${RSYNC[@]}" ~/mo/matrixone/thirdparties/install/lib "$SSH_USER@$host:mo/matrixone/thirdparties/install/"
        "${SSH[@]}" bash -s < scripts/remote/setup_matrixone.sh ;;
    file_copy|chronos)
        pw=$(grep -E '^PGSQL_PASSWORD=' .env | cut -d= -f2- | tr -d '"')
        [[ -n "$pw" ]] || { echo "PGSQL_PASSWORD missing in .env"; exit 1; }
        printf '%s\n' "$pw" | "${SSH[@]}" 'umask 077; cat > ~/pgpw'
        "${SSH[@]}" "bash -s -- $DRIVER_PRIVATE_IP" < scripts/remote/setup_pg18.sh ;;
    *) echo "unknown backend $b"; exit 1 ;;
esac
echo "== $b done"
