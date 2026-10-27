#!/usr/bin/env bash
# Put a copy of this repo, with its own .venv and .env, on a backend's server
# machine so that backend's driver process can run there
# (<BACKEND>_DRIVER_SSH in .env, see run_macro_all.sh). Used for Chronos,
# whose versioning is a client library: run next to its PostgreSQL it issues
# its statements locally, like the other engines' versioning code.
#
#   scripts/remote/setup_driver.sh chronos
#
# Idempotent: rsyncs the working tree (code, configs, generated *_pb2.py;
# no results, figures, venvs, git history or paper), installs uv and the
# build packages chronos-core needs, syncs the venv, and writes a .env that
# is this machine's .env with the per-backend host variables removed and the
# backend's own host set to 127.0.0.1.
set -euo pipefail
cd "$(dirname "$0")/../.."
source scripts/remote/hosts.env
b="$1"; var="HOST_$b"; host="${!var}"
SSH=(ssh -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 "$SSH_USER@$host")
RSYNC=(rsync -az --delete -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=accept-new")
REPO=db-fork
case "$b" in
    chronos)   extra="chronos"; hostvar=CHRONOS_HOST ;;
    file_copy) extra=""; hostvar=FILE_COPY_HOST ;;
    *) echo "setup_driver.sh: no driver recipe for $b"; exit 1 ;;
esac
echo "== driver for $b on $host"

"${RSYNC[@]}" --exclude run_stats --exclude run_stats_final --exclude figures --exclude .venv --exclude venv \
    --exclude build --exclude '*.egg-info' --exclude .git --exclude node_modules --exclude '*.img' \
    --exclude __pycache__ --exclude .env --exclude docs/paper --exclude '*.log' \
    ./ "$SSH_USER@$host:$REPO/"

# .env: drop every per-backend host/ssh variable, then point this backend at localhost.
grep -v -E '^(DBFORK_SERVER_HOST|DBFORK_SERVER_SSH|[A-Z_]+_HOST|[A-Z_]+_SSH|[A-Z_]+_DRIVER_SSH)=' .env \
    | grep -v -E '^PGSQL_HOST=' > /tmp/driver_env.$$
printf '\n# Driver co-located with the %s server (written by setup_driver.sh)\nPGSQL_HOST=127.0.0.1\n%s=127.0.0.1\n' "$b" "$hostvar" >> /tmp/driver_env.$$
"${SSH[@]}" 'umask 077; cat > '"$REPO"'/.env' < /tmp/driver_env.$$
rm -f /tmp/driver_env.$$

"${SSH[@]}" bash -s -- "$REPO" "$extra" <<'EOF'
set -euo pipefail
REPO="$1"; EXTRA="${2:-}"; [[ -z "$EXTRA" ]] || EXTRA="--extra $EXTRA"
export DEBIAN_FRONTEND=noninteractive
sudo -n apt-get install -y -qq libpq-dev libboost-dev libsqlite3-dev python3-dev g++ cmake pkg-config protobuf-compiler rsync >/dev/null
command -v ~/.local/bin/uv >/dev/null || curl -LsSf https://astral.sh/uv/install.sh | sh >/dev/null 2>&1
export PATH="$HOME/.local/bin:$PATH"
cd "$REPO"
uv python install 3.13 >/dev/null 2>&1 || true
uv sync $EXTRA >/dev/null
uv run python -c "import macrobench.task_pb2, psycopg; print('driver venv ok on', __import__('socket').gethostname())"
EOF
