#!/usr/bin/env bash
# Runs on every server machine: driver ssh key, base packages, dolt identity.
set -euo pipefail
PUBKEY="$1"
grep -qF "$PUBKEY" ~/.ssh/authorized_keys || echo "$PUBKEY" >> ~/.ssh/authorized_keys
export DEBIAN_FRONTEND=noninteractive
sudo -n apt-get update -qq >/dev/null
sudo -n apt-get install -y -qq rsync curl xfsprogs >/dev/null
echo "common done on $(hostname) $(hostname -I | awk '{print $1}')"
