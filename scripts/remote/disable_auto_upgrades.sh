#!/usr/bin/env bash
# Turn off Ubuntu's unattended upgrades on a server machine so a sweep is
# never interrupted by a package upgrade restarting the database service.
#
# Background: on 2026-10-14 06:26-07:01 UTC apt-daily-upgrade ran on all six
# EC2 servers and restarted pg18-bench five times on the file_copy and
# Chronos machines (needrestart restarts services that use upgraded
# libraries), which crashed Chronos S4/S5 and failed Chronos/file_copy S6.
#
# Usage (from the driver):
#   for h in dbf-dolt dbf-dolt_mysql dbf-seekdb dbf-matrixone dbf-file_copy dbf-chronos; do
#       ssh $h 'bash -s' < scripts/remote/disable_auto_upgrades.sh
#   done
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

# 1. apt periodic jobs off (both the list refresh and the upgrade).
sudo -n tee /etc/apt/apt.conf.d/20auto-upgrades >/dev/null <<'EOF'
APT::Periodic::Update-Package-Lists "0";
APT::Periodic::Download-Upgradeable-Packages "0";
APT::Periodic::AutocleanInterval "0";
APT::Periodic::Unattended-Upgrade "0";
EOF

# 2. The systemd timers that drive them.
for t in apt-daily.timer apt-daily-upgrade.timer; do
    sudo -n systemctl disable --now "$t" >/dev/null 2>&1 || true
    sudo -n systemctl mask "$t" >/dev/null 2>&1 || true
done
sudo -n systemctl stop apt-daily.service apt-daily-upgrade.service unattended-upgrades.service >/dev/null 2>&1 || true
sudo -n systemctl disable unattended-upgrades.service >/dev/null 2>&1 || true

# 3. needrestart: never restart services on its own (manual upgrades only list them).
if [[ -d /etc/needrestart ]]; then
    sudo -n mkdir -p /etc/needrestart/conf.d
    echo "\$nrconf{restart} = 'l';" | sudo -n tee /etc/needrestart/conf.d/90-db-fork-no-restart.conf >/dev/null
fi

echo "$(hostname): unattended upgrades off; timers: $(systemctl is-enabled apt-daily.timer apt-daily-upgrade.timer 2>&1 | tr '\n' ' '); service: $(systemctl is-enabled unattended-upgrades.service 2>&1)"
