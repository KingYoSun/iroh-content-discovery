#!/bin/sh
# Pick up the new unit files and restart a running system service on upgrade.
# The service is not enabled or started on a fresh installation.
if [ -d /run/systemd/system ]; then
    systemctl daemon-reload || :
    systemctl try-restart iroh-link-gateway.service || :
fi
