#!/bin/sh
# Pick up the new unit files and restart a running system service on upgrade.
# As is usual on Fedora and Arch Linux, the service is not enabled or started on
# a fresh installation; postinstall-deb.sh does that for Debian.
if [ -d /run/systemd/system ]; then
    systemctl daemon-reload || :
    systemctl try-restart iroh-link-gateway.service || :
fi
