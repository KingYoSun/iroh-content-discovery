#!/bin/sh
# As with other Debian services, enable and start the system service on a
# fresh installation, when dpkg passes no previously configured version. On
# upgrades, restart it only if it is running, so a disabled service stays off.
if [ -d /run/systemd/system ]; then
    systemctl daemon-reload || :
    if [ "${1:-}" = configure ] && [ -z "${2:-}" ]; then
        systemctl enable --now iroh-link-gateway.service || :
    else
        systemctl try-restart iroh-link-gateway.service || :
    fi
fi
