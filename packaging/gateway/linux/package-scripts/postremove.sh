#!/bin/sh
if [ -d /run/systemd/system ]; then
    systemctl daemon-reload || :
fi
# dpkg --purge also removes the system service's state; DynamicUser keeps it
# under /var/lib/private.
if [ "${1:-}" = purge ]; then
    rm -rf /var/lib/private/iroh-link-gateway /var/lib/iroh-link-gateway
fi
