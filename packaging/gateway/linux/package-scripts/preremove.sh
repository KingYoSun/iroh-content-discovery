#!/bin/sh
# Stop and disable the system service when the package is removed, but not when
# it is upgraded: dpkg passes "upgrade" and rpm the number of remaining versions.
case "${1:-}" in
    upgrade|[1-9]) ;;
    *)
        if [ -d /run/systemd/system ]; then
            systemctl disable --now iroh-link-gateway.service || :
        fi
        ;;
esac
