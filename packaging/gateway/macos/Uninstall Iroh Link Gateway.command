#!/bin/sh
set -eu
if [ "$(id -u)" -eq 0 ]; then
    echo 'Run this uninstaller as your own user, without sudo.' >&2
    exit 1
fi
if [ "${1:-}" != '--yes' ]; then
    printf 'Close Iroh Link Gateway before continuing. Remove the app and background login agent? Your saved data and files will be kept. [y/N] '
    read -r answer
    case "$answer" in y|Y|yes|YES) ;; *) exit 0 ;; esac
fi
app="$HOME/Applications/Iroh Link Gateway.app"
"$app/Contents/MacOS/iroh-link-gateway-background" remove-agent
# Remove installed binaries, extension copies, and shortcuts; preserve settings and logs.
/bin/rm -rf "$app" "$HOME/Applications/Iroh Link Gateway Extensions"
/bin/rm -f "$HOME/Applications/Start Iroh Link Gateway.command" "$HOME/Applications/Stop Iroh Link Gateway.command"
/bin/rm -f "$HOME/Applications/Uninstall Iroh Link Gateway.command"
printf 'Iroh Link Gateway has been uninstalled. Your data and settings have been kept.\n'
