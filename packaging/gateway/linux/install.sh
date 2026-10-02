#!/bin/sh
# Install or remove the Iroh gateway and its systemd service.
set -eu

usage() {
    cat <<'USAGE'
Usage: install.sh [--user] [--uninstall]

Installs the gateway in /usr/local and enables a systemd system service.

  --user       install in ~/.local for the current user, with a systemd user service
  --uninstall  stop the service and remove the installed files; settings are kept
USAGE
}

user=false
uninstall=false
for argument in "$@"; do
    case "$argument" in
        --user) user=true ;;
        --uninstall) uninstall=true ;;
        -h|--help) usage; exit 0 ;;
        *) usage >&2; exit 2 ;;
    esac
done

source=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
service=iroh-link-gateway.service
if $user; then
    if [ "$(id -u)" -eq 0 ]; then
        echo 'Run install.sh --user as your own user, without sudo.' >&2
        exit 1
    fi
    bin=$HOME/.local/bin
    share=$HOME/.local/share/iroh-link-gateway
    units=${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user
    unit=$source/systemd/user/$service
    systemctl='systemctl --user'
else
    if [ "$(id -u)" -ne 0 ]; then
        echo 'A system-wide installation needs root. Run install.sh with sudo, or pass --user.' >&2
        exit 1
    fi
    bin=/usr/local/bin
    share=/usr/local/share/iroh-link-gateway
    units=/etc/systemd/system
    unit=$source/systemd/system/$service
    systemctl='systemctl'
fi

# The files are still installed where systemd is absent or has no user session.
if command -v systemctl >/dev/null 2>&1 && $systemctl show-environment >/dev/null 2>&1; then
    systemd=true
else
    systemd=false
fi

if $uninstall; then
    if $systemd && [ -e "$units/$service" ]; then
        $systemctl disable --now "$service"
    fi
    rm -f "$units/$service" "$bin/iroh-link-gateway"
    rm -rf "$share/extensions"
    rmdir "$share" 2>/dev/null || true
    if $systemd; then
        $systemctl daemon-reload
    fi
    echo 'Iroh Link Gateway has been uninstalled. Your data and settings have been kept.'
    exit 0
fi

mkdir -p "$bin" "$units"
# install replaces the files instead of writing into a running executable.
install -m 755 "$source/iroh-link-gateway" "$bin/"
# Older versions installed unpacked extension files here.
rm -rf "$share/extensions"
rmdir "$share" 2>/dev/null || true
if $user; then
    sed 's|^ExecStart=iroh-link-gateway |ExecStart=%h/.local/bin/iroh-link-gateway |' "$unit" > "$units/$service"
else
    install -m 644 "$unit" "$units/$service"
fi
echo "Installed the gateway in $bin."
if $systemd; then
    $systemctl daemon-reload
    $systemctl enable "$service"
    $systemctl restart "$service"
    echo "Started $service. The gateway listens on 127.0.0.1:45475."
else
    echo "systemd is not available, so $service was installed without being started."
fi
