"""Install, reinstall, and remove the Linux packages in containers; on an ephemeral CI runner, also run the Debian package under systemd."""
import os
from pathlib import Path
import shutil
import subprocess

from smoke import listening, logs, root, user_manager, wait

dist = root / 'dist'
deb = next(dist.glob('iroh-link-gateway-*-linux-*.deb'))
rpm = next(dist.glob('iroh-link-gateway-*-linux-*.rpm'))
arch = next(dist.glob('iroh-link-gateway-*-linux-*.pkg.tar.zst'))
FILES = ['/usr/lib/systemd/system/iroh-link-gateway.service', '/usr/lib/systemd/user/iroh-link-gateway.service',
         '/usr/share/licenses/iroh-link-gateway/LICENSE-MIT']
CHECK = 'iroh-link-gateway --help >/dev/null && ' + ' && '.join(f"test -f '{path}'" for path in FILES)
GONE = '! test -e /usr/bin/iroh-link-gateway'
# Each distribution installs the package, installs it again to run the upgrade
# scripts, and removes it. There is no systemd in the containers.
DISTRIBUTIONS = {
    'debian': ('debian:stable-slim', deb, 'apt-get install -y /pkg/{0} && apt-get install -y --reinstall /pkg/{0}',
               'apt-get remove -y iroh-link-gateway'),
    'fedora': ('fedora:latest', rpm, 'dnf install -y /pkg/{0} && dnf reinstall -y /pkg/{0}',
               'dnf remove -y iroh-link-gateway'),
    'arch': ('archlinux:latest', arch, 'pacman -U --noconfirm /pkg/{0} && pacman -U --noconfirm /pkg/{0}',
             'pacman -R --noconfirm iroh-link-gateway'),
}

def containers():
    runtime = shutil.which('docker') or shutil.which('podman')
    for name, (image, package, install, remove) in DISTRIBUTIONS.items():
        if name == 'arch' and 'x86_64' not in package.name:
            continue  # Arch Linux publishes container images for x86_64 only.
        script = f'set -e; {install.format(package.name)}; {CHECK}; {remove}; {GONE}'
        result = subprocess.run([runtime, 'run', '--rm', '-v', f'{dist}:/pkg:ro', image, 'sh', '-c', script],
                                capture_output=True, text=True, timeout=600)
        (logs / f'package-{name}.log').write_text(result.stdout + result.stderr)
        assert result.returncode == 0, f'{name}:\n{result.stdout}{result.stderr}'

def systemd():
    """Run the Debian package's system and user services, then check that removal stops them."""
    service = 'iroh-link-gateway.service'

    def apt(*arguments):
        result = subprocess.run(['sudo', 'apt-get', *arguments, '-y'], capture_output=True, text=True, timeout=300)
        with (logs / 'package-apt.log').open('a') as log:
            log.write(result.stdout + result.stderr)
        assert result.returncode == 0, result.stdout + result.stderr

    try:
        apt('install', str(deb))
        # Installing enables and starts the system service.
        subprocess.run(['systemctl', 'is-enabled', '--quiet', service], check=True)
        wait(lambda: listening(45475), 'system service is not listening after install')
        apt('install', '--reinstall', str(deb))
        subprocess.run(['systemctl', 'is-active', '--quiet', service], check=True)
        wait(lambda: listening(45475), 'system service is not listening after reinstall')
        # An upgrade leaves a disabled service off.
        subprocess.run(['sudo', 'systemctl', 'disable', '--now', service], check=True)
        wait(lambda: not listening(45475), 'system service still listens after stop')
        apt('install', '--reinstall', str(deb))
        assert subprocess.run(['systemctl', 'is-enabled', '--quiet', service]).returncode != 0
        assert not listening(45475)
        user_manager()
        subprocess.run(['systemctl', '--user', 'start', service], check=True)
        wait(lambda: listening(45475), 'user service is not listening')
        subprocess.run(['systemctl', '--user', 'stop', service], check=True)
        subprocess.run(['sudo', 'systemctl', 'start', service], check=True)
        wait(lambda: listening(45475), 'system service is not listening after restart')
        apt('remove', 'iroh-link-gateway')
        wait(lambda: not listening(45475), 'system service still listens after removal')
        assert not Path('/usr/bin/iroh-link-gateway').exists()
    finally:
        for journal, name in [(['sudo', 'journalctl'], 'system'), (['journalctl', '--user'], 'user')]:
            result = subprocess.run([*journal, '--no-pager', '--unit', service], capture_output=True, text=True)
            (logs / f'package-{name}-journal.log').write_text(result.stdout + result.stderr)

if __name__ == '__main__':
    containers()
    passed = 'deb, rpm, and Arch packages install, upgrade, and remove'
    # Installing changes the machine, so it runs only on an ephemeral CI runner.
    if os.environ.get('CI') == 'true' and shutil.which('apt-get'):
        systemd()
        passed += '; Debian package system and user services'
    print('PASS: ' + passed)
