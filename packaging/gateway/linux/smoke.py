"""Run the unpacked Linux archive in a temporary directory; on an ephemeral CI runner, also install it."""
import os
from pathlib import Path
import socket
import subprocess
import tarfile
import tempfile
import time

root = Path(__file__).resolve().parents[3]
archive = next((root / 'dist').glob('*-linux-*.tar.gz'))
logs = root / 'installer-test-logs'
logs.mkdir(exist_ok=True)
BINARIES = ['iroh-link-gateway', 'iroh-link-gateway-background']

def listening(port):
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=5):
            return True
    except OSError:
        return False

def wait(condition, message):
    for _ in range(300):
        if condition():
            return
        time.sleep(0.1)
    raise AssertionError(message)

def portable(app, work):
    state = work / 'state'
    state.mkdir()
    # Avoid public discovery dependencies in the test.
    settings = '["--listen","127.0.0.1:18080","--index-server","127.0.0.1:9"]'
    (state / 'arguments.json').write_text(settings)

    def helper(*arguments):
        return subprocess.run([str(app / 'iroh-link-gateway-background'), '--state-dir', str(state), *arguments],
                              capture_output=True, text=True, timeout=60)

    try:
        for binary in BINARIES:
            # The binaries must run on any distribution, so nothing may be linked dynamically.
            linkage = subprocess.run(['file', str(app / binary)], capture_output=True, text=True, check=True).stdout
            assert 'dynamically linked' not in linkage and 'interpreter' not in linkage, linkage
        for name in ['README.md', 'LICENSE-APACHE', 'LICENSE-MIT', 'install.sh',
                     'systemd/system/iroh-link-gateway.service', 'systemd/user/iroh-link-gateway.service',
                     'extensions/chrome/manifest.json', 'extensions/firefox/manifest.json',
                     'extensions/iroh-link-firefox-unsigned.xpi', 'extensions/Install extensions.html']:
            assert (app / name).is_file(), name
        assert os.access(app / 'install.sh', os.X_OK)
        result = helper()
        assert result.returncode == 0, result.stdout + result.stderr
        assert helper('status').returncode == 0
        assert (state / 'ready').exists()
        assert listening(18080)
        result = helper('stop')
        assert result.returncode == 0, result.stdout + result.stderr
        assert helper('status').returncode != 0
        assert not (state / 'ready').exists()
        assert (state / 'arguments.json').read_text() == settings
    finally:
        helper('stop')
        for name in ['gateway.log', 'launcher.log']:
            path = state / name
            if path.exists():
                (logs / name).write_bytes(path.read_bytes())

def installed(app, label, prefix, systemctl, bin, share, unit):
    """Install, upgrade, and uninstall with install.sh, checking the systemd service."""
    service = 'iroh-link-gateway.service'
    arguments = ['--user'] if label == 'user' else []

    def run(step, *extra):
        result = subprocess.run([*prefix, str(app / 'install.sh'), *arguments, *extra],
                                capture_output=True, text=True, timeout=120)
        (logs / f'{label}-{step}.log').write_text(result.stdout + result.stderr)
        assert result.returncode == 0, result.stdout + result.stderr

    if any((bin / binary).exists() for binary in BINARIES) or unit.exists():
        raise SystemExit('Refusing to replace an existing installation')
    try:
        for step in ['install', 'upgrade']:
            run(step)
            wait(lambda: listening(45475), f'{label} service is not listening after {step}')
            subprocess.run([*systemctl, 'is-active', '--quiet', service], check=True)
            subprocess.run([*systemctl, 'is-enabled', '--quiet', service], check=True)
        for path in [bin / 'iroh-link-gateway', bin / 'iroh-link-gateway-background', unit,
                     share / 'extensions/chrome/manifest.json', share / 'extensions/Install extensions.html']:
            assert path.is_file(), path
        run('uninstall', '--uninstall')
        assert not any((bin / binary).exists() for binary in BINARIES)
        assert not unit.exists() and not (share / 'extensions').exists()
        assert subprocess.run([*systemctl, 'is-active', '--quiet', service]).returncode != 0
        wait(lambda: not listening(45475), f'{label} service still listens after uninstall')
    finally:
        journal = ['journalctl', '--no-pager', '--unit', service] if label == 'system' else \
            ['journalctl', '--no-pager', '--user', '--unit', service]
        result = subprocess.run([*prefix, *journal], capture_output=True, text=True)
        (logs / f'{label}-journal.log').write_text(result.stdout + result.stderr)

with tempfile.TemporaryDirectory(prefix='iroh-link-gateway-smoke-') as temporary:
    work = Path(temporary)
    with tarfile.open(archive) as tar:
        tar.extractall(work, filter='data')
    app = work / archive.name.removesuffix('.tar.gz')
    portable(app, work)
    passed = 'static binaries, local extensions, start, status, stop, settings retention'
    # Installing changes the machine, so it runs only on an ephemeral CI runner.
    if os.environ.get('CI') == 'true':
        home = Path.home()
        installed(app, 'system', ['sudo'], ['systemctl'], Path('/usr/local/bin'),
                  Path('/usr/local/share/iroh-link-gateway'),
                  Path('/etc/systemd/system/iroh-link-gateway.service'))
        # The runner has no login session; lingering starts its systemd user manager.
        runtime = Path(f'/run/user/{os.getuid()}')
        os.environ['XDG_RUNTIME_DIR'] = str(runtime)
        subprocess.run(['sudo', 'loginctl', 'enable-linger', str(os.getuid())], check=True)
        wait(lambda: (runtime / 'bus').exists(), 'systemd user manager did not start')
        installed(app, 'user', [], ['systemctl', '--user'], home / '.local/bin',
                  home / '.local/share/iroh-link-gateway',
                  home / '.config/systemd/user/iroh-link-gateway.service')
        passed += ', system and user install, upgrade, uninstall'
    print('PASS: ' + passed)
