"""Run the unpacked Linux archive in a temporary directory; on an ephemeral CI runner, also install it."""
import os
from pathlib import Path
import signal
import socket
import subprocess
import tarfile
import tempfile
import time

root = Path(__file__).resolve().parents[3]
logs = root / 'installer-test-logs'
logs.mkdir(exist_ok=True)

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

def user_manager():
    """Start the CI runner's systemd user manager, which needs a login session or lingering."""
    runtime = Path(f'/run/user/{os.getuid()}')
    os.environ['XDG_RUNTIME_DIR'] = str(runtime)
    subprocess.run(['sudo', 'loginctl', 'enable-linger', str(os.getuid())], check=True)
    wait(lambda: (runtime / 'bus').exists(), 'systemd user manager did not start')

def portable(app, work):
    state = work / 'state'
    gateway = app / 'iroh-link-gateway'
    # The binary must run on any distribution, so nothing may be linked dynamically.
    linkage = subprocess.run(['file', str(gateway)], capture_output=True, text=True, check=True).stdout
    assert 'dynamically linked' not in linkage and 'interpreter' not in linkage, linkage
    assert not (app / 'iroh-link-gateway-background').exists()
    for name in ['README.md', 'LICENSE-APACHE', 'LICENSE-MIT', 'install.sh',
                 'systemd/system/iroh-link-gateway.service', 'systemd/user/iroh-link-gateway.service']:
        assert (app / name).is_file(), name
    assert os.access(app / 'install.sh', os.X_OK)
    # Run the gateway as the systemd units do. Avoid public discovery dependencies in the test.
    with (logs / 'gateway.log').open('w') as log:
        process = subprocess.Popen([str(gateway), '--state-dir', str(state), '--listen', '127.0.0.1:18080',
                                    '--index-server', '127.0.0.1:9'], stdout=log, stderr=log)
        try:
            wait(lambda: (state / 'ready').exists() and listening(18080), 'gateway did not become ready')
            process.send_signal(signal.SIGTERM)
            assert process.wait(timeout=30) == 0
            assert not (state / 'ready').exists()
        finally:
            if process.poll() is None:
                process.kill()

def installed(app, label, prefix, systemctl, bin, share, unit):
    """Install, upgrade, and uninstall with install.sh, checking the systemd service."""
    service = 'iroh-link-gateway.service'
    arguments = ['--user'] if label == 'user' else []

    def run(step, *extra):
        result = subprocess.run([*prefix, str(app / 'install.sh'), *arguments, *extra],
                                capture_output=True, text=True, timeout=120)
        (logs / f'{label}-{step}.log').write_text(result.stdout + result.stderr)
        assert result.returncode == 0, result.stdout + result.stderr

    if (bin / 'iroh-link-gateway').exists() or unit.exists():
        raise SystemExit('Refusing to replace an existing installation')
    try:
        for step in ['install', 'upgrade']:
            run(step)
            wait(lambda: listening(45475), f'{label} service is not listening after {step}')
            subprocess.run([*systemctl, 'is-active', '--quiet', service], check=True)
            subprocess.run([*systemctl, 'is-enabled', '--quiet', service], check=True)
        if label == 'user':
            assert 'ExecStart=%h/.local/bin/iroh-link-gateway ' in unit.read_text()
        for path in [bin / 'iroh-link-gateway', unit]:
            assert path.is_file(), path
        run('uninstall', '--uninstall')
        assert not (bin / 'iroh-link-gateway').exists()
        assert not unit.exists() and not (share / 'extensions').exists()
        assert subprocess.run([*systemctl, 'is-active', '--quiet', service]).returncode != 0
        wait(lambda: not listening(45475), f'{label} service still listens after uninstall')
    finally:
        journal = ['journalctl', '--no-pager', '--unit', service] if label == 'system' else \
            ['journalctl', '--no-pager', '--user', '--unit', service]
        result = subprocess.run([*prefix, *journal], capture_output=True, text=True)
        (logs / f'{label}-journal.log').write_text(result.stdout + result.stderr)

if __name__ == '__main__':
    archive = next((root / 'dist').glob('*-linux-*.tar.gz'))
    with tempfile.TemporaryDirectory(prefix='iroh-link-gateway-smoke-') as temporary:
        work = Path(temporary)
        with tarfile.open(archive) as tar:
            tar.extractall(work, filter='data')
        app = work / archive.name.removesuffix('.tar.gz')
        portable(app, work)
        passed = 'static binary, start, stop'
        # Installing changes the machine, so it runs only on an ephemeral CI runner.
        if os.environ.get('CI') == 'true':
            home = Path.home()
            installed(app, 'system', ['sudo'], ['systemctl'], Path('/usr/local/bin'),
                      Path('/usr/local/share/iroh-link-gateway'),
                      Path('/etc/systemd/system/iroh-link-gateway.service'))
            user_manager()
            installed(app, 'user', [], ['systemctl', '--user'], home / '.local/bin',
                      home / '.local/share/iroh-link-gateway',
                      home / '.config/systemd/user/iroh-link-gateway.service')
            passed += ', system and user install, upgrade, uninstall'
        print('PASS: ' + passed)
