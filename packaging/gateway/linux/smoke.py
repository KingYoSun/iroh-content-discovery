"""Unpack the Linux archive and start, query, and stop the gateway in a temporary directory."""
from pathlib import Path
import socket
import subprocess
import tarfile
import tempfile

root = Path(__file__).resolve().parents[3]
archive = next((root / 'dist').glob('*-linux-*.tar.gz'))
logs = root / 'installer-test-logs'
logs.mkdir(exist_ok=True)
with tempfile.TemporaryDirectory(prefix='iroh-link-gateway-smoke-') as temporary:
    work = Path(temporary)
    with tarfile.open(archive) as tar:
        tar.extractall(work, filter='data')
    app = work / archive.name.removesuffix('.tar.gz')
    state = work / 'state'
    state.mkdir()
    # Avoid public discovery dependencies in the test.
    settings = '["--listen","127.0.0.1:18080","--index-server","127.0.0.1:9"]'
    (state / 'arguments.json').write_text(settings)

    def helper(*arguments):
        return subprocess.run([str(app / 'iroh-link-gateway-background'), '--state-dir', str(state), *arguments],
                              capture_output=True, text=True, timeout=60)

    try:
        for binary in ['iroh-link-gateway', 'iroh-link-gateway-background']:
            # The binaries must run on any distribution, so nothing may be linked dynamically.
            linkage = subprocess.run(['file', str(app / binary)], capture_output=True, text=True, check=True).stdout
            assert 'dynamically linked' not in linkage and 'interpreter' not in linkage, linkage
        for name in ['README.md', 'LICENSE-APACHE', 'LICENSE-MIT', 'extensions/chrome/manifest.json',
                     'extensions/firefox/manifest.json', 'extensions/iroh-link-firefox-unsigned.xpi',
                     'extensions/Install extensions.html']:
            assert (app / name).is_file(), name
        result = helper()
        assert result.returncode == 0, result.stdout + result.stderr
        assert helper('status').returncode == 0
        assert (state / 'ready').exists()
        with socket.create_connection(('127.0.0.1', 18080), timeout=5):
            pass
        result = helper('stop')
        assert result.returncode == 0, result.stdout + result.stderr
        assert helper('status').returncode != 0
        assert not (state / 'ready').exists()
        assert (state / 'arguments.json').read_text() == settings
        print('PASS: static binaries, local extensions, start, status, stop, settings retention')
    finally:
        helper('stop')
        for name in ['gateway.log', 'launcher.log']:
            path = state / name
            if path.exists():
                (logs / name).write_bytes(path.read_bytes())
