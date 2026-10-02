"""Stage gateway binaries and documentation."""
import hashlib
from pathlib import Path
import shutil
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[2]

def checksum(path):
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    path.with_name(path.name + '.sha256').write_text(f'{digest}  {path.name}\n')

def stage(target):
    destination = ROOT / 'dist/gateway' / target
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True)
    suffix = '.exe' if 'windows' in target else ''
    # Linux runs the gateway under systemd, which replaces the background launcher.
    binaries = ['iroh-link-gateway'] if 'linux' in target else ['iroh-link-gateway', 'iroh-link-gateway-background']
    for binary in binaries:
        shutil.copy2(ROOT / 'target' / target / 'release' / (binary + suffix), destination)
    for name in ['README.md', 'LICENSE-APACHE', 'LICENSE-MIT']:
        shutil.copy2(ROOT / 'iroh-link-gateway' / name, destination)
    return destination

def version():
    return tomllib.loads((ROOT / 'Cargo.toml').read_text())['workspace']['package']['version']

if __name__ == '__main__':
    print(stage(sys.argv[1]))
