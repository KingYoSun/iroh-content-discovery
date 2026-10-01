"""Pack the staged Linux gateway, its install script, and systemd units into a tar archive."""
import pathlib
import sys
import tarfile

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from stage import ROOT, checksum, version

ARCHITECTURES = {'x86_64-unknown-linux-musl': 'x64', 'aarch64-unknown-linux-musl': 'arm64'}

def normalize(info):
    # Archive members must not carry the build user's identity.
    info.uid = info.gid = 0
    info.uname = info.gname = 'root'
    return info

def build(target):
    name = f'iroh-link-gateway-{version()}-linux-{ARCHITECTURES[target]}'
    archive = ROOT / 'dist' / (name + '.tar.gz')
    with tarfile.open(archive, 'w:gz') as tar:
        tar.add(ROOT / 'dist/gateway' / target, arcname=name, filter=normalize)
        for extra in ['install.sh', 'systemd']:
            tar.add(pathlib.Path(__file__).with_name(extra), arcname=f'{name}/{extra}', filter=normalize)
    checksum(archive)
    return archive

if __name__ == '__main__':
    print(build(sys.argv[1]))
