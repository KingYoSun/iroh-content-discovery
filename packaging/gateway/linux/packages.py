"""Build Debian, RPM, and Arch Linux packages of the staged Linux gateway with nfpm."""
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
import tomllib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from stage import ROOT, checksum, version

HERE = pathlib.Path(__file__).resolve().parent
ARCHITECTURES = {'x86_64-unknown-linux-musl': 'amd64', 'aarch64-unknown-linux-musl': 'arm64'}
PACKAGERS = ['deb', 'rpm', 'archlinux']

def config(target):
    staged = ROOT / 'dist/gateway' / target
    scripts = HERE / 'package-scripts'
    workspace = tomllib.loads((ROOT / 'Cargo.toml').read_text())['workspace']['package']
    contents = [
        {'src': str(staged / 'iroh-link-gateway'), 'dst': '/usr/bin/iroh-link-gateway', 'file_info': {'mode': 0o755}},
        {'src': str(staged / 'README.md'), 'dst': '/usr/share/doc/iroh-link-gateway/README.md'},
    ]
    # nfpm's tree entries give Arch packages invalid directory modes, so list each file.
    for path in sorted((staged / 'extensions').rglob('*')):
        if path.is_file():
            relative = path.relative_to(staged).as_posix()
            contents.append({'src': str(path), 'dst': f'/usr/share/iroh-link-gateway/{relative}'})
    for kind in ['system', 'user']:
        contents.append({'src': str(HERE / 'systemd' / kind / 'iroh-link-gateway.service'),
                         'dst': f'/usr/lib/systemd/{kind}/iroh-link-gateway.service'})
    for name in ['LICENSE-APACHE', 'LICENSE-MIT']:
        contents.append({'src': str(staged / name), 'dst': f'/usr/share/licenses/iroh-link-gateway/{name}'})
    return {
        'name': 'iroh-link-gateway',
        'version': version(),
        'release': '1',
        'arch': ARCHITECTURES[target],
        'platform': 'linux',
        'section': 'net',
        'maintainer': workspace['authors'][0],
        'description': 'Local HTTP gateway for blake3.net and pkarr.net links, served from iroh peers',
        'homepage': 'https://github.com/n0-computer/iroh-content-discovery',
        'license': workspace['license'],
        'contents': contents,
        'scripts': {
            'postinstall': str(scripts / 'postinstall.sh'),
            'preremove': str(scripts / 'preremove.sh'),
            'postremove': str(scripts / 'postremove.sh'),
        },
        # pacman runs post_upgrade instead of post_install on upgrades.
        'archlinux': {'packager': workspace['authors'][0], 'scripts': {'postupgrade': str(scripts / 'postinstall.sh')}},
    }

def build(target):
    dist = ROOT / 'dist'
    with tempfile.TemporaryDirectory(prefix='iroh-link-gateway-nfpm-') as temporary:
        # nfpm reads YAML, and JSON is YAML.
        path = pathlib.Path(temporary) / 'nfpm.yaml'
        path.write_text(json.dumps(config(target), indent=2))
        packages = []
        for packager in PACKAGERS:
            output = pathlib.Path(temporary) / packager
            output.mkdir()
            subprocess.run(['nfpm', 'package', '--config', str(path), '--packager', packager,
                            '--target', str(output)], check=True)
            package = next(output.iterdir())
            packages.append(pathlib.Path(shutil.move(package, dist / package.name)))
    for package in packages:
        checksum(package)
    return packages

if __name__ == '__main__':
    for package in build(sys.argv[1]):
        print(package)
