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
MAINTAINER = 'n0 team <hello@n0.computer>'

def copyright():
    """Debian's machine-readable copyright file, with the MIT text from LICENSE-MIT."""
    holder, _, mit = (ROOT / 'LICENSE-MIT').read_text().partition('\n')
    # Debian marks blank lines in field text with a lone period.
    text = '\n'.join(f' {line}' if line.strip() else ' .' for line in mit.strip().splitlines())
    return f"""Format: https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/
Upstream-Name: iroh-link-gateway
Upstream-Contact: {MAINTAINER}
Source: https://github.com/n0-computer/iroh-content-discovery
Comment: The gateway is statically linked with Rust crates under their own
 licenses, mostly MIT or Apache-2.0, and also BSD, ISC, MPL-2.0, Unicode-3.0,
 and Zlib. Cargo.lock in the source repository lists them.

Files: *
Copyright: {holder.removeprefix('Copyright ')}
License: Expat or Apache-2.0

License: Expat
{text}

License: Apache-2.0
 On Debian systems, the full text of the Apache License, Version 2.0 can be
 found in /usr/share/common-licenses/Apache-2.0.
"""

def config(target, work):
    staged = ROOT / 'dist/gateway' / target
    scripts = HERE / 'package-scripts'
    workspace = tomllib.loads((ROOT / 'Cargo.toml').read_text())['workspace']['package']
    contents = [
        {'src': str(staged / 'iroh-link-gateway'), 'dst': '/usr/bin/iroh-link-gateway', 'file_info': {'mode': 0o755}},
        {'src': str(staged / 'README.md'), 'dst': '/usr/share/doc/iroh-link-gateway/README.md'},
        {'src': str(work / 'copyright'), 'dst': '/usr/share/doc/iroh-link-gateway/copyright', 'packager': 'deb'},
    ]
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
        'maintainer': MAINTAINER,
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
        'archlinux': {'packager': MAINTAINER, 'scripts': {'postupgrade': str(scripts / 'postinstall.sh')}},
        'overrides': {'deb': {'scripts': {'postinstall': str(scripts / 'postinstall-deb.sh')}}},
    }

def build(target):
    dist = ROOT / 'dist'
    with tempfile.TemporaryDirectory(prefix='iroh-link-gateway-nfpm-') as temporary:
        # nfpm reads YAML, and JSON is YAML.
        work = pathlib.Path(temporary)
        (work / 'copyright').write_text(copyright())
        path = work / 'nfpm.yaml'
        path.write_text(json.dumps(config(target, work), indent=2))
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
