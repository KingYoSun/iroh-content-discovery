"""Write the download section that heads a gateway release, linking the files in a directory."""
from pathlib import Path
import sys

STORE = 'https://chromewebstore.google.com/detail/iroh-link/aajlbmaphckgbinhnifpiggcmdfnofcd'
FIREFOX = 'https://github.com/n0-computer/iroh-content-discovery/tree/main/iroh-link-extension#install-in-firefox'
LINUX = [('.deb', 'Debian/Ubuntu'), ('.rpm', 'Fedora'), ('.pkg.tar.zst', 'Arch'), ('.tar.gz', 'archive')]

def notes(repo, tag, files):
    base = f'https://github.com/{repo}/releases/download/{tag}'
    def link(name, label=None):
        return f'[{label or name}]({base}/{name})'
    def find(suffix):
        return next((name for name in files if name.endswith(suffix)), None)
    lines = ['## Download', '']
    if mac := find('-macos-arm64.pkg'):
        lines.append(f'- **macOS** (Apple silicon): {link(mac)}')
    if windows := find('-windows-x64-setup.exe'):
        lines.append(f'- **Windows** (x64): {link(windows)}')
    for arch in ['x64', 'arm64']:
        packages = [link(name, label) for suffix, label in LINUX
                    if (name := find(f'-linux-{arch}{suffix}'))]
        if packages:
            lines.append(f'- **Linux** ({arch}): ' + ' · '.join(packages))
    if 'SHA256SUMS' in files:
        lines.append(f'- Checksums: {link("SHA256SUMS")}')
    lines += ['', 'Links to blake3.net and pkarr.net also need the iroh link browser extension: '
              f'[Chrome Web Store]({STORE}) for Chrome or Brave, or '
              f'[load it in Firefox]({FIREFOX}) until the Firefox version is listed.', '']
    return '\n'.join(lines)

if __name__ == '__main__':
    repo, tag, directory = sys.argv[1:]
    print(notes(repo, tag, sorted(path.name for path in Path(directory).iterdir())))
