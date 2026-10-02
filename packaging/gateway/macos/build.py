"""Build a macOS package restricted to the current user's home directory."""
import hashlib
import pathlib
import plistlib
import shutil
import subprocess
import tempfile
import tomllib
import xml.etree.ElementTree as ET

root = pathlib.Path(__file__).resolve().parents[3]
version = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]["package"]["version"]
identifier = "computer.n0.iroh-link-gateway.user"
dist = root / "dist"
with tempfile.TemporaryDirectory(prefix="iroh-link-gateway-pkg-") as temporary:
    work = pathlib.Path(temporary)
    staged = root / "dist/gateway/aarch64-apple-darwin"
    payload = work / "payload"
    applications = payload / "Applications"
    contents = applications / "Iroh Link Gateway.app/Contents"
    binaries = contents / "MacOS"
    binaries.mkdir(parents=True)
    for binary in ["iroh-link-gateway", "iroh-link-gateway-background"]:
        shutil.copy2(staged / binary, binaries / binary)
    with (contents / "Info.plist").open("wb") as file:
        plistlib.dump({
            "CFBundleExecutable": "iroh-link-gateway-background",
            "CFBundleIdentifier": "computer.n0.iroh-link-gateway",
            "CFBundleName": "Iroh Link Gateway",
            "CFBundlePackageType": "APPL",
            "CFBundleIconFile": "AppIcon",
            "CFBundleShortVersionString": version,
            "CFBundleVersion": version,
            "LSUIElement": True,
            "LSMinimumSystemVersion": "13.0",
        }, file)
    resources = contents / "Resources"
    resources.mkdir()
    for name in ["README.md", "LICENSE-APACHE", "LICENSE-MIT"]:
        shutil.copy2(staged / name, resources / name)
    shutil.copy2(root / "packaging/gateway/macos/AppIcon.icns", resources / "AppIcon.icns")
    subprocess.run(["codesign", "--force", "--deep", "--sign", "-", str(contents.parent)], check=True)
    for action in ["Start", "Stop"]:
        script = applications / f"{action} Iroh Link Gateway.command"
        command = "install-agent" if action == "Start" else "remove-agent"
        script.write_text('#!/bin/sh\nset -eu\n"$HOME/Applications/Iroh Link Gateway.app/Contents/MacOS/iroh-link-gateway-background" ' + command + '\n')
        script.chmod(0o755)
    uninstaller = applications / "Uninstall Iroh Link Gateway.command"
    shutil.copy2(root / "packaging/gateway/macos/Uninstall Iroh Link Gateway.command", uninstaller)
    uninstaller.chmod(0o755)
    scripts = work / "scripts"
    shutil.copytree(root / "packaging/gateway/macos/scripts", scripts)
    for script in scripts.iterdir():
        script.chmod(0o755)
    components = work / "components.plist"
    subprocess.run(["pkgbuild", "--analyze", "--root", str(payload), str(components)], check=True)
    with components.open("rb") as file:
        settings = plistlib.load(file)
    for component in settings:
        component["BundleIsRelocatable"] = False
        component["BundleOverwriteAction"] = "upgrade"
    with components.open("wb") as file:
        plistlib.dump(settings, file)
    component_pkg = work / "Iroh Link Gateway-component.pkg"
    subprocess.run(["pkgbuild", "--root", str(payload), "--component-plist", str(components),
                    "--scripts", str(scripts), "--identifier", identifier, "--version", version,
                    "--install-location", "/", str(component_pkg)], check=True)
    distribution = ET.Element("installer-gui-script", {"minSpecVersion": "2"})
    ET.SubElement(distribution, "title").text = "Iroh Link Gateway"
    ET.SubElement(distribution, "welcome", {"file": "welcome.html"})
    ET.SubElement(distribution, "conclusion", {"file": "conclusion.html"})
    ET.SubElement(distribution, "options", {"customize": "never", "require-scripts": "true", "hostArchitectures": "arm64"})
    ET.SubElement(distribution, "domains", {"enable_anywhere": "false", "enable_localSystem": "false", "enable_currentUserHome": "true"})
    allowed = ET.SubElement(distribution, "allowed-os-versions")
    ET.SubElement(allowed, "os-version", {"min": "13.0"})
    outline = ET.SubElement(distribution, "choices-outline")
    ET.SubElement(outline, "line", {"choice": "default"})
    choice = ET.SubElement(distribution, "choice", {"id": "default", "visible": "false"})
    ET.SubElement(choice, "pkg-ref", {"id": identifier})
    ET.SubElement(distribution, "pkg-ref", {"id": identifier, "version": version, "auth": "none"}).text = component_pkg.name
    xml = work / "Distribution.xml"
    ET.ElementTree(distribution).write(xml, encoding="utf-8", xml_declaration=True)
    package = dist / f"iroh-link-gateway-{version}-macos-arm64.pkg"
    subprocess.run(["productbuild", "--distribution", str(xml), "--package-path", str(work),
                    "--resources", str(root / "packaging/gateway/macos/resources"), str(package)], check=True)
    with package.open("rb") as file:
        checksum = hashlib.file_digest(file, "sha256").hexdigest()
    package.with_name(package.name + ".sha256").write_text(f"{checksum}  {package.name}\n")
    print(package)
