"""Publish the SDK-owned dependency first; never ignore registry failures."""
import json
import subprocess
import tomllib
import urllib.error
import urllib.request
from pathlib import Path

package = tomllib.loads(Path("crates/eval-scorers/Cargo.toml").read_text())["package"]
url = f"https://crates.io/api/v1/crates/{package['name']}/{package['version']}"
request = urllib.request.Request(url, headers={"User-Agent": "agnt5-sdk-release"})
try:
    with urllib.request.urlopen(request, timeout=30) as response:
        version = json.load(response)["version"]
    if version["num"] != package["version"] or version.get("yanked"):
        raise RuntimeError("Existing scorer release is not usable")
    print(f"Shared scorer {package['version']} is already published")
except urllib.error.HTTPError as error:
    if error.code != 404:
        raise
    subprocess.run(["cargo", "publish", "-p", package["name"], "--locked"], check=True)
