#!/usr/bin/env bash
# Builds the signed apt repository the release workflow publishes (.github/workflows/release.yml).
#
#   tools/build-apt-repo.sh DEB_DIR OUT_DIR [KEY_ID]
#
# DEB_DIR holds the .deb files to offer (every version kept, so `apt install xrpla=VERSION`
# works for them). OUT_DIR receives one suite, `stable`, with component `main`:
#
#   OUT_DIR/pool/main/x/xrpla/*.deb
#   OUT_DIR/dists/stable/{Release,Release.gpg,InRelease}
#   OUT_DIR/dists/stable/main/binary-amd64/Packages{,.gz}
#   OUT_DIR/xrpla-archive-keyring.gpg  (the public key, binary, for signed-by=)
#   OUT_DIR/xrpla-archive-keyring.asc  (the same, armored)
#   OUT_DIR/index.html                 (how to add the repository)
#
# XRPLA_APT_URL is the address it is published at (default: the project's GitHub Pages site).
# KEY_ID names the signing key in the caller's GnuPG keyring (default: its only secret key).
# Needs dpkg-dev (apt-ftparchive comes with apt-utils) and gnupg.
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "usage: $0 DEB_DIR OUT_DIR [KEY_ID]" >&2
  exit 2
fi
debs=$1 out=$2 key=${3:-}
suite=stable component=main arch=amd64
url=${XRPLA_APT_URL:-https://first-ledger.github.io/xrpla}

shopt -s nullglob
packages=("$debs"/*.deb)
if [[ ${#packages[@]} -eq 0 ]]; then
  echo "no .deb files in $debs" >&2
  exit 1
fi
if [[ -z $key ]]; then
  key=$(gpg --batch --list-secret-keys --with-colons | awk -F: '$1 == "fpr" {print $10; exit}')
  [[ -n $key ]] || { echo "no secret key in the GnuPG keyring" >&2; exit 1; }
fi

if [[ -e $out && -n $(ls -A "$out") ]]; then
  echo "$out is not empty" >&2
  exit 1
fi
mkdir -p "$out/pool/$component/x/xrpla" "$out/dists/$suite/$component/binary-$arch"
for package in "${packages[@]}"; do
  [[ $(dpkg-deb --field "$package" Architecture) == "$arch" ]] || { echo "$package is not $arch" >&2; exit 1; }
  cp "$package" "$out/pool/$component/x/xrpla/"
done

cd "$out"
apt-ftparchive --arch "$arch" packages "pool/$component" > "dists/$suite/$component/binary-$arch/Packages"
gzip -9nk "dists/$suite/$component/binary-$arch/Packages"
apt-ftparchive \
  -o APT::FTPArchive::Release::Origin=XRPLA \
  -o APT::FTPArchive::Release::Label=XRPLA \
  -o APT::FTPArchive::Release::Suite="$suite" \
  -o APT::FTPArchive::Release::Codename="$suite" \
  -o APT::FTPArchive::Release::Architectures="$arch" \
  -o APT::FTPArchive::Release::Components="$component" \
  -o APT::FTPArchive::Release::Description="XRPLA release packages" \
  release "dists/$suite" > "dists/$suite/Release.tmp"
mv "dists/$suite/Release.tmp" "dists/$suite/Release"

gpg --batch --yes --local-user "$key" --digest-algo SHA512 --armor --detach-sign \
  --output "dists/$suite/Release.gpg" "dists/$suite/Release"
gpg --batch --yes --local-user "$key" --digest-algo SHA512 --clearsign \
  --output "dists/$suite/InRelease" "dists/$suite/Release"
gpg --batch --export "$key" > xrpla-archive-keyring.gpg
gpg --batch --armor --export "$key" > xrpla-archive-keyring.asc
# GitHub Pages serves the files as they are, without Jekyll.
touch .nojekyll
cat > index.html <<HTML
<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>XRPLA apt repository</title></head>
<body style="font-family: system-ui, sans-serif; max-width: 48rem; margin: 2rem auto; padding: 0 1rem">
<h1>XRPLA apt repository</h1>
<p>For x86-64 Debian 12+ and Ubuntu 20.04+. Signing key fingerprint: <code>$key</code></p>
<pre>curl -fsSL $url/xrpla-archive-keyring.gpg | sudo tee /usr/share/keyrings/xrpla-archive-keyring.gpg &gt;/dev/null
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/xrpla-archive-keyring.gpg] $url stable main" | sudo tee /etc/apt/sources.list.d/xrpla.list
sudo apt update
sudo apt install xrpla</pre>
<p><a href="https://github.com/first-ledger/xrpla">Source and documentation</a></p>
</body></html>
HTML

echo "apt repository in $out: ${#packages[@]} package(s), signed by $key"
