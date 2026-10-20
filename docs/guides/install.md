# Install XRPLA

XRPLA is one command-line program, `xrpla`, plus a few helper tools. It runs on **Linux x86-64
with glibc 2.31 or later** (Ubuntu 20.04 or later, Debian 12 or later, and similar). On Windows
use Docker Desktop or WSL; native Windows is not supported.

## Install with apt (recommended)

Add the XRPLA repository once, then install:

```sh
curl -fsSL https://first-ledger.github.io/xrpla/xrpla-archive-keyring.gpg   | sudo tee /usr/share/keyrings/xrpla-archive-keyring.gpg >/dev/null
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/xrpla-archive-keyring.gpg] https://first-ledger.github.io/xrpla stable main"   | sudo tee /etc/apt/sources.list.d/xrpla.list
sudo apt update
sudo apt install xrpla
```

`sudo apt upgrade` then keeps it current; `apt install xrpla=VERSION` installs an older
release. The repository is signed; its key's fingerprint is on
[the repository page](https://first-ledger.github.io/xrpla/) and in each release's notes.
Installing starts nothing: see [Nothing starts by itself](#nothing-starts-by-itself).

Each [GitHub release](https://github.com/first-ledger/xrpla/releases) also has the `.deb`, a
`.tar.gz` of the same files under `usr/`, and their `SHA256SUMS`. To install a downloaded
package: `sudo apt install ./xrpla_VERSION_amd64.deb`.

## Build with Docker

The build image pins the official XRPL build environment (GCC 15) and all dependencies
(libxrpl 3.4.1, RocksDB 10.5.1); it runs the test suite as part of the build.

```sh
git clone https://github.com/first-ledger/xrpla.git && cd xrpla
docker build -f Dockerfile.core -t xrpla-core:local .
docker run --rm xrpla-core:local --version
```

The final image is Ubuntu 22.04 with XRPLA installed under `/usr/local`, CA certificates and
Python 3. You can run XRPLA from the image, or copy the installed tree onto a host:

```sh
id=$(docker create xrpla-core:local)
docker cp "$id":/usr/local/. ./xrpla-install/
docker rm "$id"
```

To build the `.deb` and `.tar.gz` instead, as releases are built:

```sh
docker build -f Dockerfile.core --target package --output dist .
```

The generic `Dockerfile` builds a transport-only development version without libxrpl; it cannot
verify ledgers, restore or serve. Use `Dockerfile.core`.

## Build from source

With a Conan and CMake build of the core (as `Dockerfile.core` does it):

```sh
cmake --install BUILD --prefix /usr
```

installs:

| Path | What |
|---|---|
| `/usr/bin/xrpla` | the program |
| `/usr/bin/xrpla_history_decode`, `xrpla_history_encode`, `xrpla_snapshot`, `xrpla_tx_index` | compiled tools used by the publisher |
| `/usr/share/xrpla/xrpla.example.toml` | the configuration reference |
| `/usr/share/xrpla/clio_load.py` | the Clio loader |
| `/usr/share/xrpla/tools/` | the publisher, R2 transport and audit tools (Python) |
| `/usr/lib/systemd/system/` | `xrpla-publisher.service`, `xrpla-serve.service`, `xrpla-tail.service` |
| `/usr/share/xrpla/licenses/` | third-party licence notices |

`cpack --config BUILD/CPackConfig.cmake` produces a `.deb` and a `.tar.gz` installing the same
files under `/usr`. Installation uses `patchelf` to remove build-environment paths.

Install under `/usr`: `xrpla setup` writes a systemd unit that runs the program it was started
as, and warns when that is not an installed program (a build tree, a home directory).

### Native build

The build needs GCC 15, Conan 2, CMake and Ninja on x86-64 Linux; the dependencies are pinned by
`conan.lock` and built for glibc 2.31 by `tools/core-profile`. The XRPL Foundation's
`nix-ubuntu` image named in `Dockerfile.core` has that toolchain. From the repository:

```sh
conan profile detect --force
conan remote add xrplf https://conan.xrplf.org/repository/conan/ --index 0 --force
python3 tools/prepare-sources.py ~/.cache/xrpla-sources
conan install . -of build-core --build=missing -pr:h tools/core-profile -pr:b tools/core-profile   -cc core.sources:download_cache=$HOME/.cache/xrpla-sources
cmake -S . -B build-core/Release -G Ninja -DCMAKE_BUILD_TYPE=Release   -DCMAKE_TOOLCHAIN_FILE=$PWD/build-core/generators/conan_toolchain.cmake
cmake --build build-core/Release
```

`tools/prepare-sources.py` fetches the GNU build-tool sources (m4, autoconf, automake, libtool)
some recipes download, checked against the recipes' pinned SHA-256, from a mirror
into the cache `conan install` reads. The first `conan install` builds the dependencies,
libxrpl among them, which takes a while. To run the tests, give them the system libraries the
Python tools load (`libzstd1` and `libsodium23` installed):

```sh
XRPLA_ZSTD_LIBRARY=/usr/lib/x86_64-linux-gnu/libzstd.so.1 XRPLA_SODIUM_LIBRARY=/usr/lib/x86_64-linux-gnu/libsodium.so.23   ctest --test-dir build-core/Release --output-on-failure
```

Then install with `cmake --install build-core/Release --prefix /usr`, or build packages with
`cpack` as above.

## Runtime dependencies

- `ca-certificates` (HTTPS to publications).
- `python3` for the Clio loader and, with the system `libzstd` and `libsodium`, for the
  publisher and the connected profile's local publishing pipeline. Standalone, mirror and
  xrpld restores do not need Python.

The `.deb` declares `libc6 (>= 2.31), ca-certificates, python3, libzstd1, libsodium23`.

## Nothing starts by itself

Installing creates no service and starts nothing. Choose what the machine does with
`sudo xrpla setup` (see [Choose your path](../../README.md#choose-your-path)), or run the
commands by hand.

## Try it on the included fixture

The repository includes a small signed mainnet archive (five ledgers) that needs no network:
see [v2 archive tools](../reference/archive-v2.md#try-the-included-public-fixture).
