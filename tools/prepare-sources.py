"""Seed pinned sources before running Conan; do not run concurrently with it."""
import hashlib
import os
from pathlib import Path
import sys
import urllib.request

archives = [
    ("m4/m4-1.4.19.tar.xz", "63aede5c6d33b6d9b13511cd0be2cac046f2e70fd0a07aa9573a04a82783af96"),
    ("autoconf/autoconf-2.71.tar.xz", "f14c83cfebcc9427f2c3cea7258bd90df972d92eb26752da4ddad81c87a0faa4"),
    ("automake/automake-1.16.5.tar.gz", "07bd24ad08a64bc17250ce09ec56e921d6343903943e99ccf63bbf0705e34605"),
    ("libtool/libtool-2.4.7.tar.gz", "04e96c2404ea70c590c546eba4202a4e12722c640016c12b9b2f1ce3d481e9a8"),
]
for archive, digest in archives:
    cache = Path(sys.argv[1]) / "s" / digest
    dirty = cache.with_name(cache.name + ".dirty")
    if cache.is_file() and hashlib.sha256(cache.read_bytes()).hexdigest() == digest:
        dirty.unlink(missing_ok=True)
        continue
    errors = []
    for base in ("https://www.mirrorservice.org/sites/ftp.gnu.org/gnu/", "https://ftp.gnu.org/gnu/"):
        url = base + archive
        try:
            with urllib.request.urlopen(url, timeout=20) as response:
                data = response.read(10 * 1024 * 1024)
            if hashlib.sha256(data).hexdigest() != digest:
                raise ValueError("Source differs from the Conan recipe's pinned SHA-256")
            cache.parent.mkdir(parents=True, exist_ok=True)
            temp = cache.with_suffix(".part")
            temp.write_bytes(data)
            os.replace(temp, cache)
            dirty.unlink(missing_ok=True)
            print(f"Cached verified source from {url}")
            break
        except (OSError, ValueError) as error:
            errors.append(f"{url}: {error}")
    else:
        sys.exit("Unable to prepare pinned source:\n" + "\n".join(errors))
