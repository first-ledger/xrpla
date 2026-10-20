# Make a release

For maintainers. A tag `vX.Y.Z` runs [.github/workflows/release.yml](../../.github/workflows/release.yml):
it builds the packages in the pinned build image (running the test suite), attaches the `.deb`,
`.tar.gz` and `SHA256SUMS` to a GitHub release, and republishes the signed apt repository at
<https://first-ledger.github.io/xrpla/> from the `.deb` of every release, so older versions stay
installable.

## Once: the signing key and Pages

1. Create the repository's signing key on a machine you trust, without a passphrase or with one
   you also store as a secret:

   ```sh
   gpg --quick-gen-key "XRPLA apt repository" ed25519 sign never
   gpg --armor --export-secret-keys "XRPLA apt repository" > apt-signing-key.asc
   gpg --fingerprint "XRPLA apt repository"
   ```

2. In the repository's **Settings > Secrets and variables > Actions**, add `APT_SIGNING_KEY`
   with the contents of `apt-signing-key.asc` (and `APT_SIGNING_PASSPHRASE` if the key has one).
   Then delete `apt-signing-key.asc`, keeping an offline backup of the key: a new key means
   every user must fetch it again.
3. In **Settings > Pages**, set **Source** to **GitHub Actions**.
4. Publish the fingerprint somewhere users can check it independently of the repository page,
   as with the publisher key.

## Each release

1. Set the version in `CMakeLists.txt` (`project(xrpla VERSION X.Y.Z ...)`) and commit it. The
   workflow refuses a tag that differs from it.
2. Tag and push:

   ```sh
   git tag -a vX.Y.Z -m "XRPLA X.Y.Z"
   git push origin vX.Y.Z
   ```

3. Watch the **Release** workflow. When it finishes, edit the release notes it generated: add
   the signing key's fingerprint, and flag any change to index or store formats.
4. Check the published package from a clean machine:

   ```sh
   sudo apt update && apt-cache policy xrpla
   ```

To republish the repository without a new release (after deleting a release, or with a new
signing key), run the **Release** workflow by hand (**Actions > Release > Run workflow**).

## What a release contains

| File | Contents |
|---|---|
| `xrpla_X.Y.Z_amd64.deb` | `/usr/bin/xrpla` and its tools, the systemd units, the configuration reference, the approved publications list, licence notices. Depends on `libc6 (>= 2.31), ca-certificates, python3, libzstd1, libsodium23`. Installing starts nothing. |
| `xrpla-X.Y.Z-Linux.tar.gz` | The same files under `usr/`. |
| `SHA256SUMS` | Their SHA-256 values. |

The binaries are stripped of debug information. `tools/build-apt-repo.sh` builds the apt
repository and can be run by hand: `tools/build-apt-repo.sh DEB_DIR OUT_DIR [KEY_ID]`.
