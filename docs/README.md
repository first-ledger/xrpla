# XRPLA documentation

Start with the guide for what you want to do. Each guide lists what you need, the commands (with
the `xrpla setup` wizard and by hand), what to expect, how to check the result, and how to
upgrade and troubleshoot.

## Guides

| I want to... | Guide |
|---|---|
| Install or build XRPLA | [Install XRPLA](guides/install.md) |
| Serve full history over HTTP/WebSocket without running xrpld | [Standalone](guides/standalone.md) |
| Fill an xrpld (NuDB or RocksDB + SQLite) with history | [Restore into xrpld](guides/restore-xrpld.md) |
| Fill a Clio database (Cassandra or ScyllaDB) with history | [Restore into Clio](guides/restore-clio.md) |
| Serve a copy of a publication, or run XRPLA beside my own xrpld | [Mirror and connected](guides/mirror-and-connected.md) |
| Publish signed history to R2/S3 myself | [Run a publisher](guides/publish.md) |
| Cut an XRPLA release and its apt packages (maintainers) | [Make a release](guides/release.md) |

## Reference

| Document | Contents |
|---|---|
| [Glossary](reference/glossary.md) | Publication, catalog, piece, bundle, snapshot, spool, publisher key and other terms |
| [Publication list](reference/publications.md) | The approved publications `xrpla setup` offers by name: the list's format, its trust model, and how a publisher proposes an entry |
| [config/xrpla.example.toml](../config/xrpla.example.toml) | Every setting `xrpla run` reads: what it does, which profiles read it, its default and accepted values |
| [CLI reference](reference/cli.md) | Every `xrpla` command and option, and the Python tools |
| [API reference](reference/api.md) | Supported methods, subscriptions, forwarding, and known differences from xrpld and Clio |
| [Security model](reference/security.md) | What is verified and trusted, the service account, listeners, keys |
| [SETUP.md](SETUP.md) | The setup profiles in full: the service account and its directories, restore internals, connected, standalone, mirror, publishing sources, configuration sections, logs |
| [PUBLICATION.md](PUBLICATION.md) | Publication layout and file formats, the publisher tools, local query indexes, measurements |
| [PEER-SERVING.md](PEER-SERVING.md) | The `[peers]` settings and how a standalone node behaves on the XRPL peer network |
| [v2 archive tools](reference/archive-v2.md) | `fetch`, `verify`, `build`, `export`, `tail`, `prune`, `import` and the archive services |

## Background

[Internal notes](internal/README.md): design notes, plans and benchmark reports. They explain
why XRPLA is built as it is; they are not instructions. Raw benchmark data is under
[benchmarks/](benchmarks/).

## Contributing and tests

- `ctest --test-dir BUILD --output-on-failure`: format, canonical, CLI, RPC, crash recovery,
  real-mainnet replay and recorded Clio responses.
- `python3 tests/native_integration.py`: genuine xrpld, Clio and Cassandra containers, imports,
  restart and live conformance. Build `Dockerfile.integration` first.
- `python3 -m unittest tests/config_reference_test.py`: keeps
  `config/xrpla.example.toml` and the settings `src/setup.cpp` reads in step.
- `tools/benchmark.py`: reproducible elapsed time, peak memory and logical disk measurements.

GitHub Actions runs transport and core tests on push and pull request; a manual workflow
dispatch also runs the native-server integration.
