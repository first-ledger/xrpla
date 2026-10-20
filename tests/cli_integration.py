"""Exercise the real binary against a controlled HTTP server; no mainnet access."""
import http.server
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import tomllib
import unittest

BINARY, FIXTURE_TOOL = map(os.path.abspath, sys.argv[1:3])
sys.argv = sys.argv[:1]


class CliTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="xrpla-test-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root / "source"
        self.data = self.root / "data"
        env = dict(os.environ, XRPLA_TEST_FIXTURE_DIR=str(self.source))
        subprocess.run([FIXTURE_TOOL, "--gtest_filter=Fixture.Export"], env=env,
                       check=True, capture_output=True, timeout=20)
        self.manifest = json.loads((self.source / "manifest.json").read_text())
        self.chunk = self.manifest["chunks"][0]
        self.key = self.source / "publisher.pub"
        self.mode = "normal"
        self.events = []
        self.interrupted = False
        self.mixed_once = False
        self.refused = set()
        case = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                name = self.path.lstrip("/")
                path = case.source / name
                if not path.is_file():
                    self.send_error(404)
                    return
                if case.mode == "busy" and name in ("manifest.json", case.chunk["object"]) and name not in case.refused:
                    case.refused.add(name)
                    self.send_error(503)
                    return
                body = path.read_bytes()
                is_chunk = name.startswith("chunks/")
                if case.mode == "mixed" and name == "manifest.json.sig" and not case.mixed_once:
                    case.mixed_once = True
                    body = bytes(64)
                if is_chunk and case.mode == "corrupt":
                    body = b"X" + body[1:]
                offset = int(self.headers.get("Range", "bytes=0-").split("=")[1].split("-")[0])
                case.events.append((name, offset))
                if case.mode == "ignore-range":
                    offset = 0
                if offset:
                    self.send_response(206)
                    self.send_header("Content-Range", f"bytes {offset}-{len(body)-1}/{len(body)}")
                else:
                    self.send_response(200)
                self.send_header("Content-Length", str(len(body) - offset))
                self.end_headers()
                if is_chunk and case.mode == "interrupt" and not case.interrupted:
                    case.interrupted = True
                    self.wfile.write(body[offset:offset + 8192])
                    self.wfile.flush()
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
                try:
                    self.wfile.write(body[offset:])
                except (BrokenPipeError, ConnectionResetError):
                    pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 5)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def cli(self, *args, ok=True):
        result = subprocess.run([BINARY, *map(str, args)], capture_output=True, text=True, timeout=20)
        if ok:
            self.assertEqual(result.returncode, 0, result.stderr)
            return json.loads(result.stdout)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        return json.loads(result.stderr)

    def fetch(self, source=None, **kwargs):
        return self.cli("fetch", "--archive", source or self.url, "--range", "65536-131072",
                        "--dir", self.data, "--publisher-key", self.key, **kwargs)

    def verify(self, **kwargs):
        return self.cli("verify", "--dir", self.data, "--publisher-key", self.key, **kwargs)

    def test_fetch_verify_and_idempotent_rerun(self):
        result = self.fetch()
        self.assertFalse(result["ledger_hashes_verified"])
        self.assertEqual(result["objects"], 4)
        self.assertEqual(self.verify(), result)
        count = len([name for name, _ in self.events if name.startswith("chunks/")])
        self.fetch()
        self.assertEqual(len([name for name, _ in self.events if name.startswith("chunks/")]), count)

    def test_local_directory_source(self):
        self.fetch(self.source)
        self.verify()

    def test_interruption_resumes_within_the_fetch(self):
        self.mode = "interrupt"
        self.fetch()
        self.assertTrue(self.interrupted)
        self.assertTrue(any(name.startswith("chunks/") and offset > 0 for name, offset in self.events))
        self.verify()

    def test_busy_origin_is_tried_again(self):
        self.mode = "busy"
        self.fetch()
        self.assertEqual(self.refused, {"manifest.json", self.chunk["object"]})
        self.verify()

    def test_partial_from_an_earlier_run_resumes(self):
        staging = self.data / "archive/.staging"
        staging.mkdir(parents=True)
        (staging / (self.chunk["sha256"] + ".part")).write_bytes(
            (self.source / self.chunk["object"]).read_bytes()[:1234])
        self.fetch()
        self.assertIn((self.chunk["object"], 1234), self.events)
        self.verify()

    def test_resume_when_server_ignores_range(self):
        staging = self.data / "archive/.staging"
        staging.mkdir(parents=True)
        (staging / (self.chunk["sha256"] + ".part")).write_bytes(
            (self.source / self.chunk["object"]).read_bytes()[:1234])
        self.mode = "ignore-range"
        self.fetch()
        self.verify()

    def test_corrupt_remote_never_promoted(self):
        self.mode = "corrupt"
        self.fetch(ok=False)
        self.assertFalse((self.data / "archive" / self.chunk["object"]).exists())
        self.assertFalse((self.data / "archive/selection.json").exists())

    def test_verify_detects_local_corruption(self):
        self.fetch()
        path = self.data / "archive" / self.chunk["object"]
        contents = path.read_bytes()
        path.write_bytes(b"X" + contents[1:])
        self.assertIn("corrupt", self.verify(ok=False)["error"])
        self.fetch(ok=False)

    def test_mixed_manifest_generation_retries_pair(self):
        self.mode = "mixed"
        self.fetch()
        self.assertGreaterEqual(sum(name == "manifest.json" for name, _ in self.events), 2)

    def test_signature_failure_writes_no_cache(self):
        (self.source / "manifest.json.sig").write_bytes(bytes(64))
        self.fetch(ok=False)
        self.assertFalse(self.data.exists())

    def test_verify_uses_independent_key(self):
        self.fetch()
        self.key.write_bytes(bytes(32))
        self.verify(ok=False)

    def test_symlinked_download_parent_is_rejected(self):
        outside = self.root / "outside"
        outside.mkdir()
        archive = self.data / "archive"
        archive.mkdir(parents=True)
        (archive / "chunks").symlink_to(outside, target_is_directory=True)
        self.fetch(ok=False)
        self.assertEqual(list(outside.iterdir()), [])

    def test_existing_lock_prevents_mutation(self):
        (self.data / ".xrpla-lock").mkdir(parents=True)
        self.assertIn("locked", self.fetch(ok=False)["error"])
        self.assertFalse((self.data / "archive").exists())

    def test_flags_and_unimplemented_verification_fail_explicitly(self):
        for args in [("fetch",), ("fetch", "--jobs", "-1"), ("verify", "--full"),
                     ("fetch", "--trust-ledger-hash", "abc"), ("serve",),
                     ("fetch", "--unknown", "value"), ("status", "--dir", "a", "--dir", "b")]:
            self.cli(*args, ok=False)

    def test_toml_configuration_and_cli_precedence(self):
        config = self.root / "config.toml"
        config.write_text(f'archive = "{self.url}"\nrange = "65536-65536"\n'
                          f'dir = "{self.data}"\npublisher_key = "{self.key}"\njobs = 2\n')
        result = self.cli("fetch", "--config", config, "--range", "65536-131072")
        self.assertEqual(result["range"]["last"], 131072)
        self.verify()

    def test_status_does_not_claim_verification(self):
        self.fetch()
        result = self.cli("status", "--dir", self.data)
        self.assertEqual(result["verification"], "not-run")
        self.assertFalse(result["store_available"])



def with_profiles():
    """Whether the binary has `xrpla setup` and `xrpla run` (built with libxrpl)."""
    version = subprocess.run([BINARY, "--version"], capture_output=True, text=True, check=True).stdout
    return "libxrpl" in version


@unittest.skipUnless(os.name == "posix" and with_profiles(), "xrpla setup needs a POSIX build with libxrpl")
class SetupTests(unittest.TestCase):
    """`xrpla setup` answered on stdin; the unit is written to the test's directory, so no account
    is made and nothing is given to one."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="xrpla-setup-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.config = self.root / "etc" / "xrpla.toml"
        self.unit = self.root / "units" / "xrpla.service"

    def setup(self, storage, config=None, env=None, key=None, cwd=None):
        # mirror: publication, publisher key, first ledger, the storage directory answers (asked
        # again until one can be named in the unit), decode cache, listeners, sync interval. An
        # empty publication list, so the publication is asked for by URL and key.
        answers = ["mirror", str(self.root / "bucket"), key or str(self.root / "publisher.pub"), "", *storage, "", "", "", ""]
        (self.root / "publications.toml").write_text("")
        env = {**(os.environ if env is None else env), "XRPLA_PUBLICATIONS": str(self.root / "publications.toml")}
        return subprocess.run([BINARY, "setup", "--config", str(config or self.config), "--unit-dir", str(self.unit.parent)],
                              input="\n".join(answers) + "\n", capture_output=True, text=True, timeout=60, env=env, cwd=cwd)

    def written(self):
        return tomllib.loads(self.config.read_text(encoding="utf-8"))

    def test_a_directory_a_unit_cannot_name_is_asked_again(self):
        result = self.setup([str(self.root / "with space"), str(self.root / "store%d"), str(self.root / "store")])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(result.stdout.count("A systemd unit cannot name"), 2)
        self.assertEqual(self.written()["storage"]["dir"], str(self.root / "store"))
        self.assertIn(f"ReadWritePaths={self.root / 'store'}\n", self.unit.read_text())

    def test_only_characters_a_unit_takes_as_they_are_are_accepted(self):
        # $, quotes and backslashes mean something to ExecStart= and ReadWritePaths=.
        names = ["store$HOME", 'store"q', "store'q", "store\\b", "store;x"]
        result = self.setup([str(self.root / name) for name in names] + [str(self.root / "st-o_r.e@1+2:3")])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(result.stdout.count("A systemd unit cannot name"), len(names))
        self.assertEqual(self.written()["storage"]["dir"], str(self.root / "st-o_r.e@1+2:3"))

    def test_a_publisher_key_given_as_hex_is_written_beside_the_configuration(self):
        key = "ED" + "0123456789ABCDEF" * 3 + "0123456789ABCD"
        result = self.setup([str(self.root / "store")], key=key)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        written = self.config.parent / "publisher.pub"
        self.assertEqual(written.read_text(), key + "\n")
        self.assertEqual(self.written()["publication"]["publisher_key"], str(written))

    def test_key_paths_are_recorded_absolute(self):
        # The service runs from /: neither the publisher.pub beside a relative --config nor a
        # relative key file may be recorded as setup was given it.
        self.config.parent.mkdir()
        key = "ED" + "0123456789ABCDEF" * 3 + "0123456789ABCD"
        result = self.setup([str(self.root / "store")], config=self.config.name, key=key, cwd=self.config.parent)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.written()["publication"]["publisher_key"], str(self.config.parent / "publisher.pub"))
        result = self.setup([str(self.root / "store")], key="keys/../publisher.pub", cwd=self.root)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.written()["publication"]["publisher_key"], str(self.root / "publisher.pub"))
        result = self.setup([str(self.root / "store")], key="~/keys/publisher.pub", env=dict(os.environ, HOME=str(self.root)))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.written()["publication"]["publisher_key"], str(self.root / "keys" / "publisher.pub"))

    def test_a_relative_log_file_is_not_kept_and_an_absolute_one_kept_normal(self):
        self.config.parent.mkdir()
        self.config.write_text('profile = "mirror"\n\n[log]\nfile = "logs/service.log"\n', encoding="utf-8")
        result = self.setup([str(self.root / "store")])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("is not an absolute path", result.stdout)
        self.assertNotIn("log", self.written())
        self.config.write_text(f'[log]\nfile = "{self.root}/logs/../logs/service.log"\n', encoding="utf-8")
        result = self.setup([str(self.root / "store")])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Keeping [log]", result.stdout)
        self.assertEqual(self.written()["log"], {"file": str(self.root / "logs" / "service.log")})
        self.assertIn(f"ReadWritePaths={self.root / 'logs'}\n", self.unit.read_text())

    def test_a_leading_tilde_is_home(self):
        result = self.setup(["~/store/"], env=dict(os.environ, HOME=str(self.root)))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.written()["storage"]["dir"], str(self.root / "store"))

    def test_a_config_path_a_unit_cannot_name_is_refused(self):
        config = self.root / "my etc" / "xrpla.toml"
        result = self.setup([str(self.root / "store")], config=config)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("cannot name the configuration", result.stderr)
        self.assertFalse(config.exists())
        self.assertFalse(self.unit.exists())

    def test_the_log_section_is_kept_and_its_directory_written(self):
        log = self.root / "logs" / "service.log"
        self.config.parent.mkdir()
        self.config.write_text(f'profile = "mirror"\n\n[log]\nfile = "{log}"\nmax_mb = 7\n', encoding="utf-8")
        result = self.setup([str(self.root / "store")])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Keeping [log]", result.stdout)
        self.assertEqual(self.written()["log"], {"file": str(log), "max_mb": 7})
        unit = self.unit.read_text()
        for path in (self.root / "store", log.parent):
            self.assertIn(f"ReadWritePaths={path}\n", unit)
        # A log file in the storage directory needs no path of its own.
        self.config.write_text(f'[log]\nfile = "{self.root / "store" / "xrpla.log"}"\n', encoding="utf-8")
        self.assertEqual(self.setup([str(self.root / "store")]).returncode, 0)
        self.assertEqual(self.unit.read_text().count("ReadWritePaths="), 1)

    def test_serve_takes_no_loopback_trust_as_a_switch(self):
        result = subprocess.run([BINARY, "serve", "--no-loopback-trust", "--no-loopback-trust"], capture_output=True, text=True, timeout=20)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Duplicate option: --no-loopback-trust", result.stderr)



def publication(name, network="mainnet", key="ab" * 32, mirrors=("https://history.example.com/x",), **more):
    """One [[publication]] table of a publication list."""
    lines = ["[[publication]]", f'name = "{name}"', f'network = "{network}"', f'publisher_key = "{key}"',
             "mirrors = [" + ", ".join(f'"{m}"' for m in mirrors) + "]"]
    lines += [f"{field} = {json.dumps(value)}" for field, value in more.items()]
    return "\n".join(lines) + "\n\n"


class PublicationListTests(unittest.TestCase):
    """`xrpla publications` and the list it reads (config/publications.toml, or XRPLA_PUBLICATIONS)."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="xrpla-list-")
        self.addCleanup(self.tmp.cleanup)
        self.list = Path(self.tmp.name) / "publications.toml"

    def publications(self, text=None):
        env = dict(os.environ)
        env.pop("XRPLA_PUBLICATIONS", None)
        if text is not None:
            self.list.write_text(text, encoding="utf-8")
            env["XRPLA_PUBLICATIONS"] = str(self.list)
        result = subprocess.run([BINARY, "publications"], capture_output=True, text=True, timeout=20, env=env)
        return result

    def test_valid_entries_are_listed_and_invalid_ones_named(self):
        text = (publication("first-mainnet", first_ledger=32570, description="the first", homepage="https://example.com", contact="ops@example.com")
                + publication("second-mainnet", key="AB" * 32, mirrors=("https://a.example.com/p/", "http://127.0.0.1:8080/p"))
                + publication("short-key", key="ab" * 31)
                + publication("no-mirrors", mirrors=())
                + publication("plain-http", mirrors=("http://history.example.com/x",))
                + publication("with-query", mirrors=("https://history.example.com/x?a=1",))
                + publication("a-directory", mirrors=("/srv/bucket",))
                + publication("first-mainnet")
                + publication("other")
                + publication("Upper")
                + publication("moon", network="moonnet")
                + publication("bad-first", first_ledger=0))
        result = self.publications(text)
        self.assertEqual(result.returncode, 0, result.stderr)
        listed = json.loads(result.stdout)
        self.assertEqual(listed["list"], str(self.list))
        self.assertEqual([p["name"] for p in listed["publications"]], ["first-mainnet", "second-mainnet"])
        first, second = listed["publications"]
        self.assertEqual((first["network"], first["first_ledger"], first["mirrors"], first["contact"]),
                         ("mainnet", 32570, ["https://history.example.com/x"], "ops@example.com"))
        self.assertEqual((second["publisher_key"], second["first_ledger"]), ("ab" * 32, None))  # the key lowercase
        self.assertEqual(second["mirrors"], ["https://a.example.com/p/", "http://127.0.0.1:8080/p"])
        invalid = {entry["entry"]: entry["error"] for entry in listed["invalid"]}
        self.assertEqual(set(invalid), {"short-key", "no-mirrors", "plain-http", "with-query", "a-directory", "first-mainnet", "other", "Upper", "moon", "bad-first"})
        self.assertIn("publisher_key must be 64 hexadecimal characters", invalid["short-key"])
        self.assertIn("no mirrors", invalid["no-mirrors"])
        self.assertIn("HTTPS", invalid["plain-http"])
        self.assertIn("query", invalid["with-query"])
        self.assertIn("must be a URL", invalid["a-directory"])
        self.assertIn("another entry has its name", invalid["first-mainnet"])
        self.assertIn("network must be", invalid["moon"])

    def test_a_list_that_is_not_toml_is_an_error(self):
        result = self.publications("[[publication]\nname = ")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("is not valid TOML", result.stderr)
        missing = self.list.parent / "missing.toml"
        result = subprocess.run([BINARY, "publications"], capture_output=True, text=True, timeout=20, env=dict(os.environ, XRPLA_PUBLICATIONS=str(missing)))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(f"The publication list {missing} was not found", result.stderr)
        result = self.publications('publication = "x"\n')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must hold [[publication]] tables", result.stderr)

    @unittest.skipUnless(sys.platform.startswith("linux"), "the installed list is found beside /proc/self/exe")
    def test_the_shipped_list_is_found_and_valid(self):
        # A binary run from its build directory reads the source tree's list; every entry in it
        # must be valid (an entry not filled in yet ships commented out).
        result = self.publications()
        self.assertEqual(result.returncode, 0, result.stderr)
        listed = json.loads(result.stdout)
        self.assertEqual(Path(listed["list"]).resolve(), (Path(__file__).resolve().parents[1] / "config" / "publications.toml"))
        self.assertNotIn("invalid", listed)
        shipped = tomllib.loads(Path(listed["list"]).read_text(encoding="utf-8"))
        self.assertEqual(len(shipped.get("publication", [])), len(listed["publications"]))


@unittest.skipUnless(os.name == "posix" and with_profiles(), "xrpla setup and run need a POSIX build with libxrpl")
class ListedPublicationTests(unittest.TestCase):
    """`xrpla setup` offering the list, and `xrpla run` resolving [publication] name."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="xrpla-listed-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.list = self.root / "publications.toml"
        self.list.write_text(publication("alpha-mainnet", description="Alpha's history", first_ledger=32570)
                             + publication("beta-mainnet", key="cd" * 32, mirrors=("https://beta.example.com/h", "https://beta-2.example.com/h"))
                             + publication("gamma-testnet", network="testnet", key="ef" * 32)
                             + publication("broken", key="xyz"), encoding="utf-8")
        self.env = dict(os.environ, XRPLA_PUBLICATIONS=str(self.list))
        self.config = self.root / "etc" / "xrpla.toml"

    def setup(self, *answers):
        # mirror: network, publication (and, for other, its URL and key), first ledger,
        # storage directory, decode cache, listeners, sync interval.
        answers = ["mirror", *answers, "", str(self.root / "store"), "", "", "", ""]
        result = subprocess.run([BINARY, "setup", "--config", str(self.config), "--unit-dir", str(self.root / "units")],
                                input="\n".join(answers) + "\n", capture_output=True, text=True, timeout=60, env=self.env)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout, tomllib.loads(self.config.read_text(encoding="utf-8"))

    def test_setup_offers_the_networks_publications_and_writes_the_name(self):
        printed, written = self.setup("", "2")
        self.assertIn("Which network?", printed)
        self.assertIn("1) alpha-mainnet - Alpha's history (from ledger 32570)", printed)
        self.assertIn("2) beta-mainnet - https://beta.example.com/h", printed)
        self.assertIn("3) other - ", printed)
        self.assertNotIn("gamma-testnet", printed)
        self.assertIn("Not offering broken from", printed)
        self.assertEqual(written["publication"], {"name": "beta-mainnet"})  # no URL or key: the list's apply
        self.assertNotIn("network", written)
        self.assertFalse((self.config.parent / "publisher.pub").exists())
        printed, written = self.setup("testnet", "")
        self.assertEqual(written["publication"], {"name": "gamma-testnet"})
        self.assertEqual(written["network"], {"id": "testnet"})
        printed, written = self.setup("", "alpha-mainnet")
        self.assertEqual(written["publication"], {"name": "alpha-mainnet"})

    def test_setup_goes_on_without_a_list_it_cannot_read(self):
        self.list.write_text("[[publication]\n", encoding="utf-8")
        printed, written = self.setup("https://elsewhere.example.com/pub", "0123456789abcdef" * 4)
        self.assertIn("Not offering the publication list: The publication list " + str(self.list) + " is not valid TOML", printed)
        self.assertEqual(written["publication"]["source"], "https://elsewhere.example.com/pub")

    def test_setup_warns_that_the_service_reads_the_installed_list(self):
        printed, _ = self.setup("", "")
        self.assertIn("Warning: XRPLA_PUBLICATIONS names " + str(self.list) + ", which the service will not read", printed)

    def test_setup_still_takes_another_publication_by_url_and_key(self):
        printed, written = self.setup("", "other", "https://elsewhere.example.com/pub", "0123456789abcdef" * 4)
        self.assertEqual(written["publication"], {"source": "https://elsewhere.example.com/pub", "publisher_key": str(self.config.parent / "publisher.pub")})

    def run_restore(self, publication_section):
        self.config.parent.mkdir(exist_ok=True)
        self.config.write_text('profile = "restore"\n\n[publication]\n' + publication_section
                               + f'\n[restore]\ntarget = "store"\nout = "{self.root / "out"}"\n', encoding="utf-8")
        result = subprocess.run([BINARY, "run", "--config", str(self.config)], capture_output=True, text=True, timeout=60, env=self.env)
        self.assertNotEqual(result.returncode, 0)
        return result.stderr

    def test_run_refuses_a_publication_it_cannot_resolve(self):
        self.assertIn("No publication named delta-mainnet in " + str(self.list), self.run_restore('name = "delta-mainnet"\n'))
        self.assertIn("Publication broken in " + str(self.list) + " cannot be used: its publisher_key", self.run_restore('name = "broken"\n'))
        other = self.root / "other.pub"
        other.write_text("ab" * 32 + "\n")
        self.assertIn("is not publication beta-mainnet's", self.run_restore(f'name = "beta-mainnet"\npublisher_key = "{other}"\n'))
        self.assertIn("must be a URL or directory, or an array of them", self.run_restore('source = 5\npublisher_key = "x"\n'))
        self.assertIn("must be a URL or directory, or an array of them", self.run_restore('source = []\npublisher_key = "x"\n'))
        self.assertIn("Config needs publication.name, or publication.source and publication.publisher_key", self.run_restore(""))
        self.assertIn("Config needs publication.publisher_key", self.run_restore('source = "https://a.example.com/h"\n'))
        # The listed key given again (in another form) is no conflict: the restore goes on, and
        # fails only on the source given, which holds no publication.
        same = self.root / "same.pub"
        same.write_bytes(bytes.fromhex("cd" * 32))
        self.assertNotIn("is not publication", self.run_restore(f'name = "beta-mainnet"\npublisher_key = "{same}"\nsource = "{self.root}"\n'))


if __name__ == "__main__":
    unittest.main()
