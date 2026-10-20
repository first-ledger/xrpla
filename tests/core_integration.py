"""Real CLI + canonical libxrpl fixtures + persisted RocksDB, without network access."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

BINARY, FIXTURE = map(os.path.abspath, sys.argv[1:3])
sys.argv = sys.argv[:1]


class CoreIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture_tmp = tempfile.TemporaryDirectory(prefix="xrpla-core-fixtures-")
        cls.source = Path(cls.fixture_tmp.name) / "valid"
        cls.export(cls.source)
        cls.expected = json.loads((cls.source / "expected.json").read_text())

    @classmethod
    def tearDownClass(cls):
        cls.fixture_tmp.cleanup()

    @staticmethod
    def export(path, fault=""):
        env = dict(os.environ, XRPLA_CORE_FIXTURE_DIR=str(path), XRPLA_CORE_FIXTURE_FAULT=fault)
        result = subprocess.run([FIXTURE, "--gtest_filter=CoreFixture.Export"], env=env,
                                capture_output=True, text=True, timeout=90)
        if result.returncode:
            raise AssertionError(result.stdout + result.stderr)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="xrpla-core-test-")
        self.addCleanup(self.tmp.cleanup)
        self.data = Path(self.tmp.name) / "data"

    def cli(self, *args, ok=True):
        result = subprocess.run([BINARY, *map(str, args)], capture_output=True, text=True, timeout=120)
        self.assertEqual(result.returncode == 0, ok, result.stdout + result.stderr)
        return json.loads(result.stdout if ok else result.stderr)

    def fetch(self, range="65536-65538", source=None):
        source = source or self.source
        return self.cli("fetch", "--archive", source, "--range", range, "--dir", self.data,
                        "--publisher-key", source / "publisher.pub")

    def build(self, source=None, *args, **kwargs):
        source = source or self.source
        return self.cli("build", "--dir", self.data, "--publisher-key", source / "publisher.pub", *args, **kwargs)

    def query(self, kind, key, *args, **kwargs):
        return self.cli("query", "--dir", self.data, "--kind", kind, "--key", key, *args, **kwargs)

    def test_full_verify_build_queries_and_incremental_extension(self):
        self.fetch("65536-65537")
        verified = self.cli("verify", "--dir", self.data, "--publisher-key", self.source / "publisher.pub",
                            "--full", "--trust-ledger-hash", self.expected["anchor"])
        self.assertTrue(verified["ledger_hashes_verified"])
        self.assertEqual(verified["verified_through"], 131071)
        self.assertEqual(self.build()["ledgers_added"], 2)
        self.assertEqual(self.build()["ledgers_added"], 0)
        ledger = self.query("ledger", "65536")
        self.assertEqual(ledger["transactions"], [self.expected["tx0"], self.expected["tx1"]])
        self.assertEqual(self.query("ledger", ledger["ledger_hash"]), ledger)
        tx = self.query("tx", self.expected["tx0"])
        self.assertEqual(tx["ledger_index"], 65536)
        self.assertEqual(tx["meta"]["TransactionIndex"], 0)
        history = self.query("account_tx", self.expected["account"])
        self.assertEqual([item["tx_index"] for item in history["transactions"]], [1, 0])
        directory = self.query("object", self.expected["directory_key"], "--ledger", "65536")
        self.assertEqual(len(directory["object"]["Indexes"]), 2)
        self.assertFalse(self.query("object", self.expected["second_key"], "--ledger", "65536")["found"])
        self.assertTrue(self.query("object", self.expected["second_key"], "--ledger", "65537")["found"])
        self.fetch()
        self.assertEqual(self.build()["ledgers_added"], 1)
        self.assertFalse(self.query("object", self.expected["directory_key"])["found"])
        self.assertTrue(self.query("object", self.expected["directory_key"], "--ledger", "65537")["found"])
        state = self.cli("status", "--dir", self.data)
        self.assertEqual(state["complete_ledgers"], {"first": 65536, "last": 65538})
        self.query("object", self.expected["account_key"], "--ledger", "65539", ok=False)

    def test_prelude_seeds_state_but_excludes_earlier_transactions(self):
        self.fetch("65537-65538")
        self.assertEqual(self.build()["ledgers_added"], 2)
        self.assertFalse(self.query("tx", self.expected["tx0"])["found"])
        directory = self.query("object", self.expected["directory_key"], "--ledger", "65537")
        self.assertEqual(len(directory["object"]["Indexes"]), 2)

    def test_signed_bad_patch_and_late_table_do_not_create_store(self):
        for fault in ("patch", "offset"):
            with self.subTest(fault=fault):
                source = Path(self.tmp.name) / fault
                self.export(source, fault)
                self.fetch(source=source)
                self.build(source, ok=False)
                self.assertFalse((self.data / "store").exists())
        self.fetch()
        self.build()
        before = self.cli("status", "--dir", self.data)
        self.fetch(source=Path(self.tmp.name) / "patch")
        self.build(Path(self.tmp.name) / "patch", ok=False)
        self.assertEqual(self.cli("status", "--dir", self.data), before)

    def test_unknown_trust_anchor_and_changed_baseline_fail_safely(self):
        self.fetch()
        self.build(None, "--trust-ledger-hash", "0" * 64, ok=False)
        self.assertFalse((self.data / "store").exists())
        self.build(None, "--trust-ledger-hash", self.expected["snapshot_hash"], ok=False)
        self.assertFalse((self.data / "store").exists())
        self.build()
        before = self.cli("status", "--dir", self.data)
        self.fetch("65537-65538")
        self.build(ok=False)
        self.assertEqual(self.cli("status", "--dir", self.data), before)


if __name__ == "__main__":
    unittest.main(verbosity=2)
