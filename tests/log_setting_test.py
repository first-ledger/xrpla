"""`xrpla run` with a [log] file: what it reports goes to the file instead of stdout and stderr,
and the file starts over past max_mb, keeping `keep` older ones.

The run here fails at once (a restore profile without its settings), after reporting each
unknown setting in the config: about 20,000 of them make enough output to rotate a 1 MB log.
"""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

BINARY = os.path.abspath(sys.argv[1])
sys.argv = sys.argv[:1]


class LogSetting(unittest.TestCase):
    def run_with(self, directory, log_section, unknown=0):
        config = directory / 'xrpla.toml'
        extra = ''.join(f'unknown_setting_{i:05} = {i}\n' for i in range(unknown))
        config.write_text(f'profile = "restore"\n{log_section}\n[storage]\n{extra}', encoding='utf-8')
        return subprocess.run([BINARY, 'run', '--config', str(config)], capture_output=True, text=True, timeout=120)

    def test_output_goes_to_the_log_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            d = Path(temporary)
            log = d / 'logs' / 'xrpla.log'
            result = self.run_with(d, f'[log]\nfile = "{log.as_posix()}"', unknown=3)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual((result.stdout, result.stderr), ('', ''))
            text = log.read_text(encoding='utf-8')
            self.assertEqual(text.count('config_unknown_setting'), 3)
            self.assertIn('publication.source', text)  # the error the run stopped on

    def test_the_file_starts_over_past_max_mb_keeping_keep(self):
        with tempfile.TemporaryDirectory() as temporary:
            d = Path(temporary)
            log = d / 'xrpla.log'
            # About 4 MB of output: three rotations, of which `keep` = 2 files are kept.
            self.run_with(d, f'[log]\nfile = "{log.as_posix()}"\nmax_mb = 1\nkeep = 2', unknown=30000)
            kept = [Path(f'{log}.2'), Path(f'{log}.1'), log]
            self.assertTrue(all(p.exists() for p in kept))
            self.assertFalse(Path(f'{log}.3').exists())
            self.assertTrue(all(p.stat().st_size >= 1 << 20 for p in kept[:2]))
            # Rotated at line boundaries: every line is whole, and the oldest file went first.
            lines = [line for p in kept for line in p.read_text(encoding='utf-8').splitlines()]
            self.assertTrue(all(line.startswith('{') and line.endswith('}') for line in lines))
            reported = [line for line in lines if 'config_unknown_setting' in line]
            self.assertLess(len(reported), 30000)
            self.assertIn('storage.unknown_setting_29999', reported[-1])
            self.assertNotIn('storage.unknown_setting_00000', ''.join(reported))

    def test_without_a_file_output_stays_on_stderr(self):
        with tempfile.TemporaryDirectory() as temporary:
            result = self.run_with(Path(temporary), '', unknown=1)
            self.assertIn('config_unknown_setting', result.stderr)


if __name__ == '__main__':
    unittest.main()
