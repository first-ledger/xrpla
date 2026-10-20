"""config/xrpla.example.toml documents every setting `xrpla run` reads, and nothing else.

The profiles read their settings in src/setup.cpp, as "section.name" strings and, for the
per-profile knobs, knob("name") calls (connected.name, and standalone.name for the ones the
mirrored profiles share). `xrpla setup` writes this reference with its choices filled in, so a
setting missing here would be written without its documentation, and a documented one that
nothing reads would mislead.
"""
from pathlib import Path
import re
import tomllib
import unittest

ROOT = Path(__file__).resolve().parents[1]
REFERENCE = ROOT / 'config' / 'xrpla.example.toml'
SETUP = ROOT / 'src' / 'setup.cpp'
SECTIONS = {'publication', 'history', 'restore', 'storage', 'serve', 'network', 'index', 'cache', 'mirror', 'node', 'validators',
            'connected', 'standalone', 'peers', 'log'}


def documented():
    """The reference's settings as section.name ('profile' for the top-level one), in order."""
    section, settings = '', []
    for line in REFERENCE.read_text(encoding='utf-8').splitlines():
        if m := re.fullmatch(r'#?\[([a-z_]+)\]', line): section = m.group(1)
        elif m := re.fullmatch(r'#?([a-z][a-z0-9_]*) = (.*)', line): settings.append((f'{section}.{m.group(1)}' if section else m.group(1), m.group(2)))
    return settings


def read_by_code():
    text = SETUP.read_text(encoding='utf-8')
    # Read through text(), integer(), ledger(), required(), flag_setting() and at_path(), or named in a list of them.
    accessor = r'(?:(?:text|integer|ledger|required|flag_setting|bounded)\(\s*(?:config|t),\s*|at_path\(|std::pair\{)"([a-z_]+)\.([a-z_]+)"'
    paths = {f'{s}.{n}' for s, n in re.findall(accessor, text) if s in SECTIONS}
    paths.add('profile')
    # knob("name", default): connected.name in run_connected, PROFILE.name in run_mirrored
    # (standalone, and connected with a publication).
    mirrored = text[text.index('int run_mirrored('):text.index('int run_config(')]
    for name in re.findall(r'knob\("([a-z_]+)"', text): paths.add(f'connected.{name}')
    for name in re.findall(r'knob\("([a-z_]+)"', mirrored): paths.add(f'standalone.{name}')
    return paths


class Reference(unittest.TestCase):
    def test_every_setting_read_is_documented_and_nothing_else(self):
        settings = [path for path, _ in documented()]
        self.assertEqual(len(settings), len(set(settings)), 'a setting is documented twice')
        read = read_by_code()
        self.assertEqual(sorted(read - set(settings)), [], 'read by src/setup.cpp but not documented')
        self.assertEqual(sorted(set(settings) - read), [], 'documented but read by nothing')

    def test_reference_parses_commented_and_uncommented(self):
        text = REFERENCE.read_text(encoding='utf-8')
        self.assertEqual(tomllib.loads(text), {}, 'the reference sets nothing itself')
        # Every commented setting and section line, uncommented, is valid TOML.
        uncommented = '\n'.join(line[1:] if re.fullmatch(r'#(\[[a-z_]+\]|[a-z][a-z0-9_]* = .*)', line) else line for line in text.splitlines())
        values = tomllib.loads(uncommented)
        self.assertEqual(values['profile'], 'mirror')
        self.assertEqual((values['index']['mpt_holders'], values['index']['nfts_by_issuer']), (True, True))
        self.assertEqual(values['serve']['forward'], 'http://127.0.0.1:51234')

    def test_each_setting_is_described(self):
        # A setting line follows a comment describing it, or another setting of the same group.
        lines = REFERENCE.read_text(encoding='utf-8').splitlines()
        for i, line in enumerate(lines):
            if re.fullmatch(r'#?[a-z][a-z0-9_]* = .*', line):
                above = lines[i - 1]
                self.assertTrue(above.startswith('# ') or re.fullmatch(r'#?[a-z][a-z0-9_]* = .*', above), f'{line!r} has no description')


if __name__ == '__main__':
    unittest.main()
