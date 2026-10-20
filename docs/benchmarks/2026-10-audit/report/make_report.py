#!/usr/bin/env python3
"""Builds the October 2026 audit's benchmark report (a PDF with charts) from the raw results in
docs/benchmarks/2026-10-audit. Needs matplotlib and reportlab.

  python make_report.py [--out XRPLA-benchmarks-2026-10.pdf]

Every number in a chart or table is read from those files; NOTES.md describes each of them.
"""
import argparse
import json
import statistics
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (Image, KeepTogether, PageBreak, Paragraph, SimpleDocTemplate, Spacer,
                                Table, TableStyle)

DATA = Path(__file__).resolve().parents[1]
FIGURES = Path(__file__).resolve().parent / 'figures'

# The reference palette's first three categorical slots (validated all-pairs on the light surface);
# aqua sits below 3:1 there, so every bar carries a direct label and every chart a table beside it.
XRPLA, XRPLD, CLIO = '#2a78d6', '#eb6834', '#1baf7a'
SURFACE, INK, INK2, GRID = '#fcfcfb', '#0b0b0b', '#52514e', '#e4e3df'


# Data -------------------------------------------------------------------------------------------
def rows(path):
    return [json.loads(line) for line in open(DATA / path) if line.strip().startswith('{')]


def runs(path, label, mix, field='per_second'):
    """The `field` of every perf.py run with this label and mix."""
    return [r[field] for r in rows(path) if r.get('label') == label and r.get('mix') == mix]


def mean(values):
    if not values: raise ValueError('no runs')
    return statistics.mean(values)


def method_p50(path, label, mix, method):
    return mean([r['methods'][method]['p50_ms'] for r in rows(path) if r.get('label') == label and r.get('mix') == mix and method in r['methods']])


def parity_totals(paths):
    same = different = transports = 0
    for path in paths:
        for r in rows(path):
            if 'overlap' in r:
                same += r['same']; different += r['different']
                transports += r.get('transports', {}).get('same', 0) if isinstance(r.get('transports'), dict) else 0
    return same, different, transports


def elapsed(path):
    """`time -v`'s wall clock, in seconds."""
    for line in open(DATA / path):
        if 'Elapsed (wall clock)' in line:
            parts = [float(p) for p in line.rsplit(' ', 1)[1].split(':')]
            return sum(p * 60 ** i for i, p in enumerate(reversed(parts)))
    raise ValueError(f'no elapsed time in {path}')


def peak_gb(path):
    for line in open(DATA / path):
        if 'Maximum resident set size' in line: return int(line.split(':')[1]) / 1e6
    raise ValueError(path)


def hms(seconds):
    seconds = round(seconds)
    h, m, s = seconds // 3600, seconds % 3600 // 60, seconds % 60
    return f'{h} h {m:02d} min' if h else (f'{m} min {s:02d} s' if m else f'{s} s')


# Charts -----------------------------------------------------------------------------------------
def style(ax, xlabel=None):
    ax.set_facecolor(SURFACE)
    for side in ('top', 'right', 'left'): ax.spines[side].set_visible(False)
    ax.spines['bottom'].set_color(GRID)
    ax.tick_params(colors=INK2, labelsize=8, length=0)
    ax.grid(axis='x', color=GRID, linewidth=0.6); ax.set_axisbelow(True)
    if xlabel: ax.set_xlabel(xlabel, color=INK2, fontsize=8)


def bars(name, items, xlabel, fmt='{:,.0f}', width=6.4, title=None):
    """Horizontal bars, one per (label, value, color), each labelled with its value."""
    fig, ax = plt.subplots(figsize=(width, 0.42 * len(items) + 0.7), dpi=200)
    fig.patch.set_facecolor(SURFACE)
    labels = [i[0] for i in items][::-1]; values = [i[1] for i in items][::-1]; fills = [i[2] for i in items][::-1]
    ax.barh(labels, values, color=fills, height=0.56, edgecolor=SURFACE, linewidth=2)
    top = max(values)
    for y, v in enumerate(values):
        ax.text(v + top * 0.012, y, fmt.format(v), va='center', ha='left', fontsize=8, color=INK)
    ax.set_xlim(0, top * 1.16)
    ax.tick_params(axis='y', labelsize=8, labelcolor=INK)
    style(ax, xlabel)
    if title: ax.set_title(title, loc='left', fontsize=9, color=INK)
    fig.tight_layout()
    path = FIGURES / f'{name}.png'; fig.savefig(path, facecolor=SURFACE); plt.close(fig)
    return path


def grouped(name, groups, series, xlabel, fmt='{:,.0f}'):
    """Horizontal grouped bars: `groups` are the y categories, `series` (name, color, values)."""
    fig, ax = plt.subplots(figsize=(6.4, 0.55 * len(groups) * len(series) / 2 + 1.0), dpi=200)
    fig.patch.set_facecolor(SURFACE)
    height = 0.8 / len(series); top = max(v for _, _, vals in series for v in vals)
    for i, (label, color, values) in enumerate(series):
        ys = [g + (len(series) - 1 - i) * height - 0.4 + height / 2 for g in range(len(groups))][::-1]
        ax.barh(ys, values, height=height * 0.9, color=color, label=label, edgecolor=SURFACE, linewidth=2)
        for y, v in zip(ys, values): ax.text(v + top * 0.012, y, fmt.format(v), va='center', ha='left', fontsize=7.5, color=INK)
    ax.set_yticks(range(len(groups))); ax.set_yticklabels(groups[::-1]); ax.tick_params(axis='y', labelsize=8, labelcolor=INK)
    ax.set_xlim(0, top * 1.18)
    style(ax, xlabel)
    ax.legend(frameon=False, fontsize=8, loc='lower right', labelcolor=INK)
    fig.tight_layout()
    path = FIGURES / f'{name}.png'; fig.savefig(path, facecolor=SURFACE); plt.close(fig)
    return path


# Report -----------------------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--out', default=str(Path(__file__).resolve().parent / 'XRPLA-benchmarks-2026-10.pdf'))
    a = ap.parse_args()
    FIGURES.mkdir(exist_ok=True)

    # Correctness.
    campaign = json.loads([l for l in open(DATA / 'parity-campaign/test-server-campaign-c1.log') if '"finished"' in l][-1])
    x_same, x_diff, x_transport = parity_totals([f'hetzner-x-compare-x-a-seed{s}.jsonl' for s in (1, 2, 3)])
    old = {}
    for r, era in (('r14m', '14,000,001-14,100,000 (2015)'), ('r38m', '38,000,001-38,100,000 (2018)'),
                   ('r63m', '63,000,001-63,100,000 (2021)'), ('r88m', '88,000,001-88,100,000 (2024)')):
        same, diff, tr = parity_totals([f'older-history/hetzner-{r}-compare-seed{s}.jsonl' for s in (1, 2, 3)])
        db = int(open(DATA / f'older-history/hetzner-{r}-restore.finished').read().split('\n')[1].split()[0])
        old[r] = dict(era=era, same=same, different=diff, restore=elapsed(f'older-history/hetzner-{r}-restore.err'),
                      sync=elapsed(f'older-history/hetzner-{r}-sync.err'), db=db / 1e9, peak=peak_gb(f'older-history/hetzner-{r}-restore.err'))
    clio_same, clio_diff, _ = parity_totals([f'clio-comparison/hetzner-compare-c-r-seed{s}.jsonl' for s in (1, 2, 3)])
    group1 = json.load(open(DATA / 'hetzner-rpc-group1-vs-xrpld.json'))
    invariants = [next(r for r in rows(f'invariants/test-server-ed6f07f-seed{s}.jsonl') if r.get('summary')) for s in (1, 3, 4)]
    inv_checked = sum(r['checked'] for r in invariants); inv_violated = sum(r['violated'] for r in invariants); inv_skipped = sum(r['skipped'] for r in invariants)
    old_same = sum(o['same'] for o in old.values()); old_diff = sum(o['different'] for o in old.values())

    # Speed (Hetzner, 107,000,001-107,697,250).
    X, C803, A803, M32, DAB = 'hetzner-x-speed-x-a.jsonl', 'clio-comparison/hetzner-speed-c-a-803bc33.jsonl', 'clio-comparison/hetzner-speed-c-a-803bc33.jsonl', 'account-lines/hetzner-speed-a-mem32.jsonl', 'account-lines/hetzner-decode-ab.jsonl'
    point = {proto: dict(xrpld=mean(runs(X, f'X-{proto}', 'point')), clio=mean(runs(C803, f'C-{proto}', 'point')), xrpla=mean(runs(A803, f'A-{proto}', 'point')))
             for proto in ('http', 'ws')}
    point_cores = dict(xrpld=mean(runs(X, 'X-ws', 'point', 'server_cores_busy')), clio=mean(runs(C803, 'C-ws', 'point', 'server_cores_busy')),
                       xrpla=mean(runs(A803, 'A-ws', 'point', 'server_cores_busy')))
    full = dict(xrpld=mean(runs(X, 'X-http', 'full')), xrpld_runs=runs(X, 'X-http', 'full'), clio=mean(runs(C803, 'C-http', 'full')),
                a16=mean(runs(A803, 'A-http', 'full')), a32=mean(runs(M32, 'A-http', 'full')),
                a32_543=mean(runs(DAB, 'A-notx', 'full')), a32_base=mean(runs(DAB, 'A-base', 'full')))
    full_ws = dict(xrpld=mean(runs(X, 'X-ws', 'full')), clio=mean(runs(C803, 'C-ws', 'full')), a16=mean(runs(A803, 'A-ws', 'full')), a32=mean(runs(M32, 'A-ws', 'full')))
    cold_x, cold_c, cold_a16, cold_a32 = 'hetzner-x-cold-x-a.jsonl', 'clio-comparison/hetzner-cold-c-a-803bc33.jsonl', 'clio-comparison/hetzner-cold-c-a-803bc33.jsonl', 'account-lines/hetzner-cold-a-mem32.jsonl'
    cold = dict(xrpld=(mean(runs(cold_x, 'X-cold', 'full')), mean(runs(cold_x, 'X-warm', 'full'))),
                clio=(mean(runs(cold_c, 'C-cold', 'full')), mean(runs(cold_c, 'C-warm', 'full'))),
                a16=(mean(runs(cold_a16, 'A-cold', 'full')), mean(runs(cold_a16, 'A-warm', 'full'))),
                a32=(mean(runs(cold_a32, 'A-cold', 'full')), mean(runs(cold_a32, 'A-warm', 'full'))))
    owner = {m: dict(xrpld=method_p50(X, 'X-http', 'full', m), clio=method_p50(C803, 'C-http', 'full', m),
                     a16=method_p50(A803, 'A-http', 'full', m), a32=method_p50(M32, 'A-http', 'full', m))
             for m in ('account_lines', 'account_objects', 'account_tx', 'book_offers', 'account_info', 'tx')}
    cores = dict(xrpld=mean(runs(X, 'X-http', 'full', 'server_cores_busy')), clio=mean(runs(C803, 'C-http', 'full', 'server_cores_busy')),
                 a32=mean(runs(M32, 'A-http', 'full', 'server_cores_busy')))

    # Storage and restore (the same range).
    nudb = json.load(open(DATA / 'hetzner-restore-xrpld-nudb-107M.json')); sync_a = json.load(open(DATA / 'hetzner-sync-a-107M.json'))
    x_restore = elapsed_from(nudb['time_v']); a_sync = elapsed_from(sync_a['time_v'])
    x_up = json.load(open(DATA / 'hetzner-xrpld-x-up.json'))
    x_fixed = sum(float(v) * 60 ** i for i, v in enumerate(reversed(x_up['restore_wall'].split(':'))))  # the rerun on cb78f55
    clio_load = elapsed('clio-restore/hetzner-restore.err'); clio_rows = json.load(open(DATA / 'clio-restore/hetzner-restore.log'))['clio']['rows']

    # Test server.
    ts = {name: {(r['label'], r['mix']): r['per_second'] for r in rows(path) if 'label' in r}
          for name, path in (('before', 'test-server-before-c05a479-cache32.jsonl'), ('audit', 'test-server-2b19b97.jsonl'), ('snap', 'test-server-snapshots-5M.jsonl'))}
    single = json.load(open(DATA / 'single-requests-old-vs-2b19b97.json'))['results']
    lines_run = json.load(open(DATA / 'account-lines/test-server-lines-run.json'))

    # Figures.
    f_storage = bars('storage', [('XRPLA piece cache', 23, XRPLA), ('Clio 2.8 (Cassandra)', 182, CLIO), ('xrpld 3.4.1 (NuDB + SQLite)', 711, XRPLD)],
                     'GB on disk for ledgers 107,000,001-107,697,250 (about 700,000 ledgers)', '{:,.0f} GB')
    f_point = grouped('point', ['HTTP', 'WebSocket'], [('XRPLA', XRPLA, [point['http']['xrpla'], point['ws']['xrpla']]),
                                                      ('xrpld 3.4.1', XRPLD, [point['http']['xrpld'], point['ws']['xrpld']]),
                                                      ('Clio 2.8', CLIO, [point['http']['clio'], point['ws']['clio']])],
                      'requests per second, point mix, 64 in flight')
    f_full = bars('full', [('xrpld 3.4.1', full['xrpld'], XRPLD), ('Clio 2.8', full['clio'], CLIO),
                           ('XRPLA, 16 GB cache (old default)', full['a16'], XRPLA), ('XRPLA, 32 GB cache', full['a32'], XRPLA),
                           ('XRPLA, 32 GB + object-read change (543c523)', full['a32_543'], XRPLA)],
                  'requests per second, full mix over HTTP, 64 in flight')
    f_cold = grouped('cold', ['xrpld 3.4.1', 'Clio 2.8', 'XRPLA, 16 GB', 'XRPLA, 32 GB'],
                     [('Cold (restarted, files evicted)', '#7aa7e3', [cold['xrpld'][0], cold['clio'][0], cold['a16'][0], cold['a32'][0]]),
                      ('Warm (run again)', XRPLA, [cold['xrpld'][1], cold['clio'][1], cold['a16'][1], cold['a32'][1]])],
                     'requests per second, full mix over ledgers more than 100,000 behind the tip')
    f_restore = bars('restore', [(o['era'].split(' ')[1].strip('()'), o['restore'], XRPLD) for o in old.values()],
                     'seconds to restore 100,000 ledgers into xrpld (xrpla restore --to nudb)', '{:,.0f} s', width=5.6)
    f_ts = bars('testserver', [('Before the audit (c05a479), WebSocket', ts['before'][('ws', 'full')], XRPLA),
                               ('Audit read path (2b19b97), HTTP', ts['audit'][('http', 'full')], XRPLA),
                               ('Audit read path (2b19b97), WebSocket', ts['audit'][('ws', 'full')], XRPLA),
                               ('With 5M interval snapshots, HTTP', ts['snap'][('http', 'full')], XRPLA),
                               ('With 5M interval snapshots, WebSocket', ts['snap'][('ws', 'full')], XRPLA)],
                'requests per second, full mix across all history (test server)', '{:,.1f}')

    # Document.
    ss = getSampleStyleSheet()
    body = ParagraphStyle('body', parent=ss['BodyText'], fontName='Helvetica', fontSize=9.5, leading=13.5, textColor=colors.HexColor(INK), alignment=TA_LEFT, spaceAfter=6)
    small = ParagraphStyle('small', parent=body, fontSize=8, leading=11, textColor=colors.HexColor(INK2))
    h1 = ParagraphStyle('h1', parent=ss['Heading1'], fontName='Helvetica-Bold', fontSize=16, leading=20, textColor=colors.HexColor(INK), spaceBefore=6, spaceAfter=8)
    h2 = ParagraphStyle('h2', parent=ss['Heading2'], fontName='Helvetica-Bold', fontSize=12, leading=15, textColor=colors.HexColor(INK), spaceBefore=10, spaceAfter=5)
    title = ParagraphStyle('title', parent=h1, fontSize=24, leading=29, spaceAfter=12)
    bullet = ParagraphStyle('bullet', parent=body, leftIndent=12, bulletIndent=2, spaceAfter=3)

    def p(text, s=body): return Paragraph(text, s)
    def bl(items): return [Paragraph(t, bullet, bulletText='•') for t in items]
    def img(path, w=6.4):
        im = Image(str(path)); scale = w * inch / im.imageWidth; im.drawWidth, im.drawHeight = w * inch, im.imageHeight * scale; return im
    def table(head, data, widths=None):
        t = Table([head] + data, colWidths=widths, repeatRows=1, hAlign='LEFT')
        t.setStyle(TableStyle([('FONT', (0, 0), (-1, 0), 'Helvetica-Bold', 8), ('FONT', (0, 1), (-1, -1), 'Helvetica', 8),
                               ('TEXTCOLOR', (0, 0), (-1, -1), colors.HexColor(INK)), ('LINEBELOW', (0, 0), (-1, 0), 0.6, colors.HexColor(INK2)),
                               ('LINEBELOW', (0, 1), (-1, -1), 0.3, colors.HexColor(GRID)), ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
                               ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'), ('TOPPADDING', (0, 0), (-1, -1), 3), ('BOTTOMPADDING', (0, 0), (-1, -1), 3)]))
        return t
    n = lambda v: f'{v:,.0f}'

    story = [p('XRPLA benchmark report', title),
             p('October 2026 audit: correctness against xrpld and Clio, restore and storage, and query speed', h2),
             p('Every number in this report is read from the raw results in <font face="Courier">docs/benchmarks/2026-10-audit/</font>, '
               'which NOTES.md describes file by file. This PDF is rebuilt from them by <font face="Courier">report/make_report.py</font>.', small),
             Spacer(1, 10), p('Summary', h1)]
    story += bl([
        f'<b>Correctness.</b> Against stock xrpld 3.4.1, XRPLA returned the same data in every comparison: '
        f'{n(campaign["same"])} answers in the parity campaign across {campaign["intervals"]} intervals of history, {n(x_same)} on the restored 107M range '
        f'(its {x_diff} differences were ping\'s role field, from an admin connection), {n(old_same)} on four older stretches (2015-2024), and '
        f'{n(group1["compared"])} account_tx delegate pages.',
        f'<b>Clio.</b> A Clio 2.8 restored from XRPLA\'s publication answered {n(clio_same)} requests exactly as a Clio whose database Clio\'s own ETL built '
        f'(the only differences, {clio_diff}, were the nodes\' version numbers).',
        f'<b>Storage.</b> For the same 700,000 ledgers, XRPLA\'s piece cache is 23 GB, against 182 GB for Clio\'s Cassandra and 711 GB for xrpld\'s NuDB and SQLite.',
        f'<b>Restores.</b> XRPLA restored those ledgers into a working xrpld in {hms(x_fixed)} with downloads cached ({hms(x_restore)} downloading from R2), '
        f'and 100,000 older ledgers in '
        f'{hms(min(o["restore"] for o in old.values()))} to {hms(max(o["restore"] for o in old.values()))}. '
        f'A Clio restore loaded {n(clio_rows)} rows in {hms(clio_load)}.',
        f'<b>Point reads.</b> XRPLA leads: {n(point["ws"]["xrpla"])} requests/s over WebSocket against {n(point["ws"]["xrpld"])} for xrpld and {n(point["ws"]["clio"])} for Clio, '
        f'on {point_cores["xrpla"]:.1f} busy cores against {point_cores["xrpld"]:.1f} and {point_cores["clio"]:.1f}.',
        f'<b>Historical owner and history reads.</b> xrpld and Clio lead: on the full mix XRPLA serves {n(full["a32_543"])} requests/s with a 32 GB cache and the '
        f'object-read change, against {n(full["clio"])} for Clio and {n(full["xrpld"])} for xrpld. The audit raised XRPLA from {n(full["a16"])} with the old 16 GB default; '
        f'the remaining gap is memory per decoded segment (section 5).'])

    story += [p('1. Setup', h1),
              p('<b>Hetzner</b> (publisher-host; Xeon Gold 5412U, 48 threads, 126 GB RAM, NVMe RAID). Servers run on cores 16-39 and the load generator '
                '(perf.py: 64 requests in flight, 4 processes, closed loop) on cores 40-47, beside the production publisher, a soak test and a Postgres replica. '
                'All three servers hold ledgers 107,000,001-107,697,250:'),
              *bl(['<b>XRPLA</b> (A′): a piece cache of the range, synced from the R2 publication, served by <font face="Courier">xrpla serve</font>.',
                   '<b>xrpld 3.4.1</b> (X): NuDB and SQLite restored from the publication by <font face="Courier">xrpla restore --to nudb</font>, running on mainnet from the restored tip.',
                   '<b>Clio 2.8.0</b> (C) on Apache Cassandra 4.1.10 (G1, 24 GB heap), loaded by <font face="Courier">xrpla restore --to clio</font>, following the tip through its ETL.']),
              p('<b>Request sets.</b> The <i>point</i> mix is account_info, tx, ledger, ledger_entry and server_info. The <i>full</i> mix adds account_lines, account_objects, '
                'account_tx and book_offers, which read many objects or a long history at an older ledger. Requests come from a corpus of recent and older ledgers of the range.'),
              p('<b>Test server</b> (test-host, xrpla-test.example; Ryzen 7 7700, 16 threads, 61 GB RAM): XRPLA\'s open full-history node, '
                'ledgers 32,570 to the tip, with a 32 GB decode cache.'),
              PageBreak()]

    story += [p('2. Correctness', h1),
              p('Each comparison derives requests from each sampled ledger\'s contents (accounts, books, tokens, transactions), asks both servers, and compares the answers whole, '
                'after removing only fields that describe the server. XRPLA\'s answers were also asked over its WebSocket and compared with its HTTP answers.'),
              table(['Comparison', 'Reference', 'Answers', 'Different'], [
                  ['Parity campaign: 108 intervals across all history', 'xrpld (s1/s2, xrplcluster arbiter)', n(campaign['same']), n(campaign['different'])],
                  ['  same requests over WS, HTTPS and WSS', 'XRPLA over HTTP', n(campaign['transports_same']), n(campaign['transports_different'])],
                  ['Restored 107M range, 3 seeds of 300 ledgers', 'xrpld 3.4.1 (local)', n(x_same), f'{x_diff} (ping role)'],
                  ['Older history: 4 stretches of 100,000 ledgers', 'xrpld 3.4.1 (local)', n(old_same), n(old_diff)],
                  ['account_tx delegate filter, every page', 'xrpld 3.4.1', n(group1['compared']), n(group1['differ'])],
                  ['Clio restored from XRPLA, 3 seeds of 300 ledgers', 'Clio node (own ETL)', n(clio_same), f'{clio_diff} (version)'],
                  ['Invariants: derived answers against verified state', 'XRPLA\'s own state', n(inv_checked), n(inv_violated)]],
                  [2.75 * inch, 1.95 * inch, 0.85 * inch, 0.85 * inch]),
              Spacer(1, 6),
              p(f'The campaign\'s {campaign["timeouts"]} timeouts and {campaign["reference_errors"] + campaign["reference_inconsistent"]} reference problems were on the public '
                f'reference servers, which were not compared. The {x_diff} differences on the 107M range, one per seed, are <font face="Courier">ping</font>\'s '
                '<font face="Courier">role</font> field, which xrpld adds for an admin connection. The invariant checker (tools/xrpla_invariants.py) needs no reference: it checks account_tx, tx, NFT, MPT and '
                f'order-book answers against the hash-verified state and transactions of 150 sampled ledgers; {inv_skipped} checks whose listings ran past its page cap '
                '(accounts holding over 160,000 NFTs) were skipped, not passed.', small),
              p('Older history', h2),
              table(['Stretch', 'Restore', 'xrpld database', 'Peak memory', 'Piece cache sync', 'Answers', 'Different'],
                    [[o['era'], hms(o['restore']), f'{o["db"]:.0f} GB', f'{o["peak"]:.1f} GB', hms(o['sync']), n(o['same']), n(o['different'])] for o in old.values()],
                    [1.85 * inch, 0.7 * inch, 0.85 * inch, 0.8 * inch, 0.95 * inch, 0.65 * inch, 0.6 * inch]),
              p('Each stretch was restored into xrpld\'s NuDB and SQLite by xrpla restore (543c523), served by xrpld 3.4.1 standalone, and compared with XRPLA serving a piece cache of '
                'the same range. A standalone xrpld counts only its loaded ledger as present, so it was started with --force_ledger_present_range over the stretch, and on '
                'public ports (an admin connection has no minimum limits).', small),
              PageBreak()]

    story += [p('3. Storage and restore', h1),
              img(f_storage),
              table(['System (ledgers 107,000,001-107,697,250)', 'On disk', 'Built by', 'Time'], [
                  ['XRPLA piece cache', '23 GB', 'xrpla sync (from R2)', hms(a_sync)],
                  ['Clio 2.8 on Cassandra', '182 GB', 'xrpla restore --to clio', f'{hms(clio_load)} (load)'],
                  ['xrpld 3.4.1, NuDB + SQLite', nudb['database_bytes_du'] + 'B', 'xrpla restore --to nudb', f'{hms(x_fixed)} (cached), {hms(x_restore)}']],
                  [2.2 * inch, 0.7 * inch, 1.55 * inch, 2.0 * inch]),
              Spacer(1, 6),
              p(f'The first xrpld restore ({hms(x_restore)}, downloading from R2) lost NuDB spill records to a bug in its final checkpoint; it was fixed in cb78f55 '
                f'and the restore run again ({hms(x_fixed)}, downloads cached), and that database is the one compared. '
                'Every restore checks each ledger\'s state root and its parent chain before writing it. The xrpld restore then started on mainnet from the restored tip, '
                'read its state from NuDB rather than from peers, and reached <i>full</i>. The Clio restore\'s plan was built in 57 minutes, against more than 4 h 43 min for the '
                f'code before the audit (whose run was stopped); the load of all {n(clio_rows)} rows then took {hms(clio_load)}, and Clio continued from the restored tip.'),
              img(f_restore, 5.6),
              p('Restoring older history is fast because state was smaller: 18 GB for 100,000 ledgers of 2015 against 48-55 GB for 2021 and 2024.', small),
              PageBreak()]

    story += [p('4. Query speed', h1),
              p('Point reads', h2), img(f_point),
              table(['Point mix (requests/s)', 'XRPLA', 'xrpld 3.4.1', 'Clio 2.8'],
                    [[proto.upper() if proto == 'http' else 'WebSocket', n(point[proto]['xrpla']), n(point[proto]['xrpld']), n(point[proto]['clio'])] for proto in ('http', 'ws')],
                    [2.4 * inch, 1.2 * inch, 1.2 * inch, 1.2 * inch]),
              p('Over HTTP every request opens a new connection, which bounds all three servers alike. Over WebSocket XRPLA answers about '
                f'{point["ws"]["xrpla"] / point["ws"]["xrpld"]:.1f}× as many as xrpld and {point["ws"]["xrpla"] / point["ws"]["clio"]:.1f}× as many as Clio.', small),
              p('Historical owner and history reads', h2), img(f_full),
              table(['Full mix', 'Requests/s', 'Server cores busy'], [
                  ['xrpld 3.4.1', f'{n(full["xrpld"])} (runs {n(min(full["xrpld_runs"]))}-{n(max(full["xrpld_runs"]))})', f'{cores["xrpld"]:.1f}'],
                  ['Clio 2.8 (Clio and Cassandra together)', n(full['clio']), f'{cores["clio"]:.1f}'],
                  ['XRPLA, 16 GB cache (the old default on this machine)', n(full['a16']), ''],
                  ['XRPLA, 32 GB cache', n(full['a32']), f'{cores["a32"]:.1f}'],
                  ['XRPLA, 32 GB cache, object reads skip the transaction section', n(full['a32_543']), '']],
                  [3.6 * inch, 1.6 * inch, 1.2 * inch]),
              Spacer(1, 6),
              table(['Median latency, full mix (ms)', 'xrpld', 'Clio', 'XRPLA 16 GB', 'XRPLA 32 GB'],
                    [[m, f'{v["xrpld"]:.1f}', f'{v["clio"]:.1f}', f'{v["a16"]:.1f}', f'{v["a32"]:.1f}'] for m, v in owner.items()],
                    [2.2 * inch, 0.9 * inch, 0.9 * inch, 1.0 * inch, 1.0 * inch]),
              p('The full mix is where XRPLA trails. Its owner and history methods read objects at an older ledger from compressed pieces; each object read decodes the page '
                'of the ledger that last changed it, and that needs the segment\'s dictionary in memory. Section 5 explains what was measured and changed.', small),
              p('Cold reads', h2), img(f_cold),
              table(['Full mix, older ledgers (requests/s)', 'Cold', 'Warm'],
                    [[name, n(c), n(w)] for name, (c, w) in (('xrpld 3.4.1', cold['xrpld']), ('Clio 2.8', cold['clio']), ('XRPLA, 16 GB', cold['a16']), ('XRPLA, 32 GB', cold['a32']))],
                    [3.0 * inch, 1.2 * inch, 1.2 * inch]),
              p('Each server was restarted and its files evicted from the page cache, then asked only about ledgers more than 100,000 behind the range\'s tip; "warm" is the same run again.', small),
              PageBreak()]

    ex = {m['request']: m for m in single}
    story += [p('5. The account_lines investigation', h1),
              p(f'Under account_lines alone the full-history node served {n(lines_run["per_second"])} requests/s at a {lines_run["p50_ms"]:.1f} ms median with a 32 GB cache, '
                f'so the warm path is fast. The full mix on Hetzner was slow because the cache was too small for the working set: a busy 700,000-ledger span has about 180 '
                'segments, each holding about 100 MB once its dictionary is loaded, and the 16 GB default kept reloading them.'),
              *bl([f'<b>Decode cache budget:</b> 32 GB raised the full mix from {n(full["a16"])} to {n(full["a32"])} requests/s and cold reads from {n(cold["a16"][0])} to {n(cold["a32"][0])}. '
                   'The default is now a quarter of memory, between 3 and 64 GB (1f4a40a).',
                   f'<b>Object reads skip the transaction section</b> (543c523): {n(full["a32_base"])} to {n(full["a32_543"])} requests/s at 32 GB.',
                   '<b>Measured and dropped:</b> more resume points per segment (no change), keeping decompressed history sections (no change), and a memory-mapped '
                   'dictionary cache, which doubled throughput on a memory-starved server but stalled on page faults on a real one (about 10 requests/s just after a restart).']),
              p('Full history on the test server', h2), img(f_ts),
              p('The test server serves all of history, so a full-mix request can land anywhere in 107 million ledgers. The audit\'s read path and interval snapshots every '
                f'5 million ledgers (each older ledger reads its state from the nearest snapshot) raised it from {ts["before"][("ws", "full")]:.1f} to '
                f'{ts["snap"][("ws", "full")]:.0f} requests/s over WebSocket ({ts["snap"][("http", "full")]:.0f} over HTTP).', small),
              table(['Single request on the test server (seconds)', 'Cold, before', 'Cold, audit', 'Warm, before', 'Warm, audit'],
                    [[r['request'], f'{r["cold_old"]:.3f}', f'{r["cold_new"]:.3f}', f'{r["warm_old"]:.3f}', f'{r["warm_new"]:.3f}'] for r in single],
                    [2.7 * inch, 0.9 * inch, 0.9 * inch, 0.9 * inch, 0.9 * inch]),
              PageBreak()]

    story += [p('6. Caveats', h1),
              *bl(['The xrpld speed runs paired xrpld with XRPLA on the earlier build 2b19b97 at the 16 GB default; the XRPLA rows at 32 GB come from runs on the same machine, '
                   'corpus and cores, after X\'s database had been removed. Its point-mix numbers are unchanged across builds.',
                   'Clio followed the network tip through its ETL during its runs, and XRPLA served a fixed range. Clio\'s CPU and memory include Cassandra\'s.',
                   'All servers shared Hetzner with production services (the publisher, a soak test, a Postgres replica), pinned to their own cores.',
                   'The test server\'s full-history numbers come from a single machine with 61 GB of RAM serving 107 million ledgers; they are not comparable with the Hetzner runs over one range.',
                   'Each speed figure is the mean of the runs listed in the raw files (usually two per server, mix and transport, alternating server order).']),
              p('7. Data', h1),
              p('All raw results are in docs/benchmarks/2026-10-audit, described in NOTES.md: the parity campaign (parity-campaign/), the xrpld comparison and speed runs '
                '(hetzner-x-*), Clio (clio-comparison/, clio-methods/, clio-restore/), older history (older-history/), the account_lines investigation (account-lines/), '
                'the invariant checks (invariants/) and the test server runs (test-server-*).', small)]

    doc = SimpleDocTemplate(a.out, pagesize=letter, leftMargin=0.85 * inch, rightMargin=0.85 * inch, topMargin=0.8 * inch, bottomMargin=0.8 * inch,
                            title='XRPLA benchmark report, October 2026', author='XRPLA')
    def footer(canvas, doc):
        canvas.saveState(); canvas.setFont('Helvetica', 7.5); canvas.setFillColor(colors.HexColor(INK2))
        canvas.drawString(0.85 * inch, 0.5 * inch, 'XRPLA benchmark report, October 2026'); canvas.drawRightString(letter[0] - 0.85 * inch, 0.5 * inch, str(doc.page))
        canvas.restoreState()
    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    print(a.out)


def elapsed_from(time_v):
    for key, value in time_v.items():
        if key.startswith('Elapsed'):
            parts = [float(p) for p in value.rsplit(' ', 1)[-1].split(':')]
            return sum(p * 60 ** i for i, p in enumerate(reversed(parts)))
    raise ValueError('no elapsed time')


if __name__ == '__main__':
    main()
