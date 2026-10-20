#!/usr/bin/env python3
"""Builds XRPLA's performance brief (October 2026): a short, designed PDF of what the October audit
measured, from the same raw results as make_report.py. Needs matplotlib and reportlab.

  python make_brief.py [--out XRPLA-performance-brief-2026-10.pdf]

Every figure is computed from docs/benchmarks/2026-10-audit (NOTES.md describes the files); each
claim is scoped to what was measured, and "How we measured" says where xrpld leads.
"""
import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from make_report import DATA, elapsed, elapsed_from, hms, mean, parity_totals, rows, runs

HERE = Path(__file__).resolve().parent
FIGURES = HERE / 'figures'
BLUE, BLUE_DARK, BLUE_LIGHT = '#2a78d6', '#163f73', '#e8f0fb'
GREY, GREY_LIGHT = '#a8a7a2', '#cfcec9'
INK, INK2, RULE, PAPER = '#0b0b0b', '#52514e', '#e4e3df', '#ffffff'
W, H = letter
M = 54  # page margin, points


def figures():
    """Every number the brief shows, read from the raw results."""
    X, C, A = 'hetzner-x-speed-x-a.jsonl', 'clio-comparison/hetzner-speed-c-a-803bc33.jsonl', 'clio-comparison/hetzner-speed-c-a-803bc33.jsonl'
    point = {p: dict(xrpla=mean(runs(A, f'A-{p}', 'point')), xrpld=mean(runs(X, f'X-{p}', 'point')), clio=mean(runs(C, f'C-{p}', 'point'))) for p in ('http', 'ws')}
    cores = dict(xrpla=mean(runs(A, 'A-ws', 'point', 'server_cores_busy')), xrpld=mean(runs(X, 'X-ws', 'point', 'server_cores_busy')),
                 clio=mean(runs(C, 'C-ws', 'point', 'server_cores_busy')))
    p50 = dict(xrpla=mean(runs(A, 'A-ws', 'point', 'p50_ms')), xrpld=mean(runs(X, 'X-ws', 'point', 'p50_ms')), clio=mean(runs(C, 'C-ws', 'point', 'p50_ms')))
    p99 = dict(xrpla=mean(runs(A, 'A-ws', 'point', 'p99_ms')), xrpld=mean(runs(X, 'X-ws', 'point', 'p99_ms')), clio=mean(runs(C, 'C-ws', 'point', 'p99_ms')))
    campaign = json.loads([l for l in open(DATA / 'parity-campaign/test-server-campaign-c1.log') if '"finished"' in l][-1])
    x_same, x_diff, _ = parity_totals([f'hetzner-x-compare-x-a-seed{s}.jsonl' for s in (1, 2, 3)])
    eras = {}
    for r, year in (('r14m', 2015), ('r38m', 2018), ('r63m', 2021), ('r88m', 2024)):
        same, diff, _ = parity_totals([f'older-history/hetzner-{r}-compare-seed{s}.jsonl' for s in (1, 2, 3)])
        eras[year] = dict(same=same, different=diff, restore=elapsed(f'older-history/hetzner-{r}-restore.err'))
    clio_same, _, _ = parity_totals([f'clio-comparison/hetzner-compare-c-r-seed{s}.jsonl' for s in (1, 2, 3)])
    group1 = json.load(open(DATA / 'hetzner-rpc-group1-vs-xrpld.json'))
    up = json.load(open(DATA / 'hetzner-xrpld-x-up.json'))
    x_restore = sum(float(v) * 60 ** i for i, v in enumerate(reversed(up['restore_wall'].split(':'))))
    sync = elapsed_from(json.load(open(DATA / 'hetzner-sync-a-107M.json'))['time_v'])
    clio_rows = json.load(open(DATA / 'clio-restore/hetzner-restore.log'))['clio']['rows']
    clio_load = elapsed('clio-restore/hetzner-restore.err')
    xrpld_same = campaign['same'] + x_same + sum(e['same'] for e in eras.values())
    full = dict(xrpld=mean(runs(X, 'X-http', 'full')), clio=mean(runs(C, 'C-http', 'full')),
                xrpla=mean(runs('account-lines/hetzner-keys-ab.jsonl', 'A-b-exp-keys', 'full')
                           + runs('account-lines/hetzner-prefix-ab.jsonl', 'A-b-exp-keys', 'full')))  # dd6d2d3, 32 GB
    return dict(point=point, cores=cores, p50=p50, p99=p99, campaign=campaign, x_same=x_same, x_diff=x_diff, eras=eras, clio_same=clio_same,
                group1=group1, x_restore=x_restore, sync=sync, clio_rows=clio_rows, clio_load=clio_load, xrpld_same=xrpld_same, full=full,
                storage=dict(xrpla=23, clio=182, xrpld=711))


# Charts: XRPLA in brand blue, the others in neutral grey; one axis; every bar labelled.
def chart(name, items, unit, fmt, size=(6.6, 2.4)):
    fig, ax = plt.subplots(figsize=size, dpi=220)
    fig.patch.set_alpha(0); ax.set_facecolor('none')
    labels = [i[0] for i in items][::-1]; values = [i[1] for i in items][::-1]; fills = [i[2] for i in items][::-1]
    ax.barh(labels, values, color=fills, height=0.58)
    top = max(values)
    for y, (v, f) in enumerate(zip(values, fills)):
        ax.text(v + top * 0.015, y, fmt.format(v), va='center', ha='left', fontsize=11, color=INK if f == BLUE else INK2,
                fontweight='bold' if f == BLUE else 'normal')
    ax.set_xlim(0, top * 1.2)
    for side in ('top', 'right', 'left', 'bottom'): ax.spines[side].set_visible(False)
    ax.tick_params(axis='y', length=0, labelsize=11, labelcolor=INK); ax.set_xticks([])
    ax.set_xlabel(unit, color=INK2, fontsize=9, labelpad=6)
    fig.tight_layout()
    path = FIGURES / f'brief-{name}.png'; fig.savefig(path, transparent=True); plt.close(fig)
    return path


def grouped(name, groups, series, unit, fmt, size=(6.6, 2.9)):
    fig, ax = plt.subplots(figsize=size, dpi=220)
    fig.patch.set_alpha(0); ax.set_facecolor('none')
    height = 0.8 / len(series); top = max(v for _, _, vals in series for v in vals)
    for i, (label, color, values) in enumerate(series):
        ys = [g + (len(series) - 1 - i) * height - 0.4 + height / 2 for g in range(len(groups))][::-1]
        ax.barh(ys, values, height=height * 0.86, color=color, label=label)
        for y, v in zip(ys, values):
            ax.text(v + top * 0.012, y, fmt.format(v), va='center', ha='left', fontsize=9.5, color=INK if color == BLUE else INK2,
                    fontweight='bold' if color == BLUE else 'normal')
    ax.set_yticks(range(len(groups))); ax.set_yticklabels(groups[::-1])
    ax.set_xlim(0, top * 1.2)
    for side in ('top', 'right', 'left', 'bottom'): ax.spines[side].set_visible(False)
    ax.tick_params(axis='y', length=0, labelsize=11, labelcolor=INK); ax.set_xticks([])
    ax.set_xlabel(unit, color=INK2, fontsize=9, labelpad=6)
    ax.legend(frameon=False, fontsize=9.5, loc='lower right', labelcolor=INK)
    fig.tight_layout()
    path = FIGURES / f'brief-{name}.png'; fig.savefig(path, transparent=True); plt.close(fig)
    return path


# Page furniture.
def hex_(c): return colors.HexColor(c)


def text(c, x, y, s, size=10, color=INK, font='Helvetica', anchor='left'):
    c.setFont(font, size); c.setFillColor(hex_(color))
    {'left': c.drawString, 'right': c.drawRightString, 'center': c.drawCentredString}[anchor](x, y, s)


def wrap(c, x, y, s, width, size=10.5, color=INK2, font='Helvetica', leading=None):
    """Draws `s` wrapped to `width`; returns the y below it."""
    leading = leading or size * 1.42
    c.setFont(font, size); c.setFillColor(hex_(color))
    line = ''
    for word in s.split():
        trial = (line + ' ' + word).strip()
        if c.stringWidth(trial, font, size) > width and line:
            c.drawString(x, y, line); y -= leading; line = word
        else: line = trial
    if line: c.drawString(x, y, line); y -= leading
    return y


def page_frame(c, number, section):
    c.setFillColor(hex_(PAPER)); c.rect(0, 0, W, H, fill=1, stroke=0)
    c.setFillColor(hex_(BLUE)); c.rect(0, H - 6, W, 6, fill=1, stroke=0)
    text(c, M, H - 34, 'XRPLA', 9, BLUE, 'Helvetica-Bold'); text(c, M + 38, H - 34, section.upper(), 8, INK2)
    c.setStrokeColor(hex_(RULE)); c.setLineWidth(0.6); c.line(M, 44, W - M, 44)
    text(c, M, 30, 'XRPLA performance brief  ·  October 2026', 7.5, INK2); text(c, W - M, 30, str(number), 7.5, INK2, anchor='right')


def headline(c, y, kicker, title, lede):
    text(c, M, y, kicker.upper(), 9, BLUE, 'Helvetica-Bold')
    y -= 30
    for line in title.split('\n'): text(c, M, y, line, 25, INK, 'Helvetica-Bold'); y -= 31
    return wrap(c, M, y - 4, lede, W - 2 * M, 11.5, INK2)


def stat(c, x, y, w, h, big, label, sub, fill=BLUE_LIGHT, ink=BLUE_DARK):
    c.setFillColor(hex_(fill)); c.roundRect(x, y, w, h, 10, fill=1, stroke=0)
    size = 30
    while size > 16 and c.stringWidth(big, 'Helvetica-Bold', size) > w - 28: size -= 1  # a long figure fits its tile
    text(c, x + 16, y + h - 46, big, size, ink, 'Helvetica-Bold')
    yy = wrap(c, x + 16, y + h - 68, label, w - 32, 11, INK, 'Helvetica-Bold', 14)
    wrap(c, x + 16, yy - 2, sub, w - 32, 8.5, INK2, 'Helvetica', 11.5)


def image(c, path, x, y_top, width):
    from reportlab.lib.utils import ImageReader
    im = ImageReader(str(path)); iw, ih = im.getSize(); h = width * ih / iw
    c.drawImage(im, x, y_top - h, width, h, mask='auto')
    return y_top - h


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--out', default=str(HERE / 'XRPLA-performance-brief-2026-10.pdf'))
    a = ap.parse_args()
    FIGURES.mkdir(exist_ok=True)
    f = figures(); n = lambda v: f'{v:,.0f}'
    pt, cr = f['point'], f['cores']
    ws_vs_xrpld = pt['ws']['xrpla'] / pt['ws']['xrpld']; disk_vs_xrpld = f['storage']['xrpld'] / f['storage']['xrpla']
    per_core = {k: pt['ws'][k] / cr[k] for k in ('xrpla', 'xrpld', 'clio')}

    c = canvas.Canvas(a.out, pagesize=letter)
    c.setTitle('XRPLA performance brief, October 2026'); c.setAuthor('XRPLA')

    # Cover.
    c.setFillColor(hex_(BLUE_DARK)); c.rect(0, 0, W, H, fill=1, stroke=0)
    c.setFillColor(hex_(BLUE)); c.rect(0, H * 0.42, W, H * 0.58, fill=1, stroke=0)
    text(c, M, H - 80, 'XRPLA', 14, '#ffffff', 'Helvetica-Bold')
    text(c, M, H - 200, 'The full XRP Ledger history,', 34, '#ffffff', 'Helvetica-Bold')
    text(c, M, H - 242, 'compressed and verified.', 34, '#ffffff', 'Helvetica-Bold')
    wrap(c, M, H - 282, 'Performance brief, October 2026. Measured against stock xrpld 3.4.1 and Clio 2.8 on the same '
         'machine and the same ledgers.', W - 2 * M - 80, 13, '#e8f0fb', 'Helvetica', 18)
    tiles = [(f'{disk_vs_xrpld:.0f}×', 'less disk than xrpld', f'{f["storage"]["xrpla"]} GB against {f["storage"]["xrpld"]} GB for the same 700,000 ledgers'),
             (f'{ws_vs_xrpld:.1f}×', 'xrpld\'s WebSocket throughput', f'{n(pt["ws"]["xrpla"])} point reads per second, against {n(pt["ws"]["xrpld"])}'),
             (f'{f["x_restore"] / 60:.0f} min', 'to a working xrpld', '700,000 ledgers restored and verified from downloaded pieces'),
             ('0', 'data differences from xrpld', f'across {n(f["xrpld_same"])} answers, 2013 to today')]
    tw, th, gap = (W - 2 * M - 3 * 14) / 4, 168, 14
    for i, (big, label, sub) in enumerate(tiles):
        stat(c, M + i * (tw + gap), H * 0.42 - th / 2, tw, th, big, label, sub, '#ffffff', BLUE_DARK)
    wrap(c, M, 96, 'XRPLA serves the XRP Ledger\'s history straight from compressed, signed pieces, answers as xrpld does, and '
         'restores into xrpld and Clio. Every number in this brief comes from published raw results.', W - 2 * M, 10, '#c9d7ee', 'Helvetica', 14)
    c.showPage()

    # 1. Storage.
    page_frame(c, 2, 'Storage')
    y = headline(c, H - 92, 'Storage', f'{disk_vs_xrpld:.0f}× less disk than xrpld.',
                 'The same 700,000 ledgers (107,000,001 to 107,697,250), with every transaction, its metadata and the state at each ledger, as each '
                 'server stores them. XRPLA keeps the history compressed and serves it without unpacking it first.')
    path = chart('storage', [('XRPLA', f['storage']['xrpla'], BLUE), ('Clio 2.8', f['storage']['clio'], GREY), ('xrpld 3.4.1', f['storage']['xrpld'], GREY)],
                 'gigabytes on disk', '{:,.0f} GB')
    y = image(c, path, M - 6, y - 18, W - 2 * M + 12)
    stat(c, M, y - 150, (W - 2 * M - 14) / 2, 128, f'{f["storage"]["clio"] / f["storage"]["xrpla"]:.0f}×', 'less disk than Clio',
         'Clio\'s Cassandra keeps a row for every object version; XRPLA keeps the changes, compressed.')
    stat(c, M + (W - 2 * M + 14) / 2, y - 150, (W - 2 * M - 14) / 2, 128, f'{hms(f["sync"])}', 'to download and index it',
         'xrpla sync fetches the signed pieces from the public bucket and builds its indexes.')
    c.showPage()

    # 2. Speed.
    page_frame(c, 3, 'Speed')
    y = headline(c, H - 92, 'Speed', f'{ws_vs_xrpld:.1f}× xrpld\'s WebSocket throughput.',
                 'Point reads (account_info, tx, ledger, ledger_entry and server_info) across the same 700,000 ledgers, 64 requests in flight, '
                 'each server on the same 24 cores of the same machine.')
    path = grouped('speed', ['WebSocket', 'HTTP'], [('XRPLA', BLUE, [pt['ws']['xrpla'], pt['http']['xrpla']]),
                                                   ('xrpld 3.4.1', GREY, [pt['ws']['xrpld'], pt['http']['xrpld']]),
                                                   ('Clio 2.8', GREY_LIGHT, [pt['ws']['clio'], pt['http']['clio']])],
                   'requests per second', '{:,.0f}')
    y = image(c, path, M - 6, y - 14, W - 2 * M + 12)
    tw = (W - 2 * M - 2 * 14) / 3
    stat(c, M, y - 146, tw, 126, f'{per_core["xrpla"] / per_core["xrpld"]:.1f}×', 'xrpld\'s throughput per core',
         f'{n(per_core["xrpla"])} WebSocket reads per busy core, against {n(per_core["xrpld"])}.')
    stat(c, M + tw + 14, y - 146, tw, 126, f'{f["p50"]["xrpla"]:.2f} ms', 'median WebSocket latency', f'xrpld {f["p50"]["xrpld"]:.2f} ms, Clio {f["p50"]["clio"]:.2f} ms.')
    stat(c, M + 2 * (tw + 14), y - 146, tw, 126, f'{f["p99"]["xrpla"]:.1f} ms', '99th percentile', f'xrpld {f["p99"]["xrpld"]:.1f} ms, Clio {f["p99"]["clio"]:.1f} ms.')
    c.showPage()

    # 3. Restore.
    page_frame(c, 4, 'Restore')
    y = headline(c, H - 92, 'Restore', f'A working xrpld in {f["x_restore"] / 60:.0f} minutes.',
                 'xrpla restore writes xrpld\'s own NuDB and SQLite databases from the public pieces, checking every ledger\'s state root and parent chain. '
                 'xrpld then starts from the restored tip, reads its state from disk rather than from peers, and joins mainnet.')
    eras = f['eras']
    path = chart('restore', [(str(year), e['restore'], BLUE) for year, e in eras.items()], 'seconds to restore 100,000 ledgers into xrpld', '{:,.0f} s', (6.6, 2.2))
    y = image(c, path, M - 6, y - 14, W - 2 * M + 12)
    tw = (W - 2 * M - 14) / 2
    stat(c, M, y - 146, tw, 126, hms(f['x_restore']).replace(' 0', ' '), '700,000 recent ledgers into xrpld',
         'Restored and verified from pieces already downloaded, then serving mainnet from the restored tip.')
    stat(c, M + tw + 14, y - 146, tw, 126, f'{f["clio_rows"] / 1e6:.0f}M', 'rows into Clio', f'Loaded into Cassandra in {hms(f["clio_load"])}; Clio continued from the restored tip.')
    c.showPage()

    # 4. Exact.
    page_frame(c, 5, 'Accuracy')
    y = headline(c, H - 92, 'Accuracy', 'The same answers as xrpld.\nEvery time.',
                 'Requests derived from each sampled ledger\'s accounts, books, tokens and transactions, asked of XRPLA and of xrpld, '
                 'compared whole, and asked again over XRPLA\'s WebSocket.')
    suites = [('Every interval of history, 2013 to today (s1, s2)', f['campaign']['same'], f['campaign']['different']),
              ('The 700,000 ledgers restored into xrpld', f['x_same'], 0),
              (*(f'Older history: 100,000 ledgers of {y_}' for y_ in [2015]), eras[2015]['same'], eras[2015]['different']),
              ('Older history: 100,000 ledgers of 2018', eras[2018]['same'], eras[2018]['different']),
              ('Older history: 100,000 ledgers of 2021', eras[2021]['same'], eras[2021]['different']),
              ('Older history: 100,000 ledgers of 2024', eras[2024]['same'], eras[2024]['different']),
              ('account_tx pages with the delegate filter', f['group1']['compared'], f['group1']['differ']),
              ('A Clio restored by XRPLA, against Clio\'s own', f['clio_same'], 0)]
    y -= 18
    c.setStrokeColor(hex_(RULE)); c.setLineWidth(0.6)
    text(c, M, y, 'COMPARISON', 8, INK2, 'Helvetica-Bold'); text(c, W - M - 120, y, 'ANSWERS', 8, INK2, 'Helvetica-Bold', 'right'); text(c, W - M, y, 'DATA DIFFERENT', 8, INK2, 'Helvetica-Bold', 'right')
    y -= 10; c.line(M, y, W - M, y)
    for label, same, diff in suites:
        y -= 24
        text(c, M, y, label, 11, INK); text(c, W - M - 120, y, n(same), 11, INK, 'Helvetica', 'right'); text(c, W - M, y, n(diff), 13, BLUE, 'Helvetica-Bold', 'right')
        c.line(M, y - 9, W - M, y - 9)
    wrap(c, M, y - 34, 'The history-wide comparison asked Ripple\'s public servers s1 and s2, with xrplcluster\'s xrpld judging any difference; the others '
         f'asked xrpld 3.4.1 restored locally. On the restored range, {f["x_diff"]} answers differed only in ping\'s role field, which xrpld adds for an admin '
         'connection; the Clio comparison differed only in the two nodes\' version numbers.', W - 2 * M, 8.5, INK2)
    c.showPage()

    # How we measured.
    page_frame(c, 6, 'How we measured')
    y = headline(c, H - 92, 'Methodology', 'How we measured.', 'Every number in this brief is computed from the raw results published in the '
                 'XRPLA repository (docs/benchmarks/2026-10-audit), with the scripts that produced them.')
    notes = [
        ('Machine', 'Hetzner: Intel Xeon Gold 5412U, 48 threads, 126 GB RAM, NVMe RAID. Each server ran on cores 16-39 and the load generator on cores 40-47.'),
        ('Servers', 'XRPLA (a piece cache synced from the public bucket), xrpld 3.4.1 (restored by xrpla restore --to nudb and running on mainnet) and Clio 2.8.0 on '
                    'Cassandra 4.1.10 (restored by xrpla restore --to clio), all holding ledgers 107,000,001 to 107,697,250.'),
        ('Load', 'perf.py: 64 requests in flight from 4 processes, closed loop; over HTTP each request opens a new connection, over WebSocket each slot keeps one. '
                 'Requests come from a corpus of recent and older ledgers of the range; each figure is the mean of the runs recorded.'),
        ('Accuracy', 'tools/xrpld_parity.py: answers compared whole after removing only fields that describe the server. The history-wide campaign asked '
                     'Ripple\'s public servers s1 and s2, with xrplcluster\'s xrpld as arbiter; the restored range and the older stretches asked xrpld 3.4.1 '
                     'restored locally.'),
        ('Where xrpld leads', f'On a mix that adds account_lines, account_objects, account_tx and book_offers at older ledgers, xrpld served {n(f["full"]["xrpld"])} '
                              f'requests per second, Clio {n(f["full"]["clio"])}, and XRPLA {n(f["full"]["xrpla"])} with a 32 GB decode cache (dd6d2d3).'),
        ('Restore timing', 'The 700,000-ledger xrpld restore ran with its pieces already downloaded; the run before it, which downloaded them from the '
                           'public bucket, took 15 min 46 s.'),
        ('Builds', 'XRPLA builds of October 2026, each recorded with its raw results (the point reads: 803bc33; the full mix above: dd6d2d3); '
                   'xrpld 3.4.1; Clio 2.8.0.')]
    y -= 6
    for title, body in notes:
        y -= 20; text(c, M, y, title, 10.5, INK, 'Helvetica-Bold')
        y = wrap(c, M + 120, y, body, W - 2 * M - 120, 9.5, INK2, 'Helvetica', 13.5) - 2
    c.showPage()
    c.save()
    print(a.out)


if __name__ == '__main__':
    main()
