# Query benchmarks: piece cache, round 5, 2026-10-07

These results come from the piece cache after round 5, described in `docs/internal/QUERY-BENCHMARKS.md` ("Piece cache, round 5: segment opens, resume points, WebSocket"):
- leaner loaded dictionaries;
- dictionary decompression and checksum overlapping the parse;
- up to 4 decode resume points per open segment;
- `book_offers` price levels from one key walk;
- TCP_NODELAY on accepted connections.

The run was `scripts/pieces-bench5.sh`, on the same machine, data and requests as `../pieces4/`. The load average was 11–21 from the backfills and the audit.

The build is the commit that adds these files; its binary's sha256 prefix is `b411f43813a392db`. It runs at two budgets:
- **auto**, the default of an eighth of memory: 15,973 MB on this 125 GB machine;
- **3gb**, an explicit `--cache-mb 3072`.

The files are as in `../pieces4/`. In `cache-stats-{auto,3gb}.json`:
- `dictionaries.parse_ms` is now each load's whole wall time, since decompression and parse overlap;
- `read_ms` is the decompression thread's time;
- `mean_bytes` is the memory charged per open segment: its dictionary plus 8 MB for transaction pages and cursors (4 MB before).

Every xrpld comparison matched: 3,509 matching answers. The 933 differences are the standalone-xrpld artifacts.
