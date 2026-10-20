# Query benchmarks: piece cache, round 4 without resuming, 2026-10-06

A first version of round 4 that was replaced; see `docs/internal/QUERY-BENCHMARKS.md` ("Piece cache, round 4: decoding one ledger"). In this version:
- each one-ledger decode parsed its page from the start;
- batched loads decoded the whole page when they needed 3 or more of its ledgers;
- the cache held whole pages as well as single ledgers.

The final version resumes the parse instead, and was faster in most runs with about a third of the decoded memory.

The run was `scripts/pieces-bench4.sh`, with the same machine, data and requests as `../pieces4/`. The build was not committed; its binary's sha256 prefix is `c78f402b90c6e363`. The files are as in `../pieces4/`. In `cache-stats-{auto,3gb}.json`, `pages` counts whole pages and single ledgers together; the two files are identical apart from the shares, since the requests are the same and nothing was evicted.
