# Query benchmarks: piece cache, round 4 with page promotion, 2026-10-06

A variant of round 4's first version (`../pieces4-alone/`) that was tried and dropped; see `docs/internal/QUERY-BENCHMARKS.md` ("Piece cache, round 4: decoding one ledger"). On the third ledger of a page decoded alone, it decoded and cached the whole page instead (through the page's end, for patches). That way, requests arriving one at a time would share the page, as batched loads do.

It was the same on the 2014 requests. On 2025 data every NFT run took longer, because whole 2025 pages pushed each other out of a 3 GB cache.

The run was `scripts/pieces-bench4.sh`, with the same machine, data and requests as `../pieces4-alone/`, just after it. The build was not committed; its binary's sha256 prefix is `aac0974e3944a9fa`. The files are as in `../pieces4/`.
