#pragma once
#include <xrpla/core/format.hpp>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

// Builds indexes of existing SQLite tables outside SQLite: the tables' leaf pages are read
// straight from the file, once for every index, on several threads (a table SQLite has to
// read for us, with a key column in overflow pages say, is read over rowid ranges by
// read-only connections instead); the entries are sorted as SQLite orders them, and every
// index B-tree is written at once, from its leaves up, into pages appended to the file.
// SQLite's CREATE INDEX reads the table for each index and inserts on one thread: on a
// restore of 200,000 recent mainnet ledgers, three of the five indexes took it 210 s of
// about 460.
namespace xrpla::core::sqlite_index {
struct Spec {
    std::string name;                  // as sqlite_master records it
    std::string table;
    std::vector<std::string> columns;  // key columns in order (values INTEGER, TEXT or NULL)
    bool unique = false;               // a repeated key (without NULLs) fails the build
};
struct Built {
    std::map<std::string, std::uint32_t> roots;    // each index's root page
    std::map<std::string, double> seconds;         // "read" (every entry, read and sorted; "read.*" its parts), "write" (all
                                                   // indexes, at once), and per index: its merging and writing
    std::string fallback;                          // why the file was read through SQLite instead ("": read directly)
};
// Appends the indexes' pages to the database at `path` (UTF-8, no auto-vacuum, and no
// connection writing it) and records the new size in its header; the caller then records the
// indexes in sqlite_master (a writable_schema edit). Sorted runs spill under `scratch`. An
// interrupted build is undone by the next one before it starts, and a failed one (an entry
// larger than a page's cell holds without overflow pages, say) before it throws.
Built build(fs::path const& path, std::vector<Spec> const& specs, fs::path const& scratch, unsigned threads);
// Called once the indexes are recorded: the next build no longer undoes these pages.
void built(fs::path const& path);
// Undoes an interrupted build's pages if nothing has written to the file since (build does
// this first); a writer that reopens the file before building calls it before writing.
void undo_interrupted(fs::path const& path);
}
