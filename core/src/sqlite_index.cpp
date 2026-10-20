// Indexes of an existing SQLite table, built outside SQLite (sqlite_index.hpp). The formats
// are SQLite's own (https://www.sqlite.org/fileformat2.html): records, varints, and index
// B-tree pages (leaf 0x0A, interior 0x02). Pages are only ever appended, and the header's
// page count is written last, so SQLite never allocates over them.
#include "sqlite_index.hpp"
#include <sqlite3.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <mutex>
#include <optional>
#include <queue>
#include <sstream>
#include <thread>
#include <fcntl.h>
#include <unistd.h>

namespace xrpla::core::sqlite_index {
namespace {
using U32 = std::uint32_t;
using U64 = std::uint64_t;

std::size_t put_varint(unsigned char* p, U64 v) {
    if (v & (U64(0xff000000) << 32)) {
        p[8] = static_cast<unsigned char>(v);
        v >>= 8;
        for (int i = 7; i >= 0; --i) { p[i] = static_cast<unsigned char>((v & 0x7f) | 0x80); v >>= 7; }
        return 9;
    }
    unsigned char reversed[9];
    std::size_t n = 0;
    do { reversed[n++] = static_cast<unsigned char>((v & 0x7f) | 0x80); v >>= 7; } while (v);
    reversed[0] &= 0x7f;
    for (std::size_t i = 0; i < n; ++i) p[i] = reversed[n - 1 - i];
    return n;
}
std::size_t varint_size(U64 v) { unsigned char scratch[9]; return put_varint(scratch, v); }
void append_varint(std::string& out, U64 v) { unsigned char b[9]; out.append(reinterpret_cast<char const*>(b), put_varint(b, v)); }
void put16(unsigned char* p, std::size_t v) { p[0] = static_cast<unsigned char>(v >> 8); p[1] = static_cast<unsigned char>(v); }
void put32(unsigned char* p, U64 v) { for (int i = 0; i < 4; ++i) p[i] = static_cast<unsigned char>(v >> (24 - 8 * i)); }
U32 get16(unsigned char const* p) { return U32(p[0]) << 8 | p[1]; }
U32 get32(unsigned char const* p) { return U32(p[0]) << 24 | U32(p[1]) << 16 | U32(p[2]) << 8 | p[3]; }

// A sort key per entry whose byte order is SQLite's for the index (BINARY collation): per
// column a tag in SQLite's type order (NULL < INTEGER < TEXT), then an INTEGER as 8 bytes
// big-endian with the sign flipped, or TEXT and a 0 terminator (TEXT holding a 0 byte is
// refused); then the rowid as an INTEGER.
constexpr char tag_null = 1, tag_integer = 2, tag_text = 3;
void key_integer(std::string& key, std::int64_t v) {
    auto u = static_cast<U64>(v) ^ (U64(1) << 63);
    for (int i = 7; i >= 0; --i) key.push_back(static_cast<char>(u >> (8 * i)));
}
std::int64_t read_integer(std::string_view key, std::size_t& at) {
    U64 u = 0;
    for (int i = 0; i < 8; ++i) u = u << 8 | static_cast<unsigned char>(key[at + i]);
    at += 8;
    return static_cast<std::int64_t>(u ^ (U64(1) << 63));
}
// The key without its rowid, and whether any column is NULL (NULLs never repeat in SQLite).
std::pair<std::string_view, bool> columns_of(std::string_view key, std::size_t columns) {
    std::size_t at = 0;
    bool null = false;
    for (std::size_t c = 0; c < columns; ++c) {
        auto tag = key[at++];
        if (tag == tag_null) null = true;
        else if (tag == tag_integer) at += 8;
        else at = key.find('\0', at) + 1;
    }
    return {key.substr(0, at), null};
}
// The index record for a sort key (its columns, then the rowid) as SQLite writes it, into
// `out` (reused: the merge encodes every entry through one buffer).
void record(std::string& out, std::string_view key, std::size_t columns, bool small_integers) {
    struct Value { U64 type = 0; std::string_view text; std::int64_t integer = 0; int bytes = 0; };
    std::array<Value, 16> values;
    if (columns + 1 > values.size()) throw Error("Too many index columns to build outside SQLite");
    std::size_t n = 0, header = 0, body = 0, at = 0;
    auto integer = [&](std::int64_t i) {
        auto& v = values[n++];
        v = Value{6, {}, i, 8};
        if (small_integers && (i == 0 || i == 1)) v.type = i == 0 ? 8 : 9, v.bytes = 0;
        else if (i >= -128 && i <= 127) v.type = 1, v.bytes = 1;
        else if (i >= -32768 && i <= 32767) v.type = 2, v.bytes = 2;
        else if (i >= -8388608 && i <= 8388607) v.type = 3, v.bytes = 3;
        else if (i >= -2147483648LL && i <= 2147483647LL) v.type = 4, v.bytes = 4;
        else if (i >= -140737488355328LL && i <= 140737488355327LL) v.type = 5, v.bytes = 6;
        header += varint_size(v.type);
        body += static_cast<std::size_t>(v.bytes);
    };
    for (std::size_t c = 0; c < columns; ++c) {
        auto tag = key[at++];
        if (tag == tag_null) { values[n++] = Value{}; header += 1; }
        else if (tag == tag_integer) integer(read_integer(key, at));
        else {
            auto end = key.find('\0', at);
            auto& v = values[n++];
            v = Value{2 * (end - at) + 13, key.substr(at, end - at), 0, 0};
            header += varint_size(v.type);
            body += v.text.size();
            at = end + 1;
        }
    }
    integer(read_integer(key, at));  // the rowid
    U64 size = header + 1;  // the header's size counts its own varint
    while (varint_size(size) + header != size) size = varint_size(size) + header;
    out.resize(size + body);
    auto* p = reinterpret_cast<unsigned char*>(out.data());
    p += put_varint(p, size);
    for (std::size_t i = 0; i < n; ++i) p += put_varint(p, values[i].type);
    for (std::size_t i = 0; i < n; ++i) {
        auto const& v = values[i];
        if (!v.text.empty()) { std::memcpy(p, v.text.data(), v.text.size()); p += v.text.size(); }
        for (int b = v.bytes - 1; b >= 0; --b) *p++ = static_cast<unsigned char>(static_cast<U64>(v.integer) >> (8 * b));
    }
}

// Sorted runs of keys, written by several threads (each sorts its own buffer when full) and
// merged in order.
class Runs {
public:
    Runs(fs::path directory, std::string prefix) : directory_(std::move(directory)), prefix_(std::move(prefix)) {}
    ~Runs() { std::error_code ignored; for (auto const& run : runs_) fs::remove(run, ignored); }
    class Buffer {
    public:
        Buffer(Runs& runs, std::size_t limit) : runs_(runs), limit_(limit) {}
        void add(std::string_view key) {
            if (arena_.size() + key.size() > limit_ && !starts_.empty()) spill();
            starts_.push_back(arena_.size());
            arena_.append(key);
        }
        void spill() {
            if (starts_.empty()) return;
            std::vector<std::string_view> keys;
            keys.reserve(starts_.size());
            for (std::size_t i = 0; i < starts_.size(); ++i) {
                auto end = i + 1 < starts_.size() ? starts_[i + 1] : arena_.size();
                keys.emplace_back(arena_.data() + starts_[i], end - starts_[i]);
            }
            auto sorting = std::chrono::steady_clock::now();
            std::sort(keys.begin(), keys.end(), [](std::string_view a, std::string_view b) { return compare(a, b) < 0; });
            auto writing = std::chrono::steady_clock::now();
            runs_.sort_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(writing - sorting).count();
            auto path = runs_.next();
            std::unique_ptr<std::FILE, int (*)(std::FILE*)> file(std::fopen(path.c_str(), "wb"), std::fclose);
            if (!file) throw Error("Cannot write index run " + path.string());
            std::vector<char> buffer(1 << 20);
            std::setvbuf(file.get(), buffer.data(), _IOFBF, buffer.size());
            for (auto key : keys) {
                auto n = static_cast<U32>(key.size());
                if (std::fwrite(&n, sizeof n, 1, file.get()) != 1 || std::fwrite(key.data(), 1, key.size(), file.get()) != key.size())
                    throw Error("Writing index run failed");
            }
            if (std::fflush(file.get()) != 0) throw Error("Writing index run failed");
            file.reset();
            runs_.write_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - writing).count();
            arena_.clear();
            starts_.clear();
        }
    private:
        Runs& runs_;
        std::size_t limit_;
        std::string arena_;
        std::vector<std::size_t> starts_;
    };
    static int compare(std::string_view a, std::string_view b) {
        auto n = std::memcmp(a.data(), b.data(), std::min(a.size(), b.size()));
        return n ? n : (a.size() < b.size() ? -1 : a.size() > b.size());
    }
    // Every key, in order. More runs than files kept open at once are first merged into longer
    // runs, 256 at a time.
    template <class Visit> void merge(Visit&& visit) {
        while (runs_.size() > 256) {
            std::vector<fs::path> batch(runs_.begin(), runs_.begin() + 256);
            runs_.erase(runs_.begin(), runs_.begin() + 256);
            auto merged = next();
            {
                std::unique_ptr<std::FILE, int (*)(std::FILE*)> file(std::fopen(merged.c_str(), "wb"), std::fclose);
                if (!file) throw Error("Cannot write index run " + merged.string());
                std::vector<char> buffer(1 << 20);
                std::setvbuf(file.get(), buffer.data(), _IOFBF, buffer.size());
                merge_files(batch, [&](std::string_view key) {
                    auto n = static_cast<U32>(key.size());
                    if (std::fwrite(&n, sizeof n, 1, file.get()) != 1 || std::fwrite(key.data(), 1, key.size(), file.get()) != key.size())
                        throw Error("Writing index run failed");
                });
                if (std::fflush(file.get()) != 0) throw Error("Writing index run failed");
            }
            std::error_code ignored;
            for (auto const& run : batch) fs::remove(run, ignored);
        }
        merge_files(runs_, visit);
    }
    // Drops every run (a read that is started over).
    void clear() {
        std::lock_guard lock(mutex_);
        std::error_code ignored;
        for (auto const& run : runs_) fs::remove(run, ignored);
        runs_.clear();
    }
    std::atomic<std::int64_t> sort_ns{0}, write_ns{0};  // spent sorting and writing runs, over all threads
private:
    template <class Visit> static void merge_files(std::vector<fs::path> const& paths, Visit&& visit) {
        struct Reader {
            std::unique_ptr<std::FILE, int (*)(std::FILE*)> file{nullptr, std::fclose};
            std::vector<char> buffer = std::vector<char>(1 << 20);
            std::string key;
            bool next() {
                U32 n = 0;
                if (std::fread(&n, sizeof n, 1, file.get()) != 1) return false;
                key.resize(n);
                if (std::fread(key.data(), 1, n, file.get()) != n) throw Error("Truncated index run");
                return true;
            }
        };
        std::vector<Reader> readers(paths.size());
        auto after = [&](std::size_t a, std::size_t b) { return compare(readers[a].key, readers[b].key) > 0; };
        std::priority_queue<std::size_t, std::vector<std::size_t>, decltype(after)> heap(after);
        for (std::size_t i = 0; i < paths.size(); ++i) {
            readers[i].file.reset(std::fopen(paths[i].c_str(), "rb"));
            if (!readers[i].file) throw Error("Cannot read index run " + paths[i].string());
            std::setvbuf(readers[i].file.get(), readers[i].buffer.data(), _IOFBF, readers[i].buffer.size());
            if (readers[i].next()) heap.push(i);
        }
        while (!heap.empty()) {
            auto i = heap.top();
            heap.pop();
            visit(std::string_view(readers[i].key));
            if (readers[i].next()) heap.push(i);
        }
    }
    fs::path next() {
        std::lock_guard lock(mutex_);
        runs_.push_back(directory_ / (prefix_ + std::to_string(serial_++)));
        return runs_.back();
    }
    fs::path directory_;
    std::string prefix_;
    std::mutex mutex_;
    std::vector<fs::path> runs_;
    std::size_t serial_ = 0;
};

// One index's pages, appended to the database file while other indexes append theirs. They
// are numbered from 1 as its tree adds them (children before their parents) and, a block of
// about 8 MB at a time, given the file's next free page numbers; the block's interior pages
// then name their children by their places in the file.
class Pages {
public:
    struct File {
        int fd;
        U32 page_size;
        std::mutex mutex;
        U32 next;  // the first page number no block holds yet
    };
    explicit Pages(File& file) : file_(file) {}
    U32 add(std::string const& page) {
        if (buffer_.size() >= (8u << 20)) flush();
        buffer_ += page;
        return ++added_;
    }
    void flush() {
        if (buffer_.empty()) return;
        auto count = static_cast<U32>(buffer_.size() / file_.page_size);
        U32 base;
        {
            std::lock_guard lock(file_.mutex);
            base = file_.next;
            file_.next += count;
        }
        blocks_.push_back({added_ - count + 1, base});
        for (std::size_t at = 0; at < buffer_.size(); at += file_.page_size) {
            auto* p = reinterpret_cast<unsigned char*>(buffer_.data() + at);
            if (p[0] != 0x02) continue;  // a leaf names no pages
            put32(p + 8, place(get32(p + 8)));
            for (U32 i = 0, n = get16(p + 3); i < n; ++i) {
                auto* cell = p + get16(p + 12 + 2 * i);
                put32(cell, place(get32(cell)));
            }
        }
        std::size_t done = 0;
        auto offset = static_cast<off_t>(U64(base - 1) * file_.page_size);
        while (done < buffer_.size()) {
            auto n = ::pwrite(file_.fd, buffer_.data() + done, buffer_.size() - done, offset + static_cast<off_t>(done));
            if (n <= 0) throw Error("Writing index pages failed");
            done += static_cast<std::size_t>(n);
        }
        buffer_.clear();
    }
    // Where an added page is in the file, once its block is written.
    U32 place(U32 added) const {
        auto after = std::upper_bound(blocks_.begin(), blocks_.end(), added, [](U32 a, Block const& b) { return a < b.first; });
        if (after == blocks_.begin()) throw Error("Index page placed before it was written");
        auto const& block = *std::prev(after);
        return block.base + (added - block.first);
    }
private:
    struct Block { U32 first, base; };  // its first page as numbered while adding, and that page's place
    File& file_;
    std::string buffer_;
    U32 added_ = 0;
    std::vector<Block> blocks_;
};

// One index B-tree, built from its entries in order: leaves are filled and written as they
// fill; the entry that does not fit becomes the separator their parent holds (an index B-tree
// keeps each entry once), and so on up. One entry (or separator) is held back per level, so
// the last page of every level keeps at least one cell and all leaves stay at one depth.
class Tree {
public:
    Tree(Pages& pages, U32 page_size, U32 usable) : pages_(pages), usable_(usable),
        max_local_((usable - 12) * 64 / 255 - 23), page_(page_size, '\0') {
        levels_.reserve(32);  // views into a level's entries stay valid while separators go up
        levels_.emplace_back();
    }
    void add(std::string_view entry) {
        if (entry.size() > max_local_) throw Error("Index entry too large to build outside SQLite");
        if (levels_[0].held) {  // more entries follow: the leaf is final and the held entry goes up
            auto page = write(0, 0);
            child(1, page);
            separator(1, levels_[0].held_entry);
            levels_[0].held = false;
        }
        place(0, entry);
    }
    // The root page.
    U32 finish() {
        if (levels_[0].held) {  // the last leaf would be empty: the full one's last entry goes up
            auto last = pop(0);
            auto page = write(0, 0);
            child(1, page);
            separator(1, last);
            add_cell(0, 0, levels_[0].held_entry);
            levels_[0].held = false;
        }
        auto page = write(0, 0);
        child(1, page);
        return finish(1);
    }
private:
    struct Level {
        std::string arena;               // the cells' entries (leaf) or separators (interior), in order
        std::vector<std::size_t> ends;
        std::vector<U32> lefts;          // interior: each separator's left child
        std::size_t used = 0;            // cell and pointer bytes
        std::optional<U32> pending;      // interior: the child left of the next separator
        bool held = false;               // an entry or separator that did not fit
        std::string held_entry;
        std::optional<U32> held_child;   // interior: the child right of the held separator
        std::string_view entry(std::size_t i) const { auto begin = i ? ends[i - 1] : 0; return {arena.data() + begin, ends[i] - begin}; }
    };
    std::size_t cell_size(std::size_t level, std::size_t entry) const { return (level ? 4 : 0) + varint_size(entry) + entry + 2; }
    std::size_t capacity(std::size_t level) const { return usable_ - (level ? 12 : 8); }
    void add_cell(std::size_t level, U32 left, std::string_view entry) {
        auto& l = levels_[level];
        l.used += cell_size(level, entry.size());
        l.arena.append(entry);
        l.ends.push_back(l.arena.size());
        if (level) l.lefts.push_back(left);
    }
    std::string pop(std::size_t level) {
        auto& l = levels_[level];
        std::string entry(l.entry(l.ends.size() - 1));
        l.ends.pop_back();
        l.arena.resize(l.ends.empty() ? 0 : l.ends.back());
        if (level) l.lefts.pop_back();
        l.used -= cell_size(level, entry.size());
        return entry;
    }
    void hold(std::size_t level, std::string_view entry) { levels_[level].held = true; levels_[level].held_entry.assign(entry); }
    // Adds an entry to a leaf, or holds it when the leaf is full.
    void place(std::size_t level, std::string_view entry) {
        if (levels_[level].used + cell_size(level, entry.size()) <= capacity(level)) add_cell(level, 0, entry);
        else hold(level, entry);
    }
    void child(std::size_t level, U32 page) {
        if (levels_.size() <= level) {
            if (level >= levels_.capacity()) throw Error("Index B-tree too deep");
            levels_.resize(level + 1);
        }
        auto& l = levels_[level];
        if (l.held) l.held_child = page;
        else l.pending = page;
    }
    void separator(std::size_t level, std::string_view entry) {
        if (levels_[level].held) {  // the page was full: it ends at `pending`, and the held separator goes up
            auto right = *levels_[level].pending;
            auto page = write(level, right);
            child(level + 1, page);
            separator(level + 1, levels_[level].held_entry);
            levels_[level].held = false;
            auto left = *levels_[level].held_child;
            levels_[level].held_child.reset();
            add_cell(level, left, entry);
            levels_[level].pending.reset();
            return;
        }
        if (levels_[level].used + cell_size(level, entry.size()) <= capacity(level)) {
            add_cell(level, *levels_[level].pending, entry);
            levels_[level].pending.reset();
        } else hold(level, entry);
    }
    U32 finish(std::size_t level) {
        if (levels_[level].held) {  // as for leaves: the full page's last separator goes up
            auto left = levels_[level].lefts.back();
            auto last = pop(level);
            auto page = write(level, left);
            child(level + 1, page);
            separator(level + 1, last);
            auto pending = *levels_[level].pending, right = *levels_[level].held_child;
            add_cell(level, pending, levels_[level].held_entry);
            levels_[level].held = false;
            levels_[level].held_child.reset();
            levels_[level].pending = right;
        }
        if (levels_[level].ends.empty()) return *levels_[level].pending;  // one child: the root
        auto page = write(level, *levels_[level].pending);
        if (levels_.size() <= level + 1) return page;
        child(level + 1, page);
        return finish(level + 1);
    }
    // Writes a level's page (interior pages end at `right`) and starts the next one.
    U32 write(std::size_t level, U32 right) {
        auto& l = levels_[level];
        std::fill(page_.begin(), page_.end(), '\0');
        auto* p = reinterpret_cast<unsigned char*>(page_.data());
        std::size_t header = level ? 12 : 8, content = usable_;
        for (std::size_t i = 0; i < l.ends.size(); ++i) {
            auto entry = l.entry(i);
            content -= cell_size(level, entry.size()) - 2;
            auto* c = p + content;
            if (level) { put32(c, l.lefts[i]); c += 4; }
            c += put_varint(c, entry.size());
            std::memcpy(c, entry.data(), entry.size());
            put16(p + header + 2 * i, content);
        }
        p[0] = level ? 0x02 : 0x0A;
        put16(p + 3, l.ends.size());
        put16(p + 5, content == 65536 ? 0 : content);
        if (level) put32(p + 8, right);
        l.arena.clear();
        l.ends.clear();
        l.lefts.clear();
        l.used = 0;
        return pages_.add(page_);
    }
    Pages& pages_;
    U32 usable_;
    std::size_t max_local_;
    std::string page_;
    std::vector<Level> levels_;
};

struct Header {
    U32 page_size, usable, pages, change_counter;
    bool small_integers;
};
Header read_header(int fd) {
    unsigned char h[100];
    if (::pread(fd, h, sizeof h, 0) != static_cast<ssize_t>(sizeof h) || std::memcmp(h, "SQLite format 3\0", 16) != 0) throw Error("Not an SQLite database");
    Header out;
    out.page_size = get16(h + 16) == 1 ? 65536 : get16(h + 16);
    out.usable = out.page_size - h[20];
    if (get32(h + 52) != 0) throw Error("Indexes are not built outside SQLite in auto-vacuum databases");
    if (get32(h + 56) != 1) throw Error("Indexes are built outside SQLite in UTF-8 databases only");
    out.change_counter = get32(h + 24);
    auto size = static_cast<U64>(::lseek(fd, 0, SEEK_END));
    out.pages = get32(h + 92) == out.change_counter && get32(h + 28) ? get32(h + 28) : static_cast<U32>(size / out.page_size);
    if (U64(out.pages) * out.page_size != size) throw Error("SQLite database size and header differ");
    out.small_integers = get32(h + 44) >= 4;
    return out;
}
void write_header(int fd, U32 pages, U32 change_counter) {
    unsigned char field[4];
    put32(field, pages);
    if (::pwrite(fd, field, 4, 28) != 4) throw Error("Writing the SQLite header failed");
    put32(field, change_counter);
    if (::pwrite(fd, field, 4, 24) != 4 || ::pwrite(fd, field, 4, 92) != 4) throw Error("Writing the SQLite header failed");
}
fs::path pending_marker(fs::path const& path) { return fs::path(path.string() + "-xrpla-index"); }
// The marker removed for good, its directory synced: one a power loss brought back would undo
// pages SQLite counted since (its own build, after a failed one, commits at the counter the
// marker's undo takes back).
void remove_marker(fs::path const& marker) {
    std::error_code ignored;
    if (fs::remove(marker, ignored)) sync_file(fs::absolute(marker).parent_path());
}

// An index's entries, read over rowid ranges on `threads` connections into sorted runs.
void scan(fs::path const& path, Spec const& spec, unsigned threads, Runs& runs) {
    auto open = [&] {
        sqlite3* db = nullptr;
        if (sqlite3_open_v2(path.string().c_str(), &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, nullptr) != SQLITE_OK) {
            std::string message = db ? sqlite3_errmsg(db) : "allocation failed";
            sqlite3_close(db);
            throw Error("SQLite: " + message);
        }
        sqlite3_busy_timeout(db, 10000);
        return std::unique_ptr<sqlite3, int (*)(sqlite3*)>(db, sqlite3_close);
    };
    auto prepare = [](sqlite3* db, std::string const& sql) {
        sqlite3_stmt* statement = nullptr;
        if (sqlite3_prepare_v2(db, sql.c_str(), -1, &statement, nullptr) != SQLITE_OK) throw Error("SQLite prepare: " + std::string(sqlite3_errmsg(db)));
        return std::unique_ptr<sqlite3_stmt, int (*)(sqlite3_stmt*)>(statement, sqlite3_finalize);
    };
    std::int64_t low = 0, high = -1;
    {
        auto db = open();
        auto bounds = prepare(db.get(), "SELECT min(rowid), max(rowid) FROM \"" + spec.table + "\"");
        if (sqlite3_step(bounds.get()) == SQLITE_ROW && sqlite3_column_type(bounds.get(), 0) != SQLITE_NULL) {
            low = sqlite3_column_int64(bounds.get(), 0);
            high = sqlite3_column_int64(bounds.get(), 1);
        }
    }
    if (high < low) return;
    std::string select = "SELECT ";
    for (auto const& column : spec.columns) select += "\"" + column + "\", ";
    select += "rowid FROM \"" + spec.table + "\" WHERE rowid BETWEEN ?1 AND ?2";
    auto const chunks = static_cast<std::int64_t>(threads) * 16;
    auto const width = (high - low) / chunks + 1;
    std::atomic<std::int64_t> next{0};
    std::atomic<bool> stop{false};
    std::mutex failed_mutex;
    std::exception_ptr failed;
    auto work = [&] {
        try {
            auto db = open();
            auto statement = prepare(db.get(), select);
            Runs::Buffer buffer(runs, 128u << 20);
            std::string key;
            for (std::int64_t chunk; !stop && (chunk = next++) < chunks;) {
                auto from = low + chunk * width;
                if (from > high) break;
                sqlite3_bind_int64(statement.get(), 1, from);
                sqlite3_bind_int64(statement.get(), 2, std::min(high, from + width - 1));
                int step;
                while ((step = sqlite3_step(statement.get())) == SQLITE_ROW) {
                    key.clear();
                    for (int c = 0; c < static_cast<int>(spec.columns.size()); ++c) {
                        switch (sqlite3_column_type(statement.get(), c)) {
                        case SQLITE_NULL: key.push_back(tag_null); break;
                        case SQLITE_INTEGER: key.push_back(tag_integer); key_integer(key, sqlite3_column_int64(statement.get(), c)); break;
                        case SQLITE_TEXT: {
                            auto const* text = reinterpret_cast<char const*>(sqlite3_column_text(statement.get(), c));
                            std::string_view value(text, static_cast<std::size_t>(sqlite3_column_bytes(statement.get(), c)));
                            if (value.find('\0') != std::string_view::npos) throw Error("Index text holds a 0 byte");
                            key.push_back(tag_text);
                            key.append(value);
                            key.push_back('\0');
                            break;
                        }
                        default: throw Error("Indexes are built outside SQLite over INTEGER and TEXT values only");
                        }
                    }
                    key_integer(key, sqlite3_column_int64(statement.get(), static_cast<int>(spec.columns.size())));
                    buffer.add(key);
                }
                if (step != SQLITE_DONE) throw Error("SQLite scan: " + std::string(sqlite3_errmsg(db.get())));
                sqlite3_reset(statement.get());
            }
            buffer.spill();
        } catch (...) {
            std::lock_guard lock(failed_mutex);
            if (!failed) failed = std::current_exception();
            stop = true;
        }
    };
    std::vector<std::thread> workers;
    for (unsigned i = 0; i < std::max(1u, threads); ++i) workers.emplace_back(work);
    for (auto& worker : workers) worker.join();
    if (failed) std::rethrow_exception(failed);
}

// A read that is not done straight from the file (the SQLite scan does it instead).
struct Unsupported : Error { using Error::Error; };
// Runs f(0) .. f(n - 1) on n threads, then rethrows the first exception any of them threw.
template <class F> void on_threads(unsigned n, F&& f) {
    std::mutex mutex;
    std::exception_ptr failed;
    std::vector<std::thread> pool;
    for (unsigned t = 0; t < n; ++t)
        pool.emplace_back([&, t] {
            try { f(t); }
            catch (...) { std::lock_guard lock(mutex); if (!failed) failed = std::current_exception(); }
        });
    for (auto& thread : pool) thread.join();
    if (failed) std::rethrow_exception(failed);
}
std::size_t get_varint(unsigned char const* p, unsigned char const* end, U64& v) {
    v = 0;
    for (std::size_t i = 0; i < 8; ++i) {
        if (p + i >= end) throw Unsupported("Truncated varint");
        v = v << 7 | (p[i] & 0x7f);
        if (!(p[i] & 0x80)) return i + 1;
    }
    if (p + 8 >= end) throw Unsupported("Truncated varint");
    v = v << 8 | p[8];
    return 9;
}
// A table B-tree leaf cell's payload bytes kept on its page (the rest overflows).
U64 local_size(U64 payload, U32 usable) {
    U64 max_local = usable - 35, min_local = (U64(usable) - 12) * 32 / 255 - 23;
    if (payload <= max_local) return payload;
    auto m = min_local + (payload - min_local) % (usable - 4);
    return m <= max_local ? m : min_local;
}
// A table read straight from its leaf pages, and the specs over it.
struct TableScan {
    std::string name;
    U32 root = 0;
    std::vector<std::size_t> specs;                   // indexes into the build's specs
    std::vector<std::vector<std::size_t>> positions;  // per spec: its columns' places in the table's records
    std::size_t needed = 0;                           // record columns read (the highest place + 1)
    std::vector<U64> leaves;                          // a bit per page: one of this table's leaves
    bool leaf(U32 page) const { return page / 64 < leaves.size() && (leaves[page / 64] >> (page % 64) & 1); }
};
// Every spec's entries from one read of the file: each table's interior pages are read to
// mark its leaves, then threads read the file in page order, in blocks of up to 4 MB, and
// turn every leaf's rows into their specs' keys. SQLite's scans read a page at a time, and
// cold (a table's leaves spread among the other table's and among overflow pages) one took
// 60-100 s of a 200,000-ledger restore, where this read all 39 GB for the five indexes in
// 17 s on 8 threads.
void scan_file(fs::path const& path, int fd, Header const& header, std::vector<Spec> const& specs,
               std::vector<std::unique_ptr<Runs>>& runs, unsigned threads, std::map<std::string, double>& seconds) {
    auto since = [](auto began) { return std::chrono::duration<double>(std::chrono::steady_clock::now() - began).count(); };
    auto walking = std::chrono::steady_clock::now();
    std::vector<TableScan> tables;
    {
        sqlite3* raw = nullptr;
        if (sqlite3_open_v2(path.string().c_str(), &raw, SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, nullptr) != SQLITE_OK) {
            sqlite3_close(raw);
            throw Unsupported("Cannot open the database to read its schema");
        }
        std::unique_ptr<sqlite3, int (*)(sqlite3*)> db(raw, sqlite3_close);
        auto query = [&](std::string const& sql, std::string const& name) {
            sqlite3_stmt* statement = nullptr;
            if (sqlite3_prepare_v2(db.get(), sql.c_str(), -1, &statement, nullptr) != SQLITE_OK) throw Unsupported(sqlite3_errmsg(db.get()));
            std::unique_ptr<sqlite3_stmt, int (*)(sqlite3_stmt*)> owned(statement, sqlite3_finalize);
            sqlite3_bind_text(statement, 1, name.c_str(), -1, SQLITE_TRANSIENT);
            std::vector<std::vector<std::string>> rows;
            while (sqlite3_step(statement) == SQLITE_ROW) {
                auto& row = rows.emplace_back();
                for (int c = 0; c < sqlite3_column_count(statement); ++c) {
                    auto const* text = sqlite3_column_text(statement, c);
                    row.push_back(text ? reinterpret_cast<char const*>(text) : "");
                }
            }
            return rows;
        };
        for (std::size_t i = 0; i < specs.size(); ++i) {
            auto found = std::find_if(tables.begin(), tables.end(), [&](auto const& t) { return t.name == specs[i].table; });
            if (found == tables.end()) {
                TableScan table;
                table.name = specs[i].table;
                auto master = query("SELECT rootpage, sql FROM sqlite_master WHERE type='table' AND name=?1", table.name);
                if (master.size() != 1) throw Unsupported("No table " + table.name);
                std::string sql = master[0][1];
                std::transform(sql.begin(), sql.end(), sql.begin(), [](unsigned char c) { return std::toupper(c); });
                if (sql.find("WITHOUT ROWID") != std::string::npos) throw Unsupported("A WITHOUT ROWID table");
                table.root = static_cast<U32>(std::stoul(master[0][0]));
                tables.push_back(std::move(table));
                found = std::prev(tables.end());
            }
            auto columns = query("SELECT name, upper(type), pk FROM pragma_table_info(?1) ORDER BY cid", found->name);
            // An INTEGER PRIMARY KEY is the rowid: its records hold NULL there.
            for (auto const& column : columns)
                if (column[2] == "1" && column[1] == "INTEGER") throw Unsupported("A rowid alias column");
            std::vector<std::size_t> places;
            for (auto const& name : specs[i].columns) {
                auto at = std::find_if(columns.begin(), columns.end(), [&](auto const& c) { return c[0] == name; });
                if (at == columns.end()) throw Unsupported("No column " + name);
                places.push_back(static_cast<std::size_t>(at - columns.begin()));
                found->needed = std::max(found->needed, places.back() + 1);
            }
            found->specs.push_back(i);
            found->positions.push_back(std::move(places));
        }
    }
    // Each table's leaves: its interior pages are read a level at a time, in page order on
    // several threads, and the children of a level whose first child is a leaf are all leaves
    // (a table B-tree keeps every leaf at the same depth), marked without being read. Reading
    // every leaf to learn its type, one at a time, took 301 s of a 200,000-ledger build.
    auto read_page = [&](U32 number, unsigned char* into) {
        if (number < 2 || number > header.pages) throw Unsupported("Page out of range");
        if (::pread(fd, into, header.page_size, static_cast<off_t>(U64(number - 1) * header.page_size)) != static_cast<ssize_t>(header.page_size))
            throw Unsupported("Short page read");
    };
    auto const readers = std::clamp(threads, 1u, 16u);
    auto children_of = [&](std::vector<U32> level) {
        std::sort(level.begin(), level.end());
        std::vector<std::vector<U32>> found(readers);
        auto per = (level.size() + readers - 1) / readers;
        on_threads(readers, [&](unsigned t) {
            std::vector<unsigned char> page(header.page_size);
            for (auto i = t * per; i < std::min(level.size(), (t + 1) * per); ++i) {
                read_page(level[i], page.data());
                auto const* p = page.data();
                if (p[0] != 0x05) throw Unsupported("Not a table B-tree page");
                for (U32 c = 0, n = get16(p + 3); c < n; ++c) {
                    auto offset = get16(p + 12 + 2 * c);
                    if (offset + 4 > header.usable) throw Unsupported("Cell past the page");
                    found[t].push_back(get32(p + offset));
                }
                found[t].push_back(get32(p + 8));
            }
        });
        std::vector<U32> children;
        for (auto const& some : found) children.insert(children.end(), some.begin(), some.end());
        return children;
    };
    std::vector<unsigned char> page(header.page_size);
    for (auto& table : tables) {
        table.leaves.assign(header.pages / 64 + 1, 0);
        auto mark = [&](U32 number) {
            if (number < 2 || number > header.pages) throw Unsupported("Page out of range");
            table.leaves[number / 64] |= U64(1) << (number % 64);
        };
        read_page(table.root, page.data());
        if (page[0] == 0x0D) { mark(table.root); continue; }
        std::vector<U32> level{table.root};
        for (int depth = 0;; ++depth) {
            if (depth > 40) throw Unsupported("A B-tree too deep");
            auto children = children_of(std::move(level));
            read_page(children.front(), page.data());
            if (page[0] == 0x0D) { for (auto child : children) mark(child); break; }
            if (page[0] != 0x05) throw Unsupported("Not a table B-tree page");
            level = std::move(children);
        }
    }
    seconds["read.walk"] = since(walking);
    auto scanning = std::chrono::steady_clock::now();
    std::atomic<std::int64_t> io_ns{0}, busy_ns{0};
    // The reads, a share of the file's pages per thread.
    auto const workers = std::clamp(threads, 1u, 8u);
    std::mutex failed_mutex;
    std::exception_ptr failed;
    std::atomic<bool> stop{false};
    auto work = [&](U32 first, U32 last) {
        auto began = std::chrono::steady_clock::now();
        try {
            std::vector<std::unique_ptr<Runs::Buffer>> buffers;
            for (auto& run : runs) buffers.push_back(std::make_unique<Runs::Buffer>(*run, 64u << 20));
            std::vector<unsigned char> block(std::size_t(1024) * header.page_size);
            std::string key;
            struct Column { U64 type = 0; unsigned char const* value = nullptr; };
            std::vector<Column> record_columns;
            auto owner = [&](U32 number) -> TableScan const* {
                for (auto const& table : tables) if (table.leaf(number)) return &table;
                return nullptr;
            };
            auto parse = [&](unsigned char const* p, TableScan const& table) {
                auto const* end = p + header.usable;
                for (U32 i = 0, n = get16(p + 3); i < n; ++i) {
                    auto offset = get16(p + 8 + 2 * i);
                    if (offset >= header.usable) throw Unsupported("Cell past the page");
                    auto const* c = p + offset;
                    U64 payload = 0, rowid = 0;
                    c += get_varint(c, end, payload);
                    c += get_varint(c, end, rowid);
                    auto local = local_size(payload, header.usable);
                    if (c + local > end) throw Unsupported("Cell past the page");
                    auto const* record_end = c + local;
                    U64 header_size = 0;
                    auto const* types = c + get_varint(c, record_end, header_size);
                    if (header_size > local) throw Unsupported("A record header in overflow pages");
                    auto const* types_end = c + header_size;
                    auto const* body = types_end;
                    record_columns.assign(table.needed, Column{});
                    for (std::size_t k = 0; k < table.needed && types < types_end; ++k) {
                        U64 type = 0;
                        types += get_varint(types, types_end, type);
                        U64 size = type >= 12 ? (type - 12) / 2 : type == 5 ? 6 : type == 6 || type == 7 ? 8 : type <= 4 ? type : 0;
                        if (type == 10 || type == 11) throw Unsupported("A reserved serial type");
                        record_columns[k] = Column{type, body};
                        body += size;
                        if (body > record_end) throw Unsupported("An index column in overflow pages");
                    }
                    for (std::size_t s = 0; s < table.specs.size(); ++s) {
                        key.clear();
                        for (auto place : table.positions[s]) {
                            auto const& column = record_columns[place];
                            auto type = column.type;
                            if (type == 0) key.push_back(tag_null);
                            else if (type == 8 || type == 9) { key.push_back(tag_integer); key_integer(key, type == 9); }
                            else if (type <= 6) {
                                auto bytes = type == 5 ? 6u : type == 6 ? 8u : static_cast<unsigned>(type);
                                U64 v = 0;
                                for (unsigned b = 0; b < bytes; ++b) v = v << 8 | column.value[b];
                                if (bytes < 8 && (v >> (8 * bytes - 1) & 1)) v |= ~U64(0) << (8 * bytes);  // sign
                                key.push_back(tag_integer);
                                key_integer(key, static_cast<std::int64_t>(v));
                            } else if (type >= 13 && type % 2 == 1) {
                                std::string_view text(reinterpret_cast<char const*>(column.value), (type - 13) / 2);
                                if (text.find('\0') != std::string_view::npos) throw Unsupported("Index text holds a 0 byte");
                                key.push_back(tag_text);
                                key.append(text);
                                key.push_back('\0');
                            } else throw Unsupported("A REAL or BLOB index value");
                        }
                        key_integer(key, static_cast<std::int64_t>(rowid));
                        buffers[table.specs[s]]->add(key);
                    }
                }
            };
            for (U32 number = first; number <= last && !stop;) {
                if (!owner(number)) { ++number; continue; }
                // A span of leaves, read through gaps of up to 16 other pages.
                U32 end = number, gap = 0;
                for (U32 q = number; q <= last && q - number < 1024; ++q) {
                    if (owner(q)) { end = q; gap = 0; }
                    else if (++gap > 16) break;
                }
                auto bytes = std::size_t(end - number + 1) * header.page_size;
                auto offset = static_cast<off_t>(U64(number - 1) * header.page_size);
                auto reading = std::chrono::steady_clock::now();
                for (std::size_t done = 0; done < bytes;) {
                    auto n = ::pread(fd, block.data() + done, bytes - done, offset + static_cast<off_t>(done));
                    if (n <= 0) throw Error("Reading the database failed");
                    done += static_cast<std::size_t>(n);
                }
                io_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - reading).count();
                for (U32 q = number; q <= end; ++q)
                    if (auto const* table = owner(q)) {
                        auto const* p = block.data() + std::size_t(q - number) * header.page_size;
                        if (p[0] != 0x0D) throw Unsupported("A leaf changed type");
                        parse(p, *table);
                    }
                number = end + 1;
            }
            for (auto& buffer : buffers) buffer->spill();
        } catch (...) {
            std::lock_guard lock(failed_mutex);
            if (!failed) failed = std::current_exception();
            stop = true;
        }
        busy_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - began).count();
    };
    std::vector<std::thread> pool;
    auto per = header.pages / workers + 1;
    for (unsigned t = 0; t < workers; ++t) {
        U32 first = 2 + t * per, last = std::min<U32>(header.pages, first + per - 1);
        if (first <= header.pages) pool.emplace_back(work, first, last);
    }
    for (auto& thread : pool) thread.join();
    seconds["read.scan"] = since(scanning);
    std::int64_t sort_ns = 0, write_ns = 0;
    for (auto const& run : runs) { sort_ns += run->sort_ns; write_ns += run->write_ns; }
    seconds["read.thread-io"] = io_ns / 1e9;
    seconds["read.thread-sort"] = sort_ns / 1e9;
    seconds["read.thread-runs"] = write_ns / 1e9;
    seconds["read.thread-all"] = busy_ns / 1e9;
    if (failed) std::rethrow_exception(failed);
}
} // namespace

void undo_interrupted(fs::path const& path) {
    auto marker = pending_marker(path);
    if (!fs::exists(marker)) return;
    std::error_code ignored;
    if (!fs::exists(path)) { fs::remove(marker, ignored); return; }
    // A write-ahead log holds commits the file does not show yet, its header's counter included,
    // so the rule below cannot tell what wrote since. A build and the load before it use the
    // rollback journal: another program has opened the file in WAL mode since (xrpld does).
    if (auto wal = fs::path(path.string() + "-wal"); fs::exists(wal) && fs::file_size(wal) > 0)
        throw Error(path.string() + " has a write-ahead log beside an interrupted index build: another program has written to it since. "
                    "Use a fresh output directory");
    // A hot rollback journal (SQLite stopped inside a commit, the one that records the indexes,
    // say): SQLite rolls it back first, as any opening of the file would, so the header read
    // below is the one SQLite stands by.
    if (fs::exists(path.string() + "-journal")) {
        sqlite3* db = nullptr;
        bool rolled = sqlite3_open_v2(path.c_str(), &db, SQLITE_OPEN_READWRITE, nullptr) == SQLITE_OK &&
                      sqlite3_exec(db, "SELECT count(*) FROM sqlite_master", nullptr, nullptr, nullptr) == SQLITE_OK;
        sqlite3_close(db);
        if (!rolled) throw Error("Rolling back " + path.string() + "'s journal failed");
    }
    // The marker holds the page count N and change counter C before that build. Until SQLite
    // records the indexes (a commit, counter C + 2) the build's pages count for nothing: with the
    // counter still C it stopped while writing them or its header (the file may be longer than
    // its header says, or the header half written), with C + 1 after its header. Either way the
    // file goes back to N pages. Any other counter means SQLite wrote the file since (a redone
    // restore, say) or recorded the indexes: pages past N may be live, and the marker is only
    // dropped. So is one without the counter (an older release's). The header is read as it is,
    // its page count not checked against the size: an interrupted build is what makes them differ.
    // The file is cut back and synced before its header is written, and a header that still
    // counts more than N pages (where SQLite keeps the count: its valid-for number, at 92, is the
    // counter) is taken back as a longer file is, so an undo stopped part way is done again.
    int fd = ::open(path.c_str(), O_RDWR);
    if (fd < 0) throw Error("Cannot open " + path.string());
    struct Close { int fd; ~Close() { ::close(fd); } } close{fd};
    unsigned char h[100];
    if (::pread(fd, h, sizeof h, 0) != static_cast<ssize_t>(sizeof h) || std::memcmp(h, "SQLite format 3\0", 16) != 0) throw Error("Not an SQLite database");
    U32 page_size = get16(h + 16) == 1 ? 65536 : get16(h + 16), now = get32(h + 24);
    U32 counted = get32(h + 92) == now ? get32(h + 28) : 0;
    auto size = static_cast<U64>(::lseek(fd, 0, SEEK_END));
    std::istringstream text(read_file(marker, 64));
    U32 pages = 0, counter = 0;
    bool known = static_cast<bool>(text >> pages >> counter);
    if (known && (now == counter || now == counter + 1) && (size > U64(pages) * page_size || counted > pages)) {
        if (::ftruncate(fd, static_cast<off_t>(U64(pages) * page_size)) != 0 || ::fsync(fd) != 0) throw Error("Undoing an interrupted index build failed");
        write_header(fd, pages, counter + 2);
        if (::fsync(fd) != 0) throw Error("Undoing an interrupted index build failed");
    }
    fs::remove(marker, ignored);
}

Built build(fs::path const& path, std::vector<Spec> const& specs, fs::path const& scratch, unsigned threads) {
    undo_interrupted(path);
    int fd = ::open(path.c_str(), O_RDWR);
    if (fd < 0) throw Error("Cannot open " + path.string());
    struct Close { int fd; ~Close() { ::close(fd); } } close{fd};
    auto header = read_header(fd);
    auto marker = pending_marker(path);
    // Durable before the first page is written: a header that counts the pages with no marker
    // beside it (after a power loss) would leave them in the file for good.
    write_file(marker.string() + ".part", std::to_string(header.pages) + " " + std::to_string(header.change_counter));
    durable_rename(marker.string() + ".part", marker);
    Built built;
    Pages::File file{fd, header.page_size, {}, header.pages + 1};
    // A build that fails (an entry too large for a page's cell, say) leaves the file as it was.
    // Once its header may have been written (in part, or not synced), the header is taken back as
    // undo_interrupted takes it (N pages, counter C + 2): cut back to N pages beneath a header
    // that counts more, the file is corrupt to SQLite, and without the marker for good. Should
    // the cut or the header not be made durable, the marker stays, for the next build's
    // undo_interrupted to do it. As there, a counter other than C or C + 1 means another wrote
    // the file since: pages past N may be live, and only the marker goes.
    struct Undo {
        int fd; U64 size; U32 pages, counter; fs::path marker; bool armed = true, header = false;
        ~Undo() {
            if (!armed) return;
            unsigned char field[4];
            if (::pread(fd, field, 4, 24) != 4) return;
            if (U32 now = get32(field); now == counter || now == counter + 1) {
                if (::ftruncate(fd, static_cast<off_t>(size)) != 0 || ::fsync(fd) != 0) return;
                if (header) {
                    try { write_header(fd, pages, counter + 2); } catch (std::exception const&) { return; }
                    if (::fsync(fd) != 0) return;
                }
            }
            try { remove_marker(marker); } catch (std::exception const&) {}  // the build's own failure is the one thrown
        }
    } undo{fd, U64(header.pages) * header.page_size, header.pages, header.change_counter, marker};
    std::vector<std::unique_ptr<Runs>> runs;
    for (auto const& spec : specs) runs.push_back(std::make_unique<Runs>(scratch, ".xrpla-index-" + spec.name + "-"));
    auto started = std::chrono::steady_clock::now();
    try { scan_file(path, fd, header, specs, runs, threads, built.seconds); }
    catch (Unsupported const& why) {  // read through SQLite instead
        for (auto& run : runs) run->clear();
        built.fallback = why.what();
        for (std::size_t i = 0; i < specs.size(); ++i) scan(path, specs[i], threads, *runs[i]);
    }
    built.seconds["read"] = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    // Every index is merged and written at once, each on its own thread.
    std::vector<U32> roots(specs.size());
    std::vector<double> took(specs.size());
    auto writing = std::chrono::steady_clock::now();
    on_threads(static_cast<unsigned>(specs.size()), [&](unsigned i) {
        auto const& spec = specs[i];
        auto began = std::chrono::steady_clock::now();
        Pages pages(file);
        Tree tree(pages, header.page_size, header.usable);
        std::string previous, entry;  // a unique index's last key columns; each entry's record
        bool first = true;
        runs[i]->merge([&](std::string_view key) {
            if (spec.unique) {
                auto [columns, null] = columns_of(key, spec.columns.size());
                if (!first && !null && columns == previous) throw Error("UNIQUE constraint failed: " + spec.table + " (" + spec.name + ")");
                previous.assign(columns);
            }
            first = false;
            record(entry, key, spec.columns.size(), header.small_integers);
            tree.add(entry);
        });
        runs[i].reset();  // its files go
        auto root = tree.finish();
        pages.flush();
        roots[i] = pages.place(root);
        took[i] = std::chrono::duration<double>(std::chrono::steady_clock::now() - began).count();
    });
    for (std::size_t i = 0; i < specs.size(); ++i) {
        built.roots[specs[i].name] = roots[i];
        built.seconds[specs[i].name] = took[i];
    }
    built.seconds["write"] = std::chrono::duration<double>(std::chrono::steady_clock::now() - writing).count();
    if (::fsync(fd) != 0) throw Error("Syncing index pages failed");
    // Tests stop a restore here, its pages written and not yet counted (import_rippled.cpp's
    // finish stages): undo_interrupted must take the file back from this state too.
    if (auto const* at = std::getenv("XRPLA_TEST_EXIT_IN_FINISH"); at && std::string_view(at) == "pages") std::_Exit(75);
    // The pages exist before the header counts them, and the header names them before
    // sqlite_master does.
    undo.header = true;
    write_header(fd, file.next - 1, header.change_counter + 1);
    // Tests fail a build here, its header written and not synced: Undo must take the header back.
    // Or, as if another had committed to the file meanwhile (its counter moved past C + 1): Undo
    // must leave the file as it is.
    if (auto const* at = std::getenv("XRPLA_TEST_FAIL_IN_BUILD")) {
        if (std::string_view(at) == "header") throw Error("Syncing the SQLite header failed (a test's)");
        if (std::string_view(at) == "written-since") {
            write_header(fd, file.next - 1, header.change_counter + 3);
            throw Error("Another wrote the SQLite file during the build (a test's)");
        }
    }
    if (::fsync(fd) != 0) throw Error("Syncing the SQLite header failed");
    undo.armed = false;
    return built;
}
void built(fs::path const& path) {
    remove_marker(pending_marker(path));
}
}
