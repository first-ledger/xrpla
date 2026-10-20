#include "store.hpp"
#include <xrpla/core/operations.hpp>

namespace xrpla::core {
Json compact_store(fs::path const& directory, bool native) {
    if (!fs::is_directory(directory)) throw Error("Compaction directory is missing");
    DirectoryLock lock(directory);
    if (!native) {
        if (!fs::is_regular_file(directory / "store" / "CURRENT")) throw Error("XRPLA query store is missing");
        Store store(directory / "store", true);
        store.compact();
        return {{"compacted", true}, {"kind", "xrpla"}, {"store", store.status()}};
    }
    auto identity = Json::parse(read_file(directory / "xrpla-import.json", 1024 * 1024));
    if (identity.value("format", "") != "rippled-3.3.0-rocksdb") throw Error("Compaction requires an XRPLA native RocksDB import");
    auto path = directory / "node-db";
    safe_cache_path(path);
    rocksdb::Options options; options.compression = rocksdb::kLZ4Compression; options.max_open_files = 128;
    rocksdb::DB* raw = nullptr;
    auto check = [](rocksdb::Status const& result) { if (!result.ok()) throw Error("Native compaction: " + result.ToString()); };
    check(rocksdb::DB::Open(options, path.string(), &raw));
    std::unique_ptr<rocksdb::DB> db(raw);
    rocksdb::FlushOptions flush; flush.wait = true;
    check(db->Flush(flush));
    check(db->CompactRange(rocksdb::CompactRangeOptions{}, nullptr, nullptr));
    return {{"compacted", true}, {"kind", "rippled-rocksdb"}};
}
}
