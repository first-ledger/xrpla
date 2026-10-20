#pragma once
#include <xrpla/core/verify.hpp>
#include <memory>

namespace xrpl { class NodeObject; }

namespace xrpla::core {
// Builds an xrpld NuDB node database without NuDB's insert path, which takes one node at a time
// under a lock and reads the key file to reject duplicates (about 90k nodes/s). Instead:
//   - node values are encoded as xrpld's NuDB backend encodes them, on several threads, and
//     appended to the data file in order; each record's key, offset and size are logged;
//   - at the end, the logged entries are sorted by bucket (repeated keys dropped) and the key
//     file is written in one sequential pass, spilling full buckets to the data file as NuDB's
//     own rekey does.
// The database starts as an empty store created by xrpld's backend, so its header (UID,
// application number, key size) is the one xrpld expects. Until finish() it has no key file.
// How NudbBuilder builds the key file: its buckets in regions of at most `region_entries`
// entries (each sorted in memory, about 64 bytes an entry), `sorting_regions` at a time; the
// log is distributed to at most `open_regions` region files per pass (0: as the open-file
// limit allows). Tests make regions small.
struct NudbBuilderTuning {
    std::uint64_t region_entries = 32u << 20;
    unsigned sorting_regions = 8;
    unsigned open_regions = 0;
};
class NudbBuilder {
    struct Impl;
    std::unique_ptr<Impl> impl_;
public:
    // `directory` is the node database path ([node_db] path), holding a store xrpld's backend
    // created (empty, or built earlier by this class); the entry log is kept beside it.
    using Tuning = NudbBuilderTuning;
    NudbBuilder(fs::path const& directory, std::size_t block_size = 4096, double load_factor = 0.5, Tuning tuning = {});
    ~NudbBuilder();
    struct Checkpoint { std::uint64_t data_size = 0, entries = 0; };
    // Discards anything written after `checkpoint` (an interrupted run) and continues from it.
    void resume(Checkpoint const& checkpoint);
    // add, checkpoint and stats may be called from several threads.
    void add(std::shared_ptr<xrpl::NodeObject> const& object);
    // Everything added before the call is on disk (data file and entry log, synced).
    Checkpoint checkpoint();
    // Writes the key file: the database is then complete and xrpld can open it.
    Json finish();
    Json stats() const;
    // The data file's size now: once a finished store is checkpointed, exactly its data_size.
    std::uint64_t data_file_size() const;
};
}
