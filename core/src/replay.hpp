#pragma once
#include <xrpla/core/verify.hpp>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <thread>

namespace xrpla::core {
// The checks LedgerVerifier applies to each object and transaction (verify.cpp).
namespace checks {
void object(ObjectRecord const&);
void transaction(TransactionRecord const&);
}

// Worker threads shared by a restore's pipelines (ledger replay, SQL row preparation). Tasks
// run in no particular order and must not wait for one another.
class TaskPool {
public:
    explicit TaskPool(unsigned threads);
    ~TaskPool();
    static TaskPool& shared();  // one per process, a thread per core
    void post(std::function<void()> task);
    template <class Work> auto submit(Work work) -> std::future<decltype(work())> {
        auto task = std::make_shared<std::packaged_task<decltype(work())()>>(std::move(work));
        auto result = task->get_future();
        post([task] { (*task)(); });
        return result;
    }
private:
    std::vector<std::thread> threads_;
    std::mutex mutex_;
    std::condition_variable wake_;
    std::deque<std::function<void()>> tasks_;
    bool stopping_ = false;
    void run();
};

// Replays a publication's ledgers for a restore into an xrpld node database. It checks what
// LedgerVerifier checks (each object and transaction, each ledger's parent, transaction root
// and state root against its header) but keeps the state tree in memory as hashes only: each
// leaf's key and hash, and each inner node's children (about 2.5 GB for mainnet's 20M
// objects). Nodes are named by their hashes, so once a ledger's root matches its header,
// every node sent for it is the canonical node xrpld would have written.
//
// Ledgers go through a pipeline: each is checked, its leaves hashed and its transaction tree
// built on its own task, many ledgers at once; the state tree's 16 top-level subtrees then
// take each ledger's changes in order, each subtree on its own, as far ahead of the others
// as they get; and one thread checks each ledger's root, in order, once every subtree it
// touched has taken it. Only that last step is sequential (one hash a ledger).
class StateReplay {
    struct Impl;
    std::unique_ptr<Impl> impl_;
public:
    // Receives every node built (type, hash, serialized node); called from several threads.
    using NodeSink = std::function<void(std::uint8_t, Bytes const&, Bytes const&)>;
    explicit StateReplay(NodeSink sink);
    ~StateReplay();
    // The snapshot's objects in key order, over several calls; checked and hashed on every core.
    void add_snapshot_objects(std::vector<ObjectRecord>&&);
    Header finish_snapshot(Bytes const& header, std::uint32_t sequence, std::string const& account_hash);
    // Hands the next ledger to the pipeline (its parent must be the last one handed over);
    // waits only when too many are in flight. A failure is thrown here or by wait.
    void submit(std::shared_ptr<LedgerRecord const>);
    // Waits until ledger `sequence` (and every one before it) is verified.
    void wait(std::uint32_t sequence);
    // Whether it is, without waiting (a failure is thrown here too).
    bool verified(std::uint32_t sequence) const;
    // submit, then wait for it.
    void apply(LedgerRecord const&);
    // Only a ledger's transaction tree (a baseline that already holds its state changes).
    void apply_transactions(LedgerRecord const&);
    // While false, nothing goes to the sink (replaying ledgers whose nodes are already written).
    void writing(bool);
    // The last verified ledger.
    Header header() const;
    // Seconds spent in each part of the replay, summed over threads.
    Json timings() const;
};
}
