#pragma once
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>

namespace xrpla::core {
// Calls `report` every `interval` while it lives, on a thread of its own, and not at all for work
// that ends sooner: a first sync or index build runs for hours with nothing else in the log, while
// the syncs a running node makes every few minutes finish in seconds and stay quiet.
class Progress {
    std::mutex mutex_;
    std::condition_variable wake_;
    bool done_ = false;
    std::thread thread_;
public:
    Progress(std::chrono::seconds interval, std::function<void()> report)
        : thread_([this, interval, report = std::move(report)] {
              std::unique_lock lock(mutex_);
              while (!wake_.wait_for(lock, interval, [&] { return done_; })) {
                  lock.unlock();
                  report();
                  lock.lock();
              }
          }) {}
    ~Progress() {
        { std::lock_guard lock(mutex_); done_ = true; }
        wake_.notify_all();
        thread_.join();
    }
    Progress(Progress const&) = delete;
    Progress& operator=(Progress const&) = delete;
};
}
