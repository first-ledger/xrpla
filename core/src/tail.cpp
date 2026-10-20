#include "store.hpp"
#include <xrpla/core/operations.hpp>
#include <chrono>
#include <csignal>
#include <iostream>
#include <thread>

namespace xrpla::core {
namespace {
volatile std::sig_atomic_t stopping = 0;
void stop(int) { stopping = 1; }
}
Json tail(TailOptions const& options) {
    if (!options.interval_seconds || options.interval_seconds > 3600) throw Error("Tail interval must be between 1 and 3600 seconds");
    Source source(options.fetch.archive);
    auto key = pinned_publisher_key(options.fetch.publisher_key);
    auto old_int = std::signal(SIGINT, stop), old_term = std::signal(SIGTERM, stop);
    stopping = 0;
    struct Restore { decltype(old_int) a, b; ~Restore() { std::signal(SIGINT, a); std::signal(SIGTERM, b); } } restore{old_int, old_term};
    std::uint64_t added = 0;
    Json latest;
    do {
        try {
            auto manifest = read_manifest(source, key).manifest;
            auto fetch_options = options.fetch;
            bool needed = true;
            if (fs::exists(fetch_options.dir / "store" / "CURRENT")) {
                latest = store_status(fetch_options.dir);
                if (!latest.at("complete_ledgers").is_null()) {
                    auto const& range = latest.at("complete_ledgers");
                    auto last = range.at("last").get<std::uint32_t>();
                    if (manifest.tip < last || (manifest.tip == last && manifest.tip_hash != latest.at("last_ledger_hash").get<std::string>())) throw Error("Archive tip conflicts with stored history");
                    needed = manifest.tip > last;
                    fetch_options.range = std::to_string(range.at("first").get<std::uint32_t>()) + "-" + std::to_string(manifest.tip);
                }
            }
            if (needed) {
                if (fetch_options.range.empty()) throw Error("Initial tail requires --range");
                fetch(fetch_options);
                auto result = build_store({fetch_options.dir, fetch_options.publisher_key, {}});
                added += result.at("ledgers_added").get<std::uint64_t>(); latest = result.at("store");
                if (options.state_window) {
                    auto const& range = latest.at("complete_ledgers"); auto last = range.at("last").get<std::uint32_t>();
                    auto first = std::max(range.at("first").get<std::uint32_t>(), last >= options.state_window ? last - options.state_window + 1 : 1);
                    prune_store(fetch_options.dir, first); latest = store_status(fetch_options.dir);
                }
                if (!options.once) std::cout << Json{{"event", "ledgers_added"}, {"added", result.at("ledgers_added")}, {"store", latest}}.dump() << std::endl;
            }
        } catch (std::exception const& error) {
            if (options.once) throw;
            std::cerr << Json{{"event", "tail_retry"}, {"error", error.what()}}.dump() << std::endl;
        }
        if (options.once) break;
        for (unsigned tick = 0; !stopping && tick < options.interval_seconds * 10; ++tick) std::this_thread::sleep_for(std::chrono::milliseconds{100});
    } while (!stopping);
    return {{"ledgers_added", added}, {"store", latest}, {"stopped", bool(stopping)}};
}
}
