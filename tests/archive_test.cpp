#include <xrpla/archive.hpp>
#include <gtest/gtest.h>
#include <sodium.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <iterator>

using namespace xrpla;
namespace {
struct Fixture {
    std::string key, secret;
    Json manifest, index;
    std::vector<std::pair<Object, std::string>> files;
    Fixture() {
        initialize();
        std::array<unsigned char, crypto_sign_SEEDBYTES> seed{};
        std::array<unsigned char, crypto_sign_PUBLICKEYBYTES> pk{};
        std::array<unsigned char, crypto_sign_SECRETKEYBYTES> sk{};
        // Public, deterministic TEST ONLY key. Never use for real archives.
        crypto_sign_seed_keypair(pk.data(), sk.data(), seed.data());
        key.assign(reinterpret_cast<char const*>(pk.data()), pk.size());
        secret.assign(reinterpret_cast<char const*>(sk.data()), sk.size());
        auto prefix = "snapshots/65535.0123456789abcdef";
        auto part = add(std::string(prefix) + "/part-0000.xrps", "synthetic snapshot bytes");
        index = {{"seq", 65535}, {"account_hash", std::string(64, 'a')}, {"parts", Json::array({part})}};
        auto idx = add(std::string(prefix) + "/index.json", index.dump());
        manifest = {{"format_version", 1}, {"chunk_width", 65536},
            {"published_at", "2026-09-15T00:00:00Z"},
            {"tip", {{"seq", 196607}, {"ledger_hash", std::string(64, 'b')}}},
            {"dicts", Json::array()}, {"chunks", Json::array()},
            {"snapshots", Json::array({{{"seq", 65535}, {"prefix", prefix},
                {"account_hash", std::string(64, 'a')}, {"index", idx}, {"parts", 1},
                {"total_size", part["size"]}}})}};
        for (std::uint32_t first : {65536U, 131072U}) {
            std::string data(256 * 1024, static_cast<char>(first / 65536));
            auto hash = sha256(data);
            auto c = add("chunks/" + std::to_string(first) + "-" + std::to_string(first + 65535) +
                "." + hash.substr(0, 16) + ".xrpa", data);
            c["first"] = first;
            c["last"] = first + 65535;
            c["ledger_hash_of_last"] = std::string(64, 'b');
            c["amendments"] = Json::array();
            manifest["chunks"].push_back(c);
        }
    }
    Json add(std::string path, std::string bytes) {
        Object ref{path, bytes.size(), sha256(bytes)};
        files.emplace_back(ref, bytes);
        return {{"object", ref.object}, {"size", ref.size}, {"sha256", ref.sha256}};
    }
    std::string sign(std::string const& bytes) const {
        std::string signature(crypto_sign_BYTES, '\0');
        crypto_sign_detached(reinterpret_cast<unsigned char*>(signature.data()), nullptr,
            reinterpret_cast<unsigned char const*>(bytes.data()), bytes.size(),
            reinterpret_cast<unsigned char const*>(secret.data()));
        return signature;
    }
    Manifest parsed() const { auto bytes = manifest.dump(); return authenticate_manifest(bytes, sign(bytes), key); }
};
// `f` as a v2 archive written to `source`: every chunk with a time index of the close times
// `close` gives its ledgers.
void write_day_archive(Fixture& f, fs::path const& source, std::function<std::uint32_t(std::uint32_t)> const& close) {
    f.manifest["format_version"] = 2;
    for (auto& chunk : f.manifest["chunks"]) {
        std::string bytes;
        auto append = [&](std::uint32_t value) { for (int s = 24; s >= 0; s -= 8) bytes += static_cast<char>(value >> s); };
        auto first = chunk["first"].get<std::uint32_t>(), last = chunk["last"].get<std::uint32_t>();
        for (auto seq = first; seq <= last; ++seq) { append(seq); append(close(seq)); }
        auto path = "times/" + std::to_string(first) + "-" + std::to_string(last) + "." + sha256(bytes).substr(0, 16) + ".xrpt";
        chunk["time_index"] = f.add(path, bytes);
        chunk["first_close_time"] = close(first); chunk["last_close_time"] = close(last);
    }
    fs::create_directories(source);
    for (auto const& [object, bytes] : f.files) { fs::create_directories((source / object.object).parent_path()); write_file(source / object.object, bytes); }
    auto bytes = f.manifest.dump(); write_file(source / "manifest.json", bytes);
    write_file(source / "manifest.json.sig", f.sign(bytes)); write_file(source / "publisher.pub", f.key);
}
} // namespace

TEST(Crypto, KnownSha256Vector) {
    initialize();
    EXPECT_EQ(sha256("abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
}
TEST(Manifest, AcceptsAuthenticBytes) {
    Fixture f;
    EXPECT_EQ(f.parsed().tip, 196607U);
}
TEST(Manifest, TamperingAndWrongKeyFailBeforeParsing) {
    Fixture f;
    auto bytes = f.manifest.dump();
    EXPECT_THROW(authenticate_manifest("not JSON", f.sign(bytes), f.key), Error);
    auto key = f.key; key[0] ^= 1;
    EXPECT_THROW(authenticate_manifest(bytes, f.sign(bytes), key), Error);
    EXPECT_THROW(authenticate_manifest(bytes, "short", f.key), Error);
}
TEST(Manifest, RejectsUnknownVersionAndWidth) {
    Fixture f;
    f.manifest["format_version"] = 2;
    EXPECT_EQ(f.parsed().version, 2);
    f.manifest["format_version"] = 3;
    EXPECT_THROW(f.parsed(), Error);
    f.manifest["format_version"] = 1;
    f.manifest["chunk_width"] = 64;
    EXPECT_THROW(f.parsed(), Error);
}
TEST(Manifest, RejectsDuplicateJsonKeys) {
    Fixture f;
    auto text = f.manifest.dump();
    text.insert(1, "\"format_version\":1,");
    EXPECT_THROW(authenticate_manifest(text, f.sign(text), f.key), Error);
}
TEST(Manifest, RejectsNegativeFractionalAndOverflowIntegers) {
    for (Json n : {Json(-1), Json(1.5), Json(4294967296ULL)}) {
        Fixture f; f.manifest["tip"]["seq"] = n;
        EXPECT_THROW(f.parsed(), Error);
    }
}
TEST(Manifest, RejectsOverlapAndUnalignedChunks) {
    Fixture f;
    f.manifest["chunks"].push_back(f.manifest["chunks"][0]);
    EXPECT_THROW(f.parsed(), Error);
    f.manifest["chunks"].erase(2);
    f.manifest["chunks"][0]["first"] = 65537;
    EXPECT_THROW(f.parsed(), Error);
}
TEST(Paths, RejectsTraversalSchemesAndWindowsPaths) {
    for (auto const* path : {"../secret", "/absolute", "a//b", "a/../b", "a/./b", "a\\b",
        "https://evil.example/a", "a%2fb", "c:foo", "a/", "a/nul", "a/com1.txt", "a/b.",
        // A cache's own files: no published object's key has a component starting with '.'.
        ".verified", ".xrpla-lock", ".staging/0123.part", "pieces/.hidden.xrpp", "..", "."})
        EXPECT_THROW(validate_key(path), Error) << path;
    EXPECT_NO_THROW(validate_key("snapshots/65535.abcd/part-0000.xrps"));
    for (auto const* path : {"manifest.json.sig", "catalog/current.json", "fragments/1-250.0123456789abcdef.xrpp"})
        EXPECT_NO_THROW(validate_key(path)) << path;
}
TEST(Range, ParsesLatestAndRejectsMalformed) {
    EXPECT_EQ(parse_range("latest-100", 1000).first, 901U);
    for (auto const* range : {"0-10", "10-1", "1-1001", "latest-0", "latest-1001", "90d", "1-2junk", "1-4294967296"})
        EXPECT_THROW(parse_range(range, 1000), Error) << range;
}
TEST(Planner, IncludesReplayPrerequisitesAndBothBoundaryChunks) {
    Fixture f;
    auto plan = make_plan(f.parsed(), {131071, 131072});
    EXPECT_EQ(plan.snapshot.seq, 65535U);
    EXPECT_EQ(plan.objects.size(), 3U); // index plus both chunks
}
TEST(Planner, MissingPredecessorAndGapFail) {
    Fixture f;
    EXPECT_THROW(make_plan(f.parsed(), {65535, 65536}), Error);
    f.manifest["chunks"].erase(0);
    EXPECT_THROW(make_plan(f.parsed(), {131072, 131073}), Error);
}
TEST(Snapshot, AuthenticatesIndexBeforeReadingPartReferences) {
    Fixture f;
    auto snap = f.parsed().snapshots[0];
    EXPECT_EQ(snapshot_parts(snap, f.index.dump()).size(), 1U);
    EXPECT_THROW(snapshot_parts(snap, f.index.dump() + " "), Error);
    f.index["parts"][0]["object"] = "../outside";
    auto text = f.index.dump(); snap.index.sha256 = sha256(text); snap.index.size = text.size();
    EXPECT_THROW(snapshot_parts(snap, text), Error);
}
TEST(Transport, RejectsInsecureOrCredentialedOrigins) {
    EXPECT_THROW(Source("http://example.com/v1"), Error);
    EXPECT_THROW(Source("file:///etc"), Error);
    EXPECT_THROW(Source("https://user:password@example.com/v1"), Error);
    EXPECT_THROW(Source("http://127.0.0.1:8000@evil.example/"), Error);
    EXPECT_NO_THROW(Source("http://127.0.0.1:8000/v1"));
}
TEST(Transport, CachedObjectsAreHashedAgainOnlyOnceChanged) {
    auto dir = fs::temp_directory_path() / ("xrpla-verified-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    auto source = dir / "source", cache = dir / "cache";
    fs::create_directories(source / "pieces");
    std::string data(4096, 'x');
    { std::ofstream out(source / "pieces" / "a.bin", std::ios::binary); out << data; }
    Object object{"pieces/a.bin", data.size(), sha256(data)};
    auto stamps = [&] { std::ifstream in(cache / ".verified"); return std::count(std::istreambuf_iterator<char>(in), {}, '\n'); };
    auto cached = cache / "pieces" / "a.bin";
    auto rewrite = [&](char c) { std::ofstream out(cached, std::ios::binary); out << std::string(4096, c); };
    Source(source.string()).download(object, cache);
    // Hashed as it arrived, a download is recorded at once, its time set a few seconds back:
    // the next sync does not read it again.
    EXPECT_EQ(stamps(), 1);
    EXPECT_LE(fs::last_write_time(cached), fs::file_time_type::clock::now() - std::chrono::seconds(5));
    Source(source.string()).download(object, cache);
    EXPECT_EQ(stamps(), 1);
    // Rewritten just after (other bytes, same size), its time moved on: hashed again and refused.
    rewrite('y');
    EXPECT_THROW(Source(source.string()).download(object, cache), Error);
    // A file found in place and checked: written just now, it is not recorded (a rewrite within
    // the same timestamp tick would look unchanged); once its time is a few seconds old, it is.
    rewrite('x');
    Source(source.string()).download(object, cache);
    EXPECT_EQ(stamps(), 1);
    auto old = fs::file_time_type::clock::now() - std::chrono::seconds(60);
    fs::last_write_time(cached, old);
    Source(source.string()).download(object, cache);
    EXPECT_EQ(stamps(), 2);
    // A sync finds it again: recorded as checked, it is not hashed nor recorded again.
    Source(source.string()).download(object, cache);
    EXPECT_EQ(stamps(), 2);
    // Rewritten since (other bytes, same size), it is hashed again and refused.
    rewrite('y');
    fs::last_write_time(cached, old + std::chrono::seconds(1));
    EXPECT_THROW(Source(source.string()).download(object, cache), Error);
    fs::remove_all(dir);
}
TEST(Transport, ResumedStagingThatDiffersIsFetchedAgainOnce) {
    auto dir = fs::temp_directory_path() / ("xrpla-resumed-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    auto source = dir / "source", cache = dir / "cache";
    fs::create_directories(source / "pieces"); fs::create_directories(cache / ".staging");
    std::string data(8192, '\0');
    for (std::size_t i = 0; i < data.size(); ++i) data[i] = static_cast<char>('a' + i % 26);
    { std::ofstream out(source / "pieces" / "a.bin", std::ios::binary); out << data; }
    Object object{"pieces/a.bin", data.size(), sha256(data)};
    // A crash left half the object staged, its last bytes zeros that never reached the disk:
    // resumed, the whole differs, and the object is fetched again from its start.
    { std::ofstream out(cache / ".staging" / (object.sha256 + ".part"), std::ios::binary); out << data.substr(0, 3072) << std::string(1024, '\0'); }
    Source(source.string()).download(object, cache);
    std::ifstream in(cache / "pieces" / "a.bin", std::ios::binary);
    EXPECT_EQ(std::string(std::istreambuf_iterator<char>(in), {}), data);
    EXPECT_FALSE(fs::exists(cache / ".staging" / (object.sha256 + ".part")));
    // An origin that serves other bytes is refused, its staging file removed.
    Object other{"pieces/a.bin", data.size(), sha256(data + "x")};
    EXPECT_THROW(Source(source.string()).download(other, dir / "other"), Error);
    EXPECT_FALSE(fs::exists(dir / "other" / ".staging" / (other.sha256 + ".part")));
    fs::remove_all(dir);
}
TEST(TimeIndex, HistoricalCloseTimesCanDecrease) {
    std::string bytes;
    auto append = [&](std::uint32_t value) { for (int s = 24; s >= 0; s -= 8) bytes += static_cast<char>(value >> s); };
    append(1000071); append(424159841);
    append(1000072); append(424159840);
    Chunk chunk; chunk.first = 1000071; chunk.last = 1000072;
    chunk.first_close_time = 424159841; chunk.last_close_time = 424159840;
    chunk.time_index = Object{"test", bytes.size(), sha256(bytes)};
    EXPECT_EQ(decode_times(chunk, bytes).size(), 2U);
    bytes[11] ^= 1; // Rehashed but non-contiguous sequence must still fail.
    chunk.time_index->sha256 = sha256(bytes);
    EXPECT_THROW(decode_times(chunk, bytes), Error);
}
TEST(TimeIndex, DayRangeFindsEarlierQualifyingLedgerAcrossTimeRegression) {
    Fixture f; f.manifest["format_version"] = 2;
    for (auto& chunk : f.manifest["chunks"]) {
        std::string bytes;
        auto append = [&](std::uint32_t value) { for (int s = 24; s >= 0; s -= 8) bytes += static_cast<char>(value >> s); };
        auto first = chunk["first"].get<std::uint32_t>(), last = chunk["last"].get<std::uint32_t>();
        auto close = [&](std::uint32_t seq) -> std::uint32_t {
            if (seq == 65536) return 113599;
            if (seq == 65537) return 113600; // Exactly one day before tip.
            return seq <= 131072 ? 100000 : 200000;
        };
        for (auto seq = first; seq <= last; ++seq) { append(seq); append(close(seq)); }
        auto path = "times/" + std::to_string(first) + "-" + std::to_string(last) + "." + sha256(bytes).substr(0, 16) + ".xrpt";
        chunk["time_index"] = f.add(path, bytes);
        chunk["first_close_time"] = close(first); chunk["last_close_time"] = close(last);
    }
    std::array<unsigned char, 8> random{}; randombytes_buf(random.data(), random.size());
    auto root = fs::temp_directory_path() / ("xrpla-time-" + sha256(std::string(reinterpret_cast<char*>(random.data()), random.size())).substr(0,16));
    struct Cleanup { fs::path root; ~Cleanup() { std::error_code ec; fs::remove_all(root, ec); } } cleanup{root};
    auto source = root / "source"; fs::create_directories(source);
    for (auto const& [object, bytes] : f.files) { fs::create_directories((source / object.object).parent_path()); write_file(source / object.object, bytes); }
    auto bytes = f.manifest.dump(); write_file(source / "manifest.json", bytes);
    write_file(source / "manifest.json.sig", f.sign(bytes)); write_file(source / "publisher.pub", f.key);
    auto result = fetch({source.string(), "1d", root / "data", source / "publisher.pub", 1});
    EXPECT_EQ(result.at("range").at("first"), 65537);
}
TEST(TimeIndex, DayRangeSearchByWorkersTakesTheEarliestChunkInOrder) {
    // Six chunks, none whose signed first close time qualifies: every index up to the one holding
    // the first qualifying ledger (in the fourth chunk) is read. The tip's chunk qualifies too.
    Fixture f;
    constexpr std::uint32_t width = 65536;
    for (std::uint32_t k = 3; k <= 6; ++k) {
        std::string data(1024, static_cast<char>(k));
        auto first = k * width;
        auto c = f.add("chunks/" + std::to_string(first) + "-" + std::to_string(first + width - 1) + "." + sha256(data).substr(0, 16) + ".xrpa", data);
        c["first"] = first; c["last"] = first + width - 1;
        c["ledger_hash_of_last"] = std::string(64, 'b'); c["amendments"] = Json::array();
        f.manifest["chunks"].push_back(c);
    }
    constexpr std::uint32_t tip = 7 * width - 1, qualifying = 4 * width + 10;
    f.manifest["tip"]["seq"] = tip;
    std::array<unsigned char, 8> random{}; randombytes_buf(random.data(), random.size());
    auto root = fs::temp_directory_path() / ("xrpla-days-" + sha256(std::string(reinterpret_cast<char*>(random.data()), random.size())).substr(0, 16));
    struct Cleanup { fs::path root; ~Cleanup() { std::error_code ec; fs::remove_all(root, ec); } } cleanup{root};
    auto source = root / "source";
    // One day before the tip's close time (200000) is 113600.
    write_day_archive(f, source, [&](std::uint32_t seq) -> std::uint32_t { return seq == tip ? 200000 : seq == qualifying ? 113600 : 100000; });
    for (unsigned jobs : {1u, 3u, 32u}) {
        auto result = fetch({source.string(), "1d", root / ("data-" + std::to_string(jobs)), source / "publisher.pub", jobs});
        EXPECT_EQ(result.at("range").at("first"), qualifying) << jobs;
    }
    // An index before the one found that fails its check fails the search (not only the download
    // after it), however many workers read ahead of it.
    auto times = source / f.manifest["chunks"][1]["time_index"]["object"].get<std::string>();
    auto bytes = read_file(times, 1 << 20);
    bytes[7] ^= 1;
    write_file(times, bytes);
    for (unsigned jobs : {1u, 8u}) {
        try {
            fetch({source.string(), "1d", root / ("corrupt-" + std::to_string(jobs)), source / "publisher.pub", jobs});
            ADD_FAILURE() << "A corrupt time index was accepted with " << jobs << " jobs";
        } catch (Error const& error) {
            EXPECT_NE(std::string(error.what()).find("Time index integrity check failed"), std::string::npos) << error.what();
        }
    }
}
TEST(Fixture, Export) {
    auto const* dest = std::getenv("XRPLA_TEST_FIXTURE_DIR");
    if (!dest) GTEST_SKIP() << "Synthetic transport fixture export requested by CLI integration suite only";
    Fixture f;
    fs::path root(dest);
    fs::create_directories(root);
    for (auto const& [object, bytes] : f.files) {
        fs::create_directories((root / object.object).parent_path());
        write_file(root / object.object, bytes);
    }
    auto bytes = f.manifest.dump();
    write_file(root / "manifest.json", bytes);
    write_file(root / "manifest.json.sig", f.sign(bytes));
    write_file(root / "publisher.pub", f.key);
}
