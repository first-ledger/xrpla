// The cache's `.verified` list kept to the objects present (and read once under concurrent
// downloads), remote reads ended by a stop request, and manifests read with either signature
// form without parsing them before their signature checks out (transport.cpp, archive.cpp).
#include <xrpla/archive.hpp>
#include <gtest/gtest.h>
#include <sodium.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <fstream>
#include <stop_token>
#include <thread>
#include <vector>

using namespace xrpla;
namespace {
fs::path fresh_directory(std::string const& name) {
    std::array<unsigned char, 8> random{};
    randombytes_buf(random.data(), random.size());
    auto root = fs::temp_directory_path() / ("xrpla-" + name + "-" + sha256(std::string(reinterpret_cast<char*>(random.data()), random.size())).substr(0, 16));
    fs::create_directories(root);
    return root;
}
struct Cleanup { fs::path root; ~Cleanup() { std::error_code ec; fs::remove_all(root, ec); } };
std::vector<std::string> lines(fs::path const& file) {
    std::ifstream in(file, std::ios::binary);
    std::vector<std::string> out;
    for (std::string line; std::getline(in, line);) out.push_back(line);
    return out;
}
// Public, deterministic TEST ONLY keys. Never use for real archives.
struct Signer {
    std::string key, secret;
    explicit Signer(unsigned char tag = 0) {
        initialize();
        std::array<unsigned char, crypto_sign_SEEDBYTES> seed{};
        seed[0] = tag;
        std::array<unsigned char, crypto_sign_PUBLICKEYBYTES> pk{};
        std::array<unsigned char, crypto_sign_SECRETKEYBYTES> sk{};
        crypto_sign_seed_keypair(pk.data(), sk.data(), seed.data());
        key.assign(reinterpret_cast<char const*>(pk.data()), pk.size());
        secret.assign(reinterpret_cast<char const*>(sk.data()), sk.size());
    }
    std::string sign(std::string const& bytes) const {
        std::string signature(crypto_sign_BYTES, '\0');
        crypto_sign_detached(reinterpret_cast<unsigned char*>(signature.data()), nullptr,
            reinterpret_cast<unsigned char const*>(bytes.data()), bytes.size(),
            reinterpret_cast<unsigned char const*>(secret.data()));
        return signature;
    }
};
std::string manifest_bytes(bool named) {
    Json manifest{{"format_version", 1}, {"chunk_width", 65536}, {"published_at", "2026-10-19T00:00:00Z"},
                  {"tip", {{"seq", 1}, {"ledger_hash", std::string(64, 'b')}}},
                  {"dicts", Json::array()}, {"chunks", Json::array()}, {"snapshots", Json::array()}};
    if (named) manifest["signature_format"] = "sha256-path-v1";
    return manifest.dump();
}
}

TEST(Transport, VerifiedListKeepsOnlyObjectsStillCached) {
    auto dir = fresh_directory("verified-prune");
    Cleanup cleanup{dir};
    auto source = dir / "source", cache = dir / "cache";
    fs::create_directories(source / "pieces");
    fs::create_directories(cache / "pieces");
    std::string data(4096, 'a');
    write_file(source / "pieces" / "a.bin", data);
    write_file(cache / "pieces" / "kept.bin", "kept");
    // A list left by earlier syncs: a line for an object since removed, two for one rewritten
    // (the later one current), a line no key could name, and a line without a stamp.
    write_file(cache / ".verified", "pieces/gone.bin " + std::string(64, '0') + " 4 1\n"
                                    "pieces/kept.bin " + std::string(64, '1') + " 4 1\n"
                                    "pieces/kept.bin " + std::string(64, '2') + " 4 2\n"
                                    "../outside.bin " + std::string(64, '3') + " 4 3\n"
                                    "junk\n");
    Object object{"pieces/a.bin", data.size(), sha256(data)};
    Source(source.string()).download(object, cache);
    // Read once, the list was rewritten with the newest line of each object still present; the
    // download was appended after it.
    auto kept = lines(cache / ".verified");
    ASSERT_EQ(kept.size(), 2U);
    EXPECT_EQ(kept[0], "pieces/kept.bin " + std::string(64, '2') + " 4 2");
    EXPECT_TRUE(kept[1].starts_with("pieces/a.bin " + object.sha256 + " 4096 "));
    EXPECT_FALSE(fs::exists(cache / ".verified.part"));
    // Recorded, the download is not hashed again: a matching file in place is taken as is.
    Source(source.string()).download(object, cache);
    EXPECT_EQ(lines(cache / ".verified").size(), 2U);
}

TEST(Transport, VerifiedListReadOnceUnderConcurrentDownloads) {
    auto dir = fresh_directory("verified-concurrent");
    Cleanup cleanup{dir};
    auto source = dir / "source", cache = dir / "cache";
    fs::create_directories(source / "pieces");
    fs::create_directories(cache / "pieces");
    write_file(cache / "pieces" / "kept.bin", "kept");
    write_file(cache / ".verified", "pieces/gone.bin " + std::string(64, '0') + " 4 1\n"
                                    "pieces/kept.bin " + std::string(64, '1') + " 4 1\n");
    std::vector<Object> objects;
    for (char c = 'a'; c <= 'p'; ++c) {
        std::string data(4096, c);
        auto name = std::string("pieces/") + c + ".bin";
        write_file(source / name, data);
        objects.push_back({name, data.size(), sha256(data)});
    }
    // Downloads on many threads at once into a root none has read yet: its list is read and
    // compacted once (the files it names checked without the lock), the others waiting for it,
    // and each download is appended after it.
    std::atomic<int> failures{0};
    {
        std::vector<std::jthread> threads;
        for (auto const& object : objects)
            threads.emplace_back([&, object] { try { Source(source.string()).download(object, cache); } catch (...) { ++failures; } });
    }
    EXPECT_EQ(failures.load(), 0);
    auto kept = lines(cache / ".verified");
    ASSERT_EQ(kept.size(), objects.size() + 1);
    EXPECT_EQ(kept[0], "pieces/kept.bin " + std::string(64, '1') + " 4 1");
    for (auto const& object : objects)
        EXPECT_EQ(std::count_if(kept.begin(), kept.end(), [&](std::string const& line) { return line.starts_with(object.object + ' ' + object.sha256 + " 4096 "); }), 1) << object.object;
}

TEST(Transport, VerifiedListReadAgainAfterAFailedRead) {
    auto dir = fresh_directory("verified-retry");
    Cleanup cleanup{dir};
    auto source = dir / "source", cache = dir / "cache";
    fs::create_directories(source / "pieces");
    fs::create_directories(cache / "pieces");
    std::string data(4096, 'a');
    write_file(source / "pieces" / "a.bin", data);
    write_file(cache / "pieces" / "kept.bin", "kept");
    write_file(cache / ".verified", "pieces/gone.bin " + std::string(64, '0') + " 4 1\n"
                                    "pieces/kept.bin " + std::string(64, '1') + " 4 1\n");
    // The first read's compaction cannot write its list (its partial file is a symlink): the
    // download fails, and the list is left unread rather than empty.
    write_file(dir / "elsewhere", "");
    fs::create_symlink(dir / "elsewhere", cache / ".verified.part");
    Object object{"pieces/a.bin", data.size(), sha256(data)};
    EXPECT_THROW(Source(source.string()).download(object, cache), Error);
    fs::remove(cache / ".verified.part");
    // The next use reads it again: its valid line is kept and the removed object's dropped, the
    // download appended after it.
    Source(source.string()).download(object, cache);
    auto kept = lines(cache / ".verified");
    ASSERT_EQ(kept.size(), 2U);
    EXPECT_EQ(kept[0], "pieces/kept.bin " + std::string(64, '1') + " 4 1");
    EXPECT_TRUE(kept[1].starts_with("pieces/a.bin " + object.sha256 + " 4096 "));
}

TEST(Transport, StoppedReadEndsWithoutWaitingForItsRetries) {
    // Nothing listens there: each try fails at once, and the read would be tried for minutes.
    Source source("http://127.0.0.1:9/v1");
    std::stop_source stopped;
    stopped.request_stop();
    EXPECT_THROW(source.read("manifest.json", 64, stopped.get_token()), Error);
    // A stop requested from another thread ends the wait between tries.
    std::stop_source later;
    auto started = std::chrono::steady_clock::now();
    std::jthread stopper([&] { std::this_thread::sleep_for(std::chrono::milliseconds(300)); later.request_stop(); });
    EXPECT_THROW(source.read("manifest.json", 64, later.get_token()), Error);
    EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds(30));
}

TEST(Manifest, ReadWithEitherSignatureFormWithoutParsingFirst) {
    Signer signer;
    auto dir = fresh_directory("manifest-forms");
    Cleanup cleanup{dir};
    // Named form: the signature stored under the manifest's hash.
    {
        auto root = dir / "named";
        fs::create_directories(root / "signatures");
        auto bytes = manifest_bytes(true);
        write_file(root / "manifest.json", bytes);
        write_file(root / "signatures" / (sha256(bytes) + ".sig"), signer.sign(bytes));
        auto read = read_manifest(Source(root.string()), signer.key);
        EXPECT_EQ(read.bytes, bytes);
        EXPECT_EQ(read.manifest.tip, 1U);
    }
    // Legacy form: the signature beside the manifest.
    {
        auto root = dir / "legacy";
        fs::create_directories(root);
        auto bytes = manifest_bytes(false);
        write_file(root / "manifest.json", bytes);
        write_file(root / "manifest.json.sig", signer.sign(bytes));
        EXPECT_EQ(read_manifest(Source(root.string()), signer.key).bytes, bytes);
    }
    // A named manifest whose named signature is gone, with a valid one beside it: the bytes are
    // what is signed, wherever the signature was stored.
    {
        auto root = dir / "named-beside";
        fs::create_directories(root);
        auto bytes = manifest_bytes(true);
        write_file(root / "manifest.json", bytes);
        write_file(root / "manifest.json.sig", signer.sign(bytes));
        EXPECT_EQ(read_manifest(Source(root.string()), signer.key).bytes, bytes);
    }
    // No signature, a signature by another key, or bytes that are not JSON at all (refused by
    // their signature, never parsed): refused.
    {
        auto root = dir / "refused";
        fs::create_directories(root / "signatures");
        auto bytes = manifest_bytes(true);
        write_file(root / "manifest.json", bytes);
        EXPECT_THROW(read_manifest(Source(root.string()), signer.key), Error);
        write_file(root / "signatures" / (sha256(bytes) + ".sig"), Signer(1).sign(bytes));
        EXPECT_THROW(read_manifest(Source(root.string()), signer.key), Error);
        std::string garbage(1 << 20, '[');
        write_file(root / "manifest.json", garbage);
        write_file(root / "manifest.json.sig", Signer(1).sign(garbage));
        EXPECT_THROW(read_manifest(Source(root.string()), signer.key), Error);
    }
}
