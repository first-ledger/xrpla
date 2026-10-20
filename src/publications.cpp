// The approved publication list (publications.hpp).
#include "publications.hpp"
#include <toml++/toml.h>
#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <set>

namespace xrpla {
namespace {
bool hex64(std::string const& text) {
    return text.size() == 64 && text.find_first_not_of("0123456789abcdefABCDEF") == std::string::npos;
}
std::string lowercase(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return text;
}
// A publisher key as lowercase hex, read as the publication readers read it (pinned_publisher_key).
std::string key_hex(fs::path const& key) {
    constexpr char digits[] = "0123456789abcdef";
    std::string text;
    for (unsigned char c : pinned_publisher_key(key)) { text += digits[c >> 4]; text += digits[c & 15]; }
    return text;
}
// An entry of the list, checked: throws an Error saying what is wrong with it.
Publication entry(toml::table const& table) {
    auto string = [&](char const* field, bool needed) {
        auto node = table[field];
        if (!node) {
            if (needed) throw Error(std::string("it has no ") + field);
            return std::string();
        }
        auto value = node.value_exact<std::string>();
        if (!value || (needed && value->empty())) throw Error(std::string(field) + " must be a string" + (needed ? " (not empty)" : ""));
        return *value;
    };
    Publication publication;
    publication.name = string("name", true);
    if (publication.name.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789-") != std::string::npos)
        throw Error("its name must be lowercase letters, digits and -");
    // setup offers the list's names beside these two.
    if (publication.name == "other" || publication.name == "none") throw Error("its name is one setup keeps for itself");
    publication.network = string("network", true);
    if (publication.network != "mainnet" && publication.network != "testnet" && publication.network != "devnet")
        throw Error("its network must be mainnet, testnet or devnet");
    publication.publisher_key = string("publisher_key", true);
    if (!hex64(publication.publisher_key)) throw Error("its publisher_key must be 64 hexadecimal characters");
    publication.publisher_key = lowercase(publication.publisher_key);
    auto const* mirrors = table["mirrors"].as_array();
    if (!mirrors || mirrors->empty()) throw Error("it has no mirrors");
    for (auto const& node : *mirrors) {
        auto mirror = node.value_exact<std::string>();
        if (!mirror || mirror->find("://") == std::string::npos) throw Error("each of its mirrors must be a URL");
        // As a sync takes it: HTTPS (HTTP only to this machine, for tests), nothing but a base.
        try { Source checked(*mirror); }
        catch (Error const& error) { throw Error("mirror " + *mirror + ": " + error.what()); }
        publication.mirrors.push_back(*mirror);
    }
    if (auto node = table["first_ledger"]) {
        auto first = node.value_exact<std::int64_t>();
        if (!first || *first < 1 || *first > UINT32_MAX) throw Error("its first_ledger must be a ledger sequence");
        publication.first_ledger = static_cast<std::uint32_t>(*first);
    }
    publication.description = string("description", false);
    publication.homepage = string("homepage", false);
    publication.contact = string("contact", false);
    return publication;
}
}

Publication const& PublicationList::find(std::string const& name) const {
    for (auto const& publication : entries) if (publication.name == name) return publication;
    if (file.empty()) throw Error("No publication list is installed, so no publication can be chosen by name (" + name + "); reinstall xrpla, or give the publication's source and publisher key");
    for (auto const& [entry, why] : invalid)
        if (entry == name) throw Error("Publication " + name + " in " + file.string() + " cannot be used: " + why);
    throw Error("No publication named " + name + " in " + file.string() + "; `xrpla publications` lists them");
}

Json PublicationList::describe() const {
    Json out{{"list", file.empty() ? Json() : Json(file.string())}, {"publications", Json::array()}};
    for (auto const& p : entries) {
        Json entry{{"name", p.name}, {"network", p.network}, {"first_ledger", p.first_ledger ? Json(*p.first_ledger) : Json()},
                   {"mirrors", p.mirrors}, {"publisher_key", p.publisher_key}};
        for (auto const& [field, value] : {std::pair{"description", &p.description}, {"homepage", &p.homepage}, {"contact", &p.contact}})
            if (!value->empty()) entry[field] = *value;
        out["publications"].push_back(entry);
    }
    for (auto const& [entry, why] : invalid) out["invalid"].push_back({{"entry", entry}, {"error", why}});
    return out;
}

fs::path publication_list_file() {
    if (auto* named = std::getenv("XRPLA_PUBLICATIONS"); named && *named) return named;
    std::error_code ec;
    auto exe = fs::read_symlink("/proc/self/exe", ec);
    if (ec) return {};
    if (auto installed = exe.parent_path().parent_path() / "share" / "xrpla" / "publications.toml"; fs::exists(installed)) return installed;
    // The source tree's list serves only a program run where it was built (the tests), as the
    // connected profile's tools do (setup.cpp).
    auto built = fs::weakly_canonical(XRPLA_BUILD_DIR, ec), inside = exe.lexically_relative(built);
    if (ec || inside.empty() || *inside.begin() == "..") return {};
    return fs::path(XRPLA_SOURCE_CONFIG) / "publications.toml";
}

PublicationList read_publications(fs::path const& file) {
    PublicationList list{file, {}, {}};
    if (!fs::is_regular_file(file)) throw Error("The publication list " + file.string() + " was not found");
    toml::table document;
    try { document = toml::parse_file(file.string()); }
    catch (toml::parse_error const& error) { throw Error("The publication list " + file.string() + " is not valid TOML: " + std::string(error.description())); }
    auto const* tables = document["publication"].as_array();
    if (document["publication"] && (!tables || !tables->is_array_of_tables()))
        throw Error("The publication list " + file.string() + " must hold [[publication]] tables");
    std::set<std::string> names;
    std::size_t place = 0;
    if (tables) for (auto const& node : *tables) {
        auto const& table = *node.as_table();
        auto name = table["name"].value_exact<std::string>().value_or("");
        auto label = name.empty() ? "entry " + std::to_string(place + 1) : name;
        ++place;
        try {
            auto publication = entry(table);
            if (!names.insert(publication.name).second) throw Error("another entry has its name");
            list.entries.push_back(std::move(publication));
        } catch (Error const& error) {
            list.invalid.emplace_back(label, error.what());
        }
    }
    return list;
}

PublicationList installed_publications() {
    auto file = publication_list_file();
    return file.empty() ? PublicationList{} : read_publications(file);
}

ChosenPublication choose_publication(std::string const& name, Mirrors source, fs::path const& publisher_key) {
    if (name.empty()) return {std::move(source), publisher_key};
    auto list = installed_publications();
    auto const& listed = list.find(name);
    if (!publisher_key.empty() && key_hex(publisher_key) != listed.publisher_key)
        throw Error("The publisher key given (" + publisher_key.string() + ") is not publication " + name + "'s; give the name alone, or "
                    "another publication's source and key without its name");
    return {source.empty() ? listed.mirrors : std::move(source), listed.publisher_key};
}
}
