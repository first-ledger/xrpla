#pragma once
// The approved publication list (config/publications.toml, docs/reference/publications.md): the
// publications a user can choose by name, each with its publisher's key and its mirrors.
#include <xrpla/archive.hpp>
#include <optional>
#include <utility>

namespace xrpla {
struct Publication {
    std::string name, network;
    std::string publisher_key;  // 64 hexadecimal characters
    Mirrors mirrors;  // HTTPS base URLs, tried in order
    std::optional<std::uint32_t> first_ledger;
    std::string description, homepage, contact;
};
struct PublicationList {
    fs::path file;  // where it was read from; empty when no list was found
    std::vector<Publication> entries;  // the valid ones, in the file's order
    std::vector<std::pair<std::string, std::string>> invalid;  // each entry left out (its name or place) and why
    // The entry named `name`; an Error saying why when there is none to use.
    Publication const& find(std::string const& name) const;
    // What `xrpla publications` prints.
    Json describe() const;
};
// The installed list: the file XRPLA_PUBLICATIONS names if set; else share/xrpla/publications.toml
// beside the program (BIN/../share); else, for a program run from its build directory, the
// source tree's config/publications.toml. Empty when there is none.
fs::path publication_list_file();
// A list's entries, each checked; an entry that is not valid is left out and named in `invalid`.
// A file that is not TOML, or not a list of [[publication]] tables, is an Error.
PublicationList read_publications(fs::path const& file);
// The installed list (no entries when there is none).
PublicationList installed_publications();
// The publication a configuration (or a command line) names: `name` from the installed list, or
// `source` and `publisher_key` (a file, or the key as 64 hexadecimal characters). With a name,
// a `source` given replaces the listed mirrors (a mirror of one's own, still checked against
// the listed key), and a `publisher_key` given must be the listed key.
struct ChosenPublication { Mirrors source; fs::path publisher_key; };
ChosenPublication choose_publication(std::string const& name, Mirrors source, fs::path const& publisher_key);
}
