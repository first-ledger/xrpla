#pragma once
// The documented configuration: config/xrpla.example.toml, compiled in.
#include <toml++/toml.h>
#include <set>
#include <string>
#include <string_view>
#include <vector>

namespace xrpla {
// The reference text.
std::string_view config_reference();
// Every documented setting as "section.name" ("profile" for the top-level one).
std::set<std::string> const& documented_settings();
// The reference with `config`'s sections and settings filled in; every other setting stays as a
// commented line with its default. Throws if `config` has a setting the reference lacks.
std::string documented_config(toml::table const& config);
// The settings in `config` the reference does not document, as "section.name".
std::vector<std::string> unknown_settings(toml::table const& config);
}
