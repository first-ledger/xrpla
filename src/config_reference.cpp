// The configuration reference (config_reference.hpp). In config/xrpla.example.toml, a section
// line is `[name]` or `#[name]`, a setting line is `name = value` or `#name = value` (no space
// after the #), and every other line is documentation, kept as it is.
#include "config_reference.hpp"
#include <xrpla/archive.hpp>
#include <algorithm>
#include <sstream>

namespace xrpla {
namespace {
// CMake writes config/xrpla.example.toml's bytes here (CMakeLists.txt).
constexpr unsigned char reference_bytes[] = {
#include "config_reference.inc"
};
struct Line {
    enum Kind { Text, Section, Setting } kind = Text;
    std::string section, name, value, text;
};
bool name_char(char c) { return (c >= 'a' && c <= 'z') || c == '_' || (c >= '0' && c <= '9'); }
std::vector<Line> reference_lines() {
    std::vector<Line> lines;
    std::istringstream in{std::string(config_reference())};
    std::string text, section;
    while (std::getline(in, text)) {
        if (!text.empty() && text.back() == '\r') text.pop_back();
        Line line{Line::Text, section, {}, {}, text};
        std::string_view rest = text;
        if (rest.starts_with('#')) rest.remove_prefix(1);
        if (rest.size() > 2 && rest.front() == '[' && rest.back() == ']' && std::all_of(rest.begin() + 1, rest.end() - 1, name_char)) {
            section = std::string(rest.substr(1, rest.size() - 2));
            line = {Line::Section, section, {}, {}, text};
        } else if (!rest.empty() && name_char(rest.front()) && rest.front() != '_') {
            auto equals = rest.find(" = ");
            if (equals != std::string_view::npos && std::all_of(rest.begin(), rest.begin() + static_cast<std::ptrdiff_t>(equals), name_char))
                line = {Line::Setting, section, std::string(rest.substr(0, equals)), std::string(rest.substr(equals + 3)), text};
        }
        lines.push_back(std::move(line));
    }
    return lines;
}
std::string path_of(Line const& line) { return line.section.empty() ? line.name : line.section + "." + line.name; }
}

std::string_view config_reference() { return {reinterpret_cast<char const*>(reference_bytes), sizeof(reference_bytes) - 1}; }

std::set<std::string> const& documented_settings() {
    static std::set<std::string> const settings = [] {
        std::set<std::string> out;
        for (auto const& line : reference_lines()) if (line.kind == Line::Setting) out.insert(path_of(line));
        return out;
    }();
    return settings;
}

std::vector<std::string> unknown_settings(toml::table const& config) {
    std::vector<std::string> out;
    auto const& known = documented_settings();
    for (auto const& [key, value] : config) {
        std::string name(key.str());
        if (auto table = value.as_table()) {
            for (auto const& [inner, ignored] : *table) if (!known.contains(name + "." + std::string(inner.str()))) out.push_back(name + "." + std::string(inner.str()));
        } else if (!known.contains(name)) out.push_back(name);
    }
    return out;
}

std::string documented_config(toml::table const& config) {
    for (auto const& name : unknown_settings(config)) throw Error("Setting " + name + " is not in the configuration reference");
    std::ostringstream out;
    auto format = [](toml::node const& node) { std::ostringstream text; text << toml::node_view<toml::node const>(node); return text.str(); };
    for (auto const& line : reference_lines()) {
        if (line.kind == Line::Section) {
            out << (config.contains(line.section) ? "" : "#") << "[" << line.section << "]\n";
        } else if (line.kind == Line::Setting) {
            auto node = config.at_path(path_of(line));
            if (node) out << line.name << " = " << format(*node.node()) << "\n";
            else out << "#" << line.name << " = " << line.value << "\n";
        } else out << line.text << "\n";
    }
    return out.str();
}
}
