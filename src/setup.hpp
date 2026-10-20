#pragma once
#include <xrpla/archive.hpp>
#include <iosfwd>

namespace xrpla {
// Carries out the profile in a config file written by `setup` (see setup.cpp).
int run_config(fs::path const& config);
// Asks which profile this machine runs and its settings, writes the config (and a systemd unit
// for persistent profiles into `unit_dir`), and can start a restore right away.
int setup(fs::path const& config, fs::path const& unit_dir, std::istream& in, std::ostream& out);
}
