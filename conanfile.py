from conan import ConanFile
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import copy
import os


class XrplaConan(ConanFile):
    name = "xrpla"
    version = "0.4.1"
    settings = "os", "compiler", "build_type", "arch"
    options = {"with_libxrpl": [True, False]}
    default_options = {"with_libxrpl": True, "*:shared": False,
                       "xrpl/*:tests": False, "xrpl/*:xrpld": False, "xrpl/*:benchmark": False,
                       "xrpl/*:rocksdb": True}
    exports_sources = "CMakeLists.txt", "include/*", "src/*", "core/*", "tests/*", "config/*", "tools/*", "licenses/*"

    def requirements(self):
        self.requires("libcurl/8.12.1")
        self.requires("libsodium/1.0.20")
        self.requires("nlohmann_json/3.11.3")
        # The history tools hash with it (tools/history_reader.hpp), so its headers must be
        # ours and not only libcurl's; forced, as libxrpl and grpc ask for ranges.
        self.requires("openssl/3.6.3", force=True)
        self.requires("tomlplusplus/3.4.0")
        self.requires("zstd/1.5.7")
        # The executables' allocator on Linux: glibc's malloc held a restore's SQL thread
        # in free() (rows are built on pool threads) and kept freed memory; jemalloc ran the
        # same restore 22% faster in a third less memory.
        if self.settings.os == "Linux":
            self.requires("jemalloc/5.3.0")
        if self.options.with_libxrpl:
            self.requires("xrpl/3.4.1#c863587f4c25d5984ae70baf145c7a5c")
            self.requires("rocksdb/10.5.1")
            self.requires("nudb/2.0.9")
            self.requires("sqlite3/3.53.0")
            self.requires("grpc/1.81.1")
            self.requires("protobuf/6.33.5")

    def build_requirements(self):
        self.test_requires("gtest/1.14.0")

    def layout(self):
        cmake_layout(self)
        self.folders.generators = "generators"

    def generate(self):
        for dependency in self.dependencies.host.values():
            if dependency.package_folder:
                copy(self, '*', src=os.path.join(dependency.package_folder, 'licenses'),
                     dst=os.path.join(self.generators_folder, 'licenses', str(dependency.ref.name)))
        CMakeDeps(self).generate()
        tc = CMakeToolchain(self)
        tc.variables["XRPLA_WITH_LIBXRPL"] = bool(self.options.with_libxrpl)
        tc.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
        cmake.ctest(cli_args=["--output-on-failure"])

    def package(self):
        CMake(self).install()
