cmake_minimum_required(VERSION 3.22)
project(xrpla VERSION 0.4.0 LANGUAGES CXX)

option(XRPLA_WITH_LIBXRPL "Build canonical verification, RocksDB storage, and queries" ON)
option(XRPLA_SYSTEM_DEPS "Use distro dependencies for transport-only development" OFF)
include(CTest)
find_package(Threads REQUIRED)
find_package(CURL REQUIRED)
find_package(nlohmann_json 3.10 REQUIRED)
find_package(tomlplusplus REQUIRED)
if(XRPLA_SYSTEM_DEPS)
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(SODIUM REQUIRED IMPORTED_TARGET libsodium)
  set(SODIUM_TARGET PkgConfig::SODIUM)
  pkg_check_modules(ZSTD REQUIRED IMPORTED_TARGET libzstd)
  set(ZSTD_TARGET PkgConfig::ZSTD)
else()
  find_package(libsodium REQUIRED CONFIG)
  set(SODIUM_TARGET libsodium::libsodium)
  find_package(zstd REQUIRED CONFIG)
  set(ZSTD_TARGET zstd::libzstd_static)
endif()

add_library(xrpla_archive src/manifest.cpp src/transport.cpp src/archive.cpp src/durability.cpp)
target_include_directories(xrpla_archive PUBLIC include)
target_compile_features(xrpla_archive PUBLIC cxx_std_20)
set_target_properties(xrpla_archive PROPERTIES CXX_EXTENSIONS OFF)
target_link_libraries(xrpla_archive PUBLIC nlohmann_json::nlohmann_json
  ${SODIUM_TARGET} CURL::libcurl Threads::Threads)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(xrpla_archive PRIVATE -Wall -Wextra -Wpedantic -Wconversion)
endif()
add_library(xrpla_format core/src/format.cpp)
target_include_directories(xrpla_format PUBLIC core/include)
target_link_libraries(xrpla_format PUBLIC xrpla_archive ${ZSTD_TARGET})
add_executable(xrpla src/main.cpp src/publications.cpp)
# The approved publication list is installed beside the other shared files; a binary run from its
# build directory reads the source tree's (src/publications.cpp).
target_compile_definitions(xrpla PRIVATE XRPLA_SOURCE_CONFIG="${CMAKE_CURRENT_SOURCE_DIR}/config" XRPLA_BUILD_DIR="${CMAKE_CURRENT_BINARY_DIR}")
if(UNIX)
  find_package(OpenSSL REQUIRED)
  add_executable(xrpla_history_decode tools/history_decode.cpp)
  target_compile_features(xrpla_history_decode PRIVATE cxx_std_20)
  target_link_libraries(xrpla_history_decode PRIVATE nlohmann_json::nlohmann_json
    ${SODIUM_TARGET} ${ZSTD_TARGET} OpenSSL::Crypto Threads::Threads)
  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_link_options(xrpla_history_decode PRIVATE -static-libstdc++ -static-libgcc)
  endif()
  add_executable(xrpla_tx_index tools/tx_index.cpp)
  target_compile_features(xrpla_tx_index PRIVATE cxx_std_20)
  target_link_libraries(xrpla_tx_index PRIVATE nlohmann_json::nlohmann_json
    ${SODIUM_TARGET} ${ZSTD_TARGET} OpenSSL::Crypto Threads::Threads)
  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_link_options(xrpla_tx_index PRIVATE -static-libstdc++ -static-libgcc)
  endif()
  add_executable(xrpla_history_encode tools/history_encode.cpp)
  target_compile_features(xrpla_history_encode PRIVATE cxx_std_20)
  target_link_libraries(xrpla_history_encode PRIVATE nlohmann_json::nlohmann_json
    ${SODIUM_TARGET} ${ZSTD_TARGET} OpenSSL::Crypto Threads::Threads)
  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_link_options(xrpla_history_encode PRIVATE -static-libstdc++ -static-libgcc)
  endif()
endif()
target_link_libraries(xrpla PRIVATE xrpla_archive tomlplusplus::tomlplusplus)
target_compile_definitions(xrpla_archive PUBLIC XRPLA_VERSION="${PROJECT_VERSION}")
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND UNIX)
  target_link_options(xrpla PRIVATE -static-libstdc++ -static-libgcc)
endif()
if(XRPLA_WITH_LIBXRPL)
  find_package(xrpl REQUIRED CONFIG)
  find_package(RocksDB REQUIRED CONFIG)
  find_package(nudb REQUIRED CONFIG)
  find_package(lz4 REQUIRED CONFIG)
  find_package(SQLite3 REQUIRED CONFIG)
  find_package(gRPC REQUIRED CONFIG)
  find_package(protobuf REQUIRED CONFIG)
  add_library(xrpla_core core/src/verify.cpp core/src/store.cpp core/src/store_read.cpp core/src/store_indexes.cpp core/src/pipeline.cpp core/src/rpc_client.cpp core/src/export.cpp core/src/rpc.cpp core/src/rpc_accounts.cpp core/src/rpc_books.cpp core/src/server.cpp core/src/import_rippled.cpp core/src/import_clio.cpp core/src/tail.cpp core/src/piece_history.cpp core/src/piece_index.cpp core/src/piece_feed.cpp core/src/restore.cpp core/src/validators.cpp core/src/ws_client.cpp core/src/live_history.cpp core/src/peer.cpp core/src/peer_ledgers.cpp core/src/peer_compare.cpp core/src/peer_relay.cpp core/src/peer_serving.cpp core/src/wire.cpp core/src/nudb_builder.cpp core/src/replay.cpp core/src/rpc_more.cpp core/src/rpc_owner.cpp core/src/rpc_entry.cpp core/src/rpc_clio.cpp core/src/sqlite_index.cpp)
  target_compile_features(xrpla_core PUBLIC cxx_std_23)
  target_sources(xrpla_core PRIVATE core/src/grpc_client.cpp core/src/compact.cpp)
  target_compile_definitions(xrpla_core PRIVATE XRPLA_SOURCE_TOOLS="${CMAKE_CURRENT_SOURCE_DIR}/tools" XRPLA_BUILD_DIR="${CMAKE_CURRENT_BINARY_DIR}")
  # The piece query backend reuses the XRPH reader from tools/.
  target_include_directories(xrpla_core PRIVATE tools)
  target_link_libraries(xrpla_core PUBLIC xrpla_format xrpl::libxrpl RocksDB::rocksdb SQLite::SQLite3)
  # The NuDB builder encodes nodes with xrpld's codec, which uses LZ4 directly.
  target_link_libraries(xrpla_core PRIVATE lz4::lz4)
  if(lz4_PACKAGE_FOLDER_RELEASE)  # its package config lists no include directory
    target_include_directories(xrpla_core SYSTEM PRIVATE "${lz4_PACKAGE_FOLDER_RELEASE}/include")
  endif()
  target_link_libraries(xrpla_core PRIVATE gRPC::grpc++ protobuf::libprotobuf)
  target_link_libraries(xrpla PRIVATE xrpla_core)
  target_sources(xrpla PRIVATE src/setup.cpp src/config_reference.cpp)
  # config/xrpla.example.toml, compiled in: `xrpla setup` writes it with the chosen values filled in.
  file(READ ${CMAKE_CURRENT_SOURCE_DIR}/config/xrpla.example.toml XRPLA_CONFIG_HEX HEX)
  string(REGEX REPLACE "([0-9a-f][0-9a-f])" "0x\\1," XRPLA_CONFIG_BYTES "${XRPLA_CONFIG_HEX}")
  file(CONFIGURE OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/generated/config_reference.inc CONTENT "${XRPLA_CONFIG_BYTES}0x00")
  set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/config/xrpla.example.toml)
  target_include_directories(xrpla PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/generated)
  # The source tree's tools, which `xrpla run` (connected) uses only when the binary runs from
  # its build directory and no installed tools are beside it (src/setup.cpp).
  target_compile_definitions(xrpla PRIVATE XRPLA_SOURCE_TOOLS="${CMAKE_CURRENT_SOURCE_DIR}/tools")
  target_compile_definitions(xrpla PRIVATE XRPLA_WITH_LIBXRPL=1)
  if(UNIX)
    add_executable(xrpla_snapshot tools/snapshot_tool.cpp)
    target_link_libraries(xrpla_snapshot PRIVATE xrpla_core)
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
      target_link_options(xrpla_snapshot PRIVATE -static-libstdc++ -static-libgcc)
    endif()
  endif()
endif()

if(BUILD_TESTING)
  find_package(GTest REQUIRED)
  add_executable(xrpla_tests tests/archive_test.cpp tests/transport_cache_test.cpp)
  target_link_libraries(xrpla_tests PRIVATE xrpla_archive GTest::gtest_main)
  include(GoogleTest)
  gtest_discover_tests(xrpla_tests)
  add_executable(xrpla_format_tests tests/format_test.cpp)
  target_link_libraries(xrpla_format_tests PRIVATE xrpla_format GTest::gtest_main)
  gtest_discover_tests(xrpla_format_tests)
  if(XRPLA_WITH_LIBXRPL)
    add_executable(xrpla_core_tests tests/core_test.cpp tests/index_test.cpp tests/validators_test.cpp tests/peer_test.cpp tests/sqlite_index_test.cpp tests/history_peers_test.cpp tests/piece_bounds_test.cpp tests/state_snapshot_test.cpp tests/source_fallback_test.cpp)
    target_compile_definitions(xrpla_core_tests PRIVATE XRPLA_TEST_FIXTURES="${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures")
    target_link_libraries(xrpla_core_tests PRIVATE xrpla_core GTest::gtest_main)
    # history_peers_test.cpp runs a gRPC server standing in for xrpld's.
    target_link_libraries(xrpla_core_tests PRIVATE gRPC::grpc++ protobuf::libprotobuf)
    gtest_discover_tests(xrpla_core_tests)
  endif()
  find_package(Python3 COMPONENTS Interpreter REQUIRED)
  add_test(NAME delta_codec COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/delta_codec_test.py)
  add_test(NAME node_tools COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/node_tools_test.py)
  add_test(NAME live_health COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/live_health_test.py)
  add_test(NAME live_reclaim COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/live_reclaim_test.py)
  add_test(NAME piece_split COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/piece_split_test.py)
  add_test(NAME publication_audit COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/publication_audit_test.py)
  add_test(NAME config_reference COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/config_reference_test.py)
  # Skips unless XRPLA_TEST_S3_* point at an S3-compatible endpoint such as MinIO.
  add_test(NAME s3_remote COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/s3_remote_test.py)
  add_test(NAME publication_merge COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/publication_merge_test.py)
  add_test(NAME tx_fingerprint_index COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/tx_fingerprint_index_test.py)
  # The Clio loader against an in-memory Cassandra (tests/fake_cassandra.py).
  add_test(NAME clio_load COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/clio_load_test.py)
  set_tests_properties(clio_load PROPERTIES TIMEOUT 300)
  # tools/xrpla_invariants.py's xrpld and Clio rules on hand-made metadata.
  add_test(NAME invariants_rules COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/xrpla_invariants_test.py)
  # tools/xrpld_parity.py's difference limit and its timeouts against other failures.
  add_test(NAME parity_tools COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/parity_tools_test.py)
  add_test(NAME cli_integration COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/cli_integration.py $<TARGET_FILE:xrpla>
    $<TARGET_FILE:xrpla_tests>)
  if(XRPLA_WITH_LIBXRPL)
    add_test(NAME log_setting COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/tests/log_setting_test.py $<TARGET_FILE:xrpla>)
    add_test(NAME core_integration COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/tests/core_integration.py $<TARGET_FILE:xrpla> $<TARGET_FILE:xrpla_core_tests>)
    add_test(NAME operations_integration COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/tests/operations_integration.py $<TARGET_FILE:xrpla> $<TARGET_FILE:xrpla_core_tests>)
    add_test(NAME clio_plan COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/clio_plan_test.py)
    set_tests_properties(clio_plan PROPERTIES TIMEOUT 600 ENVIRONMENT "XRPLA_BINARY=$<TARGET_FILE:xrpla>")
    add_test(NAME publisher_integration COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/tests/publisher_integration.py $<TARGET_FILE:xrpla> $<TARGET_FILE:xrpla_core_tests>)
    if(TARGET xrpla_snapshot)
      set_tests_properties(publisher_integration PROPERTIES ENVIRONMENT "XRPLA_SNAPSHOT_TOOL=$<TARGET_FILE:xrpla_snapshot>")
    endif()
    if(UNIX)
      add_test(NAME piece_publication COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/piece_publication_test.py)
      set_tests_properties(piece_publication PROPERTIES TIMEOUT 600
        ENVIRONMENT "XRPLA_HISTORY_DECODER=$<TARGET_FILE:xrpla_history_decode>;XRPLA_SNAPSHOT_TOOL=$<TARGET_FILE:xrpla_snapshot>;XRPLA_BINARY=$<TARGET_FILE:xrpla>;XRPLA_TX_INDEX=$<TARGET_FILE:xrpla_tx_index>")
      add_test(NAME piece_queries COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/piece_query_test.py)
      # The TLS listeners' test makes its certificates with the openssl program (the Conan
      # package's when built with it).
      find_program(XRPLA_OPENSSL_PROGRAM openssl HINTS "${openssl_PACKAGE_FOLDER_RELEASE}/bin")
      set_tests_properties(piece_queries PROPERTIES TIMEOUT 900
        ENVIRONMENT "XRPLA_HISTORY_DECODER=$<TARGET_FILE:xrpla_history_decode>;XRPLA_SNAPSHOT_TOOL=$<TARGET_FILE:xrpla_snapshot>;XRPLA_BINARY=$<TARGET_FILE:xrpla>;XRPLA_HISTORY_ENCODER=$<TARGET_FILE:xrpla_history_encode>;XRPLA_OPENSSL=$<$<BOOL:${XRPLA_OPENSSL_PROGRAM}>:${XRPLA_OPENSSL_PROGRAM}>")
      # A live tail: pieces up to one ledger, the archive (a spool) after it.
      add_test(NAME piece_queries_tail COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/piece_query_test.py ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/standalone-nfts 4 2 64 13)
      set_tests_properties(piece_queries_tail PROPERTIES TIMEOUT 900
        ENVIRONMENT "XRPLA_HISTORY_DECODER=$<TARGET_FILE:xrpla_history_decode>;XRPLA_SNAPSHOT_TOOL=$<TARGET_FILE:xrpla_snapshot>;XRPLA_BINARY=$<TARGET_FILE:xrpla>;XRPLA_HISTORY_ENCODER=$<TARGET_FILE:xrpla_history_encode>")
      add_test(NAME peers_live COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/peers_live_test.py)
      set_tests_properties(peers_live PROPERTIES TIMEOUT 400 ENVIRONMENT "XRPLA_BINARY=$<TARGET_FILE:xrpla>")
      add_test(NAME connected_publication COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/connected_publication_test.py)
      set_tests_properties(connected_publication PROPERTIES TIMEOUT 900
        ENVIRONMENT "XRPLA_BINARY=$<TARGET_FILE:xrpla>;XRPLA_HISTORY_DECODER=$<TARGET_FILE:xrpla_history_decode>;XRPLA_SNAPSHOT_TOOL=$<TARGET_FILE:xrpla_snapshot>")
      add_test(NAME connected COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/connected_test.py)
      set_tests_properties(connected PROPERTIES TIMEOUT 900 ENVIRONMENT "XRPLA_BINARY=$<TARGET_FILE:xrpla>")
      add_test(NAME restore COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/restore_test.py)
      set_tests_properties(restore PROPERTIES TIMEOUT 900
        ENVIRONMENT "XRPLA_HISTORY_DECODER=$<TARGET_FILE:xrpla_history_decode>;XRPLA_SNAPSHOT_TOOL=$<TARGET_FILE:xrpla_snapshot>;XRPLA_BINARY=$<TARGET_FILE:xrpla>;XRPLA_HISTORY_ENCODER=$<TARGET_FILE:xrpla_history_encode>")
      # NFT lifecycle from tests/nft_lifecycle.py: both backends and the scenario's own answers.
      add_test(NAME piece_queries_nfts COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/piece_query_test.py ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/standalone-nfts 4 2 64)
      set_tests_properties(piece_queries_nfts PROPERTIES TIMEOUT 900
        ENVIRONMENT "XRPLA_HISTORY_DECODER=$<TARGET_FILE:xrpla_history_decode>;XRPLA_SNAPSHOT_TOOL=$<TARGET_FILE:xrpla_snapshot>;XRPLA_BINARY=$<TARGET_FILE:xrpla>;XRPLA_HISTORY_ENCODER=$<TARGET_FILE:xrpla_history_encode>")
      # MPTs, price oracles and a vault from tests/token_lifecycle.py: both backends, and what xrpld
      # answered. The cache keeps every other interval snapshot (sync --snapshots-every 8; one
      # every 4 ledgers is published), so ledgers read their state from several snapshots.
      add_test(NAME piece_queries_tokens COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/piece_query_test.py ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/standalone-tokens 2 2 64)
      set_tests_properties(piece_queries_tokens PROPERTIES TIMEOUT 900
        ENVIRONMENT "XRPLA_SNAPSHOTS_EVERY=8;XRPLA_HISTORY_DECODER=$<TARGET_FILE:xrpla_history_decode>;XRPLA_SNAPSHOT_TOOL=$<TARGET_FILE:xrpla_snapshot>;XRPLA_BINARY=$<TARGET_FILE:xrpla>;XRPLA_HISTORY_ENCODER=$<TARGET_FILE:xrpla_history_encode>")
      # Derived indexes against verified state and transactions (tools/xrpla_invariants.py), every
      # ledger of each fixture, served as a store and as a piece cache: no violation.
      foreach(fixture nfts tokens transactions)
        set(archive standalone-${fixture})
        if(fixture STREQUAL "transactions")
          set(archive mainnet-transactions)
        endif()
        add_test(NAME invariants_${fixture} COMMAND ${Python3_EXECUTABLE}
          ${CMAKE_CURRENT_SOURCE_DIR}/tests/xrpla_invariants_test.py ${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/${archive})
        set_tests_properties(invariants_${fixture} PROPERTIES TIMEOUT 1200
          ENVIRONMENT "XRPLA_HISTORY_DECODER=$<TARGET_FILE:xrpla_history_decode>;XRPLA_SNAPSHOT_TOOL=$<TARGET_FILE:xrpla_snapshot>;XRPLA_BINARY=$<TARGET_FILE:xrpla>;XRPLA_HISTORY_ENCODER=$<TARGET_FILE:xrpla_history_encode>")
      endforeach()
      add_test(NAME field_codec COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/field_codec_test.py)
      add_test(NAME history_codec COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/history_codec_test.py)
      add_test(NAME history_cpp COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/history_codec_test.py $<TARGET_FILE:xrpla_history_decode>)
      add_test(NAME tx_index_tool COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/tx_index_tool_test.py)
      set_tests_properties(tx_index_tool PROPERTIES TIMEOUT 300
        ENVIRONMENT "XRPLA_TX_INDEX=$<TARGET_FILE:xrpla_tx_index>;XRPLA_HISTORY_DECODER=$<TARGET_FILE:xrpla_history_decode>;XRPLA_SNAPSHOT_TOOL=$<TARGET_FILE:xrpla_snapshot>")
      add_test(NAME history_writer COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/history_writer_test.py $<TARGET_FILE:xrpla_history_encode>)
      # tools/fast_archive.py's signing, checked reads, durable writes and header check.
      add_test(NAME fast_archive_helpers COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/fast_archive_test.py)
    endif()
    add_test(NAME recovery_integration COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/tests/recovery_integration.py $<TARGET_FILE:xrpla> $<TARGET_FILE:xrpla_core_tests>)
    add_test(NAME public_fixture_integration COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/tests/public_fixture_integration.py $<TARGET_FILE:xrpla>)
  endif()
endif()

# On Linux every executable allocates through jemalloc (conanfile.py): linked statically, its
# malloc replaces glibc's for the whole process.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND NOT XRPLA_SYSTEM_DEPS)
  find_package(jemalloc REQUIRED CONFIG)
  get_property(targets DIRECTORY PROPERTY BUILDSYSTEM_TARGETS)
  foreach(target IN LISTS targets)
    get_target_property(kind ${target} TYPE)
    if(kind STREQUAL "EXECUTABLE")
      target_link_libraries(${target} PRIVATE jemalloc::jemalloc)
    endif()
  endforeach()
endif()

include(GNUInstallDirs)
install(TARGETS xrpla RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install(FILES config/xrpla.example.toml config/xrpla-archive.example.toml config/publications.toml DESTINATION ${CMAKE_INSTALL_DATADIR}/xrpla)
install(PROGRAMS tools/clio_load.py tools/benchmark.py tools/node_probe.py tools/node_conformance.py tools/rpc_benchmark.py
  DESTINATION ${CMAKE_INSTALL_DATADIR}/xrpla)
install(DIRECTORY licenses/ DESTINATION ${CMAKE_INSTALL_DATADIR}/xrpla/licenses/upstream)
# The units name the installed programs where the install puts them, worked out when it runs:
# `cmake --install --prefix /usr` (docs/guides/install.md) and CPack (/usr, CPACK_SET_DESTDIR below) choose the
# prefix then, not when the build is configured (/usr/local by default). config/ holds them as
# for /usr. Each is written to the build tree, then installed as any file is (DESTDIR, the
# install manifest).
foreach(unit xrpla-serve xrpla-tail xrpla-publisher)
  set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/config/${unit}.service)
  install(CODE "
    set(unit_bindir \"${CMAKE_INSTALL_BINDIR}\")
    set(unit_datadir \"${CMAKE_INSTALL_DATADIR}\")
    foreach(dir unit_bindir unit_datadir)
      if(NOT IS_ABSOLUTE \"\${\${dir}}\")
        set(\${dir} \"\${CMAKE_INSTALL_PREFIX}/\${\${dir}}\")
      endif()
      if(\"\${\${dir}}\" MATCHES \"[ \t%]\")
        message(FATAL_ERROR \"\${\${dir}} cannot be named in a systemd unit (a space splits ExecStart=, % starts a specifier); install to another prefix\")
      endif()
    endforeach()
    file(READ \"${CMAKE_CURRENT_SOURCE_DIR}/config/${unit}.service\" unit_text)
    string(REPLACE \"/usr/bin/xrpla\" \"\${unit_bindir}/xrpla\" unit_text \"\${unit_text}\")
    string(REPLACE \"/usr/share/xrpla\" \"\${unit_datadir}/xrpla\" unit_text \"\${unit_text}\")
    file(WRITE \"${CMAKE_CURRENT_BINARY_DIR}/units/${unit}.service\" \"\${unit_text}\")
    file(INSTALL DESTINATION \"\${CMAKE_INSTALL_PREFIX}/lib/systemd/system\" TYPE FILE
      FILES \"${CMAKE_CURRENT_BINARY_DIR}/units/${unit}.service\")
  ")
endforeach()
# Publication: compiled codec tools and the Python publisher, client and R2 transport.
# Their dependencies are static, so they carry no RPATH to rewrite at install.
if(UNIX)
  set_target_properties(xrpla_history_decode xrpla_history_encode xrpla_tx_index PROPERTIES SKIP_BUILD_RPATH TRUE)
  if(XRPLA_WITH_LIBXRPL)
    set_target_properties(xrpla_snapshot PROPERTIES SKIP_BUILD_RPATH TRUE)
  endif()
  install(TARGETS xrpla_history_decode xrpla_history_encode xrpla_tx_index RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
  if(XRPLA_WITH_LIBXRPL)
    install(TARGETS xrpla_snapshot RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
  endif()
endif()
install(PROGRAMS tools/piece_publisher.py tools/piece_client.py tools/live_publisher.py tools/s3_remote.py
  tools/xrpld_parity.py tools/xrpla_invariants.py tools/query_bench.py tools/publication_audit.py tools/publication_merge.py DESTINATION ${CMAKE_INSTALL_DATADIR}/xrpla/tools)
install(FILES tools/capture_validations.py tools/compression_probe.py tools/delta_codec.py tools/fast_archive.py tools/field_codec.py
  tools/history_codec.py tools/transaction_columns.py tools/tx_fingerprint_index.py tools/xrpp.py
  DESTINATION ${CMAKE_INSTALL_DATADIR}/xrpla/tools)
# After every executable is copied: portable interpreter, no build RPATH.
install(SCRIPT tools/portable-install.cmake)
if(CMAKE_TOOLCHAIN_FILE)
  get_filename_component(XRPLA_GENERATORS "${CMAKE_TOOLCHAIN_FILE}" DIRECTORY)
  install(DIRECTORY "${XRPLA_GENERATORS}/licenses/" DESTINATION ${CMAKE_INSTALL_DATADIR}/xrpla/licenses OPTIONAL)
endif()
set(CPACK_PACKAGE_CONTACT "XRPLA maintainers <95x2t773@xrpla.anonaddy.com>")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Verifiable XRP Ledger history: serve, restore and publish it")
set(CPACK_DEBIAN_PACKAGE_DESCRIPTION "Verifiable XRP Ledger history: serve, restore and publish it
 XRPLA serves full XRP Ledger history over xrpld's and Clio's APIs from signed, hash-chained
 publications, restores it into xrpld or Clio, and publishes it.")
set(CPACK_DEBIAN_PACKAGE_HOMEPAGE "https://github.com/first-ledger/xrpla")
set(CPACK_DEBIAN_PACKAGE_SECTION "net")
# Release binaries without debug information (about a third the size), compressed with xz.
set(CPACK_STRIP_FILES ON)
set(CPACK_DEBIAN_COMPRESSION_TYPE xz)
# The package installs to /usr through DESTDIR, so the install code above sees /usr as the
# prefix (without it, CPack's staging directory) and the units name the installed programs.
set(CPACK_SET_DESTDIR ON)
set(CPACK_INSTALL_PREFIX /usr)
set(CPACK_PACKAGING_INSTALL_PREFIX /usr)
set(CPACK_DEBIAN_PACKAGE_DEPENDS "libc6 (>= 2.31), ca-certificates, python3, libzstd1, libsodium23")
set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)
set(CPACK_GENERATOR "TGZ;DEB")
include(CPack)
