ARG UBUNTU_VERSION=24.04
FROM ubuntu:${UBUNTU_VERSION} AS toolchain
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates cmake ninja-build g++-12 python3 python3-venv git pkg-config \
    autoconf automake libtool make perl patchelf && rm -rf /var/lib/apt/lists/*
RUN python3 -m venv /opt/conan && /opt/conan/bin/pip install --no-cache-dir conan==2.17.1
ENV PATH="/opt/conan/bin:${PATH}" CC=gcc-12 CXX=g++-12
FROM toolchain AS build
WORKDIR /src
COPY conanfile.py conan.lock ./
# Use Dockerfile.core for the C++23/libxrpl build.
ARG WITH_LIBXRPL=False
ARG XRPL_CONAN_REMOTE=https://conan.xrplf.org/repository/conan/
RUN conan profile detect --force \
    && conan remote add xrplf "$XRPL_CONAN_REMOTE" \
    && conan install . --output-folder=build --build=missing \
       -s compiler.cppstd=20 -s build_type=Release -c tools.build:jobs=2 -o "&:with_libxrpl=${WITH_LIBXRPL}"
COPY . .
RUN cmake -S . -B build/Release -G Ninja -DCMAKE_BUILD_TYPE=Release \
       -DCMAKE_TOOLCHAIN_FILE=/src/build/generators/conan_toolchain.cmake \
    && cmake --build build/Release -j 2 \
    && ctest --test-dir build/Release --output-on-failure \
    && cmake --install build/Release --prefix /install
FROM ubuntu:${UBUNTU_VERSION}
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates python3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /install /usr/local
USER 65532:65532
WORKDIR /data
ENTRYPOINT ["/usr/local/bin/xrpla"]
