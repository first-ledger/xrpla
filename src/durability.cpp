#include <xrpla/archive.hpp>
#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <cerrno>
#include <cstring>
#endif

namespace xrpla {
void sync_file(fs::path const& file) {
#ifndef _WIN32
    int fd = ::open(file.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) throw Error("Cannot open for sync: " + file.string());
    auto rc = ::fsync(fd); ::close(fd);
    if (rc != 0) throw Error("Cannot sync: " + file.string());
#endif
}
void durable_rename(fs::path const& source, fs::path const& destination) {
    sync_file(source);
    fs::rename(source, destination);
#ifndef _WIN32
    // Also persist newly created directory entries, including their parents.
    auto parent = fs::absolute(destination).parent_path();
    while (!parent.empty()) {
        sync_file(parent);
        auto next = parent.parent_path();
        if (next == parent) break;
        parent = next;
    }
    if (source.parent_path() != destination.parent_path()) sync_file(fs::absolute(source).parent_path());
#endif
}
DirectoryLock::DirectoryLock(fs::path const& root) : lock_(root / ".xrpla-lock") {
    safe_cache_path(root);
    fs::create_directories(root);
#ifndef _WIN32
    // A legacy directory lock is preserved as an explicit conflict.
    if (fs::exists(lock_)) throw DirectoryLocked("Data directory is locked: " + lock_.string());
    auto path = root / ".xrpla.lock";
    safe_cache_path(path);
    fd_ = ::open(path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd_ < 0) throw Error("Cannot open data directory lock");
    if (::flock(fd_, LOCK_EX | LOCK_NB) != 0) { ::close(fd_); fd_ = -1; throw DirectoryLocked("Data directory is locked: " + root.string()); }
#else
    if (!fs::create_directory(lock_)) throw DirectoryLocked("Data directory is locked: " + lock_.string());
#endif
}
DirectoryLock::~DirectoryLock() {
#ifndef _WIN32
    if (fd_ >= 0) ::close(fd_); // Kernel releases the lock on crash, too.
#else
    std::error_code ec; fs::remove(lock_, ec);
#endif
}
}
