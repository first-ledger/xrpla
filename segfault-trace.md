# Startup segfault, 2026-11-06 01:12:46 UTC

Host bantu. The fallback-fix `xrpla` build, linked without `-Wl,--no-relax`, was installed over `/usr/bin/xrpla` and `xrpla.service` was restarted. GNU ld on this machine is 2.42. The process opened the piece cache, logged `listening`, and then dumped. No core file and no userspace stack were saved: `/proc/sys/kernel/core_pattern` pipes to apport, `/var/lib/systemd/coredump` is empty, and apport wrote no new report under `/var/crash`. The kernel fault line is the record.

The binary was `xrpla 0.4.3 (libxrpl 3.4.1)`, sha256 `0f21f649d0697ba182229da129d2f05415c1ba430a78f388df5bdc14a94123c9`, Build ID `21c637e5fc3cdeeeb6dde7fd379c81a06dfbbc16`. That file was replaced by the `--no-relax` relink (`bf1febe`).

## Kernel

```
2026-11-06T01:12:46+00:00 bantu kernel: show_signal_msg: 109 callbacks suppressed
2026-11-06T01:12:46+00:00 bantu kernel: xrpla[1131856]: segfault at 6017d1d976c0 ip 00006017d1d976c0 sp 00007ffdcf663180 error 15 likely on CPU 20 (core 1, socket 0)
2026-11-06T01:12:46+00:00 bantu kernel: Code: 00 00 00 00 00 00 00 00 00 00 e0 39 2d 0d 21 7f 00 00 18 00 00 00 00 00 00 00 18 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 <c0> 39 2d 0d 21 7f 00 00 12 00 00 00 00 00 00 00 12 00 00 00 00 00
```

The instruction pointer is the fault address. The bytes at that address (`c0 39 2d 0d 21 7f`) are a pointer, `0x7f210d2d39c0`, not an instruction. Page-fault error 15 is a user-mode write that hit a protection fault with a reserved bit set. This is the ld 2.42 relaxation of `pushq sym@GOTPCREL(%rip)` into `addr32 call sym`: RocksDB passes `kPropertiesBlockName` that way, and the call executes the string.

## systemd

```
2026-11-06T01:12:44+00:00 bantu systemd[1]: Started xrpla.service - XRPLA (/etc/xrpla/xrpla.toml).
2026-11-06T01:12:45+00:00 bantu xrpla[1131875]: {"cache_mb":65536,"event":"piece_cache"}
2026-11-06T01:12:45+00:00 bantu xrpla[1131875]: {"event":"listening","http":"0.0.0.0:5105","https":"","ws":"0.0.0.0:6106","wss":""}
2026-11-06T01:12:46+00:00 bantu systemd[1]: xrpla.service: Main process exited, code=dumped, status=11/SEGV
2026-11-06T01:12:46+00:00 bantu systemd[1]: xrpla.service: Failed with result 'core-dump'.
2026-11-06T01:12:47+00:00 bantu systemd[1]: Stopped xrpla.service - XRPLA (/etc/xrpla/xrpla.toml).
2026-11-06T01:12:47+00:00 bantu systemd[1]: Started xrpla.service - XRPLA (/etc/xrpla/xrpla.toml).
```

The start at 01:12:47 is the packaged 0.4.3 binary restored by the install script after the dump. The kernel log has no further `xrpla` fault from that start.
