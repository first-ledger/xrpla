# Run during install/CPack, after the executables have been copied. All C++
# dependencies are static; the supported Linux runtime needs only glibc.
if(CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux")
  find_program(XRPLA_PATCHELF patchelf REQUIRED)
  foreach(binary xrpla xrpla_history_decode xrpla_history_encode xrpla_tx_index xrpla_snapshot)
    set(path "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/${binary}")
    if(EXISTS "${path}")
      execute_process(COMMAND "${XRPLA_PATCHELF}" --set-interpreter
        /lib64/ld-linux-x86-64.so.2 --remove-rpath "${path}"
        COMMAND_ERROR_IS_FATAL ANY)
    endif()
  endforeach()
endif()
