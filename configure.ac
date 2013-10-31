AC_INIT([riemann-send], [0.1], [nick.satterly@guardian.co.uk])
AC_CONFIG_AUX_DIR([build])
AM_INIT_AUTOMAKE([foreign -Wall -Werror])
AC_PROG_CC
AC_CHECK_PROGS([PROTOC_C], [protoc-c], [:])
if test "$PROTOC_C" = :; then
  AC_MSG_ERROR([This package needs 'protoc-c' tool. Try installing the 'protobuf-c-compiler' package.])
fi
AC_CONFIG_COMMANDS([riemann_pb-c.c], [protoc-c --c_out=./src riemann.proto])
AC_CONFIG_HEADERS([config.h])
AC_CHECK_LIB([protobuf-c], [protobuf_c_message_pack])
AC_CONFIG_FILES([Makefile src/Makefile])
AC_OUTPUT

