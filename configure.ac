AC_INIT([riemann-send], [0.1], [nick.satterly@guardian.co.uk])
AC_CONFIG_AUX_DIR([build])
AM_INIT_AUTOMAKE([foreign -Wall -Werror])
AC_PROG_CC
AM_PROG_CC_C_O

AC_CHECK_PROGS([PROTOC_C], [protoc-c], [:])
if test "$PROTOC_C" = :; then
  AC_MSG_ERROR([This package needs 'protoc-c' tool. Try installing the 'protobuf-c-compiler' package.])
fi
AC_CONFIG_COMMANDS([riemann_pb-c.c], [protoc-c --c_out=./src riemann.proto])
AC_CONFIG_HEADERS([config.h])
AC_CHECK_LIB([protobuf-c], [protobuf_c_message_pack])

APR_CONFIG="apr-config"

APR_INCLUDES=`${APR_CONFIG} --includes 2> /dev/null`
APR_CFLAGS=`${APR_CONFIG} --cflags 2> /dev/null`
APR_CPPFLAGS=`${APR_CONFIG} --cppflags 2> /dev/null`
APR_LDFLAGS=`${APR_CONFIG} --ldflags 2> /dev/null`
APR_LIBS=`${APR_CONFIG} --libs 2> /dev/null`

AC_SUBST(APR_INCLUDES)
AC_SUBST(APR_CFLAGS)
AC_SUBST(APR_CPPFLAGS)
AC_SUBST(APR_LDFLAGS)
AC_SUBST(APR_LIBS)

AC_CONFIG_FILES([Makefile src/Makefile])
AC_OUTPUT

