#
# TMG Dmalloc
#
AC_DEFUN([AM_WITH_TMGDMALLOC],
[AC_MSG_CHECKING([if Thomer's malloc debugging is wanted])
AC_ARG_WITH(tmgdmalloc,
[  --with-tmgdmalloc       use Thomer's malloc debugging tool],
[if test "$withval" = yes; then
  AC_MSG_RESULT(yes)
  AC_DEFINE(WITH_TMGDMALLOC,1,
            [Define if using Thomer's malloc debugging tool])
else
  AC_MSG_RESULT(no)
fi], [AC_MSG_RESULT(no)])
])


