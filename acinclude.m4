dnl $Id: acinclude.m4,v 1.19 2006/10/04 23:05:09 strib Exp $

dnl
dnl Find full path to program
dnl
AC_DEFUN([SFS_PATH_PROG],
[AC_PATH_PROG(PATH_[]translit($1, [a-z], [A-Z]), $1,,
$2[]ifelse($2,,,:)/usr/bin:/bin:/sbin:/usr/sbin:/usr/etc:/usr/libexec:/usr/ucb:/usr/bsd:/usr/5bin:$PATH:/usr/local/bin:/usr/local/sbin:/usr/X11R6/bin)
if test "$PATH_[]translit($1, [a-z], [A-Z])"; then
    AC_DEFINE_UNQUOTED(PATH_[]translit($1, [a-z], [A-Z]),
		       "$PATH_[]translit($1, [a-z], [A-Z])",
			Full path of $1 command)
fi])
dnl
dnl File path to cpp
dnl
AC_DEFUN([SFS_PATH_CPP],
[AC_PATH_PROG(_PATH_CPP, cpp,,
/usr/ccs/bin:/usr/bin:/bin:/sbin:/usr/sbin:/usr/etc:/usr/libexec:/lib:/usr/lib:/usr/ucb:/usr/bsd:/usr/5bin:$PATH:/usr/local/bin:/usr/local/sbin:/usr/X11R6/bin)
if test -z "$_PATH_CPP"; then
    if test "$GCC" = yes; then
	_PATH_CPP=`$CC -print-prog-name=cpp`
    else
	_PATH_CPP=`gcc -print-prog-name=cpp 2> /dev/null`
    fi
fi
test -x "$_PATH_CPP" || unset _PATH_CPP
if test -z "$_PATH_CPP"; then
    AC_MSG_ERROR(Cannot find path for cpp)
fi
AC_DEFINE_UNQUOTED(PATH_CPP, "$_PATH_CPP",
			Path for the C preprocessor command)
])
dnl
dnl Check for declarations
dnl
AC_DEFUN([SFS_CHECK_DECL],
[AC_CACHE_CHECK(for a declaration of $1, sfs_cv_$1_decl,
    for hdr in [patsubst(builtin(shift, $@), [,], [ ])]; do
	if test -z "${sfs_cv_$1_decl}"; then
	    AC_HEADER_EGREP($1, $hdr, sfs_cv_$1_decl=yes)
	fi
    done
    test -z "${sfs_cv_$1_decl+set}" && sfs_cv_$1_decl=no)
if test "$sfs_cv_$1_decl" = no; then
	AC_DEFINE_UNQUOTED(NEED_[]translit($1, [a-z], [A-Z])_DECL, 1,
		Define if system headers do not declare $1.)
fi])
dnl
dnl Check if lsof keeps a device cache
dnl
AC_DEFUN([SFS_LSOF_DEVCACHE],
[if test "$PATH_LSOF"; then
    AC_CACHE_CHECK(if lsof supports a device cache, sfs_cv_lsof_devcache,
    if $PATH_LSOF -h 2>&1 | fgrep -e -D > /dev/null; then
	sfs_cv_lsof_devcache=yes
    else
	sfs_cv_lsof_devcache=no
    fi)
    if test "$sfs_cv_lsof_devcache" = yes; then
	AC_DEFINE(LSOF_DEVCACHE, 1,
		Define is lsof supports the -D option)
    fi
fi])
dnl
dnl Posix time subroutine
dnl
AC_DEFUN([SFS_TIME_CHECK],
[AC_CACHE_CHECK($3, sfs_cv_time_check_$1,
AC_TRY_COMPILE([
#ifdef TIME_WITH_SYS_TIME
# include <sys/time.h>
# include <time.h>
#elif defined (HAVE_SYS_TIME_H)
# include <sys/time.h>
#else /* !TIME_WITH_SYS_TIME && !HAVE_SYS_TIME_H */
# include <time.h>
#endif /* !TIME_WITH_SYS_TIME && !HAVE_SYS_TIME_H */
], $2, sfs_cv_time_check_$1=yes, sfs_cv_time_check_$1=no))
if test "$sfs_cv_time_check_$1" = yes; then
	AC_DEFINE($1, 1, $4)
fi])
dnl
dnl Posix time stuff
dnl
AC_DEFUN([SFS_TIMESPEC],
[AC_CHECK_HEADERS(sys/time.h)
AC_HEADER_TIME
AC_CHECK_FUNCS(clock_gettime)
SFS_TIME_CHECK(HAVE_CLOCK_GETTIME_DECL,
		int (*x) () = &clock_gettime;,
		for a declaration of clock_gettime,
		Define if system headers declare clock_gettime.)
SFS_TIME_CHECK(HAVE_TIMESPEC,
		int x = sizeof (struct timespec),
		for struct timespec,
		Define if sys/time.h defines a struct timespec.)
dnl AC_EGREP_HEADER(clock_gettime, sys/time.h,
dnl 	AC_DEFINE(HAVE_CLOCK_GETTIME_DECL, 1,
dnl 	Define if system header files declare clock_gettime.))
dnl AC_EGREP_HEADER(timespec, sys/time.h,
dnl 	AC_DEFINE(HAVE_TIMESPEC, 1,
dnl 		  Define if sys/time.h defines a struct timespec.))
])
dnl
dnl Find the crypt function
dnl
AC_DEFUN([SFS_FIND_CRYPT],
[AC_SUBST(LIBCRYPT)
AC_CHECK_FUNC(crypt)
if test $ac_cv_func_crypt = no; then
	AC_CHECK_LIB(crypt, crypt, LIBCRYPT="-lcrypt")
fi
])
dnl
dnl Find pty functions
dnl
AC_DEFUN([SFS_PTYLIB],
[AC_SUBST(PTYLIB)
AC_CHECK_FUNCS(_getpty)
AC_CHECK_FUNCS(openpty)
if test $ac_cv_func_openpty = no; then
	AC_CHECK_LIB(util, openpty, PTYLIB="-lutil"
		AC_DEFINE(HAVE_OPENPTY, 1,
			Define if you have the openpty function.)
		AC_CHECK_HEADERS(util.h libutil.h))
fi

AC_MSG_CHECKING(for pseudo ttys)
if test -c /dev/ptmx && test -c /dev/pts/0
then
  AC_DEFINE(HAVE_DEV_PTMX, 1,
	    Define if you have SYSV-style /dev/ptmx and /dev/pts/.)
  AC_MSG_RESULT(streams ptys)
else
if test -c /dev/pts && test -c /dev/ptc
then
  AC_DEFINE(HAVE_DEV_PTS_AND_PTC, 1,
	    Define if you have /dev/pts and /dev/ptc devices (as in AIX).)
  AC_MSG_RESULT(/dev/pts and /dev/ptc)
else
  AC_MSG_RESULT(bsd-style ptys)
fi
fi])
dnl
dnl Use -lresolv only if we need it
dnl
AC_DEFUN([SFS_FIND_RESOLV],
[AC_CHECK_FUNC(res_mkquery)
if test "$ac_cv_func_res_mkquery" != yes; then
	AC_CHECK_LIB(resolv, res_mkquery)
fi
dnl See if the resolv functions are actually declared
SFS_CHECK_DECL(res_init, resolv.h)
SFS_CHECK_DECL(res_mkquery, resolv.h)
SFS_CHECK_DECL(dn_skipname, resolv.h)
SFS_CHECK_DECL(dn_expand, resolv.h)
])
dnl
dnl Check if first element in grouplist is egid
dnl
AC_DEFUN([SFS_CHECK_EGID_IN_GROUPLIST],
[AC_TYPE_GETGROUPS
AC_CACHE_CHECK(if egid is first element of grouplist, sfs_cv_egid_in_grouplist,
AC_TRY_RUN([changequote changequote([[,]])
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include <rpc/rpc.h>

#include "confdefs.h"

static int
getint (void *_p)
{
  unsigned char *p = _p;
  return p[0]<<24 | p[1]<<16 | p[2]<<8 | p[3];
}

int
main (int argc, char **argv)
{
  AUTH *a;
  GETGROUPS_T gids[24];
  int n, xn;
  char buf[408];
  char *p;
  XDR x;

  /* Must hard-code OSes with egid in grouplist *and* broken RPC lib */
#if __FreeBSD__
  return 0;
#endif 

  n = getgroups (24, gids);
  if (n <= 0)
    return 1;

  a = authunix_create_default ();
  xdrmem_create (&x, buf, sizeof (buf), XDR_ENCODE);
  if (!auth_marshall (a, &x))
    return 1;

  if (getint (buf) != AUTH_UNIX)
    return 1;
  p = buf + 12;			/* Skip auth flavor, length, timestamp */
  p += getint (p) + 7 & ~3;	/* Skip machine name */
  p += 8;			/* Skip uid & gid */

  xn = getint (p);		/* Length of grouplist in auth_unix */

  return n != xn + 1;
}
changequote([,])],
	sfs_cv_egid_in_grouplist=yes, sfs_cv_egid_in_grouplist=no))
if test $sfs_cv_egid_in_grouplist = yes; then
	AC_DEFINE(HAVE_EGID_IN_GROUPLIST, 1,
	  Define if the first element of a grouplist is the effective gid)
fi])
dnl
dnl Check for wide select
dnl
AC_DEFUN([SFS_CHECK_WIDE_SELECT],
[AC_CACHE_CHECK(for wide select, sfs_cv_wideselect,
fdlim_h=${srcdir}/fdlim.h
test -f ${srcdir}/async/fdlim.h && fdlim_h=${srcdir}/async/fdlim.h
test -f ${srcdir}/libasync/fdlim.h && fdlim_h=${srcdir}/libasync/fdlim.h
AC_TRY_RUN([changequote changequote([[,]])
#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include "${fdlim_h}"

struct timeval ztv;

int
main ()
{
  int pfd[2];
  int rfd, wfd;
  int maxfd;
  int i;
  fd_set *rfdsp, *wfdsp;

  maxfd = fdlim_get (1);
  fdlim_set (maxfd);
  maxfd = fdlim_get (0);
  if (maxfd <= FD_SETSIZE) {
    printf ("[small fd limit anyway] ");
    exit (1);
  }
  if (pipe (pfd) < 0)
    exit (1);

#ifdef F_DUPFD
  if ((rfd = fcntl (pfd[0], F_DUPFD, maxfd - 2)) < 0)
    exit (1);
  if ((wfd = fcntl (pfd[1], F_DUPFD, maxfd - 1)) < 0)
    exit (1);
#else /* !F_DUPFD */
  if ((rfd = dup2 (pfd[0], maxfd - 2)) < 0)
    exit (1);
  if ((wfd = dup2 (pfd[1], maxfd - 1)) < 0)
    exit (1);
#endif /* !F_DUPFD */

  rfdsp = malloc (1 + (maxfd/8));
  for (i = 0; i < 1 + (maxfd/8); i++)
    ((char *) rfdsp)[i] = '\0';
  wfdsp = malloc (1 + (maxfd/8));
  for (i = 0; i < 1 + (maxfd/8); i++)
    ((char *) wfdsp)[i] = '\0';

  FD_SET (rfd, rfdsp);
  FD_SET (wfd, wfdsp);
  if (select (maxfd, rfdsp, wfdsp, NULL, &ztv) < 0)
    exit (1);

  if (FD_ISSET (wfd, wfdsp) && !FD_ISSET (rfd, rfdsp))
    exit (0);
  else
    exit (1);
}
changequote([,])],
sfs_cv_wideselect=yes, sfs_cv_wideselect=no, sfs_cv_wideselect=no))
if test $sfs_cv_wideselect = yes; then
	AC_DEFINE(HAVE_WIDE_SELECT, 1,
		  Define if select can take file descriptors >= FD_SETSIZE)
fi])
dnl
dnl Check for 64-bit off_t
dnl
AC_DEFUN([SFS_CHECK_OFF_T_64],
[AC_CACHE_CHECK(for 64-bit off_t, sfs_cv_off_t_64,
AC_TRY_COMPILE([
#include <unistd.h>
#include <sys/types.h>
],[
switch (0) case 0: case (sizeof (off_t) <= 4):;
], sfs_cv_off_t_64=no, sfs_cv_off_t_64=yes))
if test $sfs_cv_off_t_64 = yes; then
	AC_DEFINE(HAVE_OFF_T_64, 1, Define if off_t is 64 bits wide.)
fi])
dnl
dnl Check for type
dnl
AC_DEFUN([SFS_CHECK_TYPE],
[AC_CACHE_CHECK(for $1, sfs_cv_type_$1,
AC_TRY_COMPILE([
#include <stddef.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#ifdef HAVE_RPC_RPC_H
#include <rpc/rpc.h>
#endif
],[
sizeof($1);
], sfs_cv_type_$1=yes, sfs_cv_type_$1=no))
if test $sfs_cv_type_$1 = yes; then
        AC_DEFINE(HAVE_[]translit($1, [a-z], [A-Z]), 1,
		  Define if system headers declare a $1 type.)
fi])
dnl
dnl Check for struct cmsghdr (for passing file descriptors)
dnl
AC_DEFUN([SFS_CHECK_FDPASS],
[
AC_CACHE_CHECK(for fd passing with msg_accrights in msghdr,
		sfs_cv_accrights,
AC_TRY_COMPILE([
#include <sys/types.h>
#include <sys/socket.h>
],[
struct msghdr mh;
mh.msg_accrights = 0;
], sfs_cv_accrights=yes, sfs_cv_accrights=no))

AC_CACHE_CHECK(for fd passing with struct cmsghdr, sfs_cv_cmsghdr,
if test "$sfs_cv_accrights" != "yes"; then
AC_TRY_COMPILE([
#include <sys/types.h>
#include <sys/socket.h>
],[
struct msghdr mh;
struct cmsghdr cmh;
mh.msg_control = (void *) &cmh;
], sfs_cv_cmsghdr=yes, sfs_cv_cmsghdr=no)
else
	sfs_cv_cmsghdr=no
fi)

if test $sfs_cv_accrights = yes; then
	AC_DEFINE(HAVE_ACCRIGHTS, 1,
	Define if msghdr has msg_accrights field for passing file descriptors.)
fi
if test $sfs_cv_cmsghdr = yes; then
	AC_DEFINE(HAVE_CMSGHDR, 1,
	Define if system has cmsghdr structure for passing file descriptors.)
fi])
dnl
dnl Check for sa_len in struct sockaddrs
dnl
AC_DEFUN([SFS_CHECK_SA_LEN],
[AC_CACHE_CHECK(for sa_len in struct sockaddr, sfs_cv_sa_len,
AC_TRY_COMPILE([
#include <sys/types.h>
#include <sys/socket.h>
],[
struct sockaddr sa;
sa.sa_len = 0;
], sfs_cv_sa_len=yes, sfs_cv_sa_len=no))
if test $sfs_cv_sa_len = yes; then
	AC_DEFINE(HAVE_SA_LEN, 1,
	Define if struct sockaddr has sa_len field.)
fi])
dnl
dnl Check something about the nfs_args field
dnl
AC_DEFUN([SFS_TRY_NFSARG_FIELD],
[AC_TRY_COMPILE([
#include "${srcdir}/nfsconf.h"
],[
struct nfs_args na;
$1;
], $2, $3)])
dnl
dnl Check a particular field in nfs_args
dnl
AC_DEFUN([SFS_CHECK_NFSMNT_FIELD],
[AC_CACHE_CHECK(for $1 in nfs_args structure, sfs_cv_nfsmnt_$1,
SFS_TRY_NFSARG_FIELD(na.$1, sfs_cv_nfsmnt_$1=yes, sfs_cv_nfsmnt_$1=no))
if test $sfs_cv_nfsmnt_$1 = yes; then
  AC_DEFINE(HAVE_NFSMNT_[]translit($1, [a-z], [A-Z]), 1,
	    Define if the nfs_args structure has a $1 field.)
fi])
dnl
dnl Check if nfs_args hostname field is an array
dnl
AC_DEFUN([SFS_CHECK_NFSARG_HOSTNAME_ARRAY],
[AC_CACHE_CHECK(if nfs_args hostname field is an array, sfs_cv_nfs_hostarray,
	SFS_TRY_NFSARG_FIELD(na.hostname = 0,
		sfs_cv_nfs_hostarray=no, sfs_cv_nfs_hostarray=yes))
if test $sfs_cv_nfs_hostarray = yes; then
  AC_DEFINE(HAVE_NFSARG_HOSTNAME_ARRAY)
fi])
dnl
dnl Check if addr field is a pointer or not
dnl
AC_DEFUN([SFS_CHECK_NFSARG_ADDR_PTR],
[AC_CHECK_HEADERS(tiuser.h)
AC_CACHE_CHECK(if nfs_args addr field is a pointer, sfs_cv_nfsmnt_addr_ptr,
	SFS_TRY_NFSARG_FIELD(na.addr = (void *) 0, sfs_cv_nfsmnt_addr_ptr=yes,
				sfs_cv_nfsmnt_addr_ptr=no))
if test $sfs_cv_nfsmnt_addr_ptr = yes; then
  AC_DEFINE(HAVE_NFSARG_ADDR_PTR)
  AC_CACHE_CHECK(if nfs_args addr is a netbuf *, sfs_cv_nfsmnt_addr_netbuf,
	SFS_TRY_NFSARG_FIELD(struct netbuf nb; *na.addr = nb,
	  sfs_cv_nfsmnt_addr_netbuf=yes, sfs_cv_nfsmnt_addr_netbuf=no))
  if test $sfs_cv_nfsmnt_addr_netbuf = yes; then
    AC_DEFINE(HAVE_NFSARG_ADDR_NETBUF)
  fi
fi])
dnl
dnl Check for SVR4-like nfs_fh3 structure
dnl
AC_DEFUN([SFS_CHECK_FH3_SVR4],
[if test "$sfs_cv_nfsmnt_fhsize" != yes; then
  AC_CACHE_CHECK(for SVR4-like struct nfs_fh3, sfs_cv_fh3_svr4,
  AC_TRY_COMPILE([#include "${srcdir}/nfsconf.h"],
                 [ struct nfs_fh3 fh;
                   switch (0) case 0: case sizeof (fh.fh3_u.data) == 64:; ],
                 sfs_cv_fh3_svr4=yes, sfs_cv_fh3_svr4=no))
  if test $sfs_cv_fh3_svr4 = yes; then
    AC_DEFINE(HAVE_SVR4_FH3)
  fi
fi])
dnl
dnl Check for 2 argument unmount
dnl
AC_DEFUN([SFS_CHECK_UNMOUNT_FLAGS],
[AC_CACHE_CHECK(for a 2 argument unmount, sfs_cv_umount_flags,
AC_TRY_COMPILE([
#include <sys/param.h>
#include <sys/mount.h>
],[
#ifdef HAVE_UNMOUNT
unmount
#else /* !HAVE_UNMOUNT */
umount
#endif /* !HAVE_UNMOUNT */
	(0);
], sfs_cv_umount_flags=no, sfs_cv_umount_flags=yes))
if test $sfs_cv_umount_flags = yes; then
	AC_DEFINE(UNMOUNT_FLAGS, 1,
		  Define if the unmount system call has 2 arguments.)
else
	AC_CHECK_FUNCS(umount2)
fi])
dnl
dnl Check if we can find the nfs_args structure
dnl
AC_DEFUN([SFS_CHECK_NFSMNT],
[AC_CHECK_FUNCS(vfsmount unmount)
need_nfs_nfs_h=no
AC_EGREP_HEADER(nfs_args, sys/mount.h,,
	AC_EGREP_HEADER(nfs_args, nfs/mount.h, AC_DEFINE(NEED_NFS_MOUNT_H))
	AC_EGREP_HEADER(nfs_args, nfs/nfsmount.h,
		AC_DEFINE(NEED_NFS_NFSMOUNT_H))
	AC_EGREP_HEADER(nfs_args, nfs/nfs.h, AC_DEFINE(NEED_NFS_NFS_H)
			need_nfs_nfs_h=yes))
AC_CACHE_CHECK(for nfs_args mount structure, sfs_cv_nfsmnt_ok,
	SFS_TRY_NFSARG_FIELD(, sfs_cv_nfsmnt_ok=yes, sfs_cv_nfsmnt_ok=no))
if test $sfs_cv_nfsmnt_ok = no; then
	AC_MSG_ERROR([Could not find NFS mount argument structure!])
fi
if test "$need_nfs_nfs_h" = no; then
	AC_EGREP_HEADER(nfs_fh3, nfs/nfs.h, AC_DEFINE(NEED_NFS_NFS_H)
			need_nfs_nfs_h=yes)
fi
AC_CHECK_HEADERS(linux/nfs2.h)
SFS_CHECK_NFSMNT_FIELD(addrlen)
SFS_CHECK_NFSMNT_FIELD(sotype)
SFS_CHECK_NFSMNT_FIELD(proto)
SFS_CHECK_NFSMNT_FIELD(fhsize)
SFS_CHECK_NFSMNT_FIELD(fd)

dnl Check whether we have Linux 2.2 NFS V3 mount structure
SFS_CHECK_NFSMNT_FIELD(old_root)

dnl Check whether file handle is named "root" or "fh"
SFS_CHECK_NFSMNT_FIELD(root)
SFS_CHECK_NFSMNT_FIELD(fh)
dnl ksh apparently cannot handle this as a compound test.
if test "$sfs_cv_nfsmnt_root" = "no"; then
  if test "$sfs_cv_nfsmnt_fh" = "no"; then
    AC_MSG_ERROR([Could not find the nfs_args file handle field!])
  fi
fi
AC_CHECK_HEADERS(sys/mntent.h)
SFS_CHECK_FH3_SVR4
if test "$sfs_cv_nfsmnt_fh" = yes; then
  if test "$sfs_cv_fh3_svr4" = yes -o "$sfs_cv_nfsmnt_fhsize" = yes; then
    AC_DEFINE(HAVE_NFS_V3)
  fi
elif test "$sfs_cv_nfsmnt_old_root" = yes; then
  AC_DEFINE(HAVE_NFS_V3)
fi

SFS_CHECK_NFSARG_HOSTNAME_ARRAY
SFS_CHECK_NFSARG_ADDR_PTR
SFS_CHECK_UNMOUNT_FLAGS])
dnl
dnl Use -ldb only if we need it.
dnl
AC_DEFUN([SFS_FIND_DB],
[AC_CHECK_FUNC(dbopen)
if test $ac_cv_func_dbopen = no; then
	AC_CHECK_LIB(db, dbopen)
	if test $ac_cv_lib_db_dbopen = no; then
	  AC_MSG_ERROR([Could not find library for dbopen!])
	fi
fi
])
dnl
dnl Check something about the stat structure
dnl
AC_DEFUN([SFS_TRY_STAT_FIELD],
[AC_TRY_COMPILE([
#include <sys/stat.h>
],[
struct stat s;
$1;
], $2, $3)])
dnl
dnl Check for a particular field in stat
dnl
AC_DEFUN([SFS_CHECK_STAT_FIELD],
[AC_CACHE_CHECK(for $1 in stat structure, sfs_cv_stat_$1,
SFS_TRY_STAT_FIELD(s.$1, sfs_cv_stat_$1=yes, sfs_cv_stat_$1=no))
if test $sfs_cv_stat_$1 = yes; then
  AC_DEFINE(SFS_HAVE_STAT_[]translit($1, [a-z], [A-Z]), 1,
	    Define if the stat structure has a $1 field.)
fi])

dnl
dnl  Check whether we can get away with large socket buffers.
dnl
AC_DEFUN([SFS_CHECK_SOCK_BUF],
[AC_CACHE_CHECK(whether socket buffers > 64k are allowed, 
 		sfs_cv_large_sock_buf, AC_TRY_RUN([
#include <sys/types.h>
#include <sys/socket.h>

int
main() 
{
  int bigbuf = 0x11000;
  int s = socket(AF_INET, SOCK_STREAM, 0);
  if (setsockopt(s, SOL_SOCKET, SO_RCVBUF, (char *)&bigbuf, sizeof(bigbuf))<0)
    exit(1);
  exit(0);
}
], sfs_cv_large_sock_buf=yes, 
   sfs_cv_large_sock_buf=no, 
   sfs_cv_large_sock_buf=no))
if test $sfs_cv_large_sock_buf = yes; then
	AC_DEFINE(SFS_ALLOW_LARGE_BUFFER, 1,
		  Define if SO_SNDBUF/SO_RCVBUF can exceed 64K.)
fi])
dnl
dnl Find pthreads
dnl
AC_DEFUN([SFS_FIND_PTHREADS],
[AC_ARG_WITH(pthreads,
--with-pthreads=DIR       Specify location of pthreads)
ac_save_CFLAGS=$CFLAGS
ac_save_LIBS=$LIBS
dirs="$with_pthreads ${prefix} ${prefix}/pthreads"
dirs="$dirs /usr/local /usr/local/pthreads"
AC_CACHE_CHECK(for pthread.h, sfs_cv_pthread_h,
[for dir in " " $dirs; do
    iflags="-I${dir}/include"
    CFLAGS="${ac_save_CFLAGS} $iflags"
    AC_TRY_COMPILE([#include <pthread.h>], 0,
	sfs_cv_pthread_h="${iflags}"; break)
done])
if test -z "${sfs_cv_pthread_h+set}"; then
    AC_MSG_ERROR("Can\'t find pthread.h anywhere")
fi
AC_CACHE_CHECK(for libpthread, sfs_cv_libpthread,
[for dir in "" " " $dirs; do
    case $dir in
	"") lflags=" " ;;
	" ") lflags="-lpthread" ;;
	*) lflags="-L${dir}/lib -lpthread" ;;
    esac
    LIBS="$ac_save_LIBS $lflags"
    AC_TRY_LINK([#include <pthread.h>],
	pthread_create (0, 0, 0, 0);,
	sfs_cv_libpthread=$lflags; break)
done])
if test -z ${sfs_cv_libpthread+set}; then
    AC_MSG_ERROR("Can\'t find libpthread anywhere")
fi
CFLAGS=$ac_save_CFLAGS
CPPFLAGS="$CPPFLAGS $sfs_cv_pthread_h"
LIBS="$ac_save_LIBS $sfs_cv_libpthread"])
dnl
dnl Find GMP
dnl
AC_DEFUN([SFS_GMP],
[AC_ARG_WITH(gmp,
--with-gmp[[[=/usr/local]]]   specify path for gmp)
AC_SUBST(GMP_DIR)
AC_SUBST(LIBGMP)
AC_MSG_CHECKING([for GMP library])
test "$with_gmp" = "no" && unset with_gmp
if test -z "$with_gmp"; then
    if test -z "$GMP_DIR"; then
	for dir in `cd $srcdir && echo gmp*`; do
	    if test -d "$srcdir/$dir"; then
		GMP_DIR=$dir
		break
	    fi
	done
    fi
    if test "${with_gmp+set}" != set \
		-a "$GMP_DIR" -a -d "$srcdir/$GMP_DIR"; then
	GMP_DIR=`echo $GMP_DIR | sed -e 's!/$!!'`
	if test -f "$srcdir/$GMP_DIR/gmp-h.in"; then
	    CPPFLAGS="$CPPFLAGS "'-I$(top_builddir)/'"$GMP_DIR"
	else
	    CPPFLAGS="$CPPFLAGS "'-I$(top_srcdir)/'"$GMP_DIR"
	fi
	#LDFLAGS="$LDFLAGS "'-L$(top_builddir)/'"$GMP_DIR"
    else
	GMP_DIR=
	for dir in "$prefix" /usr/local /usr; do
	    if test \( -f $dir/lib/libgmp.a -o -f $dir/lib/libgmp.la \) \
		-a \( -f $dir/include/gmp.h -o -f $dir/include/gmp3/gmp.h \
			-o -f $dir/include/gmp2/gmp.h \); then
		    with_gmp=$dir
		    break
	    fi
	done
	if test -z "$with_gmp"; then
	    AC_MSG_ERROR([Could not find GMP library])
	fi
	test "$with_gmp" = /usr -a -f /usr/include/gmp.h && unset with_gmp
    fi
fi
if test "$with_gmp"; then
    unset GMP_DIR
    if test -f ${with_gmp}/include/gmp.h; then
	test "${with_gmp}" = /usr \
	    || CPPFLAGS="$CPPFLAGS -I${with_gmp}/include"
    elif test -f ${with_gmp}/gmp.h; then
	CPPFLAGS="$CPPFLAGS -I${with_gmp}"
    elif test -f ${with_gmp}/include/gmp3/gmp.h; then
	CPPFLAGS="$CPPFLAGS -I${with_gmp}/include/gmp3"
    elif test -f ${with_gmp}/include/gmp2/gmp.h; then
	CPPFLAGS="$CPPFLAGS -I${with_gmp}/include/gmp2"
    else
	AC_MSG_ERROR([Could not find gmp.h header])
    fi

    #LDFLAGS="$LDFLAGS -L${with_gmp}/lib"
    #LIBGMP=-lgmp
    if test -f "${with_gmp}/libgmp.la"; then
	LIBGMP="${with_gmp}/libgmp.la"
    elif test -f "${with_gmp}/lib/libgmp.a"; then
	LIBGMP="${with_gmp}/lib/libgmp.a"
    else
	LIBGMP="${with_gmp}/libgmp.a"
    fi
    AC_MSG_RESULT([$LIBGMP])
elif test "$GMP_DIR"; then
    export GMP_DIR
    AC_MSG_RESULT([using distribution in $GMP_DIR subdirectory])
    LIBGMP='$(top_builddir)/'"$GMP_DIR/libgmp.la"
else
    AC_MSG_RESULT(yes)
    if test -f /usr/lib/libgmp.la; then
	LIBGMP=/usr/lib/libgmp.la
    elif test -f "/usr/local//lib/libgmp.a"; then
	LIBGMP="/usr/local/lib/libgmp.a"
    elif test -f /usr/lib/libgmp.a; then
	# FreeBSD (among others) has a broken gmp shared library
	LIBGMP=/usr/lib/libgmp.a
    else
	LIBGMP=-lgmp
    fi
fi

AC_CONFIG_SUBDIRS($GMP_DIR)

ac_save_CFLAGS="$CFLAGS"
if test "$GMP_DIR"; then
    if test -f "${srcdir}/${GMP_DIR}"/gmp-h.in; then
	CFLAGS="$CFLAGS -I${srcdir}/${GMP_DIR}"
    else
	CFLAGS="$CFLAGS -I${builddir}/${GMP_DIR}"
    fi
fi

AC_CACHE_CHECK(for overloaded C++ operators in gmp.h, sfs_cv_gmp_cxx_ops,
    if test -f "${srcdir}/${GMP_DIR}"/gmp-h.in; then
	# More recent versions of GMP all have this
	sfs_cv_gmp_cxx_ops=yes
    else
	AC_EGREP_CPP(operator<<,
[#define __cplusplus 1
#include <gmp.h>
],
		sfs_cv_gmp_cxx_ops=yes, sfs_cv_gmp_cxx_ops=no)
    fi)
test "$sfs_cv_gmp_cxx_ops" = "yes" && AC_DEFINE([HAVE_GMP_CXX_OPS], 1,
	[Define if gmp.h overloads C++ operators])

AC_CACHE_CHECK(for mpz_xor, sfs_cv_have_mpz_xor,
unset sfs_cv_have_mpz_xor
if test "$GMP_DIR"; then
	sfs_cv_have_mpz_xor=yes
else
	AC_EGREP_HEADER(mpz_xor, [gmp.h], sfs_cv_have_mpz_xor=yes)
fi)
test "$sfs_cv_have_mpz_xor" && AC_DEFINE([HAVE_MPZ_XOR], 1,
	[Define if you have mpz_xor in your GMP library.])

AC_CACHE_CHECK(size of GMP mp_limb_t, sfs_cv_mp_limb_t_size,
sfs_cv_mp_limb_t_size=no
for size in 2 4 8; do
    AC_TRY_COMPILE([#include <gmp.h>],
    [switch (0) case 0: case (sizeof (mp_limb_t) == $size):;],
    sfs_cv_mp_limb_t_size=$size; break)
done)

CFLAGS="$ac_save_CFLAGS"

test "$sfs_cv_mp_limb_t_size" = no \
    && AC_MSG_ERROR(Could not determine size of mp_limb_t.)
AC_DEFINE_UNQUOTED(GMP_LIMB_SIZE, $sfs_cv_mp_limb_t_size,
		   Define to be the size of GMP's mp_limb_t type.)])



dnl pushdef([arglist], [ifelse($#, 0, , $#, 1, [[$1]],
dnl 		    [[$1] arglist(shift($@))])])dnl

dnl
dnl SFS_TRY_SLEEPYCAT_VERSION(vers, dir)
dnl
AC_DEFUN([SFS_TRY_SLEEPYCAT_VERSION],
[vers=$1
dir=$2
majvers=`echo $vers | sed -e 's/\..*//'`
minvers=`echo $vers | sed -e 's/[^.]*\.//' -e 's/\..*//'`
escvers=`echo $vers | sed -e 's/\./\\./g'`
catvers=`echo $vers | sed -e 's/\.//g'`
: sfs_try_sleepycat_version $vers $dir $majvers $minvers $escvers $catvers

unset db_header
unset db_library

for header in \
	$dir/include/db$vers/db.h $dir/include/db$catvers/db.h \
	$dir/include/db$majvers/db.h \
	$dir/include/db$catvers.h $dir/include/db$majvers.h \
	$dir/include/db.h
do
    test -f $header || continue
    AC_EGREP_CPP(^db_version_is $majvers *\. *$minvers *\$,
[#include "$header"
db_version_is DB_VERSION_MAJOR.DB_VERSION_MINOR
], db_header=$header; break)
done

if test "$db_header"; then
    for vdir in "$dir/lib/db$catvers" "$dir/lib/db$majvers" \
		"$dir/lib/db" "$dir/lib"
    do
        for library in $vdir/libdb-$vers.la $vdir/libdb$catvers.la \
	    $vdir/libdb.la $vdir/libdb-$vers.a $vdir/libdb$catvers.a
        do
    	if test -f $library; then
    	    db_library=$library
    	    break 2;
    	fi
        done
    done
    if test -z "$db_library"; then
	case $db_header in
	*/db.h)
	    test -f $dir/lib/libdb.a && db_library=$dir/lib/libdb.a
	    ;;
	esac
    fi
    if test "$db_library"; then
	case $db_header in
	*/db.h)
	    CPPFLAGS="$CPPFLAGS -I"`dirname $db_header`
	    ;;
	*)
	    ln -s $db_header db.h
	    ;;
	esac
	case $db_library in
	*.la)
	    DB_LIB=$db_library
	    ;;
	*.a)
	    minusl=`echo $db_library | sed -e 's/^.*\/lib\(.*\)\.a$/-l\1/'`
	    DB_LIB=-L`dirname $db_library`" $minusl"
	    ;;
	*/lib*.so.*)
	    minusl=`echo $db_library | sed -e 's/^.*\/lib\(.*\)\.so\..*/-l\1/'`
	    DB_LIB=-L`dirname $db_library`" $minusl"
	    ;;
	esac
    fi
fi])

dnl
dnl SFS_SLEEPYCAT(v1 v2 v3 ..., required)
dnl
dnl   Find BekeleyDB version v1, v2, or v3...
dnl      required can be "no" if DB is not required
dnl
AC_DEFUN([SFS_SLEEPYCAT],
[AC_ARG_WITH(db,
--with-db[[[=/usr/local]]]    specify path for BerkeleyDB (from sleepycat.com))
AC_SUBST(DB_DIR)
AC_CONFIG_SUBDIRS($DB_DIR)
AC_SUBST(DB_LIB)
unset DB_LIB

rm -f db.h

for vers in $1; do
    DB_DIR=`cd $srcdir && echo db-$vers.*/dist/`
    if test -d "$srcdir/$DB_DIR"; then
        DB_DIR=`echo $DB_DIR | sed -e 's!/$!!'`
	break
    else
	unset DB_DIR
    fi
done

test -z "${with_db+set}" && with_db=yes

AC_MSG_CHECKING(for BerkeleyDB library)
if test "$DB_DIR" -a "$with_db" = yes; then
    CPPFLAGS="$CPPFLAGS "'-I$(top_builddir)/'"$DB_DIR/dist"
    DB_LIB='$(top_builddir)/'"$DB_DIR/dist/.libs/libdb-*.a"
    AC_MSG_RESULT([using distribution in $DB_DIR subdirectory])
elif test x"$with_db" != xno; then
    if test "$with_db" = yes; then
	for vers in $1; do
	    for dir in "$prefix/BerkeleyDB.$vers" \
			"/usr/BerkeleyDB.$vers" \
			"/usr/local/BerkeleyDB.$vers" \
			$prefix /usr /usr/local; do
		SFS_TRY_SLEEPYCAT_VERSION($vers, $dir)
		test -z "$DB_LIB" || break 2
	    done
	done
    else
	for vers in $1; do
	    SFS_TRY_SLEEPYCAT_VERSION($vers, $with_db)
	    test -z "$DB_LIB" || break
	done
	test -z "$DB_LIB" && AC_MSG_ERROR(Cannot find BerkeleyDB in $with_db)
    fi
fi

if test x"$DB_LIB" != x; then
    AC_MSG_RESULT($DB_LIB)
    USE_DB=yes
else
    AC_MSG_RESULT(no)
    USE_DB=no
    if test "$2" != "no"; then
        AC_MSG_ERROR(Cannot find BerkeleyDB)
    fi
fi

AM_CONDITIONAL(USE_DB, test "$USE_DB" = yes)
])




dnl
dnl Find OpenSSL
dnl
AC_DEFUN([SFS_OPENSSL],
[AC_SUBST(OPENSSL_DIR)
AC_ARG_WITH(openssl,
--with-openssl[[=/usr/local/openssl]]   Find OpenSSL libraries)
AC_MSG_CHECKING([for OpenSSL])
test "$with_openssl" = "yes" && unset with_openssl
unset OPENSSL_DIR
if test -z "$with_openssl"; then
    with_openssl=no
    for dir in /usr/local/openssl/ /usr/local/ssl/ \
		`ls -1d /usr/local/openssl-*/ 2>/dev/null | tail -1`; do
	if test -f $dir/lib/libssl.a -a -f $dir/include/openssl/ssl.h; then
	    with_openssl=`echo $dir | sed -e 's/\/$//'`
	    break
	fi
    done
fi
OPENSSL_DIR="$with_openssl"
AC_MSG_RESULT([$with_openssl])
if test "$with_openssl" = no; then
dnl    if test -z "$with_openssl"; then
dnl	AC_MSG_ERROR([Could not find OpenSSL libraries])
dnl    fi 
    unset OPENSSL_DIR
fi])


dnl
dnl Use dmalloc if requested
dnl
AC_DEFUN([SFS_DMALLOC],
[
dnl AC_ARG_WITH(small-limits,
dnl --with-small-limits       Try to trigger memory allocation bugs,
dnl CPPFLAGS="$CPPFLAGS -DSMALL_LIMITS"
dnl test "${with_dmalloc+set}" = set || with_dmalloc=yes
dnl )
AC_CHECK_HEADERS(memory.h)
AC_ARG_WITH(dmalloc,
--with-dmalloc            use debugging malloc from ftp.letters.com
			  (set MAX_FILE_LEN to 1024 when installing),
pref=$prefix
test "$pref" = NONE && pref=$ac_default_prefix
test "$withval" = yes && withval="${pref}"
test "$withval" || withval="${pref}"
using_dmalloc=no
if test "$withval" != no; then
	AC_DEFINE(DMALLOC, 1, Define if compiling with dmalloc. )
dnl	CPPFLAGS="$CPPFLAGS -DDMALLOC"
	CPPFLAGS="$CPPFLAGS -I${withval}/include"
	LIBS="$LIBS -L${withval}/lib -ldmalloc"
	using_dmalloc=yes
fi)
AM_CONDITIONAL(DMALLOC, test "$using_dmalloc" = yes)
])
dnl
dnl Find perl
dnl
AC_DEFUN([SFS_PERLINFO],
[AC_ARG_WITH(perl,
--with-perl=PATH          Specify perl executable to use,
[case "$withval" in
	yes|no|"") ;;
	*) PERL="$withval" ;;
esac])
if test -z "$PERL" || test ! -x "$PERL"; then
	AC_PATH_PROGS(PERL, perl5 perl)
fi
if test -x "$PERL" && $PERL -e 'require 5.004'; then :; else
	AC_MSG_ERROR("Can\'t find perl 5.004 or later")
fi
AC_CACHE_CHECK(for perl includes, sfs_cv_perl_ccopts,
	sfs_cv_perl_ccopts=`$PERL -MExtUtils::Embed -e ccopts`
	sfs_cv_perl_ccopts=`echo $sfs_cv_perl_ccopts`
)
AC_CACHE_CHECK(for perl libraries, sfs_cv_perl_ldopts,
	sfs_cv_perl_ldopts=`$PERL -MExtUtils::Embed -e ldopts -- -std`
	sfs_cv_perl_ldopts=`echo $sfs_cv_perl_ldopts`
)
AC_CACHE_CHECK(for perl xsubpp, sfs_cv_perl_xsubpp,
	sfs_cv_perl_xsubpp="$PERL "`$PERL -MConfig -e 'print qq(\
	-I$Config{"installarchlib"} -I$Config{"installprivlib"}\
	$Config{"installprivlib"}/ExtUtils/xsubpp\
	-typemap $Config{"installprivlib"}/ExtUtils/typemap)'`
	sfs_cv_perl_xsubpp=`echo $sfs_cv_perl_xsubpp`
)
XSUBPP="$sfs_cv_perl_xsubpp"
PERL_INC="$sfs_cv_perl_ccopts"
PERL_LIB="$sfs_cv_perl_ldopts"
PERL_XSI="$PERL -MExtUtils::Embed -e xsinit -- -std"
AC_SUBST(PERL)
AC_SUBST(PERL_INC)
AC_SUBST(PERL_LIB)
AC_SUBST(PERL_XSI)
AC_SUBST(XSUBPP)
])
dnl'
dnl Various warning flags for gcc.  This must go at the very top,
dnl right after AC_PROG_CC and AC_PROG_CXX.
dnl
AC_DEFUN([SFS_WFLAGS],
[AC_SUBST(NW)
AC_SUBST(WFLAGS)
AC_SUBST(CXXWFLAGS)
AC_SUBST(DEBUG)
AC_SUBST(CXXDEBUG)
AC_SUBST(ECFLAGS)
AC_SUBST(ECXXFLAGS)
AC_SUBST(CXXNOERR)
test -z "${CXXWFLAGS+set}" -a "${WFLAGS+set}" && CXXWFLAGS="$WFLAGS"
test -z "${CXXDEBUG+set}" -a "${DEBUG+set}" && CXXDEBUG="$DEBUG"
test "${DEBUG+set}" || DEBUG="$CFLAGS"
export DEBUG
test "${CXXDEBUG+set}" || CXXDEBUG="$CXXFLAGS"
export CXXDEBUG
case $host_os in
    openbsd*)
	sfs_gnu_WFLAGS="-ansi -Wall -Wsign-compare -Wchar-subscripts -Werror"
	sfs_gnu_CXXWFLAGS="$sfs_gnu_WFLAGS"
	;;
    linux*|freebsd*)
	sfs_gnu_WFLAGS="-Wall -Werror"
	sfs_gnu_CXXWFLAGS="$sfs_gnu_WFLAGS"
	;;
    *)
	sfs_gnu_WFLAGS="-Wall"
	sfs_gnu_CXXWFLAGS="$sfs_gnu_WFLAGS"
	;;
esac
expr "$DEBUG" : '.*-O' > /dev/null \
    || sfs_gnu_WFLAGS="$sfs_gnu_WFLAGS -Wno-unused"
expr "$CXXDEBUG" : '.*-O' > /dev/null \
    || sfs_gnu_CXXWFLAGS="$sfs_gnu_CXXWFLAGS -Wno-unused"
NW='-w'
test "$GCC" = yes -a -z "${WFLAGS+set}" && WFLAGS="$sfs_gnu_WFLAGS"
test "$GXX" = yes -a -z "${CXXWFLAGS+set}" && CXXWFLAGS="$sfs_gnu_CXXWFLAGS"
CXXNOERR=
test "$GXX" = yes && CXXNOERR='-Wno-error'
# Temporarily set CFLAGS to ansi so tests for things like __inline go correctly
if expr "$DEBUG $WFLAGS $ECFLAGS" : '.*-ansi' > /dev/null; then
	CFLAGS="$CFLAGS -ansi"
	ac_cpp="$ac_cpp -ansi"
fi
expr "$CXXDEBUG $CXXWFLAGS $ECXXFLAGS" : '.*-ansi' > /dev/null \
    && CXXFLAGS="$CXXFLAGS -ansi"
])
dnl
dnl SFS_CFLAGS puts the effects of SFS_WFLAGS into place.
dnl This must be called after all tests have been run.
dnl
AC_DEFUN([SFS_CFLAGS],
[unset CFLAGS
unset CXXFLAGS
CFLAGS='$(DEBUG) $(WFLAGS) $(ECFLAGS)'
CXXFLAGS='$(CXXDEBUG) $(CXXWFLAGS) $(ECXXFLAGS)'])
dnl
dnl Check for xdr_u_intNN_t, etc
dnl
AC_DEFUN([SFS_CHECK_XDR],
[
dnl AC_CACHE_CHECK([for a broken <rpc/xdr.h>], sfs_cv_xdr_broken,
dnl AC_EGREP_HEADER(xdr_u_int32_t, [rpc/xdr.h], 
dnl                 sfs_cv_xdr_broken=no, sfs_cv_xdr_broken=yes))
dnl if test "$sfs_cv_xdr_broken" = "yes"; then
dnl     AC_DEFINE(SFS_XDR_BROKEN)
dnl     dnl We need to know the following in order to fix rpc/xdr.h: 
dnl     AC_CHECK_SIZEOF(short)
dnl     AC_CHECK_SIZEOF(int)
dnl     AC_CHECK_SIZEOF(long)
dnl fi
SFS_CHECK_DECL(xdr_callmsg, rpc/rpc.h)
AC_CACHE_CHECK(what second xdr_getlong arg points to, sfs_cv_xdrlong_t,
AC_EGREP_HEADER(\*x_getlong.* long *\*, [rpc/rpc.h], 
                sfs_cv_xdrlong_t=long)
if test -z "$sfs_cv_xdrlong_t"; then
    AC_EGREP_HEADER(\*x_getlong.* int *\*, [rpc/rpc.h], 
                    sfs_cv_xdrlong_t=int)
elif test -z "$sfs_cv_xdrlong_t"; then
    sfs_cv_xdrlong_t=u_int32_t
fi)
AC_DEFINE_UNQUOTED(xdrlong_t, $sfs_cv_xdrlong_t,
		   What the second argument of xdr_getlong points to)
])
dnl
dnl Check for random device
dnl
AC_DEFUN([SFS_DEV_RANDOM],
[AC_CACHE_CHECK([for kernel random number generator], sfs_cv_dev_random,
for dev in /dev/urandom /dev/srandom /dev/random /dev/srnd /dev/rnd; do
    if test -c "$dev"; then
	sfs_cv_dev_random=$dev
	break
    fi
    test "$sfs_cv_dev_random" || sfs_cv_dev_random=no
done)
if test "$sfs_cv_dev_random" != no; then
pushdef([SFS_DEV_RANDOM], [[SFS_DEV_RANDOM]])
    AC_DEFINE_UNQUOTED([SFS_DEV_RANDOM], "$sfs_cv_dev_random",
		       [Path to the strongest random number device, if any.])
popdef([SFS_DEV_RANDOM])
fi
])
dnl
dnl Check for getgrouplist function
dnl
AC_DEFUN([SFS_GETGROUPLIST_TRYGID], [
if test "$sfs_cv_grouplist_t" != gid_t; then
    AC_TRY_COMPILE([
#include <sys/types.h>
#include <unistd.h>
#include <grp.h>
int getgrouplist ([$*]);
		    ], 0, sfs_cv_grouplist_t=gid_t)
fi
])
AC_DEFUN([SFS_GETGROUPLIST],
[AC_CHECK_FUNCS(getgrouplist)
AC_CACHE_CHECK([whether getgrouplist uses int or gid_t], sfs_cv_grouplist_t,
    if test "$ac_cv_func_getgrouplist" = yes; then
	sfs_cv_grouplist_t=int
	AC_EGREP_HEADER(getgrouplist.*gid_t *\*, unistd.h,
			sfs_cv_grouplist_t=gid_t)
	if test "$sfs_cv_grouplist_t" != gid_t; then
	    AC_EGREP_HEADER(getgrouplist.*gid_t *\*, grp.h,
			    sfs_cv_grouplist_t=gid_t)
	fi

	SFS_GETGROUPLIST_TRYGID(const char *, gid_t, gid_t *, int *)
	SFS_GETGROUPLIST_TRYGID(const char *, int , gid_t *, int *)
	SFS_GETGROUPLIST_TRYGID(char *, gid_t, gid_t *, int *)
	SFS_GETGROUPLIST_TRYGID(char *, int, gid_t *, int *)
    else
	sfs_cv_grouplist_t=gid_t
    fi)
AC_DEFINE_UNQUOTED([GROUPLIST_T], $sfs_cv_grouplist_t,
	[Type pointed to by 3rd argument of getgrouplist.])])
dnl
dnl Check if <grp.h> is needed for setgroups declaration (linux)
dnl
AC_DEFUN([SFS_SETGROUPS],
[AC_CACHE_CHECK([for setgroups declaration in grp.h],
	sfs_cv_setgroups_grp_h,
	AC_EGREP_HEADER(setgroups, grp.h,
		sfs_cv_setgroups_grp_h=yes, sfs_cv_setgroups_grp_h=no))
if test "$sfs_cv_setgroups_grp_h" = yes; then
AC_DEFINE([SETGROUPS_NEEDS_GRP_H], 1,
	[Define if setgroups is declared in <grp.h>.])
fi])
dnl
dnl Check if authunix_create is broken and takes a gid_t *
dnl
AC_DEFUN([SFS_AUTHUNIX_GROUP_T],
[AC_CACHE_CHECK([what last authunix_create arg points to],
	sfs_cv_authunix_group_t,
AC_EGREP_HEADER([(authunix|authsys)_create.*(uid_t|gid_t)], rpc/rpc.h,
	sfs_cv_authunix_group_t=gid_t, sfs_cv_authunix_group_t=int))
if test "$sfs_cv_authunix_group_t" = gid_t; then
    AC_DEFINE_UNQUOTED(AUTHUNIX_GID_T, 1,
	[Define if last argument of authunix_create is a gid_t *.])
fi])
dnl
dnl Check the type of the x_ops field in XDR
dnl
AC_DEFUN([SFS_XDR_OPS_T],
[AC_CACHE_CHECK([type of XDR::x_ops], sfs_cv_xdr_ops_t,
AC_EGREP_HEADER([xdr_ops *\* *x_ops;], rpc/xdr.h,
	sfs_cv_xdr_ops_t=xdr_ops, sfs_cv_xdr_ops_t=XDR::xdr_ops))
AC_DEFINE_UNQUOTED(xdr_ops_t, $sfs_cv_xdr_ops_t,
	[The C++ type name of the x_ops field in struct XDR.])])
dnl
dnl Find installed SFS libraries
dnl This is not for SFS, but for other packages that use SFS.
dnl
AC_DEFUN([SFS_SFS],
[AC_ARG_WITH(sfs,
--with-sfs[[=PATH]]         specify location of SFS libraries)
if test "$with_sfs" = yes -o "$with_sfs" = ""; then
    for dir in "$prefix" /usr/local /usr; do
	if test -f $dir/lib/sfs/libasync.la; then
	    with_sfs=$dir
	    break
	fi
    done
fi
case "$with_sfs" in
    /*) ;;
    *) with_sfs="$PWD/$with_sfs" ;;
esac

if test -f ${with_sfs}/Makefile -a -f ${with_sfs}/autoconf.h; then
    if egrep -q '#define DMALLOC' ${with_sfs}/autoconf.h > /dev/null; then
	test -z "$with_dmalloc" -o "$with_dmalloc" = no && with_dmalloc=yes
    elif test "$with_dmalloc" -a "$with_dmalloc" != no; then
	AC_MSG_ERROR("SFS libraries not compiled with dmalloc")
    fi
    sfssrcdir=`sed -ne 's/^srcdir *= *//p' ${with_sfs}/Makefile`
    case "$sfssrcdir" in
	/*) ;;
	*) sfssrcdir="${with_sfs}/${sfssrcdir}" ;;
    esac

    CPPFLAGS="$CPPFLAGS -I${with_sfs}"
    for lib in async arpc crypt sfsmisc; do
	CPPFLAGS="$CPPFLAGS -I${sfssrcdir}/$lib"
    done
    CPPFLAGS="$CPPFLAGS -I${with_sfs}/svc"
    LIBASYNC=${with_sfs}/async/libasync.la
    LIBARPC=${with_sfs}/arpc/libarpc.la
    LIBSFSCRYPT=${with_sfs}/crypt/libsfscrypt.la
    LIBSFSMISC=${with_sfs}/sfsmisc/libsfsmisc.la
    LIBSVC=${with_sfs}/svc/libsvc.la
    MALLOCK=${with_sfs}/sfsmisc/mallock.o
    RPCC=${with_sfs}/rpcc/rpcc
    TAME=${with_sfs}/tame/tame
elif test -f ${with_sfs}/include/sfs/autoconf.h \
	-a -f ${with_sfs}/lib/sfs/libasync.la; then
    sfsincludedir="${with_sfs}/include/sfs"
    sfslibdir=${with_sfs}/lib/sfs
    if egrep '#define DMALLOC' ${sfsincludedir}/autoconf.h > /dev/null; then
	test -z "$with_dmalloc" -o "$with_dmalloc" = no && with_dmalloc=yes
    else
	with_dmalloc=no
    fi
    CPPFLAGS="$CPPFLAGS -I${sfsincludedir}"
    LIBASYNC=${sfslibdir}/libasync.la
    LIBARPC=${sfslibdir}/libarpc.la
    LIBSFSCRYPT=${sfslibdir}/libsfscrypt.la
    LIBSFSMISC=${sfslibdir}/libsfsmisc.la
    LIBSVC=${sfslibdir}/libsvc.la
    MALLOCK=${sfslibdir}/mallock.o
    RPCC=${with_sfs}/bin/rpcc
    TAME=${with_sfs}/bin/tame
else
    AC_MSG_ERROR("Can\'t find SFS libraries")
fi

if test "$enable_static" = yes -a -z "${NOPAGING+set}"; then
    case "$host_os" in
	openbsd*)
	    test "$ac_cv_prog_gcc" = yes && NOPAGING="-Wl,-Bstatic,-N"
	    MALLOCK=		# mallock.o panics the OpenBSD kernel
	;;
	freebsd*)
	    test "$ac_cv_prog_gcc" = yes && NOPAGING="-Wl,-Bstatic"
	;;
    esac
fi

sfslibdir='$(libdir)/sfs'
sfsincludedir='$(libdir)/include'
AC_SUBST(sfslibdir)
AC_SUBST(sfsincludedir)

AC_SUBST(LIBASYNC)
AC_SUBST(LIBARPC)
AC_SUBST(LIBSFSCRYPT)
AC_SUBST(LIBSFSMISC)
AC_SUBST(LIBSVC)
AC_SUBST(RPCC)
AC_SUBST(TAME)
AC_SUBST(MALLOCK)
AC_SUBST(NOPAGING)

SFS_GMP
SFS_DMALLOC

LDEPS='$(LIBSFSMISC) $(LIBSVC) $(LIBSFSCRYPT) $(LIBARPC) $(LIBASYNC)'
LDADD="$LDEPS "'$(LIBGMP)'
AC_SUBST(LDEPS)
AC_SUBST(LDADD)
])
dnl
dnl Find Chord libraries
dnl This is not for Chord, but for other packages that use Chord.
dnl
AC_DEFUN([CHORD_CHORD],
[AC_ARG_WITH(chord,
--with-chord[[=PATH]]       specify location of Chord libraries)
if test "$with_chord" = yes -o "$with_chord" = ""; then
    for dir in "$prefix" /usr/local /usr; do
	if test -f $dir/lib/chord/libchord.a; then
	    with_chord=$dir
	    break
	fi
    done
fi
case "$with_chord" in
    /*) ;;
    *) with_chord="$PWD/$with_chord" ;;
esac

if test -f ${with_chord}/Makefile -a -f ${with_chord}/config.h; then
    if egrep -q '#define DMALLOC' ${with_chord}/config.h > /dev/null; then
	test -z "$with_dmalloc" -o "$with_dmalloc" = no && with_dmalloc=yes
    elif test "$with_dmalloc" -a "$with_dmalloc" != no; then
	AC_MSG_ERROR("Chord libraries not compiled with dmalloc")
    fi
    chordsrcdir=`sed -ne 's/^srcdir *= *//p' ${with_chord}/Makefile`
    case "$chordsrcdir" in
	/*) ;;
	*) chordsrcdir="${with_chord}/${chordsrcdir}" ;;
    esac

    CPPFLAGS="$CPPFLAGS -I${with_chord}"
    for lib in merkle svc chord dhc dhash sfsrodb lsd proxy; do
	CPPFLAGS="$CPPFLAGS -I${chordsrcdir}/$lib"
    done
    CPPFLAGS="$CPPFLAGS -I${with_chord}/svc"

    LIBDHASHCLIENT=${with_chord}/dhashclient.a
    LIBDHASH=${with_chord}/dhash/libdhash.a
    LIBDHC=${with_chord}/dhc/libdhc.a
    LIBMERKLE=${with_chord}/merkle/libmerkle.a
    LIBCHORDUTIL=${with_chord}/util/libutil.a
    LIBCHORD=${with_chord}/chord/libchord.a
    LIBCHORDSVC=${with_chord}/svc/libsvc.la
    LIBCHORDSFSRODB=${with_chord}/sfsrodb/libsfsrodb.a
elif test -f ${with_chord}/include/chord/config.h \
	-a -f ${with_chord}/lib/chord/libasync.la; then
dnl this is currently  DEAD CODE branch
    chordincludedir="${with_chord}/include/chord"
    chordlibdir=${with_chord}/lib/chord
    if egrep '#define DMALLOC' ${chordincludedir}/config.h > /dev/null; then
	test -z "$with_dmalloc" -o "$with_dmalloc" = no && with_dmalloc=yes
    else
	with_dmalloc=no
    fi
    CPPFLAGS="$CPPFLAGS -I${chordincludedir}"
    LIBDHASH=${chordlibdir}/libdhash.a
    LIBMERKLE=${chordlibdir}/libmerkle.a
    LIBCHORD=${chordlibdir}/libchord.a
    LIBCHORDSVC=${chordlibdir}/libsvc.la
    LIBCHORDSFSRODB=${chordlibdir}/libsfsrodb.a
else
    AC_MSG_ERROR("Can\'t find Chord libraries")
fi

if test "$enable_static" = yes -a -z "${NOPAGING+set}"; then
    case "$host_os" in
	openbsd*)
	    test "$ac_cv_prog_gcc" = yes && NOPAGING="-Wl,-Bstatic,-N"
	    MALLOCK=		# mallock.o panics the OpenBSD kernel
	;;
	freebsd*)
	    test "$ac_cv_prog_gcc" = yes && NOPAGING="-Wl,-Bstatic"
	;;
    esac
fi

chordlibdir='$(libdir)/chord'
chordincludedir='$(libdir)/include'
AC_SUBST(chordlibdir)
AC_SUBST(chordincludedir)

AC_SUBST(LIBDHASHCLIENT)
AC_SUBST(LIBDHASH)
AC_SUBST(LIBDHC)
AC_SUBST(LIBMERKLE)
AC_SUBST(LIBCHORDUTIL)
AC_SUBST(LIBCHORD)
AC_SUBST(LIBCHORDSVC)
AC_SUBST(LIBCHORDSFSRODB)

dnl Frank's hacked DB3 macro that forces DB3 to be loaded
SFS_DB3 

CPPFLAGS="$CPPFLAGS -DSLEEPYCAT"
LDEPS='$(LIBDHASHCLIENT) $(LIBDHASH) $(LIBDHC) $(LIBMERKLE) $(LIBCHORD) $(LIBCHORDUTIL) $(LIBCHORDSVC) $(LIBCHORDSFSRODB)'" $LDADD"
LDADD="$LDEPS "'$(DB3_LIB)'
AC_SUBST(LDEPS)
AC_SUBST(LDADD)
])
