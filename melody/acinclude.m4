dnl $Id: acinclude.m4,v 1.1 2003/01/06 22:56:11 jsr Exp $

dnl
dnl Find full path to program
dnl
AC_DEFUN(SFS_PATH_PROG,
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
AC_DEFUN(SFS_PATH_CPP,
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
AC_DEFUN(SFS_CHECK_DECL,
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
dnl Use -lresolv only if we need it
dnl
AC_DEFUN(SFS_FIND_RESOLV,
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
dnl Find GMP
dnl
AC_DEFUN(SFS_GMP,
[AC_ARG_WITH(gmp,
--with-gmp[[=/usr/local]]   specify path for gmp)
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
	CPPFLAGS="$CPPFLAGS "'-I$(top_srcdir)/'"$GMP_DIR"
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
	CPPFLAGS="$CPPFLAGS -I${with_gmp}/include"
    elif test -f ${with_gmp}/include/gmp3/gmp.h; then
	CPPFLAGS="$CPPFLAGS -I${with_gmp}/include/gmp3"
    elif test -f ${with_gmp}/include/gmp2/gmp.h; then
	CPPFLAGS="$CPPFLAGS -I${with_gmp}/include/gmp2"
    else
	AC_MSG_ERROR([Could not find gmp.h header])
    fi

    #LDFLAGS="$LDFLAGS -L${with_gmp}/lib"
    #LIBGMP=-lgmp
    if test -f "${with_gmp}/lib/libgmp.la"; then
	LIBGMP="${with_gmp}/lib/libgmp.la"
    else
	LIBGMP="${with_gmp}/lib/libgmp.a"
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
    elif test -f /usr/lib/libgmp.a; then
	# FreeBSD (among others) has a broken gmp shared library
	LIBGMP=/usr/lib/libgmp.a
    else
	LIBGMP=-lgmp
    fi
fi

AC_CONFIG_SUBDIRS($GMP_DIR)

ac_save_CFLAGS="$CFLAGS"
test "$GMP_DIR" && CFLAGS="$CFLAGS -I${srcdir}/${GMP_DIR}"

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
dnl
dnl Find BekeleyDB 3
dnl
AC_DEFUN(SFS_DB3,
[AC_SUBST(DB3_DIR)
AC_CONFIG_SUBDIRS($DB3_DIR)
AC_SUBST(DB3_LIB)
unset DB3_LIB
AC_ARG_WITH(db3,
--with-db3[[=/usr/local]]   Use Btree from BerkeleyDB-3 library)

DB3_DIR=`cd $srcdir && echo db-3.*/dist/`
if test -d "$srcdir/$DB3_DIR"; then
    DB3_DIR=`echo $DB3_DIR | sed -e 's!/$!!'`
else
    unset DB3_DIR
fi

if test ! "${with_db3+set}"; then
	with_db3=yes
fi

if test "$with_db3" != no; then
    AC_MSG_CHECKING([for DB3 library])
    if test "$DB3_DIR" -a "$with_db3" = yes; then
	CPPFLAGS="$CPPFLAGS "'-I$(top_builddir)/'"$DB3_DIR"
	DB3_LIB='-L$(top_builddir)/'"$DB3_DIR -ldb"
	AC_MSG_RESULT([using distribution in $DB3_DIR subdirectory])
    else
	libdbrx='^libdb([[3.-]].*)?.(la|so)$'
	if test "$with_db3" = yes; then
	    for dir in "$prefix/BerkeleyDB.3.1" /usr/local/BerkeleyDB.3.1 \
		    "$prefix/BerkeleyDB.3.0" /usr/local/BerkeleyDB.3.0 \
		    /usr "$prefix" /usr/local; do
		test -f $dir/include/db.h -o -f $dir/include/db3.h \
			-o -f $dir/include/db3/db.h || continue
		if test -f $dir/lib/libdb.a \
			|| ls $dir/lib | egrep -q "$libdbrx"; then
		    with_db3="$dir"
		    break
		fi
	    done
	fi

	if test -f $with_db3/include/db3.h; then
	    AC_DEFINE(HAVE_DB3_H, 1, [Define if BerkeleyDB header is db3.h.])
   	    if test "$with_db3" != /usr; then
	      CPPFLAGS="$CPPFLAGS -I${with_db3}/include"
	    fi
	elif test -f $with_db3/include/db3/db.h; then
   	    if test "$with_db3" != /usr; then
	      CPPFLAGS="$CPPFLAGS -I${with_db3}/include/db3"
	    fi
	elif test -f $with_db3/include/db.h; then
	    if test "$with_db3" != /usr; then
	      CPPFLAGS="$CPPFLAGS -I${with_db3}/include"
	    fi
	else
	    AC_MSG_ERROR([Could not find BerkeleyDB library version 3])
	fi

	DB3_LIB=`ls $with_db3/lib | egrep "$libdbrx" | tail -1`
	if test -f "$with_db3/lib/$DB3_LIB"; then
	    DB3_LIB="$with_db3/lib/$DB3_LIB"
	elif test "$with_db3" = /usr; then
	    with_db3=yes
	    DB3_LIB="-ldb"
	else
	    DB3_LIB="-L${with_db3}/lib -ldb"
	fi
	AC_MSG_RESULT([$with_db3])
    fi
fi

AM_CONDITIONAL(USE_DB3, test "${with_db3}" != no)
])
dnl
dnl Use dmalloc if requested
dnl
AC_DEFUN(SFS_DMALLOC,
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
AC_DEFUN(SFS_PERLINFO,
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
AC_DEFUN(SFS_WFLAGS,
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
AC_DEFUN(SFS_CFLAGS,
[unset CFLAGS
unset CXXFLAGS
CFLAGS='$(DEBUG) $(WFLAGS) $(ECFLAGS)'
CXXFLAGS='$(CXXDEBUG) $(CXXWFLAGS) $(ECXXFLAGS)'])
dnl
dnl Find installed SFS libraries
dnl This is not for SFS, but for other packages that use SFS.
dnl
AC_DEFUN(SFS_SFS,
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
dnl Find installed CHORD libraries
dnl This is not for CHORD, but for other packages that use CHORD.
dnl
AC_DEFUN(CHORD_CHORD,
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
    for lib in merkle svc chord dhash sfsrodb lsd; do
	CPPFLAGS="$CPPFLAGS -I${chordsrcdir}/$lib"
    done
    CPPFLAGS="$CPPFLAGS -I${with_chord}/svc"

    LIBDHASH=${with_chord}/dhash/libdhash.a
    LIBMERKLE=${with_chord}/merkle/libmerkle.a
    LIBCHORD=${with_chord}/chord/libchord.a
    LIBCHORDSVC=${with_chord}/svc/libsvc.la
    LIBCHORDSFSRODB=${with_chord}/sfsrodb/libsfsrodb.a
elif test -f ${with_chord}/include/chord/config.h \
	-a -f ${with_chord}/lib/chord/libasync.la; then
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

AC_SUBST(LIBDHASH)
AC_SUBST(LIBMERKLE)
AC_SUBST(LIBCHORD)
AC_SUBST(LIBCHORDSVC)
AC_SUBST(LIBCHORDSFSRODB)

dnl Frank's hacked DB3 macro that forces DB3 to be loaded
SFS_DB3 

CPPFLAGS="$CPPFLAGS -DSLEEPYCAT"
LDEPS='$(LIBDHASH) $(LIBCHORD) $(LIBCHORDSVC) $(LIBCHORDSFSRODB)'" $LDADD"
LDADD='$(LIBDHASH) $(LIBCHORD) $(LIBCHORDSVC) $(LIBCHORDSFSRODB) $(DB3_LIB)'" $LDEPS"
AC_SUBST(LDEPS)
AC_SUBST(LDADD)
])
