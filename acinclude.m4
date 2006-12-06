m4_include(acsfs.m4)
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
    for lib in svc utils merkle chord dhash; do
	CPPFLAGS="$CPPFLAGS -I${chordsrcdir}/$lib"
    done
    CPPFLAGS="$CPPFLAGS -I${with_chord}/svc"

    LIBCHORDSVC=${with_chord}/svc/libsvc.la
    LIBCHORDUTIL=${with_chord}/utils/libutil.a
    LIBCHORD=${with_chord}/chord/libchord.a
    LIBMERKLE=${with_chord}/merkle/libmerkle.a
    LIBDHASH=${with_chord}/dhash/libdhash.a
    LIBDHASHCLIENT=${with_chord}/dhash/libdhashclient.a
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
    LIBCHORDSVC=${chordlibdir}/libsvc.la
    LIBCHORDUTIL=${chordlibdir}/libutil.a
    LIBCHORD=${chordlibdir}/libchord.a
    LIBMERKLE=${chordlibdir}/libmerkle.a
    LIBDHASH=${chordlibdir}/libdhash.a
    LIBDHASHCLIENT=${chordlibdir}/libdhashclient.a
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

AC_SUBST(LIBCHORDSVC)
AC_SUBST(LIBCHORDUTIL)
AC_SUBST(LIBCHORD)
AC_SUBST(LIBMERKLE)
AC_SUBST(LIBDHASH)
AC_SUBST(LIBDHASHCLIENT)

CPPFLAGS="$CPPFLAGS -DSLEEPYCAT"
LDEPS='$(LIBDHASHCLIENT) $(LIBDHASH) $(LIBMERKLE) $(LIBCHORD) $(LIBCHORDUTIL) $(LIBCHORDSVC) '" $LDADD"
AC_SUBST(LDEPS)
AC_SUBST(LDADD)
])
