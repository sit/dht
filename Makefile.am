## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.30 2003/08/21 20:46:44 athicha Exp $


BASEDIRS = svc utils merkle chord dhc dhash sfsrodb lsd devel
SUBDIRS_ALL = $(BASEDIRS) chordcd test tst melody
SUBDIRS_SOME = $(BASEDIRS)
if COND_ALL
SUBDIRS = $(SUBDIRS_ALL)
else
SUBDIRS = $(SUBDIRS_SOME)
endif

CLEANFILES = core *.core *~

EXTRA_DIST = .cvsignore
MAINTAINERCLEANFILES = aclocal.m4 install-sh mkinstalldirs \
	missing configure config.guess config.sub config.h.in \
	ltconfig ltmain.sh INSTALL COPYING Makefile.in stamp-h.in

dist-hook:
	rm -f $(srcdir)/p2psim/p2psim.core
	tar czvf $(distdir)/p2psim.tgz -C $(srcdir) p2psim
