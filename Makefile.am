## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.29 2003/08/13 15:31:57 jsr Exp $


BASEDIRS = svc utils merkle chord dhash sfsrodb lsd proxy devel
SUBDIRS_ALL = $(BASEDIRS) chordcd test tst melody usenet
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
