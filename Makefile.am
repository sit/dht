## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.25 2003/07/11 15:00:14 athicha Exp $

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

