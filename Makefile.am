## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.21 2003/01/28 00:31:01 fdabek Exp $

#SUBDIRS = svc sfsrodb sfsrocd sfsrosd chord dhash lsd 
DIRS = svc merkle chord dhash sfsrodb lsd devel
if COND_ALL
   DIRS = svc merkle chord dhash sfsrodb lsd devel chordcd test tst melody
endif

SUBDIRS = $(DIRS)

CLEANFILES = core *.core *~

EXTRA_DIST = .cvsignore
MAINTAINERCLEANFILES = aclocal.m4 install-sh mkinstalldirs \
	missing configure config.guess config.sub config.h.in \
	ltconfig ltmain.sh INSTALL COPYING Makefile.in stamp-h.in

