## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.8 2003/07/16 17:00:47 sit Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

bin_PROGRAMS = chordcd

noinst_HEADERS = \
chordcd.h

chordcd_SOURCES = \
server.C chordcd.C

chordcd_LDADD =  ../dhash/libdhashclient.a ../svc/libsvc.la $(LDADD)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

