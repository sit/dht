## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.10 2004/10/02 03:02:52 sit Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

bin_PROGRAMS = chordcd

chordcd_SOURCES = server.C chordcd.C chordcd.h lrucache.h
chordcd_LDADD =  ../dhash/libdhashclient.a ../utils/libutil.a ../svc/libsvc.la $(LDADD)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

