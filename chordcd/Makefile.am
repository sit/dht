## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.4 2002/10/21 18:56:22 fdabek Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

bin_PROGRAMS = chordcd

noinst_HEADERS = \
chordcd.h

chordcd_SOURCES = \
server.C chordcd.C

# XXX - this should not be required!
chordcd_LDADD = ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la \
	../sfsrodb/libsfsrodb.a $(DBLIB) $(LDADD)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

