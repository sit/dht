## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.6 2003/01/18 13:13:35 jsr Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

bin_PROGRAMS = chordcd

noinst_HEADERS = \
chordcd.h

chordcd_SOURCES = \
server.C chordcd.C

# XXX - this should not be required!
chordcd_LDADD =  ../dhash/libdhash.a ../svc/libsvc.la $(LDADD)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

