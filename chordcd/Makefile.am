## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.1 2001/08/27 03:02:47 fdabek Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

bin_PROGRAMS = chordcd

noinst_HEADERS = \
chordcd.h

chordcd_SOURCES = \
server.C chordcd.C

# XXX - this should not be required!
#chordcd_LDADD = $(DBLIB) $(LDADD)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

