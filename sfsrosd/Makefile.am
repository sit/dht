## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.1 2001/01/16 22:00:09 fdabek Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

bin_PROGRAMS = sfsrosd

noinst_HEADERS = \
sfsrosd.h sfsdb.h

sfsrosd_SOURCES = sfsrosd.C client.C sfsdb.C

sfsrosd_LDADD = $(DBLIB) $(LDADD)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

