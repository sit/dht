## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.2 2001/03/02 21:08:43 fdabek Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

bin_PROGRAMS = sfsrosd

noinst_HEADERS = \
sfsrosd.h sfsdb.h

sfsrosd_SOURCES = sfsrosd.C client.C sfsdb.C

sfsrosd_LDADD = ../svc/libsvc.la $(DBLIB) $(LDADD)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

