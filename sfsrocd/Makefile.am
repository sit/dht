## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.1 2001/01/16 22:00:08 fdabek Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

bin_PROGRAMS = sfsrocd

noinst_HEADERS = \
sfsrocd.h

sfsrocd_SOURCES = \
server.C sfsrocd.C

# XXX - this should not be required!
sfsrocd_LDADD = $(DBLIB) $(LDADD)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

