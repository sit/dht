## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.4 2002/10/23 20:22:23 benjie Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

noinst_PROGRAMS = melody it


melody_SOURCES = block.C file.C cs_output.C dir.C dirpage.C http.C melody.C
melody_LDADD = ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la \
               ../sfsrodb/libsfsrodb.a $(DBLIB) $(LDADD)

it_SOURCES = it.C cs_output.C

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

