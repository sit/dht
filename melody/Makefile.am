## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.8 2003/01/11 00:45:05 jsr Exp $

$(PROGRAMS): $(LDEPS) 

noinst_PROGRAMS = melody it


melody_SOURCES = block.C file.C cs_output.C dir.C dirpage.C http.C melody.C retrieve_manager.C
melody_LDADD = ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la \
               ../sfsrodb/libsfsrodb.a $(DBLIB) $(LDADD)

it_SOURCES = it.C cs_output.C

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

