## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.12 2004/01/16 13:43:22 sit Exp $

$(PROGRAMS): $(LDEPS) libsfsrodb.a

noinst_LIBRARIES = libsfsrodb.a

bin_PROGRAMS = sfsrodb 

noinst_HEADERS = \
sfsrodb.h sfsrodb_core.h dbfe.h

libsfsrodb_a_SOURCES = \
dbfe.C

sfsrodb_SOURCES = \
sfsrodb.C sfsrodb_core.C 

#test_callback_SOURCES = test_callback.C
#test_fhdb_SOURCES = sfsrodb_core.C test_fhdb.C fhdb.C

sfsrodb_LDADD = ../dhash/libdhashclient.a ../svc/libsvc.la $(DBLIB) $(LDADD)

#test_callback_LDADD = $(DBLIB) $(LDADD)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
