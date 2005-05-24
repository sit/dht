## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.14 2005/05/24 20:58:57 sit Exp $

$(PROGRAMS): $(LDEPS) libsfsrodb.a

noinst_LIBRARIES = libsfsrodb.a

bin_PROGRAMS = sfsrodb 

TESTS = test_dbfe

check_PROGRAMS = $(TESTS)

noinst_HEADERS = \
sfsrodb.h sfsrodb_core.h dbfe.h

libsfsrodb_a_SOURCES = \
dbfe.C

sfsrodb_SOURCES = \
sfsrodb.C sfsrodb_core.C 

#test_callback_SOURCES = test_callback.C
#test_fhdb_SOURCES = sfsrodb_core.C test_fhdb.C fhdb.C

sfsrodb_LDADD = ../dhash/libdhashclient.a ../utils/libutil.a ../svc/libsvc.la $(DBLIB) $(LDADD)

#test_callback_LDADD = $(DBLIB) $(LDADD)

test_dbfe_SOURCES = test_dbfe.C
test_dbfe_LDADD = libsfsrodb.a $(DBLIB) $(LDADD)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
