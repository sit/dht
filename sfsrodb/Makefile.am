## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.3 2001/03/02 21:08:42 fdabek Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

noinst_LIBRARIES = libsfsrodb.a

bin_PROGRAMS = sfsrodb pulldb printdb

noinst_HEADERS = \
sfsrodb.h sfsrodb_core.h

libsfsrodb_a_SOURCES = \
sfsrodb_core.C dbfe.C

sfsrodb_SOURCES = \
sfsrodb.C 

printdb_SOURCES = \
printdb.C

pulldb_SOURCES = \
pulldb.C

#test_callback_SOURCES = test_callback.C
#test_fhdb_SOURCES = sfsrodb_core.C test_fhdb.C fhdb.C

sfsrodb_LDADD = ../svc/libsvc.la $(DBLIB) $(LDADD)
printdb_LDADD = $(DBLIB) $(LDADD)
pulldb_LDADD = $(DBLIB) $(LDADD)
#test_callback_LDADD = $(DBLIB) $(LDADD)


EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
