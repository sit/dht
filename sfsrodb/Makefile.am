## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.7 2003/01/18 13:13:50 jsr Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

noinst_LIBRARIES = libsfsrodb.a

bin_PROGRAMS = sfsrodb 

noinst_HEADERS = \
sfsrodb.h sfsrodb_core.h

libsfsrodb_a_SOURCES = \
sfsrodb_core.C dbfe.C

sfsrodb_SOURCES = \
sfsrodb.C 

#test_callback_SOURCES = test_callback.C
#test_fhdb_SOURCES = sfsrodb_core.C test_fhdb.C fhdb.C

sfsrodb_LDADD = ../dhash/libdhash.a ../svc/libsvc.la $(DBLIB) $(LDADD)


#test_callback_LDADD = $(DBLIB) $(LDADD)


EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
