## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.7 2002/11/11 20:48:15 thomer Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

noinst_PROGRAMS = example block testslave
noinst_LIBRARIES = libdtest.a

libdtest_a_SOURCES = testmaster.C test.C
example_SOURCES = example.C
example_LDADD = ./libdtest.a ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la ../sfsrodb/libsfsrodb.a $(DBLIB) $(LDADD)

testslave_SOURCES = testslave.C
testslave_LDADD = ./libdtest.a ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la ../sfsrodb/libsfsrodb.a $(DBLIB) $(LDADD)

block_SOURCES = block.C
block_LDADD = ./libdtest.a ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la ../sfsrodb/libsfsrodb.a $(DBLIB) $(LDADD)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
