## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.9 2002/11/25 20:37:34 thomer Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

bin_PROGRAMS = block testslave
noinst_PROGRAMS = example1 example2
noinst_LIBRARIES = libdtest.a

libdtest_a_SOURCES = testmaster.C test.C
example1_SOURCES = example1.C
example1_LDADD = ./libdtest.a ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la ../sfsrodb/libsfsrodb.a $(DBLIB) $(LDADD)

example2_SOURCES = example2.C
example2_LDADD = ./libdtest.a ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la ../sfsrodb/libsfsrodb.a $(DBLIB) $(LDADD)

testslave_SOURCES = testslave.C
testslave_LDADD = ./libdtest.a ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la ../sfsrodb/libsfsrodb.a $(DBLIB) $(LDADD)

block_SOURCES = block.C
block_LDADD = ./libdtest.a ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la ../sfsrodb/libsfsrodb.a $(DBLIB) $(LDADD)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
