## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.6 2002/11/08 05:15:51 thomer Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

noinst_PROGRAMS = example block
noinst_LIBRARIES = libdtest.a

libdtest_a_SOURCES = testmaster.C test.C
example_SOURCES = example.C
example_LDADD = ./libdtest.a ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la ../sfsrodb/libsfsrodb.a $(DBLIB) $(LDADD)

block_SOURCES = block.C
block_LDADD = ./libdtest.a ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la ../sfsrodb/libsfsrodb.a $(DBLIB) $(LDADD)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
