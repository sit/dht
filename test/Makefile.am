## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.2 2002/06/13 18:59:51 thomer Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

noinst_PROGRAMS = example testslave
noinst_LIBRARIES = libdtest.a

libdtest_a_SOURCES = testmaster.C test.C
example_SOURCES = example.C
example_LDADD = ./libdtest.a ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la $(LDADD)
testslave_SOURCES = testslave.C
testslave_LDADD = ./libdtest.a ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la $(LDADD)


EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

