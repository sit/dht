## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.1 2002/06/13 06:36:08 thomer Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

noinst_PROGRAMS = example testslave

example_SOURCES = example.C testmaster.C test.C
example_LDADD = ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la $(LDADD)
testslave_SOURCES = testslave.C test.C
testslave_LDADD = ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la $(LDADD)


EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

