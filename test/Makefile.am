## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.3 2002/07/02 08:26:55 thomer Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

noinst_PROGRAMS = example testslave
noinst_LIBRARIES = libdtest.a


SUFFIXES = .x .C .h
.x.h:
	-$(RPCC) -h -DSFSSVC $< || rm -f $@
.x.C:
	-$(RPCC) -c -DSFSSVC $< || rm -f $@
$(RPCC):
	@:


test_prot.h: test_prot.x # $(RPCC)
test_prot.C: test_prot.x # $(RPCC)
test_prot.o: test_prot.h
test_prot.lo: test_prot.h

libdtest_a_SOURCES = testmaster.C test.C test_prot.C
example_SOURCES = example.C
example_LDADD = ./libdtest.a ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la $(LDADD)
testslave_SOURCES = testslave.C
testslave_LDADD = ./libdtest.a ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la $(LDADD)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
