## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.5 2002/10/23 20:22:24 benjie Exp $

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

$(DEP_FILES): test_prot.h

test_prot.h: test_prot.x # $(RPCC)
test_prot.C: test_prot.x # $(RPCC)
test_prot.o: test_prot.h
test_prot.lo: test_prot.h

libdtest_a_SOURCES = testmaster.C test.C test_prot.C
example_SOURCES = example.C
example_LDADD = ./libdtest.a ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la ../sfsrodb/libsfsrodb.a $(DBLIB) $(LDADD)
testslave_SOURCES = testslave.C
testslave_LDADD = ./libdtest.a ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la ../sfsrodb/libsfsrodb.a $(DBLIB) $(LDADD)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
