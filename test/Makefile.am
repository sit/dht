## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.16 2004/10/02 03:02:52 sit Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

MYLDADD = ../sfsrodb/libsfsrodb.a ../dhash/libdhash.a ../merkle/libmerkle.a \
          ../chord/libchord.a ../svc/libsvc.la $(DBLIB) $(LDADD)

TESTS = test_merkle_tree \
	test_merkle_syncer

check_PROGRAMS = $(TESTS)

#stats_merkle_tree_SOURCES = stats_merkle_tree.C
#stats_merkle_tree_LDADD = $(MYLDADD)

test_merkle_tree_SOURCES = test_merkle_tree.C
test_merkle_tree_LDADD = $(MYLDADD)

test_merkle_syncer_SOURCES = test_merkle_syncer.C
test_merkle_syncer_LDADD = $(MYLDADD)

bin_PROGRAMS = block testslave
noinst_PROGRAMS = example1 example2 #stats_merkle_tree
noinst_LIBRARIES = libdtest.a

libdtest_a_SOURCES = testmaster.C test.C dhashclient_test.C \
	testmaster.h test.h dhashclient_test.h
example1_SOURCES = example1.C
example1_LDADD = ./libdtest.a ../dhash/libdhashclient.a ../svc/libsvc.la $(LDADD)

example2_SOURCES = example2.C
example2_LDADD = ./libdtest.a ../dhash/libdhashclient.a ../svc/libsvc.la $(LDADD)

testslave_SOURCES = testslave.C testslave.h
testslave_LDADD = ./libdtest.a ../dhash/libdhashclient.a ../svc/libsvc.la $(LDADD)

block_SOURCES = block.C
block_LDADD = ./libdtest.a ../dhash/libdhashclient.a ../svc/libsvc.la $(LDADD)

EXTRA_DIST = .cvsignore tesla-0.4a-dhash.tar.gz testwrapper.pl config.txt.sample
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
