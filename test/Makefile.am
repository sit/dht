## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.12 2003/01/08 01:57:05 cates Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

MYLDADD = ../sfsrodb/libsfsrodb.a ../dhash/libdhash.a ../merkle/libmerkle.a \
          ../chord/libchord.a ../svc/libsvc.la $(DBLIB) $(LDADD)

#stats_merkle_tree_SOURCES = stats_merkle_tree.C
#stats_merkle_tree_LDADD = $(MYLDADD)

test_merkle_tree_SOURCES = test_merkle_tree.C
test_merkle_tree_LDADD = $(MYLDADD)

#test_merkle_syncer_SOURCES = test_merkle_syncer.C
#test_merkle_syncer_LDADD = $(MYLDADD)

bin_PROGRAMS = block testslave
noinst_PROGRAMS = example1 example2 test_merkle_tree #stats_merkle_tree test_merkle_syncer
noinst_LIBRARIES = libdtest.a

libdtest_a_SOURCES = testmaster.C test.C dhashclient_test.C
example1_SOURCES = example1.C
example1_LDADD = ./libdtest.a $(MYLDADD)

example2_SOURCES = example2.C
example2_LDADD = ./libdtest.a $(MYLDADD)

testslave_SOURCES = testslave.C
testslave_LDADD  = ./libdtest.a $(MYLDADD)

block_SOURCES = block.C
block_LDADD  = ./libdtest.a $(MYLDADD)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
