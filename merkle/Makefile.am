## Process this file with automake to produce Makefile.in

$(PROGRAMS): $(LDEPS)


MYLDADD = ../merkle/libmerkle.a ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la $(LDADD)

#noinst_PROGRAMS = test_merkle_tree test_merkle_syncer stats_merkle_tree
#stats_merkle_tree_SOURCES = stats_merkle_tree.C
#stats_merkle_tree_LDADD = $(MYLDADD)
#test_merkle_tree_SOURCES = test_merkle_tree.C
#test_merkle_tree_LDADD = $(MYLDADD)
#test_merkle_syncer_SOURCES = test_merkle_syncer.C
#test_merkle_syncer_LDADD = $(MYLDADD)

noinst_LIBRARIES = libmerkle.a

noinst_HEADERS = merkle.h merkle_hash.h merkle_node.h merkle_tree.h merkle_misc.h

libmerkle_a_SOURCES = merkle_hash.C merkle_node.C merkle_tree.C merkle_syncer.C

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
