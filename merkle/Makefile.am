## Process this file with automake to produce Makefile.in

$(PROGRAMS): $(LDEPS) $(DBDEPS)

noinst_LIBRARIES = libmerkle.a
noinst_HEADERS = merkle.h merkle_hash.h merkle_tree.h merkle_server.h merkle_syncer.h merkle_tree_disk.h merkle_tree_bdb.h
libmerkle_a_SOURCES = merkle_server.C merkle_hash.C merkle_tree.C merkle_tree_mem.C merkle_syncer.C merkle_tree_disk.C merkle_tree_bdb.C

TESTS = test_merkle_tree test_merkle_disk test_merkle_syncer 
check_PROGRAMS = $(TESTS)

test_merkle_tree_SOURCES = test_merkle_tree.C
test_merkle_tree_LDADD = libmerkle.a ../utils/libutil.a ../svc/libsvc.la $(LIBARPC) $(LIBSFSCRYPT) $(LIBGMP) $(DBLIB) $(LIBASYNC)

test_merkle_disk_SOURCES = test_merkle_disk.C
test_merkle_disk_LDADD = libmerkle.a ../utils/libutil.a ../svc/libsvc.la $(LIBARPC) $(LIBSFSCRYPT) $(LIBGMP) $(DBLIB) $(LIBASYNC)

test_merkle_syncer_SOURCES = test_merkle_syncer.C
test_merkle_syncer_LDADD = libmerkle.a ../chord/libchord.a ../utils/libutil.a ../svc/libsvc.la $(LIBARPC) $(LIBSFSCRYPT) $(LIBGMP) $(DBLIB) $(LIBASYNC)

CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
