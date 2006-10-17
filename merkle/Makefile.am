## Process this file with automake to produce Makefile.in

$(PROGRAMS): $(LDEPS) $(DBDEPS)

noinst_LIBRARIES = libmerkle.a
noinst_HEADERS = merkle.h merkle_hash.h merkle_node.h merkle_tree.h merkle_misc.h merkle_server.h merkle_syncer.h merkle_tree_disk.h
libmerkle_a_SOURCES = merkle_server.C merkle_hash.C merkle_node.C merkle_tree.C merkle_syncer.C  merkle_misc.C merkle_tree_disk.C

check_PROGRAMS = test_merkle_disk

test_merkle_disk_SOURCES = test_merkle_disk.C
test_merkle_disk_LDADD = libmerkle.a ../utils/libutil.a ../svc/libsvc.la $(LIBARPC) $(LIBSFSCRYPT) $(LIBGMP) $(DBLIB) $(LIBASYNC)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
