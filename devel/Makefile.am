## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.27 2003/10/14 22:47:08 athicha Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

if GTK_AVAILABLE
VIS = vis
endif

noinst_PROGRAMS = dbm typetest $(VIS) walk builddb dbdump findroute \
		  coords nodeq filestore \
		  dhc_test

MYLDADD =  ../dhash/libdhash.a ../dhc/libdhc.a ../merkle/libmerkle.a ../svc/libsvc.la

walk_SOURCES = walk.C
walk_LDADD = $(MYLDADD) ../chord/libchord.a ../utils/libutil.a ../svc/libsvc.la  $(LDADD) 

coords_SOURCES = walk_coords.C
coords_LDADD = $(MYLDADD) ../chord/libchord.a ../utils/libutil.a ../svc/libsvc.la  $(LDADD) 

filestore_SOURCES = filestore.C
filestore_LDADD = ../dhash/libdhashclient.a ../dhc/libdhc.a ../utils/libutil.a ../svc/libsvc.la  $(LDADD) 

builddb_SOURCES = builddb.C
builddb_LDADD = ../sfsrodb/libsfsrodb.a ../dhash/libdhash.a ../dhc/libdhc.a ../merkle/libmerkle.a ../chord/libchord.a ../utils/libutil.a ../svc/libsvc.la $(DBLIB) $(LDADD)

dbm_SOURCES = dhash_benchmark.C
dbm_LDADD = ../dhash/libdhashclient.a ../svc/libsvc.la $(LDADD)

typetest_SOURCES = test_types.C
typetest_LDADD = ../dhash/libdhashclient.a ../svc/libsvc.la $(LDADD)

dbdump_SOURCES = dbdump.C
dbdump_LDADD = ../sfsrodb/libsfsrodb.a ../merkle/libmerkle.a $(DBLIB) $(LDADD)

findroute_SOURCES = findroute.C rpclib.C
findroute_LDADD = ../chord/libchord.a ../svc/libsvc.la ../utils/libutil.a $(LDADD)

nodeq_SOURCES = nodeq.C rpclib.C
nodeq_LDADD = ../svc/libsvc.la ../utils/libutil.a $(LDADD)

dhc_test_SOURCES = dhc_test.C
dhc_test_LDADD = ../dhash/libdhash.a ../dhc/libdhc.a ../merkle/libmerkle.a \
		 ../chord/libchord.a ../utils/libutil.a ../svc/libsvc.la \
                 $(DBLIB) $(LDADD)

if GTK_AVAILABLE
vis_SOURCES = vis.C
INCLUDES = $(GSTUFF_CFLAGS) 
vis_LDADD =  ../chord/libchord.a ../svc/libsvc.la ../utils/libutil.a $(GSTUFF_LIBS) $(LDADD)
endif

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

