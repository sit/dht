## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.44 2005/05/24 20:58:55 sit Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

if GTK_AVAILABLE
VIS = vis
endif

bin_PROGRAMS = dbm walk findroute filestore nodeq dbdump lsdping $(VIS) 
bin_SCRIPTS = dbm.py usenetlsdmon.py nodeq-filter
chorddata_DATA =  RPC.py RPCProto.py vischat.py

EXTRA_DIST = .cvsignore $(bin_SCRIPTS) $(chorddata_DATA)
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in


noinst_PROGRAMS = test_types coords dhc_test pkc builddb
noinst_HEADERS = rpclib.h

lsdping_SOURCES = lsdping.C rpclib.C
lsdping_LDADD =  ../svc/libsvc.la ../utils/libutil.a ../merkle/libmerkle.a $(LDADD)

walk_SOURCES = walk.C rpclib.C
walk_LDADD = ../chord/libchord.a ../utils/libutil.a ../svc/libsvc.la  $(LDADD) 

coords_SOURCES = walk_coords.C
coords_LDADD = ../chord/libchord.a ../utils/libutil.a ../svc/libsvc.la  $(LDADD) 

filestore_SOURCES = filestore.C
filestore_LDADD = ../dhash/libdhashclient.a ../dhc/libdhc.a ../utils/libutil.a ../svc/libsvc.la  $(LDADD) 

builddb_SOURCES = builddb.C
builddb_LDADD = ../sfsrodb/libsfsrodb.a ../dhash/libdhash.a ../dhc/libdhc.a ../merkle/libmerkle.a ../chord/libchord.a ../utils/libutil.a ../svc/libsvc.la $(DBLIB) $(LDADD)

dbm_SOURCES = dhash_benchmark.C
dbm_LDADD = ../dhash/libdhashclient.a ../utils/libutil.a ../svc/libsvc.la $(LDADD)

test_types_SOURCES = test_types.C
test_types_LDADD = ../dhash/libdhashclient.a ../utils/libutil.a ../svc/libsvc.la $(LDADD)

dbdump_SOURCES = dbdump.C
dbdump_LDADD = $(DBLIB) $(LDADD)

findroute_SOURCES = findroute.C rpclib.C
findroute_LDADD = ../chord/libchord.a ../svc/libsvc.la ../utils/libutil.a $(LDADD)

nodeq_SOURCES = nodeq.C rpclib.C
nodeq_LDADD = ../svc/libsvc.la ../utils/libutil.a ../merkle/libmerkle.a $(LDADD)

dhc_test_SOURCES = dhc_test.C
dhc_test_LDADD = ../dhash/libdhashclient.a ../utils/libutil.a ../svc/libsvc.la $(LDADD)

pkc_SOURCES = pk_consistency.C
pkc_LDADD = ../dhash/libdhashclient.a ../utils/libutil.a ../svc/libsvc.la $(LDADD)

if GTK_AVAILABLE
vis_SOURCES = vis.C vis.h vis_update.C vis_cmd.C
INCLUDES = $(GSTUFF_CFLAGS) 
vis_LDADD =  ../chord/libchord.a ../svc/libsvc.la ../utils/libutil.a $(GSTUFF_LIBS) $(LDADD)
endif
