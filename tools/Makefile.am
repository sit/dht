## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.54 2006/10/08 23:07:46 strib Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

if GTK_AVAILABLE
VIS = vis
endif

bin_PROGRAMS = dbm walk findroute filestore nodeq dbdump lsdping dbm_noauth $(VIS) 
bin_SCRIPTS = dbm.py nodeq-filter
chorddata_DATA =  RPC.py RPCProto.py vischat.py

EXTRA_DIST = $(bin_SCRIPTS) $(chorddata_DATA)
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

noinst_PROGRAMS = coords kdb_stats 
noinst_HEADERS = rpclib.h

dbm_noauth_SOURCES = dbm_noauth.C
dbm_noauth_LDADD = ../dhash/libdhashclient.a ../utils/libutil.a ../svc/libsvc.la $(LDADD)

lsdping_SOURCES = lsdping.C rpclib.C
lsdping_LDADD =  ../svc/libsvc.la ../utils/libutil.a $(LDADD)

walk_SOURCES = walk.C rpclib.C
walk_LDADD = ../chord/libchord.a ../utils/libutil.a ../svc/libsvc.la  $(LDADD) 

coords_SOURCES = walk_coords.C
coords_LDADD = ../chord/libchord.a ../utils/libutil.a ../svc/libsvc.la  $(LDADD) 

filestore_SOURCES = filestore.C
filestore_LDADD = ../dhash/libdhashclient.a  ../utils/libutil.a ../svc/libsvc.la  $(LDADD) 


dbm_SOURCES = dbm.C
dbm_LDADD = ../dhash/libdhashclient.a ../utils/libutil.a ../svc/libsvc.la $(LDADD)

dbdump_SOURCES = dbdump.C
dbdump_LDADD = $(DBLIB) ../utils/dbfe_sleepycat.o $(LDADD)

findroute_SOURCES = findroute.C rpclib.C
findroute_LDADD = ../chord/libchord.a ../svc/libsvc.la ../utils/libutil.a $(LDADD)

nodeq_SOURCES = nodeq.C rpclib.C
nodeq_LDADD = ../svc/libsvc.la ../utils/libutil.a ../merkle/libmerkle.a $(LDADD)

kdb_stats_SOURCES = kdb_stats.C
kdb_stats_LDADD = ../utils/libutil.a ../svc/libsvc.la $(LIBARPC)  $(LIBSFSCRYPT) $(LIBGMP) $(DBLIB) $(LIBASYNC) 

if GTK_AVAILABLE
vis_SOURCES = vis.C vis.h vis_update.C vis_cmd.C
INCLUDES = $(GSTUFF_CFLAGS) 
vis_LDADD =  ../chord/libchord.a ../svc/libsvc.la ../utils/libutil.a $(GSTUFF_LIBS) $(LDADD)
endif
