## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.18 2003/03/13 23:38:37 jastr Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

if GTK_AVAILABLE
VIS = vis
endif

noinst_PROGRAMS = dbm typetest $(VIS) walk builddb dbdump findroute

MYLDADD =  ../dhash/libdhash.a ../merkle/libmerkle.a ../svc/libsvc.la

walk_SOURCES = walk.C
walk_LDADD = $(MYLDADD) ../chord/libchord.a ../utils/libutil.a ../svc/libsvc.la  $(LDADD) 

builddb_SOURCES = builddb.C
builddb_LDADD = ../sfsrodb/libsfsrodb.a ../chord/libchord.a ../utils/libutil.a $(DBLIB) $(MYLDADD) $(LDADD)

dbm_SOURCES = dhash_benchmark.C
dbm_LDADD = $(MYLDADD) $(LDADD)

typetest_SOURCES = test_types.C
typetest_LDADD = $(MYLDADD) $(LDADD)

dbdump_SOURCES = dbdump.C
dbdump_LDADD = ../sfsrodb/libsfsrodb.a $(DBLIB) $(LDADD)

findroute_SOURCES = findroute.C
findroute_LDADD = ../chord/libchord.a ../svc/libsvc.la ../utils/libutil.a $(LDADD)

if GTK_AVAILABLE
vis_SOURCES = vis.C
INCLUDES = $(GTK_CFLAGS)
vis_LDADD =  ../chord/libchord.a ../svc/libsvc.la ../utils/libutil.a $(GTK_LIBS) $(LDADD)
endif

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

