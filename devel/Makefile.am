## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.15 2003/02/23 16:42:29 sit Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

if GTK_AVAILABLE
VIS = vis
endif

noinst_PROGRAMS = dbm typetest $(VIS) walk builddb dbdump findroute

MYLDADD =  ../dhash/libdhash.a ../merkle/libmerkle.a ../svc/libsvc.la

walk_SOURCES = walk.C
walk_LDADD = $(MYLDADD) ../chord/libchord.a $(LDADD)

builddb_SOURCES = builddb.C
builddb_LDADD = ../chord/libchord.a ../sfsrodb/libsfsrodb.a $(DBLIB) $(MYLDADD) $(LDADD)

dbm_SOURCES = dhash_benchmark.C
dbm_LDADD = $(MYLDADD) $(LDADD)

typetest_SOURCES = test_types.C
typetest_LDADD = $(MYLDADD) $(LDADD)

dbdump_SOURCES = dbdump.C
dbdump_LDADD = ../sfsrodb/libsfsrodb.a $(DBLIB) $(LDADD)

findroute_SOURCES = findroute.C
findroute_LDADD = ../chord/libchord.a ../svc/libsvc.la $(LDADD)

if GTK_AVAILABLE
vis_SOURCES = vis.C
INCLUDES = $(GTK_CFLAGS)
vis_LDADD =  ../chord/libchord.a ../svc/libsvc.la $(GTK_LIBS) $(LDADD)
endif

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

