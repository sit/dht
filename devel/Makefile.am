## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.11 2003/01/06 20:06:34 cates Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

if GTK_AVAILABLE
VIS = vis
endif

noinst_PROGRAMS = dbm typetest $(VIS)


MYLDADD =  ../dhash/libdhash.a ../merkle/libmerkle.a ../chord/libchord.a   \
          ../svc/libsvc.la ../sfsrodb/libsfsrodb.a

dbm_SOURCES = dhash_benchmark.C
dbm_LDADD = $(MYLDADD) $(DBLIB) $(LDADD)

typetest_SOURCES = test_types.C
typetest_LDADD = $(MYLDADD) $(DBLIB) $(LDADD)

if GTK_AVAILABLE
vis_SOURCES = vis.C
INCLUDES = $(GTK_CFLAGS)
vis_LDADD =  ../chord/libchord.a ../svc/libsvc.la $(GTK_LIBS) $(LDADD)
endif

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

