## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.12 2003/01/18 13:13:39 jsr Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

if GTK_AVAILABLE
VIS = vis
endif

noinst_PROGRAMS = dbm typetest $(VIS)


MYLDADD =  ../dhash/libdhash.a ../svc/libsvc.la

dbm_SOURCES = dhash_benchmark.C
dbm_LDADD = $(MYLDADD) $(LDADD)

typetest_SOURCES = test_types.C
typetest_LDADD = $(MYLDADD) $(LDADD)

if GTK_AVAILABLE
vis_SOURCES = vis.C
INCLUDES = $(GTK_CFLAGS)
vis_LDADD =  ../chord/libchord.a ../svc/libsvc.la $(GTK_LIBS) $(LDADD)
endif

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

