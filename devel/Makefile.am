## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.6 2002/01/26 00:04:47 fdabek Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

if BUILDVIS
VIS = vis
endif

noinst_PROGRAMS = dbm typetest $(VIS)


dbm_SOURCES = dhash_benchmark.C
dbm_LDADD = ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la $(LDADD)

typetest_SOURCES = test_types.C
typetest_LDADD = ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la $(LDADD)

vis_SOURCES = vis.C
INCLUDES = @GTK_CFLAGS@
vis_LDADD =  ../chord/libchord.a ../svc/libsvc.la @GTK_LIBS@ $(LDADD)


EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

