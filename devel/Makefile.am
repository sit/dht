## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.5 2002/01/10 17:11:30 fdabek Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

noinst_PROGRAMS =  dbm typetest

#noinst_HEADERS = trace_papers.h

#playtrace_SOURCES = playtrace.C
#playtrace_single_SOURCES = playtrace_single.C
dbm_SOURCES = dhash_benchmark.C
dbm_LDADD = ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la $(LDADD)

typetest_SOURCES = test_types.C
typetest_LDADD = ../dhash/libdhash.a ../chord/libchord.a ../svc/libsvc.la $(LDADD)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

