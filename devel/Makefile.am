## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.1 2001/06/21 19:03:12 fdabek Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

noinst_PROGRAMS = playtrace playtrace_single dbm

noinst_HEADERS = trace_papers.h

playtrace_SOURCES = playtrace.C
playtrace_single_SOURCES = playtrace_single.C
dbm_SOURCES = dhash_benchmark.C
dbm_LDADD = ../svc/libsvc.la ../dhash/libdhash.a $(LDADD)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

