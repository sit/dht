## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.2 2001/11/01 23:41:18 fdabek Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

noinst_PROGRAMS =  dbm 

#noinst_HEADERS = trace_papers.h

#playtrace_SOURCES = playtrace.C
#playtrace_single_SOURCES = playtrace_single.C
dbm_SOURCES = dhash_benchmark.C
dbm_LDADD = ../svc/libsvc.la ../dhash/libdhash.a $(LDADD)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

