## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.6 2003/10/04 21:04:13 yipal Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

bin_PROGRAMS = proxy
proxy_SOURCES = proxy.C gateway.C sync.C
proxy_LDADD = ../dhash/libdhashclient.a ../svc/libsvc.la $(DBLIB) ${LDADD} 

noinst_HEADERS = proxy.h

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

