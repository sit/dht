## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.7 2003/11/13 21:04:55 yipal Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

bin_PROGRAMS = proxy
proxy_SOURCES = proxy.C gateway.C sync.C
proxy_LDADD = ../dhash/libdhashclient.a ../svc/libsvc.la ../merkle/libmerkle.a $(DBLIB) ${LDADD} 

noinst_HEADERS = proxy.h

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

