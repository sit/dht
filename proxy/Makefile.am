## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.3 2003/08/13 20:46:54 benjie Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

bin_PROGRAMS = proxy
proxy_SOURCES = proxy.C gateway.C sync.C
proxy_LDADD = ../svc/libsvc.la $(DBLIB) ${LDADD} 

noinst_HEADERS = proxy.h

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

